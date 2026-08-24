/*
Copyright 2026 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package capacitytiers implements ordered capacity-tier selection for gangs.
//
// A PodGroup opts in by carrying an ordered list of node capacity types
// (e.g. "reserved,spot") and a per-tier acquisition budget. The plugin keeps
// the whole gang homogeneous on exactly one tier at a time via a hard
// predicate (node label must match the gang's current tier), and advances the
// gang to the next tier when the current one has failed to admit the gang for
// longer than its budget. Tier state is persisted as PodGroup annotations so
// scheduler restarts do not reset the clock, and the clock is frozen once the
// gang is running.
//
// PodGroup annotations (spec, set by the client):
//
//	exa.ai/capacity-tiers: "reserved,spot"     ordered tier list (required to opt in)
//	exa.ai/tier-fallback-seconds: "300"        per-tier acquisition budget (default 300)
//
// PodGroup annotations (state, managed by this plugin):
//
//	exa.ai/capacity-tier: "reserved"           tier currently in force
//	exa.ai/capacity-tier-since: RFC3339        when that tier was entered
//
// Scheduler configuration:
//
//	tiers:
//	- plugins:
//	  - name: capacitytiers
//	    arguments:
//	      capacitytiers.nodeLabelKey: karpenter.sh/capacity-type
package capacitytiers

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	"volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

const (
	// PluginName indicates name of volcano scheduler plugin.
	PluginName = "capacitytiers"

	// TiersAnnotation is the ordered, comma-separated capacity tier list.
	TiersAnnotation = "exa.ai/capacity-tiers"
	// FallbackSecondsAnnotation is the per-tier acquisition budget in seconds.
	FallbackSecondsAnnotation = "exa.ai/tier-fallback-seconds"
	// CurrentTierAnnotation records the tier currently in force.
	CurrentTierAnnotation = "exa.ai/capacity-tier"
	// TierSinceAnnotation records when the current tier was entered (RFC3339).
	TierSinceAnnotation = "exa.ai/capacity-tier-since"

	// nodeLabelKeyArg overrides the node label consulted by the tier predicate.
	nodeLabelKeyArg = "capacitytiers.nodeLabelKey"
	// defaultNodeLabelKey matches Karpenter-provisioned nodes.
	defaultNodeLabelKey = "karpenter.sh/capacity-type"

	// defaultFallbackSeconds bounds each non-final tier when the annotation is
	// missing or malformed (fail closed to a finite budget, never to "wait
	// forever on the first tier").
	defaultFallbackSeconds = 300
)

type capacityTiersPlugin struct {
	pluginArguments framework.Arguments
	nodeLabelKey    string
	// jobTier maps job UID to the tier in force for this session.
	jobTier map[api.JobID]string
	// now is injectable for tests.
	now func() time.Time
}

// New returns a capacitytiers plugin instance.
func New(arguments framework.Arguments) framework.Plugin {
	p := &capacityTiersPlugin{
		pluginArguments: arguments,
		nodeLabelKey:    defaultNodeLabelKey,
		jobTier:         map[api.JobID]string{},
		now:             time.Now,
	}
	if v, ok := arguments[nodeLabelKeyArg].(string); ok && v != "" {
		p.nodeLabelKey = v
	}
	return p
}

func (p *capacityTiersPlugin) Name() string {
	return PluginName
}

// tierSpec is the parsed opt-in state of one PodGroup.
type tierSpec struct {
	tiers   []string
	budget  time.Duration
	current string
	since   time.Time
}

// parseTierSpec extracts the tier configuration and durable tier state from
// PodGroup annotations. Returns nil when the PodGroup has not opted in, and an
// error when the opt-in annotation is present but unusable.
func parseTierSpec(annotations map[string]string, now time.Time) (*tierSpec, error) {
	raw, ok := annotations[TiersAnnotation]
	if !ok {
		return nil, nil
	}
	var tiers []string
	seen := map[string]struct{}{}
	for _, t := range strings.Split(raw, ",") {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		if _, dup := seen[t]; dup {
			return nil, fmt.Errorf("duplicate tier %q in %s=%q", t, TiersAnnotation, raw)
		}
		seen[t] = struct{}{}
		tiers = append(tiers, t)
	}
	if len(tiers) == 0 {
		return nil, fmt.Errorf("no tiers in %s=%q", TiersAnnotation, raw)
	}

	budget := time.Duration(defaultFallbackSeconds) * time.Second
	if rawBudget, ok := annotations[FallbackSecondsAnnotation]; ok {
		secs, err := strconv.ParseInt(rawBudget, 10, 64)
		if err != nil || secs <= 0 {
			klog.Warningf("capacitytiers: invalid %s=%q, using default %ds", FallbackSecondsAnnotation, rawBudget, defaultFallbackSeconds)
		} else {
			budget = time.Duration(secs) * time.Second
		}
	}

	spec := &tierSpec{tiers: tiers, budget: budget}

	spec.current = annotations[CurrentTierAnnotation]
	if spec.current != "" {
		if _, valid := seen[spec.current]; !valid {
			klog.Warningf("capacitytiers: stale %s=%q not in %v, restarting at first tier", CurrentTierAnnotation, spec.current, tiers)
			spec.current = ""
		}
	}
	spec.since = now
	if rawSince, ok := annotations[TierSinceAnnotation]; ok && spec.current != "" {
		since, err := time.Parse(time.RFC3339, rawSince)
		if err != nil {
			klog.Warningf("capacitytiers: invalid %s=%q, resetting tier clock", TierSinceAnnotation, rawSince)
		} else {
			spec.since = since
		}
	}
	return spec, nil
}

// tierIndex returns the position of tier in tiers, or -1.
func tierIndex(tiers []string, tier string) int {
	for i, t := range tiers {
		if t == tier {
			return i
		}
	}
	return -1
}

// resolveTier decides which tier is in force now. It returns the tier and
// whether the durable state changed (a fresh gang entering the first tier, or
// an acquiring gang whose budget expired advancing to the next tier).
func resolveTier(spec *tierSpec, acquiring bool, now time.Time) (tier string, changed bool) {
	if spec.current == "" {
		return spec.tiers[0], true
	}
	idx := tierIndex(spec.tiers, spec.current)
	if !acquiring {
		return spec.current, false
	}
	if now.Sub(spec.since) > spec.budget && idx+1 < len(spec.tiers) {
		return spec.tiers[idx+1], true
	}
	return spec.current, false
}

// jobAcquiring reports whether the gang is still acquiring capacity: the tier
// clock only runs while the gang is not yet running or completed.
func jobAcquiring(job *api.JobInfo) bool {
	switch job.PodGroup.Status.Phase {
	case scheduling.PodGroupRunning, scheduling.PodGroupCompleted:
		return false
	default:
		return true
	}
}

// stampTier durably records the tier transition on the PodGroup.
func stampTier(ssn *framework.Session, job *api.JobInfo, tier string, now time.Time) error {
	patch := fmt.Sprintf(`{"metadata":{"annotations":{%q:%q,%q:%q}}}`,
		CurrentTierAnnotation, tier, TierSinceAnnotation, now.UTC().Format(time.RFC3339))
	_, err := ssn.VCClient().SchedulingV1beta1().PodGroups(job.PodGroup.Namespace).Patch(
		context.TODO(), job.PodGroup.Name, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	return err
}

func (p *capacityTiersPlugin) OnSessionOpen(ssn *framework.Session) {
	p.jobTier = map[api.JobID]string{}
	now := p.now()

	for _, job := range ssn.Jobs {
		if job.PodGroup == nil {
			continue
		}
		spec, err := parseTierSpec(job.PodGroup.Annotations, now)
		if err != nil {
			klog.Errorf("capacitytiers: job <%s/%s>: %v; leaving gang unrestricted", job.Namespace, job.Name, err)
			continue
		}
		if spec == nil {
			continue
		}

		tier, changed := resolveTier(spec, jobAcquiring(job), now)
		p.jobTier[job.UID] = tier
		if changed {
			klog.V(3).Infof("capacitytiers: job <%s/%s> entering tier %q (was %q, budget %s)",
				job.Namespace, job.Name, tier, spec.current, spec.budget)
			if err := stampTier(ssn, job, tier, now); err != nil {
				// The in-memory tier still applies this session; the stamp is
				// retried on the next session open.
				klog.Errorf("capacitytiers: failed to stamp tier %q on podgroup <%s/%s>: %v",
					tier, job.PodGroup.Namespace, job.PodGroup.Name, err)
			}
		}
	}

	predicateFn := func(task *api.TaskInfo, node *api.NodeInfo) error {
		tier, ok := p.jobTier[task.Job]
		if !ok {
			return nil
		}
		nodeTier := ""
		if node.Node != nil {
			nodeTier = node.Node.Labels[p.nodeLabelKey]
		}
		if nodeTier != tier {
			return api.NewFitErrWithStatus(task, node, &api.Status{
				Code: api.UnschedulableAndUnresolvable,
				Reason: fmt.Sprintf("node capacity type %q does not match gang capacity tier %q",
					nodeTier, tier),
			})
		}
		return nil
	}
	ssn.AddPredicateFn(p.Name(), predicateFn)
}

func (p *capacityTiersPlugin) OnSessionClose(ssn *framework.Session) {}
