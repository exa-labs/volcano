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

package rescheduling

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/mitchellh/mapstructure"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/plugins/capacitycost"
)

// CapacityUpgradeStrategy moves running GPU work from expensive capacity to
// cheaper capacity once the cheaper tier can hold it. Capacity tiers come
// from the same node-label rank as the capacitycost plugin (reserved < spot
// < on-demand by default). A PodGroup is a candidate when at least one
// member runs above the cheapest tier, every member is at or below the
// victim priority ceiling, old enough, not opted out, and its cooldown clock
// has expired. Fit is proven per pass against a shared ledger of the target
// tier's idle capacity plus what the preempt action would free there by
// evicting strictly lower-priority pods, so a target full of filler work
// (a lower-priority Deployment soaking up the reserved pool) still counts as
// room.
//
// Single-member PodGroups are evicted directly, like gpuFragmentation; the
// controller recreates the pod and the capacitycost scorer plus preemption
// land it on the cheaper tier. Multi-member gangs are not evicted here: a
// gang move is a whole-job restart with checkpoint cost the scheduler cannot
// judge, so the strategy records a proposal on the PodGroup
// (exa.ai/capacity-upgrade) and leaves execution to the gang's lifecycle
// owner, which restarts every member atomically once its own gates pass.
const CapacityUpgradeStrategy = "capacityUpgrade"

// DefaultCapacityUpgradeConf holds the default (dry-run) configuration.
var DefaultCapacityUpgradeConf = map[string]interface{}{
	"dryRun":            true,
	"gpuResource":       "nvidia.com/gpu",
	"nodeLabelKey":      capacitycost.DefaultNodeLabelKey,
	"order":             capacitycost.DefaultOrder,
	"unlabeledRank":     capacitycost.DefaultUnlabeledRank,
	"zoneLabel":         "topology.kubernetes.io/zone",
	"optOutLabel":       "exa.ai/capacity-upgrade-eligible",
	"cooldownSeconds":   1800,
	"minPodAgeSeconds":  600,
	"maxVictims":        8,
	"maxGangProposals":  2,
	"maxVictimPriority": -1,
}

// CapacityUpgradeKillSwitchEnv disables the strategy entirely when set to
// "true" on the scheduler process.
const CapacityUpgradeKillSwitchEnv = "EXA_CAPACITY_UPGRADE_DISABLED"

const (
	// CapacityUpgradeProposalAnnotation carries a capacityUpgradeProposal
	// (JSON) on a gang's PodGroup for its lifecycle owner to execute.
	CapacityUpgradeProposalAnnotation = "exa.ai/capacity-upgrade"
	// CapacityUpgradeLastAnnotation is the PodGroup's cooldown clock: the
	// RFC3339 time of its last eviction or proposal by this strategy.
	CapacityUpgradeLastAnnotation = "exa.ai/capacity-upgrade-last"
)

type capacityUpgradeConf struct {
	DryRun      bool   `mapstructure:"dryRun"`
	GpuResource string `mapstructure:"gpuResource"`
	// NodeLabelKey, Order and UnlabeledRank define the capacity rank exactly
	// as in the capacitycost plugin; keep them identical in the scheduler
	// configuration so placement and migration agree on what is cheaper.
	NodeLabelKey  string `mapstructure:"nodeLabelKey"`
	Order         string `mapstructure:"order"`
	UnlabeledRank int    `mapstructure:"unlabeledRank"`
	// ZoneLabel groups target nodes so a gang is only proposed a placement
	// inside a single zone.
	ZoneLabel string `mapstructure:"zoneLabel"`
	// OptOutLabel excludes a pod (and thereby its whole PodGroup) when set
	// to "false".
	OptOutLabel string `mapstructure:"optOutLabel"`
	// CooldownSeconds holds a PodGroup after an eviction or proposal so a
	// job that bounces between tiers is not moved again immediately.
	CooldownSeconds int `mapstructure:"cooldownSeconds"`
	// MinPodAgeSeconds keeps freshly started pods in place: a pod younger
	// than this has done too little work to be worth restarting.
	MinPodAgeSeconds int `mapstructure:"minPodAgeSeconds"`
	// MaxVictims caps direct (single-member) evictions per pass.
	MaxVictims int `mapstructure:"maxVictims"`
	// MaxGangProposals caps gang proposals per pass.
	MaxGangProposals int `mapstructure:"maxGangProposals"`
	// MaxVictimPriority is the highest pod priority still movable; pods
	// without an explicit priority count as 0.
	MaxVictimPriority int32 `mapstructure:"maxVictimPriority"`
}

func newCapacityUpgradeConf() *capacityUpgradeConf {
	conf := &capacityUpgradeConf{}
	_ = mapstructure.Decode(DefaultCapacityUpgradeConf, conf)
	return conf
}

func (c *capacityUpgradeConf) parse(configs map[string]interface{}) {
	if len(configs) == 0 {
		return
	}
	if err := mapstructure.Decode(configs, c); err != nil {
		klog.Errorf("capacityUpgrade: bad strategy params, keeping defaults: %v", err)
		*c = *newCapacityUpgradeConf()
	}
}

func (c *capacityUpgradeConf) ranker() *capacitycost.Ranker {
	return capacitycost.NewRanker(c.NodeLabelKey, c.Order, c.UnlabeledRank)
}

// capacityUpgradeProposal is the JSON body of CapacityUpgradeProposalAnnotation.
type capacityUpgradeProposal struct {
	// Target is the capacity type (node label value) the gang fits on.
	Target string `json:"target"`
	// From is the most expensive capacity type a member currently runs on.
	From string `json:"from"`
	// Zone is the topology zone of every node in Nodes.
	Zone string `json:"zone,omitempty"`
	// Nodes lists the target nodes the simulated placement used.
	Nodes []string `json:"nodes"`
	// Members is the number of gang pods that would restart.
	Members int `json:"members"`
	// Gpus is the gang's total GPU request.
	Gpus float64 `json:"gpus"`
	// Preempting is the number of lower-priority pods the placement relies
	// on the preempt action evicting from the target nodes.
	Preempting int `json:"preempting"`
	// At is when the proposal was made (RFC3339).
	At string `json:"at"`
}

// capacityUpgradePlan is one PodGroup's planned move.
type capacityUpgradePlan struct {
	job      *api.JobInfo
	members  []*api.TaskInfo
	proposal capacityUpgradeProposal
	// gang is true when the move must go through the lifecycle owner.
	gang bool
}

var victimsFnForCapacityUpgrade = func(tasks []*api.TaskInfo) []*api.TaskInfo {
	if Session == nil {
		return nil
	}
	if os.Getenv(CapacityUpgradeKillSwitchEnv) == "true" {
		klog.V(2).Infof("capacityUpgrade: disabled via %s", CapacityUpgradeKillSwitchEnv)
		return nil
	}
	conf := newCapacityUpgradeConf()
	if params, ok := RegisteredStrategyConfigs[CapacityUpgradeStrategy].(map[string]interface{}); ok {
		conf.parse(params)
	}
	capacityUpgradePasses.Inc()

	running := make(map[types.UID]*api.TaskInfo, len(tasks))
	for _, task := range tasks {
		if task.Pod != nil {
			running[task.Pod.UID] = task
		}
	}

	probes := make(map[types.UID]*api.TaskInfo)
	preFailed := make(map[types.UID]error)
	plans := planCapacityUpgrades(Session.Nodes, Session.Jobs, running, conf, time.Now(), func(task *api.TaskInfo, node *api.NodeInfo, gang bool) error {
		uid := task.Pod.UID
		if err, ok := preFailed[uid]; ok {
			return err
		}
		probe, ok := probes[uid]
		if !ok {
			probe = probeTask(task)
			if probe == nil {
				preFailed[uid] = fmt.Errorf("task has no pod")
				return preFailed[uid]
			}
			if gang {
				// Gang peers currently pin each other to the source zone
				// through pod affinity; the planner enforces a single
				// target zone itself, so the peer terms must not veto.
				if probe.Pod.Spec.Affinity != nil {
					probe.Pod.Spec.Affinity.PodAffinity = nil
					probe.Pod.Spec.Affinity.PodAntiAffinity = nil
				}
			}
			if err := Session.PrePredicateFn(probe); err != nil {
				preFailed[uid] = err
				return err
			}
			probes[uid] = probe
		}
		return Session.PredicateFn(probe, node)
	})

	victims := make([]*api.TaskInfo, 0)
	for _, plan := range plans {
		kind := "pod"
		if plan.gang {
			kind = "gang"
		}
		pg := plan.job.PodGroup
		if conf.DryRun {
			klog.V(2).Infof("capacityUpgrade[dry-run]: would move %s %s/%s (%d pods, %v GPUs) %s -> %s on %v (preempting %d)",
				kind, pg.Namespace, pg.Name, plan.proposal.Members, plan.proposal.Gpus,
				plan.proposal.From, plan.proposal.Target, plan.proposal.Nodes, plan.proposal.Preempting)
			plan.observe(kind, "dry_run")
			continue
		}
		if plan.gang {
			if err := stampCapacityUpgradeProposal(plan); err != nil {
				klog.Errorf("capacityUpgrade: skip proposal for %s/%s: %v", pg.Namespace, pg.Name, err)
				capacityUpgradeStampFailures.WithLabelValues("proposal").Inc()
				continue
			}
			klog.V(2).Infof("capacityUpgrade: proposed moving gang %s/%s (%d pods, %v GPUs) %s -> %s on %v (preempting %d)",
				pg.Namespace, pg.Name, plan.proposal.Members, plan.proposal.Gpus,
				plan.proposal.From, plan.proposal.Target, plan.proposal.Nodes, plan.proposal.Preempting)
			plan.observe(kind, "proposed")
			continue
		}
		if err := stampCapacityUpgradeVictim(plan); err != nil {
			klog.Errorf("capacityUpgrade: skip eviction for %s/%s: %v", pg.Namespace, pg.Name, err)
			capacityUpgradeStampFailures.WithLabelValues("victim").Inc()
			continue
		}
		for _, member := range plan.members {
			klog.V(2).Infof("capacityUpgrade: evicting %s/%s from %s (%s -> %s, fits %v, preempting %d)",
				member.Namespace, member.Name, member.NodeName,
				plan.proposal.From, plan.proposal.Target, plan.proposal.Nodes, plan.proposal.Preempting)
			victims = append(victims, member)
		}
		plan.observe(kind, "live")
	}
	return victims
}

// observe records the plan in the strategy's Prometheus counters.
func (p capacityUpgradePlan) observe(kind, mode string) {
	capacityUpgradeMoves.WithLabelValues(p.proposal.Target, kind, mode).Inc()
	capacityUpgradePods.WithLabelValues(p.proposal.Target, kind, mode).Add(float64(p.proposal.Members))
	capacityUpgradeGpus.WithLabelValues(p.proposal.Target, kind, mode).Add(p.proposal.Gpus)
}

// capacityUpgradePredicate evaluates a member against a target node; gang
// tells the caller whether peer affinity must be relaxed.
type capacityUpgradePredicate func(task *api.TaskInfo, node *api.NodeInfo, gang bool) error

// planCapacityUpgrades returns the moves a pass would make, highest-priority
// then oldest PodGroup first, until the per-pass budgets are spent. For each
// candidate PodGroup, target tiers are tried cheapest first and the first
// tier that fits the whole group (within one zone, for gangs) wins. Fit is
// simulated first-fit-decreasing over a ledger shared by every plan in the
// pass, so two plans cannot claim the same freeable capacity.
func planCapacityUpgrades(
	nodes map[string]*api.NodeInfo,
	jobs map[api.JobID]*api.JobInfo,
	running map[types.UID]*api.TaskInfo,
	conf *capacityUpgradeConf,
	now time.Time,
	predicate capacityUpgradePredicate,
) []capacityUpgradePlan {
	gpu := v1.ResourceName(conf.GpuResource)
	ranker := conf.ranker()
	if ranker.MaxRank() <= 0 {
		return nil
	}

	// Target nodes per rank, restricted to GPU nodes.
	byRank := make(map[int][]*api.NodeInfo)
	for _, node := range nodes {
		if node.Node == nil || node.Allocatable.Get(gpu) <= 0 {
			continue
		}
		rank := ranker.Rank(node)
		byRank[rank] = append(byRank[rank], node)
	}
	for _, members := range byRank {
		sort.Slice(members, func(i, j int) bool { return members[i].Name < members[j].Name })
	}

	candidates := make([]capacityUpgradeCandidate, 0)
	for _, job := range jobs {
		cand, ok := capacityUpgradeCandidateFor(job, nodes, running, conf, ranker, gpu, now)
		if ok {
			candidates = append(candidates, cand)
		}
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].priority != candidates[j].priority {
			return candidates[i].priority > candidates[j].priority
		}
		if !candidates[i].started.Equal(candidates[j].started) {
			return candidates[i].started.Before(candidates[j].started)
		}
		return candidates[i].job.UID < candidates[j].job.UID
	})

	ledger := newUpgradeLedger(nodes)
	plans := make([]capacityUpgradePlan, 0)
	evictions, proposals := 0, 0
	for _, cand := range candidates {
		if cand.gang && conf.MaxGangProposals > 0 && proposals >= conf.MaxGangProposals {
			continue
		}
		if !cand.gang && conf.MaxVictims > 0 && evictions+len(cand.members) > conf.MaxVictims {
			continue
		}
		var placement *capacityUpgradePlacement
		targetRank := 0
		for rank := 0; rank < cand.maxRank; rank++ {
			targets := byRank[rank]
			if len(targets) == 0 {
				continue
			}
			placement = simulateUpgrade(targets, cand, conf, gpu, ledger, predicate)
			if placement != nil {
				targetRank = rank
				break
			}
		}
		if placement == nil {
			continue
		}
		ledger.commit(cand.members, placement)
		target := ""
		if len(placement.nodes) > 0 {
			target = nodes[placement.nodes[0]].Node.Labels[conf.NodeLabelKey]
		}
		plan := capacityUpgradePlan{
			job:     cand.job,
			members: cand.members,
			gang:    cand.gang,
			proposal: capacityUpgradeProposal{
				Target:     target,
				From:       cand.from,
				Zone:       placement.zone,
				Nodes:      placement.nodes,
				Members:    len(cand.members),
				Gpus:       cand.gpus / 1000,
				Preempting: placement.preempting,
				At:         now.UTC().Format(time.RFC3339),
			},
		}
		klog.V(4).Infof("capacityUpgrade: %s/%s fits rank %d", cand.job.Namespace, cand.job.Name, targetRank)
		plans = append(plans, plan)
		if cand.gang {
			proposals++
		} else {
			evictions += len(cand.members)
		}
	}
	return plans
}

// capacityUpgradeCandidate is a PodGroup eligible for a move.
type capacityUpgradeCandidate struct {
	job     *api.JobInfo
	members []*api.TaskInfo
	gang    bool
	// priority is the group's highest member priority.
	priority int32
	// started is the group's oldest member start time.
	started time.Time
	// maxRank is the most expensive rank a member runs on; targets must
	// rank strictly below it.
	maxRank int
	// from is the capacity type label of that most expensive node.
	from string
	// gpus is the group's total GPU request in scheduler milli-units.
	gpus float64
}

// capacityUpgradeCandidateFor decides whether a job may move and gathers
// what the planner needs. Every GPU member must be running, controlled,
// old enough, at or below the priority ceiling and not opted out; at least
// one must run above the cheapest tier; the group's cooldown must have
// expired.
func capacityUpgradeCandidateFor(
	job *api.JobInfo,
	nodes map[string]*api.NodeInfo,
	running map[types.UID]*api.TaskInfo,
	conf *capacityUpgradeConf,
	ranker *capacitycost.Ranker,
	gpu v1.ResourceName,
	now time.Time,
) (capacityUpgradeCandidate, bool) {
	cand := capacityUpgradeCandidate{job: job}
	if job.PodGroup == nil || len(job.Tasks) == 0 {
		return cand, false
	}
	if !podGroupCooled(job.PodGroup, conf, now) {
		return cand, false
	}
	minAge := time.Duration(conf.MinPodAgeSeconds) * time.Second
	for _, task := range job.Tasks {
		if task.Pod == nil {
			return cand, false
		}
		if task.Resreq.Get(gpu) <= 0 && task.InitResreq.Get(gpu) <= 0 {
			// Non-GPU members (a launcher, a sidecar job) ride along with
			// the gang but neither gate nor size the move.
			continue
		}
		sessionTask, isRunning := running[task.Pod.UID]
		if !isRunning || sessionTask.Status != api.Running {
			return cand, false
		}
		if task.Pod.Labels[conf.OptOutLabel] == "false" {
			return cand, false
		}
		if task.Priority > conf.MaxVictimPriority {
			return cand, false
		}
		if metav1.GetControllerOf(task.Pod) == nil {
			return cand, false
		}
		started := podStartTime(task.Pod)
		if started.IsZero() || now.Sub(started) < minAge {
			return cand, false
		}
		node, ok := nodes[task.NodeName]
		if !ok || node.Node == nil {
			return cand, false
		}
		rank := ranker.Rank(node)
		if rank > cand.maxRank {
			cand.maxRank = rank
			cand.from = node.Node.Labels[conf.NodeLabelKey]
		}
		if len(cand.members) == 0 {
			cand.priority = task.Priority
			cand.started = started
		} else {
			if task.Priority > cand.priority {
				cand.priority = task.Priority
			}
			if started.Before(cand.started) {
				cand.started = started
			}
		}
		cand.gpus += taskGpuNeed(sessionTask, gpu).Get(gpu)
		cand.members = append(cand.members, sessionTask)
	}
	if len(cand.members) == 0 || cand.maxRank == 0 {
		return cand, false
	}
	sort.Slice(cand.members, func(i, j int) bool { return cand.members[i].Name < cand.members[j].Name })
	cand.gang = len(cand.members) > 1 || job.PodGroup.Spec.MinMember > 1
	if !cand.gang && cand.members[0].Pod.Annotations[doNotDisruptAnnotation] == "true" {
		// Direct eviction honours the Karpenter opt-out like gpuFragmentation
		// does; gangs are handed to their owner, who manages that annotation.
		return cand, false
	}
	return cand, true
}

// podStartTime is the pod's start time, or zero when it has not started.
func podStartTime(pod *v1.Pod) time.Time {
	if pod.Status.StartTime != nil {
		return pod.Status.StartTime.Time
	}
	return time.Time{}
}

// podGroupCooled reports whether the PodGroup's cooldown clock permits a
// move. A missing clock is cooled; an unparseable or future one is not.
func podGroupCooled(pg *api.PodGroup, conf *capacityUpgradeConf, now time.Time) bool {
	raw, ok := pg.Annotations[CapacityUpgradeLastAnnotation]
	if !ok {
		return true
	}
	last, err := time.Parse(time.RFC3339, raw)
	if err != nil || last.After(now) {
		return false
	}
	return now.Sub(last) >= time.Duration(conf.CooldownSeconds)*time.Second
}

// upgradeLedger tracks, per target node, the capacity still claimable in
// this pass: idle resources plus the resources of lower-priority pods the
// preempt action would evict, net of what earlier plans already took.
type upgradeLedger struct {
	idle map[string]*api.Resource
	// preemptable holds each node's running tasks not yet consumed by a
	// plan, so a later plan cannot count on evicting the same pod twice.
	preemptable map[string][]*api.TaskInfo
	// taken marks the pods (movers and preemptees) already spoken for.
	taken map[types.UID]bool
}

func newUpgradeLedger(nodes map[string]*api.NodeInfo) *upgradeLedger {
	ledger := &upgradeLedger{
		idle:        make(map[string]*api.Resource, len(nodes)),
		preemptable: make(map[string][]*api.TaskInfo, len(nodes)),
		taken:       make(map[types.UID]bool),
	}
	for _, node := range nodes {
		ledger.idle[node.Name] = node.Idle.Clone()
		tasks := make([]*api.TaskInfo, 0, len(node.Tasks))
		for _, task := range node.Tasks {
			if task.Pod == nil || task.Status != api.Running {
				continue
			}
			tasks = append(tasks, task)
		}
		// Cheapest to preempt first: lowest priority, then smallest.
		sort.Slice(tasks, func(i, j int) bool {
			if tasks[i].Priority != tasks[j].Priority {
				return tasks[i].Priority < tasks[j].Priority
			}
			return tasks[i].Name < tasks[j].Name
		})
		ledger.preemptable[node.Name] = tasks
	}
	return ledger
}

// commit records a plan: the movers are spoken for (a mover already on a
// target node counts as freeable there, since it restarts too) and the
// target nodes lose the capacity the placement consumed.
func (l *upgradeLedger) commit(members []*api.TaskInfo, placement *capacityUpgradePlacement) {
	for _, member := range members {
		l.taken[member.Pod.UID] = true
	}
	for node, idle := range placement.idle {
		l.idle[node] = idle
	}
	for _, uid := range placement.preemptees {
		l.taken[uid] = true
	}
}

// capacityUpgradePlacement is a proven placement of one group.
type capacityUpgradePlacement struct {
	nodes      []string
	zone       string
	preempting int
	preemptees []types.UID
	// idle is the ledger's idle map for the touched nodes after placement.
	idle map[string]*api.Resource
}

// simulateUpgrade proves the whole group fits on the target nodes today,
// placing members largest-first onto the node with the most claimable
// capacity that passes predicates. Claimable capacity on a node is its
// ledger idle plus, in priority order, lower-priority running pods the
// preempt action would evict for a member of this group's priority
// (members of the group itself count as freeable, since they restart). For
// gangs, the target set is one zone at a time; the first zone that fits
// wins. Returns nil unless every member places.
func simulateUpgrade(
	targets []*api.NodeInfo,
	cand capacityUpgradeCandidate,
	conf *capacityUpgradeConf,
	gpu v1.ResourceName,
	ledger *upgradeLedger,
	predicate capacityUpgradePredicate,
) *capacityUpgradePlacement {
	zones := map[string][]*api.NodeInfo{}
	zoneNames := make([]string, 0)
	if cand.gang && conf.ZoneLabel != "" {
		for _, node := range targets {
			zone := node.Node.Labels[conf.ZoneLabel]
			if _, seen := zones[zone]; !seen {
				zoneNames = append(zoneNames, zone)
			}
			zones[zone] = append(zones[zone], node)
		}
		sort.Strings(zoneNames)
	} else {
		zones[""] = targets
		zoneNames = append(zoneNames, "")
	}

	ordered := make([]*api.TaskInfo, len(cand.members))
	copy(ordered, cand.members)
	sort.SliceStable(ordered, func(i, j int) bool {
		return taskGpuNeed(ordered[i], gpu).Get(gpu) > taskGpuNeed(ordered[j], gpu).Get(gpu)
	})
	movers := make(map[types.UID]bool, len(cand.members))
	for _, member := range cand.members {
		movers[member.Pod.UID] = true
	}

	for _, zone := range zoneNames {
		placement := simulateUpgradeInZone(zones[zone], ordered, movers, cand, gpu, ledger, predicate)
		if placement != nil {
			placement.zone = zone
			return placement
		}
	}
	return nil
}

// upgradeTarget is a target node's claimable capacity during one simulation.
type upgradeTarget struct {
	node *api.NodeInfo
	idle *api.Resource
	// queue holds this node's not-yet-claimed preemptable tasks, cheapest
	// first.
	queue      []*api.TaskInfo
	preemptees []types.UID
}

// claimable grows idle by evicting queued tasks until need fits or the queue
// is exhausted; only tasks strictly below the mover's priority (or movers
// themselves) are evictable. Returns whether need now fits.
func (t *upgradeTarget) claimable(need *api.Resource, priority int32, movers map[types.UID]bool) bool {
	for len(t.queue) > 0 {
		if need.LessEqual(t.idle, api.Zero) {
			return true
		}
		// The queue is priority-ascending, so once a non-mover at or above
		// the mover's priority is reached only movers further back remain
		// evictable.
		evictable := -1
		for i, task := range t.queue {
			if movers[task.Pod.UID] || task.Priority < priority {
				evictable = i
				break
			}
		}
		if evictable < 0 {
			return false
		}
		task := t.queue[evictable]
		t.queue = append(t.queue[:evictable:evictable], t.queue[evictable+1:]...)
		t.idle.Add(task.Resreq)
		if !movers[task.Pod.UID] {
			t.preemptees = append(t.preemptees, task.Pod.UID)
		}
	}
	return need.LessEqual(t.idle, api.Zero)
}

func simulateUpgradeInZone(
	nodes []*api.NodeInfo,
	ordered []*api.TaskInfo,
	movers map[types.UID]bool,
	cand capacityUpgradeCandidate,
	gpu v1.ResourceName,
	ledger *upgradeLedger,
	predicate capacityUpgradePredicate,
) *capacityUpgradePlacement {
	if len(nodes) == 0 {
		return nil
	}
	targets := make([]*upgradeTarget, 0, len(nodes))
	for _, node := range nodes {
		queue := make([]*api.TaskInfo, 0)
		for _, task := range ledger.preemptable[node.Name] {
			if ledger.taken[task.Pod.UID] {
				continue
			}
			queue = append(queue, task)
		}
		targets = append(targets, &upgradeTarget{
			node:  node,
			idle:  ledger.idle[node.Name].Clone(),
			queue: queue,
		})
	}

	used := make(map[string]bool)
	for _, member := range ordered {
		need := taskGpuNeed(member, gpu)
		// Prefer nodes with the most idle GPUs so a gang spreads over as
		// few preemptions as possible; ties go to name for determinism.
		sort.SliceStable(targets, func(i, j int) bool {
			ii, ij := targets[i].idle.Get(gpu), targets[j].idle.Get(gpu)
			if ii != ij {
				return ii > ij
			}
			return targets[i].node.Name < targets[j].node.Name
		})
		placed := false
		for _, target := range targets {
			if !target.claimable(need, cand.priority, movers) {
				continue
			}
			if predicate != nil {
				if err := predicate(member, target.node, cand.gang); err != nil {
					continue
				}
			}
			target.idle.Sub(need)
			used[target.node.Name] = true
			placed = true
			break
		}
		if !placed {
			return nil
		}
	}

	placement := &capacityUpgradePlacement{idle: make(map[string]*api.Resource)}
	for _, target := range targets {
		if !used[target.node.Name] {
			continue
		}
		placement.nodes = append(placement.nodes, target.node.Name)
		placement.idle[target.node.Name] = target.idle
		placement.preemptees = append(placement.preemptees, target.preemptees...)
		placement.preempting += len(target.preemptees)
	}
	sort.Strings(placement.nodes)
	return placement
}

// stampCapacityUpgradeProposal durably records the proposal and the cooldown
// clock on the gang's PodGroup in one patch.
func stampCapacityUpgradeProposal(plan capacityUpgradePlan) error {
	pg := plan.job.PodGroup
	body, err := json.Marshal(plan.proposal)
	if err != nil {
		return fmt.Errorf("encode proposal: %w", err)
	}
	patch, err := json.Marshal(map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": map[string]string{
				CapacityUpgradeProposalAnnotation: string(body),
				CapacityUpgradeLastAnnotation:     plan.proposal.At,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("encode patch: %w", err)
	}
	if _, err := Session.VCClient().SchedulingV1beta1().PodGroups(pg.Namespace).Patch(
		context.TODO(), pg.Name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("stamp podgroup %s/%s: %w", pg.Namespace, pg.Name, err)
	}
	return nil
}

// stampCapacityUpgradeVictim records the cooldown clock on a single-member
// PodGroup before its pod is evicted, and spends the shared repack eviction
// cap so gpuFragmentation does not move the replacement again in the same
// window. Failure aborts the eviction so the clock cannot be skipped.
func stampCapacityUpgradeVictim(plan capacityUpgradePlan) error {
	pg := plan.job.PodGroup
	patch := []byte(fmt.Sprintf(`{"metadata":{"annotations":{%q:%q,%q:"1"}}}`,
		CapacityUpgradeLastAnnotation, plan.proposal.At, groupEvictionAnnotation))
	if _, err := Session.VCClient().SchedulingV1beta1().PodGroups(pg.Namespace).Patch(
		context.TODO(), pg.Name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("stamp podgroup %s/%s: %w", pg.Namespace, pg.Name, err)
	}
	return nil
}
