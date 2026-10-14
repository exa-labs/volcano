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

// This file describes eviction decisions for the scheduler log: the victims
// of a shadow comparison, and the jobs behind a decision a cap caught. Each
// line is built only when V(2) is on.

package evictioncost

import (
	"fmt"
	"strings"

	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
)

// ShadowSide is what one ordering would evict: its victims, their cost with
// each gang charged once, and, where the choice is a node, that node.
type ShadowSide struct {
	Victims    []*api.TaskInfo
	GPUSeconds float64
	Node       string
}

// ShadowResult compares the baseline choice with the cost-aware one.
type ShadowResult struct {
	Baseline, CostAware ShadowSide
}

// Differs reports whether the two choices differ in cost; equal-cost choices
// are ties, not different decisions.
func (r ShadowResult) Differs() bool {
	return r.Baseline.GPUSeconds != r.CostAware.GPUSeconds
}

// Outcome is "different" or "same", as the shadow metrics label it.
func (r ShadowResult) Outcome() string {
	if r.Differs() {
		return "different"
	}
	return "same"
}

// Side prices the victims a choice evicts on node ("" when the choice is not
// a node).
func (b *Book) Side(victims []*api.TaskInfo, node string) ShadowSide {
	return ShadowSide{Victims: victims, GPUSeconds: b.Selection(victims), Node: node}
}

// JobName is the task's job as namespace/name, or the pod's own name when
// its job is not in the session.
func (b *Book) JobName(task *api.TaskInfo) string {
	if job, ok := b.jobs[task.Job]; ok && task.Job != "" {
		return job.Namespace + "/" + job.Name
	}
	return task.Namespace + "/" + task.Name
}

// FormatPrice renders a priced job as name(GPU-seconds, announced|default):
// announced when a member published restart facts, default when the cost
// rests on the configured defaults alone.
func FormatPrice(name string, price Price) string {
	source := "default"
	if price.Announced {
		source = "announced"
	}
	return fmt.Sprintf("%s(%.0f, %s)", name, price.GPUSeconds, source)
}

// Describe lists each distinct job among the victims once, first seen
// first, with its price.
func (b *Book) Describe(victims []*api.TaskInfo) string {
	seen := map[string]bool{}
	parts := make([]string, 0, len(victims))
	for _, victim := range victims {
		name := b.JobName(victim)
		if seen[name] {
			continue
		}
		seen[name] = true
		parts = append(parts, FormatPrice(name, b.Job(victim)))
	}
	return "[" + strings.Join(parts, " ") + "]"
}

// ShadowLine is the log line of one shadow comparison.
func (b *Book) ShadowLine(action, strategy string, preemptor *api.TaskInfo, r ShadowResult) string {
	var line strings.Builder
	fmt.Fprintf(&line, "evictionCost shadow: action=%s strategy=%s preemptor=%s outcome=%s baseline=%.0f costAware=%.0f",
		action, strategy, b.JobName(preemptor), r.Outcome(), r.Baseline.GPUSeconds, r.CostAware.GPUSeconds)
	if r.Baseline.Node != "" || r.CostAware.Node != "" {
		fmt.Fprintf(&line, " baselineNode=%s costAwareNode=%s", r.Baseline.Node, r.CostAware.Node)
	}
	fmt.Fprintf(&line, " baselineVictims=%s costAwareVictims=%s", b.Describe(r.Baseline.Victims), b.Describe(r.CostAware.Victims))
	return line.String()
}

// LogShadow logs one shadow comparison at V(2).
func (b *Book) LogShadow(action, strategy string, preemptor *api.TaskInfo, r ShadowResult) {
	logLine(func() string { return b.ShadowLine(action, strategy, preemptor, r) })
}

// logLine writes a decision line at V(2), building it only when V(2) is on;
// tests replace it to read the lines.
var logLine = func(line func() string) {
	if klog.V(2).Enabled() {
		klog.InfoDepth(2, line())
	}
}
