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

// Package prioritypack scores nodes so that tasks of similar priority share
// nodes, the way binpack makes tasks share nodes at all. It keeps a node's
// priority ceiling — the highest priority running on it — from rising over
// resources held by lower-priority work, because those resources stop being
// preemptable as a whole node by anything between the two priorities.
//
// Placing a task of priority p on a node whose ceiling is c costs
// max(0, p − c) × (resource the node's tasks already hold): how far the
// ceiling rises times how much it newly caps. An empty node, a node whose
// ceiling is already at or above p, and a task at the cluster's lowest
// priority all cost nothing, so the lowest tier remains free to fill any
// gap while each higher tier converges onto the nodes it already occupies.
// Costs are ranked per task across the candidate nodes: the cheapest
// candidates score weight × 100, the most expensive 0. Tasks not requesting
// the configured resource are not scored.
//
// The same node-order function ranks candidate nodes for the preempt action
// (which scores nodes after removing the candidate's victims), so a
// preemptor also lands on the node where it raises the ceiling least.
//
// Scheduler configuration:
//
//	tiers:
//	- plugins:
//	  - name: prioritypack
//	    arguments:
//	      prioritypack.weight: 10
//	      prioritypack.resource: nvidia.com/gpu
package prioritypack

import (
	"math"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

const (
	// PluginName indicates name of volcano scheduler plugin.
	PluginName = "prioritypack"

	// WeightArg scales the 0-100 node score, like binpack.weight.
	WeightArg = "prioritypack.weight"
	// ResourceArg is the resource whose holders define a node's ceiling and
	// whose amount measures what a rising ceiling caps. Only tasks
	// requesting it are scored.
	ResourceArg = "prioritypack.resource"

	// DefaultWeight matches binpack at its production weight, so among
	// nodes that cost the same, fullness still decides.
	DefaultWeight = 10
	// DefaultResource packs GPU work by priority.
	DefaultResource = "nvidia.com/gpu"

	// milli converts api.Resource scalar amounts to whole units.
	milli = 1000
)

var (
	cappedResource = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: "volcano",
			Name:      "prioritypack_capped_resource",
			Help:      "Resource (whole units) held by tasks running below their node's priority ceiling, summed over nodes, at session open.",
		}, []string{"resource"},
	)

	mixedNodes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: "volcano",
			Name:      "prioritypack_mixed_nodes",
			Help:      "Nodes whose holders of the resource run at more than one priority, at session open.",
		}, []string{"resource"},
	)
)

// holds reports whether a task occupies, or is committed to occupy, node
// resources: tasks being released do not count, pipelined preemptors do.
func holds(task *api.TaskInfo) bool {
	return api.AllocatedStatus(task.Status) || task.Status == api.Pipelined
}

// occupancy is a node's priority ceiling and how much of the resource its
// holders have. empty is true when nothing on the node holds the resource.
type occupancy struct {
	ceiling int32
	held    float64
	lowest  int32
	empty   bool
}

// occupancyOf summarizes the holders of resource on node.
func occupancyOf(node *api.NodeInfo, resource v1.ResourceName) occupancy {
	o := occupancy{empty: true}
	for _, task := range node.Tasks {
		if !holds(task) {
			continue
		}
		amount := task.Resreq.Get(resource)
		if amount <= 0 {
			continue
		}
		if o.empty || task.Priority > o.ceiling {
			o.ceiling = task.Priority
		}
		if o.empty || task.Priority < o.lowest {
			o.lowest = task.Priority
		}
		o.held += amount
		o.empty = false
	}
	return o
}

// cost is the ceiling rise a task of priority p causes on the node times
// the resource that rise caps.
func (o occupancy) cost(p int32) float64 {
	if o.empty || p <= o.ceiling {
		return 0
	}
	return float64(int64(p)-int64(o.ceiling)) * o.held
}

// capped is the resource on the node held below its ceiling.
func capped(node *api.NodeInfo, resource v1.ResourceName, ceiling int32) float64 {
	var total float64
	for _, task := range node.Tasks {
		if holds(task) && task.Priority < ceiling {
			total += task.Resreq.Get(resource)
		}
	}
	return total
}

// rank turns per-node costs into 0-100 scores, cheapest highest. Equal costs
// score equal; a batch with a single cost scores 0 everywhere.
func rank(costs map[string]float64) map[string]float64 {
	maxCost := 0.0
	for _, cost := range costs {
		maxCost = math.Max(maxCost, cost)
	}
	scores := make(map[string]float64, len(costs))
	for node, cost := range costs {
		if maxCost <= 0 {
			scores[node] = 0
			continue
		}
		scores[node] = 100 * (1 - cost/maxCost)
	}
	return scores
}

type priorityPackPlugin struct {
	weight   int
	resource v1.ResourceName
}

// New returns a prioritypack plugin instance.
func New(arguments framework.Arguments) framework.Plugin {
	weight := DefaultWeight
	resource := DefaultResource
	arguments.GetInt(&weight, WeightArg)
	arguments.GetString(&resource, ResourceArg)
	return &priorityPackPlugin{
		weight:   weight,
		resource: v1.ResourceName(resource),
	}
}

func (p *priorityPackPlugin) Name() string {
	return PluginName
}

// scores reports whether the task participates in priority packing.
func (p *priorityPackPlugin) scores(task *api.TaskInfo) bool {
	return task.Resreq.Get(p.resource) > 0 || task.InitResreq.Get(p.resource) > 0
}

// batchScores ranks the candidate nodes for the task by placement cost.
func (p *priorityPackPlugin) batchScores(task *api.TaskInfo, nodes []*api.NodeInfo) map[string]float64 {
	costs := make(map[string]float64, len(nodes))
	for _, node := range nodes {
		costs[node.Name] = occupancyOf(node, p.resource).cost(task.Priority)
	}
	scores := rank(costs)
	for node, score := range scores {
		scores[node] = float64(p.weight) * score
	}
	return scores
}

// observe publishes how much of the resource the cluster currently holds
// below a node ceiling, and on how many nodes priorities mix.
func (p *priorityPackPlugin) observe(nodes map[string]*api.NodeInfo) {
	var total float64
	var mixed int
	for _, node := range nodes {
		o := occupancyOf(node, p.resource)
		if o.empty {
			continue
		}
		if o.lowest < o.ceiling {
			mixed++
		}
		total += capped(node, p.resource, o.ceiling)
	}
	cappedResource.WithLabelValues(string(p.resource)).Set(total / milli)
	mixedNodes.WithLabelValues(string(p.resource)).Set(float64(mixed))
}

func (p *priorityPackPlugin) OnSessionOpen(ssn *framework.Session) {
	klog.V(5).Infof("Enter prioritypack plugin ...")
	defer klog.V(5).Infof("Leaving prioritypack plugin.")
	p.observe(ssn.Nodes)
	if p.weight <= 0 || p.resource == "" {
		klog.V(4).Infof("prioritypack: inert (weight %d, resource %q)", p.weight, p.resource)
		return
	}
	ssn.AddBatchNodeOrderFn(p.Name(), func(task *api.TaskInfo, nodes []*api.NodeInfo) (map[string]float64, error) {
		if !p.scores(task) {
			return nil, nil
		}
		scores := p.batchScores(task, nodes)
		klog.V(5).Infof("prioritypack: task %s/%s (priority %d) scores %v",
			task.Namespace, task.Name, task.Priority, scores)
		return scores, nil
	})
}

func (p *priorityPackPlugin) OnSessionClose(ssn *framework.Session) {}
