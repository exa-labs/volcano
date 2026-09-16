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

// Package capacitycost scores nodes by the marginal cost of the capacity they
// run on, so placement (and the fork's score-ordered preemption) prefers the
// cheapest capacity a task fits on.
//
// The cost model is a static rank over a node label: the node's label value
// is looked up in an ordered list, cheapest first, and the node scores
// weight × 100 × (maxRank − rank) / maxRank. Nodes whose label is missing or
// not in the list get a configurable rank (default 0: unlabeled pools are
// owned hardware, i.e. sunk cost). Only tasks requesting the configured
// resource are scored, so CPU-only work is unaffected.
//
// Scheduler configuration:
//
//	tiers:
//	- plugins:
//	  - name: capacitycost
//	    arguments:
//	      capacitycost.weight: 20
//	      capacitycost.nodeLabelKey: karpenter.sh/capacity-type
//	      capacitycost.order: "reserved,spot,on-demand"
//	      capacitycost.unlabeledRank: 0
//	      capacitycost.resource: nvidia.com/gpu
package capacitycost

import (
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

const (
	// PluginName indicates name of volcano scheduler plugin.
	PluginName = "capacitycost"

	// WeightArg scales the 0-100 node score, like binpack.weight.
	WeightArg = "capacitycost.weight"
	// NodeLabelKeyArg is the node label holding the capacity type.
	NodeLabelKeyArg = "capacitycost.nodeLabelKey"
	// OrderArg is the comma-separated capacity types, cheapest first.
	OrderArg = "capacitycost.order"
	// UnlabeledRankArg is the rank for nodes missing the label or carrying a
	// value not in the order.
	UnlabeledRankArg = "capacitycost.unlabeledRank"
	// ResourceArg restricts scoring to tasks requesting this resource; empty
	// scores every task.
	ResourceArg = "capacitycost.resource"

	// DefaultWeight outscores binpack at its production weight (10 × 100),
	// so a cheaper node always beats a fuller one when both fit.
	DefaultWeight = 20
	// DefaultNodeLabelKey matches Karpenter-provisioned nodes.
	DefaultNodeLabelKey = "karpenter.sh/capacity-type"
	// DefaultOrder is Karpenter's capacity types, cheapest first: reserved
	// is prepaid, spot is discounted, on-demand is list price.
	DefaultOrder = "reserved,spot,on-demand"
	// DefaultUnlabeledRank treats unlabeled nodes as sunk cost.
	DefaultUnlabeledRank = 0
	// DefaultResource limits scoring to GPU tasks.
	DefaultResource = "nvidia.com/gpu"
)

// Ranker maps a node to its capacity rank (0 = cheapest) and exposes the
// maximum rank in the configured order. It is shared with the rescheduling
// plugin's capacityUpgrade strategy so placement and migration agree on
// which capacity is cheaper.
type Ranker struct {
	labelKey      string
	ranks         map[string]int
	maxRank       int
	unlabeledRank int
}

// NewRanker parses a comma-separated order (cheapest first) into a Ranker.
// An empty order yields a Ranker under which every node ranks equal.
func NewRanker(labelKey, order string, unlabeledRank int) *Ranker {
	r := &Ranker{labelKey: labelKey, ranks: map[string]int{}, unlabeledRank: unlabeledRank}
	rank := 0
	for _, raw := range strings.Split(order, ",") {
		value := strings.TrimSpace(raw)
		if value == "" {
			continue
		}
		if _, dup := r.ranks[value]; dup {
			continue
		}
		r.ranks[value] = rank
		rank++
	}
	r.maxRank = rank - 1
	if r.maxRank < unlabeledRank {
		r.maxRank = unlabeledRank
	}
	if r.maxRank < 0 {
		r.maxRank = 0
	}
	return r
}

// Rank returns the node's capacity rank; lower is cheaper.
func (r *Ranker) Rank(node *api.NodeInfo) int {
	if node == nil || node.Node == nil {
		return r.unlabeledRank
	}
	value, ok := node.Node.Labels[r.labelKey]
	if !ok {
		return r.unlabeledRank
	}
	rank, known := r.ranks[value]
	if !known {
		return r.unlabeledRank
	}
	return rank
}

// MaxRank is the most expensive rank in the order.
func (r *Ranker) MaxRank() int {
	return r.maxRank
}

// Score returns the 0-100 cheapness score of the node: 100 for the cheapest
// rank, 0 for the most expensive, 0 for every node when the order is flat.
func (r *Ranker) Score(node *api.NodeInfo) float64 {
	if r.maxRank <= 0 {
		return 0
	}
	return 100 * float64(r.maxRank-r.Rank(node)) / float64(r.maxRank)
}

type capacityCostPlugin struct {
	pluginArguments framework.Arguments
	weight          int
	resource        v1.ResourceName
	ranker          *Ranker
}

// New returns a capacitycost plugin instance.
func New(arguments framework.Arguments) framework.Plugin {
	weight := DefaultWeight
	labelKey := DefaultNodeLabelKey
	order := DefaultOrder
	unlabeledRank := DefaultUnlabeledRank
	resource := DefaultResource
	arguments.GetInt(&weight, WeightArg)
	arguments.GetString(&labelKey, NodeLabelKeyArg)
	arguments.GetString(&order, OrderArg)
	arguments.GetInt(&unlabeledRank, UnlabeledRankArg)
	arguments.GetString(&resource, ResourceArg)
	return &capacityCostPlugin{
		pluginArguments: arguments,
		weight:          weight,
		resource:        v1.ResourceName(resource),
		ranker:          NewRanker(labelKey, order, unlabeledRank),
	}
}

func (p *capacityCostPlugin) Name() string {
	return PluginName
}

// scores reports whether the task participates in cost scoring.
func (p *capacityCostPlugin) scores(task *api.TaskInfo) bool {
	if p.resource == "" {
		return true
	}
	return task.Resreq.Get(p.resource) > 0 || task.InitResreq.Get(p.resource) > 0
}

func (p *capacityCostPlugin) OnSessionOpen(ssn *framework.Session) {
	klog.V(5).Infof("Enter capacitycost plugin ...")
	defer klog.V(5).Infof("Leaving capacitycost plugin.")
	if p.weight <= 0 || p.ranker.MaxRank() <= 0 {
		klog.V(4).Infof("capacitycost: inert (weight %d, maxRank %d)", p.weight, p.ranker.MaxRank())
		return
	}
	ssn.AddNodeOrderFn(p.Name(), func(task *api.TaskInfo, node *api.NodeInfo) (float64, error) {
		if !p.scores(task) {
			return 0, nil
		}
		score := float64(p.weight) * p.ranker.Score(node)
		klog.V(5).Infof("capacitycost: task %s/%s on node %s (rank %d) scores %.1f",
			task.Namespace, task.Name, node.Name, p.ranker.Rank(node), score)
		return score, nil
	})
}

func (p *capacityCostPlugin) OnSessionClose(ssn *framework.Session) {}
