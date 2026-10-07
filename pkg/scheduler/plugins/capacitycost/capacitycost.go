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
// With capacitycost.priceAware the static rank becomes the fallback and the
// node's published price (see price.go) the model. For each task the
// candidate nodes are scored together: a node with a fresh price is scored on
// its unit price, the hourly price divided by what the node offers of the
// accelerator the task requests (of CPU when it requests none), as the share
// of the dearest priced candidate's unit price it saves,
// weight × 100 × (maxUnit − unit) / maxUnit. A node whose price is absent,
// stale or malformed keeps its rank score. Both read the same way (100 costs
// nothing, 0 is the dearest candidate), which is how priced and unpriced
// candidates compare: priced nodes among themselves by price, unpriced ones
// by rank, and an unpriced node of the cheapest rank (owned hardware) ahead
// of anything that is paid for. When no candidate is priced the scores are
// exactly the rank scores. Price differences score in proportion, so a
// fuller node can still win over one that is only marginally cheaper.
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
//	      capacitycost.priceAware: true
//	      capacitycost.priceStalenessSeconds: 900
//	      capacitycost.acceleratorResources: "nvidia.com/gpu,amd.com/gpu"
package capacitycost

import (
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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
	// PriceAwareArg scores nodes by their published price, falling back to
	// the rank for nodes without a fresh one. Off by default.
	PriceAwareArg = "capacitycost.priceAware"
	// PriceStalenessSecondsArg is how long a published price stays usable.
	PriceStalenessSecondsArg = "capacitycost.priceStalenessSeconds"
	// AcceleratorResourcesArg is the comma-separated resources a node's
	// price is divided by: the first one a task requests is its unit; a
	// task requesting none is priced per CPU.
	AcceleratorResourcesArg = "capacitycost.acceleratorResources"

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
	// DefaultAcceleratorResources are the device resources priced per unit.
	DefaultAcceleratorResources = "nvidia.com/gpu,amd.com/gpu,aws.amazon.com/neuron,google.com/tpu"
)

// pricedNodes counts the session's nodes by the state of their published
// price. It is only set while price-aware scoring is on.
var pricedNodes = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Subsystem: "volcano",
		Name:      "capacitycost_nodes",
		Help:      "Nodes by the state of their published price at session open: fresh, stale (confirmed too long ago) or absent (missing or malformed). Stale and absent nodes are scored by capacity rank.",
	}, []string{"price"},
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
	// priceAware, staleness and accelerators configure price scoring.
	priceAware   bool
	staleness    time.Duration
	accelerators []v1.ResourceName
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
	priceAware := false
	stalenessSeconds := int(DefaultPriceStaleness / time.Second)
	accelerators := DefaultAcceleratorResources
	arguments.GetBool(&priceAware, PriceAwareArg)
	arguments.GetInt(&stalenessSeconds, PriceStalenessSecondsArg)
	arguments.GetString(&accelerators, AcceleratorResourcesArg)
	return &capacityCostPlugin{
		pluginArguments: arguments,
		weight:          weight,
		resource:        v1.ResourceName(resource),
		ranker:          NewRanker(labelKey, order, unlabeledRank),
		priceAware:      priceAware,
		staleness:       time.Duration(stalenessSeconds) * time.Second,
		accelerators:    parseResources(accelerators),
	}
}

// parseResources splits a comma-separated resource list, dropping blanks.
func parseResources(raw string) []v1.ResourceName {
	resources := make([]v1.ResourceName, 0)
	for _, part := range strings.Split(raw, ",") {
		if name := strings.TrimSpace(part); name != "" {
			resources = append(resources, v1.ResourceName(name))
		}
	}
	return resources
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

// unitResource is the resource a node's price is divided by for the task:
// the first configured accelerator the task requests, else CPU.
func (p *capacityCostPlugin) unitResource(task *api.TaskInfo) v1.ResourceName {
	for _, accelerator := range p.accelerators {
		if task.Resreq.Get(accelerator) > 0 || task.InitResreq.Get(accelerator) > 0 {
			return accelerator
		}
	}
	return v1.ResourceCPU
}

// batchScores scores the task's candidate nodes together. Nodes with a
// fresh price are scored against the dearest priced candidate's unit price;
// the others keep their rank score.
func (p *capacityCostPlugin) batchScores(book *PriceBook, task *api.TaskInfo, nodes []*api.NodeInfo) map[string]float64 {
	resource := p.unitResource(task)
	units := make(map[string]float64, len(nodes))
	maxUnit := 0.0
	for _, node := range nodes {
		if unit, ok := book.UnitPrice(node, resource); ok {
			units[node.Name] = unit
			maxUnit = max(maxUnit, unit)
		}
	}
	scores := make(map[string]float64, len(nodes))
	for _, node := range nodes {
		score := p.ranker.Score(node)
		if unit, priced := units[node.Name]; priced {
			score = 100 * (maxUnit - unit) / maxUnit
		}
		scores[node.Name] = float64(p.weight) * score
	}
	return scores
}

// observePrices publishes how many nodes carry a usable price.
func observePrices(book *PriceBook, nodes map[string]*api.NodeInfo) {
	counts := map[PriceState]int{PriceFresh: 0, PriceStale: 0, PriceAbsent: 0}
	for _, node := range nodes {
		_, state := book.State(node.Node)
		counts[state]++
	}
	for state, count := range counts {
		pricedNodes.WithLabelValues(string(state)).Set(float64(count))
	}
}

// openPriceAware registers the price-aware scorer for the session.
func (p *capacityCostPlugin) openPriceAware(ssn *framework.Session) {
	book := NewPriceBook(time.Now(), p.staleness)
	observePrices(book, ssn.Nodes)
	if p.weight <= 0 {
		klog.V(4).Infof("capacitycost: inert (weight %d)", p.weight)
		return
	}
	ssn.AddBatchNodeOrderFn(p.Name(), func(task *api.TaskInfo, nodes []*api.NodeInfo) (map[string]float64, error) {
		if !p.scores(task) {
			return nil, nil
		}
		scores := p.batchScores(book, task, nodes)
		klog.V(5).Infof("capacitycost: task %s/%s scores %v", task.Namespace, task.Name, scores)
		return scores, nil
	})
}

func (p *capacityCostPlugin) OnSessionOpen(ssn *framework.Session) {
	klog.V(5).Infof("Enter capacitycost plugin ...")
	defer klog.V(5).Infof("Leaving capacitycost plugin.")
	if p.priceAware {
		p.openPriceAware(ssn)
		return
	}
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
