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

// Prices and spend caps in the capacityUpgrade strategy.
//
// The node autoscaler publishes, per node, its current price, the spend-cap
// policies that admit it at that price and whether it is over its spend cap
// (see the capacitycost package). Three opt-in parameters make the strategy
// use them; with all three off it plans exactly as it does without this
// file.
//
// priceAware. A group whose GPU members all run on priced nodes is judged on
// price: its targets are the priced nodes that cost no more per GPU than its
// dearest source (its own nodes among them, so members may stay or swap),
// offered cheapest first (the nodes at or below each price level in turn;
// the first set the whole group fits on decides), and it moves only if the
// placement cuts its hourly cost by at least minSavingPercent. That floor is
// the hysteresis: a group moved for a saving
// cannot be moved back while prices stand, because the way back would have
// to save as much again. Where a source or a target has no fresh price the
// rank decides, as before: a group with an unpriced source is planned by
// rank alone, and a fully priced group that finds no price-based move is
// still offered the unpriced nodes of the cheaper ranks.
//
// moveAdmission. A member governed by a spend cap is never placed on a node
// that cap does not admit, and a node without a fresh verdict admits no
// capped member. The check runs when the move is planned and again before
// the movers are evicted. It restricts moves only: the successor is a
// pending pod like any other and binds wherever it fits best, which the
// hold makes the target.
//
// overrunRelief. A group with a capped member on a node flagged as over its
// spend cap, inside the window before the autoscaler acts on the node
// itself, goes first in a pass and may move to any idle capacity that
// admits it: priced nodes that are not themselves over their cap, cheapest
// first, with no saving asked. Relief only ever takes idle GPUs. If the flag
// clears or the window closes before the movers are evicted, the move is
// abandoned: past the window the node is the autoscaler's to deal with.

import (
	"fmt"
	"sort"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/plugins/capacitycost"
)

// overrunReliefReason marks, on a hold, a move off a node over its spend
// cap.
const overrunReliefReason = "overrun_relief"

// savingTolerance absorbs floating-point error at the minimum-saving
// boundary, so a saving of exactly the configured percentage qualifies.
const savingTolerance = 1e-9

// admitsMoves reports whether movers are subject to move admission.
func (c *capacityUpgradeConf) admitsMoves() bool {
	return c.MoveAdmission || c.OverrunRelief
}

// priceBook reads published prices and verdicts as of now.
func (c *capacityUpgradeConf) priceBook(now time.Time) *capacitycost.PriceBook {
	return capacitycost.NewPriceBook(now, time.Duration(c.PriceStalenessSeconds)*time.Second)
}

// minSaving is the minimum saving as a fraction of the current cost.
func (c *capacityUpgradeConf) minSaving() float64 {
	return min(max(c.MinSavingPercent, 0), 100) / 100
}

// reason is what the plan's holds record about why the group moves.
func (p capacityUpgradePlan) reason() string {
	if p.relief {
		return overrunReliefReason
	}
	return ""
}

// upgradePricing is one pass's view of the published prices: the price per
// GPU of every GPU node that has a fresh one.
type upgradePricing struct {
	conf  *capacityUpgradeConf
	book  *capacitycost.PriceBook
	gpu   v1.ResourceName
	nodes map[string]*api.NodeInfo
	// units maps each priced GPU node to its hourly price per GPU.
	units map[string]float64
	// priced lists those nodes, cheapest per GPU first, then by name.
	priced []*api.NodeInfo
}

// newUpgradePricing reads the prices the pass plans against. Nothing is
// read unless a parameter that needs prices is on.
func newUpgradePricing(nodes map[string]*api.NodeInfo, conf *capacityUpgradeConf, now time.Time, gpu v1.ResourceName) *upgradePricing {
	pricing := &upgradePricing{conf: conf, book: conf.priceBook(now), gpu: gpu, nodes: nodes, units: map[string]float64{}}
	if !conf.PriceAware && !conf.OverrunRelief {
		return pricing
	}
	for _, node := range nodes {
		if node.Node == nil {
			continue
		}
		if unit, ok := pricing.book.UnitPrice(node, gpu); ok {
			pricing.units[node.Name] = unit
			pricing.priced = append(pricing.priced, node)
		}
	}
	sort.Slice(pricing.priced, func(i, j int) bool {
		ui, uj := pricing.units[pricing.priced[i].Name], pricing.units[pricing.priced[j].Name]
		if ui != uj {
			return ui < uj
		}
		return pricing.priced[i].Name < pricing.priced[j].Name
	})
	return pricing
}

// needsRelief reports whether the member is governed by a spend cap and
// runs on a node over its cap that the autoscaler has not acted on yet.
func (p *upgradePricing) needsRelief(member *api.TaskInfo) bool {
	return reliefDue(member, p.nodes, p.book)
}

// reliefDue reports whether the task is governed by a spend cap and runs on
// a node flagged as over its cap whose relief window is still open.
func reliefDue(task *api.TaskInfo, nodes map[string]*api.NodeInfo, book *capacitycost.PriceBook) bool {
	if task.Pod == nil || capacitycost.SpendCapPolicy(task.Pod) == "" {
		return false
	}
	node := nodes[task.NodeName]
	return node != nil && book.ReliefWindowOpen(node.Node)
}

// anyReliefDue reports whether at least one of the tasks still awaits
// relief.
func anyReliefDue(tasks []*api.TaskInfo, nodes map[string]*api.NodeInfo, book *capacitycost.PriceBook) bool {
	for _, task := range tasks {
		if reliefDue(task, nodes, book) {
			return true
		}
	}
	return false
}

// single picks the member a PodGroup of independent pods moves when relief
// or prices single one out: the oldest member in need of relief, else (when
// every member's node is priced) the oldest on the dearest node. ok is
// false when neither applies and the rank decides.
func (p *upgradePricing) single(members []*api.TaskInfo) (pick *api.TaskInfo, ok bool) {
	older := func(member *api.TaskInfo) bool {
		return pick == nil || podStartTime(member.Pod).Before(podStartTime(pick.Pod))
	}
	if p.conf.OverrunRelief {
		for _, member := range members {
			if p.needsRelief(member) && older(member) {
				pick = member
			}
		}
		if pick != nil {
			return pick, true
		}
	}
	if !p.conf.PriceAware {
		return nil, false
	}
	dearest := 0.0
	for _, member := range members {
		unit, priced := p.units[member.NodeName]
		if !priced {
			return nil, false
		}
		dearest = max(dearest, unit)
	}
	for _, member := range members {
		if p.units[member.NodeName] == dearest && older(member) {
			pick = member
		}
	}
	return pick, pick != nil
}

// measure prices the candidate's members where they run and notes whether
// any of them needs relief.
func (p *upgradePricing) measure(cand *capacityUpgradeCandidate) {
	if p.conf.PriceAware {
		cand.priced = true
		for _, member := range cand.members {
			unit, priced := p.units[member.NodeName]
			if !priced {
				cand.priced, cand.cost, cand.maxUnit = false, 0, 0
				break
			}
			cand.cost += taskGpu(member, p.gpu) / gpuMilli * unit
			cand.maxUnit = max(cand.maxUnit, unit)
		}
	}
	if p.conf.OverrunRelief {
		for _, member := range cand.members {
			if p.needsRelief(member) {
				cand.relief = true
				break
			}
		}
	}
}

// unpriced returns the nodes among targets that have no fresh price.
func (p *upgradePricing) unpriced(targets []*api.NodeInfo) []*api.NodeInfo {
	kept := make([]*api.NodeInfo, 0, len(targets))
	for _, node := range targets {
		if _, priced := p.units[node.Name]; !priced {
			kept = append(kept, node)
		}
	}
	return kept
}

// cost is the hourly cost of a placement onto priced nodes.
func (p *upgradePricing) cost(placement *capacityUpgradePlacement) float64 {
	total := 0.0
	for node, gpus := range placement.placed {
		total += gpus * p.units[node]
	}
	return total
}

// cheaper places a priced candidate onto priced nodes that cost no more per
// GPU than its dearest source, provided the placement saves at least the
// configured share of the group's current hourly cost: no member ever moves
// onto a node dearer than the group already pays for, and the group's total
// decides whether the restart is worth it. It returns nil when no such
// placement exists.
func (p *upgradePricing) cheaper(cand capacityUpgradeCandidate, ledger *upgradeLedger, predicate capacityUpgradePredicate, guard *demandGuard) *capacityUpgradePlacement {
	targets := make([]*api.NodeInfo, 0, len(p.priced))
	for _, node := range p.priced {
		if p.units[node.Name] <= cand.maxUnit {
			targets = append(targets, node)
		}
	}
	placement := p.cheapestFit(targets, cand, ledger, predicate, guard)
	if placement == nil {
		return nil
	}
	saved := (cand.cost - p.cost(placement)) / cand.cost
	if saved <= 0 || saved < p.conf.minSaving()-savingTolerance {
		klog.V(4).Infof("capacityUpgrade: %s/%s not moved: saving %.1f%% is below the %.1f%% minimum",
			cand.job.Namespace, cand.job.Name, saved*100, p.conf.minSaving()*100)
		return nil
	}
	return placement
}

// relieve places a candidate in need of relief onto idle capacity of priced
// nodes that are not themselves over their spend cap, cheapest first.
// Admission of each member is the predicate's to check. Nothing is
// displaced to make room.
func (p *upgradePricing) relieve(cand capacityUpgradeCandidate, ledger *upgradeLedger, predicate capacityUpgradePredicate, guard *demandGuard) *capacityUpgradePlacement {
	targets := make([]*api.NodeInfo, 0, len(p.priced))
	for _, node := range p.priced {
		if !capacitycost.OverSpendCap(node.Node) {
			targets = append(targets, node)
		}
	}
	cand.idleOnly = true
	return p.cheapestFit(targets, cand, ledger, predicate, guard)
}

// cheapestFit offers the candidate the targets (sorted cheapest per GPU
// first) one price level at a time: the nodes at or below the lowest price,
// then at or below the next, and so on. The first set the whole group fits
// on decides the placement, so the group reaches no further up the price
// list than it has to. A set whose claimable GPUs (idle, the group's own,
// and the displaceable ones when the candidate may displace) do not add up
// to the group's is passed over without a simulation.
func (p *upgradePricing) cheapestFit(targets []*api.NodeInfo, cand capacityUpgradeCandidate, ledger *upgradeLedger, predicate capacityUpgradePredicate, guard *demandGuard) *capacityUpgradePlacement {
	own := map[string]float64{}
	for _, member := range cand.members {
		own[member.NodeName] += taskGpu(member, p.gpu)
	}
	claimable := 0.0
	for start, end := 0, 0; end < len(targets); start = end {
		level := p.units[targets[end].Name]
		for end < len(targets) && p.units[targets[end].Name] == level {
			end++
		}
		for _, node := range targets[start:end] {
			claimable += ledger.idle[node.Name].Get(p.gpu) + own[node.Name]
			if cand.idleOnly {
				continue
			}
			for _, pod := range ledger.displaceable[node.Name] {
				if !ledger.taken[pod.Pod.UID] {
					claimable += taskGpu(pod, p.gpu)
				}
			}
		}
		if claimable < cand.gpus {
			continue
		}
		if placement := simulateUpgrade(targets[:end], cand, p.conf, p.gpu, ledger, predicate, guard); placement != nil {
			return placement
		}
	}
	return nil
}

// observeOverrun publishes how many nodes are over their spend cap, split
// by whether the relief window is still open.
func (p *upgradePricing) observeOverrun(nodes map[string]*api.NodeInfo) {
	if !p.conf.OverrunRelief {
		return
	}
	open, closed := 0, 0
	for _, node := range nodes {
		switch {
		case !capacitycost.OverSpendCap(node.Node):
		case p.book.ReliefWindowOpen(node.Node):
			open++
		default:
			closed++
		}
	}
	capacityUpgradeOverrunNodes.WithLabelValues("open").Set(float64(open))
	capacityUpgradeOverrunNodes.WithLabelValues("closed").Set(float64(closed))
}

// admittedForMove puts move admission in front of a fit predicate: a task
// governed by a spend cap fails on every node that cap does not admit. With
// count set, each refused task and node is counted once.
func admittedForMove(inner capacityUpgradePredicate, book *capacitycost.PriceBook, count bool) capacityUpgradePredicate {
	refused := map[string]bool{}
	return func(task *api.TaskInfo, node *api.NodeInfo, gang bool) error {
		if task.Pod != nil && !book.AdmittedForMove(task.Pod, node.Node) {
			if key := string(task.Pod.UID) + "/" + node.Name; count && !refused[key] {
				refused[key] = true
				capacityUpgradeTargetsSkipped.WithLabelValues("move_admission").Inc()
				klog.V(4).Infof("capacityUpgrade: %s/%s not placed on %s: its spend cap %q does not admit the node",
					task.Namespace, task.Name, node.Name, capacitycost.SpendCapPolicy(task.Pod))
			}
			return fmt.Errorf("spend cap %q does not admit node %s", capacitycost.SpendCapPolicy(task.Pod), node.Name)
		}
		if inner == nil {
			return nil
		}
		return inner(task, node, gang)
	}
}
