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

// Capacity-upgrade moves are transactions that span many scheduler sessions,
// so their state lives on the cluster, not in memory:
//
//   - A hold on a target node (CapacityUpgradeHoldsAnnotation) reserves a
//     GPU quantity for one move. The hold predicate rejects every other GPU
//     pod that would eat into that quantity, so lower-priority victims that
//     are evicted to make room cannot be replaced by anything but the mover's
//     successor.
//   - A drain on a source node (CapacityUpgradeDrainsAnnotation) marks a
//     node whose GPUs are being vacated by a move. The drain predicate lets a
//     GPU pod use only the node's real idle GPUs, never the ones still held by
//     terminating pods, so the successor cannot be pipelined back onto the
//     capacity its predecessor is releasing.
//
// A move advances through phases derived from that state every session:
// clearing (holds written, victims evicted, waiting for their GPUs to become
// idle) -> placing (drains written, movers evicted, waiting for the successor
// to bind onto the held GPUs) -> done (hold claimed by the successor and
// released). A hold that does not progress within its TTL expires and is
// released, and a hold whose fragments are inconsistent is abandoned; both
// leave the cluster no worse than before the move started.

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
)

const (
	// CapacityUpgradeHoldsAnnotation carries the JSON list of capacityHold
	// fragments on a target node.
	CapacityUpgradeHoldsAnnotation = "exa.ai/capacity-upgrade-holds"
	// CapacityUpgradeDrainsAnnotation carries the JSON list of capacityDrain
	// entries on a source node.
	CapacityUpgradeDrainsAnnotation = "exa.ai/capacity-upgrade-drains"
	// CapacityUpgradeCountAnnotation is the number of moves a PodGroup (and
	// the PodGroups it succeeded) has been through; it bounds restarts.
	CapacityUpgradeCountAnnotation = "exa.ai/capacity-upgrade-count"
	// capacityUpgradeProbeAnnotation marks the in-memory probe tasks the
	// planner evaluates against target nodes so the hold predicates do not
	// treat them as foreign pods.
	capacityUpgradeProbeAnnotation = "exa.ai/capacity-upgrade-probe"
	// ownerIdentityKey is the identity pseudo-label used for pods that lack
	// the configured identity labels: the controller owner's UID.
	ownerIdentityKey = "exa.ai/owner-uid"
	// gpuMilli converts between scheduler resource units and GPU counts.
	gpuMilli = 1000
)

// capacityHold is one node's share of a move's target reservation.
type capacityHold struct {
	// Move identifies the transaction: "<namespace>/<podgroup>@<started>".
	Move string `json:"move"`
	// Group is the mover PodGroup's job key ("<namespace>/<podgroup>").
	Group string `json:"group"`
	// Nodes lists every node carrying a fragment of this move; a move whose
	// fragments do not all exist is abandoned rather than executed.
	Nodes []string `json:"nodes"`
	// Movers are the UIDs of the pods the move evicts.
	Movers []string `json:"movers"`
	// Victims are the UIDs of the lower-priority pods evicted from the
	// target nodes to make room; one still running is evicted again.
	Victims []string `json:"victims,omitempty"`
	// Identity is the label set a pod must carry to claim this hold.
	Identity map[string]string `json:"identity"`
	// Gpus is the GPU count reserved on this node.
	Gpus float64 `json:"gpus"`
	// Priority is the mover's priority; the successor must not be outbid.
	Priority int32 `json:"priority"`
	// Moves is the mover PodGroup's move count including this move; it is
	// carried to the successor PodGroup on claim.
	Moves int `json:"moves"`
	// Target and From are the capacity types for logs and metrics.
	Target string `json:"target"`
	From   string `json:"from"`
	// Until is the RFC3339 deadline of the current phase.
	Until string `json:"until"`
	// EvictedAt is set once the movers have been evicted (placing phase).
	EvictedAt string `json:"evictedAt,omitempty"`
}

// capacityDrain marks a node whose GPUs a move is vacating.
type capacityDrain struct {
	Move  string `json:"move"`
	Until string `json:"until"`
}

// gang reports whether the move restarts more than one pod.
func (h capacityHold) gang() bool { return len(h.Movers) > 1 }

func (h capacityHold) kind() string {
	if h.gang() {
		return "gang"
	}
	return "pod"
}

// capacityUpgradeMove is a move reassembled from its node fragments.
type capacityUpgradeMove struct {
	id string
	// hold is the merged view: the latest deadline and any EvictedAt.
	hold capacityHold
	// fragments maps each node that carries the move to its fragment.
	fragments map[string]capacityHold
}

// evicted reports whether the movers have been evicted (placing phase).
func (m *capacityUpgradeMove) evicted() bool { return m.hold.EvictedAt != "" }

// complete reports whether every node the move lists carries a fragment.
func (m *capacityUpgradeMove) complete() bool {
	for _, node := range m.hold.Nodes {
		if _, ok := m.fragments[node]; !ok {
			return false
		}
	}
	return len(m.fragments) == len(m.hold.Nodes)
}

// capacityUpgradeIndex is the cluster's capacity-upgrade state as read from
// node annotations at session open.
type capacityUpgradeIndex struct {
	holds  map[string][]capacityHold
	drains map[string][]capacityDrain
	moves  map[string]*capacityUpgradeMove
	// groups marks mover PodGroups with a move in flight.
	groups map[string]bool
	// movers marks pods a move in flight evicts.
	movers map[types.UID]bool
	// victims marks pods a move in flight evicts from its target nodes;
	// their GPUs are already promised to that move's hold.
	victims map[types.UID]bool
	// malformed lists nodes whose annotations could not be decoded.
	malformed map[string]string
}

func newCapacityUpgradeIndex() *capacityUpgradeIndex {
	return &capacityUpgradeIndex{
		holds:     map[string][]capacityHold{},
		drains:    map[string][]capacityDrain{},
		moves:     map[string]*capacityUpgradeMove{},
		groups:    map[string]bool{},
		movers:    map[types.UID]bool{},
		victims:   map[types.UID]bool{},
		malformed: map[string]string{},
	}
}

// indexCapacityUpgrade decodes every node's hold and drain annotations. A
// node whose annotation does not decode is recorded as malformed and
// contributes nothing; maintenance clears it.
func indexCapacityUpgrade(nodes map[string]*api.NodeInfo) *capacityUpgradeIndex {
	idx := newCapacityUpgradeIndex()
	for _, node := range nodes {
		if node.Node == nil {
			continue
		}
		var holds []capacityHold
		var drains []capacityDrain
		if raw, ok := node.Node.Annotations[CapacityUpgradeHoldsAnnotation]; ok && raw != "" {
			decoded, err := decodeHolds(raw)
			if err != nil {
				idx.malformed[node.Name] = fmt.Sprintf("holds: %v", err)
				continue
			}
			holds = decoded
		}
		if raw, ok := node.Node.Annotations[CapacityUpgradeDrainsAnnotation]; ok && raw != "" {
			if err := json.Unmarshal([]byte(raw), &drains); err != nil {
				idx.malformed[node.Name] = fmt.Sprintf("drains: %v", err)
				continue
			}
		}
		for _, hold := range holds {
			idx.addHold(node.Name, hold)
		}
		if len(drains) > 0 {
			idx.drains[node.Name] = drains
		}
	}
	return idx
}

// decodeHolds parses a node's hold annotation; any fragment that cannot
// identify its move or its reservation makes the whole annotation
// malformed, since a partially trusted node could let a move proceed with
// only some of its reservation.
func decodeHolds(raw string) ([]capacityHold, error) {
	var holds []capacityHold
	if err := json.Unmarshal([]byte(raw), &holds); err != nil {
		return nil, err
	}
	for _, hold := range holds {
		if hold.Move == "" || hold.Group == "" || hold.Gpus <= 0 || len(hold.Nodes) == 0 || len(hold.Movers) == 0 {
			return nil, fmt.Errorf("fragment for move %q missing move, group, gpus, nodes or movers", hold.Move)
		}
	}
	return holds, nil
}

func (idx *capacityUpgradeIndex) addHold(node string, hold capacityHold) {
	idx.holds[node] = append(idx.holds[node], hold)
	move, ok := idx.moves[hold.Move]
	if !ok {
		move = &capacityUpgradeMove{id: hold.Move, hold: hold, fragments: map[string]capacityHold{}}
		idx.moves[hold.Move] = move
	}
	move.fragments[node] = hold
	if hold.Until > move.hold.Until {
		move.hold.Until = hold.Until
	}
	if hold.EvictedAt != "" && move.hold.EvictedAt == "" {
		move.hold.EvictedAt = hold.EvictedAt
	}
	idx.groups[hold.Group] = true
	for _, uid := range hold.Movers {
		idx.movers[types.UID(uid)] = true
	}
	for _, uid := range hold.Victims {
		idx.victims[types.UID(uid)] = true
	}
}

// removeMove drops every fragment and drain of a move.
func (idx *capacityUpgradeIndex) removeMove(id string) (touched []string) {
	for node, holds := range idx.holds {
		kept := holds[:0:0]
		for _, hold := range holds {
			if hold.Move != id {
				kept = append(kept, hold)
			}
		}
		if len(kept) != len(holds) {
			idx.holds[node] = kept
			touched = append(touched, node)
		}
	}
	for node, drains := range idx.drains {
		kept := drains[:0:0]
		for _, drain := range drains {
			if drain.Move != id {
				kept = append(kept, drain)
			}
		}
		if len(kept) != len(drains) {
			idx.drains[node] = kept
			touched = append(touched, node)
		}
	}
	if move, ok := idx.moves[id]; ok {
		delete(idx.groups, move.hold.Group)
		for _, uid := range move.hold.Movers {
			delete(idx.movers, types.UID(uid))
		}
		for _, uid := range move.hold.Victims {
			delete(idx.victims, types.UID(uid))
		}
		delete(idx.moves, id)
	}
	sort.Strings(touched)
	return touched
}

// outstanding is the GPU quantity (scheduler units) held on a node by every
// move in flight.
func (idx *capacityUpgradeIndex) outstanding(node string) float64 {
	total := 0.0
	for _, hold := range idx.holds[node] {
		total += hold.Gpus * gpuMilli
	}
	return total
}

// drained reports whether a node carries at least one drain.
func (idx *capacityUpgradeIndex) drained(node string) bool {
	return len(idx.drains[node]) > 0
}

// claimant reports whether a task on the node is a successor of one of the
// node's holds: it carries a hold's identity and is not one of its movers.
func (idx *capacityUpgradeIndex) claimant(node string, task *api.TaskInfo) bool {
	for _, hold := range idx.holds[node] {
		if holdMatches(hold, task) {
			return true
		}
	}
	return false
}

// holdMatches reports whether task may claim hold.
func holdMatches(hold capacityHold, task *api.TaskInfo) bool {
	if task.Pod == nil {
		return false
	}
	for _, uid := range hold.Movers {
		if types.UID(uid) == task.Pod.UID {
			return false
		}
	}
	return identityMatches(hold.Identity, task.Pod)
}

// identityMatches reports whether pod carries every identity entry.
func identityMatches(identity map[string]string, pod *v1.Pod) bool {
	if len(identity) == 0 {
		return false
	}
	for key, want := range identity {
		if key == ownerIdentityKey {
			owner := metav1.GetControllerOf(pod)
			if owner == nil || string(owner.UID) != want {
				return false
			}
			continue
		}
		if pod.Labels[key] != want {
			return false
		}
	}
	return true
}

// podIdentity derives the label set the pod's successor will carry: the
// configured identity labels when the pod has all of them, else its
// controller owner's UID. A pod with neither has no identity and cannot be
// moved, since its successor could never be recognised.
func podIdentity(pod *v1.Pod, identityLabels []string) (map[string]string, bool) {
	identity := map[string]string{}
	for _, key := range identityLabels {
		value, ok := pod.Labels[key]
		if !ok || value == "" {
			identity = nil
			break
		}
		identity[key] = value
	}
	if len(identity) > 0 {
		return identity, true
	}
	owner := metav1.GetControllerOf(pod)
	if owner == nil || owner.UID == "" {
		return nil, false
	}
	return map[string]string{ownerIdentityKey: string(owner.UID)}, true
}

// groupIdentity is the identity shared by every member, or false when the
// members disagree (the successor could not be told from a sibling gang).
func groupIdentity(members []*api.TaskInfo, identityLabels []string) (map[string]string, bool) {
	var identity map[string]string
	for _, member := range members {
		id, ok := podIdentity(member.Pod, identityLabels)
		if !ok {
			return nil, false
		}
		if identity == nil {
			identity = id
			continue
		}
		if len(id) != len(identity) {
			return nil, false
		}
		for key, value := range identity {
			if id[key] != value {
				return nil, false
			}
		}
	}
	return identity, identity != nil
}

// taskGpu is the GPU quantity (scheduler units) a task occupies or requests.
func taskGpu(task *api.TaskInfo, gpu v1.ResourceName) float64 {
	return taskGpuNeed(task, gpu).Get(gpu)
}

// isProbe reports whether the task is one of the planner's fit probes.
func isProbe(task *api.TaskInfo) bool {
	return task.Pod != nil && task.Pod.Annotations[capacityUpgradeProbeAnnotation] == "true"
}

// capacityUpgradePredicateFn is the hold and drain predicate. It runs for
// every GPU pod in allocate, preempt and backfill:
//
//   - On a held node, a pod that is not a claimant of one of the node's holds
//     may use only the GPUs left after every hold is honoured, measured
//     against the node's future idle exactly as allocate does; a claimant is
//     unrestricted.
//   - On a drained node, every pod may use only the node's real idle GPUs.
//     Releasing capacity — the drained movers — is off limits until it is
//     actually free, at which point the drain has nothing left to protect
//     and node scoring decides.
//
// Rejections are UnschedulableAndUnresolvable so preempt cannot route around
// them by evicting more pods.
func capacityUpgradePredicateFn(idx *capacityUpgradeIndex, gpu v1.ResourceName) api.PredicateFn {
	return func(task *api.TaskInfo, node *api.NodeInfo) error {
		if task.Pod == nil || isProbe(task) {
			return nil
		}
		need := task.InitResreq.Get(gpu)
		if need <= 0 {
			return nil
		}
		if idx.drained(node.Name) {
			idle := node.Idle.Get(gpu) - node.Pipelined.Get(gpu)
			if need > idle {
				return api.NewFitErrWithStatus(task, node, &api.Status{
					Code:   api.UnschedulableAndUnresolvable,
					Reason: fmt.Sprintf("capacityUpgrade: node is draining, %v GPUs idle", idle/gpuMilli),
				})
			}
		}
		if outstanding := idx.outstanding(node.Name); outstanding > 0 && !idx.claimant(node.Name, task) {
			free := node.FutureIdle().Get(gpu) - outstanding
			if need > free {
				return api.NewFitErrWithStatus(task, node, &api.Status{
					Code:   api.UnschedulableAndUnresolvable,
					Reason: fmt.Sprintf("capacityUpgrade: %v GPUs held for a capacity upgrade", outstanding/gpuMilli),
				})
			}
		}
		return nil
	}
}

// heldNodePreference outweighs every other node-order signal (capacitycost
// scores tiers in the low thousands, binpack 0-100 per weight) so a
// successor lands on the capacity held for it rather than on any other node
// that happens to fit; it never vetoes, so a successor that cannot use its
// hold still schedules.
const heldNodePreference = float64(100000)

// capacityUpgradeNodeOrderFn steers a hold's claimant onto the nodes
// holding for it.
func capacityUpgradeNodeOrderFn(idx *capacityUpgradeIndex, gpu v1.ResourceName) api.NodeOrderFn {
	return func(task *api.TaskInfo, node *api.NodeInfo) (float64, error) {
		if task.Pod == nil || task.InitResreq.Get(gpu) <= 0 {
			return 0, nil
		}
		if idx.claimant(node.Name, task) {
			return heldNodePreference, nil
		}
		return 0, nil
	}
}

// nodeState is the desired annotation state of one node.
type nodeState struct {
	holds  []capacityHold
	drains []capacityDrain
}

// groupStamp is a PodGroup cooldown/count update; repack also spends the
// gpuFragmentation eviction cap shared with this strategy.
type groupStamp struct {
	namespace, name string
	last            string
	moves           int
	repack          bool
}

// moveStep is one move's action for this session. Node writes come first;
// if any fails the step's evictions are skipped and the step is retried,
// from the cluster state, next session.
type moveStep struct {
	id   string
	hold capacityHold
	// gpus is the move's total GPU count across fragments.
	gpus    float64
	outcome string
	reason  string
	nodes   map[string]nodeState
	// first lists the nodes written before the rest. Source drains are
	// written before the hold records the eviction, so a write that fails
	// part-way never leaves movers evicted (this session or a later one)
	// without their sources drained.
	first  []string
	groups []groupStamp
	// victims are evicted once the writes succeed; evicting says what they
	// are to the move: "mover" or "victim" (a target victim evicted again).
	victims  []*api.TaskInfo
	evicting string
}

// advanceCapacityUpgradeMoves derives this session's actions from the moves
// in flight. It is pure: it mutates idx to the desired end state and returns
// the steps that realise it, in order.
func advanceCapacityUpgradeMoves(
	idx *capacityUpgradeIndex,
	nodes map[string]*api.NodeInfo,
	jobs map[api.JobID]*api.JobInfo,
	conf *capacityUpgradeConf,
	now time.Time,
	predicate capacityUpgradePredicate,
) []moveStep {
	gpu := v1.ResourceName(conf.GpuResource)
	ttl := time.Duration(conf.HoldTTLSeconds) * time.Second
	steps := make([]moveStep, 0)

	for node, reason := range idx.malformed {
		step := moveStep{id: node, outcome: "malformed", reason: reason, nodes: map[string]nodeState{}}
		delete(idx.holds, node)
		delete(idx.drains, node)
		step.nodes[node] = nodeState{}
		steps = append(steps, step)
	}

	ids := make([]string, 0, len(idx.moves))
	for id := range idx.moves {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	for _, id := range ids {
		move := idx.moves[id]
		hold := move.hold
		step := moveStep{id: id, hold: hold, nodes: map[string]nodeState{}}
		for _, fragment := range move.fragments {
			step.gpus += fragment.Gpus
		}

		until, err := time.Parse(time.RFC3339, hold.Until)
		evictedAt, evictedErr := time.Time{}, error(nil)
		if move.evicted() {
			evictedAt, evictedErr = time.Parse(time.RFC3339, hold.EvictedAt)
		}
		switch {
		case err != nil:
			step.outcome, step.reason = "abandoned", "unparseable deadline"
		case evictedErr != nil:
			step.outcome, step.reason = "abandoned", "unparseable eviction time"
		case !move.complete():
			step.outcome, step.reason = "abandoned", "fragments missing"
		case !now.Before(until):
			step.outcome = "expired"
			if move.evicted() {
				step.reason = "successor never claimed the hold"
			} else {
				step.reason = "target capacity never cleared"
			}
		}
		for _, node := range hold.Nodes {
			if step.outcome == "" && nodes[node] == nil {
				step.outcome, step.reason = "abandoned", "target node "+node+" gone"
			}
		}
		if step.outcome != "" {
			for _, node := range idx.removeMove(id) {
				step.nodes[node] = nodeState{holds: idx.holds[node], drains: idx.drains[node]}
			}
			steps = append(steps, step)
			continue
		}

		movers := runningMovers(hold, jobs)

		if move.evicted() {
			// Placing: release each fragment its successor has claimed;
			// re-evict movers that are somehow still running (an earlier
			// eviction did not take). Only pods created after the eviction
			// count: a sibling of the mover that already ran on the target
			// carries the same identity but is not the successor.
			step.victims, step.evicting = append(step.victims, movers...), "mover"
			claimed := 0
			successors := map[api.JobID]*api.JobInfo{}
			for _, node := range hold.Nodes {
				fragment := move.fragments[node]
				got := 0.0
				for _, task := range nodes[node].Tasks {
					if !holdMatches(fragment, task) || !placedStatus(task.Status) || !createdAfter(task, evictedAt) {
						continue
					}
					got += taskGpu(task, gpu)
					if job := jobs[task.Job]; job != nil && job.PodGroup != nil {
						successors[task.Job] = job
					}
				}
				if got >= fragment.Gpus*gpuMilli {
					claimed++
				}
			}
			if claimed == len(hold.Nodes) {
				step.outcome, step.reason = "claimed", "successor bound onto held capacity"
				for _, job := range successors {
					step.groups = append(step.groups, groupStamp{
						namespace: job.PodGroup.Namespace, name: job.PodGroup.Name,
						last: now.UTC().Format(time.RFC3339), moves: hold.Moves,
					})
				}
				sort.Slice(step.groups, func(i, j int) bool {
					return step.groups[i].namespace+"/"+step.groups[i].name < step.groups[j].namespace+"/"+step.groups[j].name
				})
				for _, node := range idx.removeMove(id) {
					step.nodes[node] = nodeState{holds: idx.holds[node], drains: idx.drains[node]}
				}
				steps = append(steps, step)
				continue
			}
			if len(step.victims) > 0 {
				step.outcome, step.reason = "evicting", "movers still running after eviction"
				steps = append(steps, step)
			}
			continue
		}

		// Clearing: wait until every target node can really take its
		// share, then drain the sources and evict the movers.
		if len(movers) == 0 {
			// The movers are gone without this move evicting them (the
			// job finished or was deleted); nothing can claim the hold.
			step.outcome, step.reason = "abandoned", "movers gone before eviction"
			for _, node := range idx.removeMove(id) {
				step.nodes[node] = nodeState{holds: idx.holds[node], drains: idx.drains[node]}
			}
			steps = append(steps, step)
			continue
		}
		if !targetsClear(idx, move, nodes, movers, gpu) {
			if stuck := runningVictims(hold, nodes, jobs); len(stuck) > 0 {
				step.victims, step.evicting = stuck, "victim"
				step.outcome, step.reason = "evicting", "victims still running after eviction"
				steps = append(steps, step)
			}
			continue
		}
		if !moversFit(move, nodes, movers, predicate) {
			continue
		}

		evictedStamp := now.UTC().Format(time.RFC3339)
		deadline := now.Add(ttl).UTC().Format(time.RFC3339)
		sources := map[string]bool{}
		for _, mover := range movers {
			if mover.NodeName != "" {
				sources[mover.NodeName] = true
			}
		}
		for node := range sources {
			drains := idx.drains[node]
			replaced := false
			for i := range drains {
				if drains[i].Move == id {
					drains[i].Until = deadline
					replaced = true
				}
			}
			if !replaced {
				drains = append(drains, capacityDrain{Move: id, Until: deadline})
			}
			idx.drains[node] = drains
		}
		for node, holds := range idx.holds {
			for i := range holds {
				if holds[i].Move == id {
					holds[i].EvictedAt = evictedStamp
					holds[i].Until = deadline
				}
			}
			idx.holds[node] = holds
		}
		hold.EvictedAt, hold.Until = evictedStamp, deadline
		move.hold = hold
		for node := range move.fragments {
			move.fragments[node] = idx.holdOf(node, id)
		}
		touched := map[string]bool{}
		for node := range sources {
			touched[node] = true
			step.first = append(step.first, node)
		}
		sort.Strings(step.first)
		for _, node := range hold.Nodes {
			touched[node] = true
		}
		for node := range touched {
			step.nodes[node] = nodeState{holds: idx.holds[node], drains: idx.drains[node]}
		}
		step.hold = hold
		step.victims, step.evicting = movers, "mover"
		step.outcome, step.reason = "evicting", "target capacity clear"
		steps = append(steps, step)
	}

	// Drains whose move is over, or which outlived their deadline, protect
	// nothing.
	for node, drains := range idx.drains {
		kept := drains[:0:0]
		for _, drain := range drains {
			until, err := time.Parse(time.RFC3339, drain.Until)
			if _, live := idx.moves[drain.Move]; live && err == nil && now.Before(until) {
				kept = append(kept, drain)
			}
		}
		if len(kept) != len(drains) {
			idx.drains[node] = kept
			steps = append(steps, moveStep{
				id: node, outcome: "drain_released", reason: "move finished",
				nodes: map[string]nodeState{node: {holds: idx.holds[node], drains: kept}},
			})
		}
	}
	return steps
}

func (idx *capacityUpgradeIndex) holdOf(node, id string) capacityHold {
	for _, hold := range idx.holds[node] {
		if hold.Move == id {
			return hold
		}
	}
	return capacityHold{}
}

// runningMovers returns the move's mover pods that are still running.
func runningMovers(hold capacityHold, jobs map[api.JobID]*api.JobInfo) []*api.TaskInfo {
	job := jobs[api.JobID(hold.Group)]
	if job == nil {
		return nil
	}
	wanted := map[types.UID]bool{}
	for _, uid := range hold.Movers {
		wanted[types.UID(uid)] = true
	}
	movers := make([]*api.TaskInfo, 0, len(hold.Movers))
	for _, task := range job.Tasks {
		if task.Pod != nil && wanted[task.Pod.UID] && task.Status == api.Running {
			movers = append(movers, task)
		}
	}
	sort.Slice(movers, func(i, j int) bool { return movers[i].Name < movers[j].Name })
	return movers
}

// runningVictims returns the move's victims still running on its target
// nodes (an eviction that did not take), as their jobs' tasks.
func runningVictims(hold capacityHold, nodes map[string]*api.NodeInfo, jobs map[api.JobID]*api.JobInfo) []*api.TaskInfo {
	wanted := map[types.UID]bool{}
	for _, uid := range hold.Victims {
		wanted[types.UID(uid)] = true
	}
	victims := make([]*api.TaskInfo, 0)
	for _, name := range hold.Nodes {
		for _, task := range nodes[name].Tasks {
			if task.Pod == nil || !wanted[task.Pod.UID] || task.Status != api.Running {
				continue
			}
			if job := jobs[task.Job]; job != nil && job.Tasks[task.UID] != nil {
				task = job.Tasks[task.UID]
			}
			victims = append(victims, task)
		}
	}
	sort.Slice(victims, func(i, j int) bool { return victims[i].Name < victims[j].Name })
	return victims
}

// createdAfter reports whether the task's pod was created at or after the
// given time.
func createdAfter(task *api.TaskInfo, at time.Time) bool {
	created := task.Pod.CreationTimestamp
	return !created.IsZero() && !created.Time.Before(at)
}

// placedStatus reports whether a task's status means it has been bound to
// its node (a task merely allocated or pipelined in the current session
// can still fall through before binding).
func placedStatus(status api.TaskStatus) bool {
	switch status {
	case api.Binding, api.Bound, api.Running:
		return true
	}
	return false
}

// targetsClear reports whether every target node's idle GPUs (plus the GPUs
// of this move's own movers running there, which the eviction frees) cover
// everything held on it.
func targetsClear(idx *capacityUpgradeIndex, move *capacityUpgradeMove, nodes map[string]*api.NodeInfo, movers []*api.TaskInfo, gpu v1.ResourceName) bool {
	for _, name := range move.hold.Nodes {
		node := nodes[name]
		available := node.Idle.Get(gpu)
		for _, mover := range movers {
			if mover.NodeName == name {
				available += taskGpu(mover, gpu)
			}
		}
		if available < idx.outstanding(name) {
			return false
		}
	}
	return true
}

// moversFit reports whether each mover still passes predicates on at least
// one target node, so the successor is not evicted into an unschedulable
// state (a taint or label changed since planning).
func moversFit(move *capacityUpgradeMove, nodes map[string]*api.NodeInfo, movers []*api.TaskInfo, predicate capacityUpgradePredicate) bool {
	if predicate == nil {
		return true
	}
	for _, mover := range movers {
		fits := false
		for _, name := range move.hold.Nodes {
			if err := predicate(mover, nodes[name], move.hold.gang()); err == nil {
				fits = true
				break
			}
		}
		if !fits {
			return false
		}
	}
	return true
}

// capacityUpgradeStore writes capacity-upgrade state to the cluster.
type capacityUpgradeStore interface {
	writeNode(name string, state nodeState) error
	stampGroup(stamp groupStamp) error
}

// applyMoveSteps executes steps in order: node state first, then PodGroup
// stamps, then it reports the movers to evict. A step whose node write fails
// is logged and skipped; the cluster state it read from is unchanged, so the
// next session retries it. Returns the movers whose steps fully applied.
func applyMoveSteps(steps []moveStep, store capacityUpgradeStore) []*api.TaskInfo {
	victims := make([]*api.TaskInfo, 0)
	for _, step := range steps {
		if err := writeNodeStates(step.nodes, step.first, store); err != nil {
			klog.Errorf("capacityUpgrade: move %s %s: %v", step.id, step.outcome, err)
			capacityUpgradeStampFailures.WithLabelValues("hold").Inc()
			continue
		}
		for _, stamp := range step.groups {
			if err := store.stampGroup(stamp); err != nil {
				// The successor keeps running on the right capacity; only
				// its cooldown/budget carry-over is lost, which is safe.
				klog.Errorf("capacityUpgrade: move %s: carry cooldown to %s/%s: %v", step.id, stamp.namespace, stamp.name, err)
				capacityUpgradeStampFailures.WithLabelValues("successor").Inc()
			}
		}
		if step.hold.Move != "" {
			klog.V(2).Infof("capacityUpgrade: move %s (%s, %d pods, %v GPUs, %s -> %s) %s: %s",
				step.id, step.hold.kind(), len(step.hold.Movers), step.gpus,
				step.hold.From, step.hold.Target, step.outcome, step.reason)
		} else {
			klog.V(2).Infof("capacityUpgrade: node %s %s: %s", step.id, step.outcome, step.reason)
		}
		switch step.outcome {
		case "claimed":
			capacityUpgradeHoldOutcomes.WithLabelValues(step.outcome, step.hold.kind()).Inc()
			capacityUpgradePods.WithLabelValues(step.hold.Target, step.hold.kind(), "moved").Add(float64(len(step.hold.Movers)))
			capacityUpgradeGpus.WithLabelValues(step.hold.Target, step.hold.kind(), "moved").Add(step.gpus)
			capacityUpgradeMoves.WithLabelValues(step.hold.Target, step.hold.kind(), "moved").Inc()
		case "expired", "abandoned":
			capacityUpgradeHoldOutcomes.WithLabelValues(step.outcome, step.hold.kind()).Inc()
		case "malformed":
			capacityUpgradeHoldOutcomes.WithLabelValues(step.outcome, "node").Inc()
		}
		for _, victim := range step.victims {
			klog.V(2).Infof("capacityUpgrade: evicting %s %s/%s from %s for move %s",
				step.evicting, victim.Namespace, victim.Name, victim.NodeName, step.id)
			capacityUpgradeEvictions.WithLabelValues(step.hold.kind(), step.evicting).Inc()
		}
		victims = append(victims, step.victims...)
	}
	return victims
}

// writeNodeStates writes every node's state, the first nodes in the given
// order and then the rest in name order, stopping at the first failure.
func writeNodeStates(states map[string]nodeState, first []string, store capacityUpgradeStore) error {
	names := make([]string, 0, len(states))
	leading := map[string]bool{}
	for _, name := range first {
		if _, ok := states[name]; ok && !leading[name] {
			leading[name] = true
			names = append(names, name)
		}
	}
	rest := make([]string, 0, len(states))
	for name := range states {
		if !leading[name] {
			rest = append(rest, name)
		}
	}
	sort.Strings(rest)
	names = append(names, rest...)
	for _, name := range names {
		if err := store.writeNode(name, states[name]); err != nil {
			return fmt.Errorf("write node %s: %w", name, err)
		}
	}
	return nil
}

// nodeAnnotations encodes a node's desired state as annotation values; an
// empty list becomes nil so a merge patch removes the annotation.
func nodeAnnotations(state nodeState) (map[string]*string, error) {
	annotations := map[string]*string{
		CapacityUpgradeHoldsAnnotation:  nil,
		CapacityUpgradeDrainsAnnotation: nil,
	}
	if len(state.holds) > 0 {
		body, err := json.Marshal(state.holds)
		if err != nil {
			return nil, fmt.Errorf("encode holds: %w", err)
		}
		value := string(body)
		annotations[CapacityUpgradeHoldsAnnotation] = &value
	}
	if len(state.drains) > 0 {
		body, err := json.Marshal(state.drains)
		if err != nil {
			return nil, fmt.Errorf("encode drains: %w", err)
		}
		value := string(body)
		annotations[CapacityUpgradeDrainsAnnotation] = &value
	}
	return annotations, nil
}

// groupAnnotations encodes a PodGroup stamp as annotation values.
func groupAnnotations(stamp groupStamp) map[string]*string {
	last, count, one := stamp.last, strconv.Itoa(stamp.moves), "1"
	annotations := map[string]*string{
		CapacityUpgradeLastAnnotation:  &last,
		CapacityUpgradeCountAnnotation: &count,
	}
	if stamp.repack {
		annotations[groupEvictionAnnotation] = &one
	}
	return annotations
}

// annotationPatch is the JSON merge patch setting (or, for nil values,
// removing) the given metadata annotations.
func annotationPatch(annotations map[string]*string) ([]byte, error) {
	return json.Marshal(map[string]interface{}{"metadata": map[string]interface{}{"annotations": annotations}})
}

// sessionStore writes through the session's clients.
type sessionStore struct{}

func (sessionStore) writeNode(name string, state nodeState) error {
	annotations, err := nodeAnnotations(state)
	if err != nil {
		return err
	}
	patch, err := annotationPatch(annotations)
	if err != nil {
		return fmt.Errorf("encode patch: %w", err)
	}
	_, err = Session.KubeClient().CoreV1().Nodes().Patch(
		context.TODO(), name, types.MergePatchType, patch, metav1.PatchOptions{})
	return err
}

func (sessionStore) stampGroup(stamp groupStamp) error {
	patch, err := annotationPatch(groupAnnotations(stamp))
	if err != nil {
		return fmt.Errorf("encode patch: %w", err)
	}
	if _, err := Session.VCClient().SchedulingV1beta1().PodGroups(stamp.namespace).Patch(
		context.TODO(), stamp.name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("stamp podgroup %s/%s: %w", stamp.namespace, stamp.name, err)
	}
	return nil
}

// podGroupMoves is the PodGroup's move count; a malformed count is treated
// as exhausted so a bad annotation can never unlock extra restarts.
func podGroupMoves(pg *api.PodGroup, conf *capacityUpgradeConf) int {
	raw, ok := pg.Annotations[CapacityUpgradeCountAnnotation]
	if !ok || raw == "" {
		return 0
	}
	count, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || count < 0 {
		return conf.MaxMovesPerGroup
	}
	return count
}

// splitIdentityLabels parses the comma-separated identity label list.
func splitIdentityLabels(raw string) []string {
	labels := make([]string, 0)
	for _, part := range strings.Split(raw, ",") {
		if part = strings.TrimSpace(part); part != "" {
			labels = append(labels, part)
		}
	}
	return labels
}

// moveID names a move after its mover PodGroup and start time.
func moveID(group string, at time.Time) string {
	return group + "@" + at.UTC().Format(time.RFC3339)
}
