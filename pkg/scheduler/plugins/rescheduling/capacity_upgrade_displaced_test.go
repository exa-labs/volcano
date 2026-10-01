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

// Tests for capacityUpgrade's maxDisplacedPriority: which pods on a target
// count as room (planner), and how a move evicts them before its mover
// (transaction). Every test that sets no ceiling lives in
// capacity_upgrade_test.go, where nothing but the mover is ever evicted.

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"volcano.sh/volcano/pkg/scheduler/api"
)

// displacingConf is the live configuration with pods at or below ceiling
// counted as room on target nodes.
func displacingConf(ceiling int32) *capacityUpgradeConf {
	conf := liveConf()
	conf.MaxDisplacedPriority = &ceiling
	return conf
}

// fillers places count single-pod groups of gpus GPUs each at the given
// priority on node, returning their session tasks in name order.
func (f *fixture) fillers(t *testing.T, node string, count int, gpus int64, priority int32) []*api.TaskInfo {
	tasks := make([]*api.TaskInfo, 0, count)
	for i := 0; i < count; i++ {
		name := fmt.Sprintf("filler-%s-%dgpu-pri%d-%d", node, gpus, priority, i)
		tasks = append(tasks, f.only(t, f.placeGroup(t, 1, nil, tierPod(name, node, "pg-"+name, gpus, priority, time.Hour))))
	}
	return tasks
}

// only returns the single task of a one-pod job.
func (f *fixture) only(t *testing.T, job *api.JobInfo) *api.TaskInfo {
	if len(job.Tasks) != 1 {
		t.Fatalf("job %s has %d tasks, expected 1", job.UID, len(job.Tasks))
	}
	for _, task := range job.Tasks {
		return task
	}
	return nil
}

// displacedNames lists the pods a plan displaces, sorted.
func displacedNames(plan capacityUpgradePlan) []string {
	out := make([]string, 0)
	for _, pods := range plan.displaced {
		out = append(out, names(pods)...)
	}
	sort.Strings(out)
	return out
}

// fillerNode is the shape the ceiling exists for: a mover on spot and a
// reserved node with no idle GPU, every one of them running a filler.
// Returns the fixture, the mover and the fillers.
func fillerNode(t *testing.T, moverGpus int64) (*fixture, *api.TaskInfo, []*api.TaskInfo) {
	f := newFixture(t)
	f.addNode(tierNode("spot-1", "spot", "a"))
	f.addNode(tierNode("reserved-1", "reserved", "a"))
	mover := f.only(t, f.placeGroup(t, 1, nil, tierPod("train", "spot-1", "pg-train", moverGpus, -4, time.Hour)))
	return f, mover, f.fillers(t, "reserved-1", 8, 1, -9)
}

// ---- planner -------------------------------------------------------------

// Pods at or below the ceiling are room; a pod above it is not, however far
// below the mover it ranks.
func TestUpgradeDisplacesOnlyPodsAtOrBelowTheCeiling(t *testing.T) {
	build := func(fillers int, residentGpus int64) *fixture {
		f := newFixture(t)
		f.addNode(tierNode("spot-1", "spot", "a"))
		f.addNode(tierNode("reserved-1", "reserved", "a"))
		f.placeGroup(t, 1, nil, tierPod("train", "spot-1", "pg-train", 4, -4, time.Hour))
		f.placeGroup(t, 1, nil, tierPod("resident", "reserved-1", "pg-resident", residentGpus, -8, time.Hour))
		f.fillers(t, "reserved-1", fillers, 1, -9)
		return f
	}

	f := build(5, 3)
	plans := f.planUpgrades(displacingConf(-9), nil)
	if len(plans) != 1 || plans[0].nodes["reserved-1"] != 4 {
		t.Fatalf("expected train placed over the fillers, got %+v", plans)
	}
	displaced := displacedNames(plans[0])
	if len(displaced) != 4 || strings.Contains(strings.Join(displaced, ","), "resident") {
		t.Fatalf("expected 4 fillers displaced and the resident kept, got %v", displaced)
	}

	// 3 fillers are not room for 4 GPUs, and the resident above the
	// ceiling does not make up the difference.
	f = build(3, 5)
	if plans := f.planUpgrades(displacingConf(-9), nil); len(plans) != 0 {
		t.Fatalf("expected no plan over a pod above the ceiling, got %+v", plans)
	}
	// Raising the ceiling to the resident's priority makes it room too.
	if plans := f.planUpgrades(displacingConf(-8), nil); len(plans) != 1 {
		t.Fatalf("expected a plan once the resident is at the ceiling, got %+v", plans)
	}
}

// Idle GPUs are taken before anything is displaced, on any node; a node
// short of idle GPUs loses only the shortfall.
func TestUpgradePrefersIdleCapacityOverDisplacing(t *testing.T) {
	f := newFixture(t)
	f.addNode(tierNode("spot-1", "spot", "a"))
	f.addNode(tierNode("reserved-1", "reserved", "a"))
	f.addNode(tierNode("reserved-2", "reserved", "a"))
	f.placeGroup(t, 1, nil, tierPod("train", "spot-1", "pg-train", 4, -4, time.Hour))
	f.fillers(t, "reserved-1", 8, 1, -9)
	partial := f.fillers(t, "reserved-2", 6, 1, -9)

	// reserved-2 has 2 idle GPUs: 2 fillers go, not 4.
	plans := f.planUpgrades(displacingConf(-9), nil)
	if len(plans) != 1 || plans[0].nodes["reserved-2"] != 4 || len(displacedNames(plans[0])) != 2 {
		t.Fatalf("expected reserved-2 with 2 fillers displaced, got %+v", plans)
	}

	// With 4 idle GPUs there, nothing is displaced.
	f.remove(t, partial[0])
	f.remove(t, partial[1])
	plans = f.planUpgrades(displacingConf(-9), nil)
	if len(plans) != 1 || plans[0].nodes["reserved-2"] != 4 || len(displacedNames(plans[0])) != 0 {
		t.Fatalf("expected the idle GPUs taken and nothing displaced, got %+v", plans)
	}
}

// As few pods as possible are disturbed: the lowest priority goes first
// and, within it, one pod that covers the shortfall beats several that add
// up to it.
func TestUpgradeDisplacesLowestPriorityThenFewestPods(t *testing.T) {
	build := func() *fixture {
		f := newFixture(t)
		f.addNode(tierNode("spot-1", "spot", "a"))
		f.addNode(tierNode("reserved-1", "reserved", "a"))
		f.fillers(t, "reserved-1", 1, 1, -9)
		f.fillers(t, "reserved-1", 1, 2, -9)
		f.fillers(t, "reserved-1", 1, 4, -9)
		return f
	}

	f := build()
	f.placeGroup(t, 1, nil, tierPod("resident", "reserved-1", "pg-resident", 1, -2, time.Hour))
	f.placeGroup(t, 1, nil, tierPod("train", "spot-1", "pg-train", 3, -4, time.Hour))
	plans := f.planUpgrades(displacingConf(-9), nil)
	if len(plans) != 1 || strings.Join(displacedNames(plans[0]), ",") != "filler-reserved-1-4gpu-pri-9-0" {
		t.Fatalf("expected the one 4-GPU filler displaced for 3 GPUs, got %+v", plans)
	}

	f = build()
	f.fillers(t, "reserved-1", 1, 1, -10)
	f.placeGroup(t, 1, nil, tierPod("train", "spot-1", "pg-train", 1, -4, time.Hour))
	plans = f.planUpgrades(displacingConf(-9), nil)
	if len(plans) != 1 || len(displacedNames(plans[0])) != 1 || plans[0].displaced["reserved-1"][0].Priority != -10 {
		t.Fatalf("expected the lowest-priority filler displaced, got %+v", plans)
	}
}

// A pod moves have to make room over is never moved itself: displaced onto
// costlier capacity it stays there, so no chain of moves can form.
func TestUpgradeNeverMovesPodsAtOrBelowTheCeiling(t *testing.T) {
	f := newFixture(t)
	f.addNode(tierNode("spot-1", "spot", "a"))
	f.addNode(tierNode("reserved-1", "reserved", "a"))
	f.fillers(t, "spot-1", 1, 1, -9)

	if plans := f.planUpgrades(liveConf(), nil); len(plans) != 1 {
		t.Fatalf("without a ceiling the pod moves into idle reserved GPUs, got %+v", plans)
	}
	if plans := f.planUpgrades(displacingConf(-9), nil); len(plans) != 0 {
		t.Fatalf("expected no move of a pod at the ceiling, got %+v", plans)
	}
}

// Opting out of the strategy, having no controller or carrying disruption
// protection keeps a pod from being displaced exactly as it keeps it from
// being moved.
func TestUpgradeNeverDisplacesShieldedPods(t *testing.T) {
	f, _, fillers := fillerNode(t, 8)
	conf := displacingConf(-9)
	fillers[0].Pod.Labels = map[string]string{conf.OptOutLabel: "false"}
	fillers[1].Pod.OwnerReferences = nil
	fillers[2].Pod.Annotations[doNotDisruptAnnotation] = "true"
	for _, shielded := range fillers[:3] {
		clone := f.nodes["reserved-1"].Tasks[api.PodKey(shielded.Pod)]
		clone.Pod = shielded.Pod
	}
	if plans := f.planUpgrades(conf, nil); len(plans) != 0 {
		t.Fatalf("expected no plan over shielded pods, got %+v", plans)
	}

	// 5 unshielded fillers are still room for a smaller mover.
	f.remove(t, f.only(t, f.jobs["default/pg-train"]))
	f.placeGroup(t, 1, nil, tierPod("small", "spot-1", "pg-small", 5, -4, time.Hour))
	plans := f.planUpgrades(conf, nil)
	if len(plans) != 1 || len(displacedNames(plans[0])) != 5 {
		t.Fatalf("expected the 5 unshielded fillers displaced, got %+v", plans)
	}
	for _, name := range displacedNames(plans[0]) {
		for _, shielded := range fillers[:3] {
			if name == shielded.Name {
				t.Fatalf("shielded pod %s was displaced", name)
			}
		}
	}
}

// Two plans in one pass never count on displacing the same pod, and a later
// pass does not count pods a move in flight is already displacing.
func TestUpgradeLedgerCountsEachDisplacedPodOnce(t *testing.T) {
	f := newFixture(t)
	f.addNode(tierNode("spot-1", "spot", "a"))
	f.addNode(tierNode("reserved-1", "reserved", "a"))
	f.placeGroup(t, 1, nil, tierPod("train-a", "spot-1", "pg-a", 3, -4, 2*time.Hour))
	f.placeGroup(t, 1, nil, tierPod("train-b", "spot-1", "pg-b", 3, -4, time.Hour))
	f.fillers(t, "reserved-1", 8, 1, -9)
	conf := displacingConf(-9)
	store := newMemStore(f)

	plans := f.startOnly(t, conf, store)
	if len(plans) != 2 {
		t.Fatalf("expected both movers planned over 8 fillers, got %+v", plans)
	}
	seen := map[string]bool{}
	for _, plan := range plans {
		displaced := displacedNames(plan)
		if len(displaced) != 3 {
			t.Fatalf("expected 3 fillers per plan, got %v", displaced)
		}
		for _, name := range displaced {
			if seen[name] {
				t.Fatalf("%s is counted on by two plans", name)
			}
			seen[name] = true
		}
	}

	// 2 fillers are left uncounted: room for one more 2-GPU mover, not two.
	f.addNode(tierNode("spot-2", "spot", "a"))
	f.placeGroup(t, 1, nil, tierPod("train-c", "spot-2", "pg-c", 2, -4, time.Hour))
	f.placeGroup(t, 1, nil, tierPod("train-d", "spot-2", "pg-d", 2, -4, time.Minute*30))
	plans = f.planUpgrades(conf, nil)
	if len(plans) != 1 || len(displacedNames(plans[0])) != 2 {
		t.Fatalf("expected one more plan over the 2 fillers no move counts on, got %+v", plans)
	}
	for _, name := range displacedNames(plans[0]) {
		if seen[name] {
			t.Fatalf("%s is already being displaced by a move in flight", name)
		}
	}
}

// Making room does not override the pending-demand guard: fillers that
// pending work outranking the mover would preempt anyway are left to it.
func TestUpgradeLeavesFillersToPendingWorkThatOutranksTheMover(t *testing.T) {
	f, _, _ := fillerNode(t, 4)
	f.pendingGroup(t, "gang", "default", -2, 2, 8)
	f.useDemand(nil)
	if plans := f.planUpgrades(displacingConf(-9), nil); len(plans) != 0 {
		t.Fatalf("expected the filler node left to the pending gang, got %+v", plans)
	}

	f, _, _ = fillerNode(t, 4)
	f.pendingGroup(t, "gang", "default", -6, 2, 8)
	f.useDemand(nil)
	if plans := f.planUpgrades(displacingConf(-9), nil); len(plans) != 1 {
		t.Fatalf("expected the move when the pending gang ranks below the mover, got %+v", plans)
	}
}

func TestUpgradeConfParsesDisplacedPriority(t *testing.T) {
	conf := newCapacityUpgradeConf()
	if conf.MaxDisplacedPriority != nil || conf.displaces(-100) {
		t.Fatalf("expected no displaced priority by default, got %v", *conf.MaxDisplacedPriority)
	}
	// Scheduler configuration numbers arrive as float64.
	conf.parse(map[string]interface{}{"maxDisplacedPriority": float64(-9)})
	if conf.MaxDisplacedPriority == nil || *conf.MaxDisplacedPriority != -9 {
		t.Fatalf("expected a ceiling of -9, got %v", conf.MaxDisplacedPriority)
	}
	if !conf.displaces(-9) || !conf.displaces(-10) || conf.displaces(-8) {
		t.Fatalf("expected exactly the priorities at or below -9 to be displaced")
	}
}

// ---- transaction ---------------------------------------------------------

// The hold names the pods it displaces. They are evicted first, as session
// tasks; the mover is untouched and its source undrained until their GPUs
// are really idle, and the fillers the plan did not count on never move.
func TestMoveDisplacesFillersBeforeEvictingTheMover(t *testing.T) {
	f, mover, fillers := fillerNode(t, 4)
	conf := displacingConf(-9)
	store := newMemStore(f)

	// Pass 1: the hold is written with its displaced pods; nobody is
	// touched.
	plans := f.startOnly(t, conf, store)
	if len(plans) != 1 || plans[0].nodes["reserved-1"] != 4 {
		t.Fatalf("expected 1 plan holding 4 GPUs on reserved-1, got %+v", plans)
	}
	holds := f.holdsOn(t, "reserved-1")
	if len(holds) != 1 || holds[0].Gpus != 4 || len(holds[0].Displaced) != 4 || holds[0].EvictedAt != "" {
		t.Fatalf("expected a hold naming 4 displaced pods, got %+v", holds)
	}
	counted := map[string]bool{}
	for _, uid := range holds[0].Displaced {
		counted[uid] = true
	}

	// Session 2: exactly the named fillers are evicted.
	steps, evictions := f.advance(conf, store, testNow.Add(time.Second), nil)
	if len(steps) != 1 || steps[0].outcome != "displacing" || len(evictions) != 4 {
		t.Fatalf("expected the displacing step with 4 evictions, got %+v / %v", steps, names(evictions))
	}
	for _, evicted := range evictions {
		if !counted[string(evicted.Pod.UID)] {
			t.Fatalf("%s was evicted without being named in the hold", evicted.Name)
		}
		if evicted != f.running[evicted.Pod.UID] || evicted == f.nodes["reserved-1"].Tasks[api.PodKey(evicted.Pod)] {
			t.Fatalf("%s is not the session task", evicted.Name)
		}
	}
	if mover.Status != api.Running || f.drainsOn(t, "spot-1") != nil || f.holdsOn(t, "reserved-1")[0].EvictedAt != "" {
		t.Fatalf("the mover must stay until the room is made")
	}

	// Session 3: the fillers are terminating. Nothing is evicted twice and
	// the mover still waits.
	for _, evicted := range evictions {
		f.evict(t, evicted)
	}
	if steps, again := f.advance(conf, store, testNow.Add(2*time.Second), nil); len(steps) != 0 || len(again) != 0 {
		t.Fatalf("expected the move to wait for the fillers to go, got %+v / %v", steps, names(again))
	}

	// Session 4: their GPUs are idle. Drain the source, evict the mover.
	for _, evicted := range evictions {
		f.remove(t, evicted)
	}
	steps, evictions = f.advance(conf, store, testNow.Add(time.Minute), nil)
	if len(steps) != 1 || steps[0].reason != "held capacity idle" || len(evictions) != 1 || !sameTask(evictions[0], mover) {
		t.Fatalf("expected the mover evicted once the room is idle, got %+v / %v", steps, names(evictions))
	}
	if len(f.drainsOn(t, "spot-1")) != 1 {
		t.Fatalf("expected the source drained with the mover's eviction")
	}
	kept := 0
	for _, filler := range fillers {
		if !counted[string(filler.Pod.UID)] && filler.Status == api.Running && f.running[filler.Pod.UID] == filler {
			kept++
		}
	}
	if kept != 4 {
		t.Fatalf("expected the 4 fillers the plan did not count on left running, got %d", kept)
	}
}

// An eviction that did not take is repeated for that pod alone.
func TestMoveEvictsAgainOnlyTheDisplacedPodsStillRunning(t *testing.T) {
	f, _, _ := fillerNode(t, 4)
	conf := displacingConf(-9)
	store := newMemStore(f)
	f.startOnly(t, conf, store)
	_, evictions := f.advance(conf, store, testNow.Add(time.Second), nil)
	if len(evictions) != 4 {
		t.Fatalf("expected 4 fillers evicted, got %v", names(evictions))
	}
	stuck := evictions[3]
	for _, evicted := range evictions[:3] {
		f.evict(t, evicted)
	}
	_, again := f.advance(conf, store, testNow.Add(2*time.Second), nil)
	if len(again) != 1 || !sameTask(again[0], stuck) {
		t.Fatalf("expected only %s evicted again, got %v", stuck.Name, names(again))
	}
}

// Room that comes from elsewhere spares the displaced pods: only what the
// held share still lacks is evicted.
func TestMoveSparesDisplacedPodsWhenRoomComesFromElsewhere(t *testing.T) {
	build := func() (*fixture, *api.TaskInfo, []*api.TaskInfo, *memStore) {
		f := newFixture(t)
		f.addNode(tierNode("spot-1", "spot", "a"))
		f.addNode(tierNode("reserved-1", "reserved", "a"))
		mover := f.only(t, f.placeGroup(t, 1, nil, tierPod("train", "spot-1", "pg-train", 4, -4, time.Hour)))
		residents := []*api.TaskInfo{
			f.only(t, f.placeGroup(t, 1, nil, tierPod("resident-0", "reserved-1", "pg-resident-0", 2, -2, time.Hour))),
			f.only(t, f.placeGroup(t, 1, nil, tierPod("resident-1", "reserved-1", "pg-resident-1", 2, -2, time.Hour))),
		}
		f.fillers(t, "reserved-1", 4, 1, -9)
		store := newMemStore(f)
		if plans := f.startOnly(t, displacingConf(-9), store); len(plans) != 1 || len(displacedNames(plans[0])) != 4 {
			t.Fatalf("expected 1 plan displacing 4 fillers, got %+v", plans)
		}
		return f, mover, residents, store
	}

	// Both residents finish before the fillers are evicted: the held share
	// is idle and the mover goes straight away.
	f, mover, residents, store := build()
	f.remove(t, residents[0])
	f.remove(t, residents[1])
	_, evictions := f.advance(displacingConf(-9), store, testNow.Add(time.Second), nil)
	if len(evictions) != 1 || !sameTask(evictions[0], mover) {
		t.Fatalf("expected only the mover evicted, got %v", names(evictions))
	}

	// One resident finishes: 2 of the 4 held GPUs are idle, 2 fillers go.
	f, _, residents, store = build()
	f.remove(t, residents[0])
	steps, evictions := f.advance(displacingConf(-9), store, testNow.Add(time.Second), nil)
	if len(steps) != 1 || steps[0].outcome != "displacing" || len(evictions) != 2 {
		t.Fatalf("expected 2 fillers evicted for the 2 GPUs still short, got %+v / %v", steps, names(evictions))
	}

	// Both residents are terminating: their GPUs are not idle yet but will
	// be, and the hold keeps them for the move. Nobody is evicted.
	f, _, residents, store = build()
	f.evict(t, residents[0])
	f.evict(t, residents[1])
	if steps, evictions := f.advance(displacingConf(-9), store, testNow.Add(time.Second), nil); len(steps) != 0 || len(evictions) != 0 {
		t.Fatalf("expected the move to wait for the terminating residents, got %+v / %v", steps, names(evictions))
	}
}

// Nothing is displaced for a move that cannot go through: the mover no
// longer fits its target, or is gone.
func TestMoveDisplacesNothingForAMoverThatCannotLand(t *testing.T) {
	f, mover, fillers := fillerNode(t, 4)
	conf := displacingConf(-9)
	store := newMemStore(f)
	f.startOnly(t, conf, store)
	if len(f.index().displaced) != 4 {
		t.Fatalf("expected the index to carry the 4 displaced pods")
	}

	veto := func(task *api.TaskInfo, node *api.NodeInfo, gang bool) error { return errors.New("tainted") }
	if steps, evictions := f.advance(conf, store, testNow.Add(time.Second), veto); len(steps) != 0 || len(evictions) != 0 {
		t.Fatalf("expected nothing displaced while the mover cannot land, got %+v / %v", steps, names(evictions))
	}

	f.remove(t, mover)
	steps, evictions := f.advance(conf, store, testNow.Add(2*time.Second), nil)
	if len(steps) != 1 || steps[0].outcome != "abandoned" || len(evictions) != 0 {
		t.Fatalf("expected the move abandoned without evictions, got %+v / %v", steps, names(evictions))
	}
	if f.holdsOn(t, "reserved-1") != nil || len(f.index().displaced) != 0 {
		t.Fatalf("expected the hold and its displaced pods released")
	}
	for _, filler := range fillers {
		if filler.Status != api.Running {
			t.Fatalf("filler %s was disturbed", filler.Name)
		}
	}
}

// A gang's fragments each name the pods displaced on their own node.
func TestGangMoveDisplacesPerTargetNode(t *testing.T) {
	f := newFixture(t)
	for _, name := range []string{"spot-1", "spot-2"} {
		f.addNode(tierNode(name, "spot", "a"))
	}
	for _, name := range []string{"reserved-1", "reserved-2"} {
		f.addNode(tierNode(name, "reserved", "a"))
		f.fillers(t, name, 8, 1, -9)
	}
	f.placeGroup(t, 2, nil,
		tierPod("w0", "spot-1", "pg-gang", 8, -4, time.Hour),
		tierPod("w1", "spot-2", "pg-gang", 8, -4, time.Hour))
	conf := displacingConf(-9)
	store := newMemStore(f)

	plans := f.startOnly(t, conf, store)
	if len(plans) != 1 || !plans[0].gang || plans[0].displacedCount() != 16 {
		t.Fatalf("expected a gang plan displacing 16 fillers, got %+v", plans)
	}
	for _, node := range []string{"reserved-1", "reserved-2"} {
		holds := f.holdsOn(t, node)
		if len(holds) != 1 || len(holds[0].Displaced) != 8 {
			t.Fatalf("expected 8 displaced pods on %s, got %+v", node, holds)
		}
		for _, uid := range holds[0].Displaced {
			if !strings.HasPrefix(uid, "default-filler-"+node) {
				t.Fatalf("hold on %s names %s, a pod of another node", node, uid)
			}
		}
	}
	_, evictions := f.advance(conf, store, testNow.Add(time.Second), nil)
	if len(evictions) != 16 {
		t.Fatalf("expected all 16 fillers evicted, got %v", names(evictions))
	}
}
