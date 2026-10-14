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
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/api/evictioncost"
)

// startedHourAgo makes the pod's start time deterministic.
func startedHourAgo(task *api.TaskInfo) *api.TaskInfo {
	started := metav1.NewTime(time.Now().Add(-time.Hour))
	task.Pod.Status.StartTime = &started
	return task
}

// repackFixture is two equally full sources, one GPU used of eight each,
// and a fuller destination. The victim on "a-source" announces an hour of
// work it cannot resume (about 3900 GPU-seconds); the one on "b-source" is
// stateless (330).
func repackFixture(t *testing.T) *fixture {
	f := newFixture(t)
	a := f.addNode(gpuNode("a-source", 8, nil))
	b := f.addNode(gpuNode("b-source", 8, nil))
	dest := f.addNode(gpuNode("dest", 8, nil))
	startedHourAgo(f.placePod(t, a, gpuPod("costly", "a-source", 1, eligible(), map[string]string{evictioncost.AnnotationResume: "scratch"}, true), 1, ""))
	startedHourAgo(f.placePod(t, b, gpuPod("cheap", "b-source", 1, eligible(), map[string]string{evictioncost.AnnotationResume: "stateless"}, true), 1, ""))
	f.placePod(t, dest, gpuPod("resident", "dest", 6, nil, nil, true), 1, "")
	return f
}

func sources(plans []plannedMove) []string {
	out := make([]string, 0, len(plans))
	for _, plan := range plans {
		out = append(out, plan.source)
	}
	return out
}

func TestGpuFragmentationEvictionCostGate(t *testing.T) {
	skipped := func(mode string) float64 {
		return testutil.ToFloat64(gpuRepackSkipped.WithLabelValues("eviction_cost", mode))
	}
	conf := func(params map[string]interface{}) *gpuFragmentationConf {
		c := newGpuFragmentationConf()
		params["maxVictims"] = 1
		c.parse(params)
		return c
	}

	// Defaults: the gate is absent, and with equal fullness and priority
	// the name tie-break drains the costly source.
	before := skipped("live") + skipped("dry_run")
	dryBefore := skipped("dry_run")
	if got := sources(repackFixture(t).plan(conf(map[string]interface{}{}), nil)); len(got) != 1 || got[0] != "a-source" {
		t.Fatalf("defaults: expected a-source drained, got %v", got)
	}

	// Dry run: same plan as the defaults, the costly drain counted.
	dryRun := conf(map[string]interface{}{"maxEvictionCostGpuSecondsPerGpu": 1000, "evictionCostDryRun": true})
	if got := sources(repackFixture(t).plan(dryRun, nil)); len(got) != 1 || got[0] != "a-source" {
		t.Fatalf("dry run: expected a-source drained, got %v", got)
	}
	if skipped("dry_run") != dryBefore+1 || skipped("live")+skipped("dry_run") != before+1 {
		t.Fatalf("dry run: expected one dry_run count, got %v", skipped("dry_run")-dryBefore)
	}

	// On: the cheap source drains first, the costly one is never reached.
	on := conf(map[string]interface{}{"maxEvictionCostGpuSecondsPerGpu": 1000})
	if got := sources(repackFixture(t).plan(on, nil)); len(got) != 1 || got[0] != "b-source" {
		t.Fatalf("on: expected b-source drained, got %v", got)
	}

	// On, with the cheap drain over the cap too: nothing drains.
	on.MaxEvictionCostGpuSecondsPerGpu = 100
	liveBefore := skipped("live")
	if got := repackFixture(t).plan(on, nil); len(got) != 0 {
		t.Fatalf("on, low cap: expected no drain, got %v", sources(got))
	}
	if skipped("live") != liveBefore+2 {
		t.Fatalf("on, low cap: expected 2 live skips, got %v", skipped("live")-liveBefore)
	}

	// Unannounced victims keep today's behaviour unless gateUnannounced.
	f := newFixture(t)
	source := f.addNode(gpuNode("source", 8, nil))
	dest := f.addNode(gpuNode("dest", 8, nil))
	startedHourAgo(f.placePod(t, source, gpuPod("quiet", "source", 1, eligible(), nil, true), 1, ""))
	f.placePod(t, dest, gpuPod("resident", "dest", 6, nil, nil, true), 1, "")
	if got := f.plan(on, nil); len(got) != 1 {
		t.Fatalf("unannounced: expected the drain, got %v", sources(got))
	}
	on.EvictionCost.GateUnannounced = true
	if got := f.plan(on, nil); len(got) != 0 {
		t.Fatalf("gateUnannounced: expected no drain, got %v", sources(got))
	}
}

func TestEvictionCostGateDefaults(t *testing.T) {
	for name, gate := range map[string]evictionCostGate{
		"gpuFragmentation": newGpuFragmentationConf().EvictionCost,
		"capacityUpgrade":  newCapacityUpgradeConf().EvictionCost,
	} {
		if gate.DryRun || gate.GateUnannounced || gate.Defaults != evictioncost.DefaultDefaults() {
			t.Errorf("%s: unexpected default gate %+v", name, gate)
		}
	}
	if newGpuFragmentationConf().MaxEvictionCostGpuSecondsPerGpu != 0 || newCapacityUpgradeConf().MaxEvictionCostGpuSeconds != 0 {
		t.Fatal("eviction-cost caps must default to absent")
	}
	c := newCapacityUpgradeConf()
	c.parse(map[string]interface{}{"maxEvictionCostGpuSeconds": 5000, "evictionCostStartupSeconds": 900, "gateUnannounced": true})
	if c.MaxEvictionCostGpuSeconds != 5000 || c.EvictionCost.StartupSeconds != 900 || c.EvictionCost.TerminationSeconds != 30 || !c.EvictionCost.GateUnannounced {
		t.Fatalf("unexpected parsed conf %+v", c.EvictionCost)
	}
	if displacedCost(newCapacityUpgradeConf(), nil, testNow) != nil {
		t.Fatal("displaced pods must keep name order by default")
	}
}

// Room made from elsewhere leaves two of four displaced fillers to evict:
// by name by default, the cheapest once the move gate is in force.
func TestMoveDisplacesCheapestFillersFirst(t *testing.T) {
	run := func(conf *capacityUpgradeConf) []string {
		f := newFixture(t)
		f.addNode(tierNode("spot-1", "spot", "a"))
		f.addNode(tierNode("reserved-1", "reserved", "a"))
		f.only(t, f.placeGroup(t, 1, nil, tierPod("train", "spot-1", "pg-train", 4, -4, time.Hour)))
		resident := f.only(t, f.placeGroup(t, 1, nil, tierPod("resident", "reserved-1", "pg-resident", 2, -2, time.Hour)))
		f.only(t, f.placeGroup(t, 1, nil, tierPod("resident-1", "reserved-1", "pg-resident-1", 2, -2, time.Hour)))
		fillers := f.fillers(t, "reserved-1", 4, 1, -9)
		for _, filler := range fillers[2:] {
			filler.Pod.Annotations[evictioncost.AnnotationResume] = evictioncost.ResumeStateless
		}
		store := newMemStore(f)
		if plans := f.startOnly(t, conf, store); len(plans) != 1 || len(displacedNames(plans[0])) != 4 {
			t.Fatalf("expected 1 plan displacing 4 fillers, got %+v", plans)
		}
		f.remove(t, resident)
		_, evictions := f.advance(conf, store, testNow.Add(time.Second), nil)
		return names(evictions)
	}

	byName := run(displacingConf(-9))
	gated := displacingConf(-9)
	gated.MaxEvictionCostGpuSeconds = 1e9
	byCost := run(gated)
	if len(byName) != 2 || byName[0] != "filler-reserved-1-1gpu-pri-9-0" || byName[1] != "filler-reserved-1-1gpu-pri-9-1" {
		t.Fatalf("default: expected the first two fillers by name, got %v", byName)
	}
	if len(byCost) != 2 || byCost[0] != "filler-reserved-1-1gpu-pri-9-2" || byCost[1] != "filler-reserved-1-1gpu-pri-9-3" {
		t.Fatalf("gated: expected the two stateless fillers, got %v", byCost)
	}
}

// A move whose mover alone is over the cap is not started; in dry run it is
// started and counted.
func TestCapacityUpgradeEvictionCostGate(t *testing.T) {
	plan := func(conf *capacityUpgradeConf) int {
		f := newFixture(t)
		f.addNode(tierNode("spot-1", "spot", "a"))
		f.addNode(tierNode("reserved-1", "reserved", "a"))
		mover := f.only(t, f.placeGroup(t, 1, nil, tierPod("train", "spot-1", "pg-train", 4, -4, time.Hour)))
		mover.Pod.Annotations[evictioncost.AnnotationResume] = evictioncost.ResumeScratch
		return len(f.startOnly(t, conf, newMemStore(f)))
	}
	skipped := func(mode string) float64 {
		return testutil.ToFloat64(capacityUpgradeSkipped.WithLabelValues("eviction_cost", mode))
	}
	// 4 GPUs x (30 + 3600) s is over a 10000 GPU-second cap.
	conf := liveConf()
	conf.MaxEvictionCostGpuSeconds = 10000
	live := skipped("live")
	if got := plan(conf); got != 0 || skipped("live") != live+1 {
		t.Fatalf("on: expected the move skipped and counted, got %d plans", got)
	}
	conf.EvictionCost.DryRun = true
	dry := skipped("dry_run")
	if got := plan(conf); got != 1 || skipped("dry_run") != dry+1 {
		t.Fatalf("dry run: expected the move planned and counted, got %d plans", got)
	}
	if got := plan(liveConf()); got != 1 {
		t.Fatalf("defaults: expected the move planned, got %d plans", got)
	}
}

// The cheapest tier only fits by displacing announced fillers that put the
// move over the cap; the next tier has idle room, so the move goes there.
func TestCapacityUpgradeTriesNextTierOverCap(t *testing.T) {
	f := newFixture(t)
	f.addNode(tierNode("ondemand-1", "on-demand", "a"))
	f.addNode(tierNode("spot-1", "spot", "a"))
	f.addNode(tierNode("reserved-1", "reserved", "a"))
	f.only(t, f.placeGroup(t, 1, nil, tierPod("train", "ondemand-1", "pg-train", 4, -4, time.Hour)))
	for _, filler := range f.fillers(t, "reserved-1", 8, 1, -9) {
		filler.Pod.Annotations[evictioncost.AnnotationResume] = evictioncost.ResumeScratch
	}
	conf := displacingConf(-9)
	conf.MaxEvictionCostGpuSeconds = 10000
	plans := f.startOnly(t, conf, newMemStore(f))
	if len(plans) != 1 || plans[0].target != "spot" || plans[0].displacedCount() != 0 {
		t.Fatalf("expected one move to spot displacing nobody, got %+v", plans)
	}
}

// A drain over the cap counts as skipped only if it could have happened.
func TestGpuFragmentationSkipsCountOnlyFeasibleDrains(t *testing.T) {
	skipped := func() float64 { return testutil.ToFloat64(gpuRepackSkipped.WithLabelValues("eviction_cost", "live")) }
	f := newFixture(t)
	source := f.addNode(gpuNode("source", 8, nil))
	dest := f.addNode(gpuNode("dest", 8, nil))
	startedHourAgo(f.placePod(t, source, gpuPod("costly", "source", 1, eligible(), map[string]string{evictioncost.AnnotationResume: "scratch"}, true), 1, ""))
	f.placePod(t, dest, gpuPod("resident", "dest", 8, nil, nil, true), 1, "")
	conf := newGpuFragmentationConf()
	conf.MaxEvictionCostGpuSecondsPerGpu = 100
	before := skipped()
	if got := f.plan(conf, nil); len(got) != 0 || skipped() != before {
		t.Fatalf("infeasible drain: expected no drain and no skip, got %v and %v skips", sources(got), skipped()-before)
	}
}

// With the move gate in force, room is made from the cheapest fillers of a
// priority: the four stateless ones, not the scratch ones.
func TestMakeRoomDisplacesCheapestFillers(t *testing.T) {
	plan := func(conf *capacityUpgradeConf) []string {
		f := newFixture(t)
		f.addNode(tierNode("spot-1", "spot", "a"))
		f.addNode(tierNode("reserved-1", "reserved", "a"))
		f.only(t, f.placeGroup(t, 1, nil, tierPod("train", "spot-1", "pg-train", 4, -4, time.Hour)))
		for i, filler := range f.fillers(t, "reserved-1", 8, 1, -9) {
			resume := evictioncost.ResumeScratch
			if i >= 4 {
				resume = evictioncost.ResumeStateless
			}
			filler.Pod.Annotations[evictioncost.AnnotationResume] = resume
		}
		plans := f.startOnly(t, conf, newMemStore(f))
		if len(plans) != 1 {
			t.Fatalf("expected one plan, got %+v", plans)
		}
		return displacedNames(plans[0])
	}
	filler := func(i int) string { return fmt.Sprintf("filler-reserved-1-1gpu-pri-9-%d", i) }
	gated := displacingConf(-9)
	gated.MaxEvictionCostGpuSeconds = 1e9
	if got, want := plan(gated), []string{filler(4), filler(5), filler(6), filler(7)}; !reflect.DeepEqual(got, want) {
		t.Fatalf("gated: got %v, want %v", got, want)
	}
	if got := plan(displacingConf(-9)); len(got) != 4 {
		t.Fatalf("default: expected four fillers displaced, got %v", got)
	}
}

// Room for a 4-GPU move is made per GPU, not per pod: one 4-GPU stateless
// filler (4 x (30 + 300) = 1320 GPU-seconds) beats four 1-GPU fillers with
// a 1000 s startup (1030 each, 4120 together), so the gated plan displaces
// what the ungated one does.
func TestMakeRoomComparesCostPerGpu(t *testing.T) {
	plan := func(conf *capacityUpgradeConf) []string {
		f := newFixture(t)
		f.addNode(tierNode("spot-1", "spot", "a"))
		f.addNode(tierNode("reserved-1", "reserved", "a"))
		f.only(t, f.placeGroup(t, 1, nil, tierPod("train", "spot-1", "pg-train", 4, -4, time.Hour)))
		for _, filler := range f.fillers(t, "reserved-1", 1, 4, -9) {
			filler.Pod.Annotations[evictioncost.AnnotationResume] = evictioncost.ResumeStateless
		}
		for _, filler := range f.fillers(t, "reserved-1", 4, 1, -9) {
			filler.Pod.Annotations[evictioncost.AnnotationResume] = evictioncost.ResumeStateless
			filler.Pod.Annotations[evictioncost.AnnotationStartupSeconds] = "1000"
		}
		plans := f.startOnly(t, conf, newMemStore(f))
		if len(plans) != 1 {
			t.Fatalf("expected one plan, got %+v", plans)
		}
		return displacedNames(plans[0])
	}
	want := []string{"filler-reserved-1-4gpu-pri-9-0"}
	if got := plan(displacingConf(-9)); !reflect.DeepEqual(got, want) {
		t.Fatalf("default: got %v, want %v", got, want)
	}
	gated := displacingConf(-9)
	gated.MaxEvictionCostGpuSeconds = 1e9
	if got := plan(gated); !reflect.DeepEqual(got, want) {
		t.Fatalf("gated: got %v, want %v", got, want)
	}
}

// A drain mixing an announced cheap job with an unannounced costly one is
// gated on the announced share alone unless gateUnannounced.
func TestGpuFragmentationGatesAnnouncedShare(t *testing.T) {
	f := newFixture(t)
	source := f.addNode(gpuNode("source", 8, nil))
	dest := f.addNode(gpuNode("dest", 8, nil))
	startedHourAgo(f.placePod(t, source, gpuPod("cheap", "source", 1, eligible(), map[string]string{evictioncost.AnnotationResume: "stateless"}, true), 1, ""))
	startedHourAgo(f.placePod(t, source, gpuPod("quiet", "source", 1, eligible(), nil, true), 1, ""))
	f.placePod(t, dest, gpuPod("resident", "dest", 5, nil, nil, true), 1, "")
	conf := newGpuFragmentationConf()
	conf.MaxEvictionCostGpuSecondsPerGpu = 1000
	if got := f.plan(conf, nil); len(got) != 2 {
		t.Fatalf("expected both pods drained, got %v", sources(got))
	}
	conf.EvictionCost.GateUnannounced = true
	if got := f.plan(conf, nil); len(got) != 0 {
		t.Fatalf("gateUnannounced: expected no drain, got %v", sources(got))
	}
}

// captureGateLines records the gate's log lines for the rest of the test.
func captureGateLines(t *testing.T) *[]string {
	lines := &[]string{}
	saved := logGate
	logGate = func(line func() string) { *lines = append(*lines, line()) }
	t.Cleanup(func() { logGate = saved })
	return lines
}

// expectLines checks that each line holds its fragments, one line per entry.
func expectLines(t *testing.T, what string, lines *[]string, want ...[]string) {
	t.Helper()
	defer func() { *lines = (*lines)[:0] }()
	if len(*lines) != len(want) {
		t.Fatalf("%s: expected %d line(s), got %q", what, len(want), *lines)
	}
	for i, fragments := range want {
		t.Log((*lines)[i])
		for _, fragment := range fragments {
			if !strings.Contains((*lines)[i], fragment) {
				t.Fatalf("%s: line %q lacks %q", what, (*lines)[i], fragment)
			}
		}
	}
}

// Every decision a cap catches is logged once, in both modes, with the jobs
// it evicts; nothing is logged without a cap or for an infeasible drain.
func TestEvictionCostGateLogs(t *testing.T) {
	lines := captureGateLines(t)
	repack := func(params map[string]interface{}) *gpuFragmentationConf {
		c := newGpuFragmentationConf()
		params["maxVictims"] = 1
		c.parse(params)
		return c
	}
	repackFixture(t).plan(repack(map[string]interface{}{}), nil)
	expectLines(t, "repack defaults", lines)

	repackFixture(t).plan(repack(map[string]interface{}{"maxEvictionCostGpuSecondsPerGpu": 1000, "evictionCostDryRun": true}), nil)
	expectLines(t, "repack dry run", lines, []string{
		"evictionCost gate: strategy=gpuFragmentation mode=dry_run source=a-source target=dest outcome=drained unit=gpu_seconds_per_gpu",
		"cap=1000 jobs=[", ", announced)]",
	})

	repackFixture(t).plan(repack(map[string]interface{}{"maxEvictionCostGpuSecondsPerGpu": 100}), nil)
	expectLines(t, "repack live", lines,
		[]string{"mode=live source=b-source target=dest outcome=skipped", "cap=100 "},
		[]string{"mode=live source=a-source target=dest outcome=skipped", "cap=100 "})

	f := newFixture(t)
	source := f.addNode(gpuNode("source", 8, nil))
	full := f.addNode(gpuNode("dest", 8, nil))
	startedHourAgo(f.placePod(t, source, gpuPod("costly", "source", 1, eligible(), map[string]string{evictioncost.AnnotationResume: "scratch"}, true), 1, ""))
	f.placePod(t, full, gpuPod("resident", "dest", 8, nil, nil, true), 1, "")
	infeasible := newGpuFragmentationConf()
	infeasible.MaxEvictionCostGpuSecondsPerGpu = 100
	f.plan(infeasible, nil)
	expectLines(t, "infeasible drain", lines)

	move := func(conf *capacityUpgradeConf) {
		f := newFixture(t)
		f.addNode(tierNode("spot-1", "spot", "a"))
		f.addNode(tierNode("reserved-1", "reserved", "a"))
		mover := f.only(t, f.placeGroup(t, 1, nil, tierPod("train", "spot-1", "pg-train", 4, -4, time.Hour)))
		mover.Pod.Annotations[evictioncost.AnnotationResume] = evictioncost.ResumeScratch
		f.startOnly(t, conf, newMemStore(f))
	}
	move(liveConf())
	expectLines(t, "move defaults", lines)
	capped := liveConf()
	capped.MaxEvictionCostGpuSeconds = 10000
	move(capped)
	expectLines(t, "move live", lines, []string{
		"evictionCost gate: strategy=capacityUpgrade mode=live mover=", ", announced) target=reserved:reserved-1 outcome=skipped unit=gpu_seconds",
		"cap=10000 jobs=[]",
	})
	capped.EvictionCost.DryRun = true
	move(capped)
	expectLines(t, "move dry run", lines, []string{"mode=dry_run", "target=reserved:reserved-1 outcome=moved "})

	// Over the cap on the cheapest tier, moved to the next one.
	f = newFixture(t)
	f.addNode(tierNode("ondemand-1", "on-demand", "a"))
	f.addNode(tierNode("spot-1", "spot", "a"))
	f.addNode(tierNode("reserved-1", "reserved", "a"))
	f.only(t, f.placeGroup(t, 1, nil, tierPod("train", "ondemand-1", "pg-train", 4, -4, time.Hour)))
	for _, filler := range f.fillers(t, "reserved-1", 8, 1, -9) {
		filler.Pod.Annotations[evictioncost.AnnotationResume] = evictioncost.ResumeScratch
	}
	tiered := displacingConf(-9)
	tiered.MaxEvictionCostGpuSeconds = 10000
	f.startOnly(t, tiered, newMemStore(f))
	expectLines(t, "next tier", lines, []string{
		"target=reserved:reserved-1 outcome=moved_to=spot:spot-1", ", announced) ",
	})
}
