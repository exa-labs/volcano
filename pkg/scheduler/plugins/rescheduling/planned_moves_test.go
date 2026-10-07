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

// Tests for the one-plan-per-pod rule across strategies (planned_moves.go).
// The scenario is a pod both strategies want: alone on a spot node, so
// gpuFragmentation drains it onto a fuller reserved node, and on capacity
// costlier than that reserved node, so capacityUpgrade moves it there. The
// session tests run the rescheduling plugin's victim functions in a real
// session, in both strategy orders, against fake API clients.

import (
	"context"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/uthelper"
	"volcano.sh/volcano/pkg/scheduler/util"
)

// contestedPods are the pods of the scenario: "train" (one GPU, priority
// -4) alone on the spot node and a resident (four GPUs, priority 0) on the
// reserved node, which makes the reserved node the fuller one.
func contestedPods() []*v1.Pod {
	return []*v1.Pod{
		tierPod("train", "spot", "pg-train", 1, -4, time.Hour),
		tierPod("resident", "reserved", "pg-resident", 4, 0, time.Hour),
	}
}

// contestedFixture is the scenario as a planner fixture.
func contestedFixture(t *testing.T) *fixture {
	f := newFixture(t)
	f.addNode(tierNode("spot", "spot", "a"))
	f.addNode(tierNode("reserved", "reserved", "a"))
	for _, pod := range contestedPods() {
		f.placeGroup(t, 1, nil, pod)
	}
	return f
}

// tasksOf lists the fixture's running tasks, as shuffle hands them to the
// victim functions.
func (f *fixture) tasksOf() []*api.TaskInfo {
	tasks := make([]*api.TaskInfo, 0, len(f.running))
	for _, task := range f.running {
		tasks = append(tasks, task)
	}
	return tasks
}

func TestRunningTasksLeavesOutPlannedPods(t *testing.T) {
	f := contestedFixture(t)
	train := f.only(t, f.jobs["default/pg-train"])
	if got := runningTasks(f.tasksOf(), nil); len(got) != 2 {
		t.Fatalf("expected every running task without a planned set, got %d", len(got))
	}
	planned := plannedMoves{}
	planned.record(train, nil, &api.TaskInfo{})
	got := runningTasks(f.tasksOf(), planned)
	if _, ok := got[train.Pod.UID]; ok || len(got) != 1 {
		t.Fatalf("expected the planned pod left out, got %d tasks", len(got))
	}
}

// Both planners want the pod on their own; once one has planned it, the
// other, planning with the running tasks that leave it out, does not.
func TestPlannersSkipAPodTheOtherStrategyPlanned(t *testing.T) {
	f := contestedFixture(t)
	repack := newGpuFragmentationConf()
	repack.DryRun = false

	drains := planGpuFragmentationDrains(f.nodes, f.jobs, runningTasks(f.tasksOf(), nil), repack, testNow, nil)
	if len(drains) != 1 || len(drains[0].moves) != 1 || drains[0].moves[0].victim.Name != "train" {
		t.Fatalf("expected gpuFragmentation to drain train, got %+v", drains)
	}
	upgrades := planCapacityUpgrades(f.nodes, f.jobs, runningTasks(f.tasksOf(), nil), liveConf(), f.index(), testNow, nil, nil)
	if len(upgrades) != 1 || upgrades[0].members[0].Name != "train" {
		t.Fatalf("expected capacityUpgrade to move train, got %+v", upgrades)
	}

	byRepack := plannedMoves{}
	byRepack.record(drains[0].moves[0].victim)
	if plans := planCapacityUpgrades(f.nodes, f.jobs, runningTasks(f.tasksOf(), byRepack), liveConf(), f.index(), testNow, nil, nil); len(plans) != 0 {
		t.Fatalf("expected capacityUpgrade to leave a pod gpuFragmentation planned, got %+v", plans)
	}
	byUpgrade := plannedMoves{}
	byUpgrade.record(upgrades[0].members...)
	if drains := planGpuFragmentationDrains(f.nodes, f.jobs, runningTasks(f.tasksOf(), byUpgrade), repack, testNow, nil); len(drains) != 0 {
		t.Fatalf("expected gpuFragmentation to leave a pod capacityUpgrade planned, got %+v", drains)
	}
}

// onePass runs one rescheduling session with the given strategies, in
// order, both live, and returns the victims and the holds written onto the
// reserved node.
func onePass(t *testing.T, strategies ...string) (victims []string, holds []capacityHold) {
	t.Helper()
	params := map[string]map[string]interface{}{
		GpuFragmentationStrategy: {"dryRun": false},
		CapacityUpgradeStrategy:  {"dryRun": false},
	}
	configured := make([]interface{}, 0, len(strategies))
	for _, name := range strategies {
		configured = append(configured, map[string]interface{}{"name": name, "params": params[name]})
	}
	enabled := true
	tiers := []conf.Tier{{Plugins: []conf.PluginOption{{
		Name:          PluginName,
		EnabledVictim: &enabled,
		Arguments:     framework.Arguments{"interval": "5m", "strategies": configured},
	}}}}

	nodes := []*v1.Node{tierNode("spot", "spot", "a").Node, tierNode("reserved", "reserved", "a").Node}
	pods := contestedPods()
	groups := []*schedulingv1beta1.PodGroup{
		util.BuildPodGroup("pg-train", "default", "default", 1, nil, schedulingv1beta1.PodGroupRunning),
		util.BuildPodGroup("pg-resident", "default", "default", 1, nil, schedulingv1beta1.PodGroupRunning),
	}
	test := &uthelper.TestCommonStruct{
		Plugins:   map[string]framework.PluginBuilder{PluginName: New},
		Nodes:     nodes,
		Pods:      pods,
		PodGroups: groups,
		Queues:    []*schedulingv1beta1.Queue{util.BuildQueue("default", 1, nil)},
	}
	lastRescheduleTime = time.Time{}
	ssn := test.RegisterSession(tiers, nil)
	defer test.Close()

	// The strategies patch Nodes and PodGroups, so the fake clients need
	// them as well as the scheduler cache.
	for _, node := range nodes {
		if _, err := ssn.KubeClient().CoreV1().Nodes().Create(context.Background(), node, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create node %s: %v", node.Name, err)
		}
	}
	for _, pg := range groups {
		if _, err := ssn.VCClient().SchedulingV1beta1().PodGroups(pg.Namespace).Create(context.Background(), pg, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create podgroup %s: %v", pg.Name, err)
		}
	}

	tasks := make([]*api.TaskInfo, 0)
	for _, job := range ssn.Jobs {
		for _, task := range job.Tasks {
			if task.Status == api.Running {
				tasks = append(tasks, task)
			}
		}
	}
	if len(tasks) != len(pods) {
		t.Fatalf("expected %d running tasks in the session, got %d", len(pods), len(tasks))
	}
	for victim := range ssn.VictimTasks(tasks) {
		victims = append(victims, victim.Name)
	}

	reserved, err := ssn.KubeClient().CoreV1().Nodes().Get(context.Background(), "reserved", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get node reserved: %v", err)
	}
	if raw, ok := reserved.Annotations[CapacityUpgradeHoldsAnnotation]; ok {
		if holds, err = decodeHolds(raw); err != nil {
			t.Fatalf("decode holds: %v", err)
		}
	}
	return victims, holds
}

// gpuFragmentation runs first and evicts the pod; capacityUpgrade must not
// then hold capacity for it.
func TestOnePassCapacityUpgradeSkipsAPodGpuFragmentationEvicts(t *testing.T) {
	victims, holds := onePass(t, GpuFragmentationStrategy, CapacityUpgradeStrategy)
	if len(victims) != 1 || victims[0] != "train" {
		t.Fatalf("expected gpuFragmentation to evict train, got %v", victims)
	}
	if len(holds) != 0 {
		t.Fatalf("expected no hold for a pod already evicted this pass, got %+v", holds)
	}
}

// capacityUpgrade runs first and holds capacity for the pod; gpuFragmentation
// must not then evict it from under the hold.
func TestOnePassGpuFragmentationSkipsAPodCapacityUpgradeHolds(t *testing.T) {
	victims, holds := onePass(t, CapacityUpgradeStrategy, GpuFragmentationStrategy)
	if len(holds) != 1 || holds[0].Group != "default/pg-train" {
		t.Fatalf("expected capacityUpgrade to hold the reserved node for pg-train, got %+v", holds)
	}
	if len(victims) != 0 {
		t.Fatalf("expected no eviction of a pod capacityUpgrade holds for, got %v", victims)
	}
}

// Each session starts with no pod planned.
func TestPlannedMovesResetWhenTheSessionCloses(t *testing.T) {
	onePass(t, GpuFragmentationStrategy, CapacityUpgradeStrategy)
	if len(sessionPlannedMoves) != 0 {
		t.Fatalf("expected the planned set cleared on session close, got %v", sessionPlannedMoves)
	}
}
