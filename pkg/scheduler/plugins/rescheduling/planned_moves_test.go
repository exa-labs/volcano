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
// session, in both strategy orders, against fake API clients. A second
// scenario, a move that displaces fillers, checks that capacityUpgrade
// records the pods it displaces and the pods its maintenance evicts.

import (
	"context"
	"fmt"
	"sort"
	"strings"
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

// passCluster is the cluster one rescheduling session runs against.
type passCluster struct {
	nodes  []*v1.Node
	pods   []*v1.Pod
	groups []*schedulingv1beta1.PodGroup
}

// passResult is what one rescheduling session leaves behind: its victims,
// the planned set as it stood before the session closed, and the Nodes and
// PodGroups as the strategies left them in the API.
type passResult struct {
	victims []string
	planned plannedMoves
	nodes   []*v1.Node
	groups  []*schedulingv1beta1.PodGroup
}

// holdsOn decodes the holds the pass left on the named node.
func (r passResult) holdsOn(t *testing.T, name string) []capacityHold {
	t.Helper()
	for _, node := range r.nodes {
		if node.Name != name {
			continue
		}
		raw, ok := node.Annotations[CapacityUpgradeHoldsAnnotation]
		if !ok {
			return nil
		}
		holds, err := decodeHolds(raw)
		if err != nil {
			t.Fatalf("decode holds on %s: %v", name, err)
		}
		return holds
	}
	t.Fatalf("node %s not in the pass result", name)
	return nil
}

// runPass runs one rescheduling session over the cluster with the given
// strategies configured in order. With plan set the strategies plan in
// this session; without it only the hooks that run in every session do
// (capacityUpgrade's move maintenance).
func runPass(t *testing.T, cluster passCluster, params map[string]map[string]interface{}, plan bool, strategies ...string) passResult {
	t.Helper()
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

	test := &uthelper.TestCommonStruct{
		Plugins:   map[string]framework.PluginBuilder{PluginName: New},
		Nodes:     cluster.nodes,
		Pods:      cluster.pods,
		PodGroups: cluster.groups,
		Queues:    []*schedulingv1beta1.Queue{util.BuildQueue("default", 1, nil)},
	}
	lastRescheduleTime = time.Time{}
	if !plan {
		lastRescheduleTime = time.Now()
	}
	ssn := test.RegisterSession(tiers, nil)
	defer test.Close()

	// The strategies patch Nodes and PodGroups, so the fake clients need
	// them as well as the scheduler cache.
	for _, node := range cluster.nodes {
		if _, err := ssn.KubeClient().CoreV1().Nodes().Create(context.Background(), node, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create node %s: %v", node.Name, err)
		}
	}
	for _, pg := range cluster.groups {
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
	if len(tasks) != len(cluster.pods) {
		t.Fatalf("expected %d running tasks in the session, got %d", len(cluster.pods), len(tasks))
	}
	var result passResult
	for victim := range ssn.VictimTasks(tasks) {
		result.victims = append(result.victims, victim.Name)
	}
	result.planned = plannedMoves{}
	for uid := range sessionPlannedMoves {
		result.planned[uid] = true
	}

	for _, node := range cluster.nodes {
		got, err := ssn.KubeClient().CoreV1().Nodes().Get(context.Background(), node.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get node %s: %v", node.Name, err)
		}
		result.nodes = append(result.nodes, got)
	}
	for _, pg := range cluster.groups {
		got, err := ssn.VCClient().SchedulingV1beta1().PodGroups(pg.Namespace).Get(context.Background(), pg.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get podgroup %s: %v", pg.Name, err)
		}
		result.groups = append(result.groups, got)
	}
	return result
}

// onePass runs one planning session of the contested scenario with the
// given strategies, in order, both live, and returns the victims and the
// holds written onto the reserved node.
func onePass(t *testing.T, strategies ...string) (victims []string, holds []capacityHold) {
	t.Helper()
	params := map[string]map[string]interface{}{
		GpuFragmentationStrategy: {"dryRun": false},
		CapacityUpgradeStrategy:  {"dryRun": false},
	}
	cluster := passCluster{
		nodes: []*v1.Node{tierNode("spot", "spot", "a").Node, tierNode("reserved", "reserved", "a").Node},
		pods:  contestedPods(),
		groups: []*schedulingv1beta1.PodGroup{
			util.BuildPodGroup("pg-train", "default", "default", 1, nil, schedulingv1beta1.PodGroupRunning),
			util.BuildPodGroup("pg-resident", "default", "default", 1, nil, schedulingv1beta1.PodGroupRunning),
		},
	}
	result := runPass(t, cluster, params, true, strategies...)
	return result.victims, result.holdsOn(t, "reserved")
}

// displacingCluster is a mover on a spot node and a reserved node whose
// eight GPUs each run a priority -9 filler, so capacityUpgrade with
// maxDisplacedPriority -9 can only move the mover by displacing fillers.
func displacingCluster() passCluster {
	cluster := passCluster{
		nodes: []*v1.Node{tierNode("spot-1", "spot", "a").Node, tierNode("reserved-1", "reserved", "a").Node},
		pods:  []*v1.Pod{tierPod("train", "spot-1", "pg-train", 4, -4, time.Hour)},
		groups: []*schedulingv1beta1.PodGroup{
			util.BuildPodGroup("pg-train", "default", "default", 1, nil, schedulingv1beta1.PodGroupRunning),
		},
	}
	for i := 0; i < 8; i++ {
		name := fmt.Sprintf("filler-%d", i)
		cluster.pods = append(cluster.pods, tierPod(name, "reserved-1", "pg-"+name, 1, -9, time.Hour))
		cluster.groups = append(cluster.groups, util.BuildPodGroup("pg-"+name, "default", "default", 1, nil, schedulingv1beta1.PodGroupRunning))
	}
	return cluster
}

// plannedNames lists the pods of the cluster in the planned set, sorted.
func plannedNames(cluster passCluster, planned plannedMoves) []string {
	out := make([]string, 0)
	for _, pod := range cluster.pods {
		if planned[pod.UID] {
			out = append(out, pod.Name)
		}
	}
	sort.Strings(out)
	return out
}

// A move capacityUpgrade starts records its mover and the pods it will
// displace, and the session that then evicts the displaced pods records
// them too, so no strategy later in either pass plans any of them.
func TestCapacityUpgradeRecordsDisplacedAndEvictedPods(t *testing.T) {
	params := map[string]map[string]interface{}{
		CapacityUpgradeStrategy: {"dryRun": false, "maxDisplacedPriority": -9},
	}
	cluster := displacingCluster()

	started := runPass(t, cluster, params, true, CapacityUpgradeStrategy)
	if holds := started.holdsOn(t, "reserved-1"); len(holds) != 1 || holds[0].Group != "default/pg-train" {
		t.Fatalf("expected a hold on reserved-1 for pg-train, got %+v", holds)
	}
	if len(started.victims) != 0 {
		t.Fatalf("expected nothing evicted while the move starts, got %v", started.victims)
	}
	planned := plannedNames(cluster, started.planned)
	if len(planned) != 5 || planned[len(planned)-1] != "train" {
		t.Fatalf("expected train and the 4 fillers it displaces planned, got %v", planned)
	}

	// The next session evicts the displaced fillers; they are in the
	// planned set of that session as well.
	next := runPass(t, passCluster{nodes: started.nodes, pods: cluster.pods, groups: started.groups}, params, false, CapacityUpgradeStrategy)
	sort.Strings(next.victims)
	if len(next.victims) != 4 || !strings.HasPrefix(next.victims[0], "filler-") {
		t.Fatalf("expected the 4 displaced fillers evicted, got %v", next.victims)
	}
	if got := plannedNames(cluster, next.planned); strings.Join(got, ",") != strings.Join(next.victims, ",") {
		t.Fatalf("expected the evicted fillers %v planned, got %v", next.victims, got)
	}
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
