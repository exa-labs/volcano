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
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/util"
)

func apiResource(value string) *resource.Quantity {
	q := resource.MustParse(value)
	return &q
}

const testPool = "b200-pool"

func gpuNode(name string, allocatableGPUs int64, annotations map[string]string) *api.NodeInfo {
	node := util.BuildNode(name, v1.ResourceList{
		"cpu":            *apiResource("64"),
		"memory":         *apiResource("512Gi"),
		"nvidia.com/gpu": *apiResource(fmt.Sprintf("%d", allocatableGPUs)),
		"pods":           *apiResource("110"),
	}, map[string]string{"karpenter.sh/nodepool": testPool})
	node.Annotations = annotations
	return api.NewNodeInfo(node)
}

func gpuPod(name, nodeName string, gpus int64, labels, annotations map[string]string, controlled bool) *v1.Pod {
	pod := util.BuildPod("default", name, nodeName, v1.PodRunning, v1.ResourceList{
		"cpu":            *apiResource("4"),
		"memory":         *apiResource("16Gi"),
		"nvidia.com/gpu": *apiResource(fmt.Sprintf("%d", gpus)),
	}, "pg-"+name, labels, nil)
	for k, v := range annotations {
		pod.Annotations[k] = v
	}
	if controlled {
		controller := true
		pod.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "owner", UID: types.UID("owner-" + name), Controller: &controller,
		}}
	}
	return pod
}

type fixture struct {
	nodes   map[string]*api.NodeInfo
	jobs    map[api.JobID]*api.JobInfo
	running map[types.UID]*api.TaskInfo
}

func newFixture(t *testing.T) *fixture {
	return &fixture{
		nodes:   map[string]*api.NodeInfo{},
		jobs:    map[api.JobID]*api.JobInfo{},
		running: map[types.UID]*api.TaskInfo{},
	}
}

func (f *fixture) addNode(node *api.NodeInfo) *api.NodeInfo {
	f.nodes[node.Name] = node
	return node
}

func (f *fixture) placePod(t *testing.T, node *api.NodeInfo, pod *v1.Pod, minMember int32, groupEvictions string) *api.TaskInfo {
	task := api.NewTaskInfo(pod)
	if err := node.AddTask(task); err != nil {
		t.Fatalf("AddTask(%s): %v", pod.Name, err)
	}
	job := api.NewJobInfo(task.Job, task)
	pg := &api.PodGroup{PodGroup: scheduling.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "pg-" + pod.Name, Namespace: pod.Namespace},
		Spec:       scheduling.PodGroupSpec{MinMember: minMember, Queue: "default"},
	}}
	if groupEvictions != "" {
		pg.Annotations = map[string]string{groupEvictionAnnotation: groupEvictions}
	}
	job.SetPodGroup(pg)
	f.jobs[task.Job] = job
	f.running[pod.UID] = task
	return task
}

func (f *fixture) plan(conf *gpuFragmentationConf, predicate func(*api.TaskInfo, *api.NodeInfo) error) []gpuFragmentationPlan {
	return planGpuFragmentationMoves(f.nodes, f.jobs, f.running, conf, time.Now(), predicate)
}

func eligible() map[string]string {
	return map[string]string{"exa.ai/repack-eligible": "true"}
}

func TestPlanSelectsSoleEligiblePodWithFullerDestination(t *testing.T) {
	f := newFixture(t)
	source := f.addNode(gpuNode("source", 8, nil))
	dest := f.addNode(gpuNode("dest", 8, nil))
	f.placePod(t, source, gpuPod("victim", "source", 1, eligible(), nil, true), 1, "")
	f.placePod(t, dest, gpuPod("resident", "dest", 3, nil, nil, true), 1, "")

	plans := f.plan(newGpuFragmentationConf(), nil)
	if len(plans) != 1 {
		t.Fatalf("expected 1 plan, got %d", len(plans))
	}
	if plans[0].source != "source" || plans[0].destination != "dest" || plans[0].victim.Name != "victim" {
		t.Fatalf("unexpected plan: %+v", plans[0])
	}
}

func TestPlanSkipsWithoutOptIn(t *testing.T) {
	f := newFixture(t)
	source := f.addNode(gpuNode("source", 8, nil))
	dest := f.addNode(gpuNode("dest", 8, nil))
	f.placePod(t, source, gpuPod("victim", "source", 1, nil, nil, true), 1, "")
	f.placePod(t, dest, gpuPod("resident", "dest", 3, nil, nil, true), 1, "")

	if plans := f.plan(newGpuFragmentationConf(), nil); len(plans) != 0 {
		t.Fatalf("expected no plans without opt-in, got %+v", plans)
	}
}

func TestPlanSkipsDoNotDisruptAndUncontrolled(t *testing.T) {
	f := newFixture(t)
	source := f.addNode(gpuNode("source", 8, nil))
	source2 := f.addNode(gpuNode("source2", 8, nil))
	dest := f.addNode(gpuNode("dest", 8, nil))
	f.placePod(t, source, gpuPod("protected", "source", 1, eligible(),
		map[string]string{"karpenter.sh/do-not-disrupt": "true"}, true), 1, "")
	f.placePod(t, source2, gpuPod("bare", "source2", 1, eligible(), nil, false), 1, "")
	f.placePod(t, dest, gpuPod("resident", "dest", 3, nil, nil, true), 1, "")

	if plans := f.plan(newGpuFragmentationConf(), nil); len(plans) != 0 {
		t.Fatalf("expected no plans, got %+v", plans)
	}
}

func TestPlanSkipsMultiMemberGang(t *testing.T) {
	f := newFixture(t)
	source := f.addNode(gpuNode("source", 8, nil))
	dest := f.addNode(gpuNode("dest", 8, nil))
	f.placePod(t, source, gpuPod("victim", "source", 1, eligible(), nil, true), 2, "")
	f.placePod(t, dest, gpuPod("resident", "dest", 3, nil, nil, true), 1, "")

	if plans := f.plan(newGpuFragmentationConf(), nil); len(plans) != 0 {
		t.Fatalf("expected no plans for minMember>1, got %+v", plans)
	}
}

func TestPlanSkipsSpentEvictionCap(t *testing.T) {
	f := newFixture(t)
	source := f.addNode(gpuNode("source", 8, nil))
	dest := f.addNode(gpuNode("dest", 8, nil))
	f.placePod(t, source, gpuPod("victim", "source", 1, eligible(), nil, true), 1, "1")
	f.placePod(t, dest, gpuPod("resident", "dest", 3, nil, nil, true), 1, "")

	if plans := f.plan(newGpuFragmentationConf(), nil); len(plans) != 0 {
		t.Fatalf("expected no plans with spent cap, got %+v", plans)
	}
}

func TestPlanCooldownHoldsPool(t *testing.T) {
	recent := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	for _, annotation := range []string{recent, "not-a-timestamp"} {
		f := newFixture(t)
		source := f.addNode(gpuNode("source", 8, map[string]string{lastEvictionAnnotation: annotation}))
		dest := f.addNode(gpuNode("dest", 8, nil))
		f.placePod(t, source, gpuPod("victim", "source", 1, eligible(), nil, true), 1, "")
		f.placePod(t, dest, gpuPod("resident", "dest", 3, nil, nil, true), 1, "")

		if plans := f.plan(newGpuFragmentationConf(), nil); len(plans) != 0 {
			t.Fatalf("expected cooldown (%q) to hold pool, got %+v", annotation, plans)
		}
	}
}

func TestPlanExpiredCooldownAllowsMove(t *testing.T) {
	old := time.Now().Add(-2 * time.Hour).UTC().Format(time.RFC3339)
	f := newFixture(t)
	source := f.addNode(gpuNode("source", 8, map[string]string{lastEvictionAnnotation: old}))
	dest := f.addNode(gpuNode("dest", 8, nil))
	f.placePod(t, source, gpuPod("victim", "source", 1, eligible(), nil, true), 1, "")
	f.placePod(t, dest, gpuPod("resident", "dest", 3, nil, nil, true), 1, "")

	if plans := f.plan(newGpuFragmentationConf(), nil); len(plans) != 1 {
		t.Fatalf("expected 1 plan after cooldown expiry, got %+v", plans)
	}
}

func TestPlanRequiresStrictlyFullerDestinationWithRoom(t *testing.T) {
	f := newFixture(t)
	source := f.addNode(gpuNode("source", 8, nil))
	emptier := f.addNode(gpuNode("emptier", 8, nil))
	full := f.addNode(gpuNode("full", 8, nil))
	f.placePod(t, source, gpuPod("victim", "source", 3, eligible(), nil, true), 1, "")
	f.placePod(t, emptier, gpuPod("small", "emptier", 1, nil, nil, true), 1, "")
	f.placePod(t, full, gpuPod("big", "full", 7, nil, nil, true), 1, "")

	// emptier is less full than source; full has only 1 free GPU for a 3-GPU victim.
	if plans := f.plan(newGpuFragmentationConf(), nil); len(plans) != 0 {
		t.Fatalf("expected no feasible destination, got %+v", plans)
	}
}

func TestPlanSkipsNodeWithSecondGpuPod(t *testing.T) {
	f := newFixture(t)
	source := f.addNode(gpuNode("source", 8, nil))
	dest := f.addNode(gpuNode("dest", 8, nil))
	f.placePod(t, source, gpuPod("victim", "source", 1, eligible(), nil, true), 1, "")
	f.placePod(t, source, gpuPod("neighbor", "source", 1, nil, nil, true), 1, "")
	f.placePod(t, dest, gpuPod("resident", "dest", 3, nil, nil, true), 1, "")

	if plans := f.plan(newGpuFragmentationConf(), nil); len(plans) != 0 {
		t.Fatalf("expected no plans with a second GPU pod on source, got %+v", plans)
	}
}

func TestPlanRespectsPredicateVeto(t *testing.T) {
	f := newFixture(t)
	source := f.addNode(gpuNode("source", 8, nil))
	dest := f.addNode(gpuNode("dest", 8, nil))
	f.placePod(t, source, gpuPod("victim", "source", 1, eligible(), nil, true), 1, "")
	f.placePod(t, dest, gpuPod("resident", "dest", 3, nil, nil, true), 1, "")

	veto := func(*api.TaskInfo, *api.NodeInfo) error { return fmt.Errorf("no") }
	if plans := f.plan(newGpuFragmentationConf(), veto); len(plans) != 0 {
		t.Fatalf("expected predicate veto to block plan, got %+v", plans)
	}
}

func TestPlanOneVictimAcrossPools(t *testing.T) {
	f := newFixture(t)
	for _, pool := range []string{"pool-a", "pool-b"} {
		source := gpuNode(pool+"-source", 8, nil)
		source.Node.Labels["karpenter.sh/nodepool"] = pool
		dest := gpuNode(pool+"-dest", 8, nil)
		dest.Node.Labels["karpenter.sh/nodepool"] = pool
		f.addNode(source)
		f.addNode(dest)
		f.placePod(t, source, gpuPod(pool+"-victim", source.Name, 1, eligible(), nil, true), 1, "")
		f.placePod(t, dest, gpuPod(pool+"-resident", dest.Name, 3, nil, nil, true), 1, "")
	}

	conf := newGpuFragmentationConf()
	if plans := f.plan(conf, nil); len(plans) != 1 {
		t.Fatalf("expected maxVictims=1 to cap plans, got %d", len(plans))
	}
	conf.MaxVictims = 2
	if plans := f.plan(conf, nil); len(plans) != 2 {
		t.Fatalf("expected 2 plans with maxVictims=2, got %d", len(plans))
	}
}

func TestProbeTaskUnbindsPod(t *testing.T) {
	pod := gpuPod("victim", "source", 1, eligible(), nil, true)
	task := api.NewTaskInfo(pod)
	probe := probeTask(task)
	if probe.Pod.Spec.NodeName != "" {
		t.Fatalf("probe pod still bound to %q", probe.Pod.Spec.NodeName)
	}
	if pod.Spec.NodeName != "source" {
		t.Fatalf("original pod mutated")
	}
}
