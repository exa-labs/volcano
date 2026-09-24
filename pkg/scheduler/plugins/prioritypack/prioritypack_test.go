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

package prioritypack

import (
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"

	"volcano.sh/volcano/pkg/scheduler/actions/allocate"
	"volcano.sh/volcano/pkg/scheduler/actions/preempt"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/binpack"
	"volcano.sh/volcano/pkg/scheduler/plugins/conformance"
	"volcano.sh/volcano/pkg/scheduler/plugins/gang"
	"volcano.sh/volcano/pkg/scheduler/plugins/predicates"
	"volcano.sh/volcano/pkg/scheduler/plugins/priority"
	"volcano.sh/volcano/pkg/scheduler/plugins/proportion"
	"volcano.sh/volcano/pkg/scheduler/uthelper"
	"volcano.sh/volcano/pkg/scheduler/util"
)

const gpu = v1.ResourceName("nvidia.com/gpu")

func gpuNode(name string, gpus int) *api.NodeInfo {
	return api.NewNodeInfo(util.BuildNode(name, v1.ResourceList{
		"cpu":  resource.MustParse("64"),
		"pods": resource.MustParse("110"),
		gpu:    resource.MustParse(fmt.Sprint(gpus)),
	}, nil))
}

func gpuTask(name string, gpus int, prio int32, status api.TaskStatus) *api.TaskInfo {
	req := v1.ResourceList{"cpu": resource.MustParse("1")}
	if gpus > 0 {
		req[gpu] = resource.MustParse(fmt.Sprint(gpus))
	}
	task := api.NewTaskInfo(util.BuildPod("default", name, "", v1.PodPending, req, "pg", nil, nil))
	task.Priority = prio
	task.Status = status
	return task
}

func place(t *testing.T, node *api.NodeInfo, tasks ...*api.TaskInfo) {
	for _, task := range tasks {
		if err := node.AddTask(task); err != nil {
			t.Fatalf("AddTask(%s): %v", task.Name, err)
		}
	}
}

func TestCostRisesWithCeilingGapTimesHeldResource(t *testing.T) {
	empty := gpuNode("empty", 8)
	low := gpuNode("low", 8)
	place(t, low,
		gpuTask("l1", 1, -9, api.Running),
		gpuTask("l2", 1, -9, api.Running),
		gpuTask("l3", 2, -8, api.Running),
	)
	mixed := gpuNode("mixed", 8)
	place(t, mixed,
		gpuTask("m1", 4, -2, api.Running),
		gpuTask("m2", 1, -9, api.Running),
	)

	cases := []struct {
		node *api.NodeInfo
		prio int32
		want float64
	}{
		{empty, -2, 0},
		{low, -9, 0},
		{low, -8, 0},
		{low, -2, 6 * 4 * milli},
		{low, 0, 8 * 4 * milli},
		{mixed, -2, 0},
		{mixed, -1, 1 * 5 * milli},
		{mixed, -9, 0},
	}
	for _, c := range cases {
		if got := occupancyOf(c.node, gpu).cost(c.prio); got != c.want {
			t.Errorf("cost(%s, %d) = %v, want %v", c.node.Name, c.prio, got, c.want)
		}
	}
}

func TestOccupancyIgnoresReleasingAndNonResourceTasks(t *testing.T) {
	node := gpuNode("n", 8)
	place(t, node,
		gpuTask("releasing", 2, 0, api.Releasing),
		gpuTask("cpu-only", 0, 0, api.Running),
		gpuTask("pipelined", 1, -3, api.Pipelined),
		gpuTask("running", 1, -9, api.Running),
	)
	o := occupancyOf(node, gpu)
	if o.empty || o.ceiling != -3 || o.lowest != -9 || o.held != 2*milli {
		t.Fatalf("occupancy = %+v, want ceiling -3 lowest -9 held %v", o, 2*milli)
	}
	if got := capped(node, gpu, o.ceiling); got != 1*milli {
		t.Fatalf("capped = %v, want %v", got, 1*milli)
	}
	if o := occupancyOf(gpuNode("bare", 8), gpu); !o.empty {
		t.Fatalf("empty node reported occupied: %+v", o)
	}
}

func TestRankNormalizesPerBatch(t *testing.T) {
	got := rank(map[string]float64{"a": 0, "b": 25, "c": 100})
	if got["a"] != 100 || got["b"] != 75 || got["c"] != 0 {
		t.Fatalf("rank = %v", got)
	}
	flat := rank(map[string]float64{"a": 0, "b": 0})
	if flat["a"] != 0 || flat["b"] != 0 {
		t.Fatalf("all-zero batch must score 0, got %v", flat)
	}
	if len(rank(nil)) != 0 {
		t.Fatalf("empty batch must rank empty")
	}
}

func TestPluginScoresOnlyResourceTasksAndAppliesWeight(t *testing.T) {
	plugin := New(framework.Arguments{WeightArg: 3}).(*priorityPackPlugin)
	if plugin.weight != 3 || plugin.resource != gpu {
		t.Fatalf("plugin = %+v", plugin)
	}
	if plugin.scores(gpuTask("cpu", 0, -2, api.Pending)) {
		t.Fatalf("cpu-only task must not be scored")
	}
	clean := gpuNode("clean", 8)
	place(t, clean, gpuTask("c1", 4, -2, api.Running))
	dirty := gpuNode("dirty", 8)
	place(t, dirty, gpuTask("d1", 4, -9, api.Running))
	scores := plugin.batchScores(gpuTask("p", 1, -2, api.Pending), []*api.NodeInfo{clean, dirty})
	if scores["clean"] != 300 || scores["dirty"] != 0 {
		t.Fatalf("scores = %v, want clean 300 dirty 0", scores)
	}
}

// Scheduling scenarios: a "high" (diskann-like) pod arriving into a cluster
// that "low" (pythia-like) pods keep full must join the node whose ceiling is
// already high, in allocate and in preempt alike, even when binpack prefers
// the fuller pure-low node.
func schedulingTiers(withPriorityPack bool) []conf.Tier {
	trueValue := true
	plugins := []conf.PluginOption{
		{Name: conformance.PluginName, EnabledPreemptable: &trueValue},
		{
			Name:                gang.PluginName,
			EnabledJobReady:     &trueValue,
			EnabledJobPipelined: &trueValue,
			EnabledJobStarving:  &trueValue,
		},
		{
			Name:                priority.PluginName,
			EnabledTaskOrder:    &trueValue,
			EnabledJobOrder:     &trueValue,
			EnabledPreemptable:  &trueValue,
			EnabledJobPipelined: &trueValue,
			EnabledJobStarving:  &trueValue,
		},
		{
			Name:             binpack.PluginName,
			EnabledNodeOrder: &trueValue,
			Arguments: map[string]interface{}{
				binpack.BinpackWeight:              10,
				binpack.BinpackResources:           string(gpu),
				"binpack.resources.nvidia.com/gpu": 10,
			},
		},
	}
	if withPriorityPack {
		plugins = append(plugins, conf.PluginOption{
			Name:             PluginName,
			EnabledNodeOrder: &trueValue,
			Arguments:        map[string]interface{}{WeightArg: 10},
		})
	}
	plugins = append(plugins,
		conf.PluginOption{
			Name:               proportion.PluginName,
			EnabledOverused:    &trueValue,
			EnabledAllocatable: &trueValue,
			EnabledQueueOrder:  &trueValue,
			EnabledPredicate:   &trueValue,
		},
		conf.PluginOption{
			Name:               predicates.PluginName,
			EnabledPreemptable: &trueValue,
			EnabledPredicate:   &trueValue,
		},
	)
	return []conf.Tier{{Plugins: plugins}}
}

var schedulingPlugins = map[string]framework.PluginBuilder{
	conformance.PluginName: conformance.New,
	gang.PluginName:        gang.New,
	priority.PluginName:    priority.New,
	binpack.PluginName:     binpack.New,
	PluginName:             New,
	proportion.PluginName:  proportion.New,
	predicates.PluginName:  predicates.New,
}

var (
	highPrio = util.BuildPriorityClass("high", 100)
	lowPrio  = util.BuildPriorityClass("low", 10)
)

func gpuPod(name, node, pg string, prio *schedulingv1.PriorityClass, phase v1.PodPhase, preemptable bool) *v1.Pod {
	labels := map[string]string{schedulingv1beta1.PodPreemptable: fmt.Sprint(preemptable)}
	return util.BuildPodWithPriority("c1", name, node, phase,
		api.BuildResourceList("1", "1G", api.ScalarResource{Name: string(gpu), Value: "1"}),
		pg, labels, nil, &prio.Value)
}

func clusterNode(name string) *v1.Node {
	return util.BuildNode(name, api.BuildResourceList("64", "256G",
		api.ScalarResource{Name: "pods", Value: "110"},
		api.ScalarResource{Name: string(gpu), Value: "8"}), nil)
}

// lowPods returns n running low-priority pods on node, each in its own
// PodGroup; the first preemptable of them are valid victims.
func lowPods(node string, n, preemptable int) ([]*v1.Pod, []*schedulingv1beta1.PodGroup) {
	pods := make([]*v1.Pod, 0, n)
	groups := make([]*schedulingv1beta1.PodGroup, 0, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("%s-low-%d", node, i)
		pods = append(pods, gpuPod(name, node, "pg-"+name, lowPrio, v1.PodRunning, i < preemptable))
		groups = append(groups, util.BuildPodGroupWithPrio("pg-"+name, "c1", "q1", 1, nil, schedulingv1beta1.PodGroupRunning, "low"))
	}
	return pods, groups
}

func runScenario(t *testing.T, test *uthelper.TestCommonStruct, withPriorityPack bool, actions ...framework.Action) {
	test.Plugins = schedulingPlugins
	test.PriClass = []*schedulingv1.PriorityClass{highPrio, lowPrio}
	test.RegisterSession(schedulingTiers(withPriorityPack), []conf.Configuration{{
		Name: "preempt",
		Arguments: map[string]interface{}{
			preempt.EnableTopologyAwarePreemptionKey: true,
			preempt.MinCandidateNodesAbsoluteKey:     2,
			preempt.MaxCandidateNodesAbsoluteKey:     2,
		},
	}})
	defer test.Close()
	test.Run(actions)
	if err := test.CheckAll(0); err != nil {
		t.Fatal(err)
	}
}

// allocateScenario: n1 has 6 low pods (2 idle GPUs), n2 has 1 high + 4 low
// (3 idle). Binpack prefers the fuller n1; prioritypack prefers n2 whose
// ceiling is already high.
func allocateScenario() *uthelper.TestCommonStruct {
	n1Pods, n1Groups := lowPods("n1", 6, 6)
	n2Pods, n2Groups := lowPods("n2", 4, 4)
	pods := append(n1Pods, n2Pods...)
	pods = append(pods,
		gpuPod("n2-high", "n2", "pg-n2-high", highPrio, v1.PodRunning, true),
		gpuPod("arrival", "", "pg-arrival", highPrio, v1.PodPending, true),
	)
	groups := append(n1Groups, n2Groups...)
	groups = append(groups,
		util.BuildPodGroupWithPrio("pg-n2-high", "c1", "q1", 1, nil, schedulingv1beta1.PodGroupRunning, "high"),
		util.BuildPodGroupWithPrio("pg-arrival", "c1", "q1", 1, nil, schedulingv1beta1.PodGroupInqueue, "high"),
	)
	return &uthelper.TestCommonStruct{
		Pods:      pods,
		PodGroups: groups,
		Nodes:     []*v1.Node{clusterNode("n1"), clusterNode("n2")},
		Queues:    []*schedulingv1beta1.Queue{util.BuildQueue("q1", 1, nil)},
	}
}

func TestAllocateJoinsTheNodeWhoseCeilingIsAlreadyHigh(t *testing.T) {
	test := allocateScenario()
	test.ExpectBindsNum = 1
	test.ExpectBindMap = map[string]string{"c1/arrival": "n2"}
	runScenario(t, test, true, allocate.New())
}

func TestAllocateWithoutPriorityPackFollowsBinpack(t *testing.T) {
	test := allocateScenario()
	test.ExpectBindsNum = 1
	test.ExpectBindMap = map[string]string{"c1/arrival": "n1"}
	runScenario(t, test, false, allocate.New())
}

// preemptScenario: both nodes full; n1 is 8 low (all preemptable), n2 is
// 1 high + 7 low (one preemptable, so the victim is nameable). A high
// arrival must evict on n2, keeping n1 whole-node preemptable.
func preemptScenario() *uthelper.TestCommonStruct {
	n1Pods, n1Groups := lowPods("n1", 8, 8)
	n2Pods, n2Groups := lowPods("n2", 7, 1)
	pods := append(n1Pods, n2Pods...)
	pods = append(pods,
		gpuPod("n2-high", "n2", "pg-n2-high", highPrio, v1.PodRunning, false),
		gpuPod("arrival", "", "pg-arrival", highPrio, v1.PodPending, true),
	)
	groups := append(n1Groups, n2Groups...)
	groups = append(groups,
		util.BuildPodGroupWithPrio("pg-n2-high", "c1", "q1", 1, nil, schedulingv1beta1.PodGroupRunning, "high"),
		util.BuildPodGroupWithPrio("pg-arrival", "c1", "q1", 1, nil, schedulingv1beta1.PodGroupInqueue, "high"),
	)
	return &uthelper.TestCommonStruct{
		Pods:      pods,
		PodGroups: groups,
		Nodes:     []*v1.Node{clusterNode("n1"), clusterNode("n2")},
		Queues:    []*schedulingv1beta1.Queue{util.BuildQueue("q1", 1, nil)},
	}
}

func TestPreemptEvictsOnTheNodeWhoseCeilingIsAlreadyHigh(t *testing.T) {
	test := preemptScenario()
	test.ExpectEvictNum = 1
	test.ExpectEvicted = []string{"c1/n2-low-0"}
	test.ExpectPipeLined = map[string][]string{"c1/pg-arrival": {"n2"}}
	runScenario(t, test, true, preempt.New())
}
