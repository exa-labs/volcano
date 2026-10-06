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
	return gpuPodOf(name, node, pg, prio, phase, preemptable, 1)
}

// gpuPodOf builds a pod holding gpus GPUs and as many cpus and gigabytes.
func gpuPodOf(name, node, pg string, prio *schedulingv1.PriorityClass, phase v1.PodPhase, preemptable bool, gpus int) *v1.Pod {
	labels := map[string]string{schedulingv1beta1.PodPreemptable: fmt.Sprint(preemptable)}
	return util.BuildPodWithPriority("c1", name, node, phase,
		api.BuildResourceList(fmt.Sprint(gpus), fmt.Sprintf("%dG", gpus), api.ScalarResource{Name: string(gpu), Value: fmt.Sprint(gpus)}),
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
	runScenarioOn(t, test, withPriorityPack, 2, actions...)
}

// runScenarioOn runs the actions with preempt dry-running candidates nodes.
func runScenarioOn(t *testing.T, test *uthelper.TestCommonStruct, withPriorityPack bool, candidates int, actions ...framework.Action) {
	test.Plugins = schedulingPlugins
	test.PriClass = []*schedulingv1.PriorityClass{highPrio, lowPrio}
	test.RegisterSession(schedulingTiers(withPriorityPack), []conf.Configuration{{
		Name: "preempt",
		Arguments: map[string]interface{}{
			preempt.EnableTopologyAwarePreemptionKey: true,
			preempt.MinCandidateNodesAbsoluteKey:     candidates,
			preempt.MaxCandidateNodesAbsoluteKey:     candidates,
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

func TestSessionNodesScoresTheSessionRecordNotTheCopy(t *testing.T) {
	live := gpuNode("n1", 8)
	whole := gpuTask("whole", 8, -4, api.Running)
	place(t, live, whole)
	copied := live.Clone()
	if err := copied.RemoveTask(whole); err != nil {
		t.Fatalf("RemoveTask: %v", err)
	}
	stranger := gpuNode("n2", 8)

	resolved := sessionNodes(map[string]*api.NodeInfo{"n1": live}, []*api.NodeInfo{copied, stranger})
	if resolved[0] != live || resolved[1] != stranger {
		t.Fatalf("sessionNodes = %v, want the session's n1 and the passed n2", resolved)
	}
	if got, want := occupancyOf(resolved[0], gpu).cost(0), float64(4*8*milli); got != want {
		t.Fatalf("cost on the session's node = %v, want %v (the victim counts as held)", got, want)
	}
	if got := occupancyOf(copied, gpu).cost(0); got != 0 {
		t.Fatalf("cost on the copy = %v, want 0", got)
	}
}

// wholeNodeVictimScenario: both nodes full of low work. n1 is one 8-GPU
// pod; n2 is eight 1-GPU pods, one preemptable so the victim is nameable.
// A 1-GPU high arrival must evict that one pod rather than the 8-GPU pod,
// although evicting the 8-GPU pod would leave n1 empty.
func wholeNodeVictimScenario() *uthelper.TestCommonStruct {
	n2Pods, n2Groups := lowPods("n2", 8, 1)
	pods := append(n2Pods,
		gpuPodOf("n1-whole", "n1", "pg-n1-whole", lowPrio, v1.PodRunning, true, 8),
		gpuPod("arrival", "", "pg-arrival", highPrio, v1.PodPending, true),
	)
	groups := append(n2Groups,
		util.BuildPodGroupWithPrio("pg-n1-whole", "c1", "q1", 1, nil, schedulingv1beta1.PodGroupRunning, "low"),
		util.BuildPodGroupWithPrio("pg-arrival", "c1", "q1", 1, nil, schedulingv1beta1.PodGroupInqueue, "high"),
	)
	return &uthelper.TestCommonStruct{
		Pods:      pods,
		PodGroups: groups,
		Nodes:     []*v1.Node{clusterNode("n1"), clusterNode("n2")},
		Queues:    []*schedulingv1beta1.Queue{util.BuildQueue("q1", 1, nil)},
	}
}

func TestPreemptKeepsTheWholeNodeJobOverOneSmallVictim(t *testing.T) {
	test := wholeNodeVictimScenario()
	test.ExpectEvictNum = 1
	test.ExpectEvicted = []string{"c1/n2-low-0"}
	test.ExpectPipeLined = map[string][]string{"c1/pg-arrival": {"n2"}}
	runScenario(t, test, true, preempt.New())
}

func TestPreemptWithoutPriorityPackKeepsTheWholeNodeJob(t *testing.T) {
	test := wholeNodeVictimScenario()
	test.ExpectEvictNum = 1
	test.ExpectEvicted = []string{"c1/n2-low-0"}
	test.ExpectPipeLined = map[string][]string{"c1/pg-arrival": {"n2"}}
	runScenario(t, test, false, preempt.New())
}

// burstScenario: three nodes full of low work but for one idle GPU on n1,
// and four 1-GPU high arrivals. The first takes the idle GPU; the other
// three must preempt on n1 too, whose ceiling the first arrival already
// raised, instead of spreading over n2 and n3. Three of n1's pods are
// preemptable so the victims are nameable; every pod on n2 and n3 is.
func burstScenario() *uthelper.TestCommonStruct {
	n1Pods, n1Groups := lowPods("n1", 7, 3)
	n2Pods, n2Groups := lowPods("n2", 8, 8)
	n3Pods, n3Groups := lowPods("n3", 8, 8)
	pods := append(append(n1Pods, n2Pods...), n3Pods...)
	groups := append(append(n1Groups, n2Groups...), n3Groups...)
	for i := 0; i < 4; i++ {
		name := fmt.Sprintf("arrival-%d", i)
		pods = append(pods, gpuPod(name, "", "pg-"+name, highPrio, v1.PodPending, true))
		groups = append(groups, util.BuildPodGroupWithPrio("pg-"+name, "c1", "q1", 1, nil, schedulingv1beta1.PodGroupInqueue, "high"))
	}
	return &uthelper.TestCommonStruct{
		Pods:      pods,
		PodGroups: groups,
		Nodes:     []*v1.Node{clusterNode("n1"), clusterNode("n2"), clusterNode("n3")},
		Queues:    []*schedulingv1beta1.Queue{util.BuildQueue("q1", 1, nil)},
	}
}

func TestPreemptBurstJoinsTheNodeAnEarlierArrivalRaised(t *testing.T) {
	test := burstScenario()
	test.ExpectEvictNum = 3
	test.ExpectEvicted = []string{"c1/n1-low-0", "c1/n1-low-1", "c1/n1-low-2"}
	test.ExpectPipeLined = map[string][]string{
		"c1/pg-arrival-0": {"n1"},
		"c1/pg-arrival-1": {"n1"},
		"c1/pg-arrival-2": {"n1"},
		"c1/pg-arrival-3": {"n1"},
	}
	runScenarioOn(t, test, true, 3, preempt.New())
}
