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

package preempt

import (
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/api/restartcost"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/conformance"
	"volcano.sh/volcano/pkg/scheduler/plugins/gang"
	"volcano.sh/volcano/pkg/scheduler/plugins/predicates"
	"volcano.sh/volcano/pkg/scheduler/plugins/priority"
	"volcano.sh/volcano/pkg/scheduler/plugins/proportion"
	"volcano.sh/volcano/pkg/scheduler/uthelper"
	"volcano.sh/volcano/pkg/scheduler/util"
)

// gpuPod is a one-GPU pod started at the given time.
func gpuPod(name, node, group string, phase v1.PodPhase, started time.Time, annotations map[string]string) *v1.Pod {
	pod := util.BuildPod("c1", name, node, phase,
		api.BuildResourceList("1", "1G", api.ScalarResource{Name: "nvidia.com/gpu", Value: "1"}),
		group, map[string]string{schedulingv1beta1.PodPreemptable: "true"}, nil)
	for k, v := range annotations {
		pod.Annotations[k] = v
	}
	if !started.IsZero() {
		pod.Status.StartTime = &metav1.Time{Time: started}
	}
	return pod
}

func gpuNode(name string) *v1.Node {
	return util.BuildNode(name, api.BuildResourceList("2", "2G",
		api.ScalarResource{Name: "pods", Value: "10"}, api.ScalarResource{Name: "nvidia.com/gpu", Value: "1"}), nil)
}

// Two one-GPU nodes, each full with one low-priority victim. The victim on
// n1 started later, so today's latest-start-time tie-break picks n1; it has
// an hour of work it cannot resume (1 x (30 + 3600) GPU-seconds). The one
// on n2 checkpointed a minute ago (1 x (30 + 300 + 60)), so restartCost on
// picks n2. Shadow acts like off.
func TestTopologyAwarePreemptRestartCost(t *testing.T) {
	now := time.Now()
	rfc := func(at time.Time) string { return at.UTC().Format(time.RFC3339) }
	trueValue := true
	tiers := []conf.Tier{{Plugins: []conf.PluginOption{
		{Name: conformance.PluginName, EnabledPreemptable: &trueValue},
		{Name: gang.PluginName, EnabledJobReady: &trueValue, EnabledJobPipelined: &trueValue, EnabledJobStarving: &trueValue},
		{Name: priority.PluginName, EnabledTaskOrder: &trueValue, EnabledJobOrder: &trueValue, EnabledPreemptable: &trueValue,
			EnabledJobPipelined: &trueValue, EnabledJobStarving: &trueValue},
		{Name: proportion.PluginName, EnabledOverused: &trueValue, EnabledQueueOrder: &trueValue},
		{Name: predicates.PluginName, EnabledPreemptable: &trueValue, EnabledPredicate: &trueValue},
	}}}

	for mode, evicted := range map[string]string{"": "c1/fresh", "off": "c1/fresh", "shadow": "c1/fresh", "on": "c1/checkpointed"} {
		t.Run("mode="+mode, func(t *testing.T) {
			test := uthelper.TestCommonStruct{
				Plugins: map[string]framework.PluginBuilder{
					conformance.PluginName: conformance.New, gang.PluginName: gang.New, priority.PluginName: priority.New,
					proportion.PluginName: proportion.New, predicates.PluginName: predicates.New,
				},
				PriClass: []*schedulingv1.PriorityClass{util.BuildPriorityClass("high-priority", 100000), util.BuildPriorityClass("low-priority", 10)},
				PodGroups: []*schedulingv1beta1.PodGroup{
					util.BuildPodGroupWithPrio("pg-fresh", "c1", "q1", 1, nil, schedulingv1beta1.PodGroupRunning, "low-priority"),
					util.BuildPodGroupWithPrio("pg-ckpt", "c1", "q1", 1, nil, schedulingv1beta1.PodGroupRunning, "low-priority"),
					util.BuildPodGroupWithPrio("pg-new", "c1", "q1", 1, nil, schedulingv1beta1.PodGroupInqueue, "high-priority"),
				},
				Pods: []*v1.Pod{
					gpuPod("fresh", "n1", "pg-fresh", v1.PodRunning, now.Add(-time.Hour), nil),
					gpuPod("checkpointed", "n2", "pg-ckpt", v1.PodRunning, now.Add(-2*time.Hour), map[string]string{
						restartcost.AnnotationResume:         restartcost.ResumeCheckpoint,
						restartcost.AnnotationProductiveAt:   rfc(now.Add(-2*time.Hour + 5*time.Minute)),
						restartcost.AnnotationLastCheckpoint: rfc(now.Add(-time.Minute)),
					}),
					gpuPod("preemptor", "", "pg-new", v1.PodPending, time.Time{}, nil),
				},
				Nodes:          []*v1.Node{gpuNode("n1"), gpuNode("n2")},
				Queues:         []*schedulingv1beta1.Queue{util.BuildQueue("q1", 1, nil)},
				ExpectEvicted:  []string{evicted},
				ExpectEvictNum: 1,
			}
			args := map[string]interface{}{
				EnableTopologyAwarePreemptionKey:    true,
				EnableNodeOrderScoreInPreemptionKey: false,
				MinCandidateNodesAbsoluteKey:        2,
				MaxCandidateNodesAbsoluteKey:        2,
			}
			if mode != "" {
				args[restartcost.ModeKey] = mode
			}
			test.RegisterSession(tiers, []conf.Configuration{{Name: "preempt", Arguments: args}})
			defer test.Close()
			test.Run([]framework.Action{New()})
			if err := test.CheckAll(0); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Victims of equal priority on one node: the cheapest is evicted first and
// reprieved last.
func TestReprieveCostliestFirst(t *testing.T) {
	victims := []*api.TaskInfo{
		{Name: "low-cheap", Priority: 1}, {Name: "low-dear", Priority: 1},
		{Name: "mid-cheap", Priority: 5}, {Name: "mid-mid", Priority: 5}, {Name: "mid-dear", Priority: 5},
	}
	cost := map[string]float64{"low-cheap": 1, "low-dear": 9, "mid-cheap": 1, "mid-mid": 5, "mid-dear": 9}
	reprieveCostliestFirst(victims, func(task *api.TaskInfo) float64 { return cost[task.Name] },
		func(l, r *api.TaskInfo) bool { return l.Priority == r.Priority })
	want := []string{"low-dear", "low-cheap", "mid-dear", "mid-mid", "mid-cheap"}
	for i, task := range victims {
		if task.Name != want[i] {
			t.Fatalf("position %d: got %s, want %s", i, task.Name, want[i])
		}
	}
}
