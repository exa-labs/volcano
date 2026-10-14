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

package reclaim

import (
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/api/evictioncost"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/conformance"
	"volcano.sh/volcano/pkg/scheduler/plugins/gang"
	"volcano.sh/volcano/pkg/scheduler/plugins/priority"
	"volcano.sh/volcano/pkg/scheduler/plugins/proportion"
	"volcano.sh/volcano/pkg/scheduler/uthelper"
	"volcano.sh/volcano/pkg/scheduler/util"
)

// q2 overuses a two-GPU node with two equal-priority jobs. Today's order
// evicts the younger job (pg-y), which has an hour of work it cannot resume;
// pg-x is stateless, so evictionCost on evicts pg-x. Shadow acts like off.
func TestReclaimEvictionCostVictimOrder(t *testing.T) {
	started := metav1.NewTime(time.Now().Add(-time.Hour))
	gpuPod := func(name, node, group string, phase v1.PodPhase, annotations map[string]string) *v1.Pod {
		pod := util.BuildPod("c1", name, node, phase,
			api.BuildResourceList("1", "1G", api.ScalarResource{Name: "nvidia.com/gpu", Value: "1"}),
			group, map[string]string{schedulingv1beta1.PodPreemptable: "true"}, nil)
		for k, v := range annotations {
			pod.Annotations[k] = v
		}
		if node != "" {
			pod.Status.StartTime = &started
		}
		return pod
	}
	trueValue := true
	tiers := []conf.Tier{{Plugins: []conf.PluginOption{
		{Name: conformance.PluginName, EnabledReclaimable: &trueValue},
		{Name: gang.PluginName, EnabledJobStarving: &trueValue},
		// proportion's Reclaimable would pick one victim at random here; the
		// test is about the order among the victims, so conformance alone
		// names them.
		{Name: proportion.PluginName, EnabledQueueOrder: &trueValue, EnablePreemptive: &trueValue},
		{Name: priority.PluginName, EnabledJobOrder: &trueValue, EnabledTaskOrder: &trueValue},
	}}}

	for mode, evicted := range map[string]string{"": "c1/y", "off": "c1/y", "shadow": "c1/y", "on": "c1/x"} {
		t.Run("mode="+mode, func(t *testing.T) {
			// Today's order evicts the younger job first.
			older := util.BuildPodGroupWithPrio("pg-x", "c1", "q2", 1, nil, schedulingv1beta1.PodGroupRunning, "low-priority")
			older.CreationTimestamp = metav1.NewTime(started.Add(-time.Hour))
			younger := util.BuildPodGroupWithPrio("pg-y", "c1", "q2", 1, nil, schedulingv1beta1.PodGroupRunning, "low-priority")
			younger.CreationTimestamp = started
			test := uthelper.TestCommonStruct{
				Plugins: map[string]framework.PluginBuilder{
					conformance.PluginName: conformance.New, gang.PluginName: gang.New,
					proportion.PluginName: proportion.New, priority.PluginName: priority.New,
				},
				PriClass: []*schedulingv1.PriorityClass{util.BuildPriorityClass("low-priority", 10)},
				PodGroups: []*schedulingv1beta1.PodGroup{
					older, younger,
					util.BuildPodGroupWithPrio("pg-new", "c1", "q1", 1, nil, schedulingv1beta1.PodGroupInqueue, "low-priority"),
				},
				Pods: []*v1.Pod{
					gpuPod("x", "n1", "pg-x", v1.PodRunning, map[string]string{evictioncost.AnnotationResume: evictioncost.ResumeStateless}),
					gpuPod("y", "n1", "pg-y", v1.PodRunning, nil),
					gpuPod("preemptor", "", "pg-new", v1.PodPending, nil),
				},
				Nodes: []*v1.Node{util.BuildNode("n1", api.BuildResourceList("4", "4G",
					api.ScalarResource{Name: "pods", Value: "10"}, api.ScalarResource{Name: "nvidia.com/gpu", Value: "2"}), nil)},
				Queues:         []*schedulingv1beta1.Queue{util.BuildQueue("q1", 1, nil), util.BuildQueue("q2", 1, nil)},
				ExpectEvicted:  []string{evicted},
				ExpectEvictNum: 1,
			}
			var config []conf.Configuration
			if mode != "" {
				config = []conf.Configuration{{Name: "reclaim", Arguments: map[string]interface{}{evictioncost.ModeKey: mode}}}
			}
			test.RegisterSession(tiers, config)
			defer test.Close()
			test.Run([]framework.Action{New()})
			if err := test.CheckAll(0); err != nil {
				t.Fatal(err)
			}
		})
	}
}
