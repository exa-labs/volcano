/*
Copyright 2018 The Kubernetes Authors.

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

	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"

	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/capacity"
	"volcano.sh/volcano/pkg/scheduler/plugins/conformance"
	"volcano.sh/volcano/pkg/scheduler/plugins/gang"
	"volcano.sh/volcano/pkg/scheduler/plugins/predicates"
	"volcano.sh/volcano/pkg/scheduler/plugins/priority"
	"volcano.sh/volcano/pkg/scheduler/plugins/proportion"
	"volcano.sh/volcano/pkg/scheduler/uthelper"
	"volcano.sh/volcano/pkg/scheduler/util"
)

func TestReclaim(t *testing.T) {
	tests := []uthelper.TestCommonStruct{
		{
			Name: "Two Queue with one Queue overusing resource, should reclaim",
			Plugins: map[string]framework.PluginBuilder{
				conformance.PluginName: conformance.New,
				gang.PluginName:        gang.New,
				proportion.PluginName:  proportion.New,
			},
			PodGroups: []*schedulingv1beta1.PodGroup{
				util.BuildPodGroupWithPrio("pg1", "c1", "q1", 1, nil, schedulingv1beta1.PodGroupInqueue, "low-priority"),
				util.BuildPodGroupWithPrio("pg2", "c1", "q2", 1, nil, schedulingv1beta1.PodGroupInqueue, "high-priority"),
			},
			Pods: []*v1.Pod{
				util.BuildPod("c1", "preemptee1", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1", map[string]string{schedulingv1beta1.PodPreemptable: "false"}, make(map[string]string)),
				util.BuildPod("c1", "preemptee2", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1", map[string]string{schedulingv1beta1.PodPreemptable: "true"}, make(map[string]string)),
				util.BuildPod("c1", "preemptee3", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1", map[string]string{schedulingv1beta1.PodPreemptable: "false"}, make(map[string]string)),
				util.BuildPod("c1", "preemptor1", "", v1.PodPending, api.BuildResourceList("1", "1G"), "pg2", make(map[string]string), make(map[string]string)),
			},
			Nodes: []*v1.Node{
				util.BuildNode("n1", api.BuildResourceList("3", "3Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), make(map[string]string)),
			},
			Queues: []*schedulingv1beta1.Queue{
				util.BuildQueue("q1", 1, nil),
				util.BuildQueue("q2", 1, nil),
			},
			ExpectEvictNum: 1,
			ExpectEvicted:  []string{"c1/preemptee2"}, // let pod2 in the middle when sort tasks be preemptable and will not disturb
		},
		{
			Name: "sort reclaimees when reclaiming from overusing queue",
			Plugins: map[string]framework.PluginBuilder{
				conformance.PluginName: conformance.New,
				gang.PluginName:        gang.New,
				priority.PluginName:    priority.New,
				proportion.PluginName:  proportion.New,
			},
			PriClass: []*schedulingv1.PriorityClass{
				util.BuildPriorityClass("low-priority", 100),
				util.BuildPriorityClass("mid-priority", 500),
				util.BuildPriorityClass("high-priority", 1000),
			},
			PodGroups: []*schedulingv1beta1.PodGroup{
				util.BuildPodGroupWithPrio("pg1", "c1", "q1", 1, nil, schedulingv1beta1.PodGroupInqueue, "mid-priority"),
				util.BuildPodGroupWithPrio("pg2", "c1", "q2", 1, nil, schedulingv1beta1.PodGroupInqueue, "low-priority"), // reclaimed first
				util.BuildPodGroupWithPrio("pg3", "c1", "q3", 1, nil, schedulingv1beta1.PodGroupInqueue, "high-priority"),
			},
			Pods: []*v1.Pod{
				util.BuildPod("c1", "preemptee1-1", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1", map[string]string{schedulingv1beta1.PodPreemptable: "true"}, make(map[string]string)),
				util.BuildPod("c1", "preemptee1-2", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1", map[string]string{schedulingv1beta1.PodPreemptable: "true"}, make(map[string]string)),
				util.BuildPod("c1", "preemptee2-1", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg2", map[string]string{schedulingv1beta1.PodPreemptable: "true"}, make(map[string]string)),
				util.BuildPod("c1", "preemptee2-2", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg2", map[string]string{schedulingv1beta1.PodPreemptable: "false"}, make(map[string]string)),
				util.BuildPod("c1", "preemptor1", "", v1.PodPending, api.BuildResourceList("1", "1G"), "pg3", make(map[string]string), make(map[string]string)),
			},
			Nodes: []*v1.Node{
				util.BuildNode("n1", api.BuildResourceList("4", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), make(map[string]string)),
			},
			Queues: []*schedulingv1beta1.Queue{
				util.BuildQueue("q1", 1, nil),
				util.BuildQueue("q2", 1, nil),
				util.BuildQueue("q3", 1, nil),
			},
			ExpectEvictNum: 1,
			ExpectEvicted:  []string{"c1/preemptee2-1"}, // low priority job's preemptable pod is evicted
		},
		{
			Name: "sort reclaimees when reclaiming from overusing queues with different queue priority",
			Plugins: map[string]framework.PluginBuilder{
				conformance.PluginName: conformance.New,
				gang.PluginName:        gang.New,
				proportion.PluginName:  proportion.New,
			},
			PodGroups: []*schedulingv1beta1.PodGroup{
				util.BuildPodGroupWithPrio("pg1", "c1", "q1", 1, nil, schedulingv1beta1.PodGroupInqueue, "mid-priority"),
				util.BuildPodGroupWithPrio("pg2", "c1", "q2", 1, nil, schedulingv1beta1.PodGroupInqueue, "mid-priority"),
				util.BuildPodGroupWithPrio("pg3", "c1", "q3", 1, nil, schedulingv1beta1.PodGroupInqueue, "mid-priority"),
			},
			Pods: []*v1.Pod{
				util.BuildPod("c1", "preemptee1-1", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1", map[string]string{schedulingv1beta1.PodPreemptable: "true"}, make(map[string]string)),
				util.BuildPod("c1", "preemptee1-2", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1", map[string]string{schedulingv1beta1.PodPreemptable: "false"}, make(map[string]string)),
				util.BuildPod("c1", "preemptee2-1", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg2", map[string]string{schedulingv1beta1.PodPreemptable: "true"}, make(map[string]string)),
				util.BuildPod("c1", "preemptee2-2", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg2", map[string]string{schedulingv1beta1.PodPreemptable: "false"}, make(map[string]string)),
				util.BuildPod("c1", "preemptor1", "", v1.PodPending, api.BuildResourceList("1", "1G"), "pg3", make(map[string]string), make(map[string]string)),
			},
			Nodes: []*v1.Node{
				util.BuildNode("n1", api.BuildResourceList("4", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), make(map[string]string)),
			},
			Queues: []*schedulingv1beta1.Queue{
				util.BuildQueueWithPriorityAndResourcesQuantity("q1", 5, nil, nil),
				util.BuildQueueWithPriorityAndResourcesQuantity("q2", 10, nil, nil), // highest queue priority
				util.BuildQueueWithPriorityAndResourcesQuantity("q3", 1, nil, nil),
			},
			ExpectEvictNum: 1,
			ExpectEvicted:  []string{"c1/preemptee1-1"}, // low queue priority job's preemptable pod is evicted
		},
		{
			// case about #3642
			Name: "can not reclaim resources when task preemption policy is never",
			Plugins: map[string]framework.PluginBuilder{
				conformance.PluginName: conformance.New,
				gang.PluginName:        gang.New,
				proportion.PluginName:  proportion.New,
			},
			PriClass: []*schedulingv1.PriorityClass{
				util.BuildPriorityClass("low-priority", 100),
				util.BuildPriorityClassWithPreemptionPolicy("high-priority", 1000, v1.PreemptNever),
			},
			PodGroups: []*schedulingv1beta1.PodGroup{
				util.BuildPodGroupWithPrio("pg1", "c1", "q1", 0, nil, schedulingv1beta1.PodGroupInqueue, "low-priority"),
				util.BuildPodGroupWithPrio("pg2", "c1", "q2", 0, nil, schedulingv1beta1.PodGroupInqueue, "high-priority"),
			},
			Pods: []*v1.Pod{
				util.BuildPod("c1", "preemptee1", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1", map[string]string{schedulingv1beta1.PodPreemptable: "true"}, make(map[string]string)),
				util.BuildPodWithPreemptionPolicy("c1", "preemptor1", "", v1.PodPending, api.BuildResourceList("1", "1G"), "pg2", make(map[string]string), make(map[string]string), v1.PreemptNever),
			},
			Nodes: []*v1.Node{
				util.BuildNode("n1", api.BuildResourceList("1", "1Gi", []api.ScalarResource{{Name: "pods", Value: "1"}}...), make(map[string]string)),
			},
			Queues: []*schedulingv1beta1.Queue{
				util.BuildQueue("q1", 5, nil),
				util.BuildQueue("q2", 10, nil),
			},
			ExpectEvictNum: 0,
			ExpectEvicted:  []string{}, // no victims should be reclaimed
		},
		{
			Name: "can not reclaim resources when queue is not open(proportion plugin)",
			Plugins: map[string]framework.PluginBuilder{
				conformance.PluginName: conformance.New,
				gang.PluginName:        gang.New,
				proportion.PluginName:  proportion.New,
			},
			PodGroups: []*schedulingv1beta1.PodGroup{
				util.BuildPodGroupWithPrio("pg1", "c1", "q1", 1, nil, schedulingv1beta1.PodGroupRunning, "low-priority"),
				util.BuildPodGroupWithPrio("pg2", "c1", "q2", 1, nil, schedulingv1beta1.PodGroupInqueue, "high-priority"),
			},
			Pods: []*v1.Pod{
				util.BuildPod("c1", "preemptee1", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1", map[string]string{schedulingv1beta1.PodPreemptable: "false"}, make(map[string]string)),
				util.BuildPod("c1", "preemptee2", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1", map[string]string{schedulingv1beta1.PodPreemptable: "true"}, make(map[string]string)),
				util.BuildPod("c1", "preemptee3", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1", map[string]string{schedulingv1beta1.PodPreemptable: "false"}, make(map[string]string)),
				util.BuildPod("c1", "preemptor1", "", v1.PodPending, api.BuildResourceList("1", "1G"), "pg2", make(map[string]string), make(map[string]string)),
			},
			Nodes: []*v1.Node{
				util.BuildNode("n1", api.BuildResourceList("3", "3Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), make(map[string]string)),
			},
			Queues: []*schedulingv1beta1.Queue{
				util.BuildQueue("q1", 1, nil),
				util.BuildQueueWithState("q2", 1, nil, schedulingv1beta1.QueueStateClosed),
			},
			ExpectEvictNum: 0,
			ExpectEvicted:  []string{},
		},
		{
			Name: "can not reclaim resources when queue is not open(capacity plugin)",
			Plugins: map[string]framework.PluginBuilder{
				conformance.PluginName: conformance.New,
				gang.PluginName:        gang.New,
				capacity.PluginName:    capacity.New,
			},
			PodGroups: []*schedulingv1beta1.PodGroup{
				util.BuildPodGroupWithPrio("pg1", "c1", "q1", 1, nil, schedulingv1beta1.PodGroupRunning, "low-priority"),
				util.BuildPodGroupWithPrio("pg2", "c1", "q2", 1, nil, schedulingv1beta1.PodGroupInqueue, "high-priority"),
			},
			Pods: []*v1.Pod{
				util.BuildPod("c1", "preemptee1", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1", map[string]string{schedulingv1beta1.PodPreemptable: "false"}, make(map[string]string)),
				util.BuildPod("c1", "preemptee2", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1", map[string]string{schedulingv1beta1.PodPreemptable: "true"}, make(map[string]string)),
				util.BuildPod("c1", "preemptee3", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1", map[string]string{schedulingv1beta1.PodPreemptable: "false"}, make(map[string]string)),
				util.BuildPod("c1", "preemptor1", "", v1.PodPending, api.BuildResourceList("1", "1G"), "pg2", make(map[string]string), make(map[string]string)),
			},
			Nodes: []*v1.Node{
				util.BuildNode("n1", api.BuildResourceList("3", "3Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), make(map[string]string)),
			},
			Queues: []*schedulingv1beta1.Queue{
				util.BuildQueue("q1", 1, nil),
				util.BuildQueueWithState("q2", 1, nil, schedulingv1beta1.QueueStateClosed),
			},
			ExpectEvictNum: 0,
			ExpectEvicted:  []string{},
		},
	}

	reclaim := New()
	trueValue := true
	tiers := []conf.Tier{
		{
			Plugins: []conf.PluginOption{
				{
					Name:               conformance.PluginName,
					EnabledReclaimable: &trueValue,
				},
				{
					Name:               gang.PluginName,
					EnabledReclaimable: &trueValue,
					EnabledJobStarving: &trueValue,
				},
				{ // proportion plugin will cause deserved resource larger than preemptable pods' usage, and return less victims
					Name:               proportion.PluginName,
					EnabledReclaimable: &trueValue,
					EnabledQueueOrder:  &trueValue,
					EnablePreemptive:   &trueValue,
				},
				{
					Name:               capacity.PluginName,
					EnabledReclaimable: &trueValue,
					EnabledQueueOrder:  &trueValue,
					EnablePreemptive:   &trueValue,
				},
				{
					Name:             priority.PluginName,
					EnabledJobOrder:  &trueValue,
					EnabledTaskOrder: &trueValue,
				},
			},
		},
	}
	for i, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			test.RegisterSession(tiers, nil)
			defer test.Close()
			test.Run([]framework.Action{reclaim})
			if err := test.CheckAll(i); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestReclaimWithNodeSelector tests that the reclaim action's fallback node
// list correctly filters out structurally incompatible nodes (e.g. nodes whose
// labels don't match the reclaimer's nodeSelector). This validates the fix for
// the bug where, when all predicate-passing nodes were empty, the fallback
// used ALL cluster nodes — including ones that could never run the pod.
func TestReclaimWithNodeSelector(t *testing.T) {
	trueValue := true
	tiers := []conf.Tier{
		{
			Plugins: []conf.PluginOption{
				{
					Name:               conformance.PluginName,
					EnabledReclaimable: &trueValue,
				},
				{
					Name:               gang.PluginName,
					EnabledReclaimable: &trueValue,
					EnabledJobStarving: &trueValue,
				},
				{
					Name:               proportion.PluginName,
					EnabledReclaimable: &trueValue,
					EnabledQueueOrder:  &trueValue,
					EnablePreemptive:   &trueValue,
				},
				{
					Name:             priority.PluginName,
					EnabledJobOrder:  &trueValue,
					EnabledTaskOrder: &trueValue,
				},
				{
					Name:             predicates.PluginName,
					EnabledPredicate: &trueValue,
					Arguments: map[string]interface{}{
						"predicate.VolumeBindingEnable": false,
					},
				},
			},
		},
	}

	tests := []uthelper.TestCommonStruct{
		{
			// Reclaim cross-queue: q1 is overusing (3 CPU allocated on n1,
			// but with 3.5 CPU total cluster, q1 only deserves ~2.5 CPU).
			// The reclaimer (q2) has a nodeSelector matching n1 but not n2.
			// n1 is full → predicate check fails → fallback triggers.
			// Fallback must include n1 (resource-constrained, structurally
			// compatible) and exclude n2 (nodeSelector mismatch).
			// n2 is kept small so leftover fair-share doesn't inflate q1's
			// deserved back up to its allocated (proportion caps deserved
			// at the queue's total request).
			Name: "reclaim on full node with matching nodeSelector, skip non-matching node",
			Plugins: map[string]framework.PluginBuilder{
				conformance.PluginName: conformance.New,
				gang.PluginName:        gang.New,
				proportion.PluginName:  proportion.New,
				priority.PluginName:    priority.New,
				predicates.PluginName:  predicates.New,
			},
			PriClass: []*schedulingv1.PriorityClass{
				util.BuildPriorityClass("low-priority", 10),
				util.BuildPriorityClass("high-priority", 100000),
			},
			PodGroups: []*schedulingv1beta1.PodGroup{
				util.BuildPodGroupWithPrio("pg1", "c1", "q1", 1, nil, schedulingv1beta1.PodGroupInqueue, "low-priority"),
				util.BuildPodGroupWithPrio("pg2", "c1", "q2", 1, nil, schedulingv1beta1.PodGroupInqueue, "high-priority"),
			},
			Pods: []*v1.Pod{
				// q1 uses all 3 CPU on n1 — overusing (total 3.5 CPU, deserves ~2.5)
				util.BuildPod("c1", "preemptee1", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1",
					map[string]string{schedulingv1beta1.PodPreemptable: "false"}, map[string]string{"nodepool": "gpu"}),
				util.BuildPod("c1", "preemptee2", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1",
					map[string]string{schedulingv1beta1.PodPreemptable: "true"}, map[string]string{"nodepool": "gpu"}),
				util.BuildPod("c1", "preemptee3", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1",
					map[string]string{schedulingv1beta1.PodPreemptable: "false"}, map[string]string{"nodepool": "gpu"}),
				// High-priority reclaimer with nodeSelector targeting gpu nodepool
				util.BuildPod("c1", "preemptor1", "", v1.PodPending, api.BuildResourceList("1", "1G"), "pg2",
					make(map[string]string), map[string]string{"nodepool": "gpu"}),
			},
			Nodes: []*v1.Node{
				// n1: matching node, fully utilised (3 CPU)
				util.BuildNode("n1", api.BuildResourceList("3", "3Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), map[string]string{"nodepool": "gpu"}),
				// n2: non-matching node, small (500m CPU) so total stays at 3.5 CPU
				// and q1's deserved (~2.5) stays below its allocated (3)
				util.BuildNode("n2", api.BuildResourceList("500m", "500Mi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), map[string]string{"nodepool": "cpu"}),
			},
			Queues: []*schedulingv1beta1.Queue{
				util.BuildQueue("q1", 1, nil),
				util.BuildQueue("q2", 1, nil),
			},
			ExpectEvictNum: 1,
			ExpectEvicted:  []string{"c1/preemptee2"},
		},
		{
			// All nodes are structurally incompatible with the reclaimer's
			// nodeSelector. q1 is overusing (3 CPU, deserves 1.5). Fallback
			// should filter all nodes out → candidateNodes is empty → no reclaim.
			Name: "no reclaim when all nodes are structurally incompatible with nodeSelector",
			Plugins: map[string]framework.PluginBuilder{
				conformance.PluginName: conformance.New,
				gang.PluginName:        gang.New,
				proportion.PluginName:  proportion.New,
				priority.PluginName:    priority.New,
				predicates.PluginName:  predicates.New,
			},
			PriClass: []*schedulingv1.PriorityClass{
				util.BuildPriorityClass("low-priority", 10),
				util.BuildPriorityClass("high-priority", 100000),
			},
			PodGroups: []*schedulingv1beta1.PodGroup{
				util.BuildPodGroupWithPrio("pg1", "c1", "q1", 1, nil, schedulingv1beta1.PodGroupInqueue, "low-priority"),
				util.BuildPodGroupWithPrio("pg2", "c1", "q2", 1, nil, schedulingv1beta1.PodGroupInqueue, "high-priority"),
			},
			Pods: []*v1.Pod{
				// q1 overuses: 3 CPU on a 3 CPU node, deserves only 1.5
				util.BuildPod("c1", "preemptee1", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1",
					map[string]string{schedulingv1beta1.PodPreemptable: "false"}, make(map[string]string)),
				util.BuildPod("c1", "preemptee2", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1",
					map[string]string{schedulingv1beta1.PodPreemptable: "true"}, make(map[string]string)),
				util.BuildPod("c1", "preemptee3", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1",
					map[string]string{schedulingv1beta1.PodPreemptable: "false"}, make(map[string]string)),
				// High-priority reclaimer with nodeSelector that no node satisfies
				util.BuildPod("c1", "preemptor1", "", v1.PodPending, api.BuildResourceList("1", "1G"), "pg2",
					make(map[string]string), map[string]string{"nodepool": "gpu"}),
			},
			Nodes: []*v1.Node{
				// n1: only node, doesn't match reclaimer's nodeSelector
				util.BuildNode("n1", api.BuildResourceList("3", "3Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), map[string]string{"nodepool": "cpu"}),
			},
			Queues: []*schedulingv1beta1.Queue{
				util.BuildQueue("q1", 1, nil),
				util.BuildQueue("q2", 1, nil),
			},
			ExpectEvictNum: 0,
			ExpectEvicted:  []string{},
		},
		{
			// Mixed: matching node n1 is full with q1 overusing (4 CPU used,
			// deserves 2 out of 4 total), non-matching node n2 also has a q1
			// task. Reclaim should only consider n1. Without the fix, Volcano
			// would include n2 in the fallback and potentially nominate the
			// pod there, violating nodeSelector.
			Name: "reclaim only on matching node when mixed matching and non-matching nodes are full",
			Plugins: map[string]framework.PluginBuilder{
				conformance.PluginName: conformance.New,
				gang.PluginName:        gang.New,
				proportion.PluginName:  proportion.New,
				priority.PluginName:    priority.New,
				predicates.PluginName:  predicates.New,
			},
			PriClass: []*schedulingv1.PriorityClass{
				util.BuildPriorityClass("low-priority", 10),
				util.BuildPriorityClass("high-priority", 100000),
			},
			PodGroups: []*schedulingv1beta1.PodGroup{
				util.BuildPodGroupWithPrio("pg1", "c1", "q1", 0, nil, schedulingv1beta1.PodGroupInqueue, "low-priority"),
				util.BuildPodGroupWithPrio("pg2", "c1", "q2", 1, nil, schedulingv1beta1.PodGroupInqueue, "high-priority"),
			},
			Pods: []*v1.Pod{
				// q1 overuses on n1: 3 CPU on n1 + 1 CPU on n2 = 4 used, deserves 2
				util.BuildPod("c1", "preemptee1", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1",
					map[string]string{schedulingv1beta1.PodPreemptable: "false"}, map[string]string{"nodepool": "gpu"}),
				util.BuildPod("c1", "preemptee2", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1",
					map[string]string{schedulingv1beta1.PodPreemptable: "true"}, map[string]string{"nodepool": "gpu"}),
				util.BuildPod("c1", "preemptee3", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1",
					map[string]string{schedulingv1beta1.PodPreemptable: "false"}, map[string]string{"nodepool": "gpu"}),
				// q1 task on n2 (non-matching node)
				util.BuildPod("c1", "preemptee4", "n2", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1",
					map[string]string{schedulingv1beta1.PodPreemptable: "true"}, make(map[string]string)),
				// High-priority reclaimer targeting gpu nodepool
				util.BuildPod("c1", "preemptor1", "", v1.PodPending, api.BuildResourceList("1", "1G"), "pg2",
					make(map[string]string), map[string]string{"nodepool": "gpu"}),
			},
			Nodes: []*v1.Node{
				util.BuildNode("n1", api.BuildResourceList("3", "3Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), map[string]string{"nodepool": "gpu"}),
				util.BuildNode("n2", api.BuildResourceList("1", "1Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), map[string]string{"nodepool": "cpu"}),
			},
			Queues: []*schedulingv1beta1.Queue{
				util.BuildQueue("q1", 1, nil),
				util.BuildQueue("q2", 1, nil),
			},
			ExpectEvictNum: 1,
			ExpectEvicted:  []string{"c1/preemptee2"},
		},
	}

	reclaim := New()
	for i, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			test.RegisterSession(tiers, nil)
			defer test.Close()
			test.Run([]framework.Action{reclaim})
			if err := test.CheckAll(i); err != nil {
				t.Fatal(err)
			}
		})
	}
}
