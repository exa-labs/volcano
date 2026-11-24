/*
Copyright 2025 The Volcano Authors.

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

package allocate

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	schedulingv1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/binpack"
	"volcano.sh/volcano/pkg/scheduler/plugins/capacitycost"
	"volcano.sh/volcano/pkg/scheduler/plugins/gang"
	"volcano.sh/volcano/pkg/scheduler/plugins/predicates"
	"volcano.sh/volcano/pkg/scheduler/plugins/proportion"
	"volcano.sh/volcano/pkg/scheduler/uthelper"
	"volcano.sh/volcano/pkg/scheduler/util"
)

func buildAffinityGangWorker(namespace, name string, req v1.ResourceList, groupName string, topologyKeys ...string) *v1.Pod {
	gangLabels := map[string]string{"gang": groupName}
	pod := util.BuildPod(namespace, name, "", v1.PodPending, req, groupName, gangLabels, nil)
	pod.Annotations[batchv1alpha1.TaskSpecKey] = "worker"
	terms := make([]v1.PodAffinityTerm, 0, len(topologyKeys))
	for _, key := range topologyKeys {
		terms = append(terms, v1.PodAffinityTerm{
			LabelSelector: &metav1.LabelSelector{MatchLabels: gangLabels},
			TopologyKey:   key,
		})
	}
	pod.Spec.Affinity = &v1.Affinity{
		PodAffinity: &v1.PodAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: terms,
		},
	}
	return pod
}

func TestAllocateParseGangPlacementRetries(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		action := New()
		test := uthelper.TestCommonStruct{Name: "default retries"}
		test.RegisterSession(nil, nil)
		defer test.Close()
		test.Run([]framework.Action{action})
		assert.Equal(t, defaultGangPlacementRetries, action.gangPlacementRetries)
	})

	t.Run("custom", func(t *testing.T) {
		action := New()
		test := uthelper.TestCommonStruct{Name: "custom retries"}
		test.RegisterSession(nil, []conf.Configuration{{
			Name:      action.Name(),
			Arguments: map[string]interface{}{GangPlacementRetriesKey: 5},
		}})
		defer test.Close()
		test.Run([]framework.Action{action})
		assert.Equal(t, 5, action.gangPlacementRetries)
	})

	t.Run("negative clamps to zero", func(t *testing.T) {
		action := New()
		test := uthelper.TestCommonStruct{Name: "negative retries"}
		test.RegisterSession(nil, []conf.Configuration{{
			Name:      action.Name(),
			Arguments: map[string]interface{}{GangPlacementRetriesKey: -3},
		}})
		defer test.Close()
		test.Run([]framework.Action{action})
		assert.Equal(t, 0, action.gangPlacementRetries)
	})
}

func TestAllocateGangPlacementRetries(t *testing.T) {
	trueValue := true
	plugins := map[string]framework.PluginBuilder{
		gang.PluginName:         gang.New,
		predicates.PluginName:   predicates.New,
		binpack.PluginName:      binpack.New,
		capacitycost.PluginName: capacitycost.New,
		proportion.PluginName:   proportion.New,
	}
	tiers := []conf.Tier{
		{
			Plugins: []conf.PluginOption{
				{
					Name:                gang.PluginName,
					EnabledJobReady:     &trueValue,
					EnabledJobPipelined: &trueValue,
					EnabledJobEnqueued:  &trueValue,
				},
				{
					Name:               proportion.PluginName,
					EnabledQueueOrder:  &trueValue,
					EnabledOverused:    &trueValue,
					EnabledAllocatable: &trueValue,
				},
				{
					Name:             predicates.PluginName,
					EnabledPredicate: &trueValue,
				},
				{
					Name:             binpack.PluginName,
					EnabledNodeOrder: &trueValue,
					Arguments: map[string]interface{}{
						binpack.BinpackWeight: 10,
						binpack.BinpackCPU:    1,
						binpack.BinpackMemory: 1,
					},
				},
				{
					Name:             capacitycost.PluginName,
					EnabledNodeOrder: &trueValue,
					Arguments: map[string]interface{}{
						"capacitycost.weight":        20,
						"capacitycost.nodeLabelKey":  "capacity-type",
						"capacitycost.order":         "reserved,spot",
						"capacitycost.unlabeledRank": 0,
					},
				},
			},
		},
	}

	t.Run("retries into a viable domain when higher-scoring binpack domain lacks capacity", func(t *testing.T) {
		// n1 (zone-a, gpu-a) has less memory than n2/n3 (zone-b, gpu-b), so binpack
		// scores n1 strictly higher for a 1G pod. Placing w0 on n1 strands w1 on
		// required (zone, gpu) pod affinity. Retrying excludes the dead-end domain
		// and binds both workers in (zone-b, gpu-b).
		test := &uthelper.TestCommonStruct{
			Name:    "binpack dead-end retry",
			Plugins: plugins,
			PodGroups: []*schedulingv1.PodGroup{
				util.BuildPodGroup("pg1", "c1", "q1", 2, map[string]int32{"worker": 2}, schedulingv1.PodGroupInqueue),
			},
			Pods: []*v1.Pod{
				buildAffinityGangWorker("c1", "w0", api.BuildResourceList("2", "1G"), "pg1", "zone", "gpu"),
				buildAffinityGangWorker("c1", "w1", api.BuildResourceList("2", "1G"), "pg1", "zone", "gpu"),
			},
			Nodes: []*v1.Node{
				util.BuildNode("n1", api.BuildResourceList("2", "2G", []api.ScalarResource{{Name: "pods", Value: "10"}}...), map[string]string{"zone": "a", "gpu": "type-a", "capacity-type": "spot"}),
				util.BuildNode("n2", api.BuildResourceList("2", "4G", []api.ScalarResource{{Name: "pods", Value: "10"}}...), map[string]string{"zone": "b", "gpu": "type-b", "capacity-type": "spot"}),
				util.BuildNode("n3", api.BuildResourceList("2", "4G", []api.ScalarResource{{Name: "pods", Value: "10"}}...), map[string]string{"zone": "b", "gpu": "type-b", "capacity-type": "spot"}),
			},
			Queues: []*schedulingv1.Queue{
				util.BuildQueue("q1", 1, nil),
			},
			ExpectBindsNum:   2,
			MinimalBindCheck: true,
		}
		ssn := test.RegisterSession(tiers, nil)
		defer test.Close()
		test.Run([]framework.Action{New()})
		if err := test.CheckAll(0); err != nil {
			t.Fatal(err)
		}
		for _, task := range ssn.Jobs["c1/pg1"].Tasks {
			assert.Contains(t, []string{"n2", "n3"}, task.NodeName, "task %s bound to unexpected node %s", task.Name, task.NodeName)
		}
	})

	t.Run("gangPlacementRetries=0 disables retries and reproduces dead-end stall", func(t *testing.T) {
		test := &uthelper.TestCommonStruct{
			Name:    "retries disabled",
			Plugins: plugins,
			PodGroups: []*schedulingv1.PodGroup{
				util.BuildPodGroup("pg1", "c1", "q1", 2, map[string]int32{"worker": 2}, schedulingv1.PodGroupInqueue),
			},
			Pods: []*v1.Pod{
				buildAffinityGangWorker("c1", "w0", api.BuildResourceList("2", "1G"), "pg1", "zone", "gpu"),
				buildAffinityGangWorker("c1", "w1", api.BuildResourceList("2", "1G"), "pg1", "zone", "gpu"),
			},
			Nodes: []*v1.Node{
				util.BuildNode("n1", api.BuildResourceList("2", "2G", []api.ScalarResource{{Name: "pods", Value: "10"}}...), map[string]string{"zone": "a", "gpu": "type-a", "capacity-type": "spot"}),
				util.BuildNode("n2", api.BuildResourceList("2", "4G", []api.ScalarResource{{Name: "pods", Value: "10"}}...), map[string]string{"zone": "b", "gpu": "type-b", "capacity-type": "spot"}),
				util.BuildNode("n3", api.BuildResourceList("2", "4G", []api.ScalarResource{{Name: "pods", Value: "10"}}...), map[string]string{"zone": "b", "gpu": "type-b", "capacity-type": "spot"}),
			},
			Queues: []*schedulingv1.Queue{
				util.BuildQueue("q1", 1, nil),
			},
			ExpectBindsNum: 0,
		}
		test.RegisterSession(tiers, []conf.Configuration{{
			Name:      "allocate",
			Arguments: map[string]interface{}{GangPlacementRetriesKey: 0},
		}})
		defer test.Close()
		test.Run([]framework.Action{New()})
		if err := test.CheckAll(0); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("walks past six partial domains to reach a viable domain", func(t *testing.T) {
		nodes := make([]*v1.Node, 0, 8)
		// 1 reserved partial domain (highest capacitycost) + 5 spot type-a partial domains (higher binpack).
		nodes = append(nodes, util.BuildNode("reserved-0", api.BuildResourceList("2", "2G", []api.ScalarResource{{Name: "pods", Value: "10"}}...), map[string]string{
			"zone": "z-reserved", "gpu": "type-a", "capacity-type": "reserved",
		}))
		for i := 0; i < 5; i++ {
			nodes = append(nodes, util.BuildNode(fmt.Sprintf("partial-%d", i), api.BuildResourceList("2", "2G", []api.ScalarResource{{Name: "pods", Value: "10"}}...), map[string]string{
				"zone": fmt.Sprintf("z-partial-%d", i), "gpu": "type-a", "capacity-type": "spot",
			}))
		}
		// Domain 7: 2 empty type-b nodes (4G memory -> lower binpack than type-a, spot -> lower capacitycost than reserved).
		nodes = append(nodes,
			util.BuildNode("viable-0", api.BuildResourceList("2", "4G", []api.ScalarResource{{Name: "pods", Value: "10"}}...), map[string]string{
				"zone": "z-viable", "gpu": "type-b", "capacity-type": "spot",
			}),
			util.BuildNode("viable-1", api.BuildResourceList("2", "4G", []api.ScalarResource{{Name: "pods", Value: "10"}}...), map[string]string{
				"zone": "z-viable", "gpu": "type-b", "capacity-type": "spot",
			}),
		)

		test := &uthelper.TestCommonStruct{
			Name:    "six partial domains ahead of viable domain",
			Plugins: plugins,
			PodGroups: []*schedulingv1.PodGroup{
				util.BuildPodGroup("pg1", "c1", "q1", 2, map[string]int32{"worker": 2}, schedulingv1.PodGroupInqueue),
			},
			Pods: []*v1.Pod{
				buildAffinityGangWorker("c1", "w0", api.BuildResourceList("2", "1G"), "pg1", "zone", "gpu"),
				buildAffinityGangWorker("c1", "w1", api.BuildResourceList("2", "1G"), "pg1", "zone", "gpu"),
			},
			Nodes: nodes,
			Queues: []*schedulingv1.Queue{
				util.BuildQueue("q1", 1, nil),
			},
			ExpectBindsNum:   2,
			MinimalBindCheck: true,
		}
		ssn := test.RegisterSession(tiers, nil)
		defer test.Close()
		test.Run([]framework.Action{New()})
		if err := test.CheckAll(0); err != nil {
			t.Fatal(err)
		}
		for _, task := range ssn.Jobs["c1/pg1"].Tasks {
			assert.Contains(t, []string{"viable-0", "viable-1"}, task.NodeName)
		}
	})

	t.Run("skips stale nominatedNodeName in excluded dead-end domain on retry", func(t *testing.T) {
		w0 := buildAffinityGangWorker("c1", "w0", api.BuildResourceList("2", "1G"), "pg1", "zone")
		w0.Status.NominatedNodeName = "n1"
		w1 := buildAffinityGangWorker("c1", "w1", api.BuildResourceList("2", "1G"), "pg1", "zone")

		test := &uthelper.TestCommonStruct{
			Name:    "stale nominated node in dead-end zone",
			Plugins: plugins,
			PodGroups: []*schedulingv1.PodGroup{
				util.BuildPodGroup("pg1", "c1", "q1", 2, map[string]int32{"worker": 2}, schedulingv1.PodGroupInqueue),
			},
			Pods: []*v1.Pod{w0, w1},
			Nodes: []*v1.Node{
				util.BuildNode("n1", api.BuildResourceList("2", "4G", []api.ScalarResource{{Name: "pods", Value: "10"}}...), map[string]string{"zone": "a", "capacity-type": "spot"}),
				util.BuildNode("n2", api.BuildResourceList("2", "4G", []api.ScalarResource{{Name: "pods", Value: "10"}}...), map[string]string{"zone": "b", "capacity-type": "spot"}),
				util.BuildNode("n3", api.BuildResourceList("2", "4G", []api.ScalarResource{{Name: "pods", Value: "10"}}...), map[string]string{"zone": "b", "capacity-type": "spot"}),
			},
			Queues: []*schedulingv1.Queue{
				util.BuildQueue("q1", 1, nil),
			},
			ExpectBindsNum:   2,
			MinimalBindCheck: true,
		}
		ssn := test.RegisterSession(tiers, nil)
		defer test.Close()
		test.Run([]framework.Action{New()})
		if err := test.CheckAll(0); err != nil {
			t.Fatal(err)
		}
		for _, task := range ssn.Jobs["c1/pg1"].Tasks {
			assert.Contains(t, []string{"n2", "n3"}, task.NodeName)
		}
	})

	t.Run("clears stale UnschedulableAndUnresolvable fit errors after abandoned attempts", func(t *testing.T) {
		// Neither zone-a nor zone-b can fit 2 tasks. After allocate gives up,
		// job.NodesFitErrors must not retain phantom UnschedulableAndUnresolvable
		// affinity errors from rolled-back tentative placements.
		test := &uthelper.TestCommonStruct{
			Name:    "no viable domain leaves clean fit errors",
			Plugins: plugins,
			PodGroups: []*schedulingv1.PodGroup{
				util.BuildPodGroup("pg1", "c1", "q1", 2, map[string]int32{"worker": 2}, schedulingv1.PodGroupInqueue),
			},
			Pods: []*v1.Pod{
				buildAffinityGangWorker("c1", "w0", api.BuildResourceList("2", "1G"), "pg1", "zone"),
				buildAffinityGangWorker("c1", "w1", api.BuildResourceList("2", "1G"), "pg1", "zone"),
			},
			Nodes: []*v1.Node{
				util.BuildNode("n1", api.BuildResourceList("2", "2G", []api.ScalarResource{{Name: "pods", Value: "10"}}...), map[string]string{"zone": "a", "capacity-type": "spot"}),
				util.BuildNode("n2", api.BuildResourceList("2", "4G", []api.ScalarResource{{Name: "pods", Value: "10"}}...), map[string]string{"zone": "b", "capacity-type": "spot"}),
			},
			Queues: []*schedulingv1.Queue{
				util.BuildQueue("q1", 1, nil),
			},
			ExpectBindsNum: 0,
		}
		ssn := test.RegisterSession(tiers, nil)
		defer test.Close()
		test.Run([]framework.Action{New()})
		if err := test.CheckAll(0); err != nil {
			t.Fatal(err)
		}
		job := ssn.Jobs["c1/pg1"]
		for _, task := range job.Tasks {
			unresolvable := ssn.FilterOutUnschedulableAndUnresolvableNodesForTask(task)
			assert.Len(t, unresolvable, 2, "task %s should not have healthy nodes hidden by rolled-back fit errors", task.Name)
		}
	})
}
