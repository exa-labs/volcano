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

package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	"volcano.sh/volcano/pkg/scheduler/api"
)

func buildAffinityTask(name, role string, req v1.ResourceList, gang string, topologyKeys ...string) *api.TaskInfo {
	podLabels := map[string]string{"gang": gang}
	pod := BuildPod("ns1", name, "", v1.PodPending, req, gang, podLabels, nil)
	pod.Annotations[batchv1alpha1.TaskSpecKey] = role
	if len(topologyKeys) > 0 {
		terms := make([]v1.PodAffinityTerm, 0, len(topologyKeys))
		for _, key := range topologyKeys {
			terms = append(terms, v1.PodAffinityTerm{
				LabelSelector: &metav1.LabelSelector{MatchLabels: podLabels},
				TopologyKey:   key,
			})
		}
		pod.Spec.Affinity = &v1.Affinity{
			PodAffinity: &v1.PodAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: terms,
			},
		}
	}
	return api.NewTaskInfo(pod)
}

func buildLabeledNodeInfo(name string, nodeLabels map[string]string) *api.NodeInfo {
	return api.NewNodeInfo(BuildNode(name, api.BuildResourceList("8", "16Gi"), nodeLabels))
}

func TestExpandExcludedGangNodes(t *testing.T) {
	allNodes := []*api.NodeInfo{
		buildLabeledNodeInfo("n1", map[string]string{"zone": "a", "gpu": "type1", "rack": "r1"}),
		buildLabeledNodeInfo("n2", map[string]string{"zone": "a", "gpu": "type1", "rack": "r2"}),
		buildLabeledNodeInfo("n3", map[string]string{"zone": "a", "gpu": "type2", "rack": "r2"}),
		buildLabeledNodeInfo("n4", map[string]string{"zone": "b", "gpu": "type1", "rack": "r3"}),
		buildLabeledNodeInfo("n5", map[string]string{"zone": "b", "gpu": "type2", "rack": "r3"}),
	}

	t.Run("expands homogeneous multi-key affinity domain", func(t *testing.T) {
		t1 := buildAffinityTask("w0", "worker", api.BuildResourceList("4", "8Gi"), "pg1", "zone", "gpu")
		t2 := buildAffinityTask("w1", "worker", api.BuildResourceList("4", "8Gi"), "pg1", "zone", "gpu")

		got := ExpandExcludedGangNodes(allNodes, sets.New("n1"), []*api.TaskInfo{t1}, []*api.TaskInfo{t1, t2})
		assert.Equal(t, sets.New("n1", "n2"), got)
	})

	t.Run("does not expand when a member carries an extra narrower affinity term", func(t *testing.T) {
		t1 := buildAffinityTask("w0", "worker", api.BuildResourceList("4", "8Gi"), "pg1", "zone")
		t2 := buildAffinityTask("w1", "worker", api.BuildResourceList("4", "8Gi"), "pg1", "zone", "rack")

		got := ExpandExcludedGangNodes(allNodes, sets.New("n1"), []*api.TaskInfo{t1}, []*api.TaskInfo{t1, t2})
		assert.Equal(t, sets.New("n1"), got)
	})

	t.Run("does not expand when a non-self required affinity term is present", func(t *testing.T) {
		t1 := buildAffinityTask("w0", "worker", api.BuildResourceList("4", "8Gi"), "pg1", "zone")
		t2 := buildAffinityTask("w1", "worker", api.BuildResourceList("4", "8Gi"), "pg1", "zone")
		extTerm := v1.PodAffinityTerm{
			LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "cache"}},
			TopologyKey:   "rack",
		}
		t1.Pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
			t1.Pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution, extTerm,
		)
		t2.Pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
			t2.Pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution, extTerm,
		)

		got := ExpandExcludedGangNodes(allNodes, sets.New("n1"), []*api.TaskInfo{t1}, []*api.TaskInfo{t1, t2})
		assert.Equal(t, sets.New("n1"), got)
	})

	t.Run("does not expand heterogeneous resource requests", func(t *testing.T) {
		small := buildAffinityTask("w0", "worker", api.BuildResourceList("2", "4Gi"), "pg1", "zone")
		large := buildAffinityTask("w1", "worker", api.BuildResourceList("8", "16Gi"), "pg1", "zone")

		got := ExpandExcludedGangNodes(allNodes, sets.New("n1"), []*api.TaskInfo{small}, []*api.TaskInfo{small, large})
		assert.Equal(t, sets.New("n1"), got)
	})

	t.Run("does not expand when tasks lack self-affinity", func(t *testing.T) {
		t1 := buildAffinityTask("w0", "worker", api.BuildResourceList("4", "8Gi"), "pg1")
		t2 := buildAffinityTask("w1", "worker", api.BuildResourceList("4", "8Gi"), "pg1")

		got := ExpandExcludedGangNodes(allNodes, sets.New("n1"), []*api.TaskInfo{t1}, []*api.TaskInfo{t1, t2})
		assert.Equal(t, sets.New("n1"), got)
	})

	t.Run("does not expand when chosen nodes span multiple domains", func(t *testing.T) {
		t1 := buildAffinityTask("w0", "worker", api.BuildResourceList("4", "8Gi"), "pg1", "zone")
		t2 := buildAffinityTask("w1", "worker", api.BuildResourceList("4", "8Gi"), "pg1", "zone")

		got := ExpandExcludedGangNodes(allNodes, sets.New("n1", "n4"), []*api.TaskInfo{t1}, []*api.TaskInfo{t1, t2})
		assert.Equal(t, sets.New("n1", "n4"), got)
	})
}
