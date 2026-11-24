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
	"slices"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"

	"volcano.sh/volcano/pkg/scheduler/api"
)

// ExpandExcludedGangNodes returns the node names to exclude on the next retry
// after a gang placement attempt placed tasks on chosen without reaching
// minAvailable.
//
// It always includes chosen. When every task in gangTasks shares the same
// resource request and task role, and every gang task carries required
// pod-affinity terms matching the placed tasks on the same topology keys whose
// label values agree across chosen, it also excludes every other node in
// allNodes sharing that topology tuple. No reordering inside an exhausted
// homogeneous affinity domain can satisfy minAvailable, so excluding the whole
// domain prevents spending retries on remaining nodes in the same dead end.
func ExpandExcludedGangNodes(
	allNodes []*api.NodeInfo,
	chosen sets.Set[string],
	placedTasks []*api.TaskInfo,
	gangTasks []*api.TaskInfo,
) sets.Set[string] {
	excluded := chosen.Clone()
	if chosen.Len() == 0 || len(placedTasks) == 0 || len(gangTasks) == 0 {
		return excluded
	}

	if !tasksHomogeneous(placedTasks, gangTasks) {
		return excluded
	}

	topologyKeys := sharedSelfAffinityTopologyKeys(placedTasks, gangTasks)
	if len(topologyKeys) == 0 {
		return excluded
	}

	domainValues, ok := sharedDomainValues(allNodes, chosen, topologyKeys)
	if !ok {
		return excluded
	}

	for _, node := range allNodes {
		if node == nil || node.Node == nil {
			continue
		}
		if matchesDomain(node.Node.Labels, domainValues) {
			excluded.Insert(node.Name)
		}
	}
	return excluded
}

func tasksHomogeneous(placedTasks, gangTasks []*api.TaskInfo) bool {
	anchor := placedTasks[0]
	if anchor == nil || anchor.Pod == nil || anchor.InitResreq == nil {
		return false
	}
	for _, group := range [][]*api.TaskInfo{placedTasks, gangTasks} {
		for _, t := range group {
			if t == nil || t.Pod == nil || t.InitResreq == nil {
				return false
			}
			if t.Namespace != anchor.Namespace || t.TaskRole != anchor.TaskRole {
				return false
			}
			if !t.InitResreq.Equal(anchor.InitResreq, api.Zero) {
				return false
			}
		}
	}
	return true
}

// sharedSelfAffinityTopologyKeys returns topology keys required by every task
// in gangTasks via a required pod affinity term whose selector matches all
// placedTasks.
func sharedSelfAffinityTopologyKeys(placedTasks, gangTasks []*api.TaskInfo) []string {
	var common sets.Set[string]
	for i, task := range gangTasks {
		keys := taskSelfAffinityTopologyKeys(task.Pod, placedTasks)
		if keys.Len() == 0 {
			return nil
		}
		if i == 0 {
			common = keys
		} else {
			common = common.Intersection(keys)
		}
		if common.Len() == 0 {
			return nil
		}
	}
	return sets.List(common)
}

func taskSelfAffinityTopologyKeys(pod *v1.Pod, placedTasks []*api.TaskInfo) sets.Set[string] {
	if pod == nil || pod.Spec.Affinity == nil || pod.Spec.Affinity.PodAffinity == nil {
		return nil
	}
	keys := sets.New[string]()
	for _, term := range pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
		if term.TopologyKey == "" || term.LabelSelector == nil || term.NamespaceSelector != nil {
			continue
		}
		sel, err := metav1.LabelSelectorAsSelector(term.LabelSelector)
		if err != nil {
			continue
		}
		matchesAll := true
		for _, pt := range placedTasks {
			if len(term.Namespaces) > 0 && !slices.Contains(term.Namespaces, pt.Namespace) {
				matchesAll = false
				break
			}
			if !sel.Matches(labels.Set(pt.Pod.Labels)) {
				matchesAll = false
				break
			}
		}
		if matchesAll {
			keys.Insert(term.TopologyKey)
		}
	}
	return keys
}

func sharedDomainValues(allNodes []*api.NodeInfo, chosen sets.Set[string], topologyKeys []string) (map[string]string, bool) {
	values := make(map[string]string, len(topologyKeys))
	seen := 0
	for _, node := range allNodes {
		if node == nil || !chosen.Has(node.Name) {
			continue
		}
		if node.Node == nil {
			return nil, false
		}
		for _, key := range topologyKeys {
			val, exists := node.Node.Labels[key]
			if !exists {
				return nil, false
			}
			if seen == 0 {
				values[key] = val
			} else if values[key] != val {
				return nil, false
			}
		}
		seen++
	}
	if seen != chosen.Len() {
		return nil, false
	}
	return values, true
}

func matchesDomain(nodeLabels map[string]string, domainValues map[string]string) bool {
	for key, want := range domainValues {
		if got, exists := nodeLabels[key]; !exists || got != want {
			return false
		}
	}
	return true
}
