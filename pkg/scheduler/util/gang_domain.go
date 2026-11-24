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
	"maps"
	"reflect"
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
// It always includes chosen. When every task in gangTasks shares the exact same
// resource request, role, node selector, and affinity constraints (with no pod
// anti-affinity or topology spread constraints), and every required pod
// affinity term matches all placed tasks on topology keys whose values agree
// across chosen, it also excludes every other node in allNodes sharing that
// topology tuple. Because no narrower affinity, anti-affinity, or per-task
// constraint can subdivide that domain, no reordering inside it can satisfy
// minAvailable. Otherwise it falls back to chosen only.
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

	if !tasksHomogeneousForDomainPruning(placedTasks, gangTasks) {
		return excluded
	}

	topologyKeys := exactSelfAffinityTopologyKeys(placedTasks[0].Pod, placedTasks, gangTasks)
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

func tasksHomogeneousForDomainPruning(placedTasks, gangTasks []*api.TaskInfo) bool {
	anchor := placedTasks[0]
	if anchor == nil || anchor.Pod == nil || anchor.InitResreq == nil {
		return false
	}
	if len(anchor.Pod.Spec.TopologySpreadConstraints) > 0 {
		return false
	}
	if anchor.Pod.Spec.Affinity == nil || anchor.Pod.Spec.Affinity.PodAffinity == nil {
		return false
	}
	if anchor.Pod.Spec.Affinity.PodAntiAffinity != nil {
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
			if len(t.Pod.Spec.TopologySpreadConstraints) > 0 {
				return false
			}
			if !maps.Equal(t.Pod.Spec.NodeSelector, anchor.Pod.Spec.NodeSelector) {
				return false
			}
			if !reflect.DeepEqual(t.Pod.Spec.Affinity, anchor.Pod.Spec.Affinity) {
				return false
			}
		}
	}
	return true
}

// exactSelfAffinityTopologyKeys returns the topology keys of pod's required pod
// affinity terms if and only if every required term is self-matching across all
// placedTasks and gangTasks. Any non-self or namespace-selector term aborts
// domain expansion so a narrower external constraint cannot be pruned over.
func exactSelfAffinityTopologyKeys(pod *v1.Pod, placedTasks, gangTasks []*api.TaskInfo) []string {
	terms := pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if len(terms) == 0 {
		return nil
	}
	keys := sets.New[string]()
	for _, term := range terms {
		if term.TopologyKey == "" || term.LabelSelector == nil || term.NamespaceSelector != nil {
			return nil
		}
		sel, err := metav1.LabelSelectorAsSelector(term.LabelSelector)
		if err != nil {
			return nil
		}
		for _, group := range [][]*api.TaskInfo{placedTasks, gangTasks} {
			for _, t := range group {
				if len(term.Namespaces) > 0 && !slices.Contains(term.Namespaces, t.Namespace) {
					return nil
				}
				if !sel.Matches(labels.Set(t.Pod.Labels)) {
					return nil
				}
			}
		}
		keys.Insert(term.TopologyKey)
	}
	return sets.List(keys)
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
