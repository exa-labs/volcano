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

// Disruption budgets for rescheduling.
//
// The scheduler evicts through the Eviction API, so the API server refuses
// an eviction a PodDisruptionBudget does not allow. The strategies consult
// the same budgets while planning, so a pass skips a victim its budget
// protects and moves on to the next candidate instead of planning a move
// the API server will refuse.

import (
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
)

// disruptionBudget is one PodDisruptionBudget and the voluntary disruptions
// it still allows in this pass.
type disruptionBudget struct {
	pdb      *policyv1.PodDisruptionBudget
	selector labels.Selector
	allowed  int32
}

// disruptionBudgets is the PodDisruptionBudget view of one rescheduling
// pass: every budget's remaining allowance, spent as the pass plans
// evictions. A nil *disruptionBudgets allows every eviction, for callers
// that run without a budget source.
type disruptionBudgets struct {
	budgets []*disruptionBudget
}

// newDisruptionBudgets indexes the budgets for one pass. A budget whose
// selector does not parse, or is empty, selects nothing (the Eviction API
// treats both the same way).
func newDisruptionBudgets(pdbs []*policyv1.PodDisruptionBudget) *disruptionBudgets {
	b := &disruptionBudgets{budgets: make([]*disruptionBudget, 0, len(pdbs))}
	for _, pdb := range pdbs {
		if pdb == nil {
			continue
		}
		selector, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
		if err != nil || selector.Empty() {
			continue
		}
		b.budgets = append(b.budgets, &disruptionBudget{
			pdb:      pdb,
			selector: selector,
			allowed:  pdb.Status.DisruptionsAllowed,
		})
	}
	return b
}

// sessionDisruptionBudgets reads the cluster's PodDisruptionBudgets from
// the session's informer. Without one, or when the list fails, it returns
// nil, which allows every eviction: the Eviction API still enforces the
// budgets, so planning without them only costs refused evictions.
func sessionDisruptionBudgets() *disruptionBudgets {
	if Session == nil || Session.InformerFactory() == nil {
		return nil
	}
	pdbs, err := Session.InformerFactory().Policy().V1().PodDisruptionBudgets().Lister().List(labels.Everything())
	if err != nil {
		klog.Errorf("rescheduling: list PodDisruptionBudgets: %v", err)
		return nil
	}
	return newDisruptionBudgets(pdbs)
}

// covering returns the budgets that would count an eviction of pod. A pod
// the budget already lists as disrupted has been counted by the API server.
func (b *disruptionBudgets) covering(pod *v1.Pod) []*disruptionBudget {
	if b == nil || pod == nil || len(pod.Labels) == 0 {
		return nil
	}
	covering := make([]*disruptionBudget, 0)
	for _, budget := range b.budgets {
		if budget.pdb.Namespace != pod.Namespace || !budget.selector.Matches(labels.Set(pod.Labels)) {
			continue
		}
		if _, disrupted := budget.pdb.Status.DisruptedPods[pod.Name]; disrupted {
			continue
		}
		covering = append(covering, budget)
	}
	return covering
}

// allows reports whether evicting every task together stays within each
// budget's remaining allowance.
func (b *disruptionBudgets) allows(tasks []*api.TaskInfo) bool {
	if b == nil {
		return true
	}
	spend := map[*disruptionBudget]int32{}
	for _, task := range tasks {
		for _, budget := range b.covering(task.Pod) {
			spend[budget]++
			if spend[budget] > budget.allowed {
				klog.V(3).Infof("rescheduling: %s/%s is protected by PodDisruptionBudget %s/%s (%d disruptions allowed)",
					task.Namespace, task.Name, budget.pdb.Namespace, budget.pdb.Name, budget.allowed)
				return false
			}
		}
	}
	return true
}

// protects reports whether a budget covering the task allows no further
// disruption.
func (b *disruptionBudgets) protects(task *api.TaskInfo) bool {
	for _, budget := range b.covering(task.Pod) {
		if budget.allowed <= 0 {
			return true
		}
	}
	return false
}

// take spends the allowance the tasks' evictions use, so later plans in the
// pass see what is left.
func (b *disruptionBudgets) take(tasks []*api.TaskInfo) {
	if b == nil {
		return
	}
	for _, task := range tasks {
		for _, budget := range b.covering(task.Pod) {
			budget.allowed--
		}
	}
}

// admit returns the tasks whose evictions fit the remaining allowance,
// taken in order and spent as they are admitted.
func (b *disruptionBudgets) admit(tasks []*api.TaskInfo) []*api.TaskInfo {
	if b == nil {
		return tasks
	}
	admitted := make([]*api.TaskInfo, 0, len(tasks))
	for _, task := range tasks {
		one := []*api.TaskInfo{task}
		if !b.allows(one) {
			continue
		}
		b.take(one)
		admitted = append(admitted, task)
	}
	return admitted
}
