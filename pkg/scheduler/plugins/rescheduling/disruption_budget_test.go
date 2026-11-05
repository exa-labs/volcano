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

import (
	"testing"
	"time"

	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"volcano.sh/volcano/pkg/scheduler/api"
)

// budget builds a PodDisruptionBudget in the default namespace selecting
// app=<app> that currently allows `allowed` disruptions.
func budget(name, app string, allowed int32) *policyv1.PodDisruptionBudget {
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: policyv1.PodDisruptionBudgetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": app}},
		},
		Status: policyv1.PodDisruptionBudgetStatus{DisruptionsAllowed: allowed},
	}
}

func withApp(task *api.TaskInfo, app string) *api.TaskInfo {
	task.Pod.Labels["app"] = app
	return task
}

func taskNamed(name string, app string) *api.TaskInfo {
	pod := tierPod(name, "n", "pg-"+name, 1, -1, time.Hour)
	pod.Labels["app"] = app
	return api.NewTaskInfo(pod)
}

func TestDisruptionBudgetsSpendAcrossAPass(t *testing.T) {
	b := newDisruptionBudgets([]*policyv1.PodDisruptionBudget{budget("one", "web", 1)})
	a, c := taskNamed("a", "web"), taskNamed("c", "web")
	other := taskNamed("o", "batch")

	if !b.allows([]*api.TaskInfo{a}) {
		t.Fatalf("one disruption allowed: a single eviction must fit")
	}
	if b.allows([]*api.TaskInfo{a, c}) {
		t.Fatalf("one disruption allowed: two evictions together must not fit")
	}
	b.take([]*api.TaskInfo{a})
	if b.allows([]*api.TaskInfo{c}) || !b.protects(c) {
		t.Fatalf("budget spent by a: c must be protected")
	}
	if !b.allows([]*api.TaskInfo{other}) || b.protects(other) {
		t.Fatalf("a pod no budget selects is never protected")
	}
	if got := b.admit([]*api.TaskInfo{c, other}); len(got) != 1 || got[0] != other {
		t.Fatalf("admit: expected only the unselected pod, got %v", names(got))
	}
}

func TestDisruptionBudgetsIgnoreAlreadyDisruptedPodsAndEmptySelectors(t *testing.T) {
	spent := budget("spent", "web", 0)
	spent.Status.DisruptedPods = map[string]metav1.Time{"a": metav1.NewTime(testNow)}
	empty := budget("empty", "web", 0)
	empty.Spec.Selector = &metav1.LabelSelector{}
	b := newDisruptionBudgets([]*policyv1.PodDisruptionBudget{spent, empty})

	if !b.allows([]*api.TaskInfo{taskNamed("a", "web")}) {
		t.Fatalf("a pod the budget already counts as disrupted is not charged again")
	}
	if b.allows([]*api.TaskInfo{taskNamed("c", "web")}) {
		t.Fatalf("a budget allowing no disruption protects the pods it selects")
	}
	var none *disruptionBudgets
	if !none.allows([]*api.TaskInfo{taskNamed("c", "web")}) || none.protects(taskNamed("c", "web")) {
		t.Fatalf("without budgets every eviction is allowed")
	}
}

// A drain whose victim a budget protects is skipped, and the pass carries
// on with the next source instead.
func TestGpuFragmentationSkipsBudgetProtectedSourceForTheNext(t *testing.T) {
	f := newFixture(t)
	first := f.addNode(gpuNode("first", 8, nil))
	second := f.addNode(gpuNode("second", 8, nil))
	dest := f.addNode(gpuNode("dest", 8, nil))
	f.placePod(t, first, gpuPod("protected", "first", 1, map[string]string{"app": "web"}, nil, true), 1, "")
	f.placePod(t, second, gpuPod("movable", "second", 2, map[string]string{"app": "batch"}, nil, true), 1, "")
	f.placePod(t, dest, gpuPod("resident", "dest", 5, nil, nil, true), 1, "")
	f.budgets = newDisruptionBudgets([]*policyv1.PodDisruptionBudget{budget("web", "web", 0)})

	plans := f.plan(newGpuFragmentationConf(), nil)
	for _, plan := range plans {
		if plan.victim.Name == "protected" {
			t.Fatalf("drained a pod its PodDisruptionBudget protects: %+v", plans)
		}
	}
	if len(plans) != 1 || plans[0].victim.Name != "movable" || plans[0].source != "second" {
		t.Fatalf("expected the next source drained instead, got %+v", plans)
	}
}

// Two victims under one budget that allows a single disruption: only one
// drain in the pass may spend it.
func TestGpuFragmentationSpendsABudgetOncePerPass(t *testing.T) {
	f := newFixture(t)
	first := f.addNode(gpuNode("first", 8, nil))
	second := f.addNode(gpuNode("second", 8, nil))
	destA := f.addNode(gpuNode("dest-a", 8, nil))
	destB := f.addNode(gpuNode("dest-b", 8, nil))
	f.placePod(t, first, gpuPod("web-1", "first", 1, map[string]string{"app": "web"}, nil, true), 1, "")
	f.placePod(t, second, gpuPod("web-2", "second", 1, map[string]string{"app": "web"}, nil, true), 1, "")
	f.placePod(t, destA, gpuPod("resident-a", "dest-a", 6, nil, nil, true), 1, "")
	f.placePod(t, destB, gpuPod("resident-b", "dest-b", 6, nil, nil, true), 1, "")

	if plans := f.plan(newGpuFragmentationConf(), nil); len(plans) != 2 {
		t.Fatalf("without budgets both sources drain, got %+v", plans)
	}
	f.budgets = newDisruptionBudgets([]*policyv1.PodDisruptionBudget{budget("web", "web", 1)})
	if plans := f.plan(newGpuFragmentationConf(), nil); len(plans) != 1 {
		t.Fatalf("one disruption allowed: expected one drain, got %+v", plans)
	}
}

// capacityFixture: one idle reserved GPU, wanted by two spot pods. The
// protected pod ranks first (higher priority) and is selected by a budget.
func capacityFixture(t *testing.T, allowed int32) (*fixture, *api.TaskInfo, *api.TaskInfo) {
	f := newFixture(t)
	f.addNode(tierNode("spot-1", "spot", "a"))
	f.addNode(tierNode("reserved-1", "reserved", "a"))
	f.placeGroup(t, 1, nil, tierPod("resident", "reserved-1", "pg-resident", 7, 0, time.Hour))
	protectedPod := tierPod("serving", "spot-1", "pg-serving", 1, -2, time.Hour)
	protectedPod.Labels["app"] = "web"
	protected := f.placeGroup(t, 1, nil, protectedPod)
	batch := f.placeGroup(t, 1, nil, tierPod("batch", "spot-1", "pg-batch", 1, -4, time.Hour))
	f.budgets = newDisruptionBudgets([]*policyv1.PodDisruptionBudget{budget("web", "web", allowed)})
	var p, b *api.TaskInfo
	for _, task := range protected.Tasks {
		p = task
	}
	for _, task := range batch.Tasks {
		b = task
	}
	return f, p, b
}

// The mover a budget protects is passed over; the next candidate takes the
// idle reserved GPU.
func TestUpgradeSkipsBudgetProtectedMoverForTheNext(t *testing.T) {
	f, _, _ := capacityFixture(t, 0)
	plans := f.planUpgrades(liveConf(), nil)
	if len(plans) != 1 {
		t.Fatalf("expected 1 plan, got %d", len(plans))
	}
	if got := plans[0].job.PodGroup.Name; got != "pg-batch" {
		t.Fatalf("expected the unprotected pod moved, got %s", got)
	}

	f.budgets = nil
	plans = f.planUpgrades(liveConf(), nil)
	if len(plans) != 1 || plans[0].job.PodGroup.Name != "pg-serving" {
		t.Fatalf("without budgets the higher-priority pod moves first, got %+v", plans)
	}
}

// A move planned while the budget allowed it waits, held, once the budget
// no longer does, and evicts the mover when the budget allows it again.
func TestUpgradeMoveWaitsWhileBudgetProtectsMover(t *testing.T) {
	f, mover, _ := capacityFixture(t, 1)
	store := newMemStore(f)
	plans := f.startOnly(t, liveConf(), store)
	if len(plans) != 1 || plans[0].job.PodGroup.Name != "pg-serving" {
		t.Fatalf("expected the move started for pg-serving, got %+v", plans)
	}

	f.budgets = newDisruptionBudgets([]*policyv1.PodDisruptionBudget{budget("web", "web", 0)})
	steps, evictions := f.advance(liveConf(), store, testNow.Add(time.Minute), nil)
	if len(evictions) != 0 {
		t.Fatalf("evicted %v while its PodDisruptionBudget allows no disruption", names(evictions))
	}
	for _, step := range steps {
		if step.outcome == "evicting" || step.hold.EvictedAt != "" {
			t.Fatalf("move advanced to eviction under a protecting budget: %+v", step)
		}
	}

	f.budgets = newDisruptionBudgets([]*policyv1.PodDisruptionBudget{budget("web", "web", 1)})
	_, evictions = f.advance(liveConf(), store, testNow.Add(2*time.Minute), nil)
	if len(evictions) != 1 || !sameTask(evictions[0], mover) {
		t.Fatalf("expected the mover evicted once its budget allows it, got %v", names(evictions))
	}
}
