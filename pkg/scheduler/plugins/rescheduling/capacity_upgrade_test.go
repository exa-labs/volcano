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
	"errors"
	"fmt"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/util"
)

const (
	capacityTypeLabel = "karpenter.sh/capacity-type"
	zoneLabel         = "topology.kubernetes.io/zone"
)

var testNow = time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)

// tierNode builds an 8-GPU node on the given capacity type and zone.
func tierNode(name, capacityType, zone string) *api.NodeInfo {
	labels := map[string]string{"karpenter.sh/nodepool": testPool, zoneLabel: zone}
	if capacityType != "" {
		labels[capacityTypeLabel] = capacityType
	}
	node := util.BuildNode(name, v1.ResourceList{
		"cpu":            *apiResource("64"),
		"memory":         *apiResource("512Gi"),
		"nvidia.com/gpu": *apiResource("8"),
		"pods":           *apiResource("110"),
	}, labels)
	return api.NewNodeInfo(node)
}

// tierPod builds a running, controller-owned GPU pod in PodGroup group with
// the given priority, started age ago.
func tierPod(name, nodeName, group string, gpus int64, priority int32, age time.Duration) *v1.Pod {
	pod := util.BuildPod("default", name, nodeName, v1.PodRunning, v1.ResourceList{
		"cpu":            *apiResource("4"),
		"memory":         *apiResource("16Gi"),
		"nvidia.com/gpu": *apiResource(fmt.Sprintf("%d", gpus)),
	}, group, map[string]string{}, nil)
	controller := true
	pod.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "owner", UID: types.UID("owner-" + group), Controller: &controller,
	}}
	pod.Spec.Priority = &priority
	started := metav1.NewTime(testNow.Add(-age))
	pod.Status.StartTime = &started
	return pod
}

// placeGroup binds pods to their nodes and registers them as one job whose
// PodGroup has minMember members and the given annotations.
func (f *fixture) placeGroup(t *testing.T, minMember int32, annotations map[string]string, pods ...*v1.Pod) *api.JobInfo {
	var job *api.JobInfo
	for _, pod := range pods {
		node, ok := f.nodes[pod.Spec.NodeName]
		if !ok {
			t.Fatalf("pod %s placed on unknown node %s", pod.Name, pod.Spec.NodeName)
		}
		task := api.NewTaskInfo(pod)
		if err := node.AddTask(task); err != nil {
			t.Fatalf("AddTask(%s): %v", pod.Name, err)
		}
		if job == nil {
			job = api.NewJobInfo(task.Job, task)
			pg := &api.PodGroup{PodGroup: scheduling.PodGroup{
				ObjectMeta: metav1.ObjectMeta{Name: string(task.Job), Namespace: pod.Namespace, Annotations: annotations},
				Spec:       scheduling.PodGroupSpec{MinMember: minMember, Queue: "default"},
			}}
			job.SetPodGroup(pg)
			f.jobs[task.Job] = job
		} else {
			job.AddTaskInfo(task)
		}
		f.running[pod.UID] = task
	}
	return job
}

func (f *fixture) planUpgrades(conf *capacityUpgradeConf, predicate capacityUpgradePredicate) []capacityUpgradePlan {
	return planCapacityUpgrades(f.nodes, f.jobs, f.running, conf, testNow, predicate)
}

func TestUpgradeSinglePodMovesToIdleReserved(t *testing.T) {
	f := newFixture(t)
	f.addNode(tierNode("spot-1", "spot", "a"))
	f.addNode(tierNode("reserved-1", "reserved", "a"))
	f.placeGroup(t, 1, nil, tierPod("train", "spot-1", "pg-train", 1, -4, time.Hour))

	plans := f.planUpgrades(newCapacityUpgradeConf(), nil)
	if len(plans) != 1 {
		t.Fatalf("expected 1 plan, got %d", len(plans))
	}
	p := plans[0]
	if p.gang || p.proposal.Target != "reserved" || p.proposal.From != "spot" || p.proposal.Preempting != 0 {
		t.Fatalf("unexpected plan: %+v", p.proposal)
	}
	if len(p.proposal.Nodes) != 1 || p.proposal.Nodes[0] != "reserved-1" || p.proposal.Gpus != 1 {
		t.Fatalf("unexpected placement: %+v", p.proposal)
	}
}

func TestUpgradeCountsLowerPriorityFillerAsRoom(t *testing.T) {
	f := newFixture(t)
	f.addNode(tierNode("spot-1", "spot", "a"))
	f.addNode(tierNode("reserved-1", "reserved", "a"))
	f.placeGroup(t, 1, nil, tierPod("train", "spot-1", "pg-train", 4, -4, time.Hour))
	for i := 0; i < 8; i++ {
		name := fmt.Sprintf("filler-%d", i)
		f.placeGroup(t, 1, nil, tierPod(name, "reserved-1", "pg-"+name, 1, -9, time.Hour))
	}

	plans := f.planUpgrades(newCapacityUpgradeConf(), nil)
	if len(plans) != 1 {
		t.Fatalf("expected 1 plan, got %d", len(plans))
	}
	if plans[0].proposal.Preempting != 4 || plans[0].proposal.Nodes[0] != "reserved-1" {
		t.Fatalf("expected 4 preemptions on reserved-1, got %+v", plans[0].proposal)
	}
}

func TestUpgradeDoesNotPreemptEqualOrHigherPriority(t *testing.T) {
	f := newFixture(t)
	f.addNode(tierNode("spot-1", "spot", "a"))
	f.addNode(tierNode("reserved-1", "reserved", "a"))
	f.placeGroup(t, 1, nil, tierPod("train", "spot-1", "pg-train", 4, -4, time.Hour))
	f.placeGroup(t, 1, nil, tierPod("peer", "reserved-1", "pg-peer", 6, -4, time.Hour))
	f.placeGroup(t, 1, nil, tierPod("boss", "reserved-1", "pg-boss", 2, -2, time.Hour))

	if plans := f.planUpgrades(newCapacityUpgradeConf(), nil); len(plans) != 0 {
		t.Fatalf("expected no plan, got %+v", plans)
	}
}

func TestUpgradeGangIsProposedNotEvictedAndStaysInOneZone(t *testing.T) {
	f := newFixture(t)
	f.addNode(tierNode("spot-1", "spot", "a"))
	f.addNode(tierNode("spot-2", "spot", "a"))
	f.addNode(tierNode("reserved-a", "reserved", "a"))
	f.addNode(tierNode("reserved-b", "reserved", "b"))
	f.placeGroup(t, 2, nil,
		tierPod("w0", "spot-1", "pg-gang", 8, -4, time.Hour),
		tierPod("w1", "spot-2", "pg-gang", 8, -4, time.Hour))

	// One 8-GPU node per zone: the gang needs two nodes in one zone.
	if plans := f.planUpgrades(newCapacityUpgradeConf(), nil); len(plans) != 0 {
		t.Fatalf("expected no plan across zones, got %+v", plans)
	}

	f.addNode(tierNode("reserved-a2", "reserved", "a"))
	plans := f.planUpgrades(newCapacityUpgradeConf(), nil)
	if len(plans) != 1 {
		t.Fatalf("expected 1 plan, got %d", len(plans))
	}
	p := plans[0]
	if !p.gang || p.proposal.Zone != "a" || p.proposal.Members != 2 || p.proposal.Gpus != 16 {
		t.Fatalf("unexpected gang plan: %+v", p.proposal)
	}
	if len(p.proposal.Nodes) != 2 || p.proposal.Nodes[0] != "reserved-a" || p.proposal.Nodes[1] != "reserved-a2" {
		t.Fatalf("unexpected nodes: %v", p.proposal.Nodes)
	}
}

func TestUpgradeGangRelaxesPeerAffinityInPredicate(t *testing.T) {
	f := newFixture(t)
	f.addNode(tierNode("spot-1", "spot", "a"))
	f.addNode(tierNode("reserved-1", "reserved", "a"))
	f.addNode(tierNode("reserved-2", "reserved", "a"))
	f.placeGroup(t, 2, nil,
		tierPod("w0", "spot-1", "pg-gang", 4, -4, time.Hour),
		tierPod("w1", "spot-1", "pg-gang", 4, -4, time.Hour))

	seenGang := false
	plans := f.planUpgrades(newCapacityUpgradeConf(), func(task *api.TaskInfo, node *api.NodeInfo, gang bool) error {
		seenGang = seenGang || gang
		return nil
	})
	if len(plans) != 1 || !seenGang {
		t.Fatalf("expected gang plan with gang predicate, got %d plans (gang=%v)", len(plans), seenGang)
	}
}

func TestUpgradePredicateFailureVetoesNode(t *testing.T) {
	f := newFixture(t)
	f.addNode(tierNode("spot-1", "spot", "a"))
	f.addNode(tierNode("reserved-1", "reserved", "a"))
	f.placeGroup(t, 1, nil, tierPod("train", "spot-1", "pg-train", 1, -4, time.Hour))

	plans := f.planUpgrades(newCapacityUpgradeConf(), func(task *api.TaskInfo, node *api.NodeInfo, gang bool) error {
		return errors.New("taint")
	})
	if len(plans) != 0 {
		t.Fatalf("expected predicate veto, got %+v", plans)
	}
}

func TestUpgradeSkipsCooldownYoungAndOptedOut(t *testing.T) {
	f := newFixture(t)
	f.addNode(tierNode("spot-1", "spot", "a"))
	f.addNode(tierNode("reserved-1", "reserved", "a"))
	f.placeGroup(t, 1, map[string]string{CapacityUpgradeLastAnnotation: testNow.Add(-time.Minute).Format(time.RFC3339)},
		tierPod("cooling", "spot-1", "pg-cooling", 1, -4, time.Hour))
	f.placeGroup(t, 1, nil, tierPod("young", "spot-1", "pg-young", 1, -4, time.Minute))
	optOut := tierPod("optout", "spot-1", "pg-optout", 1, -4, time.Hour)
	optOut.Labels["exa.ai/capacity-upgrade-eligible"] = "false"
	f.placeGroup(t, 1, nil, optOut)
	protected := tierPod("protected", "spot-1", "pg-protected", 1, -4, time.Hour)
	protected.Annotations[doNotDisruptAnnotation] = "true"
	f.placeGroup(t, 1, nil, protected)
	f.placeGroup(t, 1, nil, tierPod("high", "spot-1", "pg-high", 1, 0, time.Hour))

	if plans := f.planUpgrades(newCapacityUpgradeConf(), nil); len(plans) != 0 {
		t.Fatalf("expected no plan, got %+v", plans)
	}

	// An expired clock unlocks the group.
	f.jobs["default/pg-cooling"].PodGroup.Annotations[CapacityUpgradeLastAnnotation] = testNow.Add(-time.Hour).Format(time.RFC3339)
	plans := f.planUpgrades(newCapacityUpgradeConf(), nil)
	if len(plans) != 1 || plans[0].members[0].Name != "cooling" {
		t.Fatalf("expected cooled pod to move, got %+v", plans)
	}
}

func TestUpgradeLedgerPreventsDoubleBooking(t *testing.T) {
	f := newFixture(t)
	f.addNode(tierNode("spot-1", "spot", "a"))
	f.addNode(tierNode("reserved-1", "reserved", "a"))
	f.placeGroup(t, 1, nil, tierPod("a", "spot-1", "pg-a", 4, -4, time.Hour))
	f.placeGroup(t, 1, nil, tierPod("b", "spot-1", "pg-b", 4, -4, time.Hour))
	// Two 2-GPU fillers: only one mover's worth of preemptable room.
	f.placeGroup(t, 1, nil, tierPod("f0", "reserved-1", "pg-f0", 2, -9, time.Hour))
	f.placeGroup(t, 1, nil, tierPod("f1", "reserved-1", "pg-f1", 2, -9, time.Hour))
	f.placeGroup(t, 1, nil, tierPod("stay", "reserved-1", "pg-stay", 4, -4, time.Hour))

	plans := f.planUpgrades(newCapacityUpgradeConf(), nil)
	if len(plans) != 1 {
		t.Fatalf("expected exactly 1 plan, got %d", len(plans))
	}
	if plans[0].proposal.Preempting != 2 {
		t.Fatalf("expected 2 preemptions, got %+v", plans[0].proposal)
	}
}

func TestUpgradeOrdersHighestPriorityThenOldest(t *testing.T) {
	f := newFixture(t)
	f.addNode(tierNode("spot-1", "spot", "a"))
	f.addNode(tierNode("reserved-1", "reserved", "a"))
	f.placeGroup(t, 1, nil, tierPod("low-old", "spot-1", "pg-low-old", 4, -6, 3*time.Hour))
	f.placeGroup(t, 1, nil, tierPod("high-new", "spot-1", "pg-high-new", 4, -4, time.Hour))
	f.placeGroup(t, 1, nil, tierPod("high-old", "spot-1", "pg-high-old", 4, -4, 2*time.Hour))
	// Room for two movers.
	conf := newCapacityUpgradeConf()
	plans := f.planUpgrades(conf, nil)
	if len(plans) != 2 {
		t.Fatalf("expected 2 plans, got %d", len(plans))
	}
	if plans[0].members[0].Name != "high-old" || plans[1].members[0].Name != "high-new" {
		t.Fatalf("unexpected order: %s, %s", plans[0].members[0].Name, plans[1].members[0].Name)
	}
}

func TestUpgradeRespectsPerPassBudgets(t *testing.T) {
	f := newFixture(t)
	f.addNode(tierNode("spot-1", "spot", "a"))
	f.addNode(tierNode("reserved-1", "reserved", "a"))
	for i := 0; i < 4; i++ {
		name := fmt.Sprintf("p%d", i)
		f.placeGroup(t, 1, nil, tierPod(name, "spot-1", "pg-"+name, 1, -4, time.Hour))
	}
	conf := newCapacityUpgradeConf()
	conf.MaxVictims = 2
	if plans := f.planUpgrades(conf, nil); len(plans) != 2 {
		t.Fatalf("expected victim budget of 2, got %d", len(plans))
	}

	g := newFixture(t)
	g.addNode(tierNode("spot-1", "spot", "a"))
	g.addNode(tierNode("spot-2", "spot", "a"))
	for i := 0; i < 8; i++ {
		g.addNode(tierNode(fmt.Sprintf("reserved-%d", i), "reserved", "a"))
	}
	for i := 0; i < 3; i++ {
		group := fmt.Sprintf("pg-gang%d", i)
		g.placeGroup(t, 2, nil,
			tierPod(group+"-w0", "spot-1", group, 2, -4, time.Hour),
			tierPod(group+"-w1", "spot-2", group, 2, -4, time.Hour))
	}
	conf = newCapacityUpgradeConf()
	conf.MaxGangProposals = 1
	if plans := g.planUpgrades(conf, nil); len(plans) != 1 || !plans[0].gang {
		t.Fatalf("expected 1 gang proposal, got %+v", plans)
	}
}

func TestUpgradeIgnoresGroupsAlreadyOnCheapestTier(t *testing.T) {
	f := newFixture(t)
	f.addNode(tierNode("reserved-1", "reserved", "a"))
	f.addNode(tierNode("reserved-2", "reserved", "a"))
	f.addNode(tierNode("owned", "", "a"))
	f.placeGroup(t, 1, nil, tierPod("home", "reserved-1", "pg-home", 1, -4, time.Hour))
	f.placeGroup(t, 1, nil, tierPod("sunk", "owned", "pg-sunk", 1, -4, time.Hour))

	if plans := f.planUpgrades(newCapacityUpgradeConf(), nil); len(plans) != 0 {
		t.Fatalf("expected no plan, got %+v", plans)
	}
}

func TestUpgradeMixedGangCountsOwnReservedMembersAsRoom(t *testing.T) {
	f := newFixture(t)
	f.addNode(tierNode("spot-1", "spot", "a"))
	f.addNode(tierNode("reserved-1", "reserved", "a"))
	f.addNode(tierNode("reserved-2", "reserved", "a"))
	// w0 already occupies all of reserved-1; the gang needs both reserved
	// nodes, which only works if w0's own GPUs count as freeable.
	f.placeGroup(t, 2, nil,
		tierPod("w0", "reserved-1", "pg-gang", 8, -4, time.Hour),
		tierPod("w1", "spot-1", "pg-gang", 8, -4, time.Hour))

	plans := f.planUpgrades(newCapacityUpgradeConf(), nil)
	if len(plans) != 1 || plans[0].proposal.Preempting != 0 || len(plans[0].proposal.Nodes) != 2 {
		t.Fatalf("expected gang to fit both reserved nodes without preemption, got %+v", plans)
	}
}

func TestUpgradeConfParseFallsBackOnBadParams(t *testing.T) {
	conf := newCapacityUpgradeConf()
	conf.parse(map[string]interface{}{"cooldownSeconds": "not-a-number"})
	if conf.CooldownSeconds != 1800 || !conf.DryRun {
		t.Fatalf("expected defaults after bad params, got %+v", conf)
	}
	conf.parse(map[string]interface{}{"dryRun": false, "maxGangProposals": 5, "order": "reserved,spot"})
	if conf.DryRun || conf.MaxGangProposals != 5 || conf.Order != "reserved,spot" {
		t.Fatalf("unexpected parsed conf: %+v", conf)
	}
}
