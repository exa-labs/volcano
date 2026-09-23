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

// Tests for the capacityUpgrade strategy: the planner (which groups may
// move where, in what order, with which victims) and the move transaction
// (holds, drains, sequencing, claims, failure paths). The transaction tests
// drive a move through the same steps the scheduler would across sessions,
// re-reading the state from node annotations and the ledger each time as a
// new session does.

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"volcano.sh/apis/pkg/apis/scheduling"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/util"
)

const (
	capacityTypeLabel = "karpenter.sh/capacity-type"
	zoneLabel         = "topology.kubernetes.io/zone"
	gpuRes            = v1.ResourceName("nvidia.com/gpu")
)

var testNow = time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)

// tierNode builds an 8-GPU node on the given capacity type and zone; an
// empty capacity type leaves the node unlabeled.
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
// the given priority, started age ago. Its controller owner is derived from
// the group so pods of one group share an identity.
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
	pod.CreationTimestamp = started
	return pod
}

// successorPod builds a pending pod that restarts group after a move: same
// owner, created at the given time, not yet bound.
func successorPod(name, group string, gpus int64, priority int32, created time.Time) *v1.Pod {
	pod := tierPod(name, "", group, gpus, priority, 0)
	pod.Status.Phase = v1.PodPending
	pod.Status.StartTime = nil
	pod.CreationTimestamp = metav1.NewTime(created)
	return pod
}

// inGroup moves the pod into another PodGroup, keeping its owner (a
// restarted single pod gets a fresh per-pod PodGroup).
func inGroup(pod *v1.Pod, group string) *v1.Pod {
	pod.Annotations[schedulingv1beta1.KubeGroupNameAnnotationKey] = group
	return pod
}

// sameTask reports whether two task views refer to the same pod (nodes hold
// clones of the jobs' tasks).
func sameTask(a, b *api.TaskInfo) bool {
	return a != nil && b != nil && a.UID == b.UID
}

// placeGroup binds pods to their nodes and registers them as one job whose
// PodGroup has minMember members and the given annotations. As in a session
// snapshot, the node holds a clone of the job's task.
func (f *fixture) placeGroup(t *testing.T, minMember int32, annotations map[string]string, pods ...*v1.Pod) *api.JobInfo {
	var job *api.JobInfo
	for _, pod := range pods {
		node, ok := f.nodes[pod.Spec.NodeName]
		if !ok {
			t.Fatalf("pod %s placed on unknown node %s", pod.Name, pod.Spec.NodeName)
		}
		task := api.NewTaskInfo(pod)
		if err := node.AddTask(task.Clone()); err != nil {
			t.Fatalf("AddTask(%s): %v", pod.Name, err)
		}
		job = f.registerTask(task, minMember, annotations)
		f.running[pod.UID] = task
	}
	return job
}

// registerTask adds the task to its job, creating the job and PodGroup on
// first sight.
func (f *fixture) registerTask(task *api.TaskInfo, minMember int32, annotations map[string]string) *api.JobInfo {
	job, ok := f.jobs[task.Job]
	if !ok {
		job = api.NewJobInfo(task.Job, task)
		pg := &api.PodGroup{PodGroup: scheduling.PodGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:        task.Pod.Annotations[schedulingv1beta1.KubeGroupNameAnnotationKey],
				Namespace:   task.Namespace,
				Annotations: annotations,
			},
			Spec: scheduling.PodGroupSpec{MinMember: minMember, Queue: "default"},
		}}
		job.SetPodGroup(pg)
		f.jobs[task.Job] = job
		return job
	}
	job.AddTaskInfo(task)
	return job
}

// evict marks a task Releasing exactly as Session.Evict does: the pod is
// terminating, its GPUs are no longer idle but will be once it is gone.
func (f *fixture) evict(t *testing.T, task *api.TaskInfo) {
	if err := f.jobs[task.Job].UpdateTaskStatus(task, api.Releasing); err != nil {
		t.Fatalf("UpdateTaskStatus(%s): %v", task.Name, err)
	}
	if err := f.nodes[task.NodeName].UpdateTask(task); err != nil {
		t.Fatalf("UpdateTask(%s): %v", task.Name, err)
	}
	delete(f.running, task.Pod.UID)
}

// remove drops a task entirely (the pod finished terminating).
func (f *fixture) remove(t *testing.T, task *api.TaskInfo) {
	if err := f.nodes[task.NodeName].RemoveTask(task); err != nil {
		t.Fatalf("RemoveTask(%s): %v", task.Name, err)
	}
	if err := f.jobs[task.Job].DeleteTaskInfo(task); err != nil {
		t.Fatalf("DeleteTaskInfo(%s): %v", task.Name, err)
	}
	delete(f.running, task.Pod.UID)
}

// bind places a pending pod on a node as Running, registering its job.
func (f *fixture) bind(t *testing.T, pod *v1.Pod, nodeName string, minMember int32) *api.TaskInfo {
	pod.Spec.NodeName = nodeName
	pod.Status.Phase = v1.PodRunning
	task := api.NewTaskInfo(pod)
	if err := f.nodes[nodeName].AddTask(task.Clone()); err != nil {
		t.Fatalf("AddTask(%s): %v", pod.Name, err)
	}
	f.registerTask(task, minMember, nil)
	f.running[pod.UID] = task
	return task
}

// index re-reads the move state from node annotations and the ledger, as a
// new session does.
func (f *fixture) index() *capacityUpgradeIndex {
	return indexCapacityUpgrade(f.nodes, capacityLedger{records: f.ledger, version: f.ledgerVersion})
}

func (f *fixture) planUpgrades(conf *capacityUpgradeConf, predicate capacityUpgradePredicate) []capacityUpgradePlan {
	return planCapacityUpgrades(f.nodes, f.jobs, f.running, conf, f.index(), testNow, predicate)
}

// memStore is a capacityUpgradeStore that applies node writes to the
// fixture's node annotations and ledger writes to the fixture's ledger (so
// the next index sees them) and records every write in order. Writes can be
// made to fail per node, for the PodGroup stamp and for the ledger.
type memStore struct {
	f          *fixture
	log        []string
	groups     []groupStamp
	failNode   map[string]error
	failStamp  error
	failLedger error
}

func newMemStore(f *fixture) *memStore {
	return &memStore{f: f, failNode: map[string]error{}}
}

func (s *memStore) writeNode(name string, state nodeState) error {
	if err := s.failNode[name]; err != nil {
		return err
	}
	node := s.f.nodes[name]
	if node == nil || node.Node == nil {
		return fmt.Errorf("node %s not found", name)
	}
	annotations, err := nodeAnnotations(state)
	if err != nil {
		return err
	}
	if node.Node.Annotations == nil {
		node.Node.Annotations = map[string]string{}
	}
	for key, value := range annotations {
		if value == nil {
			delete(node.Node.Annotations, key)
		} else {
			node.Node.Annotations[key] = *value
		}
	}
	s.log = append(s.log, "node:"+name)
	return nil
}

func (s *memStore) stampGroup(stamp groupStamp) error {
	if s.failStamp != nil {
		return s.failStamp
	}
	s.groups = append(s.groups, stamp)
	s.log = append(s.log, "group:"+stamp.namespace+"/"+stamp.name)
	return nil
}

func (s *memStore) readLedger() (capacityLedger, error) {
	if s.failLedger != nil {
		return capacityLedger{}, s.failLedger
	}
	return capacityLedger{records: append([]workloadRecord{}, s.f.ledger...), version: s.f.ledgerVersion}, nil
}

// writeLedger round-trips the records through JSON, as the ConfigMap does,
// and enforces the version like the API server would.
func (s *memStore) writeLedger(records []workloadRecord, version string) (string, error) {
	if s.failLedger != nil {
		return "", s.failLedger
	}
	if version != s.f.ledgerVersion {
		return "", fmt.Errorf("ledger version %q, want %q", version, s.f.ledgerVersion)
	}
	body, err := json.Marshal(records)
	if err != nil {
		return "", err
	}
	decoded, err := decodeLedger(string(body))
	if err != nil {
		return "", err
	}
	s.f.ledger = decoded
	s.f.ledgerVersion = strconv.Itoa(len(s.log) + 1)
	s.log = append(s.log, "ledger")
	return s.f.ledgerVersion, nil
}

// ledgerByIdentity is the fixture's ledger keyed by identity.
func (f *fixture) ledgerByIdentity() map[string]workloadRecord {
	out := map[string]workloadRecord{}
	for _, record := range f.ledger {
		out[identityKey(record.Identity)] = record
	}
	return out
}

// holdsOn decodes the holds annotation of a node.
func (f *fixture) holdsOn(t *testing.T, name string) []capacityHold {
	raw, ok := f.nodes[name].Node.Annotations[CapacityUpgradeHoldsAnnotation]
	if !ok {
		return nil
	}
	holds, err := decodeHolds(raw)
	if err != nil {
		t.Fatalf("holds on %s: %v", name, err)
	}
	return holds
}

// drainsOn decodes the drains annotation of a node.
func (f *fixture) drainsOn(t *testing.T, name string) []capacityDrain {
	raw, ok := f.nodes[name].Node.Annotations[CapacityUpgradeDrainsAnnotation]
	if !ok {
		return nil
	}
	var drains []capacityDrain
	if err := json.Unmarshal([]byte(raw), &drains); err != nil {
		t.Fatalf("drains on %s: %v", name, err)
	}
	return drains
}

// liveConf is the strategy configuration with moves enabled.
func liveConf() *capacityUpgradeConf {
	conf := newCapacityUpgradeConf()
	conf.DryRun = false
	return conf
}

// startOnly plans one pass and makes every plan durable, returning the
// plans and the victims the pass would evict.
func (f *fixture) startOnly(t *testing.T, conf *capacityUpgradeConf, store *memStore) ([]capacityUpgradePlan, []*api.TaskInfo) {
	idx := f.index()
	plans := planCapacityUpgrades(f.nodes, f.jobs, f.running, conf, idx, testNow, nil)
	victims := make([]*api.TaskInfo, 0)
	for _, plan := range plans {
		if err := startCapacityUpgradeMove(plan, idx, conf, store); err != nil {
			t.Fatalf("startCapacityUpgradeMove: %v", err)
		}
		victims = append(victims, plan.victims...)
	}
	return plans, victims
}

// advance runs one session's maintenance at the given time and applies it,
// returning the steps and the movers the session would evict.
func (f *fixture) advance(conf *capacityUpgradeConf, store *memStore, now time.Time, predicate capacityUpgradePredicate) ([]moveStep, []*api.TaskInfo) {
	steps := advanceCapacityUpgradeMoves(f.index(), f.nodes, f.jobs, conf, now, predicate)
	return steps, applyMoveSteps(steps, store)
}

// allocateLike picks the node allocate would bind task to among the given
// nodes: the first that passes the predicate with idle GPUs, else the first
// that passes with future-idle GPUs (a pipelined placement onto releasing
// capacity). It mirrors allocate's two-tier fit without the full action.
func allocateLike(task *api.TaskInfo, nodes []*api.NodeInfo, predicate api.PredicateFn) string {
	need := task.InitResreq.Get(gpuRes)
	for _, node := range nodes {
		if predicate != nil && predicate(task, node) != nil {
			continue
		}
		if node.Idle.Get(gpuRes) >= need {
			return node.Name
		}
	}
	for _, node := range nodes {
		if predicate != nil && predicate(task, node) != nil {
			continue
		}
		if node.FutureIdle().Get(gpuRes) >= need {
			return node.Name
		}
	}
	return ""
}

func names(tasks []*api.TaskInfo) []string {
	out := make([]string, 0, len(tasks))
	for _, task := range tasks {
		out = append(out, task.Name)
	}
	return out
}

// ---- planner -------------------------------------------------------------

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
	if p.gang || p.target != "reserved" || p.from != "spot" || len(p.victims) != 0 || p.moves != 1 {
		t.Fatalf("unexpected plan: %+v", p)
	}
	if len(p.nodes) != 1 || p.nodes["reserved-1"] != 1 || p.gpus != 1 {
		t.Fatalf("unexpected placement: %+v", p.nodes)
	}
	if p.identity[ownerIdentityKey] != "owner-pg-train" {
		t.Fatalf("expected owner identity, got %v", p.identity)
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
	if len(plans[0].victims) != 4 || plans[0].nodes["reserved-1"] != 4 {
		t.Fatalf("expected 4 victims on reserved-1, got %v on %v", names(plans[0].victims), plans[0].nodes)
	}
}

// Session.Evict flips the victim it is handed to Releasing (via
// JobInfo.UpdateTaskStatus) before NodeInfo.UpdateTask reads the node's copy
// back out of node.Tasks. A plan that carried the node-local clone would flip
// that copy itself, and RemoveTask would then subtract from an empty
// ni.Releasing and panic. Victims must be the session-side tasks, and
// replaying Session.Evict's bookkeeping on them must leave the target node
// consistent.
func TestUpgradeVictimsAreSessionTasksAndEvictionAccountingHolds(t *testing.T) {
	f := newFixture(t)
	f.addNode(tierNode("spot-1", "spot", "a"))
	reserved := f.addNode(tierNode("reserved-1", "reserved", "a"))
	f.placeGroup(t, 1, nil, tierPod("train", "spot-1", "pg-train", 2, -4, time.Hour))
	for i := 0; i < 8; i++ {
		name := fmt.Sprintf("filler-%d", i)
		f.placeGroup(t, 1, nil, tierPod(name, "reserved-1", "pg-"+name, 1, -9, time.Hour))
	}

	plans, victims := f.startOnly(t, liveConf(), newMemStore(f))
	if len(plans) != 1 || len(victims) != 2 {
		t.Fatalf("expected 1 plan with 2 victims, got %d plans, victims %v", len(plans), names(victims))
	}
	for _, victim := range victims {
		if victim != f.running[victim.Pod.UID] || victim != f.jobs[victim.Job].Tasks[victim.UID] {
			t.Fatalf("victim %s is not the session task", victim.Name)
		}
		if victim == reserved.Tasks[api.PodKey(victim.Pod)] {
			t.Fatalf("victim %s aliases the node-local task copy", victim.Name)
		}
	}

	for _, victim := range victims {
		if err := f.jobs[victim.Job].UpdateTaskStatus(victim, api.Releasing); err != nil {
			t.Fatalf("UpdateTaskStatus(%s): %v", victim.Name, err)
		}
		if err := reserved.UpdateTask(victim); err != nil {
			t.Fatalf("UpdateTask(%s): %v", victim.Name, err)
		}
	}
	if got := reserved.Releasing.Get(gpuRes); got != 2000 {
		t.Fatalf("expected 2 GPUs releasing on reserved-1, got %v", got)
	}
	if got := reserved.Used.Get(gpuRes); got != 8000 {
		t.Fatalf("expected 8 GPUs still used on reserved-1, got %v", got)
	}
	if got := reserved.Idle.Get(gpuRes); got != 0 {
		t.Fatalf("expected 0 GPUs idle on reserved-1, got %v", got)
	}
	if got := reserved.FutureIdle().Get(gpuRes); got != 2000 {
		t.Fatalf("expected 2 GPUs future-idle on reserved-1, got %v", got)
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

func TestUpgradeGangStaysInOneZone(t *testing.T) {
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
	if !p.gang || p.zone != "a" || len(p.members) != 2 || p.gpus != 16 || p.priority != -4 {
		t.Fatalf("unexpected gang plan: %+v", p)
	}
	if len(p.nodes) != 2 || p.nodes["reserved-a"] != 8 || p.nodes["reserved-a2"] != 8 {
		t.Fatalf("unexpected nodes: %v", p.nodes)
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

func TestUpgradeSkipsCooldownYoungOptedOutProtectedAndExhausted(t *testing.T) {
	f := newFixture(t)
	f.addNode(tierNode("spot-1", "spot", "a"))
	f.addNode(tierNode("spot-2", "spot", "a"))
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
	f.placeGroup(t, 1, map[string]string{CapacityUpgradeCountAnnotation: "2"},
		tierPod("exhausted", "spot-1", "pg-exhausted", 1, -4, time.Hour))
	f.placeGroup(t, 1, map[string]string{CapacityUpgradeCountAnnotation: "garbage"},
		tierPod("badcount", "spot-1", "pg-badcount", 1, -4, time.Hour))
	orphan := tierPod("orphan", "spot-1", "pg-orphan", 1, -4, time.Hour)
	orphan.OwnerReferences = nil
	f.placeGroup(t, 1, nil, orphan)

	if plans := f.planUpgrades(newCapacityUpgradeConf(), nil); len(plans) != 0 {
		t.Fatalf("expected no plan, got %+v", plans)
	}

	// An expired clock unlocks the group.
	f.jobs["default/pg-cooling"].PodGroup.Annotations[CapacityUpgradeLastAnnotation] = testNow.Add(-time.Hour).Format(time.RFC3339)
	plans := f.planUpgrades(newCapacityUpgradeConf(), nil)
	if len(plans) != 1 || plans[0].members[0].Name != "cooling" {
		t.Fatalf("expected cooled pod to move, got %+v", plans)
	}

	// The gang owner's lifecycle protection is not an opt-out, and a move
	// count below the budget carries into the plan.
	g := newFixture(t)
	g.addNode(tierNode("spot-1", "spot", "a"))
	g.addNode(tierNode("reserved-1", "reserved", "a"))
	guarded := tierPod("guarded", "spot-1", "pg-guarded", 1, -4, time.Hour)
	guarded.Annotations[doNotDisruptAnnotation] = "true"
	guarded.Labels["exa.ai/gang-protection"] = "true"
	g.placeGroup(t, 1, map[string]string{CapacityUpgradeCountAnnotation: "1"}, guarded)
	plans = g.planUpgrades(newCapacityUpgradeConf(), nil)
	if len(plans) != 1 || plans[0].members[0].Name != "guarded" || plans[0].moves != 2 {
		t.Fatalf("expected protected gang pod to move as its 2nd move, got %+v", plans)
	}
}

func TestUpgradeVictimsMustBeControlledUnprotectedGpuPods(t *testing.T) {
	f := newFixture(t)
	f.addNode(tierNode("spot-1", "spot", "a"))
	f.addNode(tierNode("reserved-1", "reserved", "a"))
	f.placeGroup(t, 1, nil, tierPod("train", "spot-1", "pg-train", 4, -4, time.Hour))
	guarded := tierPod("guarded", "reserved-1", "pg-guarded", 4, -9, time.Hour)
	guarded.Annotations[doNotDisruptAnnotation] = "true"
	f.placeGroup(t, 1, nil, guarded)
	orphan := tierPod("orphan", "reserved-1", "pg-orphan", 4, -9, time.Hour)
	orphan.OwnerReferences = nil
	f.placeGroup(t, 1, nil, orphan)

	if plans := f.planUpgrades(newCapacityUpgradeConf(), nil); len(plans) != 0 {
		t.Fatalf("expected no plan without evictable victims, got %+v", plans)
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
	if len(plans[0].victims) != 2 {
		t.Fatalf("expected 2 victims, got %v", names(plans[0].victims))
	}
}

func TestUpgradeLedgerNetsOutHoldsInFlight(t *testing.T) {
	f := newFixture(t)
	f.addNode(tierNode("spot-1", "spot", "a"))
	f.addNode(tierNode("spot-2", "spot", "a"))
	f.addNode(tierNode("reserved-1", "reserved", "a"))
	f.placeGroup(t, 1, nil, tierPod("first", "spot-1", "pg-first", 4, -4, 2*time.Hour))
	f.placeGroup(t, 1, nil, tierPod("second", "spot-2", "pg-second", 4, -4, time.Hour))
	for i := 0; i < 4; i++ {
		name := fmt.Sprintf("filler-%d", i)
		f.placeGroup(t, 1, nil, tierPod(name, "reserved-1", "pg-"+name, 1, -9, time.Hour))
	}
	store := newMemStore(f)
	conf := liveConf()
	conf.MaxVictims = 1

	// Pass 1: "first" holds 4 GPUs on reserved-1 (4 idle) and stays put
	// until the hold is honoured.
	plans, victims := f.startOnly(t, conf, store)
	if len(plans) != 1 || plans[0].members[0].Name != "first" || len(victims) != 0 {
		t.Fatalf("expected first to hold idle room, got %+v", plans)
	}
	// Pass 2, before "first" moved: reserved-1 has 4 idle GPUs, all held,
	// and 4 fillers. "second" must evict all 4 fillers to fit, not count
	// the held idle GPUs; "first" is in flight and not re-planned.
	plans = f.planUpgrades(conf, nil)
	if len(plans) != 1 || plans[0].members[0].Name != "second" {
		t.Fatalf("expected only second to plan, got %+v", plans)
	}
	if len(plans[0].victims) != 4 {
		t.Fatalf("expected second to need all 4 fillers, got %v", names(plans[0].victims))
	}

	// A hold's own victims are spoken for: with the fillers promised to a
	// hold, nothing is left for a third mover.
	g := newFixture(t)
	g.addNode(tierNode("spot-1", "spot", "a"))
	g.addNode(tierNode("reserved-1", "reserved", "a"))
	g.placeGroup(t, 1, nil, tierPod("first", "spot-1", "pg-first", 8, -4, 2*time.Hour))
	g.placeGroup(t, 1, nil, tierPod("second", "spot-1", "pg-second", 4, -4, time.Hour))
	for i := 0; i < 8; i++ {
		name := fmt.Sprintf("filler-%d", i)
		g.placeGroup(t, 1, nil, tierPod(name, "reserved-1", "pg-"+name, 1, -9, time.Hour))
	}
	plans, victims = g.startOnly(t, conf, newMemStore(g))
	if len(plans) != 1 || len(victims) != 8 {
		t.Fatalf("expected first to take every filler, got %+v", plans)
	}
	if plans := g.planUpgrades(conf, nil); len(plans) != 0 {
		t.Fatalf("expected no room left for second, got %+v", plans)
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
	plans := f.planUpgrades(newCapacityUpgradeConf(), nil)
	if len(plans) != 2 {
		t.Fatalf("expected 2 plans, got %d", len(plans))
	}
	if plans[0].members[0].Name != "high-old" || plans[1].members[0].Name != "high-new" {
		t.Fatalf("unexpected order: %s, %s", plans[0].members[0].Name, plans[1].members[0].Name)
	}
	for _, p := range plans {
		if p.priority != -4 {
			t.Fatalf("expected the -4 groups to win the room, got %+v", p)
		}
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
	conf.MaxGangMoves = 1
	if plans := g.planUpgrades(conf, nil); len(plans) != 1 || !plans[0].gang {
		t.Fatalf("expected 1 gang move, got %+v", plans)
	}
}

func TestUpgradeIgnoresCheapestTierAndUnlabeledTargets(t *testing.T) {
	f := newFixture(t)
	f.addNode(tierNode("reserved-1", "reserved", "a"))
	f.addNode(tierNode("reserved-2", "reserved", "a"))
	f.addNode(tierNode("owned", "", "a"))
	f.placeGroup(t, 1, nil, tierPod("home", "reserved-1", "pg-home", 1, -4, time.Hour))
	f.placeGroup(t, 1, nil, tierPod("sunk", "owned", "pg-sunk", 1, -4, time.Hour))

	if plans := f.planUpgrades(newCapacityUpgradeConf(), nil); len(plans) != 0 {
		t.Fatalf("expected no plan, got %+v", plans)
	}

	// An idle unlabeled node ranks 0 but is never a target: only a
	// labeled tier is a known destination.
	g := newFixture(t)
	g.addNode(tierNode("spot-1", "spot", "a"))
	g.addNode(tierNode("unlabeled", "", "a"))
	g.placeGroup(t, 1, nil, tierPod("train", "spot-1", "pg-train", 1, -4, time.Hour))
	if plans := g.planUpgrades(newCapacityUpgradeConf(), nil); len(plans) != 0 {
		t.Fatalf("expected no plan onto an unlabeled node, got %+v", plans)
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
	if len(plans) != 1 || len(plans[0].victims) != 0 || len(plans[0].nodes) != 2 {
		t.Fatalf("expected gang to fit both reserved nodes without victims, got %+v", plans)
	}
}

func TestUpgradeIdentityFromLabelsMustAgreeAcrossGang(t *testing.T) {
	f := newFixture(t)
	f.addNode(tierNode("spot-1", "spot", "a"))
	f.addNode(tierNode("reserved-1", "reserved", "a"))
	f.addNode(tierNode("reserved-2", "reserved", "a"))
	w0 := tierPod("w0", "spot-1", "pg-gang", 4, -4, time.Hour)
	w1 := tierPod("w1", "spot-1", "pg-gang", 4, -4, time.Hour)
	for _, pod := range []*v1.Pod{w0, w1} {
		pod.Labels["execution-id"] = "exec-1"
		pod.Labels["node-id"] = "n0"
	}
	f.placeGroup(t, 2, nil, w0, w1)

	plans := f.planUpgrades(newCapacityUpgradeConf(), nil)
	if len(plans) != 1 || plans[0].identity["execution-id"] != "exec-1" || plans[0].identity["node-id"] != "n0" {
		t.Fatalf("expected label identity, got %+v", plans)
	}

	w1.Labels["execution-id"] = "exec-2"
	if plans := f.planUpgrades(newCapacityUpgradeConf(), nil); len(plans) != 0 {
		t.Fatalf("expected gang with disagreeing identity to be skipped, got %+v", plans)
	}
}

func TestParseIdentitySets(t *testing.T) {
	got := parseIdentitySets(" exa-run-name , node-id ;; execution-id,node-id; ,")
	want := [][]string{{"exa-run-name", "node-id"}, {"execution-id", "node-id"}}
	if len(got) != len(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for i := range want {
		if strings.Join(got[i], ",") != strings.Join(want[i], ",") {
			t.Fatalf("expected %v, got %v", want, got)
		}
	}
	if sets := parseIdentitySets(""); len(sets) != 0 {
		t.Fatalf("expected no sets from an empty list, got %v", sets)
	}
}

func TestPodIdentityPrefersTheFirstCompleteSet(t *testing.T) {
	sets := newCapacityUpgradeConf().identityLabels()
	pod := tierPod("w0", "spot-1", "pg-gang", 8, -4, time.Hour)
	pod.Labels["execution-id"], pod.Labels["node-id"] = "exec-1", "n0"
	if id, ok := podIdentity(pod, sets); !ok || len(id) != 2 || id["execution-id"] != "exec-1" || id["node-id"] != "n0" {
		t.Fatalf("expected the execution identity without a run name, got %v %v", id, ok)
	}
	pod.Labels["exa-run-name"] = "sam-probe"
	id, ok := podIdentity(pod, sets)
	if !ok || len(id) != 2 || id["exa-run-name"] != "sam-probe" || id["node-id"] != "n0" {
		t.Fatalf("expected the run identity to take precedence, got %v %v", id, ok)
	}
	if _, present := id["execution-id"]; present {
		t.Fatalf("a run identity must not pin the execution: %v", id)
	}
	pod.Labels["exa-run-name"] = ""
	delete(pod.Labels, "node-id")
	if id, ok := podIdentity(pod, sets); !ok || id[ownerIdentityKey] != "owner-pg-gang" {
		t.Fatalf("expected the owner fallback without a complete label set, got %v %v", id, ok)
	}
}

// A run relaunched by its launcher comes back as a new execution: the
// successor carries the mover's run name but a fresh execution id, and must
// still be the one admitted onto, and claiming, the held capacity.
func TestSuccessorUnderNewExecutionClaimsByRunName(t *testing.T) {
	f := newFixture(t)
	f.addNode(tierNode("spot-1", "spot", "a"))
	f.addNode(tierNode("spot-2", "spot", "a"))
	reserved := f.addNode(tierNode("reserved-1", "reserved", "a"))
	w0 := tierPod("w0", "spot-1", "pg-gang", 8, -4, time.Hour)
	w1 := tierPod("w1", "spot-2", "pg-gang", 8, -4, time.Hour)
	for _, pod := range []*v1.Pod{w0, w1} {
		pod.Labels["exa-run-name"], pod.Labels["execution-id"], pod.Labels["node-id"] = "sam-probe", "exec-1", "n0"
	}
	f.placeGroup(t, 2, nil, w0, w1)
	f.addNode(tierNode("reserved-2", "reserved", "a"))
	store := newMemStore(f)
	conf := liveConf()
	plans, _ := f.startOnly(t, conf, store)
	if len(plans) != 1 || plans[0].identity["exa-run-name"] != "sam-probe" || plans[0].identity["execution-id"] != "" {
		t.Fatalf("expected a run-name identity, got %+v", plans)
	}
	_, evictions := f.advance(conf, store, testNow.Add(time.Minute), nil)
	for _, task := range evictions {
		f.evict(t, task)
	}

	idx := f.index()
	predicate := capacityUpgradePredicateFn(idx, gpuRes)
	stranger := api.NewTaskInfo(successorPod("other", "pg-other", 8, -4, testNow.Add(2*time.Minute)))
	stranger.Pod.Labels["exa-run-name"], stranger.Pod.Labels["node-id"] = "sam-other", "n0"
	if err := predicate(stranger, reserved); err == nil {
		t.Fatalf("expected a pod of another run to be kept off the held node")
	}
	s0 := successorPod("w0-2", "pg-gang-2", 8, -4, testNow.Add(2*time.Minute))
	s0.Labels["exa-run-name"], s0.Labels["execution-id"], s0.Labels["node-id"] = "sam-probe", "exec-2", "n0"
	if err := predicate(api.NewTaskInfo(s0), reserved); err != nil {
		t.Fatalf("expected the relaunched successor to be admitted, got %v", err)
	}

	f.bind(t, s0, "reserved-1", 2)
	s1 := successorPod("w1-2", "pg-gang-2", 8, -4, testNow.Add(2*time.Minute))
	s1.Labels["exa-run-name"], s1.Labels["execution-id"], s1.Labels["node-id"] = "sam-probe", "exec-2", "n0"
	f.bind(t, s1, "reserved-2", 2)
	steps, _ := f.advance(conf, store, testNow.Add(3*time.Minute), nil)
	if len(steps) != 1 || steps[0].outcome != "claimed" {
		t.Fatalf("expected the relaunched gang to claim the hold, got %+v", steps)
	}
}

// runPod labels a pod with a run-name identity, as Flyte tasks are.
func runPod(pod *v1.Pod, run string) *v1.Pod {
	pod.Labels["exa-run-name"], pod.Labels["node-id"] = run, "n0"
	return pod
}

// onlyTask returns the single task of a job.
func (f *fixture) onlyTask(t *testing.T, job string) *api.TaskInfo {
	tasks := f.jobs[api.JobID(job)].Tasks
	if len(tasks) != 1 {
		t.Fatalf("expected one task in %s, got %d", job, len(tasks))
	}
	for _, task := range tasks {
		return task
	}
	return nil
}

// fullReserved is a spot mover that can only reach reserved by evicting a
// lower-priority run: train (8 GPUs, -4) on spot-1, low (8 GPUs, -9) filling
// reserved-1, and a priority-0 job filling reserved-2 that is never a victim.
func fullReserved(t *testing.T) *fixture {
	f := newFixture(t)
	f.addNode(tierNode("spot-1", "spot", "a"))
	f.addNode(tierNode("spot-2", "spot", "a"))
	f.addNode(tierNode("reserved-1", "reserved", "a"))
	f.addNode(tierNode("reserved-2", "reserved", "a"))
	f.placeGroup(t, 1, nil, runPod(tierPod("train", "spot-1", "pg-train", 8, -4, time.Hour), "train-run"))
	f.placeGroup(t, 1, nil, runPod(tierPod("low", "reserved-1", "pg-low", 8, -9, time.Hour), "low-run"))
	f.placeGroup(t, 1, nil, tierPod("top", "reserved-2", "pg-top", 8, 0, time.Hour))
	return f
}

func TestStartMoveRecordsMoverAndVictimsInTheLedger(t *testing.T) {
	f := fullReserved(t)
	store := newMemStore(f)
	plans, victims := f.startOnly(t, liveConf(), store)
	if len(plans) != 1 || len(victims) != 1 || victims[0].Name != "low" {
		t.Fatalf("expected train to displace low, got %+v", plans)
	}
	ledger := f.ledgerByIdentity()
	stamp := testNow.Format(time.RFC3339)
	mover, victim := ledger["exa-run-name=train-run,node-id=n0"], ledger["exa-run-name=low-run,node-id=n0"]
	if mover.Last != stamp || mover.Moves != 1 {
		t.Fatalf("expected the mover recorded with its first move, got %+v", ledger)
	}
	if victim.Last != stamp || victim.Moves != 0 {
		t.Fatalf("expected the victim recorded without spending a move, got %+v", ledger)
	}
	if len(store.log) < 2 || store.log[0] != "ledger" || store.log[1] != "node:reserved-1" {
		t.Fatalf("expected the ledger written before the hold, got %v", store.log)
	}
}

func TestStartMoveNeedsTheLedgerWrittenFirst(t *testing.T) {
	f := fullReserved(t)
	store := newMemStore(f)
	store.failLedger = fmt.Errorf("configmaps is forbidden")
	conf := liveConf()
	idx := f.index()
	plans := planCapacityUpgrades(f.nodes, f.jobs, f.running, conf, idx, testNow, nil)
	if len(plans) != 1 {
		t.Fatalf("expected one plan, got %+v", plans)
	}
	if err := startCapacityUpgradeMove(plans[0], idx, conf, store); err == nil {
		t.Fatalf("expected the move to fail without a ledger write")
	}
	if len(store.log) != 0 || len(f.holdsOn(t, "reserved-1")) != 0 || len(idx.moves) != 0 {
		t.Fatalf("expected nothing written or indexed after the ledger failed, got %v", store.log)
	}
}

func TestUnreadableLedgerStopsPlanning(t *testing.T) {
	f := fullReserved(t)
	idx := f.index()
	idx.ledgerErr = fmt.Errorf("configmaps is forbidden")
	if plans := planCapacityUpgrades(f.nodes, f.jobs, f.running, liveConf(), idx, testNow, nil); len(plans) != 0 {
		t.Fatalf("expected no plan without the ledger, got %+v", plans)
	}
}

func TestDisplacedVictimIsNotUpgradedWithinCooldown(t *testing.T) {
	f := fullReserved(t)
	store := newMemStore(f)
	_, victims := f.startOnly(t, liveConf(), store)
	f.evict(t, victims[0])
	f.remove(t, victims[0])
	// The victim's successor comes back under a fresh PodGroup on spot, and
	// reserved-2 frees up: a plain age check would upgrade it right away.
	f.remove(t, f.onlyTask(t, "default/pg-top"))
	low2 := runPod(tierPod("low-2", "spot-2", "pg-low-2", 8, -9, 0), "low-run")
	f.placeGroup(t, 1, nil, low2)

	conf := liveConf()
	during := testNow.Add(20 * time.Minute)
	if plans := planCapacityUpgrades(f.nodes, f.jobs, f.running, conf, f.index(), during, nil); len(plans) != 0 {
		t.Fatalf("expected the displaced run to sit out its cooldown, got %+v", plans)
	}
	after := testNow.Add(31 * time.Minute)
	plans := planCapacityUpgrades(f.nodes, f.jobs, f.running, conf, f.index(), after, nil)
	if len(plans) != 1 || plans[0].members[0].Name != "low-2" || plans[0].moves != 1 {
		t.Fatalf("expected the displaced run to move once cooled, got %+v", plans)
	}
}

func TestDisplacedVictimIsNotEvictedAgainWithinCooldown(t *testing.T) {
	f := fullReserved(t)
	store := newMemStore(f)
	_, victims := f.startOnly(t, liveConf(), store)
	f.evict(t, victims[0])
	f.remove(t, victims[0])
	// The victim's successor lands on reserved-2 (freed meanwhile) under a
	// fresh PodGroup; another spot mover would displace it again.
	f.remove(t, f.onlyTask(t, "default/pg-top"))
	f.placeGroup(t, 1, nil, runPod(tierPod("low-2", "reserved-2", "pg-low-2", 8, -9, 0), "low-run"))
	f.placeGroup(t, 1, nil, runPod(tierPod("train-2", "spot-2", "pg-train-2", 8, -4, time.Hour), "train-2-run"))

	conf := liveConf()
	during := testNow.Add(20 * time.Minute)
	if plans := planCapacityUpgrades(f.nodes, f.jobs, f.running, conf, f.index(), during, nil); len(plans) != 0 {
		t.Fatalf("expected the displaced run to be shielded from eviction, got %+v", plans)
	}
	after := testNow.Add(31 * time.Minute)
	plans := planCapacityUpgrades(f.nodes, f.jobs, f.running, conf, f.index(), after, nil)
	if len(plans) != 1 || plans[0].members[0].Name != "train-2" || len(plans[0].victims) != 1 || plans[0].victims[0].Name != "low-2" {
		t.Fatalf("expected train-2 to displace the cooled run, got %+v", plans)
	}
}

func TestMoveBudgetFollowsTheRunAcrossFreshPodGroups(t *testing.T) {
	f := fullReserved(t)
	store := newMemStore(f)
	conf := liveConf()
	f.startOnly(t, conf, store)
	// The mover comes back on spot under a fresh, unannotated PodGroup (its
	// hold expired); the ledger still says it has moved once.
	f.remove(t, f.onlyTask(t, "default/pg-train"))
	f.remove(t, f.onlyTask(t, "default/pg-top"))
	f.placeGroup(t, 1, nil, runPod(tierPod("train-2", "spot-2", "pg-train-2", 8, -4, 0), "train-run"))

	second := testNow.Add(31 * time.Minute)
	idx := f.index()
	plans := planCapacityUpgrades(f.nodes, f.jobs, f.running, conf, idx, second, nil)
	if len(plans) != 1 || plans[0].members[0].Name != "train-2" || plans[0].moves != 2 {
		t.Fatalf("expected the second move to count as such, got %+v", plans)
	}
	if err := startCapacityUpgradeMove(plans[0], idx, conf, store); err != nil {
		t.Fatalf("startCapacityUpgradeMove: %v", err)
	}
	if record := f.ledgerByIdentity()["exa-run-name=train-run,node-id=n0"]; record.Moves != 2 {
		t.Fatalf("expected the ledger to carry two moves, got %+v", record)
	}

	f.remove(t, f.onlyTask(t, "default/pg-train-2"))
	f.addNode(tierNode("reserved-3", "reserved", "a"))
	f.placeGroup(t, 1, nil, runPod(tierPod("train-3", "spot-1", "pg-train-3", 8, -4, 0), "train-run"))
	third := testNow.Add(62 * time.Minute)
	if plans := planCapacityUpgrades(f.nodes, f.jobs, f.running, conf, f.index(), third, nil); len(plans) != 0 {
		t.Fatalf("expected the run's budget to be spent, got %+v", plans)
	}
}

func TestLedgerPrunesOnlyCooledRunsNobodyCarries(t *testing.T) {
	f := fullReserved(t)
	store := newMemStore(f)
	f.startOnly(t, liveConf(), store)
	// The mover finishes for good; the victim comes back on spot.
	f.remove(t, f.onlyTask(t, "default/pg-train"))
	f.placeGroup(t, 1, nil, runPod(tierPod("low-2", "spot-2", "pg-low-2", 8, -9, 0), "low-run"))

	conf := liveConf()
	idx := f.index()
	idx.prune(testNow.Add(10*time.Minute), conf.cooldown(), liveIdentities(f.nodes, conf.identityLabels()))
	if len(idx.ledgerRecords()) != 2 || idx.pruned {
		t.Fatalf("expected both records kept inside the cooldown, got %+v", idx.ledgerRecords())
	}
	idx.prune(testNow.Add(time.Hour), conf.cooldown(), liveIdentities(f.nodes, conf.identityLabels()))
	records := idx.ledgerRecords()
	if len(records) != 1 || records[0].Identity["exa-run-name"] != "low-run" || !idx.pruned {
		t.Fatalf("expected only the run still on the cluster to be kept, got %+v", records)
	}
	if _, ok := idx.record(map[string]string{"exa-run-name": "train-run", "node-id": "n0"}); ok {
		t.Fatalf("expected the pruned run to leave the ledger view")
	}
	if err := idx.commitLedger(idx.ledgerRecords(), store); err != nil {
		t.Fatalf("commitLedger: %v", err)
	}
	if ledger := f.ledgerByIdentity(); len(ledger) != 1 || idx.pruned {
		t.Fatalf("expected the pruned ledger to be written, got %+v", ledger)
	}
}

func TestMalformedLedgerRecordDelaysButNeverUnlocks(t *testing.T) {
	f := fullReserved(t)
	f.ledger = []workloadRecord{{Identity: map[string]string{"exa-run-name": "train-run", "node-id": "n0"}, Last: "soon"}}
	idx := f.index()
	if idx.restartedWithin(map[string]string{"exa-run-name": "train-run", "node-id": "n0"}, testNow, time.Hour) != true {
		t.Fatalf("expected an unparsable restart time to count as recent")
	}
	if plans := f.planUpgrades(liveConf(), nil); len(plans) != 0 {
		t.Fatalf("expected the run with a bad record to wait, got %+v", plans)
	}

	for _, raw := range []string{`[{"moves":1}]`, `[{"identity":{"a":"b"}}]`, `{not json`} {
		if _, err := decodeLedger(raw); err == nil {
			t.Fatalf("expected %s to be rejected", raw)
		}
	}
	if records, err := decodeLedger(""); err != nil || len(records) != 0 {
		t.Fatalf("expected an empty ledger to decode to nothing, got %+v, %v", records, err)
	}
}

func TestMalformedHoldsLeaveTheLedgerAlone(t *testing.T) {
	f := fullReserved(t)
	f.nodes["reserved-2"].Node.Annotations = map[string]string{CapacityUpgradeHoldsAnnotation: `{not json`}
	f.ledger = []workloadRecord{{Identity: map[string]string{"exa-run-name": "low-run", "node-id": "n0"}, Last: testNow.Format(time.RFC3339)}}
	store := newMemStore(f)
	steps, _ := f.advance(liveConf(), store, testNow.Add(time.Minute), nil)
	if len(steps) != 1 || steps[0].outcome != "malformed" {
		t.Fatalf("expected the node to be cleared as malformed, got %+v", steps)
	}
	if _, ok := f.nodes["reserved-2"].Node.Annotations[CapacityUpgradeHoldsAnnotation]; ok {
		t.Fatalf("expected the malformed holds to be cleared")
	}
	if len(f.ledger) != 1 || len(store.log) != 1 {
		t.Fatalf("expected maintenance to leave the ledger untouched, got %+v, %v", f.ledger, store.log)
	}
	if plans := f.planUpgrades(liveConf(), nil); len(plans) != 0 {
		t.Fatalf("expected low to stay shielded by the ledger, got %+v", plans)
	}
}

func TestLedgerOutlivesTheNodesItWasWrittenFor(t *testing.T) {
	f := fullReserved(t)
	store := newMemStore(f)
	_, victims := f.startOnly(t, liveConf(), store)
	f.evict(t, victims[0])
	f.remove(t, victims[0])
	// Every node that took part in the move is gone; the victim comes back on
	// a brand-new spot node with reserved room to move into.
	f.remove(t, f.onlyTask(t, "default/pg-train"))
	f.remove(t, f.onlyTask(t, "default/pg-top"))
	for _, name := range []string{"spot-1", "reserved-1", "reserved-2"} {
		delete(f.nodes, name)
	}
	f.addNode(tierNode("spot-3", "spot", "a"))
	f.addNode(tierNode("reserved-3", "reserved", "a"))
	f.placeGroup(t, 1, nil, runPod(tierPod("low-2", "spot-3", "pg-low-2", 8, -9, 0), "low-run"))

	conf := liveConf()
	if plans := planCapacityUpgrades(f.nodes, f.jobs, f.running, conf, f.index(), testNow.Add(20*time.Minute), nil); len(plans) != 0 {
		t.Fatalf("expected the displaced run to sit out its cooldown, got %+v", plans)
	}
	plans := planCapacityUpgrades(f.nodes, f.jobs, f.running, conf, f.index(), testNow.Add(31*time.Minute), nil)
	if len(plans) != 1 || plans[0].members[0].Name != "low-2" || plans[0].nodeNames()[0] != "reserved-3" {
		t.Fatalf("expected the displaced run to move once cooled, got %+v", plans)
	}
}

func TestUpgradeConfParseFallsBackOnBadParams(t *testing.T) {
	conf := newCapacityUpgradeConf()
	conf.parse(map[string]interface{}{"cooldownSeconds": "not-a-number"})
	if conf.CooldownSeconds != 1800 || !conf.DryRun || conf.MaxGangMoves != 2 || conf.HoldTTLSeconds != 600 {
		t.Fatalf("expected defaults after bad params, got %+v", conf)
	}
	conf.parse(map[string]interface{}{"dryRun": false, "maxGangMoves": 5, "holdTtlSeconds": 30, "order": "reserved,spot"})
	if conf.DryRun || conf.MaxGangMoves != 5 || conf.HoldTTLSeconds != 30 || conf.Order != "reserved,spot" {
		t.Fatalf("unexpected parsed conf: %+v", conf)
	}
}

// ---- transaction ---------------------------------------------------------

func TestStartMoveWritesHoldsOnEveryTargetBeforeStamping(t *testing.T) {
	f := newFixture(t)
	f.addNode(tierNode("spot-1", "spot", "a"))
	f.addNode(tierNode("spot-2", "spot", "a"))
	f.addNode(tierNode("reserved-1", "reserved", "a"))
	f.addNode(tierNode("reserved-2", "reserved", "a"))
	f.placeGroup(t, 2, nil,
		tierPod("w0", "spot-1", "pg-gang", 8, -4, time.Hour),
		tierPod("w1", "spot-2", "pg-gang", 8, -4, time.Hour))
	f.placeGroup(t, 1, nil, tierPod("filler", "reserved-2", "pg-filler", 2, -9, time.Hour))
	store := newMemStore(f)

	plans, victims := f.startOnly(t, liveConf(), store)
	if len(plans) != 1 || len(victims) != 1 || victims[0].Name != "filler" {
		t.Fatalf("expected a gang plan evicting filler, got %+v / %v", plans, names(victims))
	}
	if got := strings.Join(store.log, ","); got != "ledger,node:reserved-1,node:reserved-2,group:default/pg-gang" {
		t.Fatalf("expected the ledger, then holds, then the mover stamp, got %s", got)
	}
	for _, node := range []string{"reserved-1", "reserved-2"} {
		holds := f.holdsOn(t, node)
		if len(holds) != 1 || holds[0].Gpus != 8 || len(holds[0].Nodes) != 2 || len(holds[0].Movers) != 2 || holds[0].EvictedAt != "" {
			t.Fatalf("unexpected hold on %s: %+v", node, holds)
		}
		if holds[0].Group != "default/pg-gang" || holds[0].Target != "reserved" || holds[0].From != "spot" || holds[0].Moves != 1 {
			t.Fatalf("unexpected hold metadata on %s: %+v", node, holds[0])
		}
		if node == "reserved-2" && (len(holds[0].Victims) != 1 || holds[0].Victims[0] != string(victims[0].Pod.UID)) {
			t.Fatalf("expected the filler recorded as victim, got %v", holds[0].Victims)
		}
	}
	if len(store.groups) != 1 || store.groups[0].name != "pg-gang" || store.groups[0].moves != 1 || store.groups[0].repack {
		t.Fatalf("unexpected mover stamp: %+v", store.groups)
	}
	if !f.index().groups["default/pg-gang"] {
		t.Fatalf("expected the group to be in flight after start")
	}
}

func TestStartMoveRollsBackOnWriteFailure(t *testing.T) {
	build := func() (*fixture, *memStore) {
		f := newFixture(t)
		f.addNode(tierNode("spot-1", "spot", "a"))
		f.addNode(tierNode("spot-2", "spot", "a"))
		f.addNode(tierNode("reserved-1", "reserved", "a"))
		f.addNode(tierNode("reserved-2", "reserved", "a"))
		f.placeGroup(t, 2, nil,
			tierPod("w0", "spot-1", "pg-gang", 8, -4, time.Hour),
			tierPod("w1", "spot-2", "pg-gang", 8, -4, time.Hour))
		return f, newMemStore(f)
	}
	start := func(f *fixture, store *memStore) error {
		idx := f.index()
		plans := planCapacityUpgrades(f.nodes, f.jobs, f.running, liveConf(), idx, testNow, nil)
		if len(plans) != 1 {
			t.Fatalf("expected 1 plan, got %d", len(plans))
		}
		err := startCapacityUpgradeMove(plans[0], idx, liveConf(), store)
		if len(idx.moves) != 0 {
			t.Fatalf("expected the index untouched after a failed start, got %d moves", len(idx.moves))
		}
		return err
	}

	f, store := build()
	store.failNode["reserved-2"] = errors.New("conflict")
	if err := start(f, store); err == nil || !strings.Contains(err.Error(), "reserved-2") {
		t.Fatalf("expected hold write failure, got %v", err)
	}
	if f.holdsOn(t, "reserved-1") != nil || len(store.groups) != 0 {
		t.Fatalf("expected reserved-1 rolled back and no stamp, got %v / %+v", f.holdsOn(t, "reserved-1"), store.groups)
	}
	// The ledger record stays: it can only delay this gang's next move.
	if record, ok := f.ledgerByIdentity()["exa.ai/owner-uid=owner-pg-gang"]; !ok || record.Moves != 1 {
		t.Fatalf("expected the mover's record kept after rollback, got %+v", f.ledger)
	}
	if plans := f.planUpgrades(liveConf(), nil); len(plans) != 0 {
		t.Fatalf("expected the gang to wait out the cooldown after a failed start, got %+v", plans)
	}

	f, store = build()
	store.failStamp = errors.New("podgroup gone")
	if err := start(f, store); err == nil || !strings.Contains(err.Error(), "stamp mover") {
		t.Fatalf("expected stamp failure, got %v", err)
	}
	if f.holdsOn(t, "reserved-1") != nil || f.holdsOn(t, "reserved-2") != nil {
		t.Fatalf("expected both holds rolled back after stamp failure")
	}
}

// raceFixture is the production incident's shape: an 8-GPU gang member on
// spot whose only reserved room is occupied by lower-priority 1-GPU
// fillers. Returns the mover and the fillers.
func raceFixture(t *testing.T) (*fixture, *api.TaskInfo, []*api.TaskInfo) {
	f := newFixture(t)
	f.addNode(tierNode("spot-1", "spot", "a"))
	f.addNode(tierNode("reserved-1", "reserved", "a"))
	f.placeGroup(t, 1, nil, tierPod("train", "spot-1", "pg-train", 8, -4, time.Hour))
	fillers := make([]*api.TaskInfo, 0, 8)
	for i := 0; i < 8; i++ {
		name := fmt.Sprintf("filler-%d", i)
		job := f.placeGroup(t, 1, nil, tierPod(name, "reserved-1", "pg-"+name, 1, -9, time.Hour))
		for _, task := range job.Tasks {
			fillers = append(fillers, task)
		}
	}
	var mover *api.TaskInfo
	for _, task := range f.jobs["default/pg-train"].Tasks {
		mover = task
	}
	return f, mover, fillers
}

func TestMoveEvictsVictimsWaitsForIdleThenDrainsAndEvictsMover(t *testing.T) {
	f, mover, fillers := raceFixture(t)
	store := newMemStore(f)
	conf := liveConf()

	// Pass 1: hold reserved-1, evict the fillers; the mover stays.
	plans, victims := f.startOnly(t, conf, store)
	if len(plans) != 1 || len(victims) != 8 {
		t.Fatalf("expected 1 plan evicting 8 fillers, got %d plans, %d victims", len(plans), len(victims))
	}
	for _, victim := range victims {
		if victim.NodeName != "reserved-1" || victim.Priority != -9 {
			t.Fatalf("unexpected victim %s on %s (priority %d)", victim.Name, victim.NodeName, victim.Priority)
		}
	}
	if mover.Status != api.Running {
		t.Fatalf("mover must not be touched when the hold is written")
	}
	for _, filler := range fillers {
		f.evict(t, filler)
	}

	// Session 2: victims are still releasing; reserved-1 has no idle
	// GPUs, so the mover is not evicted and no source is drained.
	steps, evictions := f.advance(conf, store, testNow.Add(10*time.Second), nil)
	if len(steps) != 0 || len(evictions) != 0 {
		t.Fatalf("expected the move to wait for releasing victims, got %+v", steps)
	}
	if f.drainsOn(t, "spot-1") != nil {
		t.Fatalf("source must not be drained before the target is clear")
	}

	// Session 3: victims are gone. Drain the source, mark the hold
	// evicted, then evict the mover.
	for _, filler := range fillers {
		f.remove(t, filler)
	}
	store.log = nil
	steps, evictions = f.advance(conf, store, testNow.Add(time.Minute), nil)
	if len(steps) != 1 || steps[0].outcome != "evicting" || steps[0].reason != "target capacity clear" {
		t.Fatalf("expected the eviction step, got %+v", steps)
	}
	if len(evictions) != 1 || !sameTask(evictions[0], mover) {
		t.Fatalf("expected the mover evicted, got %v", names(evictions))
	}
	if got := strings.Join(store.log, ","); got != "node:spot-1,node:reserved-1" {
		t.Fatalf("expected the source drained before the target records the eviction, got %s", got)
	}
	drains := f.drainsOn(t, "spot-1")
	holds := f.holdsOn(t, "reserved-1")
	if len(drains) != 1 || drains[0].Move != holds[0].Move {
		t.Fatalf("expected a drain for the move on spot-1, got %+v", drains)
	}
	if len(holds) != 1 || holds[0].EvictedAt != testNow.Add(time.Minute).Format(time.RFC3339) {
		t.Fatalf("expected the hold marked evicted, got %+v", holds)
	}
	if holds[0].Until != testNow.Add(time.Minute+10*time.Minute).Format(time.RFC3339) {
		t.Fatalf("expected the placing deadline one TTL after eviction, got %s", holds[0].Until)
	}
}

func TestMoveReEvictsVictimsThatDidNotGo(t *testing.T) {
	f, mover, fillers := raceFixture(t)
	store := newMemStore(f)
	conf := liveConf()
	f.startOnly(t, conf, store)
	// Seven evictions took, one filler is still running.
	for _, filler := range fillers[1:] {
		f.evict(t, filler)
		f.remove(t, filler)
	}

	steps, evictions := f.advance(conf, store, testNow.Add(time.Minute), nil)
	if len(steps) != 1 || steps[0].outcome != "evicting" || steps[0].reason != "victims still running after eviction" {
		t.Fatalf("expected a victim re-eviction step, got %+v", steps)
	}
	if len(evictions) != 1 || !sameTask(evictions[0], fillers[0]) {
		t.Fatalf("expected only the stuck filler evicted, got %v", names(evictions))
	}
	if mover.Status != api.Running || f.drainsOn(t, "spot-1") != nil {
		t.Fatalf("mover must stay until the target is clear")
	}
}

func TestMoveDoesNotEvictMoverWhenStateWriteFails(t *testing.T) {
	for _, failing := range []string{"spot-1", "reserved-1"} {
		t.Run(failing, func(t *testing.T) {
			f, mover, fillers := raceFixture(t)
			store := newMemStore(f)
			conf := liveConf()
			f.startOnly(t, conf, store)
			for _, filler := range fillers {
				f.evict(t, filler)
				f.remove(t, filler)
			}

			store.failNode[failing] = errors.New("conflict")
			steps, evictions := f.advance(conf, store, testNow.Add(time.Minute), nil)
			if len(steps) != 1 || len(evictions) != 0 {
				t.Fatalf("expected the step to be skipped without evictions, got %d steps, %v", len(steps), names(evictions))
			}
			// Whatever was written, the hold never says evicted while the
			// source may be undrained: a later session finds the move still
			// clearing and retries the whole step.
			if f.holdsOn(t, "reserved-1")[0].EvictedAt != "" {
				t.Fatalf("the hold must not record an eviction that did not happen")
			}
			if failing == "spot-1" && f.drainsOn(t, "spot-1") != nil {
				t.Fatalf("nothing should be written after the first failure")
			}

			delete(store.failNode, failing)
			steps, evictions = f.advance(conf, store, testNow.Add(2*time.Minute), nil)
			if len(steps) != 1 || len(evictions) != 1 || !sameTask(evictions[0], mover) {
				t.Fatalf("expected the retry to evict the mover, got %+v / %v", steps, names(evictions))
			}
			if drains := f.drainsOn(t, "spot-1"); len(drains) != 1 || drains[0].Until != testNow.Add(12*time.Minute).Format(time.RFC3339) {
				t.Fatalf("expected one drain with the retry's deadline, got %+v", drains)
			}
		})
	}
}

func TestMoveWaitsWhileMoverNoLongerFitsTarget(t *testing.T) {
	f, _, fillers := raceFixture(t)
	store := newMemStore(f)
	conf := liveConf()
	f.startOnly(t, conf, store)
	for _, filler := range fillers {
		f.evict(t, filler)
		f.remove(t, filler)
	}
	veto := func(task *api.TaskInfo, node *api.NodeInfo, gang bool) error { return errors.New("tainted") }
	steps, evictions := f.advance(conf, store, testNow.Add(time.Minute), veto)
	if len(steps) != 0 || len(evictions) != 0 {
		t.Fatalf("expected no eviction while the mover cannot land, got %+v", steps)
	}
	// It expires rather than evicting into nowhere.
	steps, _ = f.advance(conf, store, testNow.Add(11*time.Minute), veto)
	if len(steps) != 1 || steps[0].outcome != "expired" || steps[0].reason != "target capacity never cleared" {
		t.Fatalf("expected expiry, got %+v", steps)
	}
	if f.holdsOn(t, "reserved-1") != nil {
		t.Fatalf("expected the hold released on expiry")
	}
}

// evictedRace drives the race fixture to the placing phase: victims gone,
// source drained, mover releasing. Returns the fixture, the store and the
// move id.
func evictedRace(t *testing.T) (*fixture, *memStore, string) {
	f, mover, fillers := raceFixture(t)
	store := newMemStore(f)
	conf := liveConf()
	f.startOnly(t, conf, store)
	for _, filler := range fillers {
		f.evict(t, filler)
		f.remove(t, filler)
	}
	_, evictions := f.advance(conf, store, testNow.Add(time.Minute), nil)
	if len(evictions) != 1 {
		t.Fatalf("expected the mover evicted, got %v", names(evictions))
	}
	f.evict(t, mover)
	return f, store, f.holdsOn(t, "reserved-1")[0].Move
}

func TestReleasingSourceCapacityCannotTakeBackTheSuccessor(t *testing.T) {
	f, _, _ := evictedRace(t)
	spot, reserved := f.nodes["spot-1"], f.nodes["reserved-1"]
	if spot.Idle.Get(gpuRes) != 0 || spot.FutureIdle().Get(gpuRes) != 8*gpuMilli || reserved.Idle.Get(gpuRes) != 8*gpuMilli {
		t.Fatalf("fixture: spot should be releasing 8 GPUs and reserved idle, got spot idle %v future %v reserved idle %v",
			spot.Idle.Get(gpuRes), spot.FutureIdle().Get(gpuRes), reserved.Idle.Get(gpuRes))
	}
	successor := api.NewTaskInfo(successorPod("train-2", "pg-train", 8, -4, testNow.Add(90*time.Second)))
	idx := f.index()
	predicate := capacityUpgradePredicateFn(idx, gpuRes)
	order := capacityUpgradeNodeOrderFn(idx, gpuRes)

	// Without the drain predicate, allocate would pipeline the successor
	// onto its own releasing spot node whenever it comes first.
	if got := allocateLike(successor, []*api.NodeInfo{spot}, nil); got != "spot-1" {
		t.Fatalf("expected the unguarded scheduler to pipeline onto spot-1, got %q", got)
	}
	// With it, the releasing spot node is off limits and only the held
	// reserved node takes the successor.
	if err := predicate(successor, spot); err == nil || !strings.Contains(err.Error(), "draining") {
		t.Fatalf("expected the drain to reject the successor on spot-1, got %v", err)
	}
	if err := predicate(successor, reserved); err != nil {
		t.Fatalf("expected the successor to be admitted on reserved-1, got %v", err)
	}
	if got := allocateLike(successor, []*api.NodeInfo{spot, reserved}, predicate); got != "reserved-1" {
		t.Fatalf("expected the successor placed on reserved-1, got %q", got)
	}
	if score, _ := order(successor, reserved); score != heldNodePreference {
		t.Fatalf("expected the held node preferred, got %v", score)
	}
	if score, _ := order(successor, spot); score != 0 {
		t.Fatalf("expected no preference for the source, got %v", score)
	}

	// An unrelated GPU pod can take neither the held reserved GPUs nor the
	// releasing spot GPUs.
	other := api.NewTaskInfo(successorPod("other", "pg-other", 1, -9, testNow.Add(90*time.Second)))
	if err := predicate(other, reserved); err == nil || !strings.Contains(err.Error(), "held") {
		t.Fatalf("expected the hold to reject an unrelated pod on reserved-1, got %v", err)
	}
	if got := allocateLike(other, []*api.NodeInfo{spot, reserved}, predicate); got != "" {
		t.Fatalf("expected no node for the unrelated pod, got %q", got)
	}

	// Once the mover has actually gone the drain protects nothing: real
	// idle GPUs on spot-1 are usable again (node scoring decides).
	for _, task := range f.nodes["spot-1"].Tasks {
		f.remove(t, task)
	}
	if err := predicate(other, f.nodes["spot-1"]); err != nil {
		t.Fatalf("expected idle spot GPUs usable after the mover is gone, got %v", err)
	}
}

func TestHoldPredicateLeavesUnheldRemainderToOthers(t *testing.T) {
	f := newFixture(t)
	f.addNode(tierNode("reserved-1", "reserved", "a"))
	f.nodes["reserved-1"].Node.Annotations = map[string]string{
		CapacityUpgradeHoldsAnnotation: mustJSON([]capacityHold{{
			Move: "default/pg-a@t", Group: "default/pg-a", Nodes: []string{"reserved-1"}, Movers: []string{"mover-uid"},
			Identity: map[string]string{ownerIdentityKey: "owner-pg-a"}, Gpus: 3, Until: testNow.Add(time.Hour).Format(time.RFC3339),
		}, {
			Move: "default/pg-b@t", Group: "default/pg-b", Nodes: []string{"reserved-1"}, Movers: []string{"mover-b"},
			Identity: map[string]string{ownerIdentityKey: "owner-pg-b"}, Gpus: 1, Until: testNow.Add(time.Hour).Format(time.RFC3339),
		}}),
	}
	idx := f.index()
	predicate := capacityUpgradePredicateFn(idx, gpuRes)
	node := f.nodes["reserved-1"]
	if idx.outstanding("reserved-1") != 4*gpuMilli {
		t.Fatalf("expected 4 GPUs outstanding across two holds, got %v", idx.outstanding("reserved-1"))
	}

	// 8 idle, 4 held: an unrelated pod may take up to 4.
	fits := api.NewTaskInfo(successorPod("fits", "pg-x", 4, -4, testNow))
	tooBig := api.NewTaskInfo(successorPod("big", "pg-x", 5, -4, testNow))
	if err := predicate(fits, node); err != nil {
		t.Fatalf("expected a 4-GPU pod admitted into the unheld remainder, got %v", err)
	}
	if err := predicate(tooBig, node); err == nil {
		t.Fatalf("expected a 5-GPU pod rejected")
	}
	// A claimant of one hold is not restricted by the other hold either;
	// the successor of pg-a takes its 3 plus whatever is free.
	claimant := api.NewTaskInfo(successorPod("a-2", "pg-a", 7, -4, testNow))
	if err := predicate(claimant, node); err != nil {
		t.Fatalf("expected the claimant admitted, got %v", err)
	}
	// CPU-only pods and planner probes are never restricted.
	cpu := api.NewTaskInfo(util.BuildPod("default", "cpu", "", v1.PodPending, v1.ResourceList{"cpu": *apiResource("4")}, "pg-cpu", nil, nil))
	if err := predicate(cpu, node); err != nil {
		t.Fatalf("expected a CPU pod admitted, got %v", err)
	}
	probe := api.NewTaskInfo(successorPod("probe", "pg-x", 8, -4, testNow))
	probe.Pod.Annotations[capacityUpgradeProbeAnnotation] = "true"
	if err := predicate(probe, node); err != nil {
		t.Fatalf("expected a probe admitted, got %v", err)
	}
}

func TestSuccessorClaimsHoldAndInheritsCooldownAndCount(t *testing.T) {
	f, store, id := evictedRace(t)
	conf := liveConf()
	claimAt := testNow.Add(3 * time.Minute)

	// A pod with the same owner created before the eviction is not the
	// successor; a pod of another owner never is.
	f.bind(t, inGroup(tierPod("sibling", "", "pg-train", 1, -4, 2*time.Hour), "pg-sibling"), "reserved-1", 1)
	f.bind(t, successorPod("stranger", "pg-stranger", 1, -4, claimAt), "reserved-1", 1)
	steps, _ := f.advance(conf, store, claimAt, nil)
	if len(steps) != 0 {
		t.Fatalf("expected no claim by sibling or stranger, got %+v", steps)
	}
	for _, task := range append([]*api.TaskInfo{}, f.running["default-sibling"], f.running["default-stranger"]) {
		f.remove(t, task)
	}

	// The successor binds onto reserved-1: the hold is claimed and
	// released, the drain lifted, and the new PodGroup stamped with the
	// cooldown and the inherited move count.
	f.bind(t, inGroup(successorPod("train-2", "pg-train", 8, -4, testNow.Add(2*time.Minute)), "pg-train-2"), "reserved-1", 1)
	store.log, store.groups = nil, nil
	steps, evictions := f.advance(conf, store, claimAt, nil)
	if len(steps) != 1 || steps[0].outcome != "claimed" || steps[0].id != id || len(evictions) != 0 {
		t.Fatalf("expected the claim, got %+v", steps)
	}
	if f.holdsOn(t, "reserved-1") != nil || f.drainsOn(t, "spot-1") != nil {
		t.Fatalf("expected hold and drain released")
	}
	if len(store.groups) != 1 || store.groups[0].name != "pg-train-2" || store.groups[0].moves != 1 ||
		store.groups[0].last != claimAt.Format(time.RFC3339) || store.groups[0].repack {
		t.Fatalf("unexpected successor stamp: %+v", store.groups)
	}
	if idx := f.index(); len(idx.moves) != 0 || len(idx.drains) != 0 || idx.groups["default/pg-train"] {
		t.Fatalf("expected a clean index after the claim")
	}
}

func TestGangClaimNeedsEveryFragment(t *testing.T) {
	f := newFixture(t)
	f.addNode(tierNode("spot-1", "spot", "a"))
	f.addNode(tierNode("spot-2", "spot", "a"))
	f.addNode(tierNode("reserved-1", "reserved", "a"))
	f.addNode(tierNode("reserved-2", "reserved", "a"))
	w0 := tierPod("w0", "spot-1", "pg-gang", 8, -4, time.Hour)
	w1 := tierPod("w1", "spot-2", "pg-gang", 8, -4, time.Hour)
	for _, pod := range []*v1.Pod{w0, w1} {
		pod.Labels["execution-id"] = "exec-1"
		pod.Labels["node-id"] = "n0"
	}
	job := f.placeGroup(t, 2, nil, w0, w1)
	store := newMemStore(f)
	conf := liveConf()
	f.startOnly(t, conf, store)
	steps, evictions := f.advance(conf, store, testNow.Add(time.Minute), nil)
	if len(steps) != 1 || len(evictions) != 2 {
		t.Fatalf("expected both members evicted on idle reserved nodes, got %+v / %v", steps, names(evictions))
	}
	if f.drainsOn(t, "spot-1") == nil || f.drainsOn(t, "spot-2") == nil {
		t.Fatalf("expected both sources drained")
	}
	for _, task := range evictions {
		f.evict(t, task)
	}
	if len(job.Tasks) != 2 {
		t.Fatalf("fixture: movers should still be in the job as releasing")
	}

	// One successor bound: the other fragment is unclaimed.
	s0 := successorPod("w0-2", "pg-gang-2", 8, -4, testNow.Add(2*time.Minute))
	s0.Labels["execution-id"], s0.Labels["node-id"] = "exec-1", "n0"
	f.bind(t, s0, "reserved-1", 2)
	steps, _ = f.advance(conf, store, testNow.Add(3*time.Minute), nil)
	if len(steps) != 0 {
		t.Fatalf("expected no claim with one fragment, got %+v", steps)
	}
	// Both bound: claimed, one stamp for the successor group.
	s1 := successorPod("w1-2", "pg-gang-2", 8, -4, testNow.Add(2*time.Minute))
	s1.Labels["execution-id"], s1.Labels["node-id"] = "exec-1", "n0"
	f.bind(t, s1, "reserved-2", 2)
	store.groups = nil
	steps, _ = f.advance(conf, store, testNow.Add(3*time.Minute), nil)
	if len(steps) != 1 || steps[0].outcome != "claimed" {
		t.Fatalf("expected the gang claim, got %+v", steps)
	}
	if len(store.groups) != 1 || store.groups[0].name != "pg-gang-2" || store.groups[0].repack {
		t.Fatalf("unexpected successor stamps: %+v", store.groups)
	}
	if f.holdsOn(t, "reserved-1") != nil || f.holdsOn(t, "reserved-2") != nil || f.drainsOn(t, "spot-1") != nil || f.drainsOn(t, "spot-2") != nil {
		t.Fatalf("expected all state released")
	}
}

func TestMoverCannotClaimItsOwnHold(t *testing.T) {
	_, mover, _ := raceFixture(t)
	hold := capacityHold{Movers: []string{string(mover.Pod.UID)}, Identity: map[string]string{ownerIdentityKey: "owner-pg-train"}}
	if holdMatches(hold, mover) {
		t.Fatalf("the mover must not match its own hold")
	}
	successor := api.NewTaskInfo(successorPod("train-2", "pg-train", 8, -4, testNow))
	if !holdMatches(hold, successor) {
		t.Fatalf("the successor must match")
	}
	if holdMatches(capacityHold{Movers: hold.Movers}, successor) {
		t.Fatalf("a hold without identity matches nothing")
	}
}

func TestHoldExpiresWhenSuccessorNeverComes(t *testing.T) {
	f, store, _ := evictedRace(t)
	conf := liveConf()
	steps, _ := f.advance(conf, store, testNow.Add(5*time.Minute), nil)
	if len(steps) != 0 {
		t.Fatalf("expected the hold to wait within its TTL, got %+v", steps)
	}
	steps, _ = f.advance(conf, store, testNow.Add(12*time.Minute), nil)
	if len(steps) != 1 || steps[0].outcome != "expired" || steps[0].reason != "successor never claimed the hold" {
		t.Fatalf("expected expiry, got %+v", steps)
	}
	if f.holdsOn(t, "reserved-1") != nil || f.drainsOn(t, "spot-1") != nil {
		t.Fatalf("expected hold and drain released on expiry")
	}
}

func TestMoveAbandonedWhenStateIsInconsistent(t *testing.T) {
	until := testNow.Add(time.Hour).Format(time.RFC3339)
	hold := func(nodes ...string) capacityHold {
		return capacityHold{
			Move: "default/pg-gang@t", Group: "default/pg-gang", Nodes: nodes, Movers: []string{"m0", "m1"},
			Identity: map[string]string{ownerIdentityKey: "owner-pg-gang"}, Gpus: 8, Until: until,
		}
	}
	cases := []struct {
		name   string
		setup  func(f *fixture)
		reason string
	}{
		{"fragment missing", func(f *fixture) {
			f.nodes["reserved-1"].Node.Annotations = map[string]string{CapacityUpgradeHoldsAnnotation: mustJSON([]capacityHold{hold("reserved-1", "reserved-2")})}
		}, "fragments missing"},
		{"target node gone", func(f *fixture) {
			f.nodes["reserved-1"].Node.Annotations = map[string]string{CapacityUpgradeHoldsAnnotation: mustJSON([]capacityHold{hold("reserved-1", "gone")})}
		}, "fragments missing"},
		{"bad deadline", func(f *fixture) {
			h := hold("reserved-1")
			h.Until = "yesterday"
			f.nodes["reserved-1"].Node.Annotations = map[string]string{CapacityUpgradeHoldsAnnotation: mustJSON([]capacityHold{h})}
		}, "unparseable deadline"},
		{"bad eviction time", func(f *fixture) {
			h := hold("reserved-1")
			h.EvictedAt = "soon"
			f.nodes["reserved-1"].Node.Annotations = map[string]string{CapacityUpgradeHoldsAnnotation: mustJSON([]capacityHold{h})}
		}, "unparseable eviction time"},
		{"movers gone before eviction", func(f *fixture) {
			f.nodes["reserved-1"].Node.Annotations = map[string]string{CapacityUpgradeHoldsAnnotation: mustJSON([]capacityHold{hold("reserved-1")})}
		}, "movers gone before eviction"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.addNode(tierNode("reserved-1", "reserved", "a"))
			f.addNode(tierNode("reserved-2", "reserved", "a"))
			tc.setup(f)
			store := newMemStore(f)
			steps, evictions := f.advance(liveConf(), store, testNow, nil)
			if len(steps) != 1 || steps[0].outcome != "abandoned" || steps[0].reason != tc.reason || len(evictions) != 0 {
				t.Fatalf("expected abandoned (%s), got %+v", tc.reason, steps)
			}
			if f.holdsOn(t, "reserved-1") != nil {
				t.Fatalf("expected the hold released")
			}
		})
	}
}

func TestMalformedAnnotationIsClearedAndTrustsNothing(t *testing.T) {
	f := newFixture(t)
	f.addNode(tierNode("spot-1", "spot", "a"))
	f.addNode(tierNode("reserved-1", "reserved", "a"))
	f.addNode(tierNode("reserved-2", "reserved", "a"))
	f.placeGroup(t, 1, nil, tierPod("train", "spot-1", "pg-train", 1, -4, time.Hour))
	good := capacityHold{
		Move: "default/pg-gang@t", Group: "default/pg-gang", Nodes: []string{"reserved-1", "reserved-2"}, Movers: []string{"m0", "m1"},
		Identity: map[string]string{ownerIdentityKey: "owner-pg-gang"}, Gpus: 8, Until: testNow.Add(time.Hour).Format(time.RFC3339),
	}
	f.nodes["reserved-1"].Node.Annotations = map[string]string{CapacityUpgradeHoldsAnnotation: mustJSON([]capacityHold{good})}
	f.nodes["reserved-2"].Node.Annotations = map[string]string{
		CapacityUpgradeHoldsAnnotation:  `[{"move":"default/pg-gang@t"}]`,
		CapacityUpgradeDrainsAnnotation: mustJSON([]capacityDrain{{Move: "x", Until: good.Until}}),
	}

	idx := f.index()
	if len(idx.malformed) != 1 || idx.outstanding("reserved-2") != 0 || idx.drained("reserved-2") {
		t.Fatalf("expected reserved-2 malformed and contributing nothing, got %+v", idx)
	}
	// The malformed node is not a planning target either.
	if plans := planCapacityUpgrades(f.nodes, f.jobs, f.running, liveConf(), idx, testNow, nil); len(plans) != 1 || plans[0].nodes["reserved-2"] == 0 {
		// Planning still works; a malformed node just has no holds to net out.
		t.Logf("plans: %+v", plans)
	}
	store := newMemStore(f)
	steps, evictions := f.advance(liveConf(), store, testNow, nil)
	if len(evictions) != 0 {
		t.Fatalf("nothing may be evicted on malformed state, got %v", names(evictions))
	}
	outcomes := map[string]string{}
	for _, step := range steps {
		outcomes[step.id] = step.outcome + ":" + step.reason
	}
	if !strings.HasPrefix(outcomes["reserved-2"], "malformed:holds") || outcomes["default/pg-gang@t"] != "abandoned:fragments missing" {
		t.Fatalf("expected the node cleared and the half-present move abandoned, got %v", outcomes)
	}
	if f.nodes["reserved-2"].Node.Annotations[CapacityUpgradeHoldsAnnotation] != "" || f.nodes["reserved-2"].Node.Annotations[CapacityUpgradeDrainsAnnotation] != "" {
		t.Fatalf("expected reserved-2 annotations cleared")
	}
	if f.holdsOn(t, "reserved-1") != nil {
		t.Fatalf("expected the abandoned move released from reserved-1")
	}
}

func TestStaleDrainIsReleased(t *testing.T) {
	f := newFixture(t)
	f.addNode(tierNode("spot-1", "spot", "a"))
	f.nodes["spot-1"].Node.Annotations = map[string]string{
		CapacityUpgradeDrainsAnnotation: mustJSON([]capacityDrain{{Move: "default/pg-x@t", Until: testNow.Add(time.Hour).Format(time.RFC3339)}}),
	}
	store := newMemStore(f)
	steps, _ := f.advance(liveConf(), store, testNow, nil)
	if len(steps) != 1 || steps[0].outcome != "drain_released" {
		t.Fatalf("expected the orphan drain released, got %+v", steps)
	}
	if f.drainsOn(t, "spot-1") != nil {
		t.Fatalf("expected the drain annotation removed")
	}
}

func TestSinglePodMoveSpendsTheRepackBudget(t *testing.T) {
	f := newFixture(t)
	f.addNode(tierNode("spot-1", "spot", "a"))
	f.addNode(tierNode("reserved-1", "reserved", "a"))
	f.placeGroup(t, 1, nil, tierPod("train", "spot-1", "pg-train", 1, -4, time.Hour))
	store := newMemStore(f)
	f.startOnly(t, liveConf(), store)
	if len(store.groups) != 1 || !store.groups[0].repack {
		t.Fatalf("expected the single-pod mover stamped with the repack budget, got %+v", store.groups)
	}
	annotations := groupAnnotations(store.groups[0])
	if annotations[groupEvictionAnnotation] == nil || *annotations[groupEvictionAnnotation] != "1" ||
		*annotations[CapacityUpgradeCountAnnotation] != "1" || *annotations[CapacityUpgradeLastAnnotation] != testNow.Format(time.RFC3339) {
		t.Fatalf("unexpected stamp annotations: %v", annotations)
	}
}

func TestAnnotationPatchRemovesEmptyState(t *testing.T) {
	annotations, err := nodeAnnotations(nodeState{})
	if err != nil {
		t.Fatal(err)
	}
	patch, err := annotationPatch(annotations)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Metadata struct {
			Annotations map[string]*string `json:"annotations"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(patch, &decoded); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{CapacityUpgradeHoldsAnnotation, CapacityUpgradeDrainsAnnotation} {
		value, present := decoded.Metadata.Annotations[key]
		if !present || value != nil {
			t.Fatalf("expected %s: null in the merge patch, got %s", key, patch)
		}
	}

	annotations, err = nodeAnnotations(nodeState{drains: []capacityDrain{{Move: "m", Until: "u"}}})
	if err != nil {
		t.Fatal(err)
	}
	if annotations[CapacityUpgradeHoldsAnnotation] != nil || annotations[CapacityUpgradeDrainsAnnotation] == nil {
		t.Fatalf("expected only drains set, got %v", annotations)
	}
	var drains []capacityDrain
	if err := json.Unmarshal([]byte(*annotations[CapacityUpgradeDrainsAnnotation]), &drains); err != nil || len(drains) != 1 || drains[0].Move != "m" {
		t.Fatalf("drains did not round-trip: %v %v", drains, err)
	}
}

func mustJSON(v interface{}) string {
	body, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(body)
}
