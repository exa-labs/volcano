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

// Tests for move admission in the strategies that evict without holding a
// target: gpuFragmentation (every simulated destination must admit the
// victim's spend cap) and lowNodeUtilization (a capped victim needs at
// least one admitting target node).

import (
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/plugins/capacitycost"
)

// verdict builds the annotations of a node whose price was confirmed age
// ago, with the given applicable and admitting spend-cap policies.
func verdict(age time.Duration, applied, admitted string) map[string]string {
	return map[string]string{
		capacitycost.OfferingPriceAnnotation:           "32",
		capacitycost.OfferingPriceObservedAtAnnotation: time.Now().Add(-age).UTC().Format(time.RFC3339),
		capacitycost.SpendCapsAppliedAnnotation:        applied,
		capacitycost.SpendCapsAdmittedAnnotation:       admitted,
	}
}

func policy(name string) map[string]string {
	return map[string]string{capacitycost.PodSpendCapPolicyAnnotation: name}
}

func repackAdmissionConf() *gpuFragmentationConf {
	conf := newGpuFragmentationConf()
	conf.MoveAdmission = true
	return conf
}

func TestRepackAdmissionParamsDefaultOff(t *testing.T) {
	conf := newGpuFragmentationConf()
	if conf.MoveAdmission || conf.PriceStalenessSeconds != 900 {
		t.Fatalf("unexpected defaults: %+v", conf)
	}
	conf.parse(map[string]interface{}{"moveAdmission": true, "priceStalenessSeconds": 30})
	if !conf.MoveAdmission || conf.PriceStalenessSeconds != 30 {
		t.Fatalf("unexpected parsed conf: %+v", conf)
	}
}

// One capped victim on a nearly empty node and one fuller destination: the
// drain is planned only when the destination admits the victim's cap.
func TestRepackAdmissionDecidesWhereACappedVictimMayGo(t *testing.T) {
	cases := []struct {
		name        string
		victim      map[string]string
		destination map[string]string
		move        bool
	}{
		{"cap applies and admits the destination", policy("a"), verdict(time.Minute, "a,b", "a"), true},
		{"cap applies and does not admit the destination", policy("b"), verdict(time.Minute, "a,b", "a"), false},
		{"cap does not apply to the destination", policy("c"), verdict(time.Minute, "a,b", "a"), true},
		{"destination has no verdict", policy("a"), nil, false},
		{"destination verdict is stale", policy("a"), verdict(time.Hour, "a", "a"), false},
		{"uncapped victim, destination without a verdict", nil, nil, true},
		{"uncapped victim, destination admitting nobody", nil, verdict(time.Minute, "a", ""), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFixture(t)
			source := f.addNode(gpuNode("source", 8, nil))
			dest := f.addNode(gpuNode("dest", 8, c.destination))
			f.placePod(t, source, gpuPod("victim", "source", 1, nil, c.victim, true), 1, "")
			f.placePod(t, dest, gpuPod("resident", "dest", 3, nil, nil, true), 1, "")

			// Without admission the drain is planned whatever the cap says.
			if plans := f.plan(newGpuFragmentationConf(), nil); len(plans) != 1 || plans[0].destination != "dest" {
				t.Fatalf("expected the unguarded drain onto dest, got %+v", plans)
			}

			var plans []plannedMove
			refused := counted(gpuRepackTargetsSkipped.WithLabelValues("move_admission"), func() {
				plans = f.plan(repackAdmissionConf(), nil)
			})
			if c.move {
				if len(plans) != 1 || plans[0].destination != "dest" || refused != 0 {
					t.Fatalf("expected the drain onto dest and no refusal, got %+v (refused %v)", plans, refused)
				}
				return
			}
			if len(plans) != 0 || refused != 1 {
				t.Fatalf("expected the drain refused and counted once, got %+v (refused %v)", plans, refused)
			}
		})
	}
}

// A refused destination is passed over for the next fullest one; a drain
// that leaves any victim without an admitting destination is not planned
// at all.
func TestRepackAdmissionPassesOverRefusedDestinations(t *testing.T) {
	f := newFixture(t)
	source := f.addNode(gpuNode("source", 8, nil))
	fullest := f.addNode(gpuNode("fullest", 8, verdict(time.Minute, "a", "")))
	fuller := f.addNode(gpuNode("fuller", 8, verdict(time.Minute, "a", "a")))
	f.placePod(t, source, gpuPod("victim", "source", 1, nil, policy("a"), true), 1, "")
	f.placePod(t, source, gpuPod("bystander", "source", 1, nil, nil, true), 1, "")
	f.placePod(t, fullest, gpuPod("resident-1", "fullest", 5, nil, nil, true), 1, "")
	f.placePod(t, fuller, gpuPod("resident-2", "fuller", 4, nil, nil, true), 1, "")

	destinations := func(plans []plannedMove) map[string]string {
		got := map[string]string{}
		for _, plan := range plans {
			got[plan.victim.Name] = plan.destination
		}
		return got
	}
	if got := destinations(f.plan(newGpuFragmentationConf(), nil)); got["victim"] != "fullest" || got["bystander"] != "fullest" {
		t.Fatalf("expected the unguarded drain onto the fullest node, got %v", got)
	}
	if got := destinations(f.plan(repackAdmissionConf(), nil)); got["victim"] != "fuller" || got["bystander"] != "fullest" {
		t.Fatalf("expected the capped victim on the admitting node, got %v", got)
	}

	fuller.Node.Annotations[capacitycost.SpendCapsAdmittedAnnotation] = ""
	if plans := f.plan(repackAdmissionConf(), nil); len(plans) != 0 {
		t.Fatalf("expected no drain when the capped victim has nowhere admitted to go, got %+v", plans)
	}
}

// Admission composes with the caller's predicate: both must pass.
func TestRepackAdmissionKeepsTheFitPredicate(t *testing.T) {
	f := newFixture(t)
	source := f.addNode(gpuNode("source", 8, nil))
	dest := f.addNode(gpuNode("dest", 8, verdict(time.Minute, "a", "a")))
	f.placePod(t, source, gpuPod("victim", "source", 1, nil, policy("a"), true), 1, "")
	f.placePod(t, dest, gpuPod("resident", "dest", 3, nil, nil, true), 1, "")
	veto := func(*api.TaskInfo, *api.NodeInfo) error { return errVeto }
	if plans := f.plan(repackAdmissionConf(), veto); len(plans) != 0 {
		t.Fatalf("expected the predicate veto to stand, got %+v", plans)
	}
	pass := func(*api.TaskInfo, *api.NodeInfo) error { return nil }
	if plans := f.plan(repackAdmissionConf(), pass); len(plans) != 1 {
		t.Fatalf("expected the drain planned, got %+v", plans)
	}
}

var errVeto = &vetoError{}

type vetoError struct{}

func (*vetoError) Error() string { return "veto" }

// lowNodeUtilization evicts toward the underutilized nodes without naming a
// destination per pod, so a capped victim is kept only when one of those
// nodes admits it.
func TestAdmittedVictimsNeedOneAdmittingTarget(t *testing.T) {
	now := time.Now()
	target := func(name string, annotations map[string]string) *NodeUtilization {
		return &NodeUtilization{nodeInfo: &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: annotations}}}
	}
	victim := func(name string, annotations map[string]string) *api.TaskInfo {
		return api.NewTaskInfo(gpuPod(name, "busy", 1, nil, annotations, true))
	}
	victims := []*api.TaskInfo{
		victim("uncapped", nil),
		victim("admitted", policy("a")),
		victim("refused", policy("b")),
		victim("not-applicable", policy("c")),
	}
	targets := []*NodeUtilization{
		target("unpriced", nil),
		target("rejects-all", verdict(time.Minute, "a,b", "")),
		target("admits-a", verdict(time.Minute, "a,b", "a")),
	}
	cases := []struct {
		name    string
		params  map[string]interface{}
		targets []*NodeUtilization
		want    []string
	}{
		{"admission off", map[string]interface{}{"thresholds": map[interface{}]interface{}{"cpu": 20}}, targets,
			[]string{"uncapped", "admitted", "refused", "not-applicable"}},
		{"admission on", map[string]interface{}{"moveAdmission": true}, targets,
			[]string{"uncapped", "admitted", "not-applicable"}},
		{"admission on, only unpriced targets", map[string]interface{}{"moveAdmission": true}, targets[:1],
			[]string{"uncapped"}},
		{"admission on, verdicts older than the configured staleness",
			map[string]interface{}{"moveAdmission": true, "priceStalenessSeconds": 30}, targets,
			[]string{"uncapped"}},
		{"params that do not decode evict nothing", map[string]interface{}{"moveAdmission": "maybe"}, targets, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := names(admittedVictims("lowNodeUtilization", victims, c.targets, c.params, now))
			if len(got) != len(c.want) {
				t.Fatalf("victims = %v, want %v", got, c.want)
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Fatalf("victims = %v, want %v", got, c.want)
				}
			}
		})
	}
}
