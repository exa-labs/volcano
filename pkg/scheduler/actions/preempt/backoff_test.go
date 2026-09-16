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

package preempt

import (
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/conformance"
	"volcano.sh/volcano/pkg/scheduler/plugins/gang"
	"volcano.sh/volcano/pkg/scheduler/plugins/priority"
	"volcano.sh/volcano/pkg/scheduler/plugins/proportion"
	"volcano.sh/volcano/pkg/scheduler/uthelper"
	"volcano.sh/volcano/pkg/scheduler/util"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }
func newFakeClock() *fakeClock               { return &fakeClock{t: time.Unix(1_000_000, 0)} }
func newTestBackoff(clock *fakeClock) *preemptBackoff {
	b := newPreemptBackoff()
	b.now = clock.now
	return b
}

func TestPreemptBackoffDisabledSkipsNothing(t *testing.T) {
	b := newTestBackoff(newFakeClock())
	b.configure(0, time.Minute)
	b.recordFailure("j1")
	if b.shouldSkip("j1") {
		t.Fatal("disabled backoff must not skip")
	}
	if len(b.entries) != 0 {
		t.Fatalf("disabled backoff must not record entries, got %d", len(b.entries))
	}
}

func TestPreemptBackoffDoublesAndSaturates(t *testing.T) {
	clock := newFakeClock()
	b := newTestBackoff(clock)
	b.configure(2*time.Second, 10*time.Second)

	for i, want := range []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second, 10 * time.Second} {
		b.recordFailure("j1")
		if got := b.entries["j1"].until.Sub(clock.t); got != want {
			t.Fatalf("failure %d: delay %v, want %v", i+1, got, want)
		}
		if !b.shouldSkip("j1") {
			t.Fatalf("failure %d: expected skip right after failure", i+1)
		}
		clock.advance(want - time.Millisecond)
		if !b.shouldSkip("j1") {
			t.Fatalf("failure %d: expected skip just before expiry", i+1)
		}
		clock.advance(time.Millisecond)
		if b.shouldSkip("j1") {
			t.Fatalf("failure %d: expected retry at expiry", i+1)
		}
	}
}

func TestPreemptBackoffSuccessAndPruneReset(t *testing.T) {
	clock := newFakeClock()
	b := newTestBackoff(clock)
	b.configure(2*time.Second, time.Minute)

	b.recordFailure("j1")
	b.recordFailure("j1")
	b.recordSuccess("j1")
	if b.shouldSkip("j1") {
		t.Fatal("success must clear the backoff")
	}
	b.recordFailure("j1")
	if got := b.entries["j1"].until.Sub(clock.t); got != 2*time.Second {
		t.Fatalf("delay after reset %v, want base", got)
	}

	b.recordFailure("j2")
	b.prune(sets.New[api.JobID]("j2"))
	if _, kept := b.entries["j1"]; kept {
		t.Fatal("prune must drop jobs that are no longer starving")
	}
	if !b.shouldSkip("j2") {
		t.Fatal("prune must keep jobs that are still starving")
	}
}

func TestPreemptBackoffConfigureClampsMax(t *testing.T) {
	b := newTestBackoff(newFakeClock())
	b.configure(30*time.Second, 5*time.Second)
	if b.max != 30*time.Second {
		t.Fatalf("max below base must be raised to base, got %v", b.max)
	}
	b.configure(-1, 5*time.Second)
	if b.enabled() {
		t.Fatal("negative base must disable the backoff")
	}
}

// TestPreemptBackoffAcrossSessions runs the same Action over consecutive
// sessions, as the scheduler does, and checks that a job whose preemption
// keeps failing is retried only once its backoff has expired while a job that
// preempts successfully is never held back.
func TestPreemptBackoffAcrossSessions(t *testing.T) {
	plugins := map[string]framework.PluginBuilder{
		conformance.PluginName: conformance.New,
		gang.PluginName:        gang.New,
		priority.PluginName:    priority.New,
		proportion.PluginName:  proportion.New,
	}
	highPrio := util.BuildPriorityClass("high-priority", 100000)
	lowPrio := util.BuildPriorityClass("low-priority", 10)
	trueValue := true
	tiers := []conf.Tier{{Plugins: []conf.PluginOption{
		{Name: conformance.PluginName, EnabledPreemptable: &trueValue},
		{Name: gang.PluginName, EnabledPreemptable: &trueValue, EnabledJobPipelined: &trueValue, EnabledJobStarving: &trueValue},
		{Name: priority.PluginName, EnabledTaskOrder: &trueValue, EnabledJobOrder: &trueValue, EnabledPreemptable: &trueValue, EnabledJobPipelined: &trueValue, EnabledJobStarving: &trueValue},
		{Name: proportion.PluginName, EnabledOverused: &trueValue, EnabledAllocatable: &trueValue, EnabledQueueOrder: &trueValue},
	}}}

	// n1 is full of a high-priority job's pods; the low-priority pg2 cannot
	// preempt them. pg3 is high priority and can preempt pg1 on n1.
	failing := uthelper.TestCommonStruct{
		Name: "low-priority job cannot preempt a full node",
		PodGroups: []*schedulingv1beta1.PodGroup{
			util.BuildPodGroupWithPrio("pg1", "c1", "q1", 1, map[string]int32{"": 2}, schedulingv1beta1.PodGroupRunning, "high-priority"),
			util.BuildPodGroupWithPrio("pg2", "c1", "q1", 1, map[string]int32{"": 1}, schedulingv1beta1.PodGroupInqueue, "low-priority"),
		},
		Pods: []*v1.Pod{
			util.BuildPod("c1", "preemptee1", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1", make(map[string]string), make(map[string]string)),
			util.BuildPod("c1", "preemptee2", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1", make(map[string]string), make(map[string]string)),
			util.BuildPod("c1", "preemptor1", "", v1.PodPending, api.BuildResourceList("1", "1G"), "pg2", make(map[string]string), make(map[string]string)),
		},
		Nodes: []*v1.Node{
			util.BuildNode("n1", api.BuildResourceList("2", "2G", []api.ScalarResource{{Name: "pods", Value: "10"}}...), make(map[string]string)),
		},
		Queues:   []*schedulingv1beta1.Queue{util.BuildQueue("q1", 1, nil)},
		Plugins:  plugins,
		PriClass: []*schedulingv1.PriorityClass{highPrio, lowPrio},
	}
	succeeding := uthelper.TestCommonStruct{
		Name: "high-priority job preempts a full node",
		PodGroups: []*schedulingv1beta1.PodGroup{
			util.BuildPodGroupWithPrio("pg1", "c1", "q1", 1, map[string]int32{"": 2}, schedulingv1beta1.PodGroupRunning, "low-priority"),
			util.BuildPodGroupWithPrio("pg3", "c1", "q1", 1, map[string]int32{"": 1}, schedulingv1beta1.PodGroupInqueue, "high-priority"),
		},
		Pods: []*v1.Pod{
			util.BuildPod("c1", "preemptee1", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1", make(map[string]string), make(map[string]string)),
			util.BuildPod("c1", "preemptee2", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1", make(map[string]string), make(map[string]string)),
			util.BuildPod("c1", "preemptor1", "", v1.PodPending, api.BuildResourceList("1", "1G"), "pg3", make(map[string]string), make(map[string]string)),
		},
		Nodes: []*v1.Node{
			util.BuildNode("n1", api.BuildResourceList("2", "2G", []api.ScalarResource{{Name: "pods", Value: "10"}}...), make(map[string]string)),
		},
		Queues:         []*schedulingv1beta1.Queue{util.BuildQueue("q1", 1, nil)},
		Plugins:        plugins,
		PriClass:       []*schedulingv1.PriorityClass{highPrio, lowPrio},
		ExpectEvicted:  []string{"c1/preemptee1"},
		ExpectEvictNum: 1,
	}

	clock := newFakeClock()
	action := New()
	action.backoff.now = clock.now
	config := []conf.Configuration{{Name: action.Name(), Arguments: map[string]interface{}{
		EnableTopologyAwarePreemptionKey: false,
		PreemptionBackoffSecondsKey:      5,
		PreemptionMaxBackoffSecondsKey:   20,
	}}}
	runSession := func(test uthelper.TestCommonStruct, check bool) {
		test.RegisterSession(tiers, config)
		defer test.Close()
		test.Run([]framework.Action{action})
		if check {
			if err := test.CheckAll(0); err != nil {
				t.Fatal(err)
			}
		}
	}
	failures := func(job api.JobID) int {
		if entry, found := action.backoff.entries[job]; found {
			return entry.failures
		}
		return 0
	}

	runSession(failing, false)
	if got := failures("c1/pg2"); got != 1 {
		t.Fatalf("after first failed session: failures=%d, want 1", got)
	}

	clock.advance(4 * time.Second)
	runSession(failing, false)
	if got := failures("c1/pg2"); got != 1 {
		t.Fatalf("session inside the backoff window must skip the job: failures=%d, want 1", got)
	}

	clock.advance(time.Second)
	runSession(failing, false)
	if got := failures("c1/pg2"); got != 2 {
		t.Fatalf("session after the backoff expired must retry: failures=%d, want 2", got)
	}

	// pg2 is no longer starving in this session, so its entry is pruned; pg3
	// preempts successfully and must never be held back.
	runSession(succeeding, true)
	if got := failures("c1/pg2"); got != 0 {
		t.Fatalf("entry for a job that stopped starving must be pruned: failures=%d", got)
	}
	if got := failures("c1/pg3"); got != 0 {
		t.Fatalf("successful preemption must not record a failure: failures=%d", got)
	}
}
