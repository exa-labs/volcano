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

package cache

import (
	"context"
	"errors"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"

	"volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/volcano/pkg/scheduler/api"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }

func testBackoff() (*evictionBackoff, *fakeClock) {
	clock := &fakeClock{t: time.Unix(1_800_000_000, 0)}
	b := newEvictionBackoff()
	b.now = clock.now
	return b, clock
}

// Each failure of the same pod doubles its hold, from 10 s up to 5 min.
func TestEvictionBackoffGrowsToItsCap(t *testing.T) {
	b, _ := testBackoff()
	want := []time.Duration{10 * time.Second, 20 * time.Second, 40 * time.Second, 80 * time.Second,
		160 * time.Second, 5 * time.Minute, 5 * time.Minute}
	for i, w := range want {
		if got := b.failed("p"); got != w {
			t.Fatalf("failure %d: held %v, want %v", i+1, got, w)
		}
	}
	for i := 0; i < 100; i++ {
		if got := b.failed("p"); got != 5*time.Minute {
			t.Fatalf("after %d failures held %v, want the 5m cap", 8+i, got)
		}
	}
}

// A pod is held only until its interval ends; refused again, it waits longer.
func TestEvictionBackoffExpiresAndKeepsItsCount(t *testing.T) {
	b, clock := testBackoff()
	b.failed("p")
	if !b.active().Has("p") {
		t.Fatalf("a pod whose eviction just failed is not held")
	}
	clock.t = clock.t.Add(10 * time.Second)
	if b.active().Has("p") {
		t.Fatalf("pod still held after its 10s interval")
	}
	if got := b.failed("p"); got != 20*time.Second {
		t.Fatalf("second failure after expiry held %v, want 20s", got)
	}
}

// An accepted eviction (or the pod's deletion) forgets the pod, so a later
// failure starts again from the base interval.
func TestEvictionBackoffForgetResets(t *testing.T) {
	b, _ := testBackoff()
	b.failed("p")
	b.failed("p")
	b.forget("p")
	if b.active().Has("p") {
		t.Fatalf("forgotten pod still held")
	}
	if got := b.failed("p"); got != 10*time.Second {
		t.Fatalf("failure after forget held %v, want 10s", got)
	}
}

func TestEvictionBackoffNilHoldsNothing(t *testing.T) {
	var b *evictionBackoff
	if got := b.failed("p"); got != 0 {
		t.Fatalf("nil backoff held %v", got)
	}
	b.forget("p")
	if b.active().Len() != 0 {
		t.Fatalf("nil backoff reports held pods")
	}
}

// evictedCache is a cache holding the evictable pod as a running task.
func evictedCache(t *testing.T, evictor Evictor) (*SchedulerCache, *api.TaskInfo) {
	t.Helper()
	recorder := record.NewFakeRecorder(10)
	sc := &SchedulerCache{
		Jobs:            make(map[api.JobID]*api.JobInfo),
		Nodes:           make(map[string]*api.NodeInfo),
		errTasks:        workqueue.NewTypedRateLimitingQueue[string](workqueue.DefaultTypedControllerRateLimiter[string]()),
		Recorder:        recorder,
		Evictor:         evictor,
		evictionBackoff: newEvictionBackoff(),
	}
	sc.AddOrUpdateNode(buildNode("n1", api.BuildResourceList("4", "16G", []api.ScalarResource{{Name: "pods", Value: "10"}}...)))
	task := api.NewTaskInfo(evictablePod())
	task.Job = "ns/j1"
	if err := sc.addTask(task); err != nil {
		t.Fatalf("addTask: %v", err)
	}
	sc.Jobs[task.Job].SetPodGroup(&api.PodGroup{PodGroup: scheduling.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "j1", Namespace: "ns"},
	}})
	return sc, task
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A refused eviction holds the pod out of the next sessions' victim
// selection: the snapshot carries it.
func TestCacheEvictRefusedHoldsThePodOutOfTheNextSnapshot(t *testing.T) {
	pod := evictablePod()
	client, _ := evictionClient(pod, apierrors.NewTooManyRequests(budgetRefusal, 0))
	sc, task := evictedCache(t, &defaultEvictor{kubeclient: client, recorder: record.NewFakeRecorder(10)})

	if err := sc.Evict(task, "preempt"); err != nil {
		t.Fatalf("Evict: %v", err)
	}
	waitFor(t, "the refused pod to be held", func() bool { return sc.evictionBackoff.active().Has(task.UID) })
}

// The session reads the held pods from the snapshot.
func TestSnapshotCarriesTheEvictionBackoff(t *testing.T) {
	sc := NewDefaultMockSchedulerCache("volcano")
	sc.evictionBackoff.failed("held")
	if got := sc.Snapshot().EvictionBackoff; !got.Has("held") || got.Len() != 1 {
		t.Fatalf("snapshot backoff = %v, want [held]", got)
	}
}

// Any failed eviction is held the same way, not just a budget refusal: a
// scheduler without pods/eviction RBAC (403) must not retry every session.
func TestCacheEvictForbiddenHoldsThePod(t *testing.T) {
	pod := evictablePod()
	forbidden := apierrors.NewForbidden(schema.GroupResource{Resource: "pods/eviction"}, pod.Name, errors.New("no RBAC"))
	client, _ := evictionClient(pod, forbidden)
	sc, task := evictedCache(t, &defaultEvictor{kubeclient: client, recorder: record.NewFakeRecorder(10)})

	if err := sc.Evict(task, "reclaim"); err != nil {
		t.Fatalf("Evict: %v", err)
	}
	waitFor(t, "the forbidden pod to be held", func() bool { return sc.evictionBackoff.active().Has(task.UID) })
	if _, err := client.CoreV1().Pods("ns").Get(context.TODO(), "victim", metav1.GetOptions{}); err != nil {
		t.Fatalf("a forbidden eviction fell back to removing the pod: %v", err)
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "delete" {
			t.Fatalf("a forbidden eviction fell back to a delete: %v", verbs(client))
		}
	}
}

// An accepted eviction forgets an earlier failure.
func TestCacheEvictAcceptedForgetsTheBackoff(t *testing.T) {
	pod := evictablePod()
	client, evictions := evictionClient(pod, nil)
	sc, task := evictedCache(t, &defaultEvictor{kubeclient: client, recorder: record.NewFakeRecorder(10)})
	sc.evictionBackoff.failed(task.UID)
	sc.evictionBackoff.now = func() time.Time { return time.Now().Add(time.Hour) } // the hold has ended

	if err := sc.Evict(task, "preempt"); err != nil {
		t.Fatalf("Evict: %v", err)
	}
	waitFor(t, "the backoff to be forgotten", func() bool {
		sc.evictionBackoff.mu.Lock()
		defer sc.evictionBackoff.mu.Unlock()
		return len(sc.evictionBackoff.entries) == 0
	})
	// The evictor forgets only after the API accepted the eviction.
	if len(*evictions) != 1 {
		t.Fatalf("expected one eviction request, got actions %v", verbs(client))
	}
}

// A deleted pod is forgotten, so the backoff does not outlive its pods.
func TestCacheDeletePodForgetsTheBackoff(t *testing.T) {
	sc, task := evictedCache(t, &defaultEvictor{})
	sc.evictionBackoff.failed(task.UID)
	if err := sc.deletePod(task.Pod); err != nil {
		t.Fatalf("deletePod: %v", err)
	}
	if len(sc.evictionBackoff.entries) != 0 {
		t.Fatalf("deleted pod still in the backoff")
	}
}

// A pod already gone is not an error: nothing is evicted, nothing written.
func TestEvictorTreatsAGonePodAsDone(t *testing.T) {
	pod := evictablePod()
	client, _ := evictionClient(pod, apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, pod.Name))
	evictor := &defaultEvictor{kubeclient: client, recorder: record.NewFakeRecorder(10)}
	if err := evictor.Evict(pod, "preempt"); err != nil {
		t.Fatalf("eviction of a gone pod returned %v", err)
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "update" || action.GetVerb() == "delete" {
			t.Fatalf("eviction of a gone pod wrote %v", verbs(client))
		}
	}
}

func TestEvictionOutcome(t *testing.T) {
	gr := schema.GroupResource{Resource: "pods"}
	for _, c := range []struct {
		err  error
		want string
	}{
		{apierrors.NewTooManyRequests(budgetRefusal, 0), "budget"},
		{apierrors.NewConflict(gr, "victim", errors.New("uid precondition")), "conflict"},
		{apierrors.NewForbidden(gr, "victim", errors.New("no RBAC")), "forbidden"},
		{apierrors.NewInternalError(errors.New("etcd")), "error"},
		{errors.New("timeout"), "error"},
	} {
		if got := evictionOutcome(c.err); got != c.want {
			t.Errorf("evictionOutcome(%v) = %q, want %q", c.err, got, c.want)
		}
	}
}
