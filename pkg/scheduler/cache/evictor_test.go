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
	"strings"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"

	"volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/volcano/pkg/scheduler/api"
)

const budgetRefusal = "Cannot evict pod as it would violate the pod's disruption budget."

func evictablePod() *v1.Pod {
	pod := buildPod("ns", "victim", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"),
		[]metav1.OwnerReference{buildOwnerReference("j1")}, map[string]string{"app": "web"})
	pod.UID = types.UID("victim-uid")
	pod.Status.Conditions = []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue}}
	return pod
}

// evictionClient is a fake clientset holding pod whose Eviction API answers
// with refuse (nil: the eviction is accepted). It records every eviction
// request it receives.
func evictionClient(pod *v1.Pod, refuse error) (*fake.Clientset, *[]*policyv1.Eviction) {
	client := fake.NewSimpleClientset(pod)
	evictions := &[]*policyv1.Eviction{}
	client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "eviction" {
			return false, nil, nil
		}
		*evictions = append(*evictions, action.(k8stesting.CreateAction).GetObject().(*policyv1.Eviction))
		return true, nil, refuse
	})
	return client, evictions
}

func verbs(client *fake.Clientset) []string {
	out := make([]string, 0)
	for _, action := range client.Actions() {
		out = append(out, action.GetVerb()+" "+action.GetResource().Resource+"/"+action.GetSubresource())
	}
	return out
}

func TestEvictorEvictsThroughTheEvictionAPI(t *testing.T) {
	pod := evictablePod()
	client, evictions := evictionClient(pod, nil)
	evictor := &defaultEvictor{kubeclient: client, recorder: record.NewFakeRecorder(10)}

	if err := evictor.Evict(pod, "shuffle"); err != nil {
		t.Fatalf("Evict: %v", err)
	}
	if len(*evictions) != 1 {
		t.Fatalf("expected one eviction request, got actions %v", verbs(client))
	}
	ev := (*evictions)[0]
	if ev.Name != "victim" || ev.Namespace != "ns" || ev.DeleteOptions == nil ||
		ev.DeleteOptions.Preconditions == nil || *ev.DeleteOptions.Preconditions.UID != pod.UID {
		t.Fatalf("eviction not pinned to the pod's UID: %+v", ev)
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "delete" {
			t.Fatalf("pod deleted directly, bypassing PodDisruptionBudgets: %v", verbs(client))
		}
	}
	got, err := client.CoreV1().Pods("ns").Get(context.TODO(), "victim", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	disrupted := false
	for _, c := range got.Status.Conditions {
		if c.Type == v1.DisruptionTarget && c.Status == v1.ConditionTrue && c.Reason == v1.PodReasonPreemptionByScheduler {
			disrupted = true
		}
		if c.Type == v1.PodReady && c.Status != v1.ConditionFalse {
			t.Fatalf("evicted pod still marked ready: %+v", got.Status.Conditions)
		}
	}
	if !disrupted {
		t.Fatalf("evicted pod lacks the scheduler's DisruptionTarget: %+v", got.Status.Conditions)
	}
}

func TestEvictorRespectsAPodDisruptionBudgetRefusal(t *testing.T) {
	pod := evictablePod()
	client, evictions := evictionClient(pod, apierrors.NewTooManyRequests(budgetRefusal, 0))
	recorder := record.NewFakeRecorder(10)
	evictor := &defaultEvictor{kubeclient: client, recorder: recorder}

	err := evictor.Evict(pod, "shuffle")
	if !apierrors.IsTooManyRequests(err) {
		t.Fatalf("expected the budget refusal returned, got %v", err)
	}
	if len(*evictions) != 1 {
		t.Fatalf("expected one eviction request, got actions %v", verbs(client))
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "delete" || action.GetVerb() == "update" {
			t.Fatalf("refused eviction still touched the pod: %v", verbs(client))
		}
	}
	got, err := client.CoreV1().Pods("ns").Get(context.TODO(), "victim", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("pod gone after a refused eviction: %v", err)
	}
	if len(got.Status.Conditions) != 1 || got.Status.Conditions[0].Status != v1.ConditionTrue {
		t.Fatalf("refused eviction changed the pod's conditions: %+v", got.Status.Conditions)
	}
	select {
	case event := <-recorder.Events:
		if !strings.Contains(event, "EvictionRefused") {
			t.Fatalf("expected an EvictionRefused event, got %q", event)
		}
	default:
		t.Fatalf("no event recorded for the refused eviction")
	}
}

// A refused eviction leaves the task for resync, so the next session sees
// the pod running again instead of releasing.
func TestCacheEvictRefusedByBudgetResyncsTheTask(t *testing.T) {
	pod := evictablePod()
	client, _ := evictionClient(pod, apierrors.NewTooManyRequests(budgetRefusal, 0))
	recorder := record.NewFakeRecorder(10)
	sc := &SchedulerCache{
		Jobs:     make(map[api.JobID]*api.JobInfo),
		Nodes:    make(map[string]*api.NodeInfo),
		errTasks: workqueue.NewTypedRateLimitingQueue[string](workqueue.DefaultTypedControllerRateLimiter[string]()),
		Recorder: recorder,
		Evictor:  &defaultEvictor{kubeclient: client, recorder: recorder},
	}
	sc.AddOrUpdateNode(buildNode("n1", api.BuildResourceList("4", "16G", []api.ScalarResource{{Name: "pods", Value: "10"}}...)))
	task := api.NewTaskInfo(pod)
	task.Job = "ns/j1"
	if err := sc.addTask(task); err != nil {
		t.Fatalf("addTask: %v", err)
	}
	sc.Jobs[task.Job].SetPodGroup(&api.PodGroup{PodGroup: scheduling.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "j1", Namespace: "ns"},
	}})

	if err := sc.Evict(task, "shuffle"); err != nil {
		t.Fatalf("Evict: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for sc.errTasks.Len() == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("refused eviction was not queued for resync")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
