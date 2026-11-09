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

package framework

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/sets"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
)

// backoffSession has one plugin that offers every task it is given as a
// victim, for preempt, reclaim and shuffle, and holds "held" in backoff.
func backoffSession(held ...api.TaskID) *Session {
	enabled := true
	everything := func(_ *api.TaskInfo, tasks []*api.TaskInfo) ([]*api.TaskInfo, int) {
		return tasks, 1
	}
	return &Session{
		Tiers: []conf.Tier{{Plugins: []conf.PluginOption{{
			Name:               "all",
			EnabledPreemptable: &enabled,
			EnabledReclaimable: &enabled,
			EnabledVictim:      &enabled,
		}}}},
		preemptableFns:  map[string]api.EvictableFn{"all": everything},
		reclaimableFns:  map[string]api.EvictableFn{"all": everything},
		victimTasksFns:  map[string][]api.VictimTasksFn{"all": {func(tasks []*api.TaskInfo) []*api.TaskInfo { return tasks }}},
		evictionBackoff: sets.New(held...),
	}
}

func uids(tasks []*api.TaskInfo) sets.Set[api.TaskID] {
	out := sets.New[api.TaskID]()
	for _, task := range tasks {
		out.Insert(task.UID)
	}
	return out
}

func backoffTasks() []*api.TaskInfo {
	return []*api.TaskInfo{
		{UID: "refused", Namespace: "ns", Name: "refused"},
		{UID: "other", Namespace: "ns", Name: "other"},
	}
}

// A task whose eviction failed recently is not offered as a victim again,
// so preempt and reclaim move on to the next candidate.
func TestPreemptableAndReclaimableSkipTasksInEvictionBackoff(t *testing.T) {
	ssn := backoffSession("refused")
	preemptor := &api.TaskInfo{UID: "preemptor"}
	for name, got := range map[string][]*api.TaskInfo{
		"Preemptable": ssn.Preemptable(preemptor, backoffTasks()),
		"Reclaimable": ssn.Reclaimable(preemptor, backoffTasks()),
	} {
		if !uids(got).Equal(sets.New[api.TaskID]("other")) {
			t.Errorf("%s offered %v, want only the task not in backoff", name, sets.List(uids(got)))
		}
	}
}

// With every candidate in backoff there is no victim at all: nothing is
// evicted, and nothing is deleted instead.
func TestPreemptableOffersNoVictimWhenAllAreInBackoff(t *testing.T) {
	ssn := backoffSession("refused", "other")
	if got := ssn.Preemptable(&api.TaskInfo{UID: "preemptor"}, backoffTasks()); len(got) != 0 {
		t.Fatalf("Preemptable offered %v, want none", sets.List(uids(got)))
	}
}

func TestVictimTasksSkipTasksInEvictionBackoff(t *testing.T) {
	ssn := backoffSession("refused")
	got := sets.New[api.TaskID]()
	for task := range ssn.VictimTasks(backoffTasks()) {
		got.Insert(task.UID)
	}
	if !got.Equal(sets.New[api.TaskID]("other")) {
		t.Fatalf("VictimTasks offered %v, want only the task not in backoff", sets.List(got))
	}
}

// Without a backoff every candidate stays eligible.
func TestNoEvictionBackoffKeepsEveryCandidate(t *testing.T) {
	ssn := backoffSession()
	if got := ssn.Preemptable(&api.TaskInfo{UID: "preemptor"}, backoffTasks()); len(got) != 2 {
		t.Fatalf("Preemptable offered %v, want both", sets.List(uids(got)))
	}
}
