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

// Restart detection for the capacityUpgrade strategy.
//
// The strategy keeps no record of the workloads it restarted. Whether a pod
// is a recent restart, and how often its workload has restarted, is read off
// the pod itself: Flyte names a task's pod <execution>-<node-id>-<attempt>,
// and names the PyTorchJob it creates for a gang the same way, so the
// trailing number counts the workload's restarts within its execution, by
// this strategy, by capacity reclaim or by failure alike. A gang relaunched
// as a new execution starts over at attempt 0 and is only known by its age.
//
// A pod is therefore left alone, as mover and as victim, while it is
// younger than the cooldown, unless it is provably the first attempt of a
// workload whose restarts stay within its execution: a pod named with
// attempt 0 directly. Everything else that is young (a gang member, a pod
// whose name says nothing, any pod at attempt > 0) counts as just restarted.

import (
	"strconv"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// workloadAttempt is the pod's attempt index and whether it was read from
// the pod's own name (direct) rather than its controller's. A name yields an
// attempt when it ends in "-<node-id value>-<n>", where node-id is the
// pod's label attemptLabel. Pods named neither way have no known attempt.
func workloadAttempt(pod *v1.Pod, attemptLabel string) (attempt int, direct, ok bool) {
	nodeID := pod.Labels[attemptLabel]
	if nodeID == "" {
		return 0, false, false
	}
	if attempt, ok := nameAttempt(pod.Name, nodeID); ok {
		return attempt, true, true
	}
	if owner := metav1.GetControllerOf(pod); owner != nil {
		if attempt, ok := nameAttempt(owner.Name, nodeID); ok {
			return attempt, false, true
		}
	}
	return 0, false, false
}

// nameAttempt parses the attempt out of a Flyte-generated name ending in
// "-<nodeID>-<n>".
func nameAttempt(name, nodeID string) (int, bool) {
	cut := strings.LastIndex(name, "-")
	if cut <= 0 || cut == len(name)-1 {
		return 0, false
	}
	attempt, err := strconv.Atoi(name[cut+1:])
	if err != nil || attempt < 0 {
		return 0, false
	}
	if !strings.HasSuffix(name[:cut], "-"+nodeID) {
		return 0, false
	}
	return attempt, true
}

// recentlyRestarted reports whether the pod counts as a restart still inside
// the cooldown: it started less than cooldown ago and is not, by its own
// name, the first attempt of its workload. A pod that has not started is
// not a restart of anything yet.
func recentlyRestarted(pod *v1.Pod, attemptLabel string, now time.Time, cooldown time.Duration) bool {
	started := podStartTime(pod)
	if started.IsZero() || now.Sub(started) >= cooldown {
		return false
	}
	attempt, direct, ok := workloadAttempt(pod, attemptLabel)
	return !(ok && direct && attempt == 0)
}
