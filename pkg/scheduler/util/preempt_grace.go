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

package util

import (
	"fmt"
	"time"

	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
)

// PreemptGraceAnnotation is a pod annotation carrying a Go time.ParseDuration
// value (e.g. "10m", "600s") that overrides the action's default grace period
// for that task.
const PreemptGraceAnnotation = "exa.ai/preempt-grace"

// DefaultPreemptGraceKey is the action argument name for the default grace
// period (a Go time.ParseDuration string; unset or 0 disables the gate).
const DefaultPreemptGraceKey = "defaultPreemptGrace"

// ParseGraceDuration parses a Go duration string and rejects negative values.
func ParseGraceDuration(raw string) (time.Duration, error) {
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, err
	}
	if d < 0 {
		return 0, fmt.Errorf("negative duration %q", raw)
	}
	return d, nil
}

// PreemptGraceRemaining reports how much of a starving job's grace period is
// left before preemption/reclaim may act on its behalf, giving an external
// autoscaler time to add capacity for the pending pods first.
//
// Only the job's pending, non-scheduling-gated tasks with a pod are
// considered. Each task's grace is its PreemptGraceAnnotation value, falling
// back to defaultGrace when the annotation is missing or unparsable. The job's
// grace is the maximum across those tasks; the wait already elapsed is measured
// from the oldest such pod's CreationTimestamp. The result is clamped at zero
// and is zero when there are no such tasks or the grace is zero.
func PreemptGraceRemaining(job *api.JobInfo, defaultGrace time.Duration, now time.Time) time.Duration {
	var grace time.Duration
	var earliest time.Time
	for _, task := range job.TaskStatusIndex[api.Pending] {
		if task.SchGated || task.Pod == nil {
			continue
		}

		taskGrace := defaultGrace
		if raw, ok := task.Pod.Annotations[PreemptGraceAnnotation]; ok {
			d, err := ParseGraceDuration(raw)
			if err != nil {
				klog.V(3).Infof("Task <%s/%s> has invalid %s annotation %q: %v; using default grace",
					task.Namespace, task.Name, PreemptGraceAnnotation, raw, err)
			} else {
				taskGrace = d
			}
		}
		if taskGrace > grace {
			grace = taskGrace
		}
		if earliest.IsZero() || task.Pod.CreationTimestamp.Time.Before(earliest) {
			earliest = task.Pod.CreationTimestamp.Time
		}
	}

	if earliest.IsZero() || grace <= 0 {
		return 0
	}
	remaining := grace - now.Sub(earliest)
	if remaining < 0 {
		return 0
	}
	return remaining
}
