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

// Per-job backoff for starving jobs whose preemption keeps failing.
//
// A starving job that cannot be pipelined by preemption is retried in every
// session, and each retry dry-runs victim selection against every candidate
// node. When the cluster is saturated with pods the job may not preempt, that
// work fails the same way session after session and dominates the preempt
// action's runtime. The backoff remembers such jobs and skips them for an
// exponentially growing interval, so their retries cost a bounded share of
// the scheduler's time while jobs that can make progress keep the rest.

package preempt

import (
	"time"

	"k8s.io/apimachinery/pkg/util/sets"

	"volcano.sh/volcano/pkg/scheduler/api"
)

type backoffEntry struct {
	failures int
	until    time.Time
}

// preemptBackoff holds the failed-preemption state that outlives a session.
// A zero base disables it: every method then reports nothing to skip.
type preemptBackoff struct {
	base    time.Duration
	max     time.Duration
	now     func() time.Time
	entries map[api.JobID]*backoffEntry
}

func newPreemptBackoff() *preemptBackoff {
	return &preemptBackoff{now: time.Now, entries: map[api.JobID]*backoffEntry{}}
}

// configure sets the interval bounds. max below base is raised to base.
func (b *preemptBackoff) configure(base, max time.Duration) {
	if base < 0 {
		base = 0
	}
	if max < base {
		max = base
	}
	b.base, b.max = base, max
}

func (b *preemptBackoff) enabled() bool {
	return b.base > 0
}

// shouldSkip reports whether the job's last failure is still being backed off.
func (b *preemptBackoff) shouldSkip(jobID api.JobID) bool {
	if !b.enabled() {
		return false
	}
	entry, found := b.entries[jobID]
	return found && b.now().Before(entry.until)
}

// recordFailure extends the job's backoff: base after the first failure,
// doubling per consecutive failure, capped at max.
func (b *preemptBackoff) recordFailure(jobID api.JobID) {
	if !b.enabled() {
		return
	}
	entry, found := b.entries[jobID]
	if !found {
		entry = &backoffEntry{}
		b.entries[jobID] = entry
	}
	entry.failures++
	entry.until = b.now().Add(b.delay(entry.failures))
}

// recordSuccess forgets the job: a committed preemption resets the interval.
func (b *preemptBackoff) recordSuccess(jobID api.JobID) {
	delete(b.entries, jobID)
}

// prune drops entries for jobs no longer among the session's starving jobs;
// they were scheduled, completed or deleted, so a later starving spell of
// the same job starts from a fresh interval.
func (b *preemptBackoff) prune(starving sets.Set[api.JobID]) {
	for jobID := range b.entries {
		if !starving.Has(jobID) {
			delete(b.entries, jobID)
		}
	}
}

// delay is base * 2^(failures-1), saturating at max.
func (b *preemptBackoff) delay(failures int) time.Duration {
	d := b.base
	for i := 1; i < failures; i++ {
		if d >= b.max/2 {
			return b.max
		}
		d *= 2
	}
	if d > b.max {
		return b.max
	}
	return d
}
