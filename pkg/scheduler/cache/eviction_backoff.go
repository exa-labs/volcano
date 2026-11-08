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
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"

	"volcano.sh/volcano/pkg/scheduler/api"
)

// A refused eviction never falls back to deleting the pod. Instead the pod is
// held out of victim selection for a bounded interval, so preempt, reclaim
// and shuffle move on to other candidates rather than asking the API server
// for the same refused eviction every session.
const (
	evictionBackoffBase = 10 * time.Second
	evictionBackoffMax  = 5 * time.Minute
)

type evictionBackoffEntry struct {
	failures int
	until    time.Time
}

// evictionBackoff remembers pods whose eviction failed. Each failure of the
// same pod doubles its interval, from base up to max; an accepted eviction or
// the pod's deletion forgets it. It lives in memory only: a restarted
// scheduler starts with every pod eligible again. A nil *evictionBackoff
// holds nothing.
type evictionBackoff struct {
	mu      sync.Mutex
	base    time.Duration
	max     time.Duration
	now     func() time.Time
	entries map[api.TaskID]*evictionBackoffEntry
}

func newEvictionBackoff() *evictionBackoff {
	return &evictionBackoff{
		base:    evictionBackoffBase,
		max:     evictionBackoffMax,
		now:     time.Now,
		entries: map[api.TaskID]*evictionBackoffEntry{},
	}
}

// failed records a failed eviction of the pod and returns how long it is
// held out of victim selection.
func (b *evictionBackoff) failed(uid api.TaskID) time.Duration {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	entry, ok := b.entries[uid]
	if !ok {
		entry = &evictionBackoffEntry{}
		b.entries[uid] = entry
	}
	entry.failures++
	interval := b.max
	// base << (failures-1), without overflowing once it passes max.
	if entry.failures <= 32 {
		if d := b.base << (entry.failures - 1); d > 0 && d < b.max {
			interval = d
		}
	}
	entry.until = b.now().Add(interval)
	return interval
}

// forget drops the pod: its eviction was accepted or it is gone.
func (b *evictionBackoff) forget(uid api.TaskID) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.entries, uid)
}

// active returns the pods still held out of victim selection. An expired
// entry keeps its failure count, so a pod refused again waits longer.
func (b *evictionBackoff) active() sets.Set[api.TaskID] {
	held := sets.New[api.TaskID]()
	if b == nil {
		return held
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	for uid, entry := range b.entries {
		if now.Before(entry.until) {
			held.Insert(uid)
		}
	}
	return held
}
