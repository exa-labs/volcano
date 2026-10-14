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

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Evictions by the action (preempt, reclaim, shuffle) and strategy that chose
// them, and the eviction cost they threw away. Costs are per gang: a job with
// several evicted members is observed once.
var (
	evictionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: VolcanoSubSystemName,
			Name:      "evictions_total",
			Help:      "Pods evicted, by action and strategy.",
		}, []string{"action", "strategy"},
	)

	evictionCost = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: VolcanoSubSystemName,
			Name:      "eviction_cost_gpu_seconds",
			Help:      "Estimated GPU-seconds of work lost per evicted job, by action and strategy.",
			Buckets:   prometheus.ExponentialBuckets(60, 4, 11),
		}, []string{"action", "strategy"},
	)

	evictionCostShadowDecisions = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: VolcanoSubSystemName,
			Name:      "eviction_cost_shadow_decisions_total",
			Help:      "Victim selections evaluated in evictionCost shadow mode, by action and whether the cost-aware choice differed (same, different).",
		}, []string{"action", "outcome"},
	)

	evictionCostShadowGpuSeconds = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: VolcanoSubSystemName,
			Name:      "eviction_cost_shadow_gpu_seconds_total",
			Help:      "Eviction cost of shadow-mode victim selections, by action and choice: baseline (acted on) or cost_aware (what evictionCost on would evict). The difference is the saving.",
		}, []string{"action", "choice"},
	)
)

// RegisterEvictions counts pods evicted by an action and observes the
// eviction cost of each evicted job.
func RegisterEvictions(action, strategy string, pods int, jobCosts []float64) {
	if pods == 0 {
		return
	}
	evictionsTotal.WithLabelValues(action, strategy).Add(float64(pods))
	for _, cost := range jobCosts {
		evictionCost.WithLabelValues(action, strategy).Observe(cost)
	}
}

// RegisterEvictionCostShadow records one shadow-mode comparison between the
// victims an action evicted and the ones cost-aware ordering would evict.
func RegisterEvictionCostShadow(action string, differs bool, baseline, costAware float64) {
	outcome := "same"
	if differs {
		outcome = "different"
	}
	evictionCostShadowDecisions.WithLabelValues(action, outcome).Inc()
	evictionCostShadowGpuSeconds.WithLabelValues(action, "baseline").Add(baseline)
	evictionCostShadowGpuSeconds.WithLabelValues(action, "cost_aware").Add(costAware)
}
