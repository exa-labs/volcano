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

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Prometheus metrics for the capacityUpgrade strategy. "kind" is pod (one
// pod restarted on its own) or gang (a whole PodGroup restarted together);
// "mode" is dry_run (planned only), held (move started: holds written) or
// moved (successor claimed the held capacity).
var (
	capacityUpgradePasses = promauto.NewCounter(
		prometheus.CounterOpts{
			Subsystem: "volcano",
			Name:      "capacity_upgrade_passes_total",
			Help:      "Rescheduling passes that evaluated the capacityUpgrade strategy.",
		},
	)

	capacityUpgradeMoves = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: "volcano",
			Name:      "capacity_upgrade_moves_total",
			Help:      "PodGroup moves planned by capacityUpgrade, by target capacity type, kind and mode.",
		}, []string{"target", "kind", "mode"},
	)

	capacityUpgradePods = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: "volcano",
			Name:      "capacity_upgrade_pods_total",
			Help:      "Pods covered by capacityUpgrade moves, by target capacity type, kind and mode.",
		}, []string{"target", "kind", "mode"},
	)

	capacityUpgradeGpus = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: "volcano",
			Name:      "capacity_upgrade_gpus_total",
			Help:      "GPUs covered by capacityUpgrade moves, by target capacity type, kind and mode.",
		}, []string{"target", "kind", "mode"},
	)

	capacityUpgradeTargetsSkipped = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: "volcano",
			Name:      "capacity_upgrade_targets_skipped_total",
			Help:      "Target nodes a candidate was not placed on, by reason: pending_demand (a pending task outranking the mover could reclaim the node through preempt). Counted once per candidate and node per pass.",
		}, []string{"reason"},
	)

	capacityUpgradeStampFailures = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: "volcano",
			Name:      "capacity_upgrade_stamp_failures_total",
			Help:      "Cluster write failures, by kind: mover (PodGroup cooldown/budget, skips the move), hold (node hold/drain state, skips or delays the step), successor (cooldown carry-over lost).",
		}, []string{"kind"},
	)

	capacityUpgradeEvictions = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: "volcano",
			Name:      "capacity_upgrade_evictions_total",
			Help:      "Movers evicted for capacity-upgrade moves, by move kind. Retried evictions count again.",
		}, []string{"kind"},
	)

	capacityUpgradeDisplaced = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: "volcano",
			Name:      "capacity_upgrade_displaced_total",
			Help:      "Pods at or below maxDisplacedPriority evicted from target nodes to make room for a capacity-upgrade move, by move kind. Retried evictions count again.",
		}, []string{"kind"},
	)

	capacityUpgradeHoldOutcomes = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: "volcano",
			Name:      "capacity_upgrade_hold_outcomes_total",
			Help:      "Finished capacity-upgrade moves by outcome (claimed, expired, abandoned) and kind; malformed counts node annotations that were cleared.",
		}, []string{"outcome", "kind"},
	)
)
