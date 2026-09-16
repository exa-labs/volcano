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

// Prometheus metrics for the capacityUpgrade strategy. "kind" is pod (a
// single-member PodGroup evicted directly) or gang (a proposal left for the
// lifecycle owner); "mode" is dry_run, live (evicted) or proposed.
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

	capacityUpgradeStampFailures = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: "volcano",
			Name:      "capacity_upgrade_stamp_failures_total",
			Help:      "PodGroup annotation failures, by kind (proposal skips the gang, victim skips the eviction).",
		}, []string{"kind"},
	)
)
