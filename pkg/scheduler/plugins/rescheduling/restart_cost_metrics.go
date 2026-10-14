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

// Decisions the restart-cost gates caught, by reason (restart_cost) and mode:
// live (skipped) or dry_run (counted, not skipped).
var (
	gpuRepackSkipped = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: "volcano",
			Name:      "gpu_repack_skipped_total",
			Help:      "Drain sources gpuFragmentation passed over, by reason and mode (live skips vs dry_run counts).",
		}, []string{"reason", "mode"},
	)

	capacityUpgradeSkipped = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: "volcano",
			Name:      "capacity_upgrade_skipped_total",
			Help:      "Moves capacityUpgrade passed over after placing them, by reason and mode (live skips vs dry_run counts).",
		}, []string{"reason", "mode"},
	)
)
