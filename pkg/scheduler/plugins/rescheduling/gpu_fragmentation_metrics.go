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

// Prometheus metrics for the gpuFragmentation strategy, exported from the
// scheduler's default registry alongside the volcano_* scheduling metrics.
// "mode" distinguishes dry-run plans from live drains so a dry-run soak
// still shows what the strategy would have done.
var (
	gpuRepackPasses = promauto.NewCounter(
		prometheus.CounterOpts{
			Subsystem: "volcano",
			Name:      "gpu_repack_passes_total",
			Help:      "Rescheduling passes that evaluated the gpuFragmentation strategy.",
		},
	)

	gpuRepackDrains = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: "volcano",
			Name:      "gpu_repack_drains_total",
			Help:      "Node drains planned by gpuFragmentation, by pool and mode (dry_run plans vs live executions).",
		}, []string{"pool", "mode"},
	)

	gpuRepackVictims = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: "volcano",
			Name:      "gpu_repack_victims_total",
			Help:      "Pods evicted (or planned in dry_run) by gpuFragmentation, by pool and mode.",
		}, []string{"pool", "mode"},
	)

	gpuRepackGpusFreed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: "volcano",
			Name:      "gpu_repack_gpus_freed_total",
			Help:      "GPUs freed on drained source nodes by gpuFragmentation, by pool and mode.",
		}, []string{"pool", "mode"},
	)

	gpuRepackStampFailures = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: "volcano",
			Name:      "gpu_repack_stamp_failures_total",
			Help:      "Cooldown/eviction-count stamping failures, by kind (source aborts the drain, victim skips the pod).",
		}, []string{"kind"},
	)

	gpuRepackLastDrain = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: "volcano",
			Name:      "gpu_repack_last_drain_timestamp_seconds",
			Help:      "Unix time of the last live drain planned by gpuFragmentation, by pool.",
		}, []string{"pool"},
	)
)
