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
	"time"

	"github.com/prometheus/client_golang/prometheus"
	v1 "k8s.io/api/core/v1"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/api/restartcost"
	"volcano.sh/volcano/pkg/scheduler/metrics"
)

// restartCostGate prices a strategy's evictions by the work they throw away
// (see restartcost) and, once the strategy sets a cap, skips the decisions
// that cost more than the cap and orders the rest cheapest first. Without a
// cap, or in dry run, the strategy decides as it always has and the gate only
// counts what it would skip. Only jobs that announce restart facts count
// against the cap unless GateUnannounced is set.
type restartCostGate struct {
	DryRun               bool `mapstructure:"restartCostDryRun"`
	GateUnannounced      bool `mapstructure:"gateUnannounced"`
	restartcost.Defaults `mapstructure:",squash"`
}

func newRestartCostGate() restartCostGate {
	return restartCostGate{Defaults: restartcost.DefaultDefaults()}
}

// orders reports whether restart cost changes the strategy's decisions.
func (g restartCostGate) orders(limit float64) bool {
	return limit > 0 && !g.DryRun
}

// over reports whether a decision whose gated cost is gated exceeds limit.
func (g restartCostGate) over(limit, gated float64) bool {
	return limit > 0 && gated > limit
}

// count records a decision the gate caught and reports whether to skip it:
// live skips, dry run only counts.
func (g restartCostGate) count(skipped *prometheus.CounterVec) bool {
	mode := "live"
	if g.DryRun {
		mode = "dry_run"
	}
	skipped.WithLabelValues("restart_cost", mode).Inc()
	return !g.DryRun
}

// skip reports whether a decision must be skipped under limit, counting it
// whenever the gate catches it.
func (g restartCostGate) skip(skipped *prometheus.CounterVec, limit, gated float64) bool {
	return g.over(limit, gated) && g.count(skipped)
}

// book prices jobs with the gate's defaults.
func (g restartCostGate) book(jobs map[api.JobID]*api.JobInfo, gpu v1.ResourceName, now time.Time) *restartcost.Book {
	return restartcost.NewBook(jobs, gpu, g.Defaults, now)
}

// price is a decision's restart cost. total orders decisions; gated is what
// the cap is compared with: the share of jobs that announced restart facts,
// or all of it under GateUnannounced.
type price struct {
	total, gated float64
}

func (p *price) add(job restartcost.Price, gateUnannounced bool) {
	p.total += job.GPUSeconds
	if job.Announced || gateUnannounced {
		p.gated += job.GPUSeconds
	}
}

// victimsPrice prices evicting the victims, each job once.
func (g restartCostGate) victimsPrice(book *restartcost.Book, victims []*api.TaskInfo) price {
	var p price
	seen := map[api.JobID]bool{}
	for _, victim := range victims {
		if victim.Job != "" && seen[victim.Job] {
			continue
		}
		seen[victim.Job] = true
		p.add(book.Job(victim), g.GateUnannounced)
	}
	return p
}

// drainPrice is a drain's restart cost per GPU it frees.
func (g restartCostGate) drainPrice(book *restartcost.Book, source *api.NodeInfo, victims []*api.TaskInfo, gpu v1.ResourceName) price {
	p := g.victimsPrice(book, victims)
	if freed := source.Used.Get(gpu) / gpuMilli; freed > 0 {
		p.total, p.gated = p.total/freed, p.gated/freed
	}
	return p
}

// movePrice is a capacity move's restart cost: the mover's members as one
// gang plus every job the placement displaces.
func (g restartCostGate) movePrice(book *restartcost.Book, members []*api.TaskInfo, displaced map[string][]*api.TaskInfo) price {
	var pods []*api.TaskInfo
	for _, onNode := range displaced {
		pods = append(pods, onNode...)
	}
	p := g.victimsPrice(book, pods)
	p.add(book.Of(members), g.GateUnannounced)
	return p
}

// recordShuffleEvictions counts the strategy's evictions and their cost.
func recordShuffleEvictions(strategy string, book *restartcost.Book, victims []*api.TaskInfo) {
	metrics.RegisterEvictions("shuffle", strategy, len(victims), book.GangCosts(victims))
}

// displacedCost orders a capacity move's displaced pods by restart cost once
// the move gate is in force; nil keeps name order.
func displacedCost(conf *capacityUpgradeConf, jobs map[api.JobID]*api.JobInfo, now time.Time) func(*api.TaskInfo) float64 {
	if !conf.RestartCost.orders(conf.MaxRestartGpuSeconds) {
		return nil
	}
	return conf.RestartCost.book(jobs, v1.ResourceName(conf.GpuResource), now).Cost
}
