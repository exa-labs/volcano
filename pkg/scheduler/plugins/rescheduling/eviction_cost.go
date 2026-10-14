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
	"volcano.sh/volcano/pkg/scheduler/api/evictioncost"
	"volcano.sh/volcano/pkg/scheduler/metrics"
)

// evictionCostGate prices a strategy's evictions by the work they throw away
// (see evictioncost) and, once the strategy sets a cap, skips the decisions
// that cost more than the cap and orders the rest cheapest first. Without a
// cap, or in dry run, the strategy decides as it always has and the gate only
// counts what it would skip. Only jobs that announce restart facts count
// against the cap unless GateUnannounced is set.
type evictionCostGate struct {
	DryRun                bool `mapstructure:"evictionCostDryRun"`
	GateUnannounced       bool `mapstructure:"gateUnannounced"`
	evictioncost.Defaults `mapstructure:",squash"`
}

func newEvictionCostGate() evictionCostGate {
	return evictionCostGate{Defaults: evictioncost.DefaultDefaults()}
}

// orders reports whether eviction cost changes the strategy's decisions.
func (g evictionCostGate) orders(limit float64) bool {
	return limit > 0 && !g.DryRun
}

// over reports whether a decision whose gated cost is gated exceeds limit.
func (g evictionCostGate) over(limit, gated float64) bool {
	return limit > 0 && gated > limit
}

// count records a decision the gate caught and reports whether to skip it:
// live skips, dry run only counts.
func (g evictionCostGate) count(skipped *prometheus.CounterVec) bool {
	mode := "live"
	if g.DryRun {
		mode = "dry_run"
	}
	skipped.WithLabelValues("eviction_cost", mode).Inc()
	return !g.DryRun
}

// clone copies the ledger, so a drain can be simulated without committing it.
func (l *drainLedger) clone() *drainLedger {
	out := &drainLedger{idle: make(map[string]*api.Resource, len(l.idle)), drained: map[string]bool{}, filled: map[string]bool{}}
	for name, idle := range l.idle {
		out.idle[name] = idle.Clone()
	}
	for name := range l.drained {
		out.drained[name] = true
	}
	for name := range l.filled {
		out.filled[name] = true
	}
	return out
}

// book prices jobs with the gate's defaults.
func (g evictionCostGate) book(jobs map[api.JobID]*api.JobInfo, gpu v1.ResourceName, now time.Time) *evictioncost.Book {
	return evictioncost.NewBook(jobs, gpu, g.Defaults, now)
}

// price is a decision's eviction cost. total orders decisions; gated is what
// the cap is compared with: the share of jobs that announced restart facts,
// or all of it under GateUnannounced.
type price struct {
	total, gated float64
}

func (p *price) add(job evictioncost.Price, gateUnannounced bool) {
	p.total += job.GPUSeconds
	if job.Announced || gateUnannounced {
		p.gated += job.GPUSeconds
	}
}

// victimsPrice prices evicting the victims, each job once.
func (g evictionCostGate) victimsPrice(book *evictioncost.Book, victims []*api.TaskInfo) price {
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

// drainPrice is a drain's eviction cost per GPU it frees.
func (g evictionCostGate) drainPrice(book *evictioncost.Book, source *api.NodeInfo, victims []*api.TaskInfo, gpu v1.ResourceName) price {
	p := g.victimsPrice(book, victims)
	if freed := source.Used.Get(gpu) / gpuMilli; freed > 0 {
		p.total, p.gated = p.total/freed, p.gated/freed
	}
	return p
}

// movePrice is a capacity move's eviction cost: the mover's members as one
// gang plus every job the placement displaces.
func (g evictionCostGate) movePrice(book *evictioncost.Book, members []*api.TaskInfo, displaced map[string][]*api.TaskInfo) price {
	var pods []*api.TaskInfo
	for _, onNode := range displaced {
		pods = append(pods, onNode...)
	}
	p := g.victimsPrice(book, pods)
	p.add(book.Of(members), g.GateUnannounced)
	return p
}

// recordShuffleEvictions counts the strategy's evictions and their cost.
func recordShuffleEvictions(strategy string, book *evictioncost.Book, victims []*api.TaskInfo) {
	metrics.RegisterEvictions("shuffle", strategy, len(victims), book.GangCosts(victims))
}

// displacedCost orders a capacity move's displaced pods by eviction cost once
// the move gate is in force; nil keeps name order.
func displacedCost(conf *capacityUpgradeConf, jobs map[api.JobID]*api.JobInfo, now time.Time) func(*api.TaskInfo) float64 {
	if !conf.EvictionCost.orders(conf.MaxEvictionCostGpuSeconds) {
		return nil
	}
	return conf.EvictionCost.book(jobs, v1.ResourceName(conf.GpuResource), now).Cost
}
