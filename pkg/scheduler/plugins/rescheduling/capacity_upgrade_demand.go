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
	"sort"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
)

// pendingDemand tells the planner which target nodes are spoken for. Idle
// GPUs on a target are only worth moving into when they will stay the
// mover's: a pending task that outranks the mover and that preempt could
// pipeline onto the node today (evicting what the preemptable plugins let
// it) takes the node as soon as preempt runs, and a mover placed into that
// gap is the first pod it evicts. Such a move costs the mover two restarts
// and frees nothing the pending task would not have taken anyway.
type pendingDemand interface {
	// claimant returns a pending task that would reclaim node from the
	// candidate, or nil when the node's idle GPUs are safe to move into.
	claimant(node *api.NodeInfo, cand capacityUpgradeCandidate) *api.TaskInfo
}

// upgradeDemand is the pendingDemand preempt's own rules imply: a preemptor
// is a pending GPU task of a starving job that is past the queue gate; it
// reclaims a node when its request fits the node's future idle plus what
// its victims there release; it evicts the mover only from its own queue
// and only when its job outranks the mover's.
type upgradeDemand struct {
	// preemptors are the pending GPU tasks that may preempt, one per
	// distinct request per job, highest job priority first.
	preemptors []*api.TaskInfo
	jobs       map[api.JobID]*api.JobInfo
	// victims returns the preemptees the preemptor may evict, as the
	// session's Preemptable does.
	victims func(preemptor *api.TaskInfo, preemptees []*api.TaskInfo) []*api.TaskInfo
	// predicate probes whether the preemptor may run on a node at all.
	predicate capacityUpgradePredicate
	gpu       v1.ResourceName
	// reclaims caches, per preemptor and node, whether the preemptor could
	// be pipelined onto the node; node state does not change within a pass.
	reclaims map[string]bool
}

// newUpgradeDemand collects the pending preemptors from the session's jobs.
// starving reports whether a job is short of resources (nil: every job with
// pending GPU tasks counts).
func newUpgradeDemand(
	jobs map[api.JobID]*api.JobInfo,
	gpu v1.ResourceName,
	starving func(job *api.JobInfo) bool,
	victims func(preemptor *api.TaskInfo, preemptees []*api.TaskInfo) []*api.TaskInfo,
	predicate capacityUpgradePredicate,
) *upgradeDemand {
	demand := &upgradeDemand{jobs: jobs, victims: victims, predicate: predicate, gpu: gpu, reclaims: map[string]bool{}}
	for _, job := range jobs {
		if job.IsPending() || len(job.TaskStatusIndex[api.Pending]) == 0 {
			continue
		}
		if starving != nil && !starving(job) {
			continue
		}
		requests := map[string]bool{}
		pending := make([]*api.TaskInfo, 0, len(job.TaskStatusIndex[api.Pending]))
		for _, task := range job.TaskStatusIndex[api.Pending] {
			pending = append(pending, task)
		}
		sort.Slice(pending, func(i, j int) bool { return pending[i].Name < pending[j].Name })
		for _, task := range pending {
			if task.Pod == nil || task.InitResreq.Get(gpu) <= 0 {
				continue
			}
			key := task.InitResreq.String()
			if requests[key] {
				continue
			}
			requests[key] = true
			demand.preemptors = append(demand.preemptors, task)
		}
	}
	sort.SliceStable(demand.preemptors, func(i, j int) bool {
		pi, pj := jobs[demand.preemptors[i].Job].Priority, jobs[demand.preemptors[j].Job].Priority
		if pi != pj {
			return pi > pj
		}
		return demand.preemptors[i].Name < demand.preemptors[j].Name
	})
	return demand
}

func (d *upgradeDemand) claimant(node *api.NodeInfo, cand capacityUpgradeCandidate) *api.TaskInfo {
	mover := cand.job
	for _, preemptor := range d.preemptors {
		job := d.jobs[preemptor.Job]
		if job.Queue != mover.Queue || job.Priority <= mover.Priority {
			continue
		}
		if d.reclaim(preemptor, node) {
			return preemptor
		}
	}
	return nil
}

// reclaim reports whether preempt could pipeline the preemptor onto node:
// the node admits it and its request fits the node's future idle plus what
// its victims there (same queue, other jobs, as the preemptable chain
// allows) would release.
func (d *upgradeDemand) reclaim(preemptor *api.TaskInfo, node *api.NodeInfo) bool {
	key := string(preemptor.UID) + "/" + node.Name
	if verdict, ok := d.reclaims[key]; ok {
		return verdict
	}
	verdict := d.fits(preemptor, node)
	d.reclaims[key] = verdict
	return verdict
}

func (d *upgradeDemand) fits(preemptor *api.TaskInfo, node *api.NodeInfo) bool {
	if preemptor.InitResreq.Get(d.gpu) > node.Allocatable.Get(d.gpu) {
		return false
	}
	job := d.jobs[preemptor.Job]
	room := node.FutureIdle()
	preemptees := make([]*api.TaskInfo, 0, len(node.Tasks))
	for _, task := range node.Tasks {
		if !api.PreemptableStatus(task.Status) || !task.Preemptable || task.Job == preemptor.Job {
			continue
		}
		victim, found := d.jobs[task.Job]
		if !found || victim.Queue != job.Queue {
			continue
		}
		preemptees = append(preemptees, task.Clone())
	}
	if d.victims != nil {
		for _, victim := range d.victims(preemptor, preemptees) {
			room.Add(victim.Resreq)
		}
	}
	if !preemptor.InitResreq.LessEqual(room, api.Zero) {
		return false
	}
	// Pending gang members pin each other through pod affinity that no
	// running peer satisfies yet; the node must not be ruled out on that.
	return d.predicate == nil || d.predicate(preemptor, node, true) == nil
}

// sessionDemand builds the pendingDemand from the live session: preempt's
// job gates, the session's Preemptable chain and the planner's predicate.
func sessionDemand(gpu v1.ResourceName, predicate capacityUpgradePredicate) *upgradeDemand {
	starving := func(job *api.JobInfo) bool {
		if vr := Session.JobValid(job); vr != nil && !vr.Pass {
			return false
		}
		return Session.JobStarving(job)
	}
	return newUpgradeDemand(Session.Jobs, gpu, starving, Session.Preemptable, predicate)
}

// demandGuard applies a pendingDemand to one candidate's placement, caching
// the verdict per node and counting the nodes passed over.
type demandGuard struct {
	demand  pendingDemand
	cand    capacityUpgradeCandidate
	verdict map[string]*api.TaskInfo
	skipped int
}

func newDemandGuard(demand pendingDemand, cand capacityUpgradeCandidate) *demandGuard {
	if demand == nil {
		return nil
	}
	return &demandGuard{demand: demand, cand: cand, verdict: map[string]*api.TaskInfo{}}
}

// spokenFor reports whether node must be left alone for this candidate.
func (g *demandGuard) spokenFor(node *api.NodeInfo) bool {
	if g == nil {
		return false
	}
	claimant, seen := g.verdict[node.Name]
	if !seen {
		claimant = g.demand.claimant(node, g.cand)
		g.verdict[node.Name] = claimant
		if claimant != nil {
			g.skipped++
			capacityUpgradeTargetsSkipped.WithLabelValues("pending_demand").Inc()
			klog.V(4).Infof("capacityUpgrade: %s/%s not placed on %s: pending %s/%s (priority %d) would reclaim it",
				g.cand.job.Namespace, g.cand.job.Name, node.Name, claimant.Namespace, claimant.Name, claimant.Priority)
		}
	}
	return claimant != nil
}
