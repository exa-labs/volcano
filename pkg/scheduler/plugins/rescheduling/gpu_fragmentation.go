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
	"context"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/mitchellh/mapstructure"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
)

// GpuFragmentationStrategy drains at most one fragmented node per pool per
// pass: every GPU pod on the node must be movable and provably fit, under a
// simulated first-fit-decreasing placement, onto other nodes in the same pool
// that are at least as (fractionally) full. Replacements are recreated by the
// pods' controllers and scheduled normally; binpack scoring plus a penalty on
// the recently drained source steer them to the fuller nodes. Cooldown and
// per-PodGroup eviction caps are the anti-thrash mechanism.
const GpuFragmentationStrategy = "gpuFragmentation"

// DefaultGpuFragmentationConf holds the default (dry-run) configuration.
var DefaultGpuFragmentationConf = map[string]interface{}{
	"dryRun":            true,
	"gpuResource":       "nvidia.com/gpu",
	"poolLabel":         "karpenter.sh/nodepool",
	"optOutLabel":       "exa.ai/repack-eligible",
	"cooldownSeconds":   1800,
	"maxVictims":        8,
	"maxVictimPriority": -1,
}

// KillSwitchEnv disables the strategy entirely when set to "true" on the
// scheduler process, e.g. `kubectl -n volcano set env deploy/<scheduler>
// EXA_GPU_REPACK_DISABLED=true`.
const KillSwitchEnv = "EXA_GPU_REPACK_DISABLED"

const (
	lastEvictionAnnotation = "exa.ai/repack-last-eviction"
	// drainSourceAnnotation marks only the drained source so the node-order
	// penalty steers replacements away from it — the pool-wide cooldown
	// clock (lastEvictionAnnotation) also lands on destinations, which must
	// NOT be penalized: they are where the replacements should go.
	drainSourceAnnotation   = "exa.ai/repack-drain-source"
	groupEvictionAnnotation = "exa.ai/repack-evictions"
	doNotDisruptAnnotation  = "karpenter.sh/do-not-disrupt"
)

type gpuFragmentationConf struct {
	DryRun      bool   `mapstructure:"dryRun"`
	GpuResource string `mapstructure:"gpuResource"`
	PoolLabel   string `mapstructure:"poolLabel"`
	// OptOutLabel excludes a pod from repacking when set to "false".
	OptOutLabel     string `mapstructure:"optOutLabel"`
	CooldownSeconds int    `mapstructure:"cooldownSeconds"`
	// MaxVictims caps total evictions per pass. A node's move set is atomic:
	// it is only taken when the whole set fits in the remaining budget. The
	// default of 8 covers the largest drainable set on an 8-GPU node: 7
	// one-GPU pods, since a fully-used node is never a source.
	MaxVictims int `mapstructure:"maxVictims"`
	// MaxVictimPriority is the highest pod priority still movable. Pods
	// without an explicit priority count as 0, so the default (-1) restricts
	// repacking to negative-priority (interruptible) workloads.
	MaxVictimPriority int32 `mapstructure:"maxVictimPriority"`
}

func newGpuFragmentationConf() *gpuFragmentationConf {
	conf := &gpuFragmentationConf{}
	_ = mapstructure.Decode(DefaultGpuFragmentationConf, conf)
	return conf
}

func (c *gpuFragmentationConf) parse(configs map[string]interface{}) {
	if len(configs) == 0 {
		return
	}
	if err := mapstructure.Decode(configs, c); err != nil {
		klog.Errorf("gpuFragmentation: bad strategy params, keeping defaults: %v", err)
		*c = *newGpuFragmentationConf()
	}
}

var victimsFnForGpuFragmentation = func(tasks []*api.TaskInfo) []*api.TaskInfo {
	if Session == nil {
		return nil
	}
	if os.Getenv(KillSwitchEnv) == "true" {
		klog.V(2).Infof("gpuFragmentation: disabled via %s", KillSwitchEnv)
		return nil
	}
	conf := newGpuFragmentationConf()
	if params, ok := RegisteredStrategyConfigs[GpuFragmentationStrategy].(map[string]interface{}); ok {
		conf.parse(params)
	}
	gpuRepackPasses.Inc()

	running := make(map[types.UID]*api.TaskInfo, len(tasks))
	for _, task := range tasks {
		if task.Pod != nil {
			running[task.Pod.UID] = task
		}
	}

	// Probe and PrePredicate once per victim, not per (victim, candidate,
	// source): predicate cost otherwise scales O(sources x candidates x
	// victims) with a pod deep-copy per call.
	probes := make(map[types.UID]*api.TaskInfo)
	preFailed := make(map[types.UID]error)
	drains := planGpuFragmentationDrains(Session.Nodes, Session.Jobs, running, conf, time.Now(), func(task *api.TaskInfo, node *api.NodeInfo) error {
		uid := task.Pod.UID
		if err, ok := preFailed[uid]; ok {
			return err
		}
		probe, ok := probes[uid]
		if !ok {
			probe = probeTask(task)
			if probe == nil {
				preFailed[uid] = fmt.Errorf("task has no pod")
				return preFailed[uid]
			}
			if err := Session.PrePredicateFn(probe); err != nil {
				preFailed[uid] = err
				return err
			}
			probes[uid] = probe
		}
		return Session.PredicateFn(probe, node)
	})

	victims := make([]*api.TaskInfo, 0)
	for _, drain := range drains {
		if conf.DryRun {
			klog.V(2).Infof("gpuFragmentation[dry-run]: would drain %s (pool %s): %d pods, frees %v GPUs -> %v",
				drain.source, drain.pool, len(drain.moves), drain.gain, drain.destinations())
			drain.observe("dry_run", len(drain.moves))
			continue
		}
		// The drain set is atomic in planning; keep execution as close to
		// that as possible: one node stamp for the whole set (plus the
		// destinations, which outlive the drained source), aborting the set
		// when the clock cannot be recorded.
		if err := stampGpuFragmentationNodes(drain); err != nil {
			klog.Errorf("gpuFragmentation: skip drain of %s, failed to record cooldown: %v", drain.source, err)
			gpuRepackStampFailures.WithLabelValues("source").Inc()
			continue
		}
		klog.V(2).Infof("gpuFragmentation: draining %s (pool %s): %d pods, frees %v GPUs -> %v",
			drain.source, drain.pool, len(drain.moves), drain.gain, drain.destinations())
		evicted := 0
		for _, move := range drain.moves {
			if err := stampGpuFragmentationVictim(move.victim); err != nil {
				klog.Errorf("gpuFragmentation: skip eviction of %s/%s, failed to record move: %v",
					move.victim.Namespace, move.victim.Name, err)
				gpuRepackStampFailures.WithLabelValues("victim").Inc()
				continue
			}
			klog.V(2).Infof("gpuFragmentation: evicting %s/%s from %s (destination %s fits)",
				move.victim.Namespace, move.victim.Name, drain.source, move.destination)
			victims = append(victims, move.victim)
			evicted++
		}
		drain.observe("live", evicted)
		gpuRepackLastDrain.WithLabelValues(drain.pool).SetToCurrentTime()
	}
	return victims
}

// gpuFragmentationNodeOrderFn steers replacement pods away from nodes whose
// repack cooldown clock is still running. Without it, an equally-utilized
// drained node scores identically to the intended destination under binpack
// and the replacement lands on either at random, undoing the drain.
func gpuFragmentationNodeOrderFn(conf *gpuFragmentationConf) api.NodeOrderFn {
	gpu := v1.ResourceName(conf.GpuResource)
	return func(task *api.TaskInfo, node *api.NodeInfo) (float64, error) {
		if node.Node == nil {
			return 0, nil
		}
		if task.Resreq.Get(gpu) <= 0 && task.InitResreq.Get(gpu) <= 0 {
			return 0, nil
		}
		raw, ok := node.Node.Annotations[drainSourceAnnotation]
		if !ok {
			return 0, nil
		}
		last, err := time.Parse(time.RFC3339, raw)
		if err != nil || time.Since(last) >= time.Duration(conf.CooldownSeconds)*time.Second {
			return 0, nil
		}
		return -drainedNodePenalty, nil
	}
}

// drainedNodePenalty outweighs a binpack tie (binpack scores 0-100 per
// weight) without vetoing the node: a replacement can still land on a
// cooling node when nothing else fits.
const drainedNodePenalty = float64(100)

type gpuFragmentationDrain struct {
	pool   string
	source string
	moves  []gpuFragmentationMove
	gain   float64
}

// observe records the drain in the strategy's Prometheus counters. gain is
// in the scheduler's milli-units, so it is scaled back to whole GPUs.
func (d gpuFragmentationDrain) observe(mode string, victims int) {
	gpuRepackDrains.WithLabelValues(d.pool, mode).Inc()
	gpuRepackVictims.WithLabelValues(d.pool, mode).Add(float64(victims))
	gpuRepackGpusFreed.WithLabelValues(d.pool, mode).Add(d.gain / 1000)
}

func (d gpuFragmentationDrain) destinations() []string {
	names := make([]string, 0, len(d.moves))
	for _, move := range d.moves {
		names = append(names, move.destination)
	}
	return names
}

// probeTask builds an unbound copy of a running task so session predicates
// evaluate it against candidate destinations rather than its current node.
func probeTask(task *api.TaskInfo) *api.TaskInfo {
	if task.Pod == nil {
		return nil
	}
	pod := task.Pod.DeepCopy()
	pod.Spec.NodeName = ""
	pod.Status = v1.PodStatus{}
	return api.NewTaskInfo(pod)
}

// planGpuFragmentationDrains drains at most one node per pool, capped at
// conf.MaxVictims evictions overall. A node drains only when every GPU task
// on it is movable (single-member PodGroup, unspent eviction cap, at or below
// the priority ceiling, controller-owned, not opted out or protected), the
// pool's cooldown clock permits it, and a simulated first-fit-decreasing
// placement fits all of them onto other pool nodes that are at least as full
// (fractionally, so mixed-size pools compare fullness fairly), passing
// resources and predicates. Less-utilized nodes are drained first;
// equally-utilized nodes tie-break toward the one running lower-priority
// victims, then fewer victims, so the cheapest-to-disrupt workload moves.
func planGpuFragmentationDrains(
	nodes map[string]*api.NodeInfo,
	jobs map[api.JobID]*api.JobInfo,
	running map[types.UID]*api.TaskInfo,
	conf *gpuFragmentationConf,
	now time.Time,
	predicate func(*api.TaskInfo, *api.NodeInfo) error,
) []gpuFragmentationDrain {
	gpu := v1.ResourceName(conf.GpuResource)
	pools := make(map[string][]*api.NodeInfo)
	for _, node := range nodes {
		if node.Node == nil || node.Allocatable.Get(gpu) <= 0 {
			continue
		}
		pool, ok := node.Node.Labels[conf.PoolLabel]
		if !ok || pool == "" {
			continue
		}
		pools[pool] = append(pools[pool], node)
	}

	poolNames := make([]string, 0, len(pools))
	for pool := range pools {
		poolNames = append(poolNames, pool)
	}
	sort.Strings(poolNames)

	drains := make([]gpuFragmentationDrain, 0)
	planned := 0
	for _, pool := range poolNames {
		if conf.MaxVictims > 0 && planned >= conf.MaxVictims {
			break
		}
		members := pools[pool]
		// Cooldown is a pool-wide budget: any recent (or unparseable)
		// eviction clock in the pool holds the whole pool.
		cooled := true
		for _, node := range members {
			if raw, ok := node.Node.Annotations[lastEvictionAnnotation]; ok {
				last, err := time.Parse(time.RFC3339, raw)
				if err != nil || last.After(now) || now.Sub(last) < time.Duration(conf.CooldownSeconds)*time.Second {
					cooled = false
					break
				}
			}
		}
		if !cooled {
			continue
		}
		type drainable struct {
			source  *api.NodeInfo
			victims []*api.TaskInfo
		}
		sources := make([]drainable, 0, len(members))
		for _, member := range members {
			victims := movableGpuTasks(member, jobs, running, conf, gpu)
			if len(victims) == 0 {
				continue
			}
			sources = append(sources, drainable{source: member, victims: victims})
		}
		sort.SliceStable(sources, func(i, j int) bool {
			ui, uj := gpuFullness(sources[i].source, gpu), gpuFullness(sources[j].source, gpu)
			if ui != uj {
				return ui < uj
			}
			pi, pj := maxVictimPriority(sources[i].victims), maxVictimPriority(sources[j].victims)
			if pi != pj {
				return pi < pj
			}
			if len(sources[i].victims) != len(sources[j].victims) {
				return len(sources[i].victims) < len(sources[j].victims)
			}
			return sources[i].source.Name < sources[j].source.Name
		})
		for _, cand := range sources {
			source, victims := cand.source, cand.victims
			if conf.MaxVictims > 0 && planned+len(victims) > conf.MaxVictims {
				// Falling through to a fuller source under budget pressure
				// would silently invert the emptiest-first policy.
				break
			}
			moves := simulateDrain(members, source, victims, gpu, predicate)
			if len(moves) == 0 {
				continue
			}
			drains = append(drains, gpuFragmentationDrain{
				pool:   pool,
				source: source.Name,
				moves:  moves,
				gain:   source.Used.Get(gpu),
			})
			planned += len(moves)
			break
		}
	}
	return drains
}

// gpuFullness is the node's GPU utilization as a fraction of allocatable, so
// differently-sized nodes in one pool compare fairly: which node survives a
// consolidation should not depend on absolute GPU counts (or names).
func gpuFullness(node *api.NodeInfo, gpu v1.ResourceName) float64 {
	allocatable := node.Allocatable.Get(gpu)
	if allocatable <= 0 {
		return 0
	}
	return node.Used.Get(gpu) / allocatable
}

// movableGpuTasks returns every GPU-consuming task on the node iff all of
// them are safe to move: running, not opted out, not protected, at or below
// the movable priority ceiling, owned by a controller that will recreate
// them, in a single-member PodGroup with an unspent eviction cap. Draining is
// all-or-nothing — one immovable GPU task keeps the node's GPUs stranded, so
// evicting the others would churn pods without freeing the node.
func movableGpuTasks(
	node *api.NodeInfo,
	jobs map[api.JobID]*api.JobInfo,
	running map[types.UID]*api.TaskInfo,
	conf *gpuFragmentationConf,
	gpu v1.ResourceName,
) []*api.TaskInfo {
	used := node.Used.Get(gpu)
	if used <= 0 || used >= node.Allocatable.Get(gpu) {
		return nil
	}
	victims := make([]*api.TaskInfo, 0, len(node.Tasks))
	for _, task := range node.Tasks {
		if task.Resreq.Get(gpu) <= 0 && task.InitResreq.Get(gpu) <= 0 {
			continue
		}
		sessionTask := movableTask(task, jobs, running, conf)
		if sessionTask == nil {
			return nil
		}
		victims = append(victims, sessionTask)
	}
	sort.Slice(victims, func(i, j int) bool { return victims[i].Name < victims[j].Name })
	return victims
}

// movableTask returns the session-side counterpart of a node-local task iff
// the task is safe to move, nil otherwise.
func movableTask(
	task *api.TaskInfo,
	jobs map[api.JobID]*api.JobInfo,
	running map[types.UID]*api.TaskInfo,
	conf *gpuFragmentationConf,
) *api.TaskInfo {
	if task.Pod == nil {
		return nil
	}
	// node.Tasks holds node-local clones; the eviction path mutates the
	// victim's status in place, so the session-side task must be returned
	// or node resource accounting corrupts and the scheduler panics.
	sessionTask, isRunning := running[task.Pod.UID]
	if !isRunning {
		return nil
	}
	if task.Pod.Labels[conf.OptOutLabel] == "false" {
		return nil
	}
	if task.Pod.Annotations[doNotDisruptAnnotation] == "true" {
		return nil
	}
	priority := int32(0)
	if task.Pod.Spec.Priority != nil {
		priority = *task.Pod.Spec.Priority
	}
	if priority > conf.MaxVictimPriority {
		return nil
	}
	if metav1.GetControllerOf(task.Pod) == nil {
		return nil
	}
	if task.Job == "" {
		return nil
	}
	job, ok := jobs[task.Job]
	if !ok || job.PodGroup == nil {
		return nil
	}
	if len(job.Tasks) != 1 || job.PodGroup.Spec.MinMember > 1 {
		return nil
	}
	if raw, ok := job.PodGroup.Annotations[groupEvictionAnnotation]; ok && raw != "0" {
		return nil
	}
	return sessionTask
}

// maxVictimPriority returns the highest pod priority in the victim set;
// pods without an explicit priority count as 0.
func maxVictimPriority(victims []*api.TaskInfo) int32 {
	var max int32
	for i, victim := range victims {
		priority := int32(0)
		if victim.Pod != nil && victim.Pod.Spec.Priority != nil {
			priority = *victim.Pod.Spec.Priority
		}
		if i == 0 || priority > max {
			max = priority
		}
	}
	return max
}

type gpuFragmentationMove struct {
	victim      *api.TaskInfo
	destination string
}

// simulateDrain proves the whole victim set fits on other pool nodes today
// via first-fit-decreasing placement over cloned idle capacity, so two
// victims cannot both claim the same free GPU. Destinations must be at least
// as (fractionally) full as the source (equally-full nodes consolidate in
// one deterministic direction — the planner's source ordering — instead of
// swapping pods), and are tried fullest-first. Returns nil unless every
// victim places; the replacements are not pinned — binpack scoring makes the
// fuller nodes the likely landing spots.
//
// The simulation covers resources, not placed-pod-dependent predicates:
// predicates run against live node state, so constraints that react to pods
// already on a node (anti-affinity, topology spread) can admit fewer victims
// at execution time than the simulation placed, leaving replacements Pending.
func simulateDrain(
	members []*api.NodeInfo,
	source *api.NodeInfo,
	victims []*api.TaskInfo,
	gpu v1.ResourceName,
	predicate func(*api.TaskInfo, *api.NodeInfo) error,
) []gpuFragmentationMove {
	if len(victims) == 0 {
		return nil
	}
	sourceFullness := gpuFullness(source, gpu)
	type candidate struct {
		node *api.NodeInfo
		idle *api.Resource
	}
	candidates := make([]candidate, 0, len(members))
	for _, dest := range members {
		if dest.Name == source.Name {
			continue
		}
		if gpuFullness(dest, gpu) < sourceFullness {
			continue
		}
		candidates = append(candidates, candidate{node: dest, idle: dest.Idle.Clone()})
	}
	if len(candidates) == 0 {
		return nil
	}
	sort.Slice(candidates, func(i, j int) bool {
		ui, uj := gpuFullness(candidates[i].node, gpu), gpuFullness(candidates[j].node, gpu)
		if ui != uj {
			return ui > uj
		}
		return candidates[i].node.Name < candidates[j].node.Name
	})

	ordered := make([]*api.TaskInfo, len(victims))
	copy(ordered, victims)
	sort.SliceStable(ordered, func(i, j int) bool {
		return taskGpuNeed(ordered[i], gpu).Get(gpu) > taskGpuNeed(ordered[j], gpu).Get(gpu)
	})

	moves := make([]gpuFragmentationMove, 0, len(ordered))
placement:
	for _, victim := range ordered {
		need := taskGpuNeed(victim, gpu)
		for i := range candidates {
			if !need.LessEqual(candidates[i].idle, api.Zero) {
				continue
			}
			if predicate != nil {
				if err := predicate(victim, candidates[i].node); err != nil {
					continue
				}
			}
			candidates[i].idle.Sub(need)
			moves = append(moves, gpuFragmentationMove{victim: victim, destination: candidates[i].node.Name})
			continue placement
		}
		return nil
	}
	return moves
}

// taskGpuNeed picks the larger of the task's init and running resource
// requests, keyed on the GPU dimension.
func taskGpuNeed(task *api.TaskInfo, gpu v1.ResourceName) *api.Resource {
	if task.Resreq.Get(gpu) > task.InitResreq.Get(gpu) {
		return task.Resreq
	}
	return task.InitResreq
}

// stampGpuFragmentationNodes durably records the drain's cooldown clock
// before any eviction, once per drain rather than once per victim. The
// destinations are stamped too: they survive the drain by construction,
// while the emptied source is reaped along with the pool's only cooldown
// record — which would erase the anti-thrash window exactly when the drain
// succeeds. A source stamp failure aborts the drain so the budget cannot be
// overspent; a destination stamp failure is logged but not fatal, since the
// source clock alone already holds the pool.
func stampGpuFragmentationNodes(drain gpuFragmentationDrain) error {
	now := time.Now().UTC().Format(time.RFC3339)
	sourcePatch := []byte(fmt.Sprintf(`{"metadata":{"annotations":{%q:%q,%q:%q}}}`,
		lastEvictionAnnotation, now, drainSourceAnnotation, now))
	if _, err := Session.KubeClient().CoreV1().Nodes().Patch(
		context.TODO(), drain.source, types.MergePatchType, sourcePatch, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("stamp node %s: %w", drain.source, err)
	}
	nodePatch := []byte(fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, lastEvictionAnnotation, now))
	stamped := map[string]bool{drain.source: true}
	for _, move := range drain.moves {
		if stamped[move.destination] {
			continue
		}
		stamped[move.destination] = true
		if _, err := Session.KubeClient().CoreV1().Nodes().Patch(
			context.TODO(), move.destination, types.MergePatchType, nodePatch, metav1.PatchOptions{}); err != nil {
			klog.Errorf("gpuFragmentation: failed to stamp destination %s: %v", move.destination, err)
		}
	}
	return nil
}

// stampGpuFragmentationVictim records the victim PodGroup's eviction count
// before eviction. Failure aborts this victim's eviction so its cap cannot
// be silently overspent.
func stampGpuFragmentationVictim(victim *api.TaskInfo) error {
	job := Session.Jobs[victim.Job]
	if job == nil || job.PodGroup == nil {
		return fmt.Errorf("podgroup for %s/%s not found", victim.Namespace, victim.Name)
	}
	pgPatch := []byte(fmt.Sprintf(`{"metadata":{"annotations":{%q:"1"}}}`, groupEvictionAnnotation))
	if _, err := Session.VCClient().SchedulingV1beta1().PodGroups(job.PodGroup.Namespace).Patch(
		context.TODO(), job.PodGroup.Name, types.MergePatchType, pgPatch, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("stamp podgroup %s/%s: %w", job.PodGroup.Namespace, job.PodGroup.Name, err)
	}
	return nil
}
