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
// that are at least as full. Replacements are recreated by the pods'
// controllers and scheduled normally; binpack scoring steers them to the
// fuller nodes. Cooldown and per-PodGroup eviction caps are the anti-thrash
// mechanism.
const GpuFragmentationStrategy = "gpuFragmentation"

// DefaultGpuFragmentationConf holds the default (dry-run) configuration.
var DefaultGpuFragmentationConf = map[string]interface{}{
	"dryRun":            true,
	"gpuResource":       "nvidia.com/gpu",
	"poolLabel":         "karpenter.sh/nodepool",
	"optOutLabel":       "exa.ai/repack-eligible",
	"cooldownSeconds":   1800,
	"maxVictims":        4,
	"maxVictimPriority": -1,
}

// KillSwitchEnv disables the strategy entirely when set to "true" on the
// scheduler process, e.g. `kubectl -n volcano set env deploy/<scheduler>
// EXA_GPU_REPACK_DISABLED=true`.
const KillSwitchEnv = "EXA_GPU_REPACK_DISABLED"

const (
	lastEvictionAnnotation  = "exa.ai/repack-last-eviction"
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
	// it is only taken when the whole set fits in the remaining budget.
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

	running := make(map[types.UID]*api.TaskInfo, len(tasks))
	for _, task := range tasks {
		if task.Pod != nil {
			running[task.Pod.UID] = task
		}
	}

	plans := planGpuFragmentationMoves(Session.Nodes, Session.Jobs, running, conf, time.Now(), func(task *api.TaskInfo, node *api.NodeInfo) error {
		probe := probeTask(task)
		if probe == nil {
			return fmt.Errorf("task has no pod")
		}
		if err := Session.PrePredicateFn(probe); err != nil {
			return err
		}
		return Session.PredicateFn(probe, node)
	})

	victims := make([]*api.TaskInfo, 0, len(plans))
	for _, plan := range plans {
		if conf.DryRun {
			klog.V(2).Infof("gpuFragmentation[dry-run]: would evict %s/%s from %s (pool %s, source empties %v GPUs; destination %s fits)",
				plan.victim.Namespace, plan.victim.Name, plan.source, plan.pool, plan.gain, plan.destination)
			continue
		}
		if err := stampGpuFragmentationMove(plan); err != nil {
			klog.Errorf("gpuFragmentation: skip eviction of %s/%s, failed to record move: %v",
				plan.victim.Namespace, plan.victim.Name, err)
			continue
		}
		klog.V(2).Infof("gpuFragmentation: evicting %s/%s from %s (pool %s, frees %v GPUs; destination %s fits)",
			plan.victim.Namespace, plan.victim.Name, plan.source, plan.pool, plan.gain, plan.destination)
		victims = append(victims, plan.victim)
	}
	return victims
}

type gpuFragmentationPlan struct {
	victim      *api.TaskInfo
	pool        string
	source      string
	destination string
	gain        float64
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

// planGpuFragmentationMoves drains at most one node per pool, capped at
// conf.MaxVictims evictions overall. A node drains only when every GPU task
// on it is movable (single-member PodGroup, unspent eviction cap, at or below
// the priority ceiling, controller-owned, not opted out or protected), the
// pool's cooldown clock permits it, and a simulated first-fit-decreasing
// placement fits all of them onto other pool nodes that are at least as full,
// passing resources and predicates. Emptier nodes are drained first.
func planGpuFragmentationMoves(
	nodes map[string]*api.NodeInfo,
	jobs map[api.JobID]*api.JobInfo,
	running map[types.UID]*api.TaskInfo,
	conf *gpuFragmentationConf,
	now time.Time,
	predicate func(*api.TaskInfo, *api.NodeInfo) error,
) []gpuFragmentationPlan {
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

	plans := make([]gpuFragmentationPlan, 0)
	for _, pool := range poolNames {
		if conf.MaxVictims > 0 && len(plans) >= conf.MaxVictims {
			break
		}
		members := pools[pool]
		sort.Slice(members, func(i, j int) bool {
			ui, uj := members[i].Used.Get(gpu), members[j].Used.Get(gpu)
			if ui != uj {
				return ui < uj
			}
			return members[i].Name < members[j].Name
		})
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
		for _, source := range members {
			victims := movableGpuTasks(source, jobs, running, conf, gpu)
			if len(victims) == 0 {
				continue
			}
			if conf.MaxVictims > 0 && len(plans)+len(victims) > conf.MaxVictims {
				continue
			}
			moves := simulateDrain(members, source, victims, gpu, predicate)
			if moves == nil {
				continue
			}
			for _, move := range moves {
				plans = append(plans, gpuFragmentationPlan{
					victim:      move.victim,
					pool:        pool,
					source:      source.Name,
					destination: move.destination,
					gain:        source.Used.Get(gpu),
				})
			}
			break
		}
	}
	return plans
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

type gpuFragmentationMove struct {
	victim      *api.TaskInfo
	destination string
}

// simulateDrain proves the whole victim set fits on other pool nodes today
// via first-fit-decreasing placement over cloned idle capacity, so two
// victims cannot both claim the same free GPU. Destinations must be at least
// as full as the source (name-ordered on ties, so two equally-empty nodes
// consolidate in one deterministic direction instead of swapping pods), and
// are tried fullest-first. Returns nil unless every victim places; the
// replacements are not pinned — binpack scoring makes the fuller nodes the
// likely landing spots.
func simulateDrain(
	members []*api.NodeInfo,
	source *api.NodeInfo,
	victims []*api.TaskInfo,
	gpu v1.ResourceName,
	predicate func(*api.TaskInfo, *api.NodeInfo) error,
) []gpuFragmentationMove {
	sourceUsed := source.Used.Get(gpu)
	type candidate struct {
		node *api.NodeInfo
		idle *api.Resource
	}
	candidates := make([]candidate, 0, len(members))
	for _, dest := range members {
		if dest.Name == source.Name {
			continue
		}
		destUsed := dest.Used.Get(gpu)
		if destUsed < sourceUsed || (destUsed == sourceUsed && dest.Name < source.Name) {
			continue
		}
		candidates = append(candidates, candidate{node: dest, idle: dest.Idle.Clone()})
	}
	if len(candidates) == 0 {
		return nil
	}
	sort.Slice(candidates, func(i, j int) bool {
		ui, uj := candidates[i].node.Used.Get(gpu), candidates[j].node.Used.Get(gpu)
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

// stampGpuFragmentationMove durably records the move before eviction: the
// source node's cooldown clock and the PodGroup's eviction count. Failure to
// record aborts the move so budgets can never be overspent.
func stampGpuFragmentationMove(plan gpuFragmentationPlan) error {
	now := time.Now().UTC().Format(time.RFC3339)
	nodePatch := []byte(fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, lastEvictionAnnotation, now))
	if _, err := Session.KubeClient().CoreV1().Nodes().Patch(
		context.TODO(), plan.source, types.MergePatchType, nodePatch, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("stamp node %s: %w", plan.source, err)
	}
	job := Session.Jobs[plan.victim.Job]
	if job == nil || job.PodGroup == nil {
		return fmt.Errorf("podgroup for %s/%s not found", plan.victim.Namespace, plan.victim.Name)
	}
	pgPatch := []byte(fmt.Sprintf(`{"metadata":{"annotations":{%q:"1"}}}`, groupEvictionAnnotation))
	if _, err := Session.VCClient().SchedulingV1beta1().PodGroups(job.PodGroup.Namespace).Patch(
		context.TODO(), job.PodGroup.Name, types.MergePatchType, pgPatch, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("stamp podgroup %s/%s: %w", job.PodGroup.Namespace, job.PodGroup.Name, err)
	}
	return nil
}
