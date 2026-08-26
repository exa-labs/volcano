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

// GpuFragmentationStrategy evicts at most one eligible GPU pod per node pool
// whose departure empties its node of GPU work and which provably fits on a
// fuller node in the same pool. The replacement is recreated by the pod's
// controller and scheduled normally; binpack scoring steers it to the fuller
// node. Cooldown and per-PodGroup eviction caps are the anti-thrash mechanism.
const GpuFragmentationStrategy = "gpuFragmentation"

// DefaultGpuFragmentationConf holds the default (dry-run) configuration.
var DefaultGpuFragmentationConf = map[string]interface{}{
	"dryRun":            true,
	"gpuResource":       "nvidia.com/gpu",
	"poolLabel":         "karpenter.sh/nodepool",
	"optOutLabel":       "exa.ai/repack-eligible",
	"cooldownSeconds":   1800,
	"maxVictims":        1,
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
	MaxVictims      int    `mapstructure:"maxVictims"`
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

// planGpuFragmentationMoves selects at most conf.MaxVictims moves, one per
// pool. A move requires: the source node's entire GPU usage is the single
// eligible task, the task's PodGroup has exactly one member, the node's
// cooldown clock permits it, its PodGroup eviction cap is unspent, and some
// strictly fuller node in the same pool passes resources and predicates.
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
		sort.Slice(members, func(i, j int) bool { return members[i].Name < members[j].Name })
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
			victim := movableSoleGpuTask(source, jobs, running, conf, gpu)
			if victim == nil {
				continue
			}
			gain := source.Used.Get(gpu)
			dest := findDestination(members, source, victim, gpu, predicate)
			if dest == nil {
				continue
			}
			plans = append(plans, gpuFragmentationPlan{
				victim:      victim,
				pool:        pool,
				source:      source.Name,
				destination: dest.Name,
				gain:        gain,
			})
			break
		}
	}
	return plans
}

// movableSoleGpuTask returns the node's single GPU-consuming task iff that
// task is safe to move: it is running, not opted out, not protected, at or below
// the movable priority ceiling, owned by a controller that will recreate it,
// and its PodGroup has exactly one member.
func movableSoleGpuTask(
	node *api.NodeInfo,
	jobs map[api.JobID]*api.JobInfo,
	running map[types.UID]*api.TaskInfo,
	conf *gpuFragmentationConf,
	gpu v1.ResourceName,
) *api.TaskInfo {
	used := node.Used.Get(gpu)
	if used <= 0 || used >= node.Allocatable.Get(gpu) {
		return nil
	}
	var sole *api.TaskInfo
	for _, task := range node.Tasks {
		if task.Resreq.Get(gpu) <= 0 && task.InitResreq.Get(gpu) <= 0 {
			continue
		}
		if sole != nil {
			return nil
		}
		sole = task
	}
	if sole == nil || sole.Pod == nil {
		return nil
	}
	// node.Tasks holds node-local clones; the eviction path mutates the
	// victim's status in place, so the session-side task must be returned
	// or node resource accounting corrupts and the scheduler panics.
	sessionTask, isRunning := running[sole.Pod.UID]
	if !isRunning {
		return nil
	}
	if sole.Pod.Labels[conf.OptOutLabel] == "false" {
		return nil
	}
	if sole.Pod.Annotations[doNotDisruptAnnotation] == "true" {
		return nil
	}
	priority := int32(0)
	if sole.Pod.Spec.Priority != nil {
		priority = *sole.Pod.Spec.Priority
	}
	if priority > conf.MaxVictimPriority {
		return nil
	}
	if metav1.GetControllerOf(sole.Pod) == nil {
		return nil
	}
	if sole.Job == "" {
		return nil
	}
	job, ok := jobs[sole.Job]
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

// findDestination proves at least one strictly fuller node in the pool can
// host the task today. The replacement is not pinned there; binpack scoring
// makes the fuller node the likely landing spot.
func findDestination(
	members []*api.NodeInfo,
	source *api.NodeInfo,
	victim *api.TaskInfo,
	gpu v1.ResourceName,
	predicate func(*api.TaskInfo, *api.NodeInfo) error,
) *api.NodeInfo {
	need := victim.InitResreq
	if victim.Resreq.Get(gpu) > need.Get(gpu) {
		need = victim.Resreq
	}
	for _, dest := range members {
		if dest.Name == source.Name {
			continue
		}
		if dest.Used.Get(gpu) <= source.Used.Get(gpu) {
			continue
		}
		if !need.LessEqual(dest.Idle, api.Zero) {
			continue
		}
		if predicate != nil {
			if err := predicate(victim, dest); err != nil {
				continue
			}
		}
		return dest
	}
	return nil
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
