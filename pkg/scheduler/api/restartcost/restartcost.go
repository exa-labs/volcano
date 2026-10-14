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

// Package restartcost estimates the accelerator work an eviction throws
// away, in GPU-seconds, from facts workloads publish as pod annotations.
//
// Declared at launch:
//
//	exa.ai/resume               checkpoint | stateless | scratch
//	exa.ai/startup-seconds      start to useful work (S)
//	exa.ai/termination-seconds  how long a graceful stop takes (D)
//
// Announced by the running pod:
//
//	exa.ai/productive-at, exa.ai/last-checkpoint      RFC 3339 UTC seconds
//	exa.ai/last-checkpoint-step, exa.ai/checkpoint-interval-seconds
//
// Every key is optional: absent or unparseable means unknown. The unit of
// cost is the gang: evicting one member of a PodGroup restarts all of it, so
// a victim is priced as its whole job.
//
// GPUSeconds is the pure cost function; testdata/vectors.json pins it for
// other implementations. ParsePod reads one pod, Gang aggregates members and
// Book prices victims for one scheduling action.
package restartcost

import (
	"errors"
	"math"
	"strconv"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
)

// Pod annotations a workload may publish.
const (
	AnnotationResume                    = "exa.ai/resume"
	AnnotationStartupSeconds            = "exa.ai/startup-seconds"
	AnnotationTerminationSeconds        = "exa.ai/termination-seconds"
	AnnotationProductiveAt              = "exa.ai/productive-at"
	AnnotationLastCheckpoint            = "exa.ai/last-checkpoint"
	AnnotationLastCheckpointStep        = "exa.ai/last-checkpoint-step"
	AnnotationCheckpointIntervalSeconds = "exa.ai/checkpoint-interval-seconds"
)

// Values of AnnotationResume, cheapest restart first.
const (
	ResumeStateless  = "stateless"
	ResumeCheckpoint = "checkpoint"
	ResumeScratch    = "scratch"
)

// Scheduler configuration keys shared by every consumer.
const (
	ModeKey               = "restartCost"
	StartupSecondsKey     = "restartCostStartupSeconds"
	TerminationSecondsKey = "restartCostTerminationSeconds"
	GateUnannouncedKey    = "gateUnannounced"
	// GPUResourceKey names the resource preempt and reclaim count as GPUs.
	GPUResourceKey = "restartCostGpuResource"
	// GPUResource is the resource restart cost counts for preempt and reclaim.
	GPUResource                  = v1.ResourceName("nvidia.com/gpu")
	defaultStartupSeconds        = 300
	defaultTerminationSeconds    = 30
	kubernetesDefaultGraceSecond = 30
	gpuMilli                     = 1000
)

// Mode selects how an action uses restart cost when choosing victims.
type Mode string

const (
	// ModeOff ignores restart cost.
	ModeOff Mode = "off"
	// ModeShadow chooses as ModeOff does and exports what ModeOn would save.
	ModeShadow Mode = "shadow"
	// ModeOn evicts the cheapest victims among equal priority.
	ModeOn Mode = "on"
)

// ModeOf reads the configured restartCost argument. An unquoted YAML on/off
// arrives as a bool, so true is ModeOn and false is ModeOff.
func ModeOf(raw interface{}) Mode {
	switch v := raw.(type) {
	case nil:
		return ModeOff
	case bool:
		if v {
			return ModeOn
		}
		return ModeOff
	case string:
		return ParseMode(v)
	}
	klog.Warningf("restartCost: unknown mode %v, using %q", raw, ModeOff)
	return ModeOff
}

// ParseMode reads a configured mode; anything unrecognised is ModeOff.
func ParseMode(raw string) Mode {
	switch Mode(raw) {
	case ModeOff, ModeShadow, ModeOn:
		return Mode(raw)
	case "":
		return ModeOff
	}
	klog.Warningf("restartCost: unknown mode %q, using %q", raw, ModeOff)
	return ModeOff
}

// Defaults stand in for declarations a workload did not make.
type Defaults struct {
	StartupSeconds     float64 `json:"startup_s" mapstructure:"restartCostStartupSeconds"`
	TerminationSeconds float64 `json:"termination_s" mapstructure:"restartCostTerminationSeconds"`
}

// DefaultDefaults returns S = 300 s and D = 30 s.
func DefaultDefaults() Defaults {
	return Defaults{StartupSeconds: defaultStartupSeconds, TerminationSeconds: defaultTerminationSeconds}
}

// Facts are what is known about a pod or a gang. Times are Unix seconds and
// a nil pointer is unknown.
type Facts struct {
	GPUs         float64 `json:"gpus"`
	StartedAt    float64 `json:"started_at"`
	GraceSeconds float64 `json:"grace_s"`
	// Resume is "" when undeclared.
	Resume             string   `json:"resume"`
	StartupSeconds     *float64 `json:"startup_s"`
	TerminationSeconds *float64 `json:"termination_s"`
	ProductiveAt       *float64 `json:"productive_at"`
	LastCheckpoint     *float64 `json:"last_checkpoint"`
	// Announced is true when any restart annotation parsed.
	Announced bool `json:"-"`
}

// GPUSeconds is the work lost by evicting the workload at now: the drain
// (D, capped by the grace period), plus redoing startup (S, measured when
// the workload announced when it became productive), plus the work since its
// last durable state. A workload still starting loses only the startup so far.
func GPUSeconds(f Facts, now float64, d Defaults) float64 {
	termination := d.TerminationSeconds
	if f.TerminationSeconds != nil {
		termination = *f.TerminationSeconds
	}
	drain := math.Min(termination, f.GraceSeconds)

	startup := d.StartupSeconds
	if f.StartupSeconds != nil {
		startup = *f.StartupSeconds
	}
	productive := f.StartedAt + startup
	if f.ProductiveAt != nil {
		startup = *f.ProductiveAt - f.StartedAt
		productive = *f.ProductiveAt
	}
	if now < productive {
		return f.GPUs * (drain + math.Max(now-f.StartedAt, 0))
	}

	resume := f.Resume
	if resume == "" {
		resume = ResumeScratch
		if f.LastCheckpoint != nil {
			resume = ResumeCheckpoint
		}
	}
	var lost float64
	switch {
	case resume == ResumeStateless:
		lost = 0
	case resume == ResumeCheckpoint && f.LastCheckpoint != nil:
		lost = now - math.Max(*f.LastCheckpoint, productive)
	default:
		lost = now - productive
	}
	return f.GPUs * (drain + startup + math.Max(lost, 0))
}

// ParsePod reads a pod's restart facts; GPUs is left to the caller.
// started_at is status.startTime, falling back to creation, and to now (age
// 0) when neither is set; the grace period defaults to Kubernetes' 30 s.
// Unparseable annotations, and a productive-at before the start, are logged
// at V(4) and treated as absent.
func ParsePod(pod *v1.Pod, now float64) Facts {
	f := Facts{GraceSeconds: kubernetesDefaultGraceSecond}
	if pod.Spec.TerminationGracePeriodSeconds != nil {
		f.GraceSeconds = float64(*pod.Spec.TerminationGracePeriodSeconds)
	}
	switch {
	case pod.Status.StartTime != nil:
		f.StartedAt = float64(pod.Status.StartTime.Unix())
	case !pod.CreationTimestamp.IsZero():
		f.StartedAt = float64(pod.CreationTimestamp.Unix())
	default:
		klog.V(4).Infof("restartCost: pod %s/%s has no start or creation time, pricing it as just started", pod.Namespace, pod.Name)
		f.StartedAt = now
	}

	p := parser{pod: pod, announced: &f.Announced}
	if raw, ok := pod.Annotations[AnnotationResume]; ok {
		switch raw {
		case ResumeStateless, ResumeCheckpoint, ResumeScratch:
			f.Resume = raw
			f.Announced = true
		default:
			p.garbage(AnnotationResume, raw)
		}
	}
	f.StartupSeconds = p.value(AnnotationStartupSeconds, parseSeconds)
	f.TerminationSeconds = p.value(AnnotationTerminationSeconds, parseSeconds)
	f.ProductiveAt = p.value(AnnotationProductiveAt, func(raw string) (float64, error) {
		// Productive before it started would make the measured startup,
		// and with it the price, negative.
		at, err := parseTime(raw)
		if err == nil && at < f.StartedAt {
			err = errBeforeStart
		}
		return at, err
	})
	f.LastCheckpoint = p.value(AnnotationLastCheckpoint, parseTime)
	p.value(AnnotationLastCheckpointStep, parseSeconds)
	p.value(AnnotationCheckpointIntervalSeconds, parseSeconds)
	return f
}

type parser struct {
	pod       *v1.Pod
	announced *bool
}

// value parses one annotation, nil when absent or garbage.
func (p parser) value(key string, parse func(string) (float64, error)) *float64 {
	raw, ok := p.pod.Annotations[key]
	if !ok {
		return nil
	}
	v, err := parse(raw)
	if err != nil {
		p.garbage(key, raw)
		return nil
	}
	*p.announced = true
	return &v
}

func (p parser) garbage(key, raw string) {
	klog.V(4).Infof("restartCost: ignoring unparseable annotation %s=%q on pod %s/%s", key, raw, p.pod.Namespace, p.pod.Name)
}

// parseSeconds accepts a plain non-negative decimal integer.
func parseSeconds(raw string) (float64, error) {
	v, err := strconv.ParseUint(raw, 10, 63)
	return float64(v), err
}

func parseTime(raw string) (float64, error) {
	t, err := time.Parse(time.RFC3339, raw)
	return float64(t.Unix()), err
}

var errBeforeStart = errors.New("before the pod started")

// Gang aggregates members into the facts of the whole: GPUs add up, the
// earliest start and productive time and the latest checkpoint win, and
// declarations take the most expensive value (largest S, D and grace;
// scratch over checkpoint over stateless). ok is false without members.
func Gang(tasks []*api.TaskInfo, gpu v1.ResourceName, now float64) (f Facts, ok bool) {
	for _, task := range tasks {
		if task.Pod == nil {
			continue
		}
		m := ParsePod(task.Pod, now)
		m.GPUs = task.Resreq.Get(gpu) / gpuMilli
		if !ok {
			f, ok = m, true
			continue
		}
		f.GPUs += m.GPUs
		f.StartedAt = math.Min(f.StartedAt, m.StartedAt)
		f.GraceSeconds = math.Max(f.GraceSeconds, m.GraceSeconds)
		f.Resume = costlierResume(f.Resume, m.Resume)
		f.StartupSeconds = pick(f.StartupSeconds, m.StartupSeconds, math.Max)
		f.TerminationSeconds = pick(f.TerminationSeconds, m.TerminationSeconds, math.Max)
		f.ProductiveAt = pick(f.ProductiveAt, m.ProductiveAt, math.Min)
		f.LastCheckpoint = pick(f.LastCheckpoint, m.LastCheckpoint, math.Max)
		f.Announced = f.Announced || m.Announced
	}
	return f, ok
}

func pick(a, b *float64, choose func(x, y float64) float64) *float64 {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	v := choose(*a, *b)
	return &v
}

var resumeRank = map[string]int{ResumeStateless: 1, ResumeCheckpoint: 2, ResumeScratch: 3}

func costlierResume(a, b string) string {
	if resumeRank[b] > resumeRank[a] {
		return b
	}
	return a
}

// Price is a job's restart cost and whether any member announced facts.
type Price struct {
	GPUSeconds float64
	Announced  bool
}

// Book prices victims by their gang for one scheduling action. A job is
// priced once, from its running members, the first time it is asked for; it
// is safe for concurrent use.
type Book struct {
	jobs     map[api.JobID]*api.JobInfo
	gpu      v1.ResourceName
	defaults Defaults
	now      float64

	mu     sync.Mutex
	prices map[api.JobID]Price
	// released are the members already Releasing when the book opened: their
	// jobs are being torn down already, so they add nothing.
	released map[api.TaskID]bool
}

// NewBook returns a book over the session's jobs.
func NewBook(jobs map[api.JobID]*api.JobInfo, gpu v1.ResourceName, defaults Defaults, now time.Time) *Book {
	released := map[api.TaskID]bool{}
	for _, job := range jobs {
		for uid := range job.TaskStatusIndex[api.Releasing] {
			released[uid] = true
		}
	}
	return &Book{jobs: jobs, gpu: gpu, defaults: defaults, now: float64(now.Unix()), prices: map[api.JobID]Price{}, released: released}
}

// Of prices facts aggregated over an arbitrary member set.
func (b *Book) Of(tasks []*api.TaskInfo) Price {
	f, ok := Gang(tasks, b.gpu, b.now)
	if !ok {
		return Price{}
	}
	return Price{GPUSeconds: GPUSeconds(f, b.now, b.defaults), Announced: f.Announced}
}

// Job prices the task's job from its running members; a task whose job is
// not in the session is priced alone. Members the action itself marks
// Releasing as it evicts them still count, so a job does not get cheaper
// mid-selection; members already Releasing when the book opened do not.
func (b *Book) Job(task *api.TaskInfo) Price {
	b.mu.Lock()
	defer b.mu.Unlock()
	if price, ok := b.prices[task.Job]; ok && task.Job != "" {
		return price
	}
	job, ok := b.jobs[task.Job]
	if !ok {
		return b.Of([]*api.TaskInfo{task})
	}
	running := make([]*api.TaskInfo, 0, len(job.Tasks))
	for _, member := range job.Tasks {
		if member.Status == api.Running || (member.Status == api.Releasing && !b.released[member.UID]) {
			running = append(running, member)
		}
	}
	price := b.Of(running)
	b.prices[task.Job] = price
	return price
}

// Cost is the GPU-seconds of the task's job; see Job.
func (b *Book) Cost(task *api.TaskInfo) float64 {
	return b.Job(task).GPUSeconds
}

// GangCosts prices each distinct job among the victims once, in first-seen
// order: once one member of a job is chosen the others add nothing.
func (b *Book) GangCosts(victims []*api.TaskInfo) []float64 {
	seen := map[api.JobID]bool{}
	costs := make([]float64, 0, len(victims))
	for _, victim := range victims {
		if victim.Job != "" && seen[victim.Job] {
			continue
		}
		seen[victim.Job] = true
		costs = append(costs, b.Cost(victim))
	}
	return costs
}

// Selection is the total cost of evicting the victims, each gang once.
func (b *Book) Selection(victims []*api.TaskInfo) float64 {
	total := 0.0
	for _, cost := range b.GangCosts(victims) {
		total += cost
	}
	return total
}

// Shadow compares the victims two orderings would evict to fit need next to
// idle: the cost of each, and whether they differ in cost (equal-cost
// choices are ties, not different decisions).
func (b *Book) Shadow(baseline, costAware []*api.TaskInfo, need, idle *api.Resource) (differs bool, baselineCost, costAwareCost float64) {
	baselineCost, costAwareCost = b.Selection(Cover(baseline, need, idle)), b.Selection(Cover(costAware, need, idle))
	return baselineCost != costAwareCost, baselineCost, costAwareCost
}

// Cover returns the shortest prefix of ordered whose resources, added to
// idle, fit need; all of ordered when nothing shorter does. It is how an
// action's eviction loop would consume an ordering, for shadow comparisons.
func Cover(ordered []*api.TaskInfo, need, idle *api.Resource) []*api.TaskInfo {
	free := idle.Clone()
	for i, task := range ordered {
		if need.LessEqual(free, api.Zero) {
			return ordered[:i]
		}
		free.Add(task.Resreq)
	}
	return ordered
}
