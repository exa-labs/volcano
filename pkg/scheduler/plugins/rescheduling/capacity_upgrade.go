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
	"volcano.sh/volcano/pkg/scheduler/plugins/capacitycost"
)

// CapacityUpgradeStrategy moves running GPU work from expensive capacity to
// cheaper capacity once the cheaper tier can hold it. Capacity tiers come
// from the same node-label rank as the capacitycost plugin (reserved < spot
// < on-demand by default); nodes without the label have no tier and are
// neither sources nor targets. A PodGroup is a candidate when at least one
// member runs above the cheapest tier, every member is at or below the
// victim priority ceiling, old enough, not opted out, its cooldown clock has
// expired and its move budget is not spent. Fit is proven per pass against a
// shared ledger of the target tier's idle capacity plus what evicting
// strictly lower-priority pods there would free, net of capacity already
// held by moves in flight.
//
// The strategy owns the whole move as a transaction (see
// capacity_upgrade_holds.go): it writes a hold for the mover onto each
// target node, stamps the mover's cooldown and budget, and evicts the
// lower-priority victims. Once the held GPUs are really idle it drains the
// mover's source nodes and evicts the mover; the successor the controller
// creates is the only pod allowed onto the held GPUs and cannot be pipelined
// back onto the capacity its predecessor is still releasing. Gangs (PodGroups
// with minMember > 1) move as a whole; a PodGroup of independent pods moves
// one pod per pass.
const CapacityUpgradeStrategy = "capacityUpgrade"

// DefaultCapacityUpgradeConf holds the default (dry-run) configuration.
var DefaultCapacityUpgradeConf = map[string]interface{}{
	"dryRun":            true,
	"gpuResource":       "nvidia.com/gpu",
	"nodeLabelKey":      capacitycost.DefaultNodeLabelKey,
	"order":             capacitycost.DefaultOrder,
	"unlabeledRank":     capacitycost.DefaultUnlabeledRank,
	"zoneLabel":         "topology.kubernetes.io/zone",
	"optOutLabel":       "exa.ai/capacity-upgrade-eligible",
	"protectedLabel":    "exa.ai/gang-protection",
	"identityLabels":    "exa-run-name,node-id;execution-id,node-id",
	"cooldownSeconds":   1800,
	"minPodAgeSeconds":  600,
	"holdTtlSeconds":    600,
	"maxVictims":        8,
	"maxGangMoves":      2,
	"maxMovesPerGroup":  2,
	"maxVictimPriority": -1,
}

// CapacityUpgradeKillSwitchEnv disables the strategy entirely when set to
// "true" on the scheduler process. Moves already in flight still finish or
// expire; only new moves stop.
const CapacityUpgradeKillSwitchEnv = "EXA_CAPACITY_UPGRADE_DISABLED"

// CapacityUpgradeLastAnnotation is the PodGroup's cooldown clock: the
// RFC3339 time of its last move by this strategy.
const CapacityUpgradeLastAnnotation = "exa.ai/capacity-upgrade-last"

type capacityUpgradeConf struct {
	// DryRun logs the moves a pass would start without starting them.
	DryRun      bool   `mapstructure:"dryRun"`
	GpuResource string `mapstructure:"gpuResource"`
	// NodeLabelKey, Order and UnlabeledRank define the capacity rank exactly
	// as in the capacitycost plugin; keep them identical in the scheduler
	// configuration so placement and migration agree on what is cheaper.
	NodeLabelKey  string `mapstructure:"nodeLabelKey"`
	Order         string `mapstructure:"order"`
	UnlabeledRank int    `mapstructure:"unlabeledRank"`
	// ZoneLabel groups target nodes so a gang is only placed inside a
	// single zone.
	ZoneLabel string `mapstructure:"zoneLabel"`
	// OptOutLabel excludes a pod (and thereby its whole PodGroup) when set
	// to "false".
	OptOutLabel string `mapstructure:"optOutLabel"`
	// ProtectedLabel marks pods whose karpenter.sh/do-not-disrupt
	// annotation is lifecycle protection against node disruption managed
	// by the gang's owner, not an opt-out from scheduler-driven moves.
	// Any other pod carrying do-not-disrupt is never moved or evicted.
	ProtectedLabel string `mapstructure:"protectedLabel"`
	// IdentityLabels identify a workload across restarts: a successor
	// carrying the mover's values may claim its hold. Alternatives are
	// separated by ";", each a comma-separated label set; a pod is identified
	// by the first set it carries in full, so a run-level name that survives
	// a relaunch under a new execution takes precedence over the execution
	// id, which only survives an in-place retry. Pods carrying none are
	// identified by their controller owner.
	IdentityLabels string `mapstructure:"identityLabels"`
	// CooldownSeconds holds a PodGroup after a move so a job that bounces
	// between tiers is not moved again immediately.
	CooldownSeconds int `mapstructure:"cooldownSeconds"`
	// MinPodAgeSeconds keeps freshly started pods in place: a pod younger
	// than this has done too little work to be worth restarting.
	MinPodAgeSeconds int `mapstructure:"minPodAgeSeconds"`
	// HoldTTLSeconds bounds each phase of a move; a move that does not
	// progress within it is released.
	HoldTTLSeconds int `mapstructure:"holdTtlSeconds"`
	// MaxVictims caps single-pod movers per pass.
	MaxVictims int `mapstructure:"maxVictims"`
	// MaxGangMoves caps gang moves started per pass.
	MaxGangMoves int `mapstructure:"maxGangMoves"`
	// MaxMovesPerGroup caps how often a workload (a PodGroup and the
	// PodGroups that succeed it) is restarted by this strategy.
	MaxMovesPerGroup int `mapstructure:"maxMovesPerGroup"`
	// MaxVictimPriority is the highest pod priority still movable; pods
	// without an explicit priority count as 0.
	MaxVictimPriority int32 `mapstructure:"maxVictimPriority"`
}

func newCapacityUpgradeConf() *capacityUpgradeConf {
	conf := &capacityUpgradeConf{}
	_ = mapstructure.Decode(DefaultCapacityUpgradeConf, conf)
	return conf
}

func (c *capacityUpgradeConf) parse(configs map[string]interface{}) {
	if len(configs) == 0 {
		return
	}
	if err := mapstructure.Decode(configs, c); err != nil {
		klog.Errorf("capacityUpgrade: bad strategy params, keeping defaults: %v", err)
		*c = *newCapacityUpgradeConf()
	}
}

func (c *capacityUpgradeConf) ranker() *capacitycost.Ranker {
	return capacitycost.NewRanker(c.NodeLabelKey, c.Order, c.UnlabeledRank)
}

func (c *capacityUpgradeConf) identityLabels() [][]string {
	return parseIdentitySets(c.IdentityLabels)
}

// loadCapacityUpgradeConf builds the strategy configuration from the
// registered parameters.
func loadCapacityUpgradeConf() *capacityUpgradeConf {
	conf := newCapacityUpgradeConf()
	if params, ok := RegisteredStrategyConfigs[CapacityUpgradeStrategy].(map[string]interface{}); ok {
		conf.parse(params)
	}
	return conf
}

// capacityUpgradePlan is one PodGroup's planned move.
type capacityUpgradePlan struct {
	job     *api.JobInfo
	members []*api.TaskInfo
	// gang is true when the members must restart together.
	gang bool
	// target and from are the capacity types the move goes to and from.
	target, from string
	zone         string
	// nodes maps each target node to the GPU count placed on it.
	nodes map[string]float64
	// gpus is the group's total GPU count.
	gpus float64
	// victims are the lower-priority pods on the target nodes the
	// placement relies on evicting.
	victims  []*api.TaskInfo
	priority int32
	identity map[string]string
	// moves is the mover's move count including this move.
	moves int
	at    time.Time
}

func (p capacityUpgradePlan) kind() string {
	if p.gang {
		return "gang"
	}
	return "pod"
}

func (p capacityUpgradePlan) nodeNames() []string {
	names := make([]string, 0, len(p.nodes))
	for name := range p.nodes {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// observe records the plan in the strategy's Prometheus counters.
func (p capacityUpgradePlan) observe(mode string) {
	capacityUpgradeMoves.WithLabelValues(p.target, p.kind(), mode).Inc()
	capacityUpgradePods.WithLabelValues(p.target, p.kind(), mode).Add(float64(len(p.members)))
	capacityUpgradeGpus.WithLabelValues(p.target, p.kind(), mode).Add(p.gpus)
}

// capacityUpgradePredicate evaluates a member against a target node; gang
// tells the caller whether peer affinity must be relaxed.
type capacityUpgradePredicate func(task *api.TaskInfo, node *api.NodeInfo, gang bool) error

// sessionPredicate is the planner's fit check: each member is probed as a
// fresh, unbound copy of itself through the session's PrePredicate and
// Predicate chains, with the probe marked so the hold predicates let it
// through (the ledger accounts for holds).
func sessionPredicate() capacityUpgradePredicate {
	probes := make(map[types.UID]*api.TaskInfo)
	preFailed := make(map[types.UID]error)
	return func(task *api.TaskInfo, node *api.NodeInfo, gang bool) error {
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
			if probe.Pod.Annotations == nil {
				probe.Pod.Annotations = map[string]string{}
			}
			probe.Pod.Annotations[capacityUpgradeProbeAnnotation] = "true"
			if gang {
				// Gang peers currently pin each other to the source zone
				// through pod affinity; the planner enforces a single
				// target zone itself, so the peer terms must not veto.
				if probe.Pod.Spec.Affinity != nil {
					probe.Pod.Spec.Affinity.PodAffinity = nil
					probe.Pod.Spec.Affinity.PodAntiAffinity = nil
				}
			}
			if err := Session.PrePredicateFn(probe); err != nil {
				preFailed[uid] = err
				return err
			}
			probes[uid] = probe
		}
		return Session.PredicateFn(probe, node)
	}
}

// sessionCapacityUpgrade is the session's single view of the moves in
// flight. The hold/drain predicate, the hold preference, move maintenance
// and planning all read and update the same index, so a hold released or
// written earlier in the session is honoured by everything that runs after.
var sessionCapacityUpgrade *capacityUpgradeIndex

// capacityUpgradeSessionIndex returns the session's index, decoding the
// node annotations on first use.
func capacityUpgradeSessionIndex() *capacityUpgradeIndex {
	if sessionCapacityUpgrade == nil {
		sessionCapacityUpgrade = indexCapacityUpgrade(Session.Nodes)
	}
	return sessionCapacityUpgrade
}

// victimsFnForCapacityUpgradeMoves advances the moves in flight; it runs in
// every session so a move progresses as soon as the cluster lets it.
var victimsFnForCapacityUpgradeMoves = func(tasks []*api.TaskInfo) []*api.TaskInfo {
	if Session == nil {
		return nil
	}
	conf := loadCapacityUpgradeConf()
	idx := capacityUpgradeSessionIndex()
	if len(idx.moves) == 0 && len(idx.malformed) == 0 && len(idx.drains) == 0 {
		return nil
	}
	steps := advanceCapacityUpgradeMoves(idx, Session.Nodes, Session.Jobs, conf, time.Now(), sessionPredicate())
	return applyMoveSteps(steps, sessionStore{})
}

// victimsFnForCapacityUpgrade plans and starts new moves; it runs on the
// rescheduling interval.
var victimsFnForCapacityUpgrade = func(tasks []*api.TaskInfo) []*api.TaskInfo {
	if Session == nil {
		return nil
	}
	if os.Getenv(CapacityUpgradeKillSwitchEnv) == "true" {
		klog.V(2).Infof("capacityUpgrade: disabled via %s", CapacityUpgradeKillSwitchEnv)
		return nil
	}
	conf := loadCapacityUpgradeConf()
	capacityUpgradePasses.Inc()

	running := make(map[types.UID]*api.TaskInfo, len(tasks))
	for _, task := range tasks {
		if task.Pod != nil {
			running[task.Pod.UID] = task
		}
	}

	idx := capacityUpgradeSessionIndex()
	plans := planCapacityUpgrades(Session.Nodes, Session.Jobs, running, conf, idx, time.Now(), sessionPredicate())

	victims := make([]*api.TaskInfo, 0)
	for _, plan := range plans {
		pg := plan.job.PodGroup
		if conf.DryRun {
			klog.V(2).Infof("capacityUpgrade[dry-run]: would move %s %s/%s (%d pods, %v GPUs) %s -> %s on %v (evicting %d)",
				plan.kind(), pg.Namespace, pg.Name, len(plan.members), plan.gpus,
				plan.from, plan.target, plan.nodeNames(), len(plan.victims))
			plan.observe("dry_run")
			continue
		}
		if err := startCapacityUpgradeMove(plan, idx, conf, sessionStore{}); err != nil {
			klog.Errorf("capacityUpgrade: not moving %s %s/%s: %v", plan.kind(), pg.Namespace, pg.Name, err)
			continue
		}
		klog.V(2).Infof("capacityUpgrade: holding %v for %s %s/%s (%d pods, %v GPUs) %s -> %s, evicting %d lower-priority pods",
			plan.nodeNames(), plan.kind(), pg.Namespace, pg.Name, len(plan.members), plan.gpus,
			plan.from, plan.target, len(plan.victims))
		for _, victim := range plan.victims {
			klog.V(2).Infof("capacityUpgrade: evicting victim %s/%s (priority %d) from %s for %s/%s",
				victim.Namespace, victim.Name, victim.Priority, victim.NodeName, pg.Namespace, pg.Name)
			capacityUpgradeEvictions.WithLabelValues(plan.kind(), "victim").Inc()
		}
		plan.observe("held")
		victims = append(victims, plan.victims...)
	}
	return victims
}

// startCapacityUpgradeMove makes the move durable: it writes a hold
// fragment onto every target node, then stamps the mover's cooldown and
// budget. Nothing is evicted unless every write succeeded; a write failure
// rolls back the fragments already written. A fragment that cannot be
// rolled back is a durable hold like any other, so maintenance carries it
// on (the hold itself records the move count the successor inherits) or
// expires it. The index is updated so later plans in the pass see the new
// holds.
func startCapacityUpgradeMove(plan capacityUpgradePlan, idx *capacityUpgradeIndex, conf *capacityUpgradeConf, store capacityUpgradeStore) error {
	pg := plan.job.PodGroup
	at := plan.at.UTC().Format(time.RFC3339)
	group := pg.Namespace + "/" + pg.Name
	id := moveID(group, plan.at)
	movers := make([]string, 0, len(plan.members))
	for _, member := range plan.members {
		movers = append(movers, string(member.Pod.UID))
	}
	victims := make([]string, 0, len(plan.victims))
	for _, victim := range plan.victims {
		victims = append(victims, string(victim.Pod.UID))
	}
	names := plan.nodeNames()
	until := plan.at.Add(time.Duration(conf.HoldTTLSeconds) * time.Second).UTC().Format(time.RFC3339)
	fragment := func(name string) capacityHold {
		return capacityHold{
			Move: id, Group: group, Nodes: names, Movers: movers, Victims: victims, Identity: plan.identity,
			Gpus: plan.nodes[name], Priority: plan.priority, Moves: plan.moves,
			Target: plan.target, From: plan.from, Until: until,
		}
	}
	written := make([]string, 0, len(names))
	rollback := func() {
		for _, done := range written {
			if rerr := store.writeNode(done, nodeState{holds: idx.holds[done], drains: idx.drains[done]}); rerr != nil {
				klog.Errorf("capacityUpgrade: roll back hold on %s for %s: %v", done, id, rerr)
			}
		}
	}
	for _, name := range names {
		holds := append(append([]capacityHold{}, idx.holds[name]...), fragment(name))
		if err := store.writeNode(name, nodeState{holds: holds, drains: idx.drains[name]}); err != nil {
			capacityUpgradeStampFailures.WithLabelValues("hold").Inc()
			rollback()
			return fmt.Errorf("hold %s on %s (at %s): %w", id, name, at, err)
		}
		written = append(written, name)
	}
	if err := stampCapacityUpgradeMover(plan, store); err != nil {
		capacityUpgradeStampFailures.WithLabelValues("mover").Inc()
		rollback()
		return fmt.Errorf("stamp mover for %s: %w", id, err)
	}
	for _, name := range names {
		idx.addHold(name, fragment(name))
	}
	return nil
}

// stampCapacityUpgradeMover records the cooldown clock and move count on the
// mover's PodGroup, and for a single pod spends the shared repack eviction
// cap so gpuFragmentation does not move the replacement again in the same
// window.
func stampCapacityUpgradeMover(plan capacityUpgradePlan, store capacityUpgradeStore) error {
	pg := plan.job.PodGroup
	return store.stampGroup(groupStamp{
		namespace: pg.Namespace, name: pg.Name,
		last: plan.at.UTC().Format(time.RFC3339), moves: plan.moves,
		repack: !plan.gang,
	})
}

// planCapacityUpgrades returns the moves a pass would start, highest-priority
// then oldest PodGroup first, until the per-pass budgets are spent. For each
// candidate PodGroup, target tiers are tried cheapest first and the first
// tier that fits the whole group (within one zone, for gangs) wins. Fit is
// simulated first-fit-decreasing over a ledger shared by every plan in the
// pass and seeded with the holds in flight, so two plans cannot claim the
// same capacity.
func planCapacityUpgrades(
	nodes map[string]*api.NodeInfo,
	jobs map[api.JobID]*api.JobInfo,
	running map[types.UID]*api.TaskInfo,
	conf *capacityUpgradeConf,
	idx *capacityUpgradeIndex,
	now time.Time,
	predicate capacityUpgradePredicate,
) []capacityUpgradePlan {
	gpu := v1.ResourceName(conf.GpuResource)
	ranker := conf.ranker()
	if ranker.MaxRank() <= 0 {
		return nil
	}

	// Target nodes per rank, restricted to labelled GPU nodes.
	byRank := make(map[int][]*api.NodeInfo)
	for _, node := range nodes {
		if node.Node == nil || node.Allocatable.Get(gpu) <= 0 {
			continue
		}
		if _, labelled := node.Node.Labels[conf.NodeLabelKey]; !labelled {
			continue
		}
		rank := ranker.Rank(node)
		byRank[rank] = append(byRank[rank], node)
	}
	for _, members := range byRank {
		sort.Slice(members, func(i, j int) bool { return members[i].Name < members[j].Name })
	}

	candidates := make([]capacityUpgradeCandidate, 0)
	for _, job := range jobs {
		cand, ok := capacityUpgradeCandidateFor(job, nodes, running, conf, ranker, gpu, idx, now)
		if ok {
			candidates = append(candidates, cand)
		}
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].priority != candidates[j].priority {
			return candidates[i].priority > candidates[j].priority
		}
		if !candidates[i].started.Equal(candidates[j].started) {
			return candidates[i].started.Before(candidates[j].started)
		}
		return candidates[i].job.UID < candidates[j].job.UID
	})

	ledger := newUpgradeLedger(nodes, running, idx, conf, gpu)
	plans := make([]capacityUpgradePlan, 0)
	evictions, gangs := 0, 0
	for _, cand := range candidates {
		if cand.gang && conf.MaxGangMoves > 0 && gangs >= conf.MaxGangMoves {
			continue
		}
		if !cand.gang && conf.MaxVictims > 0 && evictions+len(cand.members) > conf.MaxVictims {
			continue
		}
		var placement *capacityUpgradePlacement
		targetRank := 0
		for rank := 0; rank < cand.maxRank; rank++ {
			targets := byRank[rank]
			if len(targets) == 0 {
				continue
			}
			placement = simulateUpgrade(targets, cand, conf, gpu, ledger, predicate)
			if placement != nil {
				targetRank = rank
				break
			}
		}
		if placement == nil {
			continue
		}
		ledger.commit(cand.members, placement)
		plan := capacityUpgradePlan{
			job:      cand.job,
			members:  cand.members,
			gang:     cand.gang,
			target:   nodes[placement.nodeNames()[0]].Node.Labels[conf.NodeLabelKey],
			from:     cand.from,
			zone:     placement.zone,
			nodes:    placement.placed,
			gpus:     cand.gpus / gpuMilli,
			victims:  placement.preemptees,
			priority: cand.priority,
			identity: cand.identity,
			moves:    cand.moves + 1,
			at:       now,
		}
		klog.V(4).Infof("capacityUpgrade: %s/%s fits rank %d", cand.job.Namespace, cand.job.Name, targetRank)
		plans = append(plans, plan)
		if cand.gang {
			gangs++
		} else {
			evictions += len(cand.members)
		}
	}
	return plans
}

// capacityUpgradeCandidate is a PodGroup eligible for a move.
type capacityUpgradeCandidate struct {
	job     *api.JobInfo
	members []*api.TaskInfo
	gang    bool
	// priority is the group's highest member priority.
	priority int32
	// started is the group's oldest member start time.
	started time.Time
	// maxRank is the most expensive rank a member runs on; targets must
	// rank strictly below it.
	maxRank int
	// from is the capacity type label of that most expensive node.
	from string
	// gpus is the group's total GPU request in scheduler units.
	gpus float64
	// identity is what the successor will be recognised by.
	identity map[string]string
	// moves is the group's move count so far.
	moves int
}

// capacityUpgradeCandidateFor decides whether a job may move and gathers
// what the planner needs. Every GPU member must be running, controlled,
// old enough, at or below the priority ceiling, not opted out and not
// disruption-protected (unless the protection is the owner's lifecycle
// protection); at least one must run above the cheapest tier; the group's
// cooldown must have expired, its budget must remain, no move for it may be
// in flight and its members must share an identity. A gang (minMember > 1)
// moves whole; a PodGroup of independent pods moves its single most
// expensive, oldest member.
func capacityUpgradeCandidateFor(
	job *api.JobInfo,
	nodes map[string]*api.NodeInfo,
	running map[types.UID]*api.TaskInfo,
	conf *capacityUpgradeConf,
	ranker *capacitycost.Ranker,
	gpu v1.ResourceName,
	idx *capacityUpgradeIndex,
	now time.Time,
) (capacityUpgradeCandidate, bool) {
	cand := capacityUpgradeCandidate{job: job}
	if job.PodGroup == nil || len(job.Tasks) == 0 {
		return cand, false
	}
	if idx.groups[job.PodGroup.Namespace+"/"+job.PodGroup.Name] {
		return cand, false
	}
	if !podGroupCooled(job.PodGroup, conf, now) {
		return cand, false
	}
	cand.moves = podGroupMoves(job.PodGroup, conf)
	if conf.MaxMovesPerGroup > 0 && cand.moves >= conf.MaxMovesPerGroup {
		return cand, false
	}
	minAge := time.Duration(conf.MinPodAgeSeconds) * time.Second
	ranks := map[types.UID]int{}
	for _, task := range job.Tasks {
		if task.Pod == nil {
			return cand, false
		}
		if task.Resreq.Get(gpu) <= 0 && task.InitResreq.Get(gpu) <= 0 {
			// Non-GPU members (a launcher, a sidecar job) ride along with
			// the gang but neither gate nor size the move.
			continue
		}
		sessionTask, isRunning := running[task.Pod.UID]
		if !isRunning || sessionTask.Status != api.Running || idx.movers[task.Pod.UID] {
			return cand, false
		}
		if task.Pod.Labels[conf.OptOutLabel] == "false" {
			return cand, false
		}
		if task.Priority > conf.MaxVictimPriority {
			return cand, false
		}
		if metav1.GetControllerOf(task.Pod) == nil {
			return cand, false
		}
		if task.Pod.Annotations[doNotDisruptAnnotation] == "true" && task.Pod.Labels[conf.ProtectedLabel] != "true" {
			return cand, false
		}
		started := podStartTime(task.Pod)
		if started.IsZero() || now.Sub(started) < minAge {
			return cand, false
		}
		node, ok := nodes[task.NodeName]
		if !ok || node.Node == nil {
			return cand, false
		}
		rank := ranker.Rank(node)
		ranks[task.Pod.UID] = rank
		if rank > cand.maxRank {
			cand.maxRank = rank
			cand.from = node.Node.Labels[conf.NodeLabelKey]
		}
		if len(cand.members) == 0 {
			cand.priority = task.Priority
			cand.started = started
		} else {
			if task.Priority > cand.priority {
				cand.priority = task.Priority
			}
			if started.Before(cand.started) {
				cand.started = started
			}
		}
		cand.members = append(cand.members, sessionTask)
	}
	if len(cand.members) == 0 || cand.maxRank == 0 {
		return cand, false
	}
	sort.Slice(cand.members, func(i, j int) bool { return cand.members[i].Name < cand.members[j].Name })
	cand.gang = job.PodGroup.Spec.MinMember > 1
	if !cand.gang && len(cand.members) > 1 {
		// Independent pods sharing a PodGroup: move one at a time, the
		// oldest of those on the most expensive tier.
		var pick *api.TaskInfo
		for _, member := range cand.members {
			if ranks[member.Pod.UID] != cand.maxRank {
				continue
			}
			if pick == nil || podStartTime(member.Pod).Before(podStartTime(pick.Pod)) {
				pick = member
			}
		}
		cand.members = []*api.TaskInfo{pick}
		cand.priority = pick.Priority
		cand.started = podStartTime(pick.Pod)
	}
	for _, member := range cand.members {
		cand.gpus += taskGpu(member, gpu)
	}
	identity, ok := groupIdentity(cand.members, conf.identityLabels())
	if !ok {
		return cand, false
	}
	cand.identity = identity
	return cand, true
}

// podStartTime is the pod's start time, or zero when it has not started.
func podStartTime(pod *v1.Pod) time.Time {
	if pod.Status.StartTime != nil {
		return pod.Status.StartTime.Time
	}
	return time.Time{}
}

// podGroupCooled reports whether the PodGroup's cooldown clock permits a
// move. A missing clock is cooled; an unparseable or future one is not.
func podGroupCooled(pg *api.PodGroup, conf *capacityUpgradeConf, now time.Time) bool {
	raw, ok := pg.Annotations[CapacityUpgradeLastAnnotation]
	if !ok {
		return true
	}
	last, err := time.Parse(time.RFC3339, raw)
	if err != nil || last.After(now) {
		return false
	}
	return now.Sub(last) >= time.Duration(conf.CooldownSeconds)*time.Second
}

// upgradeLedger tracks, per target node, the capacity still claimable in
// this pass: idle resources net of holds in flight, plus the resources of
// lower-priority pods a plan may evict, net of what earlier plans already
// took.
type upgradeLedger struct {
	idle map[string]*api.Resource
	// preemptable holds each node's evictable running tasks not yet
	// consumed by a plan, so a later plan cannot count on evicting the
	// same pod twice.
	preemptable map[string][]*api.TaskInfo
	// taken marks the pods (movers and preemptees) already spoken for.
	taken map[types.UID]bool
}

// newUpgradeLedger seeds the ledger from the session. A running task is
// evictable when it occupies GPUs, has a controller to recreate it, is not
// disruption-protected, is not spoken for by a move in flight (as mover or
// victim) and is not a successor claiming a hold on its node. A node's idle
// GPUs net of its holds may be negative while a hold's victims are still
// releasing; a plan then has to evict that much more to place there.
//
// Preemptable tasks are the session-side tasks (from running), never the
// node-local clones in node.Tasks: Session.Evict flips the victim it is
// handed to Releasing before NodeInfo.UpdateTask reconciles it against the
// node's copy, so evicting the node copy itself makes RemoveTask subtract
// from an empty Releasing and panic.
func newUpgradeLedger(nodes map[string]*api.NodeInfo, running map[types.UID]*api.TaskInfo, idx *capacityUpgradeIndex, conf *capacityUpgradeConf, gpu v1.ResourceName) *upgradeLedger {
	ledger := &upgradeLedger{
		idle:        make(map[string]*api.Resource, len(nodes)),
		preemptable: make(map[string][]*api.TaskInfo, len(nodes)),
		taken:       make(map[types.UID]bool),
	}
	for uid := range idx.movers {
		ledger.taken[uid] = true
	}
	for uid := range idx.victims {
		ledger.taken[uid] = true
	}
	for _, node := range nodes {
		idle := node.Idle.Clone()
		if outstanding := idx.outstanding(node.Name); outstanding > 0 {
			idle.SetScalar(gpu, idle.Get(gpu)-outstanding)
		}
		ledger.idle[node.Name] = idle
		tasks := make([]*api.TaskInfo, 0, len(node.Tasks))
		for _, task := range node.Tasks {
			if task.Pod == nil || task.Status != api.Running || taskGpu(task, gpu) <= 0 {
				continue
			}
			if metav1.GetControllerOf(task.Pod) == nil || task.Pod.Annotations[doNotDisruptAnnotation] == "true" {
				continue
			}
			if idx.claimant(node.Name, task) {
				continue
			}
			sessionTask, isRunning := running[task.Pod.UID]
			if !isRunning || sessionTask.Status != api.Running {
				continue
			}
			tasks = append(tasks, sessionTask)
		}
		// Cheapest to preempt first: lowest priority, then smallest.
		sort.Slice(tasks, func(i, j int) bool {
			if tasks[i].Priority != tasks[j].Priority {
				return tasks[i].Priority < tasks[j].Priority
			}
			return tasks[i].Name < tasks[j].Name
		})
		ledger.preemptable[node.Name] = tasks
	}
	return ledger
}

// commit records a plan: the movers are spoken for (a mover already on a
// target node counts as freeable there, since it restarts too) and the
// target nodes lose the capacity the placement consumed.
func (l *upgradeLedger) commit(members []*api.TaskInfo, placement *capacityUpgradePlacement) {
	for _, member := range members {
		l.taken[member.Pod.UID] = true
	}
	for node, idle := range placement.idle {
		l.idle[node] = idle
	}
	for _, task := range placement.preemptees {
		l.taken[task.Pod.UID] = true
	}
}

// capacityUpgradePlacement is a proven placement of one group.
type capacityUpgradePlacement struct {
	// placed maps each target node to the GPU count placed on it.
	placed     map[string]float64
	zone       string
	preemptees []*api.TaskInfo
	// idle is the ledger's idle map for the touched nodes after placement.
	idle map[string]*api.Resource
}

func (p *capacityUpgradePlacement) nodeNames() []string {
	names := make([]string, 0, len(p.placed))
	for name := range p.placed {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// simulateUpgrade proves the whole group fits on the target nodes today,
// placing members largest-first onto the node with the most claimable
// capacity that passes predicates. Claimable capacity on a node is its
// ledger idle plus, in priority order, lower-priority running pods the
// plan would evict for a member of this group's priority (members of the
// group itself count as freeable, since they restart). For gangs, the
// target set is one zone at a time; the first zone that fits wins. Returns
// nil unless every member places.
func simulateUpgrade(
	targets []*api.NodeInfo,
	cand capacityUpgradeCandidate,
	conf *capacityUpgradeConf,
	gpu v1.ResourceName,
	ledger *upgradeLedger,
	predicate capacityUpgradePredicate,
) *capacityUpgradePlacement {
	zones := map[string][]*api.NodeInfo{}
	zoneNames := make([]string, 0)
	if cand.gang && conf.ZoneLabel != "" {
		for _, node := range targets {
			zone := node.Node.Labels[conf.ZoneLabel]
			if _, seen := zones[zone]; !seen {
				zoneNames = append(zoneNames, zone)
			}
			zones[zone] = append(zones[zone], node)
		}
		sort.Strings(zoneNames)
	} else {
		zones[""] = targets
		zoneNames = append(zoneNames, "")
	}

	ordered := make([]*api.TaskInfo, len(cand.members))
	copy(ordered, cand.members)
	sort.SliceStable(ordered, func(i, j int) bool {
		return taskGpu(ordered[i], gpu) > taskGpu(ordered[j], gpu)
	})
	movers := make(map[types.UID]bool, len(cand.members))
	for _, member := range cand.members {
		movers[member.Pod.UID] = true
	}

	for _, zone := range zoneNames {
		placement := simulateUpgradeInZone(zones[zone], ordered, movers, cand, gpu, ledger, predicate)
		if placement != nil {
			placement.zone = zone
			return placement
		}
	}
	return nil
}

// upgradeTarget is a target node's claimable capacity during one simulation.
type upgradeTarget struct {
	node *api.NodeInfo
	idle *api.Resource
	// queue holds this node's not-yet-claimed preemptable tasks, cheapest
	// first.
	queue      []*api.TaskInfo
	preemptees []*api.TaskInfo
	placed     float64
}

// claimable grows idle by evicting queued tasks until need fits or the queue
// is exhausted; only tasks strictly below the mover's priority (or movers
// themselves) are evictable. Returns whether need now fits.
func (t *upgradeTarget) claimable(need *api.Resource, priority int32, movers map[types.UID]bool) bool {
	for len(t.queue) > 0 {
		if need.LessEqual(t.idle, api.Zero) {
			return true
		}
		// The queue is priority-ascending, so once a non-mover at or above
		// the mover's priority is reached only movers further back remain
		// evictable.
		evictable := -1
		for i, task := range t.queue {
			if movers[task.Pod.UID] || task.Priority < priority {
				evictable = i
				break
			}
		}
		if evictable < 0 {
			return false
		}
		task := t.queue[evictable]
		t.queue = append(t.queue[:evictable:evictable], t.queue[evictable+1:]...)
		t.idle.Add(task.Resreq)
		if !movers[task.Pod.UID] {
			t.preemptees = append(t.preemptees, task)
		}
	}
	return need.LessEqual(t.idle, api.Zero)
}

func simulateUpgradeInZone(
	nodes []*api.NodeInfo,
	ordered []*api.TaskInfo,
	movers map[types.UID]bool,
	cand capacityUpgradeCandidate,
	gpu v1.ResourceName,
	ledger *upgradeLedger,
	predicate capacityUpgradePredicate,
) *capacityUpgradePlacement {
	if len(nodes) == 0 {
		return nil
	}
	targets := make([]*upgradeTarget, 0, len(nodes))
	for _, node := range nodes {
		queue := make([]*api.TaskInfo, 0)
		for _, task := range ledger.preemptable[node.Name] {
			if ledger.taken[task.Pod.UID] {
				continue
			}
			queue = append(queue, task)
		}
		targets = append(targets, &upgradeTarget{
			node:  node,
			idle:  ledger.idle[node.Name].Clone(),
			queue: queue,
		})
	}

	for _, member := range ordered {
		need := taskGpuNeed(member, gpu)
		// Prefer nodes with the most idle GPUs so a gang spreads over as
		// few evictions as possible; ties go to name for determinism.
		sort.SliceStable(targets, func(i, j int) bool {
			ii, ij := targets[i].idle.Get(gpu), targets[j].idle.Get(gpu)
			if ii != ij {
				return ii > ij
			}
			return targets[i].node.Name < targets[j].node.Name
		})
		placed := false
		for _, target := range targets {
			if !target.claimable(need, cand.priority, movers) {
				continue
			}
			if predicate != nil {
				if err := predicate(member, target.node, cand.gang); err != nil {
					continue
				}
			}
			target.idle.Sub(need)
			target.placed += need.Get(gpu) / gpuMilli
			placed = true
			break
		}
		if !placed {
			return nil
		}
	}

	placement := &capacityUpgradePlacement{placed: map[string]float64{}, idle: make(map[string]*api.Resource)}
	for _, target := range targets {
		if target.placed <= 0 {
			continue
		}
		placement.placed[target.node.Name] = target.placed
		placement.idle[target.node.Name] = target.idle
		placement.preemptees = append(placement.preemptees, target.preemptees...)
	}
	sort.Slice(placement.preemptees, func(i, j int) bool {
		return placement.preemptees[i].Name < placement.preemptees[j].Name
	})
	return placement
}
