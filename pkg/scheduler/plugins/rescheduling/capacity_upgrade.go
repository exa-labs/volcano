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
// priority ceiling, old enough, not opted out, its cooldown clock has expired
// and its move budget is not spent. Fit is proven per pass against a shared
// ledger of the target tier's idle capacity, net of capacity already held by
// moves in flight. By default nothing running on a target is evicted to make
// room: a group moves only into GPUs that are idle already, so no move can
// displace another workload and set off a chain of moves. Idle GPUs that
// pending work outranking the mover could reclaim through preempt are not
// a target either (see capacity_upgrade_demand.go): the mover would only
// be evicted again once preempt takes the node.
//
// maxDisplacedPriority opens one exception for filler workloads: running
// pods on a target at or below that priority count as room next to the
// idle GPUs, and a move evicts the ones its placement relies on. The same
// pods are never movers, so a displaced pod that relands on costlier
// capacity stays there and the chain ends with it.
//
// The strategy owns the whole move as a transaction (see
// capacity_upgrade_holds.go): it writes a hold for the mover onto each
// target node and stamps the mover's cooldown and budget, then evicts the
// pods the hold displaces. Once the held GPUs are idle it drains the mover's
// source nodes and evicts the mover; the
// successor the controller creates is the only pod allowed onto the held
// GPUs and cannot be pipelined back onto the capacity its predecessor is
// still releasing. Gangs (PodGroups with minMember > 1) move as a whole; a
// PodGroup of independent pods moves one pod per pass.
//
// Three opt-in parameters bring the node autoscaler's published prices and
// spend caps into the strategy (see capacity_upgrade_price.go): priceAware
// compares priced sources and targets by price instead of rank and asks for
// a minimum saving, moveAdmission keeps a pod governed by a spend cap off
// nodes its cap does not accept, and overrunRelief moves capped work off
// nodes flagged as over their spend cap.
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
	"maxPodMoves":       8,
	"maxGangMoves":      2,
	"maxMovesPerGroup":  2,
	"maxVictimPriority": -1,

	// Published prices and spend caps, all off by default.
	"priceAware":            false,
	"minSavingPercent":      10,
	"moveAdmission":         false,
	"overrunRelief":         false,
	"priceStalenessSeconds": 900,
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
	// MaxPodMoves caps single-pod movers per pass.
	MaxPodMoves int `mapstructure:"maxPodMoves"`
	// MaxGangMoves caps gang moves started per pass.
	MaxGangMoves int `mapstructure:"maxGangMoves"`
	// MaxMovesPerGroup caps how often a workload (a PodGroup and the
	// PodGroups that succeed it) is restarted by this strategy.
	MaxMovesPerGroup int `mapstructure:"maxMovesPerGroup"`
	// MaxVictimPriority is the highest pod priority still movable; pods
	// without an explicit priority count as 0.
	MaxVictimPriority int32 `mapstructure:"maxVictimPriority"`
	// MaxDisplacedPriority, when set, lets a move make room for itself:
	// running GPU pods on a target node at or below this priority count as
	// claimable capacity next to the node's idle GPUs, and the move evicts
	// the ones its placement relies on. Pods at or below it are never
	// movers, so a displaced pod cannot become the next mover. Unset (the
	// default), a move only ever takes idle GPUs and restarts nothing but
	// the mover.
	MaxDisplacedPriority *int32 `mapstructure:"maxDisplacedPriority"`
	// PriceAware decides what is cheaper by the nodes' published prices
	// wherever a group's source nodes and a target are all priced: the
	// group moves only when the placement cuts its hourly cost by at least
	// MinSavingPercent. Unpriced sources or targets keep the rank rule.
	PriceAware bool `mapstructure:"priceAware"`
	// MinSavingPercent is the smallest cost reduction, in percent of the
	// group's current hourly cost, worth a price-based move. It is the
	// hysteresis that keeps price noise from moving work back and forth.
	MinSavingPercent float64 `mapstructure:"minSavingPercent"`
	// MoveAdmission refuses a target node to a mover governed by a spend
	// cap unless the node's published verdict admits that cap at the node's
	// current price; without a fresh verdict the node is refused.
	MoveAdmission bool `mapstructure:"moveAdmission"`
	// OverrunRelief makes PodGroups with capped members on a node flagged
	// as over its spend cap, while the autoscaler has not yet acted on it,
	// the first candidates of a pass, and lets them move to any idle
	// capacity that admits them, whatever its rank or price. Such a move
	// never evicts anyone to make room. Implies MoveAdmission.
	OverrunRelief bool `mapstructure:"overrunRelief"`
	// PriceStalenessSeconds is how long a published price (and the
	// admission verdict written with it) stays usable.
	PriceStalenessSeconds int `mapstructure:"priceStalenessSeconds"`
}

// displaces reports whether pods of the given priority are room to be made
// on a target node rather than workloads to move.
func (c *capacityUpgradeConf) displaces(priority int32) bool {
	return c.MaxDisplacedPriority != nil && priority <= *c.MaxDisplacedPriority
}

// shielded reports whether the strategy must leave the pod where it is: it
// opted out, has no controller to recreate it, or is disruption-protected
// by anything other than its owner's lifecycle protection.
func (c *capacityUpgradeConf) shielded(pod *v1.Pod) bool {
	if pod.Labels[c.OptOutLabel] == "false" || metav1.GetControllerOf(pod) == nil {
		return true
	}
	return pod.Annotations[doNotDisruptAnnotation] == "true" && pod.Labels[c.ProtectedLabel] != "true"
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
	// displaced maps each target node to the pods running there that the
	// placement relies on evicting.
	displaced map[string][]*api.TaskInfo
	priority  int32
	identity  map[string]string
	// moves is the mover's move count including this move.
	moves int
	at    time.Time
	// relief marks a move off a node that is over its spend cap.
	relief bool
}

// displacedCount is the number of pods the move evicts to make room.
func (p capacityUpgradePlan) displacedCount() int {
	total := 0
	for _, pods := range p.displaced {
		total += len(pods)
	}
	return total
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
	if p.relief {
		capacityUpgradeOverrunRelief.WithLabelValues(mode, p.kind()).Inc()
	}
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
// rescheduling interval. It evicts nothing itself: a started move is a hold
// on target GPUs, and victimsFnForCapacityUpgradeMoves evicts the pods the
// hold displaces and then, once the hold and its source drains are durable,
// the mover.
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
	predicate := sessionPredicate()
	plans := planCapacityUpgrades(Session.Nodes, Session.Jobs, running, conf, idx, time.Now(), predicate, sessionDemand(v1.ResourceName(conf.GpuResource), predicate))

	for _, plan := range plans {
		pg := plan.job.PodGroup
		if conf.DryRun {
			klog.V(2).Infof("capacityUpgrade[dry-run]: would move %s %s/%s (%d pods, %v GPUs) %s -> %s on %v, displacing %d pods",
				plan.kind(), pg.Namespace, pg.Name, len(plan.members), plan.gpus,
				plan.from, plan.target, plan.nodeNames(), plan.displacedCount())
			plan.observe("dry_run")
			continue
		}
		if err := startCapacityUpgradeMove(plan, idx, conf, sessionStore{}); err != nil {
			klog.Errorf("capacityUpgrade: not moving %s %s/%s: %v", plan.kind(), pg.Namespace, pg.Name, err)
			continue
		}
		klog.V(2).Infof("capacityUpgrade: holding %v for %s %s/%s (%d pods, %v GPUs) %s -> %s, displacing %d pods",
			plan.nodeNames(), plan.kind(), pg.Namespace, pg.Name, len(plan.members), plan.gpus,
			plan.from, plan.target, plan.displacedCount())
		plan.observe("held")
	}
	return nil
}

// startCapacityUpgradeMove makes the move durable: it writes a hold
// fragment onto every target node, naming the pods the move displaces
// there, then stamps the mover's cooldown and budget. A write failure rolls
// back the fragments already written; a
// fragment that cannot be rolled back is a durable hold like any other, so
// maintenance carries it on (the hold itself records the move count the
// successor inherits) or expires it. The index is updated so later plans in
// the pass see the new holds.
func startCapacityUpgradeMove(plan capacityUpgradePlan, idx *capacityUpgradeIndex, conf *capacityUpgradeConf, store capacityUpgradeStore) error {
	pg := plan.job.PodGroup
	at := plan.at.UTC().Format(time.RFC3339)
	group := pg.Namespace + "/" + pg.Name
	id := moveID(group, plan.at)
	movers := make([]string, 0, len(plan.members))
	for _, member := range plan.members {
		movers = append(movers, string(member.Pod.UID))
	}
	names := plan.nodeNames()
	until := plan.at.Add(time.Duration(conf.HoldTTLSeconds) * time.Second).UTC().Format(time.RFC3339)
	fragment := func(name string) capacityHold {
		var displaced []string
		for _, pod := range plan.displaced[name] {
			displaced = append(displaced, string(pod.Pod.UID))
		}
		return capacityHold{
			Move: id, Group: group, Nodes: names, Movers: movers, Displaced: displaced, Identity: plan.identity,
			Gpus: plan.nodes[name], Priority: plan.priority, Moves: plan.moves,
			Target: plan.target, From: plan.from, Until: until, Reason: plan.reason(),
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
// same capacity or count on displacing the same pod. Target nodes that
// pending demand would reclaim from the group are passed over (demand may
// be nil).
//
// With overrunRelief, groups on nodes over their spend cap come first and
// may take any idle capacity that admits them. With priceAware, a group
// whose source nodes are all priced is first offered the priced nodes that
// cost no more per GPU than its dearest source, cheapest first, and moves
// when the placement saves enough; the rank tiers then only offer it nodes
// without a price. With moveAdmission (or overrunRelief), no member is
// placed on a node its spend cap does not admit.
func planCapacityUpgrades(
	nodes map[string]*api.NodeInfo,
	jobs map[api.JobID]*api.JobInfo,
	running map[types.UID]*api.TaskInfo,
	conf *capacityUpgradeConf,
	idx *capacityUpgradeIndex,
	now time.Time,
	predicate capacityUpgradePredicate,
	demand pendingDemand,
) []capacityUpgradePlan {
	gpu := v1.ResourceName(conf.GpuResource)
	ranker := conf.ranker()
	if ranker.MaxRank() <= 0 && !conf.PriceAware && !conf.OverrunRelief {
		return nil
	}
	pricing := newUpgradePricing(nodes, conf, now, gpu)
	if conf.admitsMoves() {
		predicate = admittedForMove(predicate, pricing.book, true)
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
		cand, ok := capacityUpgradeCandidateFor(job, nodes, running, conf, ranker, gpu, idx, now, pricing)
		if ok {
			candidates = append(candidates, cand)
		}
	}
	pricing.observeOverrun(nodes)
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].relief != candidates[j].relief {
			return candidates[i].relief
		}
		if candidates[i].priority != candidates[j].priority {
			return candidates[i].priority > candidates[j].priority
		}
		if !candidates[i].started.Equal(candidates[j].started) {
			return candidates[i].started.Before(candidates[j].started)
		}
		return candidates[i].job.UID < candidates[j].job.UID
	})

	ledger := newUpgradeLedger(nodes, running, conf, idx, gpu)
	plans := make([]capacityUpgradePlan, 0)
	pods, gangs := 0, 0
	for _, cand := range candidates {
		if cand.gang && conf.MaxGangMoves > 0 && gangs >= conf.MaxGangMoves {
			continue
		}
		if !cand.gang && conf.MaxPodMoves > 0 && pods+len(cand.members) > conf.MaxPodMoves {
			continue
		}
		var placement *capacityUpgradePlacement
		fits := ""
		guard := newDemandGuard(demand, cand)
		relief := false
		if cand.relief {
			placement = pricing.relieve(cand, ledger, predicate, guard)
			relief, fits = placement != nil, "admitted idle capacity"
			if !relief {
				capacityUpgradeOverrunRelief.WithLabelValues("no_capacity", cand.kind()).Inc()
				klog.V(3).Infof("capacityUpgrade: %s/%s is on a node over its spend cap, but no idle capacity admits it",
					cand.job.Namespace, cand.job.Name)
			}
		}
		if placement == nil && cand.priced {
			placement, fits = pricing.cheaper(cand, ledger, predicate, guard), "cheaper priced nodes"
		}
		for rank := 0; placement == nil && rank < cand.maxRank; rank++ {
			targets := byRank[rank]
			if cand.priced {
				// Priced targets were judged on price above.
				targets = pricing.unpriced(targets)
			}
			if len(targets) == 0 {
				continue
			}
			placement = simulateUpgrade(targets, cand, conf, gpu, ledger, predicate, guard)
			fits = fmt.Sprintf("rank %d", rank)
		}
		if placement == nil {
			if guard != nil && guard.skipped > 0 {
				klog.V(3).Infof("capacityUpgrade: %s/%s not moved: %d target node(s) spoken for by pending work",
					cand.job.Namespace, cand.job.Name, guard.skipped)
			}
			continue
		}
		ledger.commit(placement)
		plan := capacityUpgradePlan{
			job:       cand.job,
			members:   cand.members,
			gang:      cand.gang,
			target:    nodes[placement.nodeNames()[0]].Node.Labels[conf.NodeLabelKey],
			from:      cand.from,
			zone:      placement.zone,
			nodes:     placement.placed,
			gpus:      cand.gpus / gpuMilli,
			displaced: placement.displaced,
			priority:  cand.priority,
			identity:  cand.identity,
			moves:     cand.moves + 1,
			at:        now,
			relief:    relief,
		}
		klog.V(4).Infof("capacityUpgrade: %s/%s fits %s", cand.job.Namespace, cand.job.Name, fits)
		plans = append(plans, plan)
		if cand.gang {
			gangs++
		} else {
			pods += len(cand.members)
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
	// priced is true when prices decide what is cheaper for this group:
	// priceAware is on and every member's node has a fresh price.
	priced bool
	// cost is the group's hourly cost on its current nodes and maxUnit the
	// highest price per GPU among them; both are set when priced.
	cost, maxUnit float64
	// relief is true when a member governed by a spend cap runs on a node
	// over that cap that the autoscaler has not acted on yet.
	relief bool
	// idleOnly keeps the placement to idle GPUs: nothing is displaced.
	idleOnly bool
}

func (c capacityUpgradeCandidate) kind() string {
	if c.gang {
		return "gang"
	}
	return "pod"
}

// capacityUpgradeCandidateFor decides whether a job may move and gathers
// what the planner needs. Every GPU member must be running, controlled,
// old enough, at or below the priority ceiling and above the displaced
// one (a pod moves have to make room over is never moved itself), not opted
// out and not disruption-protected (unless the protection is the owner's
// lifecycle protection); at least one must run above the cheapest tier (or,
// with priceAware, every member on a priced node; or, with overrunRelief,
// a capped member on a node over its spend cap); the
// group's
// cooldown must have expired, its budget must remain, no move for it may be
// in flight and its members must share an identity. A gang (minMember > 1)
// moves whole; a PodGroup of independent pods moves its single most
// expensive, oldest member (first one in need of relief, if any).
func capacityUpgradeCandidateFor(
	job *api.JobInfo,
	nodes map[string]*api.NodeInfo,
	running map[types.UID]*api.TaskInfo,
	conf *capacityUpgradeConf,
	ranker *capacitycost.Ranker,
	gpu v1.ResourceName,
	idx *capacityUpgradeIndex,
	now time.Time,
	pricing *upgradePricing,
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
		if task.Priority > conf.MaxVictimPriority || conf.displaces(task.Priority) {
			return cand, false
		}
		if conf.shielded(task.Pod) {
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
	if len(cand.members) == 0 {
		return cand, false
	}
	sort.Slice(cand.members, func(i, j int) bool { return cand.members[i].Name < cand.members[j].Name })
	cand.gang = job.PodGroup.Spec.MinMember > 1
	if !cand.gang && len(cand.members) > 1 {
		// Independent pods sharing a PodGroup: move one at a time, the
		// oldest of those on the most expensive tier, unless relief or
		// prices single out another.
		pick, singled := pricing.single(cand.members)
		if singled {
			cand.maxRank = ranks[pick.Pod.UID]
			cand.from = nodes[pick.NodeName].Node.Labels[conf.NodeLabelKey]
		}
		for _, member := range cand.members {
			if singled || ranks[member.Pod.UID] != cand.maxRank {
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
	pricing.measure(&cand)
	if cand.maxRank == 0 && !cand.priced && !cand.relief {
		return cand, false
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

// upgradeLedger tracks, per target node, the GPU capacity still claimable in
// this pass: idle resources net of holds in flight and of what earlier plans
// in the pass already took, plus the displaceable pods no plan relies on
// yet. No other running pod is part of it.
type upgradeLedger struct {
	idle map[string]*api.Resource
	// displaceable lists each node's pods a plan may evict to make room,
	// lowest priority then smallest first; empty unless the displaced
	// priority ceiling is set.
	displaceable map[string][]*api.TaskInfo
	// taken marks the displaceable pods an earlier plan in the pass relies
	// on evicting, so two plans cannot count on the same pod.
	taken map[types.UID]bool
}

// newUpgradeLedger seeds the ledger from the session: each node's idle
// resources net of the GPUs held by moves in flight, and its displaceable
// pods.
func newUpgradeLedger(nodes map[string]*api.NodeInfo, running map[types.UID]*api.TaskInfo, conf *capacityUpgradeConf, idx *capacityUpgradeIndex, gpu v1.ResourceName) *upgradeLedger {
	ledger := &upgradeLedger{
		idle:         make(map[string]*api.Resource, len(nodes)),
		displaceable: make(map[string][]*api.TaskInfo),
		taken:        make(map[types.UID]bool),
	}
	for _, node := range nodes {
		idle := node.Idle.Clone()
		if outstanding := idx.outstanding(node.Name); outstanding > 0 {
			idle.SetScalar(gpu, max(idle.Get(gpu)-outstanding, 0))
		}
		ledger.idle[node.Name] = idle
		if pods := displaceablePods(node, running, conf, idx, gpu); len(pods) > 0 {
			ledger.displaceable[node.Name] = pods
		}
	}
	return ledger
}

// displaceablePods returns the pods on node a plan may evict to make room,
// lowest priority first and smallest first within a priority: running GPU
// pods at or below the displaced priority ceiling that the strategy is not
// shielded from and that no move in flight counts on already (as a mover, as
// a successor on its held GPUs, or as a pod being displaced, whose GPUs are
// promised to that move's hold). The tasks returned are the session's (from
// running), never the node-local clones: Session.Evict reconciles the task
// it is handed against the node's copy, so handing it the copy itself
// corrupts the node's accounting.
func displaceablePods(node *api.NodeInfo, running map[types.UID]*api.TaskInfo, conf *capacityUpgradeConf, idx *capacityUpgradeIndex, gpu v1.ResourceName) []*api.TaskInfo {
	if conf.MaxDisplacedPriority == nil {
		return nil
	}
	pods := make([]*api.TaskInfo, 0)
	for _, task := range node.Tasks {
		if task.Pod == nil || !conf.displaces(task.Priority) || taskGpu(task, gpu) <= 0 || conf.shielded(task.Pod) {
			continue
		}
		uid := task.Pod.UID
		if idx.movers[uid] || idx.displaced[uid] || idx.claimant(node.Name, task) {
			continue
		}
		sessionTask, isRunning := running[uid]
		if !isRunning || sessionTask.Status != api.Running {
			continue
		}
		pods = append(pods, sessionTask)
	}
	sort.Slice(pods, func(i, j int) bool {
		if pods[i].Priority != pods[j].Priority {
			return pods[i].Priority < pods[j].Priority
		}
		if gi, gj := taskGpu(pods[i], gpu), taskGpu(pods[j], gpu); gi != gj {
			return gi < gj
		}
		return pods[i].Name < pods[j].Name
	})
	return pods
}

// commit records a plan: the target nodes lose the capacity the placement
// consumed and the pods it displaces are spoken for.
func (l *upgradeLedger) commit(placement *capacityUpgradePlacement) {
	for node, idle := range placement.idle {
		l.idle[node] = idle
	}
	for _, pods := range placement.displaced {
		for _, pod := range pods {
			l.taken[pod.Pod.UID] = true
		}
	}
}

// capacityUpgradePlacement is a proven placement of one group.
type capacityUpgradePlacement struct {
	// placed maps each target node to the GPU count placed on it.
	placed map[string]float64
	zone   string
	// idle is the ledger's idle map for the touched nodes after placement.
	idle map[string]*api.Resource
	// displaced maps each target node to the pods the placement relies on
	// evicting there.
	displaced map[string][]*api.TaskInfo
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
// placing members largest-first onto the node with the most idle capacity
// that passes predicates. Idle capacity on a node is its ledger idle plus the
// resources of the group's own members already running there (they restart,
// so their GPUs come free). A member that fits no node's idle capacity is
// placed where displacing the node's displaceable pods makes it fit; no
// other running pod is counted. Nodes the guard reports spoken for are
// skipped. For gangs, the target set is one zone at a time; the first zone
// that fits wins. Returns nil unless every member places.
func simulateUpgrade(
	targets []*api.NodeInfo,
	cand capacityUpgradeCandidate,
	conf *capacityUpgradeConf,
	gpu v1.ResourceName,
	ledger *upgradeLedger,
	predicate capacityUpgradePredicate,
	guard *demandGuard,
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

	for _, zone := range zoneNames {
		placement := simulateUpgradeInZone(zones[zone], ordered, cand, gpu, ledger, predicate, guard)
		if placement != nil {
			placement.zone = zone
			return placement
		}
	}
	return nil
}

// upgradeTarget is a target node's claimable capacity during one
// simulation.
type upgradeTarget struct {
	node *api.NodeInfo
	// idle is what this group may claim without evicting anyone further:
	// the ledger's idle plus the GPUs of its own members running here and
	// of the pods it already displaces.
	idle *api.Resource
	// queue holds the node's displaceable pods nothing relies on yet, in
	// the ledger's order.
	queue []*api.TaskInfo
	// displaced are the pods this group's placement relies on evicting.
	displaced []*api.TaskInfo
	// ledger is the ledger's idle for the node, which only this group's
	// placements reduce; later plans in the pass never count on GPUs the
	// mover has yet to release.
	ledger *api.Resource
	placed float64
}

// subClamped subtracts rr from r dimension by dimension, flooring at zero:
// a placement onto a group's own releasing GPUs leaves the node no idle
// capacity for later plans, never a negative amount.
func subClamped(r, rr *api.Resource) {
	r.MilliCPU = max(r.MilliCPU-rr.MilliCPU, 0)
	r.Memory = max(r.Memory-rr.Memory, 0)
	for name, quantity := range rr.ScalarResources {
		r.SetScalar(name, max(r.Get(name)-quantity, 0))
	}
}

// makeRoom returns the queued pods to displace so that need fits the
// target, and the queue left without them; ok is false when need does not
// fit even with every queued pod gone. Pods go lowest priority first and,
// within a priority, the smallest one that covers the GPU shortfall on its
// own, else the largest, so as few pods and GPUs as possible are disturbed.
func (t *upgradeTarget) makeRoom(need *api.Resource, gpu v1.ResourceName) (displaced, rest []*api.TaskInfo, ok bool) {
	idle := t.idle.Clone()
	rest = append(rest, t.queue...)
	for !need.LessEqual(idle, api.Zero) {
		if len(rest) == 0 {
			return nil, nil, false
		}
		shortfall := need.Get(gpu) - idle.Get(gpu)
		pick := 0
		for i, pod := range rest {
			if pod.Priority != rest[0].Priority {
				break
			}
			pick = i
			if taskGpu(pod, gpu) >= shortfall {
				break
			}
		}
		idle.Add(rest[pick].Resreq)
		displaced = append(displaced, rest[pick])
		rest = append(rest[:pick], rest[pick+1:]...)
	}
	return displaced, rest, true
}

func simulateUpgradeInZone(
	nodes []*api.NodeInfo,
	ordered []*api.TaskInfo,
	cand capacityUpgradeCandidate,
	gpu v1.ResourceName,
	ledger *upgradeLedger,
	predicate capacityUpgradePredicate,
	guard *demandGuard,
) *capacityUpgradePlacement {
	if len(nodes) == 0 {
		return nil
	}
	targets := make([]*upgradeTarget, 0, len(nodes))
	for _, node := range nodes {
		if guard.spokenFor(node) {
			continue
		}
		idle := ledger.idle[node.Name].Clone()
		for _, member := range cand.members {
			if member.NodeName == node.Name {
				idle.Add(member.Resreq)
			}
		}
		queue := make([]*api.TaskInfo, 0, len(ledger.displaceable[node.Name]))
		for _, pod := range ledger.displaceable[node.Name] {
			if !cand.idleOnly && !ledger.taken[pod.Pod.UID] {
				queue = append(queue, pod)
			}
		}
		targets = append(targets, &upgradeTarget{node: node, idle: idle, queue: queue, ledger: ledger.idle[node.Name].Clone()})
	}

	for _, member := range ordered {
		need := taskGpuNeed(member, gpu)
		// Prefer nodes with the most idle GPUs so a gang spreads over as
		// few nodes as possible; ties go to name for determinism.
		sort.SliceStable(targets, func(i, j int) bool {
			ii, ij := targets[i].idle.Get(gpu), targets[j].idle.Get(gpu)
			if ii != ij {
				return ii > ij
			}
			return targets[i].node.Name < targets[j].node.Name
		})
		// place puts the member on the first target that takes it: within
		// idle capacity alone, or (displace) only where pods have to be
		// displaced for it. Idle capacity anywhere is tried first, so
		// nothing is displaced that did not have to be.
		place := func(displace bool) bool {
			for _, target := range targets {
				fits := need.LessEqual(target.idle, api.Zero)
				if fits == displace {
					continue
				}
				displaced, rest := []*api.TaskInfo(nil), target.queue
				if displace {
					var ok bool
					if displaced, rest, ok = target.makeRoom(need, gpu); !ok {
						continue
					}
				}
				if predicate != nil {
					if err := predicate(member, target.node, cand.gang); err != nil {
						continue
					}
				}
				for _, pod := range displaced {
					target.idle.Add(pod.Resreq)
				}
				target.queue = rest
				target.displaced = append(target.displaced, displaced...)
				target.idle.Sub(need)
				subClamped(target.ledger, need)
				target.placed += need.Get(gpu) / gpuMilli
				return true
			}
			return false
		}
		if !place(false) && !place(true) {
			return nil
		}
	}

	placement := &capacityUpgradePlacement{
		placed:    map[string]float64{},
		idle:      make(map[string]*api.Resource),
		displaced: map[string][]*api.TaskInfo{},
	}
	for _, target := range targets {
		if target.placed <= 0 {
			continue
		}
		placement.placed[target.node.Name] = target.placed
		placement.idle[target.node.Name] = target.ledger
		if len(target.displaced) > 0 {
			placement.displaced[target.node.Name] = target.displaced
		}
	}
	return placement
}
