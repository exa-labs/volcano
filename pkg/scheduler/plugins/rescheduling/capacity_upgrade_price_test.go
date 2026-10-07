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

// Tests for capacityUpgrade's use of published prices and spend caps:
// priceAware (what is cheaper, and by how much it has to be), moveAdmission
// (where a capped mover may go, at planning and again before eviction) and
// overrunRelief (moving capped work off nodes over their spend cap). With
// none of the three set, every other capacityUpgrade test file applies
// unchanged.

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	v1 "k8s.io/api/core/v1"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/plugins/capacitycost"
)

// pricedNode builds an 8-GPU node whose hourly price was confirmed a minute
// before testNow; price 8 is one per GPU-hour.
func pricedNode(name, capacityType, zone, price string) *api.NodeInfo {
	return observedNode(name, capacityType, zone, price, time.Minute)
}

// observedNode is pricedNode with the price confirmed age before testNow.
func observedNode(name, capacityType, zone, price string, age time.Duration) *api.NodeInfo {
	node := tierNode(name, capacityType, zone)
	node.Node.Annotations = map[string]string{
		capacitycost.OfferingPriceAnnotation:           price,
		capacitycost.OfferingPriceObservedAtAnnotation: testNow.Add(-age).Format(time.RFC3339),
	}
	return node
}

// caps records which spend-cap policies apply to the node and which of
// them admit it at its current price.
func caps(node *api.NodeInfo, applied, admitted string) *api.NodeInfo {
	node.Node.Annotations[capacitycost.SpendCapsAppliedAnnotation] = applied
	node.Node.Annotations[capacitycost.SpendCapsAdmittedAnnotation] = admitted
	return node
}

// overCap flags the node as over its spend cap since five minutes ago, the
// autoscaler acting on it window after testNow.
func overCap(node *api.NodeInfo, window time.Duration) *api.NodeInfo {
	node.Node.Annotations[capacitycost.SpendCapExceededAnnotation] = testNow.Add(-5 * time.Minute).Format(time.RFC3339)
	node.Node.Annotations[capacitycost.SpendCapActionAfterAnnotation] = testNow.Add(window).Format(time.RFC3339)
	return node
}

// capped puts the pod under a spend-cap policy.
func capped(pod *v1.Pod, policy string) *v1.Pod {
	pod.Annotations[capacitycost.PodSpendCapPolicyAnnotation] = policy
	return pod
}

// pricingConf is the default configuration with the given parameters set.
func pricingConf(set func(*capacityUpgradeConf)) *capacityUpgradeConf {
	conf := liveConf()
	set(conf)
	return conf
}

func priceAwareConf() *capacityUpgradeConf {
	return pricingConf(func(c *capacityUpgradeConf) { c.PriceAware = true })
}

func admissionConf() *capacityUpgradeConf {
	return pricingConf(func(c *capacityUpgradeConf) { c.MoveAdmission = true })
}

func reliefConf() *capacityUpgradeConf {
	return pricingConf(func(c *capacityUpgradeConf) { c.OverrunRelief = true })
}

// counted returns how much the counter grew while fn ran.
func counted(counter prometheus.Counter, fn func()) float64 {
	before := testutil.ToFloat64(counter)
	fn()
	return testutil.ToFloat64(counter) - before
}

// onlyPlanOnto asserts a single plan placing everything on the named node.
func onlyPlanOnto(t *testing.T, plans []capacityUpgradePlan, node string) capacityUpgradePlan {
	t.Helper()
	if len(plans) != 1 {
		t.Fatalf("expected 1 plan onto %s, got %d: %+v", node, len(plans), plans)
	}
	if names := plans[0].nodeNames(); len(names) != 1 || names[0] != node {
		t.Fatalf("expected the plan onto %s, got %v", node, names)
	}
	return plans[0]
}

// ---- configuration -------------------------------------------------------

func TestPricingParamsDefaultOffAndParse(t *testing.T) {
	conf := newCapacityUpgradeConf()
	if conf.PriceAware || conf.MoveAdmission || conf.OverrunRelief || conf.admitsMoves() {
		t.Fatalf("pricing parameters must default off: %+v", conf)
	}
	if conf.MinSavingPercent != 10 || conf.PriceStalenessSeconds != 900 {
		t.Fatalf("unexpected defaults: saving %v staleness %d", conf.MinSavingPercent, conf.PriceStalenessSeconds)
	}
	// As decoded from the scheduler configuration: integers and floats.
	conf.parse(map[string]interface{}{"priceAware": true, "minSavingPercent": 15, "moveAdmission": true, "priceStalenessSeconds": 60})
	if !conf.PriceAware || !conf.MoveAdmission || conf.OverrunRelief || conf.MinSavingPercent != 15 || conf.PriceStalenessSeconds != 60 {
		t.Fatalf("unexpected parsed conf: %+v", conf)
	}
	conf.parse(map[string]interface{}{"minSavingPercent": 12.5, "overrunRelief": true})
	if conf.MinSavingPercent != 12.5 || !conf.OverrunRelief {
		t.Fatalf("unexpected parsed conf: %+v", conf)
	}
	for percent, want := range map[float64]float64{-5: 0, 0: 0, 10: 0.1, 100: 1, 250: 1} {
		conf.MinSavingPercent = percent
		if got := conf.minSaving(); got != want {
			t.Errorf("minSaving(%v%%) = %v, want %v", percent, got, want)
		}
	}
	relief := reliefConf()
	if !relief.admitsMoves() {
		t.Fatalf("overrunRelief must imply move admission")
	}
}

// With an unlabeledRank below the cheapest tier, a PodGroup of independent
// pods that all run on unlabeled nodes has no member on any tier of the
// order: it is not a candidate, with the pricing parameters off or on.
func TestUpgradeGroupRankedBelowEveryTierIsNotACandidate(t *testing.T) {
	for name, conf := range map[string]*capacityUpgradeConf{
		"defaults": liveConf(), "priceAware": priceAwareConf(), "overrunRelief": reliefConf(),
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			f.addNode(tierNode("owned-1", "", "a"))
			f.addNode(tierNode("owned-2", "", "a"))
			f.addNode(tierNode("reserved-1", "reserved", "a"))
			f.placeGroup(t, 1, nil,
				tierPod("p0", "owned-1", "pg-pods", 1, -4, time.Hour),
				tierPod("p1", "owned-2", "pg-pods", 1, -4, 2*time.Hour))
			conf.UnlabeledRank = -1
			if plans := f.planUpgrades(conf, nil); len(plans) != 0 {
				t.Fatalf("expected no plan, got %+v", plans)
			}
		})
	}
}

// ---- priceAware ----------------------------------------------------------

// Source and target are both spot, so the rank sees nothing to gain; only
// the prices tell them apart. The source costs 10 per GPU-hour.
func TestPriceUpgradeMovesOnlyForTheMinimumSaving(t *testing.T) {
	cases := []struct {
		minSaving   float64
		targetPrice string
		move        bool
	}{
		{10, "72", true},     // exactly 10% cheaper
		{10, "72.08", false}, // 9.9% cheaper
		{10, "64", true},     // 20% cheaper
		{10, "80", false},    // same price
		{10, "88", false},    // dearer
		{25, "64", false},    // 20% is not 25%
		{25, "60", true},     // exactly 25%
		{0, "80", false},     // no floor still needs a real saving
		{0, "79.92", true},   // 0.1% cheaper
		{-10, "80", false},   // a negative floor is no floor
		{100, "0.08", false}, // nothing saves 100%
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("min %v%% target %s", c.minSaving, c.targetPrice), func(t *testing.T) {
			f := newFixture(t)
			f.addNode(pricedNode("source", "spot", "a", "80"))
			f.addNode(pricedNode("target", "spot", "a", c.targetPrice))
			f.placeGroup(t, 1, nil, tierPod("train", "source", "pg-train", 1, -4, time.Hour))
			conf := priceAwareConf()
			conf.MinSavingPercent = c.minSaving
			plans := f.planUpgrades(conf, nil)
			if !c.move {
				if len(plans) != 0 {
					t.Fatalf("expected no move, got %+v", plans)
				}
				return
			}
			if p := onlyPlanOnto(t, plans, "target"); p.relief || p.gpus != 1 {
				t.Fatalf("unexpected plan: %+v", p)
			}
		})
	}
}

// Where source and target are both priced the price decides, even against
// the rank; where either is not, the rank decides as it always did.
func TestPriceUpgradeUsesPriceWhenBothPricedElseRank(t *testing.T) {
	stale := func(name, capacityType, price string) *api.NodeInfo {
		return observedNode(name, capacityType, "a", price, time.Hour)
	}
	malformed := func(name, capacityType string) *api.NodeInfo {
		return pricedNode(name, capacityType, "a", "n/a")
	}
	cases := []struct {
		name           string
		conf           *capacityUpgradeConf
		source, target *api.NodeInfo
		move           bool
	}{
		{"both priced, reserved dearer than spot: price says stay",
			priceAwareConf(), pricedNode("source", "spot", "a", "16"), pricedNode("target", "reserved", "a", "24"), false},
		{"both priced, spot cheaper than reserved: price says move",
			priceAwareConf(), pricedNode("source", "reserved", "a", "24"), pricedNode("target", "spot", "a", "16"), true},
		{"priceAware off, reserved dearer than spot: rank says move",
			liveConf(), pricedNode("source", "spot", "a", "16"), pricedNode("target", "reserved", "a", "24"), true},
		{"priceAware off, spot cheaper than reserved: rank says stay",
			liveConf(), pricedNode("source", "reserved", "a", "24"), pricedNode("target", "spot", "a", "16"), false},
		{"source unpriced: rank says move",
			priceAwareConf(), tierNode("source", "spot", "a"), pricedNode("target", "reserved", "a", "800"), true},
		{"source price stale: rank says move",
			priceAwareConf(), stale("source", "spot", "16"), pricedNode("target", "reserved", "a", "800"), true},
		{"source price malformed: rank says stay on the cheapest rank",
			priceAwareConf(), malformed("source", "reserved"), pricedNode("target", "spot", "a", "1"), false},
		{"target unpriced: rank says move",
			priceAwareConf(), pricedNode("source", "spot", "a", "16"), tierNode("target", "reserved", "a"), true},
		{"target price stale: rank says move",
			priceAwareConf(), pricedNode("source", "spot", "a", "16"), stale("target", "reserved", "1"), true},
		{"target price malformed: rank says stay",
			priceAwareConf(), pricedNode("source", "reserved", "a", "800"), malformed("target", "spot"), false},
		{"neither priced: rank says move",
			priceAwareConf(), tierNode("source", "spot", "a"), tierNode("target", "reserved", "a"), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFixture(t)
			f.addNode(c.source)
			f.addNode(c.target)
			f.placeGroup(t, 1, nil, tierPod("train", "source", "pg-train", 1, -4, time.Hour))
			plans := f.planUpgrades(c.conf, nil)
			if !c.move {
				if len(plans) != 0 {
					t.Fatalf("expected no move, got %+v", plans)
				}
				return
			}
			onlyPlanOnto(t, plans, "target")
		})
	}
}

// A flat capacity order leaves the rank with nothing to say; prices still
// move work.
func TestPriceUpgradeWorksWithAFlatCapacityOrder(t *testing.T) {
	f := newFixture(t)
	f.addNode(pricedNode("source", "spot", "a", "80"))
	f.addNode(pricedNode("target", "spot", "a", "40"))
	f.placeGroup(t, 1, nil, tierPod("train", "source", "pg-train", 1, -4, time.Hour))
	conf := priceAwareConf()
	conf.Order = "spot"
	onlyPlanOnto(t, f.planUpgrades(conf, nil), "target")
	conf.PriceAware = false
	if plans := f.planUpgrades(conf, nil); len(plans) != 0 {
		t.Fatalf("a flat order without prices must plan nothing, got %+v", plans)
	}
}

// Targets are offered cheapest first, and the group goes no further up the
// price list than it must to fit.
func TestPriceUpgradeTakesTheCheapestLevelThatFits(t *testing.T) {
	for _, c := range []struct {
		gpus   int64
		target string
	}{{1, "cheapest"}, {4, "cheaper"}} {
		t.Run(fmt.Sprintf("%d GPUs", c.gpus), func(t *testing.T) {
			f := newFixture(t)
			f.addNode(pricedNode("source", "spot", "a", "80"))
			f.addNode(pricedNode("cheaper", "spot", "a", "16"))
			f.addNode(pricedNode("cheapest", "spot", "a", "8"))
			// Seven of the cheapest node's GPUs are taken by work that
			// does not move (priority above the ceiling).
			f.placeGroup(t, 1, nil, tierPod("resident", "cheapest", "pg-resident", 7, 0, time.Hour))
			f.placeGroup(t, 1, nil, tierPod("train", "source", "pg-train", c.gpus, -4, time.Hour))
			onlyPlanOnto(t, f.planUpgrades(priceAwareConf(), nil), c.target)
		})
	}
}

// A gang is judged on what the whole placement costs, not node by node: one
// member sits on a node at 1 per GPU-hour, the other on one at 3, and the
// only room is on a third node. The gang moves when the total falls by the
// minimum saving, and not when moving the cheap member up eats the gain.
func TestPriceUpgradeGangSavingIsTheGroupsTotal(t *testing.T) {
	for _, c := range []struct {
		targetPrice string
		move        bool
	}{
		{"20", true},  // 4x2.5 + 4x1 = 14 against 16: saves 12.5%
		{"23", false}, // 4x2.875 + 4x1 = 15.5 against 16: saves 3.1%
	} {
		t.Run("target at "+c.targetPrice, func(t *testing.T) {
			f := newFixture(t)
			f.addNode(pricedNode("cheap", "spot", "a", "8"))
			f.addNode(pricedNode("dear", "spot", "a", "24"))
			f.addNode(pricedNode("target", "spot", "a", c.targetPrice))
			f.placeGroup(t, 1, nil, tierPod("resident", "cheap", "pg-resident", 4, 0, time.Hour))
			f.placeGroup(t, 2, nil,
				tierPod("w0", "cheap", "pg-gang", 4, -4, time.Hour),
				tierPod("w1", "dear", "pg-gang", 4, -4, time.Hour))
			plans := f.planUpgrades(priceAwareConf(), nil)
			if !c.move {
				if len(plans) != 0 {
					t.Fatalf("expected no move, got %+v", plans)
				}
				return
			}
			if len(plans) != 1 || !plans[0].gang || plans[0].nodes["target"] != 4 || plans[0].nodes["cheap"] != 4 {
				t.Fatalf("expected the gang on target and its own GPUs on cheap, got %+v", plans)
			}
		})
	}
}

// A PodGroup of independent pods moves the member on the dearest node, not
// the oldest one, and that member is judged on its own node's rank too.
func TestPriceUpgradeSinglesOutTheMemberOnTheDearestNode(t *testing.T) {
	build := func(target *api.NodeInfo) *fixture {
		f := newFixture(t)
		f.addNode(pricedNode("dear", "spot", "a", "80"))
		f.addNode(pricedNode("mid", "on-demand", "a", "40"))
		f.addNode(target)
		// The sibling's node is full, so it is nowhere to move to.
		f.placeGroup(t, 1, nil, tierPod("resident", "mid", "pg-resident", 7, 0, time.Hour))
		f.placeGroup(t, 1, nil,
			tierPod("alpha", "dear", "pg-pods", 1, -4, time.Hour),
			tierPod("beta", "mid", "pg-pods", 1, -4, 3*time.Hour))
		return f
	}
	p := onlyPlanOnto(t, build(pricedNode("target", "spot", "a", "8")).planUpgrades(priceAwareConf(), nil), "target")
	if len(p.members) != 1 || p.members[0].Name != "alpha" || p.from != "spot" {
		t.Fatalf("expected the member on the dearest node to move, got %v from %q", names(p.members), p.from)
	}
	// An unpriced spot node is no cheaper by rank than the spot node the
	// singled-out member runs on, whatever rank its sibling is on.
	if plans := build(tierNode("target", "spot", "a")).planUpgrades(priceAwareConf(), nil); len(plans) != 0 {
		t.Fatalf("expected no move onto an unpriced node of the member's own rank, got %+v", plans)
	}
	onlyPlanOnto(t, build(tierNode("target", "reserved", "a")).planUpgrades(priceAwareConf(), nil), "target")
}

// A gang whose members all sit on its dearest price can still save by moving
// only some of them: its own nodes stay on offer, and the total decides.
func TestPriceUpgradeGangMayMovePartOfItsMembers(t *testing.T) {
	f := newFixture(t)
	f.addNode(pricedNode("cheap", "spot", "a", "8"))
	f.addNode(pricedNode("dear-1", "spot", "a", "24"))
	f.addNode(pricedNode("dear-2", "spot", "a", "24"))
	// Each node has room for one member only.
	for _, node := range []string{"cheap", "dear-1", "dear-2"} {
		f.placeGroup(t, 1, nil, tierPod("resident-"+node, node, "pg-resident-"+node, 4, 0, time.Hour))
	}
	f.placeGroup(t, 2, nil,
		tierPod("w0", "dear-1", "pg-gang", 4, -4, time.Hour),
		tierPod("w1", "dear-2", "pg-gang", 4, -4, time.Hour))
	// 4x1 + 4x3 = 16 against 24: saves a third.
	plans := f.planUpgrades(priceAwareConf(), nil)
	if len(plans) != 1 || !plans[0].gang || plans[0].nodes["cheap"] != 4 || plans[0].gpus != 8 || len(plans[0].nodes) != 2 {
		t.Fatalf("expected one member on cheap and one on the gang's own GPUs, got %+v", plans)
	}
	conf := priceAwareConf()
	conf.MinSavingPercent = 40
	if plans := f.planUpgrades(conf, nil); len(plans) != 0 {
		t.Fatalf("expected a third not to be worth a 40%% minimum, got %+v", plans)
	}
}

// No member moves onto a node dearer than the group's dearest source, even
// where the group's total would still fall. Here the capped member is
// admitted only on a node dearer than anything the gang runs on, and the
// other member could move somewhere cheap enough to pay for it.
func TestPriceUpgradeNeverMovesAMemberOntoADearerNode(t *testing.T) {
	f := newFixture(t)
	f.addNode(caps(pricedNode("cheap", "spot", "a", "8"), "a", ""))
	f.addNode(caps(pricedNode("dearer", "spot", "a", "32"), "a", "a"))
	f.addNode(caps(pricedNode("source-1", "spot", "a", "24"), "a", ""))
	f.addNode(caps(pricedNode("source-2", "spot", "a", "24"), "a", ""))
	for _, node := range []string{"cheap", "dearer", "source-1", "source-2"} {
		f.placeGroup(t, 1, nil, tierPod("resident-"+node, node, "pg-resident-"+node, 4, 0, time.Hour))
	}
	f.placeGroup(t, 2, nil,
		capped(tierPod("w0", "source-1", "pg-gang", 4, -4, time.Hour), "a"),
		tierPod("w1", "source-2", "pg-gang", 4, -4, time.Hour))
	conf := pricingConf(func(c *capacityUpgradeConf) { c.PriceAware, c.MoveAdmission = true, true })
	// 4x4 + 4x1 = 20 against 24 would save a sixth.
	if plans := f.planUpgrades(conf, nil); len(plans) != 0 {
		t.Fatalf("expected no move onto a node dearer than the gang's sources, got %+v", plans)
	}
	// Once the cheap node admits the capped member the gang has a move
	// that needs no dearer node.
	caps(f.nodes["cheap"], "a", "a")
	plans := f.planUpgrades(conf, nil)
	if len(plans) != 1 || plans[0].nodes["cheap"] != 4 || plans[0].nodes["dearer"] != 0 {
		t.Fatalf("expected the capped member on cheap, got %+v", plans)
	}
}

// ---- moveAdmission -------------------------------------------------------

// The mover runs on spot and reserved capacity is idle: the rank says move.
// Whether it may depends on the mover's spend cap and the target's verdict.
func TestMoveAdmissionDecidesWhereACappedMoverMayGo(t *testing.T) {
	cases := []struct {
		name   string
		policy string
		target *api.NodeInfo
		move   bool
	}{
		{"cap applies and admits the target", "a", caps(pricedNode("target", "reserved", "a", "8"), "a,b", "a"), true},
		{"cap applies and does not admit the target", "b", caps(pricedNode("target", "reserved", "a", "8"), "a,b", "a"), false},
		{"cap does not apply to the target", "c", caps(pricedNode("target", "reserved", "a", "8"), "a,b", "a"), true},
		{"target has no verdict", "a", tierNode("target", "reserved", "a"), false},
		{"target verdict is stale", "a", caps(observedNode("target", "reserved", "a", "8", time.Hour), "a", "a"), false},
		{"target price is malformed", "a", caps(pricedNode("target", "reserved", "a", "eight"), "a", "a"), false},
		{"uncapped mover, target without a verdict", "", tierNode("target", "reserved", "a"), true},
		{"uncapped mover, target admitting nobody", "", caps(pricedNode("target", "reserved", "a", "8"), "a", ""), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFixture(t)
			f.addNode(tierNode("source", "spot", "a"))
			f.addNode(c.target)
			pod := tierPod("train", "source", "pg-train", 1, -4, time.Hour)
			if c.policy != "" {
				capped(pod, c.policy)
			}
			f.placeGroup(t, 1, nil, pod)

			// Without admission the move is planned whatever the cap says.
			onlyPlanOnto(t, f.planUpgrades(liveConf(), nil), "target")

			var plans []capacityUpgradePlan
			refused := counted(capacityUpgradeTargetsSkipped.WithLabelValues("move_admission"), func() {
				plans = f.planUpgrades(admissionConf(), nil)
			})
			if c.move {
				onlyPlanOnto(t, plans, "target")
				if refused != 0 {
					t.Fatalf("an admitted move must count no refusal, got %v", refused)
				}
				return
			}
			if len(plans) != 0 {
				t.Fatalf("expected the move refused, got %+v", plans)
			}
			if refused != 1 {
				t.Fatalf("expected the refusal counted once, got %v", refused)
			}
		})
	}
}

// A refused node is passed over, not a veto on the move: the mover goes to
// the next node that admits it.
func TestMoveAdmissionPassesOverRefusedTargets(t *testing.T) {
	f := newFixture(t)
	f.addNode(tierNode("source", "spot", "a"))
	f.addNode(caps(pricedNode("reserved-1", "reserved", "a", "8"), "a", ""))
	f.addNode(caps(pricedNode("reserved-2", "reserved", "a", "8"), "a", "a"))
	f.placeGroup(t, 1, nil, capped(tierPod("train", "source", "pg-train", 1, -4, time.Hour), "a"))
	onlyPlanOnto(t, f.planUpgrades(liveConf(), nil), "reserved-1")
	onlyPlanOnto(t, f.planUpgrades(admissionConf(), nil), "reserved-2")
}

// Every member of a gang needs a node that admits it; one capped member
// without one keeps the whole gang where it is.
func TestMoveAdmissionAppliesToEveryGangMember(t *testing.T) {
	build := func(admitted string) *fixture {
		f := newFixture(t)
		f.addNode(tierNode("spot-1", "spot", "a"))
		f.addNode(tierNode("spot-2", "spot", "a"))
		f.addNode(caps(pricedNode("reserved-1", "reserved", "a", "8"), "a", admitted))
		f.addNode(caps(pricedNode("reserved-2", "reserved", "a", "8"), "a", admitted))
		f.placeGroup(t, 2, nil,
			tierPod("w0", "spot-1", "pg-gang", 8, -4, time.Hour),
			capped(tierPod("w1", "spot-2", "pg-gang", 8, -4, time.Hour), "a"))
		return f
	}
	if plans := build("").planUpgrades(admissionConf(), nil); len(plans) != 0 {
		t.Fatalf("expected the gang to stay, got %+v", plans)
	}
	plans := build("a").planUpgrades(admissionConf(), nil)
	if len(plans) != 1 || !plans[0].gang || len(plans[0].members) != 2 {
		t.Fatalf("expected the gang to move, got %+v", plans)
	}
}

// Admission is asked again when the move is about to evict: a verdict that
// turned against the mover while the hold waited keeps it in place, and the
// move goes ahead once the target admits it again.
func TestMoveAdmissionIsAskedAgainBeforeEviction(t *testing.T) {
	f := newFixture(t)
	f.addNode(tierNode("source", "spot", "a"))
	target := f.addNode(caps(pricedNode("target", "reserved", "a", "8"), "a", "a"))
	mover := f.only(t, f.placeGroup(t, 1, nil, capped(tierPod("train", "source", "pg-train", 8, -4, time.Hour), "a")))
	store := newMemStore(f)
	conf := admissionConf()
	if plans := f.startOnly(t, conf, store); len(plans) != 1 {
		t.Fatalf("expected the move started, got %+v", plans)
	}

	target.Node.Annotations[capacitycost.SpendCapsAdmittedAnnotation] = ""
	steps, evictions := f.advance(conf, store, testNow.Add(10*time.Second), nil)
	if len(steps) != 0 || len(evictions) != 0 || mover.Status != api.Running {
		t.Fatalf("expected the mover kept while its target does not admit it, got %+v", steps)
	}
	if f.drainsOn(t, "source") != nil {
		t.Fatalf("the source must not be drained for a move that is not admitted")
	}

	// Without admission the same state evicts: the check is what held it.
	if steps := advanceCapacityUpgradeMoves(f.index(), f.nodes, f.jobs, liveConf(), testNow.Add(10*time.Second), nil); len(steps) != 1 || steps[0].outcome != "evicting" {
		t.Fatalf("expected the unguarded move to evict, got %+v", steps)
	}

	target.Node.Annotations[capacitycost.SpendCapsAdmittedAnnotation] = "a"
	steps, evictions = f.advance(conf, store, testNow.Add(20*time.Second), nil)
	if len(steps) != 1 || steps[0].outcome != "evicting" || len(evictions) != 1 || !sameTask(evictions[0], mover) {
		t.Fatalf("expected the mover evicted once admitted again, got %+v", steps)
	}
}

// ---- overrunRelief -------------------------------------------------------

// reliefFixture has a capped pod on a node over its spend cap and a second
// node of the same capacity type that costs twice as much: neither rank nor
// price gives a reason to move.
func reliefFixture(t *testing.T, target *api.NodeInfo) *fixture {
	f := newFixture(t)
	f.addNode(overCap(caps(pricedNode("over", "spot", "a", "80"), "a", ""), 10*time.Minute))
	if target != nil {
		f.addNode(target)
	}
	f.placeGroup(t, 1, nil, capped(tierPod("train", "over", "pg-train", 4, -4, time.Hour), "a"))
	return f
}

// admitting is a node at twice the over-cap node's price that admits
// policy a.
func admitting(name string) *api.NodeInfo {
	return caps(pricedNode(name, "spot", "a", "160"), "a", "a")
}

func TestOverrunReliefMovesACappedGroupToAdmittedIdleCapacity(t *testing.T) {
	f := reliefFixture(t, admitting("ok"))
	for name, conf := range map[string]*capacityUpgradeConf{
		"defaults": liveConf(), "priceAware": priceAwareConf(), "moveAdmission": admissionConf(),
	} {
		if plans := f.planUpgrades(conf, nil); len(plans) != 0 {
			t.Fatalf("%s: nothing but relief has a reason to move the pod, got %+v", name, plans)
		}
	}

	var plans []capacityUpgradePlan
	conf := reliefConf()
	conf.DryRun = true
	planned := counted(capacityUpgradeOverrunRelief.WithLabelValues("dry_run", "pod"), func() {
		plans = f.planUpgrades(conf, nil)
		for _, plan := range plans {
			plan.observe("dry_run")
		}
	})
	p := onlyPlanOnto(t, plans, "ok")
	if !p.relief || p.reason() != overrunReliefReason || p.gpus != 4 || p.displacedCount() != 0 {
		t.Fatalf("unexpected relief plan: %+v", p)
	}
	if planned != 1 {
		t.Fatalf("expected the relief plan counted once, got %v", planned)
	}
	if got := testutil.ToFloat64(capacityUpgradeOverrunNodes.WithLabelValues("open")); got != 1 {
		t.Fatalf("expected 1 node with an open relief window, got %v", got)
	}
}

func TestOverrunReliefNeedsAdmittedIdleCapacity(t *testing.T) {
	full := func(f *fixture) {
		f.placeGroup(t, 1, nil, tierPod("resident", "ok", "pg-resident", 8, 0, time.Hour))
	}
	cases := []struct {
		name   string
		target *api.NodeInfo
		after  func(*fixture)
	}{
		{"no other node", nil, nil},
		{"target does not admit the cap", caps(pricedNode("ok", "spot", "a", "160"), "a", ""), nil},
		{"target has no price", tierNode("ok", "spot", "a"), nil},
		{"target price is stale", caps(observedNode("ok", "spot", "a", "160", time.Hour), "a", "a"), nil},
		{"target is over its own cap", overCap(admitting("ok"), 10*time.Minute), nil},
		{"target has no idle GPUs", admitting("ok"), full},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := reliefFixture(t, c.target)
			if c.after != nil {
				c.after(f)
			}
			var plans []capacityUpgradePlan
			unrelieved := counted(capacityUpgradeOverrunRelief.WithLabelValues("no_capacity", "pod"), func() {
				plans = f.planUpgrades(reliefConf(), nil)
			})
			if len(plans) != 0 {
				t.Fatalf("expected no relief, got %+v", plans)
			}
			if unrelieved != 1 {
				t.Fatalf("expected the unrelieved group counted once, got %v", unrelieved)
			}
		})
	}
}

// Relief only ever takes idle GPUs, even where the strategy is otherwise
// allowed to make room over filler pods.
func TestOverrunReliefNeverEvictsToMakeRoom(t *testing.T) {
	f := reliefFixture(t, admitting("ok"))
	fillers := f.fillers(t, "ok", 8, 1, -9)
	conf := reliefConf()
	ceiling := int32(-9)
	conf.MaxDisplacedPriority = &ceiling
	if plans := f.planUpgrades(conf, nil); len(plans) != 0 {
		t.Fatalf("expected no relief over filler pods, got %+v", plans)
	}
	// Four fillers gone: the pod fits the idle GPUs and displaces nobody.
	for _, filler := range fillers[:4] {
		f.remove(t, filler)
	}
	if p := onlyPlanOnto(t, f.planUpgrades(conf, nil), "ok"); !p.relief || p.displacedCount() != 0 {
		t.Fatalf("unexpected relief plan: %+v", p)
	}

	// Two admitting nodes with two idle GPUs each hold four between them,
	// but the pod needs four on one node: still nobody is displaced.
	split := reliefFixture(t, admitting("ok-1"))
	split.addNode(admitting("ok-2"))
	split.fillers(t, "ok-1", 6, 1, -9)
	split.fillers(t, "ok-2", 6, 1, -9)
	if plans := split.planUpgrades(conf, nil); len(plans) != 0 {
		t.Fatalf("expected no relief that needs filler pods displaced, got %+v", plans)
	}
}

// Relief does not depend on the capacity rank: a capped pod on an over-cap
// node of the cheapest rank is relieved like any other.
func TestOverrunReliefAppliesOnTheCheapestRank(t *testing.T) {
	f := newFixture(t)
	f.addNode(overCap(caps(pricedNode("over", "reserved", "a", "80"), "a", ""), 10*time.Minute))
	f.addNode(caps(pricedNode("ok", "reserved", "a", "160"), "a", "a"))
	f.placeGroup(t, 1, nil, capped(tierPod("train", "over", "pg-train", 4, -4, time.Hour), "a"))
	if p := onlyPlanOnto(t, f.planUpgrades(reliefConf(), nil), "ok"); !p.relief {
		t.Fatalf("expected a relief plan, got %+v", p)
	}
}

// Only a capped pod on a node inside its relief window is relieved.
func TestOverrunReliefSourceMustBeFlaggedInsideItsWindow(t *testing.T) {
	flagged := func(mutate func(annotations map[string]string)) *api.NodeInfo {
		node := overCap(caps(pricedNode("over", "spot", "a", "80"), "a", ""), 10*time.Minute)
		mutate(node.Node.Annotations)
		return node
	}
	cases := []struct {
		name   string
		source *api.NodeInfo
		policy string
		relief bool
	}{
		{"flagged, window open", flagged(func(map[string]string) {}), "a", true},
		{"window closed", flagged(func(a map[string]string) {
			a[capacitycost.SpendCapActionAfterAnnotation] = testNow.Add(-time.Second).Format(time.RFC3339)
		}), "a", false},
		{"no action time", flagged(func(a map[string]string) { delete(a, capacitycost.SpendCapActionAfterAnnotation) }), "a", false},
		{"malformed action time", flagged(func(a map[string]string) { a[capacitycost.SpendCapActionAfterAnnotation] = "later" }), "a", false},
		{"not flagged", flagged(func(a map[string]string) { delete(a, capacitycost.SpendCapExceededAnnotation) }), "a", false},
		{"malformed flag", flagged(func(a map[string]string) { a[capacitycost.SpendCapExceededAnnotation] = "yes" }), "a", false},
		{"source price stale", flagged(func(a map[string]string) {
			a[capacitycost.OfferingPriceObservedAtAnnotation] = testNow.Add(-time.Hour).Format(time.RFC3339)
		}), "a", false},
		{"pod not capped", flagged(func(map[string]string) {}), "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFixture(t)
			f.addNode(c.source)
			f.addNode(admitting("ok"))
			pod := tierPod("train", "over", "pg-train", 4, -4, time.Hour)
			if c.policy != "" {
				capped(pod, c.policy)
			}
			f.placeGroup(t, 1, nil, pod)
			plans := f.planUpgrades(reliefConf(), nil)
			if !c.relief {
				if len(plans) != 0 {
					t.Fatalf("expected no relief, got %+v", plans)
				}
				return
			}
			if p := onlyPlanOnto(t, plans, "ok"); !p.relief {
				t.Fatalf("expected a relief plan, got %+v", p)
			}
		})
	}
}

// Groups in need of relief are planned before everything else, so they get
// the pass's budget and the idle capacity first.
func TestOverrunReliefGoesFirst(t *testing.T) {
	f := newFixture(t)
	f.addNode(overCap(caps(pricedNode("over", "spot", "a", "80"), "a", ""), 10*time.Minute))
	f.addNode(tierNode("spot-1", "spot", "a"))
	f.addNode(caps(pricedNode("reserved-1", "reserved", "a", "8"), "a", "a"))
	f.placeGroup(t, 1, nil, tierPod("important", "spot-1", "pg-important", 8, -1, 5*time.Hour))
	f.placeGroup(t, 1, nil, capped(tierPod("relieved", "over", "pg-relieved", 8, -5, time.Hour), "a"))

	// By priority and age the plain upgrade goes first and takes the node.
	plans := f.planUpgrades(liveConf(), nil)
	if len(plans) != 1 || plans[0].job.Name != "pg-important" {
		t.Fatalf("expected the higher-priority group to move, got %+v", plans)
	}
	plans = f.planUpgrades(reliefConf(), nil)
	if len(plans) != 1 || plans[0].job.Name != "pg-relieved" || !plans[0].relief {
		t.Fatalf("expected the group on the over-cap node to move first, got %+v", plans)
	}
}

// A gang on an over-cap node moves whole, into one zone; a PodGroup of
// independent pods moves the member that needs relief.
func TestOverrunReliefMovesWholeGangsAndTheMemberInNeed(t *testing.T) {
	t.Run("gang", func(t *testing.T) {
		f := newFixture(t)
		f.addNode(overCap(caps(pricedNode("over", "spot", "a", "80"), "a", ""), 10*time.Minute))
		f.addNode(tierNode("spot-1", "spot", "a"))
		f.addNode(caps(pricedNode("ok-a", "spot", "a", "160"), "a", "a"))
		f.addNode(caps(pricedNode("ok-b", "spot", "b", "160"), "a", "a"))
		f.placeGroup(t, 2, nil,
			capped(tierPod("w0", "over", "pg-gang", 8, -4, time.Hour), "a"),
			tierPod("w1", "spot-1", "pg-gang", 8, -4, time.Hour))
		// One admitting node per zone holds one member each: no zone
		// fits the gang.
		if plans := f.planUpgrades(reliefConf(), nil); len(plans) != 0 {
			t.Fatalf("expected no relief across zones, got %+v", plans)
		}
		f.addNode(caps(pricedNode("ok-a2", "spot", "a", "160"), "a", "a"))
		plans := f.planUpgrades(reliefConf(), nil)
		if len(plans) != 1 || !plans[0].relief || !plans[0].gang || len(plans[0].members) != 2 || plans[0].zone != "a" {
			t.Fatalf("expected the whole gang relieved into zone a, got %+v", plans)
		}
		if plans[0].nodes["ok-a"] != 8 || plans[0].nodes["ok-a2"] != 8 {
			t.Fatalf("unexpected nodes: %v", plans[0].nodes)
		}
	})
	t.Run("independent pods", func(t *testing.T) {
		f := newFixture(t)
		f.addNode(overCap(caps(pricedNode("over", "spot", "a", "80"), "a", ""), 10*time.Minute))
		// The older sibling's node does not admit the cap either.
		f.addNode(caps(pricedNode("fine", "spot", "a", "80"), "a", ""))
		f.addNode(admitting("ok"))
		f.placeGroup(t, 1, nil,
			tierPod("older", "fine", "pg-pods", 1, -4, 3*time.Hour),
			capped(tierPod("in-need", "over", "pg-pods", 1, -4, time.Hour), "a"))
		p := onlyPlanOnto(t, f.planUpgrades(reliefConf(), nil), "ok")
		if !p.relief || len(p.members) != 1 || p.members[0].Name != "in-need" {
			t.Fatalf("expected the member on the over-cap node relieved, got %v", names(p.members))
		}
	})
}

// A relief move is the same transaction as any other: hold, drain, evict,
// claim. Its hold says why, and its outcome is counted.
func TestOverrunReliefMoveRunsToTheClaim(t *testing.T) {
	f := reliefFixture(t, admitting("ok"))
	mover := f.only(t, f.jobs["default/pg-train"])
	store := newMemStore(f)
	conf := reliefConf()

	plans := f.startOnly(t, conf, store)
	if len(plans) != 1 || !plans[0].relief {
		t.Fatalf("expected the relief move started, got %+v", plans)
	}
	holds := f.holdsOn(t, "ok")
	if len(holds) != 1 || holds[0].Reason != overrunReliefReason || holds[0].Gpus != 4 {
		t.Fatalf("expected a relief hold on ok, got %+v", holds)
	}

	steps, evictions := f.advance(conf, store, testNow.Add(10*time.Second), nil)
	if len(steps) != 1 || steps[0].outcome != "evicting" || len(evictions) != 1 || !sameTask(evictions[0], mover) {
		t.Fatalf("expected the mover evicted, got %+v", steps)
	}
	if len(f.drainsOn(t, "over")) != 1 {
		t.Fatalf("expected the over-cap source drained")
	}
	f.evict(t, mover)
	f.remove(t, mover)

	f.bind(t, inGroup(capped(successorPod("train-2", "pg-train", 4, -4, testNow.Add(20*time.Second)), "a"), "pg-train-2"), "ok", 1)
	moved := counted(capacityUpgradeOverrunRelief.WithLabelValues("moved", "pod"), func() {
		steps, _ = f.advance(conf, store, testNow.Add(30*time.Second), nil)
	})
	if len(steps) != 1 || steps[0].outcome != "claimed" || moved != 1 {
		t.Fatalf("expected the relief claimed and counted, got %+v (counted %v)", steps, moved)
	}
	if f.holdsOn(t, "ok") != nil || f.drainsOn(t, "over") != nil {
		t.Fatalf("expected hold and drain released")
	}
}

// Relief is only due while the node is over its cap and the autoscaler has
// not reached the time it acts. A hold that outlives either is abandoned
// with the mover still running.
func TestOverrunReliefAbandonedOnceNoLongerDue(t *testing.T) {
	cases := []struct {
		name   string
		change func(source *api.NodeInfo) time.Time
	}{
		{"flag cleared", func(source *api.NodeInfo) time.Time {
			delete(source.Node.Annotations, capacitycost.SpendCapExceededAnnotation)
			delete(source.Node.Annotations, capacitycost.SpendCapActionAfterAnnotation)
			return testNow.Add(10 * time.Second)
		}},
		{"window closed", func(source *api.NodeInfo) time.Time {
			return testNow.Add(5 * time.Minute)
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := reliefFixture(t, admitting("ok"))
			overCap(f.nodes["over"], 5*time.Minute)
			mover := f.only(t, f.jobs["default/pg-train"])
			store := newMemStore(f)
			conf := reliefConf()
			if plans := f.startOnly(t, conf, store); len(plans) != 1 || !plans[0].relief {
				t.Fatalf("expected the relief move started, got %+v", plans)
			}

			at := c.change(f.nodes["over"])
			var steps []moveStep
			var evictions []*api.TaskInfo
			abandoned := counted(capacityUpgradeOverrunRelief.WithLabelValues("abandoned", "pod"), func() {
				steps, evictions = f.advance(conf, store, at, nil)
			})
			if len(steps) != 1 || steps[0].outcome != "abandoned" || steps[0].reason != "source no longer awaiting overrun relief" {
				t.Fatalf("expected the relief abandoned, got %+v", steps)
			}
			if len(evictions) != 0 || mover.Status != api.Running {
				t.Fatalf("nothing may be evicted for an abandoned relief")
			}
			if f.holdsOn(t, "ok") != nil || abandoned != 1 {
				t.Fatalf("expected the hold released and the outcome counted (%v)", abandoned)
			}
		})
	}
}

// ---- no reversal ---------------------------------------------------------

// A pod moved from one node to another is never planned back while the
// published prices and verdicts stand: every scenario plans the move with
// the pod on "from", and plans nothing with the pod on "to". Where the move
// itself changes what the autoscaler publishes about the node it left, the
// way back is checked against that too.
func TestMovesAreNeverReversedWhilePricesStand(t *testing.T) {
	all := pricingConf(func(c *capacityUpgradeConf) {
		c.PriceAware, c.MoveAdmission, c.OverrunRelief = true, true, true
	})
	spot := func(name, price string) func() *api.NodeInfo {
		return func() *api.NodeInfo { return pricedNode(name, "spot", "a", price) }
	}
	rejecting := func() *api.NodeInfo { return caps(pricedNode("from", "spot", "a", "80"), "a", "") }
	cases := []struct {
		name     string
		conf     *capacityUpgradeConf
		from, to func() *api.NodeInfo
		// vacated is the node moved off as it stands afterwards, when the
		// move changes it (nil: unchanged).
		vacated func() *api.NodeInfo
		policy  string
	}{
		{name: "cheaper by price", conf: priceAwareConf(), from: spot("from", "80"), to: spot("to", "64")},
		{name: "cheaper by price with no minimum saving",
			conf: pricingConf(func(c *capacityUpgradeConf) { c.PriceAware, c.MinSavingPercent = true, 0 }),
			from: spot("from", "80"), to: spot("to", "79")},
		{name: "cheaper by price against the rank", conf: priceAwareConf(),
			from: func() *api.NodeInfo { return pricedNode("from", "reserved", "a", "80") },
			to:   func() *api.NodeInfo { return pricedNode("to", "on-demand", "a", "8") }},
		{name: "cheaper by rank, neither priced", conf: priceAwareConf(),
			from: func() *api.NodeInfo { return tierNode("from", "spot", "a") },
			to:   func() *api.NodeInfo { return tierNode("to", "reserved", "a") }},
		{name: "cheaper by rank, only the source priced", conf: priceAwareConf(),
			from: spot("from", "8"),
			to:   func() *api.NodeInfo { return tierNode("to", "reserved", "a") }},
		{name: "cheaper by rank, only the target priced", conf: priceAwareConf(),
			from: func() *api.NodeInfo { return tierNode("from", "spot", "a") },
			to:   func() *api.NodeInfo { return pricedNode("to", "reserved", "a", "800") }},
		// Relief moved the pod to a node at twice the price; by price
		// alone the way back would save half, and admission refuses it,
		// whether the vacated node is still flagged or not.
		{name: "overrun relief onto dearer capacity, source still flagged", conf: all,
			from:   func() *api.NodeInfo { return overCap(rejecting(), 10*time.Minute) },
			to:     func() *api.NodeInfo { return caps(pricedNode("to", "spot", "a", "160"), "a", "a") },
			policy: "a"},
		{name: "overrun relief onto dearer capacity, source flag cleared", conf: all,
			from:    func() *api.NodeInfo { return overCap(rejecting(), 10*time.Minute) },
			to:      func() *api.NodeInfo { return caps(pricedNode("to", "spot", "a", "160"), "a", "a") },
			vacated: rejecting,
			policy:  "a"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			world := func(from *api.NodeInfo, podOn string) *fixture {
				f := newFixture(t)
				f.addNode(from)
				f.addNode(c.to())
				pod := tierPod("train", podOn, "pg-train", 4, -4, time.Hour)
				if c.policy != "" {
					capped(pod, c.policy)
				}
				f.placeGroup(t, 1, nil, pod)
				return f
			}
			onlyPlanOnto(t, world(c.from(), "from").planUpgrades(c.conf, nil), "to")
			vacated := c.from()
			if c.vacated != nil {
				vacated = c.vacated()
			}
			if plans := world(vacated, "to").planUpgrades(c.conf, nil); len(plans) != 0 {
				t.Fatalf("the move was planned back: %+v", plans)
			}
		})
	}
}

// The same through the whole transaction: the pod is moved for a saving,
// its successor claims the cheaper node, and with every other brake off
// (no cooldown, no move budget, no minimum age) the next passes still leave
// it there.
func TestCompletedPriceMoveIsNotUndoneByLaterPasses(t *testing.T) {
	f := newFixture(t)
	f.addNode(pricedNode("dear", "spot", "a", "80"))
	f.addNode(pricedNode("cheap", "spot", "a", "64"))
	mover := f.only(t, f.placeGroup(t, 1, nil, tierPod("train", "dear", "pg-train", 4, -4, time.Hour)))
	store := newMemStore(f)
	conf := pricingConf(func(c *capacityUpgradeConf) {
		c.PriceAware = true
		c.CooldownSeconds, c.MaxMovesPerGroup, c.MinPodAgeSeconds = 0, 0, 0
	})

	onlyPlanOnto(t, f.startOnly(t, conf, store), "cheap")
	if _, evictions := f.advance(conf, store, testNow.Add(10*time.Second), nil); len(evictions) != 1 {
		t.Fatalf("expected the mover evicted, got %v", names(evictions))
	}
	f.evict(t, mover)
	f.remove(t, mover)
	successor := inGroup(successorPod("train-2", "pg-train", 4, -4, testNow.Add(20*time.Second)), "pg-train-2")
	f.bind(t, successor, "cheap", 1)
	started := successor.CreationTimestamp
	successor.Status.StartTime = &started
	if steps, _ := f.advance(conf, store, testNow.Add(30*time.Second), nil); len(steps) != 1 || steps[0].outcome != "claimed" {
		t.Fatalf("expected the move claimed, got %+v", steps)
	}

	for pass := 1; pass <= 3; pass++ {
		later := testNow.Add(time.Duration(pass) * time.Hour)
		for _, node := range f.nodes {
			node.Node.Annotations[capacitycost.OfferingPriceObservedAtAnnotation] = later.Format(time.RFC3339)
		}
		if plans := planCapacityUpgrades(f.nodes, f.jobs, f.running, conf, f.index(), later, nil, nil); len(plans) != 0 {
			t.Fatalf("pass %d planned the pod back: %+v", pass, plans)
		}
	}
}
