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

package capacitycost

// Tests for PriceBook: which published prices count, what a unit of a
// node's capacity costs, where a capped pod may be moved, and when an
// over-cap node is still inside its relief window.

import (
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/util"
)

var bookNow = time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)

// at formats an instant relative to bookNow as the publisher writes it.
func at(offset time.Duration) string {
	return bookNow.Add(offset).Format(time.RFC3339)
}

// annotated builds a bare node carrying the given annotations.
func annotated(annotations map[string]string) *v1.Node {
	return &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n", Annotations: annotations}}
}

// priced builds the annotations of a node whose price was confirmed age ago.
func priced(price string, age time.Duration, extra ...string) map[string]string {
	annotations := map[string]string{
		OfferingPriceAnnotation:           price,
		OfferingPriceObservedAtAnnotation: at(-age),
	}
	for i := 0; i+1 < len(extra); i += 2 {
		annotations[extra[i]] = extra[i+1]
	}
	return annotations
}

// cappedPod builds a pod governed by the given spend-cap policy ("" for
// none).
func cappedPod(policy string) *v1.Pod {
	pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default"}}
	if policy != "" {
		pod.Annotations = map[string]string{PodSpendCapPolicyAnnotation: policy}
	}
	return pod
}

func TestPriceStateTreatsStaleAndMalformedAsUnpriced(t *testing.T) {
	book := NewPriceBook(bookNow, 15*time.Minute)
	cases := []struct {
		name        string
		annotations map[string]string
		wantPrice   float64
		wantState   PriceState
	}{
		{"fresh", priced("32.000", time.Minute), 32, PriceFresh},
		{"fresh with spaces", priced(" 4.5 ", time.Minute), 4.5, PriceFresh},
		{"observed exactly one staleness ago", priced("32", 15*time.Minute), 32, PriceFresh},
		{"observed just past the staleness", priced("32", 15*time.Minute+time.Second), 0, PriceStale},
		{"observed within clock skew ahead", priced("32", -time.Minute), 32, PriceFresh},
		{"observed beyond clock skew ahead", priced("32", -time.Minute-time.Second), 0, PriceStale},
		{"no annotations", nil, 0, PriceAbsent},
		{"price without observation time", map[string]string{OfferingPriceAnnotation: "32"}, 0, PriceAbsent},
		{"observation time without price", map[string]string{OfferingPriceObservedAtAnnotation: at(0)}, 0, PriceAbsent},
		{"price not a number", priced("cheap", time.Minute), 0, PriceAbsent},
		{"price empty", priced("", time.Minute), 0, PriceAbsent},
		{"price zero", priced("0.000", time.Minute), 0, PriceAbsent},
		{"price negative", priced("-1", time.Minute), 0, PriceAbsent},
		{"price NaN", priced("NaN", time.Minute), 0, PriceAbsent},
		{"price infinite", priced("+Inf", time.Minute), 0, PriceAbsent},
		{"observation time not RFC3339", map[string]string{
			OfferingPriceAnnotation: "32", OfferingPriceObservedAtAnnotation: "2026-10-06 12:00:00",
		}, 0, PriceAbsent},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			price, state := book.State(annotated(c.annotations))
			if price != c.wantPrice || state != c.wantState {
				t.Fatalf("State = (%v, %s), want (%v, %s)", price, state, c.wantPrice, c.wantState)
			}
			if _, ok := book.Price(annotated(c.annotations)); ok != (c.wantState == PriceFresh) {
				t.Fatalf("Price ok = %v for state %s", ok, state)
			}
		})
	}
	if _, state := book.State(nil); state != PriceAbsent {
		t.Fatalf("nil node state = %s, want absent", state)
	}
}

func TestPriceBookDefaultsStaleness(t *testing.T) {
	book := NewPriceBook(bookNow, 0)
	if _, ok := book.Price(annotated(priced("1", DefaultPriceStaleness))); !ok {
		t.Fatalf("a price confirmed one default staleness ago must be fresh")
	}
	if _, ok := book.Price(annotated(priced("1", DefaultPriceStaleness+time.Second))); ok {
		t.Fatalf("a price older than the default staleness must not be fresh")
	}
	short := NewPriceBook(bookNow, time.Minute)
	if _, ok := short.Price(annotated(priced("1", 2*time.Minute))); ok {
		t.Fatalf("a configured staleness must be honoured")
	}
}

// offering builds a node offering 64 CPUs and the given GPUs at a price.
func offering(name string, gpus string, annotations map[string]string) *api.NodeInfo {
	resources := v1.ResourceList{"cpu": resource.MustParse("64")}
	if gpus != "" {
		resources["nvidia.com/gpu"] = resource.MustParse(gpus)
	}
	node := util.BuildNode(name, resources, nil)
	node.Annotations = annotations
	return api.NewNodeInfo(node)
}

func TestUnitPriceDividesByWhatTheNodeOffers(t *testing.T) {
	book := NewPriceBook(bookNow, 15*time.Minute)
	fresh := priced("32", time.Minute)
	cases := []struct {
		name     string
		node     *api.NodeInfo
		resource v1.ResourceName
		want     float64
		ok       bool
	}{
		{"per GPU", offering("n", "8", fresh), "nvidia.com/gpu", 4, true},
		{"per GPU on a smaller node", offering("n", "4", fresh), "nvidia.com/gpu", 8, true},
		{"per CPU", offering("n", "8", fresh), v1.ResourceCPU, 0.5, true},
		{"resource the node does not offer", offering("n", "", fresh), "nvidia.com/gpu", 0, false},
		{"stale price", offering("n", "8", priced("32", time.Hour)), "nvidia.com/gpu", 0, false},
		{"no price", offering("n", "8", nil), "nvidia.com/gpu", 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := book.UnitPrice(c.node, c.resource)
			if got != c.want || ok != c.ok {
				t.Fatalf("UnitPrice = (%v, %v), want (%v, %v)", got, ok, c.want, c.ok)
			}
		})
	}
	if _, ok := book.UnitPrice(nil, v1.ResourceCPU); ok {
		t.Fatalf("a nil node has no unit price")
	}
}

func TestAdmittedForMoveFailsClosedForCappedPods(t *testing.T) {
	book := NewPriceBook(bookNow, 15*time.Minute)
	cases := []struct {
		name        string
		policy      string
		annotations map[string]string
		want        bool
	}{
		// A pod without a policy is not capped: it may go anywhere, priced
		// or not.
		{"uncapped pod, unpriced node", "", nil, true},
		{"uncapped pod, node capping others", "", priced("4", time.Minute, SpendCapsAppliedAnnotation, "a,b", SpendCapsAdmittedAnnotation, ""), true},
		// No fresh verdict, no move.
		{"capped pod, unpriced node", "a", map[string]string{SpendCapsAdmittedAnnotation: "a", SpendCapsAppliedAnnotation: "a"}, false},
		{"capped pod, stale price", "a", priced("4", time.Hour, SpendCapsAppliedAnnotation, "a", SpendCapsAdmittedAnnotation, "a"), false},
		{"capped pod, malformed price", "a", priced("four", time.Minute, SpendCapsAppliedAnnotation, "a", SpendCapsAdmittedAnnotation, "a"), false},
		// A policy that does not apply to the node does not restrict it.
		{"policy does not apply to the node", "a", priced("4", time.Minute, SpendCapsAppliedAnnotation, "b,c", SpendCapsAdmittedAnnotation, "b"), true},
		{"node capped by no policy", "a", priced("4", time.Minute), true},
		{"node with empty lists", "a", priced("4", time.Minute, SpendCapsAppliedAnnotation, "", SpendCapsAdmittedAnnotation, ""), true},
		// A policy that applies must admit the node's current price.
		{"policy applies and admits", "a", priced("4", time.Minute, SpendCapsAppliedAnnotation, "a,b", SpendCapsAdmittedAnnotation, "a"), true},
		{"policy applies and does not admit", "b", priced("4", time.Minute, SpendCapsAppliedAnnotation, "a,b", SpendCapsAdmittedAnnotation, "a"), false},
		{"policy applies, nothing admitted", "a", priced("4", time.Minute, SpendCapsAppliedAnnotation, "a", SpendCapsAdmittedAnnotation, ""), false},
		{"policy applies, admitted list missing", "a", priced("4", time.Minute, SpendCapsAppliedAnnotation, "a"), false},
		{"lists with spaces", " a ", priced("4", time.Minute, SpendCapsAppliedAnnotation, "b, a", SpendCapsAdmittedAnnotation, " a ,b"), true},
		{"applied list with spaces still applies", "a", priced("4", time.Minute, SpendCapsAppliedAnnotation, " a , b", SpendCapsAdmittedAnnotation, "b"), false},
		{"name that is only a prefix of an admitted one", "a", priced("4", time.Minute, SpendCapsAppliedAnnotation, "a,ab", SpendCapsAdmittedAnnotation, "ab"), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := book.AdmittedForMove(cappedPod(c.policy), annotated(c.annotations)); got != c.want {
				t.Fatalf("AdmittedForMove = %v, want %v", got, c.want)
			}
		})
	}
	if book.AdmittedForMove(cappedPod("a"), nil) {
		t.Fatalf("a capped pod is not admitted on a node that is not there")
	}
	if !book.AdmittedForMove(nil, annotated(nil)) || SpendCapPolicy(nil) != "" {
		t.Fatalf("a missing pod has no policy to refuse it")
	}
}

func TestReliefWindowOpenOnlyWhileTheAutoscalerWaits(t *testing.T) {
	book := NewPriceBook(bookNow, 15*time.Minute)
	cases := []struct {
		name        string
		annotations map[string]string
		over, open  bool
	}{
		{"not flagged", priced("4", time.Minute), false, false},
		{"flagged, action still ahead", priced("4", time.Minute,
			SpendCapExceededAnnotation, at(-5*time.Minute), SpendCapActionAfterAnnotation, at(10*time.Minute)), true, true},
		{"flagged, action time reached", priced("4", time.Minute,
			SpendCapExceededAnnotation, at(-15*time.Minute), SpendCapActionAfterAnnotation, at(0)), true, false},
		{"flagged, action time passed", priced("4", time.Minute,
			SpendCapExceededAnnotation, at(-20*time.Minute), SpendCapActionAfterAnnotation, at(-5*time.Minute)), true, false},
		{"flagged without an action time", priced("4", time.Minute,
			SpendCapExceededAnnotation, at(-5*time.Minute)), true, false},
		{"flagged, malformed action time", priced("4", time.Minute,
			SpendCapExceededAnnotation, at(-5*time.Minute), SpendCapActionAfterAnnotation, "soon"), true, false},
		{"malformed flag", priced("4", time.Minute,
			SpendCapExceededAnnotation, "true", SpendCapActionAfterAnnotation, at(10*time.Minute)), false, false},
		{"flagged, price stale", priced("4", time.Hour,
			SpendCapExceededAnnotation, at(-5*time.Minute), SpendCapActionAfterAnnotation, at(10*time.Minute)), true, false},
		{"flagged, no price", map[string]string{
			SpendCapExceededAnnotation: at(-5 * time.Minute), SpendCapActionAfterAnnotation: at(10 * time.Minute)}, true, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			node := annotated(c.annotations)
			if got := OverSpendCap(node); got != c.over {
				t.Fatalf("OverSpendCap = %v, want %v", got, c.over)
			}
			if got := book.ReliefWindowOpen(node); got != c.open {
				t.Fatalf("ReliefWindowOpen = %v, want %v", got, c.open)
			}
		})
	}
	if OverSpendCap(nil) || book.ReliefWindowOpen(nil) {
		t.Fatalf("a nil node is not over its cap")
	}
}
