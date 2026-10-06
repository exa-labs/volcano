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

// Node prices and spend caps, as published by the node autoscaler.
//
// The autoscaler that buys the nodes knows what each one currently costs and
// which spend-cap policies accept it at that cost. It publishes both as
// annotations; the scheduler only reads them:
//
//   - a node carries its current hourly price and the time the price was
//     last confirmed. A price older than the staleness window, or one that
//     does not parse, is treated as absent;
//   - a node lists the spend-cap policies that apply to it and, among
//     those, the ones that admit it at its current price;
//   - a node that every tenant's policy rejects is flagged as over its
//     spend cap, with the time the autoscaler will act on it;
//   - a pod governed by a spend-cap policy names that policy.
//
// PriceBook reads those annotations as of one instant. It is shared by the
// capacitycost scorer and the rescheduling strategies so that placement and
// migration agree on what a node costs and on where a pod may be moved.
// Binding a pending pod is never restricted by any of this; only voluntary
// moves of running pods are (AdmittedForMove).

import (
	"math"
	"strconv"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"

	"volcano.sh/volcano/pkg/scheduler/api"
)

const (
	// OfferingPriceAnnotation is the node's current price in currency units
	// per node-hour, a positive decimal.
	OfferingPriceAnnotation = "karpenter.sh/offering-price"
	// OfferingPriceObservedAtAnnotation is the RFC3339 time the autoscaler
	// last confirmed the price.
	OfferingPriceObservedAtAnnotation = "karpenter.sh/offering-price-observed-at"
	// SpendCapsAppliedAnnotation lists, comma-separated, the spend-cap
	// policies that apply to the node, whether they admit it or not. A
	// policy not listed does not cap the node.
	SpendCapsAppliedAnnotation = "karpenter.sh/workload-overlays-capped"
	// SpendCapsAdmittedAnnotation lists, comma-separated, the applicable
	// policies that admit the node at its current price.
	SpendCapsAdmittedAnnotation = "karpenter.sh/workload-overlays-admitted"
	// SpendCapExceededAnnotation is present, holding the RFC3339 time it
	// began, while every tenant of the node is capped below its price.
	SpendCapExceededAnnotation = "karpenter.sh/spend-cap-exceeded"
	// SpendCapActionAfterAnnotation is the RFC3339 time the autoscaler acts
	// on an over-cap node itself; until then the node's tenants may be
	// moved off it.
	SpendCapActionAfterAnnotation = "karpenter.sh/spend-cap-action-after"
	// PodSpendCapPolicyAnnotation names the spend-cap policy governing a
	// pod. Pods without it are not capped.
	PodSpendCapPolicyAnnotation = "karpenter.sh/workload-overlay"

	// DefaultPriceStaleness is how long a published price stays usable.
	DefaultPriceStaleness = 15 * time.Minute
	// priceClockSkew is how far in the future an observation time may lie
	// (publisher and scheduler clocks differ) before it is distrusted.
	priceClockSkew = time.Minute

	// milliUnits converts api.Resource amounts (milli-CPU, milli-units of
	// scalar resources) to whole units.
	milliUnits = 1000
)

// PriceState classifies a node's published price.
type PriceState string

const (
	// PriceFresh is a well-formed price confirmed within the staleness
	// window.
	PriceFresh PriceState = "fresh"
	// PriceStale is a well-formed price whose confirmation is too old (or
	// lies in the future).
	PriceStale PriceState = "stale"
	// PriceAbsent is a missing or malformed price or confirmation time.
	PriceAbsent PriceState = "absent"
)

// PriceBook answers price and move-admission questions as of one instant.
type PriceBook struct {
	now       time.Time
	staleness time.Duration
}

// NewPriceBook returns a PriceBook reading prices as of now. A staleness
// that is not positive falls back to DefaultPriceStaleness.
func NewPriceBook(now time.Time, staleness time.Duration) *PriceBook {
	if staleness <= 0 {
		staleness = DefaultPriceStaleness
	}
	return &PriceBook{now: now, staleness: staleness}
}

// State classifies the node's price and returns it when it is fresh.
func (b *PriceBook) State(node *v1.Node) (float64, PriceState) {
	if node == nil {
		return 0, PriceAbsent
	}
	price, err := strconv.ParseFloat(strings.TrimSpace(node.Annotations[OfferingPriceAnnotation]), 64)
	if err != nil || math.IsNaN(price) || math.IsInf(price, 0) || price <= 0 {
		return 0, PriceAbsent
	}
	observed, err := time.Parse(time.RFC3339, strings.TrimSpace(node.Annotations[OfferingPriceObservedAtAnnotation]))
	if err != nil {
		return 0, PriceAbsent
	}
	if age := b.now.Sub(observed); age > b.staleness || age < -priceClockSkew {
		return 0, PriceStale
	}
	return price, PriceFresh
}

// Price is the node's hourly price; ok is false unless it is fresh.
func (b *PriceBook) Price(node *v1.Node) (price float64, ok bool) {
	price, state := b.State(node)
	return price, state == PriceFresh
}

// UnitPrice is the node's hourly price per whole unit of resource it offers
// (per CPU core, per GPU); ok is false when the node has no fresh price or
// none of the resource.
func (b *PriceBook) UnitPrice(node *api.NodeInfo, resource v1.ResourceName) (unit float64, ok bool) {
	if node == nil || node.Allocatable == nil {
		return 0, false
	}
	price, ok := b.Price(node.Node)
	if !ok {
		return 0, false
	}
	offered := node.Allocatable.Get(resource) / milliUnits
	if offered <= 0 {
		return 0, false
	}
	return price / offered, true
}

// SpendCapPolicy is the spend-cap policy governing the pod, or "" when the
// pod is not capped.
func SpendCapPolicy(pod *v1.Pod) string {
	if pod == nil {
		return ""
	}
	return strings.TrimSpace(pod.Annotations[PodSpendCapPolicyAnnotation])
}

// AdmittedForMove reports whether a running pod may be moved onto the node
// voluntarily. A pod without a spend-cap policy may go anywhere. A capped
// pod may only go where its policy accepts the node's current price: the
// node needs a fresh price (no verdict, no move), and the policy must
// either not apply to the node or be among those that admit it.
func (b *PriceBook) AdmittedForMove(pod *v1.Pod, node *v1.Node) bool {
	policy := SpendCapPolicy(pod)
	if policy == "" {
		return true
	}
	if _, ok := b.Price(node); !ok {
		return false
	}
	if !listed(node.Annotations[SpendCapsAppliedAnnotation], policy) {
		return true
	}
	return listed(node.Annotations[SpendCapsAdmittedAnnotation], policy)
}

// OverSpendCap reports whether the node is flagged as over its spend cap.
// A flag that is not a timestamp is treated as absent.
func OverSpendCap(node *v1.Node) bool {
	if node == nil {
		return false
	}
	_, err := time.Parse(time.RFC3339, strings.TrimSpace(node.Annotations[SpendCapExceededAnnotation]))
	return err == nil
}

// ReliefWindowOpen reports whether the node is over its spend cap and the
// autoscaler has not yet reached the time it acts on it: the interval in
// which moving the node's tenants elsewhere is the scheduler's to do. The
// node's price must be fresh, since the flag is only as current as the
// publisher that also confirms the price.
func (b *PriceBook) ReliefWindowOpen(node *v1.Node) bool {
	if !OverSpendCap(node) {
		return false
	}
	if _, ok := b.Price(node); !ok {
		return false
	}
	after, err := time.Parse(time.RFC3339, strings.TrimSpace(node.Annotations[SpendCapActionAfterAnnotation]))
	return err == nil && after.After(b.now)
}

// listed reports whether name is an element of the comma-separated list.
func listed(list, name string) bool {
	for _, element := range strings.Split(list, ",") {
		if strings.TrimSpace(element) == name {
			return true
		}
	}
	return false
}
