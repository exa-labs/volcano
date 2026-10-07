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

// Move admission for the strategies that evict a pod so that it restarts
// somewhere else. A pod governed by a spend cap may only be moved to a node
// that cap admits at the node's published price (capacitycost.PriceBook),
// a pod the node autoscaler has not classified (no spend-cap annotation) is
// not moved at all, and a pod classified as uncapped (empty annotation) may
// go anywhere. Each strategy applies that to the destinations it plans with,
// behind its own moveAdmission parameter:
//
//   - capacityUpgrade checks every target it places a mover on (see
//     capacity_upgrade_price.go);
//   - gpuFragmentation checks every destination of its simulated repack
//     (repackAdmission);
//   - lowNodeUtilization names no destination per pod, so a pod is only
//     evicted when at least one of the underutilized nodes the strategy
//     evicts toward admits it (admittedVictims).
//
// With moveAdmission off (the default), none of this applies.
//
// Binding is not restricted: the pod that replaces an evicted one is
// scheduled like any pending pod.

import (
	"fmt"
	"time"

	"github.com/mitchellh/mapstructure"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/plugins/capacitycost"
)

// repackAdmission puts move admission in front of gpuFragmentation's fit
// predicate: a victim fails on every destination its spend cap does not
// admit, and an unclassified victim on every destination. Each refused
// victim and node is counted once.
func repackAdmission(inner func(*api.TaskInfo, *api.NodeInfo) error, conf *gpuFragmentationConf, now time.Time) func(*api.TaskInfo, *api.NodeInfo) error {
	book := capacitycost.NewPriceBook(now, time.Duration(conf.PriceStalenessSeconds)*time.Second)
	refused := map[string]bool{}
	return func(task *api.TaskInfo, node *api.NodeInfo) error {
		if task.Pod != nil && !book.AdmittedForMove(task.Pod, node.Node) {
			if key := string(task.Pod.UID) + "/" + node.Name; !refused[key] {
				refused[key] = true
				gpuRepackTargetsSkipped.WithLabelValues("move_admission").Inc()
				klog.V(4).Infof("gpuFragmentation: %s/%s not repacked onto %s: its spend cap %q does not admit the node",
					task.Namespace, task.Name, node.Name, capacitycost.SpendCapPolicy(task.Pod))
			}
			return fmt.Errorf("spend cap %q does not admit node %s", capacitycost.SpendCapPolicy(task.Pod), node.Name)
		}
		if inner == nil {
			return nil
		}
		return inner(task, node)
	}
}

// moveAdmissionParams are the admission parameters a strategy without a
// typed configuration of its own reads from its params.
type moveAdmissionParams struct {
	MoveAdmission         bool `mapstructure:"moveAdmission"`
	PriceStalenessSeconds int  `mapstructure:"priceStalenessSeconds"`
}

// admittedVictims applies move admission to a strategy that evicts toward a
// set of target nodes without assigning each victim one: with moveAdmission
// set in params, a victim is kept only when some target admits it (never,
// for an unclassified victim). Params that do not decode evict nothing,
// since whether admission was asked for is then unknown.
func admittedVictims(strategy string, victims []*api.TaskInfo, targets []*NodeUtilization, params map[string]interface{}, now time.Time) []*api.TaskInfo {
	var conf moveAdmissionParams
	if err := mapstructure.Decode(params, &conf); err != nil {
		klog.Errorf("%s: bad move admission params, evicting nothing: %v", strategy, err)
		return nil
	}
	if !conf.MoveAdmission {
		return victims
	}
	book := capacitycost.NewPriceBook(now, time.Duration(conf.PriceStalenessSeconds)*time.Second)
	admitted := make([]*api.TaskInfo, 0, len(victims))
	for _, victim := range victims {
		if victim.Pod == nil || !admittedOnAny(book, victim, targets) {
			klog.V(3).Infof("%s: not evicting %s/%s: no target node admits it (spend cap %q, classified %t)", strategy, victim.Namespace, victim.Name,
				capacitycost.SpendCapPolicy(victim.Pod), capacitycost.Classified(victim.Pod))
			continue
		}
		admitted = append(admitted, victim)
	}
	return admitted
}

// admittedOnAny reports whether at least one target admits the victim.
func admittedOnAny(book *capacitycost.PriceBook, victim *api.TaskInfo, targets []*NodeUtilization) bool {
	for _, target := range targets {
		if book.AdmittedForMove(victim.Pod, target.nodeInfo) {
			return true
		}
	}
	return false
}
