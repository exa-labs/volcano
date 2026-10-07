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

// One plan per pod per pass. The strategies run one after another as victim
// functions of the same session, each planning from the same snapshot, and
// shuffle evicts the union of what they return. Left alone, two strategies
// can both plan the same pod: one evicts it while the other writes a hold
// for it that then waits out its TTL. sessionPlannedMoves records every pod
// a strategy has committed to moving in the session (gpuFragmentation's
// victims, capacityUpgrade's movers and the pods its moves displace or
// evict), and each planning strategy leaves those pods out of the running
// tasks it plans with, so a strategy that runs later treats them as gone:
// gpuFragmentation does not drain a node holding one, and capacityUpgrade
// does not move a PodGroup with one among its members or count on
// displacing one.

import (
	"k8s.io/apimachinery/pkg/types"

	"volcano.sh/volcano/pkg/scheduler/api"
)

// plannedMoves is a set of pods, by UID, already planned for a move.
type plannedMoves map[types.UID]bool

// sessionPlannedMoves holds the pods planned for a move in the current
// session; OnSessionClose resets it.
var sessionPlannedMoves = plannedMoves{}

// record adds the tasks' pods to the set.
func (p plannedMoves) record(tasks ...*api.TaskInfo) {
	for _, task := range tasks {
		if task != nil && task.Pod != nil {
			p[task.Pod.UID] = true
		}
	}
}

// runningTasks indexes the running tasks a strategy plans with by pod UID,
// leaving out the pods already planned for a move (planned may be nil).
func runningTasks(tasks []*api.TaskInfo, planned plannedMoves) map[types.UID]*api.TaskInfo {
	running := make(map[types.UID]*api.TaskInfo, len(tasks))
	for _, task := range tasks {
		if task.Pod != nil && !planned[task.Pod.UID] {
			running[task.Pod.UID] = task
		}
	}
	return running
}
