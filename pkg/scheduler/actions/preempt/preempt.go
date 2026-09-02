/*
Copyright 2018 The Kubernetes Authors.
Copyright 2018-2025 The Volcano Authors.

Modifications made by Volcano authors:
- Added topology-aware preemption
- Enhanced with predicate error caching and BestEffort constraints
- Added victim selection algorithms with scoring and ordering

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

package preempt

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	k8sutil "k8s.io/kubernetes/pkg/scheduler/util"

	fwk "k8s.io/kube-scheduler/framework"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/metrics"
	"volcano.sh/volcano/pkg/scheduler/util"
)

const (
	EnableTopologyAwarePreemptionKey = "enableTopologyAwarePreemption"

	// This setting has no effect unless topology-aware preemption is enabled.
	EnableNodeOrderScoreInPreemptionKey = "enableNodeOrderScoreInPreemption"

	// Scale float node-order scores for the int64 candidate comparator.
	nodeOrderScorePrecision = 1000

	TopologyAwarePreemptWorkerNumKey = "topologyAwarePreemptWorkerNum"

	MinCandidateNodesPercentageKey = "minCandidateNodesPercentage"
	MinCandidateNodesAbsoluteKey   = "minCandidateNodesAbsolute"
	MaxCandidateNodesAbsoluteKey   = "maxCandidateNodesAbsolute"

	// GangPlacementRetriesKey bounds how many times a starving job's preemption
	// transaction is retried with the previously chosen nodes excluded when the
	// job could not be pipelined as a whole. 0 disables retries.
	GangPlacementRetriesKey = "gangPlacementRetries"
)

type Action struct {
	ssn *framework.Session

	enablePredicateErrorCache bool

	enableTopologyAwarePreemption bool

	enableNodeOrderScoreInPreemption bool

	topologyAwarePreemptWorkerNum int
	minCandidateNodesPercentage   int
	minCandidateNodesAbsolute     int
	maxCandidateNodesAbsolute     int
	gangPlacementRetries          int
}

func New() *Action {
	return &Action{
		enablePredicateErrorCache:        true,
		enableTopologyAwarePreemption:    false,
		enableNodeOrderScoreInPreemption: true,
		topologyAwarePreemptWorkerNum:    16,
		minCandidateNodesPercentage:      10,
		minCandidateNodesAbsolute:        1,
		maxCandidateNodesAbsolute:        100,
		gangPlacementRetries:             2,
	}
}

func (pmpt *Action) Name() string {
	return "preempt"
}

func (pmpt *Action) Initialize() {}

func (pmpt *Action) parseArguments(ssn *framework.Session) {
	arguments := framework.GetArgOfActionFromConf(ssn.Configurations, pmpt.Name())
	arguments.GetBool(&pmpt.enablePredicateErrorCache, conf.EnablePredicateErrCacheKey)
	arguments.GetBool(&pmpt.enableTopologyAwarePreemption, EnableTopologyAwarePreemptionKey)
	arguments.GetBool(&pmpt.enableNodeOrderScoreInPreemption, EnableNodeOrderScoreInPreemptionKey)
	arguments.GetInt(&pmpt.topologyAwarePreemptWorkerNum, TopologyAwarePreemptWorkerNumKey)
	arguments.GetInt(&pmpt.minCandidateNodesPercentage, MinCandidateNodesPercentageKey)
	arguments.GetInt(&pmpt.minCandidateNodesAbsolute, MinCandidateNodesAbsoluteKey)
	arguments.GetInt(&pmpt.maxCandidateNodesAbsolute, MaxCandidateNodesAbsoluteKey)
	arguments.GetInt(&pmpt.gangPlacementRetries, GangPlacementRetriesKey)
	pmpt.ssn = ssn
}

func (pmpt *Action) Execute(ssn *framework.Session) {
	klog.V(5).Infof("Enter Preempt ...")
	defer klog.V(5).Infof("Leaving Preempt ...")

	pmpt.parseArguments(ssn)

	preemptorsMap := map[api.QueueID]*util.PriorityQueue{}
	preemptorTasks := map[api.JobID]*util.PriorityQueue{}

	var underRequest []*api.JobInfo
	queues := map[api.QueueID]*api.QueueInfo{}

	for _, job := range ssn.Jobs {
		if job.IsPending() {
			continue
		}

		if vr := ssn.JobValid(job); vr != nil && !vr.Pass {
			klog.V(4).Infof("Job <%s/%s> Queue <%s> skip preemption, reason: %v, message %v", job.Namespace, job.Name, job.Queue, vr.Reason, vr.Message)
			continue
		}

		if queue, found := ssn.Queues[job.Queue]; !found {
			continue
		} else if _, existed := queues[queue.UID]; !existed {
			klog.V(3).Infof("Added Queue <%s> for Job <%s/%s>",
				queue.Name, job.Namespace, job.Name)
			queues[queue.UID] = queue
		}

		// check job if starving for more resources.
		if !ssn.JobStarving(job) {
			continue
		}

		// TODO: Currently, jobs containing networkTopology do not support preemption. Related issue: https://github.com/volcano-sh/volcano/issues/4374
		if job.ContainsNetworkTopology() {
			klog.V(3).Infof("Job <%s/%s> Queue <%s> skip preemption, reason: jobs containing networkTopology do not support preemption",
				job.Namespace, job.Name, job.Queue)
			continue
		}

		if _, found := preemptorsMap[job.Queue]; !found {
			preemptorsMap[job.Queue] = util.NewPriorityQueue(ssn.JobOrderFn)
		}
		preemptorsMap[job.Queue].Push(job)
		underRequest = append(underRequest, job)
		preemptorTasks[job.UID] = pendingPreemptorTasks(ssn, job)
	}

	// The predicate error cache is keyed by task role within a job, so helpers
	// are held per job: sharing one across jobs buys nothing, and a job's
	// helper is replaced when a placement attempt is abandoned.
	predicateHelpers := map[api.JobID]util.PredicateHelper{}
	// Preemption between Jobs within Queue.
	for _, queue := range queues {
		for {
			preemptors := preemptorsMap[queue.UID]

			// If no preemptors, no preemption.
			if preemptors == nil || preemptors.Empty() {
				klog.V(4).Infof("No preemptors in Queue <%s>, break.", queue.Name)
				break
			}

			preemptorJob := preemptors.Pop().(*api.JobInfo)

			// Commit changes only if job is pipelined, otherwise try next job.
			assigned, committed := pmpt.preemptForJob(ssn, preemptorJob, preemptorTasks, predicateHelpers)
			if !committed {
				continue
			}

			if assigned {
				preemptors.Push(preemptorJob)
			}
		}

		// Preemption between Task within Job.
		for _, job := range underRequest {
			// Fix: preemptor numbers lose when in same job
			preemptorTasks[job.UID] = pendingPreemptorTasks(ssn, job)
			for {
				if _, found := preemptorTasks[job.UID]; !found {
					break
				}

				if preemptorTasks[job.UID].Empty() {
					break
				}

				preemptor := preemptorTasks[job.UID].Pop().(*api.TaskInfo)

				stmt := framework.NewStatement(ssn)
				assigned, err := pmpt.preempt(ssn, stmt, preemptor, func(task *api.TaskInfo) bool {
					// Ignore non running task.
					if !api.PreemptableStatus(task.Status) {
						return false
					}
					// BestEffort pod is not supported to preempt unBestEffort pod.
					if preemptor.BestEffort && !task.BestEffort {
						return false
					}
					// should skip not preemptable pod
					if !task.Preemptable {
						return false
					}

					// Preempt tasks within job.
					return preemptor.Job == task.Job
				}, jobPredicateHelper(predicateHelpers, job.UID), nil)
				if err != nil {
					klog.V(3).Infof("Preemptor <%s/%s> failed to preempt Task , err: %s", preemptor.Namespace, preemptor.Name, err)
				}
				stmt.Commit()

				// If no preemption, next job.
				if !assigned {
					break
				}
			}
		}
	}
}

func (pmpt *Action) UnInitialize() {}

// pendingPreemptorTasks queues the job's pending, non-gated tasks in task order.
func pendingPreemptorTasks(ssn *framework.Session, job *api.JobInfo) *util.PriorityQueue {
	tasks := util.NewPriorityQueue(ssn.TaskOrderFn)
	for _, task := range job.TaskStatusIndex[api.Pending] {
		if task.SchGated {
			continue
		}
		tasks.Push(task)
	}
	return tasks
}

// jobPredicateHelper returns the job's predicate helper, creating it on first use.
func jobPredicateHelper(helpers map[api.JobID]util.PredicateHelper, jobID api.JobID) util.PredicateHelper {
	if helper, found := helpers[jobID]; found {
		return helper
	}
	helper := util.NewPredicateHelper()
	helpers[jobID] = helper
	return helper
}

// pipelinedNodes returns the nodes hosting the job's pipelined tasks, skipping
// tasks listed in ignore (those pipelined by an earlier, committed statement).
func pipelinedNodes(job *api.JobInfo, ignore sets.Set[api.TaskID]) sets.Set[string] {
	nodes := sets.New[string]()
	for uid, task := range job.TaskStatusIndex[api.Pipelined] {
		if !ignore.Has(uid) {
			nodes.Insert(task.NodeName)
		}
	}
	return nodes
}

// preemptForJob runs preemption transactions for a starving job until one
// leaves the job pipelined and is committed, or the retry budget is spent.
//
// Tasks are placed one at a time, so the node chosen for an early task can
// leave later tasks with no candidate at all — e.g. a gang bound together by
// a topology pod affinity whose first task lands in a domain that has no
// other preemptable node. A single pass would discard the transaction every
// session and the job would starve indefinitely while a viable placement
// existed elsewhere. Each retry excludes the nodes the previous attempt
// pipelined onto, steering the whole gang into a different region of the
// cluster.
//
// Each retry replaces the job's predicate helper: its error cache is keyed
// by task role, so failures recorded for one sibling under the abandoned
// placement would otherwise be replayed against the others — in this attempt
// and in every later use of the helper for this job.
//
// Returns whether the last task attempt was assigned (the caller re-queues
// the job to keep preempting for its remaining tasks) and whether the
// transaction was committed.
func (pmpt *Action) preemptForJob(
	ssn *framework.Session,
	job *api.JobInfo,
	preemptorTasks map[api.JobID]*util.PriorityQueue,
	predicateHelpers map[api.JobID]util.PredicateHelper,
) (assigned, committed bool) {
	committedTasks := sets.KeySet(job.TaskStatusIndex[api.Pipelined])
	excludedNodes := sets.New[string]()
	for attempt := 0; ; attempt++ {
		stmt := framework.NewStatement(ssn)
		assigned = pmpt.preemptJobTasks(ssn, stmt, job, preemptorTasks[job.UID], jobPredicateHelper(predicateHelpers, job.UID), excludedNodes)
		if ssn.JobPipelined(job) {
			stmt.Commit()
			return assigned, true
		}

		chosen := pipelinedNodes(job, committedTasks)
		stmt.Discard()

		if attempt >= pmpt.gangPlacementRetries || chosen.Len() == 0 {
			return false, false
		}

		klog.V(3).Infof("Job <%s/%s> not pipelined after placing tasks on %v, retrying preemption without those nodes",
			job.Namespace, job.Name, sets.List(chosen))
		excludedNodes = excludedNodes.Union(chosen)
		predicateHelpers[job.UID] = util.NewPredicateHelper()
		preemptorTasks[job.UID] = pendingPreemptorTasks(ssn, job)
		clearLastTxContexts(job)
	}
}

// clearLastTxContexts drops the transaction context Discard left on the job's
// pending tasks, so the scheduling reason and nominated node published at
// session close describe the final attempt alone rather than a mix of
// placements from abandoned ones.
func clearLastTxContexts(job *api.JobInfo) {
	for _, task := range job.TaskStatusIndex[api.Pending] {
		task.ClearLastTxContext()
	}
}

// preemptJobTasks preempts for the job's queued tasks in order within a single
// statement, stopping once the job is no longer starving or no tasks remain.
// Returns whether the last attempted task was assigned.
func (pmpt *Action) preemptJobTasks(
	ssn *framework.Session,
	stmt *framework.Statement,
	job *api.JobInfo,
	tasks *util.PriorityQueue,
	predicateHelper util.PredicateHelper,
	excludedNodes sets.Set[string],
) bool {
	var assigned bool
	for {
		// If job is not request more resource, then stop preempting.
		if !ssn.JobStarving(job) {
			break
		}

		// If not preemptor tasks, next job.
		if tasks.Empty() {
			klog.V(3).Infof("No preemptor task in job <%s/%s>.", job.Namespace, job.Name)
			break
		}

		preemptor := tasks.Pop().(*api.TaskInfo)

		var err error
		assigned, err = pmpt.preempt(ssn, stmt, preemptor, func(task *api.TaskInfo) bool {
			// Ignore non running task.
			if !api.PreemptableStatus(task.Status) {
				return false
			}
			// BestEffort pod is not supported to preempt unBestEffort pod.
			if preemptor.BestEffort && !task.BestEffort {
				return false
			}
			if !task.Preemptable {
				return false
			}
			victimJob, found := ssn.Jobs[task.Job]
			if !found {
				return false
			}
			// Preempt other jobs within queue
			return victimJob.Queue == job.Queue && preemptor.Job != task.Job
		}, predicateHelper, excludedNodes)
		if err != nil {
			klog.V(3).Infof("Preemptor <%s/%s> failed to preempt Task , err: %s", preemptor.Namespace, preemptor.Name, err)
		}
	}
	return assigned
}

func (pmpt *Action) preempt(
	ssn *framework.Session,
	stmt *framework.Statement,
	preemptor *api.TaskInfo,
	filter func(*api.TaskInfo) bool,
	predicateHelper util.PredicateHelper,
	excludedNodes sets.Set[string],
) (bool, error) {
	// Eligibility predicates the nominated node, which needs the task's cycle state.
	if err := ssn.PrePredicateFn(preemptor); err != nil {
		return false, fmt.Errorf("PrePredicate for task %s/%s failed for: %v", preemptor.Namespace, preemptor.Name, err)
	}

	// Check whether the task is eligible to preempt others, e.g., check preemptionPolicy is `Never` or not
	if err := pmpt.taskEligibleToPreempt(preemptor); err != nil {
		return false, err
	}

	// we should filter out those nodes that are UnschedulableAndUnresolvable status got in allocate action
	allNodes := ssn.FilterOutUnschedulableAndUnresolvableNodesForTask(preemptor)
	if excludedNodes.Len() > 0 {
		// Drop the nodes abandoned gang placement attempts chose before
		// PredicateNodes samples its candidates, so the exclusion cannot
		// shrink an already truncated sample.
		allNodes = slices.DeleteFunc(slices.Clone(allNodes), func(n *api.NodeInfo) bool {
			return excludedNodes.Has(n.Name)
		})
	}
	predicateNodes, fitErrors := predicateHelper.PredicateNodes(preemptor, allNodes, ssn.PredicateForPreemptAction, pmpt.enablePredicateErrorCache, ssn.NodesInShard)

	// When predicate filtering returns no candidates the cluster is effectively
	// fully utilised — exactly when preemption is most needed. Fall back to the
	// resource-constrained subset of allNodes (i.e. exclude nodes newly marked
	// UnschedulableAndUnresolvable by predicate plugins, e.g. nodeSelector or
	// affinity mismatches discovered at predicate time) so victim selection can
	// still run. Structurally incompatible nodes are skipped because preemption
	// can never make them schedulable for this task.
	if len(predicateNodes) == 0 && fitErrors != nil {
		unresolvable := fitErrors.GetUnschedulableAndUnresolvableNodes()
		for _, n := range allNodes {
			if _, skip := unresolvable[n.Name]; !skip {
				predicateNodes = append(predicateNodes, n)
			}
		}
	}

	candidateNodes := util.GetPredicatedNodeByShard(predicateNodes, ssn.NodesInShard)
	var preemptSuccess bool
	var err error
	//try to preempt in order if multiple candidate Nodes group with priority exist
	for _, nodes := range candidateNodes {
		if pmpt.enableTopologyAwarePreemption {
			if preemptSuccess, err = pmpt.topologyAwarePreempt(ssn, stmt, preemptor, filter, nodes); preemptSuccess {
				break
			}
		} else if preemptSuccess, err = pmpt.normalPreempt(ssn, stmt, preemptor, filter, nodes); preemptSuccess {
			break
		}
	}
	return preemptSuccess, err
}

func (pmpt *Action) normalPreempt(
	ssn *framework.Session,
	stmt *framework.Statement,
	preemptor *api.TaskInfo,
	filter func(*api.TaskInfo) bool,
	predicateNodes []*api.NodeInfo,
) (bool, error) {
	selectedNodes := sortNodesByGradient(ssn, preemptor, predicateNodes)

	job, found := ssn.Jobs[preemptor.Job]
	if !found {
		return false, fmt.Errorf("not found Job %s in Session", preemptor.Job)
	}

	currentQueue := ssn.Queues[job.Queue]

	assigned := false

	for _, node := range selectedNodes {
		klog.V(3).Infof("Considering Task <%s/%s> on Node <%s>.",
			preemptor.Namespace, preemptor.Name, node.Name)

		var preemptees []*api.TaskInfo
		for _, task := range node.Tasks {
			if filter == nil {
				preemptees = append(preemptees, task.Clone())
			} else if filter(task) {
				preemptees = append(preemptees, task.Clone())
			}
		}
		victims := ssn.Preemptable(preemptor, preemptees)
		metrics.UpdatePreemptionVictimsCount(len(victims))

		if err := util.ValidateVictims(preemptor, node, victims); err != nil {
			klog.V(3).Infof("No validated victims on Node <%s>: %v", node.Name, err)
			continue
		}

		victimsQueue := ssn.BuildVictimsPriorityQueue(victims, preemptor)
		// Preempt victims for tasks, pick lowest priority task first.
		preempted := api.EmptyResource()

		for !victimsQueue.Empty() {
			// If reclaimed enough resources, break loop to avoid Sub panic.
			// Preempt action is about preempt in same queue, which job is not allocatable in allocate action, due to:
			// 1. cluster has free resource, but queue not allocatable
			// 2. cluster has no free resource, but queue not allocatable
			// 3. cluster has no free resource, but queue allocatable
			// for case 1 and 2, high priority job/task can preempt low priority job/task in same queue;
			// for case 3, it need to do reclaim resource from other queue, in reclaim action;
			// so if current queue is not allocatable(the queue will be overused when consider current preemptor's requests)
			// or current idle resource is not enough for preemptor, it need to continue preempting
			// otherwise, break out
			if ssn.Allocatable(currentQueue, preemptor) && preemptor.InitResreq.LessEqual(node.FutureIdle(), api.Zero) {
				break
			}
			preemptee := victimsQueue.Pop().(*api.TaskInfo)
			klog.V(3).Infof("Try to preempt Task <%s/%s> for Task <%s/%s>",
				preemptee.Namespace, preemptee.Name, preemptor.Namespace, preemptor.Name)
			if err := stmt.Evict(preemptee, "preempt"); err != nil {
				klog.Errorf("Failed to preempt Task <%s/%s> for Task <%s/%s>: %v",
					preemptee.Namespace, preemptee.Name, preemptor.Namespace, preemptor.Name, err)
				continue
			}
			preempted.Add(preemptee.Resreq)
		}

		evictionOccurred := false
		if !preempted.IsEmpty() {
			evictionOccurred = true
		}

		metrics.RegisterPreemptionAttempts()
		klog.V(3).Infof("Preempted <%v> for Task <%s/%s> requested <%v>.",
			preempted, preemptor.Namespace, preemptor.Name, preemptor.InitResreq)

		// If preemptor's queue is not allocatable, it means preemptor cannot be allocated. So no need care about the node idle resource
		if ssn.Allocatable(currentQueue, preemptor) && preemptor.InitResreq.LessEqual(node.FutureIdle(), api.Zero) {
			if err := stmt.Pipeline(preemptor, node.Name, evictionOccurred); err != nil {
				klog.Errorf("Failed to pipeline Task <%s/%s> on Node <%s>",
					preemptor.Namespace, preemptor.Name, node.Name)
				if rollbackErr := stmt.UnPipeline(preemptor); rollbackErr != nil {
					klog.Errorf("Failed to unpipeline Task %v on %v in Session %v for %v.",
						preemptor.UID, node.Name, ssn.UID, rollbackErr)
				}
			}

			// Ignore pipeline error, will be corrected in next scheduling loop.
			assigned = true

			break
		}
	}

	return assigned, nil
}

func (pmpt *Action) taskEligibleToPreempt(preemptor *api.TaskInfo) error {
	if preemptor.Pod.Spec.PreemptionPolicy != nil && *preemptor.Pod.Spec.PreemptionPolicy == v1.PreemptNever {
		return fmt.Errorf("not eligible to preempt other tasks due to preemptionPolicy is Never")
	}

	nomNodeName := preemptor.Pod.Status.NominatedNodeName
	if len(nomNodeName) > 0 {
		nodeInfo, ok := pmpt.ssn.Nodes[nomNodeName]
		if !ok {
			return fmt.Errorf("not eligible due to the pod's nominated node is not found in the session")
		}

		// Diverges from upstream, which rejects the task when its nominated
		// node passes predicates. Predicates ignore resource fit, so a passing
		// nominated node does not mean the task can run there; that is
		// allocate's call. Preempt stays eligible so the task can join its
		// gang siblings' transaction (pipelineOnFittingNode keeps it from
		// evicting anyone if the node really does fit).
		if err := pmpt.ssn.PredicateFn(preemptor, nodeInfo); err != nil {
			fitError, ok := err.(*api.FitError)
			if !ok {
				return fmt.Errorf("not eligible due to the predicate returned a non-FitError error, the error is: %v", err)
			}

			// If the pod's nominated node is considered as UnschedulableAndUnresolvable by the predicate,
			// then the pod should be considered for preempting again.
			if fitError.Status.ContainsUnschedulableAndUnresolvable() {
				return nil
			}
		}

		preemptorPodPriority := PodPriority(preemptor.Pod)
		for _, p := range nodeInfo.Pods() {
			if PodPriority(p) < preemptorPodPriority && podTerminatingByPreemption(p) {
				// There is a terminating pod on the nominated node.
				return fmt.Errorf("not eligible due to a terminating pod caused by preemption on the nominated node")
			}
		}
	}
	return nil
}

// pipelineOnFittingNode pipelines the preemptor without evicting anyone when
// some node's future idle resources already fit it — typically capacity being
// released by victims of earlier preemptions in the same session. Node-order
// plugins (binpack et al.) choose among fitting nodes, so successive tasks of
// a preempting job stack into holes already opened by their siblings instead
// of each evicting a fresh victim on another node. normalPreempt gets this
// behavior from its zero-victim path (ValidateVictims allows empty victim
// sets); the topology-aware path requires at least one victim per candidate,
// so without this it must evict somewhere even when eviction is unnecessary.
func (pmpt *Action) pipelineOnFittingNode(
	ssn *framework.Session,
	stmt *framework.Statement,
	preemptor *api.TaskInfo,
	predicateNodes []*api.NodeInfo,
) bool {
	job, found := ssn.Jobs[preemptor.Job]
	if !found {
		return false
	}

	if !ssn.Allocatable(ssn.Queues[job.Queue], preemptor) {
		return false
	}

	// predicateNodes may include nodes kept only because their predicate
	// failures are resolvable by eviction (e.g. anti-affinity); a resource fit
	// alone doesn't make the task runnable there, so re-check predicates
	// against the current session state before pipelining without victims.
	var fittingNodes []*api.NodeInfo
	for _, node := range predicateNodes {
		if preemptor.InitResreq.LessEqual(node.FutureIdle(), api.Zero) &&
			ssn.PredicateFn(preemptor, node) == nil {
			fittingNodes = append(fittingNodes, node)
		}
	}
	if len(fittingNodes) == 0 {
		return false
	}

	best := fittingNodes[0]
	if len(fittingNodes) > 1 {
		sortedNodes := sortNodesByGradient(ssn, preemptor, fittingNodes)
		if len(sortedNodes) == 0 {
			return false
		}
		best = sortedNodes[0]
	}

	klog.V(3).Infof("Task <%s/%s> fits future idle of Node <%s>, pipelining without evictions",
		preemptor.Namespace, preemptor.Name, best.Name)

	if err := stmt.Pipeline(preemptor, best.Name, false); err != nil {
		klog.Errorf("Failed to pipeline Task <%s/%s> on Node <%s>",
			preemptor.Namespace, preemptor.Name, best.Name)
		if rollbackErr := stmt.UnPipeline(preemptor); rollbackErr != nil {
			klog.Errorf("Failed to unpipeline Task %v on %v in Session %v for %v.",
				preemptor.UID, best.Name, ssn.UID, rollbackErr)
		}
	}

	// Ignore pipeline error, will be corrected in next scheduling loop.
	return true
}

// sortNodesByGradient orders candidate nodes so that nodes whose current Idle
// resources satisfy the task come before nodes that only fit after releasing
// resources free up, mirroring allocate's gradient partition in
// prioritizeNodes. Nodes are scored and sorted within each gradient.
func sortNodesByGradient(ssn *framework.Session, task *api.TaskInfo, nodes []*api.NodeInfo) []*api.NodeInfo {
	var idleFitNodes, futureFitNodes []*api.NodeInfo
	for _, node := range nodes {
		if task.InitResreq.LessEqual(node.Idle, api.Zero) {
			idleFitNodes = append(idleFitNodes, node)
		} else {
			futureFitNodes = append(futureFitNodes, node)
		}
	}

	sorted := make([]*api.NodeInfo, 0, len(nodes))
	for _, gradient := range [][]*api.NodeInfo{idleFitNodes, futureFitNodes} {
		if len(gradient) == 0 {
			continue
		}
		nodeScores := util.PrioritizeNodes(task, gradient, ssn.BatchNodeOrderFn, ssn.NodeOrderMapFn, ssn.NodeOrderReduceFn)
		sorted = append(sorted, util.SortNodes(nodeScores)...)
	}
	return sorted
}

func (pmpt *Action) topologyAwarePreempt(
	ssn *framework.Session,
	stmt *framework.Statement,
	preemptor *api.TaskInfo,
	filter func(*api.TaskInfo) bool,
	predicateNodes []*api.NodeInfo,
) (bool, error) {
	if pmpt.pipelineOnFittingNode(ssn, stmt, preemptor, predicateNodes) {
		return true, nil
	}

	// Find all preemption candidates.
	candidates, nodeToStatusMap, err := pmpt.findCandidates(preemptor, filter, predicateNodes, stmt)
	if err != nil && len(candidates) == 0 {
		return false, err
	}

	// Return error when there are no candidates that fit the pod.
	if len(candidates) == 0 {
		// Specify nominatedNodeName to clear the pod's nominatedNodeName status, if applicable.
		return false, fmt.Errorf("no candidates that fit the pod, the status of the nodes are %v", nodeToStatusMap)
	}

	// Find the best candidate.
	bestCandidate := SelectCandidate(candidates, ssn, preemptor, pmpt.enableNodeOrderScoreInPreemption)
	if bestCandidate == nil || len(bestCandidate.Name()) == 0 {
		return false, fmt.Errorf("no candidate node for preemption")
	}

	if status := prepareCandidate(bestCandidate, preemptor.Pod, stmt, ssn); !status.IsSuccess() {
		return false, fmt.Errorf("failed to prepare candidate: %v", status)
	}

	if err := stmt.Pipeline(preemptor, bestCandidate.Name(), true); err != nil {
		klog.Errorf("Failed to pipeline Task <%s/%s> on Node <%s>",
			preemptor.Namespace, preemptor.Name, bestCandidate.Name())
		if rollbackErr := stmt.UnPipeline(preemptor); rollbackErr != nil {
			klog.Errorf("Failed to unpipeline Task %v on %v in Session %v for %v.",
				preemptor.UID, bestCandidate.Name(), ssn.UID, rollbackErr)
		}
	}

	return true, nil
}

func (pmpt *Action) findCandidates(preemptor *api.TaskInfo, filter func(*api.TaskInfo) bool, predicateNodes []*api.NodeInfo, stmt *framework.Statement) ([]*candidate, map[string]api.Status, error) {
	if len(predicateNodes) == 0 {
		klog.V(3).Infof("No nodes are eligible to preempt task %s/%s", preemptor.Namespace, preemptor.Name)
		return nil, nil, nil
	}
	klog.Infof("the predicateNodes number is %d", len(predicateNodes))

	nodeToStatusMap := make(map[string]api.Status)

	offset, numCandidates := pmpt.GetOffsetAndNumCandidates(len(predicateNodes))

	candidates, nodeStatuses, err := pmpt.DryRunPreemption(preemptor, predicateNodes, offset, numCandidates, filter, stmt)
	for node, nodeStatus := range nodeStatuses {
		nodeToStatusMap[node] = nodeStatus
	}

	return candidates, nodeToStatusMap, err
}

// prepareCandidate evicts the victim pods before nominating the selected candidate
func prepareCandidate(c *candidate, pod *v1.Pod, stmt *framework.Statement, ssn *framework.Session) *api.Status {
	for _, victim := range c.Victims() {
		klog.V(3).Infof("Try to preempt Task <%s/%s> for Task <%s/%s>",
			victim.Namespace, victim.Name, pod.Namespace, pod.Name)
		if err := stmt.Evict(victim, "preempt"); err != nil {
			klog.Errorf("Failed to preempt Task <%s/%s> for Task <%s/%s>: %v",
				victim.Namespace, victim.Name, pod.Namespace, pod.Name, err)
			return api.AsStatus(err)
		}
	}

	metrics.RegisterPreemptionAttempts()

	return nil
}

// podTerminatingByPreemption returns true if the pod is in the termination state caused by preempt action.
func podTerminatingByPreemption(p *v1.Pod) bool {
	if p.DeletionTimestamp == nil {
		return false
	}

	for _, condition := range p.Status.Conditions {
		if condition.Type == v1.DisruptionTarget {
			return condition.Status == v1.ConditionTrue && condition.Reason == v1.PodReasonPreemptionByScheduler
		}
	}
	return false
}

// PodPriority returns priority of the given pod.
func PodPriority(pod *v1.Pod) int32 {
	if pod.Spec.Priority != nil {
		return *pod.Spec.Priority
	}
	// When priority of a running pod is nil, it means it was created at a time
	// that there was no global default priority class and the priority class
	// name of the pod was empty. So, we resolve to the static default priority.
	return 0
}

// calculateNumCandidates returns the number of candidates the FindCandidates
// method must produce from dry running based on the constraints given by
// <minCandidateNodesPercentage> and <minCandidateNodesAbsolute>. The number of
// candidates returned will never be greater than <numNodes>.
func (pmpt *Action) calculateNumCandidates(numNodes int) int {
	n := (numNodes * pmpt.minCandidateNodesPercentage) / 100

	if n < pmpt.minCandidateNodesAbsolute {
		n = pmpt.minCandidateNodesAbsolute
	}

	if n > pmpt.maxCandidateNodesAbsolute {
		n = pmpt.maxCandidateNodesAbsolute
	}

	if n > numNodes {
		n = numNodes
	}

	return n
}

// offset is used to randomly select a starting point in the potentialNodes array.
// This helps distribute the preemption checks across different nodes and avoid
// always starting from the beginning of the node list, which could lead to
// uneven distribution of preemption attempts.
// GetOffsetAndNumCandidates chooses a random offset and calculates the number
// of candidates that should be shortlisted for dry running preemption.
func (pmpt *Action) GetOffsetAndNumCandidates(numNodes int) (int, int) {
	return rand.Intn(numNodes), pmpt.calculateNumCandidates(numNodes)
}

func (pmpt *Action) DryRunPreemption(preemptor *api.TaskInfo, potentialNodes []*api.NodeInfo, offset, numCandidates int, filter func(*api.TaskInfo) bool, stmt *framework.Statement) ([]*candidate, map[string]api.Status, error) {
	candidates := newCandidateList(numCandidates)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	nodeStatuses := make(map[string]api.Status)
	var statusesLock sync.Mutex
	var errs []error

	job, found := pmpt.ssn.Jobs[preemptor.Job]
	if !found {
		return nil, nil, fmt.Errorf("not found Job %s in Session", preemptor.Job)
	}

	currentQueue := pmpt.ssn.Queues[job.Queue]

	state := pmpt.ssn.GetCycleState(preemptor.UID)

	checkNode := func(i int) {
		nodeInfoCopy := potentialNodes[(int(offset)+i)%len(potentialNodes)].Clone()
		stateCopy := state.Clone()

		victims, status := SelectVictimsOnNode(ctx, stateCopy, preemptor, currentQueue, nodeInfoCopy, pmpt.ssn, filter, stmt)
		if status.IsSuccess() && len(victims) != 0 {
			c := &candidate{
				victims: victims,
				name:    nodeInfoCopy.Name,
			}
			candidates.add(c)
			if candidates.size() >= numCandidates {
				cancel()
			}
			return
		}
		if status.IsSuccess() && len(victims) == 0 {
			status = api.AsStatus(fmt.Errorf("expected at least one victim pod on node %q", nodeInfoCopy.Name))
		}
		statusesLock.Lock()
		if status.Code == api.Error {
			errs = append(errs, status.AsError())
		}
		nodeStatuses[nodeInfoCopy.Name] = *status
		statusesLock.Unlock()
	}

	workqueue.ParallelizeUntil(ctx, pmpt.topologyAwarePreemptWorkerNum, len(potentialNodes), checkNode)
	return candidates.get(), nodeStatuses, utilerrors.NewAggregate(errs)
}

type candidate struct {
	victims []*api.TaskInfo
	name    string
}

// Victims returns s.victims.
func (s *candidate) Victims() []*api.TaskInfo {
	return s.victims
}

// Name returns s.name.
func (s *candidate) Name() string {
	return s.name
}

type candidateList struct {
	idx   int32
	items []*candidate
}

func newCandidateList(size int) *candidateList {
	return &candidateList{idx: -1, items: make([]*candidate, size)}
}

// add adds a new candidate to the internal array atomically.
func (cl *candidateList) add(c *candidate) {
	if idx := atomic.AddInt32(&cl.idx, 1); idx < int32(len(cl.items)) {
		cl.items[idx] = c
	}
}

// size returns the number of candidate stored. Note that some add() operations
// might still be executing when this is called, so care must be taken to
// ensure that all add() operations complete before accessing the elements of
// the list.
func (cl *candidateList) size() int {
	n := int(atomic.LoadInt32(&cl.idx) + 1)
	if n >= len(cl.items) {
		n = len(cl.items)
	}
	return n
}

// get returns the internal candidate array. This function is NOT atomic and
// assumes that all add() operations have been completed.
func (cl *candidateList) get() []*candidate {
	return cl.items[:cl.size()]
}

// SelectVictimsOnNode finds minimum set of pods on the given node that should be preempted in order to make enough room
// for "pod" to be scheduled.
func SelectVictimsOnNode(
	ctx context.Context,
	state fwk.CycleState,
	preemptor *api.TaskInfo,
	currentQueue *api.QueueInfo,
	nodeInfo *api.NodeInfo,
	ssn *framework.Session,
	filter func(*api.TaskInfo) bool,
	stmt *framework.Statement,
) ([]*api.TaskInfo, *api.Status) {
	var potentialVictims []*api.TaskInfo

	removeTask := func(rti *api.TaskInfo) error {
		err := ssn.SimulateRemoveTaskFn(ctx, state, preemptor, rti, nodeInfo)
		if err != nil {
			return err
		}

		if err := nodeInfo.RemoveTask(rti); err != nil {
			return err
		}
		return nil
	}

	addTask := func(ati *api.TaskInfo) error {
		err := ssn.SimulateAddTaskFn(ctx, state, preemptor, ati, nodeInfo)
		if err != nil {
			return err
		}

		if err := nodeInfo.AddTask(ati); err != nil {
			return err
		}
		return nil
	}

	var preemptees []*api.TaskInfo
	for _, task := range nodeInfo.Tasks {
		if filter == nil {
			preemptees = append(preemptees, task.Clone())
		} else if filter(task) {
			preemptees = append(preemptees, task.Clone())
		}
	}

	klog.V(3).Infof("all preemptees: %v", preemptees)

	allVictims := ssn.Preemptable(preemptor, preemptees)
	metrics.UpdatePreemptionVictimsCount(len(allVictims))

	if err := util.ValidateVictims(preemptor, nodeInfo, allVictims); err != nil {
		klog.V(3).Infof("No validated victims on Node <%s>: %v", nodeInfo.Name, err)
		return nil, api.AsStatus(fmt.Errorf("no validated victims on Node <%s>: %v", nodeInfo.Name, err))
	}

	klog.V(3).Infof("allVictims: %v", allVictims)

	// Sort potentialVictims by pod priority from high to low, which ensures to
	// reprieve higher priority pods first.
	sort.Slice(allVictims, func(i, j int) bool { return k8sutil.MoreImportantPod(allVictims[i].Pod, allVictims[j].Pod) })

	victimsQueue := ssn.BuildVictimsPriorityQueue(allVictims, preemptor)

	for !victimsQueue.Empty() {
		task := victimsQueue.Pop().(*api.TaskInfo)
		potentialVictims = append(potentialVictims, task)
		if err := removeTask(task); err != nil {
			return nil, api.AsStatus(err)
		}

		if ssn.SimulateAllocatableFn(ctx, state, currentQueue, preemptor) && preemptor.InitResreq.LessEqual(nodeInfo.FutureIdle(), api.Zero) {
			if err := ssn.SimulatePredicateFn(ctx, state, preemptor, nodeInfo); err == nil {
				klog.V(3).Infof("Pod %v/%v can be scheduled on node %v after preempt %v/%v, stop evicting more pods", preemptor.Namespace, preemptor.Name, nodeInfo.Name, task.Namespace, task.Name)
				break
			}
		}
	}

	// No potential victims are found, and so we don't need to evaluate the node again since its state didn't change.
	if len(potentialVictims) == 0 {
		return nil, api.AsStatus(fmt.Errorf("no preemption victims found for incoming pod"))
	}

	// If the new pod does not fit after removing all potential victim pods,
	// we are almost done and this node is not suitable for preemption. The only
	// condition that we could check is if the "pod" is failing to schedule due to
	// inter-pod affinity to one or more victims, but we have decided not to
	// support this case for performance reasons. Having affinity to lower
	// priority pods is not a recommended configuration anyway.
	if err := ssn.SimulatePredicateFn(ctx, state, preemptor, nodeInfo); err != nil {
		return nil, api.AsStatus(fmt.Errorf("failed to predicate pod %s/%s on node %s: %v", preemptor.Namespace, preemptor.Name, nodeInfo.Name, err))
	}

	var victims []*api.TaskInfo

	klog.V(3).Infof("potentialVictims---: %v, nodeInfo: %v", potentialVictims, nodeInfo.Name)

	// TODO: consider the PDB violation here

	reprievePod := func(pi *api.TaskInfo) (bool, error) {
		if err := addTask(pi); err != nil {
			klog.ErrorS(err, "Failed to add task", "task", klog.KObj(pi.Pod))
			return false, err
		}

		var fits bool
		if ssn.SimulateAllocatableFn(ctx, state, currentQueue, preemptor) && preemptor.InitResreq.LessEqual(nodeInfo.FutureIdle(), api.Zero) {
			err := ssn.SimulatePredicateFn(ctx, state, preemptor, nodeInfo)
			fits = err == nil
		}

		if !fits {
			if err := removeTask(pi); err != nil {
				return false, err
			}
			victims = append(victims, pi)
			klog.V(3).Info("Pod is a potential preemption victim on node", "pod", klog.KObj(pi.Pod), "node", klog.KObj(nodeInfo.Node))
		}
		klog.Infof("reprievePod for task: %v, fits: %v", pi.Name, fits)
		return fits, nil
	}

	// Now we try to reprieve non-violating victims.
	for _, p := range potentialVictims {
		if _, err := reprievePod(p); err != nil {
			return nil, api.AsStatus(err)
		}
	}

	klog.Infof("victims: %v", victims)

	return victims, &api.Status{
		Reason: "",
	}
}

// SelectCandidate chooses the best-fit candidate from given <candidates> and return it.
// NOTE: This method is exported for easier testing in default preemption.
func SelectCandidate(candidates []*candidate, ssn *framework.Session, preemptor *api.TaskInfo, enableNodeOrderScore bool) *candidate {
	if len(candidates) == 0 {
		return nil
	}
	if len(candidates) == 1 {
		return candidates[0]
	}

	victimsMap := CandidatesToVictimsMap(candidates)
	scoreFuncs := OrderedScoreFuncs(ssn, preemptor, victimsMap, enableNodeOrderScore)
	candidateNode := pickOneNodeForPreemption(victimsMap, scoreFuncs)

	// Same as candidatesToVictimsMap, this logic is not applicable for out-of-tree
	// preemption plugins that exercise different candidates on the same nominated node.
	if victims := victimsMap[candidateNode]; victims != nil {
		return &candidate{
			victims: victims,
			name:    candidateNode,
		}
	}

	// We shouldn't reach here.
	klog.Error(errors.New("no candidate selected"), "Should not reach here", "candidates", candidates)
	// To not break the whole flow, return the first candidate.
	return candidates[0]
}

func CandidatesToVictimsMap(candidates []*candidate) map[string][]*api.TaskInfo {
	m := make(map[string][]*api.TaskInfo, len(candidates))
	for _, c := range candidates {
		m[c.Name()] = c.Victims()
	}
	return m
}

// OrderedScoreFuncs returns the ordered candidate-node scoring functions.
func OrderedScoreFuncs(
	ssn *framework.Session,
	preemptor *api.TaskInfo,
	nodesToVictims map[string][]*api.TaskInfo,
	enableNodeOrderScore bool,
) []func(node string) int64 {
	if !enableNodeOrderScore {
		return nil
	}

	defaults := defaultPreemptionScoreFuncs(nodesToVictims)
	nodeOrderScore := nodeOrderScoreFunc(ssn, preemptor, nodesToVictims)
	return append([]func(node string) int64{defaults[0], nodeOrderScore}, defaults[1:]...)
}

// defaultPreemptionScoreFuncs returns the default ordered candidate-node
// scoring functions.
func defaultPreemptionScoreFuncs(nodesToVictims map[string][]*api.TaskInfo) []func(node string) int64 {
	minHighestPriorityScoreFunc := func(node string) int64 {
		// highestPodPriority is the highest priority among the victims on this node.
		highestPodPriority := PodPriority(nodesToVictims[node][0].Pod)
		// The smaller the highestPodPriority, the higher the score.
		return -int64(highestPodPriority)
	}
	minSumPrioritiesScoreFunc := func(node string) int64 {
		var sumPriorities int64
		for _, task := range nodesToVictims[node] {
			// We add MaxInt32+1 to all priorities to make all of them >= 0. This is
			// needed so that a node with a few pods with negative priority is not
			// picked over a node with a smaller number of pods with the same negative
			// priority (and similar scenarios).
			sumPriorities += int64(PodPriority(task.Pod)) + int64(math.MaxInt32+1)
		}
		// The smaller the sumPriorities, the higher the score.
		return -sumPriorities
	}
	minNumPodsScoreFunc := func(node string) int64 {
		// The smaller the length of pods, the higher the score.
		return -int64(len(nodesToVictims[node]))
	}
	latestStartTimeScoreFunc := func(node string) int64 {
		// Get the earliest start time of all pods on the current node.
		earliestStartTimeOnNode := GetEarliestPodStartTime(nodesToVictims[node])
		if earliestStartTimeOnNode == nil {
			klog.Error(errors.New("earliestStartTime is nil for node"), "Should not reach here", "node", node)
			return int64(math.MinInt64)
		}
		// The bigger the earliestStartTimeOnNode, the higher the score.
		return earliestStartTimeOnNode.UnixNano()
	}

	return []func(string) int64{
		// A node with a minimum highest priority victim is preferable.
		minHighestPriorityScoreFunc,
		// A node with the smallest sum of priorities is preferable.
		minSumPrioritiesScoreFunc,
		// A node with the minimum number of pods is preferable.
		minNumPodsScoreFunc,
		// A node with the latest start time of all highest priority victims is preferable.
		latestStartTimeScoreFunc,
	}
}

// nodeOrderScoreFunc returns a candidate-node score based on the scheduler's
// node-order plugins after removing the candidate's victims.
func nodeOrderScoreFunc(
	ssn *framework.Session,
	preemptor *api.TaskInfo,
	nodesToVictims map[string][]*api.TaskInfo,
) func(node string) int64 {
	nodes := make([]*api.NodeInfo, 0, len(nodesToVictims))
	for name, victims := range nodesToVictims {
		node, found := ssn.Nodes[name]
		if !found {
			continue
		}

		nodeAfterEviction := node.Clone()
		for _, victim := range victims {
			if err := nodeAfterEviction.RemoveTask(victim); err != nil {
				klog.V(3).Infof("Failed to remove victim task <%s/%s> from node <%s> for node-order scoring: %v; using live node state",
					victim.Namespace, victim.Name, name, err)
				nodeAfterEviction = node
				break
			}
		}
		nodes = append(nodes, nodeAfterEviction)
	}

	nodeScores := util.PrioritizeNodes(
		preemptor,
		nodes,
		ssn.BatchNodeOrderFn,
		ssn.NodeOrderMapFn,
		ssn.NodeOrderReduceFn,
	)
	nodeOrderScores := make(map[string]int64, len(nodes))
	for score, scoredNodes := range nodeScores {
		for _, node := range scoredNodes {
			nodeOrderScores[node.Name] = int64(math.Round(score * nodeOrderScorePrecision))
		}
	}
	return func(node string) int64 {
		score, found := nodeOrderScores[node]
		if !found {
			return math.MinInt64
		}
		return score
	}
}

// pickOneNodeForPreemption chooses one node among the given nodes.
// It assumes pods in each map entry are ordered by decreasing priority.
// If the scoreFuncs is not empty, It picks a node based on score scoreFuncs returns.
// If the scoreFuncs is empty, it picks a node based on the following criteria:
// 1. A node with minimum highest priority victim is picked.
// 2. Ties are broken by sum of priorities of all victims.
// 3. If there are still ties, node with the minimum number of victims is picked.
// 4. If there are still ties, node with the latest start time of all highest priority victims is picked.
// 5. If there are still ties, the first such node is picked (sort of randomly).
// The 'minNodes1' and 'minNodes2' are being reused here to save the memory
// allocation and garbage collection time.
func pickOneNodeForPreemption(nodesToVictims map[string][]*api.TaskInfo, scoreFuncs []func(node string) int64) string {
	if len(nodesToVictims) == 0 {
		return ""
	}

	allCandidates := make([]string, 0, len(nodesToVictims))
	for node := range nodesToVictims {
		allCandidates = append(allCandidates, node)
	}

	if len(scoreFuncs) == 0 {
		scoreFuncs = defaultPreemptionScoreFuncs(nodesToVictims)
	}

	for _, f := range scoreFuncs {
		selectedNodes := []string{}
		maxScore := int64(math.MinInt64)
		for _, node := range allCandidates {
			score := f(node)
			if score > maxScore {
				maxScore = score
				selectedNodes = []string{}
			}
			if score == maxScore {
				selectedNodes = append(selectedNodes, node)
			}
		}
		if len(selectedNodes) == 1 {
			return selectedNodes[0]
		}
		allCandidates = selectedNodes
	}

	return allCandidates[0]
}

// GetEarliestPodStartTime returns the earliest start time of all pods that
// have the highest priority among all victims.
func GetEarliestPodStartTime(tasks []*api.TaskInfo) *metav1.Time {
	if len(tasks) == 0 {
		// should not reach here.
		klog.Background().Error(nil, "victims.Pods is empty. Should not reach here")
		return nil
	}

	earliestPodStartTime := GetPodStartTime(tasks[0].Pod)
	maxPriority := PodPriority(tasks[0].Pod)

	for _, task := range tasks {
		if podPriority := PodPriority(task.Pod); podPriority == maxPriority {
			if podStartTime := GetPodStartTime(task.Pod); podStartTime.Before(earliestPodStartTime) {
				earliestPodStartTime = podStartTime
			}
		} else if podPriority > maxPriority {
			maxPriority = podPriority
			earliestPodStartTime = GetPodStartTime(task.Pod)
		}
	}

	return earliestPodStartTime
}

// GetPodStartTime returns start time of the given pod or current timestamp
// if it hasn't started yet.
func GetPodStartTime(pod *v1.Pod) *metav1.Time {
	if pod.Status.StartTime != nil {
		return pod.Status.StartTime
	}
	// Assumed pods and bound pods that haven't started don't have a StartTime yet.
	return &metav1.Time{Time: time.Now()}
}
