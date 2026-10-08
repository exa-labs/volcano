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

// This file covers how a gang that cannot fit in one scheduling session
// interacts with smaller jobs competing for the same free nodes.
//
// A gang whose min member exceeds what currently fits has its partial
// placements rolled back by allocate (stmt.Discard), so the nodes it almost
// filled are handed to whatever job comes next; on a busy cluster a wide gang
// therefore never accumulates nodes that free up one at a time. A
// JobPipelinedFn that permits the job suppresses that rollback and the partial
// placements hold the nodes for the rest of the session. The sla plugin
// provides such a permit once the job is older than its waiting time, but only
// from a tier ahead of gang: JobPipelined stops at the first rejecting vote,
// and gang rejects any job below its min member.

package allocate

import (
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	schedulingv1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/gang"
	"volcano.sh/volcano/pkg/scheduler/plugins/nodeorder"
	"volcano.sh/volcano/pkg/scheduler/plugins/predicates"
	"volcano.sh/volcano/pkg/scheduler/plugins/priority"
	"volcano.sh/volcano/pkg/scheduler/plugins/sla"
	"volcano.sh/volcano/pkg/scheduler/uthelper"
	"volcano.sh/volcano/pkg/scheduler/util"
)

const (
	slaTestNamespace = "gang"
	slaTestQueue     = "default"
	gangWaitingTime  = "30m"
)

// nodeResources is one node's allocatable, and taskResources is a request for
// all of it, so one task occupies one node and a four member gang needs four
// free nodes.
func nodeResources() v1.ResourceList {
	return api.BuildResourceList("8", "8Gi", api.ScalarResource{Name: "pods", Value: "10"})
}

func taskResources() v1.ResourceList {
	return api.BuildResourceList("8", "8Gi")
}

// slaTestTiers returns the tier layout under test. slaTier selects where the
// sla plugin goes: "" omits it, "first" puts it ahead of gang, "with-gang" puts
// it in gang's own tier.
func slaTestTiers(slaTier string) []conf.Tier {
	trueValue := true
	slaPlugin := conf.PluginOption{
		Name:                sla.PluginName,
		EnabledJobOrder:     &trueValue,
		EnabledJobPipelined: &trueValue,
		EnabledJobEnqueued:  &trueValue,
	}
	gangTier := conf.Tier{
		Plugins: []conf.PluginOption{
			{
				Name:            priority.PluginName,
				EnabledJobOrder: &trueValue,
			},
			{
				Name:                gang.PluginName,
				EnabledJobOrder:     &trueValue,
				EnabledJobReady:     &trueValue,
				EnabledJobPipelined: &trueValue,
				EnabledJobStarving:  &trueValue,
			},
		},
	}
	predicateTier := conf.Tier{
		Plugins: []conf.PluginOption{
			{
				Name:             predicates.PluginName,
				EnabledPredicate: &trueValue,
			},
			{
				Name:             nodeorder.PluginName,
				EnabledNodeOrder: &trueValue,
			},
		},
	}

	switch slaTier {
	case "first":
		return []conf.Tier{{Plugins: []conf.PluginOption{slaPlugin}}, gangTier, predicateTier}
	case "with-gang":
		gangTier.Plugins = append(gangTier.Plugins, slaPlugin)
		return []conf.Tier{gangTier, predicateTier}
	default:
		return []conf.Tier{gangTier, predicateTier}
	}
}

// TestAllocateGangReservation schedules a four member gang against a cluster
// with only two free nodes, plus a single pod job that fits one of them. The
// gang can never become ready in this session; what differs per case is
// whether its two placements survive to keep the single pod job out.
func TestAllocateGangReservation(t *testing.T) {
	plugins := map[string]framework.PluginBuilder{
		gang.PluginName:       gang.New,
		priority.PluginName:   priority.New,
		predicates.PluginName: predicates.New,
		nodeorder.PluginName:  nodeorder.New,
		sla.PluginName:        sla.New,
	}

	// The gang is old enough to have passed its waiting time; the single pod
	// job has just arrived.
	gangCreation := metav1.NewTime(time.Now().Add(-2 * time.Hour))
	smallCreation := metav1.NewTime(time.Now().Add(-1 * time.Minute))

	// n1 and n2 are occupied by a running job, n3 and n4 are free. Every object
	// is rebuilt per case because the scheduler mutates the ones it schedules.
	newNodes := func() []*v1.Node {
		return []*v1.Node{
			util.BuildNode("n1", nodeResources(), nil),
			util.BuildNode("n2", nodeResources(), nil),
			util.BuildNode("n3", nodeResources(), nil),
			util.BuildNode("n4", nodeResources(), nil),
		}
	}

	newOccupiedPods := func() []*v1.Pod {
		return []*v1.Pod{
			util.BuildPod(slaTestNamespace, "occupied-0", "n1", v1.PodRunning, taskResources(), "pg-occupied", nil, nil),
			util.BuildPod(slaTestNamespace, "occupied-1", "n2", v1.PodRunning, taskResources(), "pg-occupied", nil, nil),
		}
	}

	newGangPods := func(annotations map[string]string) []*v1.Pod {
		pods := make([]*v1.Pod, 0, 4)
		for _, name := range []string{"gang-0", "gang-1", "gang-2", "gang-3"} {
			pod := util.BuildPod(slaTestNamespace, name, "", v1.PodPending, taskResources(), "pg-gang", nil, nil)
			for k, v := range annotations {
				pod.Annotations[k] = v
			}
			pods = append(pods, pod)
		}
		return pods
	}
	newSmallPod := func() *v1.Pod {
		return util.BuildPod(slaTestNamespace, "small-0", "", v1.PodPending, taskResources(), "pg-small", nil, nil)
	}

	newPodGroups := func(gangAnnotations map[string]string) []*schedulingv1.PodGroup {
		occupiedPG := util.BuildPodGroup("pg-occupied", slaTestNamespace, slaTestQueue, 2, nil, schedulingv1.PodGroupRunning)
		gangPG := util.BuildPodGroupWithAnno("pg-gang", slaTestNamespace, slaTestQueue, 4, nil, schedulingv1.PodGroupInqueue, gangAnnotations)
		gangPG.CreationTimestamp = gangCreation
		smallPG := util.BuildPodGroup("pg-small", slaTestNamespace, slaTestQueue, 1, nil, schedulingv1.PodGroupInqueue)
		smallPG.CreationTimestamp = smallCreation
		return []*schedulingv1.PodGroup{occupiedPG, gangPG, smallPG}
	}

	podGroupWaitingTime := map[string]string{schedulingv1.JobWaitingTime: gangWaitingTime}

	tests := []struct {
		name string
		// slaTier is where the sla plugin sits, see slaTestTiers.
		slaTier string
		// podGroupAnnotations and podAnnotations declare the gang's waiting
		// time on the PodGroup and on its pods respectively.
		podGroupAnnotations map[string]string
		podAnnotations      map[string]string
		// expectGangHolds is the number of nodes the gang keeps for itself,
		// and expectSmallBound whether the single pod job gets a node.
		expectGangHolds  int
		expectSmallBound bool
	}{
		{
			name:             "without sla the gang releases its partial placements to the single pod job",
			slaTier:          "",
			expectGangHolds:  0,
			expectSmallBound: true,
		},
		{
			name:                "sla ahead of gang holds the free nodes for the aged gang",
			slaTier:             "first",
			podGroupAnnotations: podGroupWaitingTime,
			expectGangHolds:     2,
			expectSmallBound:    false,
		},
		{
			name:            "waiting time declared on the pods is equivalent to declaring it on the podgroup",
			slaTier:         "first",
			podAnnotations:  map[string]string{schedulingv1.JobWaitingTime: gangWaitingTime},
			expectGangHolds: 2,
		},
		{
			name:                "sla in gang's own tier is voted down by gang",
			slaTier:             "with-gang",
			podGroupAnnotations: podGroupWaitingTime,
			expectGangHolds:     0,
			expectSmallBound:    true,
		},
		{
			name:             "a gang without a waiting time is unaffected by the sla plugin",
			slaTier:          "first",
			expectGangHolds:  0,
			expectSmallBound: true,
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expectBinds := 0
			if test.expectSmallBound {
				expectBinds = 1
			}
			common := uthelper.TestCommonStruct{
				Name:             test.name,
				Plugins:          plugins,
				Nodes:            newNodes(),
				Pods:             append(append(newGangPods(test.podAnnotations), newOccupiedPods()...), newSmallPod()),
				PodGroups:        newPodGroups(test.podGroupAnnotations),
				Queues:           []*schedulingv1.Queue{util.BuildQueue(slaTestQueue, 1, nil)},
				ExpectBindsNum:   expectBinds,
				MinimalBindCheck: true,
			}

			ssn := common.RegisterSession(slaTestTiers(test.slaTier), nil)
			defer common.Close()
			common.Run([]framework.Action{New()})
			if err := common.CheckAll(i); err != nil {
				t.Fatal(err)
			}

			gangJob := ssn.Jobs[api.JobID(slaTestNamespace+"/pg-gang")]
			if gangJob == nil {
				t.Fatalf("gang job not found in session")
			}
			if holds := int(gangJob.ReadyTaskNum() + gangJob.WaitingTaskNum()); holds != test.expectGangHolds {
				t.Errorf("gang holds %d nodes, want %d", holds, test.expectGangHolds)
			}
		})
	}
}
