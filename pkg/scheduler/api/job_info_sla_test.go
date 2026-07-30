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

package api

import (
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"volcano.sh/apis/pkg/apis/scheduling"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
)

func TestJobInfoWaitingTime(t *testing.T) {
	podWithAnnotations := func(name string, annotations map[string]string) *TaskInfo {
		return NewTaskInfo(&v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   "default",
				Name:        name,
				Annotations: annotations,
			},
		})
	}
	podGroupWithAnnotations := func(annotations map[string]string) *PodGroup {
		return &PodGroup{
			PodGroup: scheduling.PodGroup{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:   "default",
					Name:        "pg",
					Annotations: annotations,
				},
			},
		}
	}

	hour := time.Hour
	minute := time.Minute

	testCases := []struct {
		name                string
		podGroupAnnotations map[string]string
		podAnnotations      map[string]string
		// podGroupLast adds the pods before the podgroup, as happens when a
		// job's pods reach the cache first.
		podGroupLast bool
		expected     *time.Duration
	}{
		{
			name:     "no waiting time anywhere",
			expected: nil,
		},
		{
			name:                "prefixed podgroup annotation",
			podGroupAnnotations: map[string]string{schedulingv1beta1.JobWaitingTime: "1h"},
			expected:            &hour,
		},
		{
			name:                "bare podgroup annotation",
			podGroupAnnotations: map[string]string{JobWaitingTime: "1h"},
			expected:            &hour,
		},
		{
			name:                "unparseable podgroup annotation is ignored",
			podGroupAnnotations: map[string]string{schedulingv1beta1.JobWaitingTime: "soon"},
			expected:            nil,
		},
		{
			name:                "non positive podgroup annotation is ignored",
			podGroupAnnotations: map[string]string{schedulingv1beta1.JobWaitingTime: "0s"},
			expected:            nil,
		},
		{
			name:           "pod annotation applies when the podgroup has none",
			podAnnotations: map[string]string{schedulingv1beta1.JobWaitingTime: "1m"},
			expected:       &minute,
		},
		{
			name:           "pod annotation applies whichever order the objects arrive in",
			podAnnotations: map[string]string{schedulingv1beta1.JobWaitingTime: "1m"},
			podGroupLast:   true,
			expected:       &minute,
		},
		{
			name:                "podgroup annotation wins over the pods",
			podGroupAnnotations: map[string]string{schedulingv1beta1.JobWaitingTime: "1h"},
			podAnnotations:      map[string]string{schedulingv1beta1.JobWaitingTime: "1m"},
			expected:            &hour,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			job := NewJobInfo("default/pg")
			addPods := func() {
				job.AddTaskInfo(podWithAnnotations("pod-0", testCase.podAnnotations))
				job.AddTaskInfo(podWithAnnotations("pod-1", testCase.podAnnotations))
			}

			if !testCase.podGroupLast {
				job.SetPodGroup(podGroupWithAnnotations(testCase.podGroupAnnotations))
				addPods()
			} else {
				addPods()
				job.SetPodGroup(podGroupWithAnnotations(testCase.podGroupAnnotations))
			}

			switch {
			case testCase.expected == nil && job.WaitingTime != nil:
				t.Errorf("waiting time is %v, want none", *job.WaitingTime)
			case testCase.expected != nil && job.WaitingTime == nil:
				t.Errorf("waiting time is unset, want %v", *testCase.expected)
			case testCase.expected != nil && *job.WaitingTime != *testCase.expected:
				t.Errorf("waiting time is %v, want %v", *job.WaitingTime, *testCase.expected)
			}
		})
	}
}
