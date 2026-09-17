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

package util

import (
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"volcano.sh/volcano/pkg/scheduler/api"
)

func TestParseGraceDuration(t *testing.T) {
	for _, tc := range []struct {
		raw     string
		want    time.Duration
		wantErr bool
	}{
		{raw: "10m", want: 10 * time.Minute},
		{raw: "600s", want: 10 * time.Minute},
		{raw: "0", want: 0},
		{raw: "-1m", wantErr: true},
		{raw: "banana", wantErr: true},
	} {
		got, err := ParseGraceDuration(tc.raw)
		if tc.wantErr {
			if err == nil {
				t.Errorf("ParseGraceDuration(%q) = %v, want error", tc.raw, got)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("ParseGraceDuration(%q) = %v, %v; want %v, nil", tc.raw, got, err, tc.want)
		}
	}
}

func pendingTask(name string, age time.Duration, annotation string, schGated bool) *api.TaskInfo {
	pod := &v1.Pod{}
	pod.Name = name
	pod.CreationTimestamp = metav1.NewTime(testNow.Add(-age))
	if annotation != "" {
		pod.Annotations = map[string]string{PreemptGraceAnnotation: annotation}
	}
	return &api.TaskInfo{
		UID:      api.TaskID(name),
		Name:     name,
		SchGated: schGated,
		Pod:      pod,
	}
}

var testNow = time.Now()

func jobWithTasks(tasks ...*api.TaskInfo) *api.JobInfo {
	job := &api.JobInfo{
		TaskStatusIndex: map[api.TaskStatus]api.TasksMap{
			api.Pending: {},
		},
	}
	for _, task := range tasks {
		job.TaskStatusIndex[api.Pending][task.UID] = task
	}
	return job
}

func TestPreemptGraceRemaining(t *testing.T) {
	for _, tc := range []struct {
		name         string
		defaultGrace time.Duration
		tasks        []*api.TaskInfo
		want         time.Duration
	}{
		{
			name:         "no pending tasks",
			defaultGrace: 10 * time.Minute,
			want:         0,
		},
		{
			name: "default zero and no annotation",
			tasks: []*api.TaskInfo{
				pendingTask("t1", 2*time.Minute, "", false),
			},
			want: 0,
		},
		{
			name:         "default grace minus pod age",
			defaultGrace: 10 * time.Minute,
			tasks: []*api.TaskInfo{
				pendingTask("t1", 2*time.Minute, "", false),
			},
			want: 8 * time.Minute,
		},
		{
			name:         "annotation overrides default",
			defaultGrace: 10 * time.Minute,
			tasks: []*api.TaskInfo{
				pendingTask("t1", 2*time.Minute, "5m", false),
			},
			want: 3 * time.Minute,
		},
		{
			name:         "invalid annotation falls back to default",
			defaultGrace: 10 * time.Minute,
			tasks: []*api.TaskInfo{
				pendingTask("t1", 2*time.Minute, "banana", false),
			},
			want: 8 * time.Minute,
		},
		{
			name:         "max grace with oldest pod",
			defaultGrace: 0,
			tasks: []*api.TaskInfo{
				pendingTask("t1", 1*time.Minute, "5m", false),
				pendingTask("t2", 3*time.Minute, "10m", false),
			},
			want: 7 * time.Minute,
		},
		{
			name:         "scheduling-gated pod ignored",
			defaultGrace: 10 * time.Minute,
			tasks: []*api.TaskInfo{
				pendingTask("t1", 2*time.Minute, "", true),
			},
			want: 0,
		},
		{
			name:         "grace elapsed",
			defaultGrace: 10 * time.Minute,
			tasks: []*api.TaskInfo{
				pendingTask("t1", 12*time.Minute, "", false),
			},
			want: 0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := PreemptGraceRemaining(jobWithTasks(tc.tasks...), tc.defaultGrace, testNow)
			if got != tc.want {
				t.Errorf("PreemptGraceRemaining() = %v, want %v", got, tc.want)
			}
		})
	}
}
