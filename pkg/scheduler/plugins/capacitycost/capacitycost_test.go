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

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/util"
)

func node(name, capacityType string) *api.NodeInfo {
	labels := map[string]string{}
	if capacityType != "" {
		labels[DefaultNodeLabelKey] = capacityType
	}
	return api.NewNodeInfo(util.BuildNode(name, v1.ResourceList{
		"cpu":            resource.MustParse("8"),
		"nvidia.com/gpu": resource.MustParse("8"),
	}, labels))
}

func task(name string, gpus string) *api.TaskInfo {
	req := v1.ResourceList{"cpu": resource.MustParse("1")}
	if gpus != "" {
		req["nvidia.com/gpu"] = resource.MustParse(gpus)
	}
	return api.NewTaskInfo(util.BuildPod("default", name, "", v1.PodPending, req, "pg", nil, nil))
}

func TestRankerScoresCheapestHighest(t *testing.T) {
	r := NewRanker(DefaultNodeLabelKey, DefaultOrder, DefaultUnlabeledRank)
	if r.MaxRank() != 2 {
		t.Fatalf("maxRank = %d, want 2", r.MaxRank())
	}
	cases := map[string]float64{"reserved": 100, "spot": 50, "on-demand": 0, "": 100, "unknown": 100}
	for capacityType, want := range cases {
		if got := r.Score(node("n", capacityType)); got != want {
			t.Errorf("Score(%q) = %v, want %v", capacityType, got, want)
		}
	}
}

func TestRankerUnlabeledRankAndFlatOrder(t *testing.T) {
	r := NewRanker(DefaultNodeLabelKey, "reserved,spot", 1)
	if got := r.Rank(node("n", "")); got != 1 {
		t.Fatalf("unlabeled rank = %d, want 1", got)
	}
	if got := r.Score(node("n", "")); got != 0 {
		t.Fatalf("unlabeled score = %v, want 0", got)
	}
	flat := NewRanker(DefaultNodeLabelKey, "reserved", 0)
	if flat.MaxRank() != 0 || flat.Score(node("n", "reserved")) != 0 {
		t.Fatalf("flat order must score every node 0, got maxRank %d", flat.MaxRank())
	}
	dup := NewRanker(DefaultNodeLabelKey, "reserved, spot ,reserved,,", 0)
	if dup.MaxRank() != 1 || dup.Rank(node("n", "spot")) != 1 {
		t.Fatalf("expected duplicates and blanks ignored, got maxRank %d", dup.MaxRank())
	}
}

func TestPluginScoresOnlyGpuTasksAndAppliesWeight(t *testing.T) {
	plugin := New(framework.Arguments{WeightArg: 20}).(*capacityCostPlugin)
	score := func(task *api.TaskInfo, n *api.NodeInfo) float64 {
		if !plugin.scores(task) {
			return 0
		}
		return float64(plugin.weight) * plugin.ranker.Score(n)
	}
	if got := score(task("gpu", "1"), node("r", "reserved")); got != 2000 {
		t.Fatalf("reserved gpu score = %v, want 2000", got)
	}
	if got := score(task("gpu", "1"), node("s", "spot")); got != 1000 {
		t.Fatalf("spot gpu score = %v, want 1000", got)
	}
	if got := score(task("cpu", ""), node("r", "reserved")); got != 0 {
		t.Fatalf("cpu task must not be scored, got %v", got)
	}
	all := New(framework.Arguments{ResourceArg: ""}).(*capacityCostPlugin)
	if !all.scores(task("cpu", "")) {
		t.Fatalf("empty resource must score every task")
	}
}
