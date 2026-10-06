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

// Tests for price-aware scoring: how priced and unpriced candidates score
// together, what a unit is for a task, and that allocate follows prices only
// when the plugin is told to.

import (
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"

	"volcano.sh/volcano/pkg/scheduler/actions/allocate"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/conformance"
	"volcano.sh/volcano/pkg/scheduler/plugins/gang"
	"volcano.sh/volcano/pkg/scheduler/plugins/predicates"
	"volcano.sh/volcano/pkg/scheduler/plugins/priority"
	"volcano.sh/volcano/pkg/scheduler/plugins/proportion"
	"volcano.sh/volcano/pkg/scheduler/uthelper"
	"volcano.sh/volcano/pkg/scheduler/util"
)

// candidate builds a node of the given capacity type ("" for unlabeled)
// offering 64 CPUs and gpus GPUs, carrying the given annotations.
func candidate(name, capacityType, gpus string, annotations map[string]string) *api.NodeInfo {
	labels := map[string]string{}
	if capacityType != "" {
		labels[DefaultNodeLabelKey] = capacityType
	}
	node := util.BuildNode(name, v1.ResourceList{
		"cpu":            resource.MustParse("64"),
		"pods":           resource.MustParse("110"),
		"nvidia.com/gpu": resource.MustParse(gpus),
	}, labels)
	node.Annotations = annotations
	return api.NewNodeInfo(node)
}

// priceAware returns a plugin with price scoring on and the given extra
// arguments.
func priceAware(extra framework.Arguments) *capacityCostPlugin {
	arguments := framework.Arguments{WeightArg: 20, PriceAwareArg: true}
	for key, value := range extra {
		arguments[key] = value
	}
	return New(arguments).(*capacityCostPlugin)
}

func TestPriceArgumentsDefaultOff(t *testing.T) {
	plain := New(framework.Arguments{}).(*capacityCostPlugin)
	if plain.priceAware || plain.staleness != DefaultPriceStaleness {
		t.Fatalf("defaults: priceAware %v staleness %v", plain.priceAware, plain.staleness)
	}
	if got := plain.accelerators; len(got) != 4 || got[0] != "nvidia.com/gpu" {
		t.Fatalf("default accelerators = %v", got)
	}
	tuned := priceAware(framework.Arguments{PriceStalenessSecondsArg: 60, AcceleratorResourcesArg: " a.io/x, b.io/y ,,"})
	if !tuned.priceAware || tuned.staleness != time.Minute {
		t.Fatalf("configured: priceAware %v staleness %v", tuned.priceAware, tuned.staleness)
	}
	if got := tuned.accelerators; len(got) != 2 || got[0] != "a.io/x" || got[1] != "b.io/y" {
		t.Fatalf("configured accelerators = %v", got)
	}
}

// With no usable price on any candidate the price-aware scorer is the rank
// scorer: same scores, node for node.
func TestBatchScoresWithoutPricesAreTheRankScores(t *testing.T) {
	plugin := priceAware(nil)
	book := NewPriceBook(bookNow, plugin.staleness)
	nodes := []*api.NodeInfo{
		candidate("reserved", "reserved", "8", nil),
		candidate("spot", "spot", "8", priced("32", time.Hour)),
		candidate("on-demand", "on-demand", "8", priced("thirty-two", time.Minute)),
		candidate("unlabeled", "", "8", map[string]string{OfferingPriceAnnotation: "32"}),
	}
	scores := plugin.batchScores(book, task("gpu", "1"), nodes)
	for _, n := range nodes {
		if want := float64(plugin.weight) * plugin.ranker.Score(n); scores[n.Name] != want {
			t.Errorf("score(%s) = %v, want the rank score %v", n.Name, scores[n.Name], want)
		}
	}
	if scores["reserved"] != 2000 || scores["spot"] != 1000 || scores["on-demand"] != 0 || scores["unlabeled"] != 2000 {
		t.Fatalf("unexpected rank scores: %v", scores)
	}
}

// Priced candidates score the share of the dearest candidate's unit price
// they save, whatever their capacity type says.
func TestBatchScoresPricedNodesByUnitPrice(t *testing.T) {
	plugin := priceAware(nil)
	book := NewPriceBook(bookNow, plugin.staleness)
	fresh := func(price string) map[string]string { return priced(price, time.Minute) }
	cases := []struct {
		name  string
		nodes []*api.NodeInfo
		want  map[string]float64
	}{
		{
			name: "cheaper is higher, in proportion",
			nodes: []*api.NodeInfo{
				candidate("a", "spot", "8", fresh("8")),
				candidate("b", "spot", "8", fresh("16")),
				candidate("c", "spot", "8", fresh("32")),
			},
			want: map[string]float64{"a": 1500, "b": 1000, "c": 0},
		},
		{
			name: "price overrides the capacity rank",
			nodes: []*api.NodeInfo{
				candidate("spot", "spot", "8", fresh("32")),
				candidate("on-demand", "on-demand", "8", fresh("16")),
			},
			want: map[string]float64{"spot": 0, "on-demand": 1000},
		},
		{
			name: "unit is the price per requested GPU",
			nodes: []*api.NodeInfo{
				candidate("eight", "spot", "8", fresh("32")),
				candidate("four", "spot", "4", fresh("32")),
			},
			want: map[string]float64{"eight": 1000, "four": 0},
		},
		{
			name: "equal unit prices tie",
			nodes: []*api.NodeInfo{
				candidate("a", "reserved", "8", fresh("32")),
				candidate("b", "on-demand", "4", fresh("16")),
			},
			want: map[string]float64{"a": 0, "b": 0},
		},
		{
			name: "a marginally cheaper node scores marginally higher",
			nodes: []*api.NodeInfo{
				candidate("a", "spot", "8", fresh("31.68")),
				candidate("b", "spot", "8", fresh("32")),
			},
			want: map[string]float64{"a": 20, "b": 0},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			scores := plugin.batchScores(book, task("gpu", "1"), c.nodes)
			for name, want := range c.want {
				if got := scores[name]; got < want-1e-6 || got > want+1e-6 {
					t.Errorf("score(%s) = %v, want %v", name, got, want)
				}
			}
		})
	}
}

// Priced nodes compare among themselves by price, unpriced ones keep their
// rank score, and both sit on one scale.
func TestBatchScoresMixPricedAndUnpricedCandidates(t *testing.T) {
	plugin := priceAware(nil)
	book := NewPriceBook(bookNow, plugin.staleness)
	fresh := func(price string) map[string]string { return priced(price, time.Minute) }
	nodes := []*api.NodeInfo{
		candidate("owned", "", "8", nil),
		candidate("spot-cheap", "spot", "8", fresh("8")),
		candidate("spot-dear", "spot", "8", fresh("32")),
		candidate("spot-stale", "spot", "8", priced("1", time.Hour)),
		candidate("on-demand-malformed", "on-demand", "8", priced("free", time.Minute)),
	}
	scores := plugin.batchScores(book, task("gpu", "1"), nodes)
	want := map[string]float64{
		"owned":               2000, // unpriced, cheapest rank: ahead of anything paid for
		"spot-cheap":          1500, // priced: saves 75% of the dearest priced candidate
		"spot-dear":           0,    // priced: the dearest
		"spot-stale":          1000, // stale price ignored: spot's rank score
		"on-demand-malformed": 0,    // malformed price ignored: on-demand's rank score
	}
	for name, score := range want {
		if scores[name] != score {
			t.Errorf("score(%s) = %v, want %v", name, scores[name], score)
		}
	}

	// A lone priced candidate has no priced peer to be cheaper than.
	lone := plugin.batchScores(book, task("gpu", "1"), []*api.NodeInfo{
		candidate("priced", "reserved", "8", fresh("32")),
		candidate("unpriced", "spot", "8", nil),
	})
	if lone["priced"] != 0 || lone["unpriced"] != 1000 {
		t.Fatalf("lone priced candidate: %v", lone)
	}
}

func TestUnitResourceIsTheFirstRequestedAcceleratorElseCPU(t *testing.T) {
	plugin := priceAware(framework.Arguments{ResourceArg: "", AcceleratorResourcesArg: "example.com/npu,nvidia.com/gpu"})
	if got := plugin.unitResource(task("gpu", "1")); got != "nvidia.com/gpu" {
		t.Fatalf("gpu task unit = %s", got)
	}
	if got := plugin.unitResource(task("cpu", "")); got != v1.ResourceCPU {
		t.Fatalf("cpu task unit = %s", got)
	}
	both := api.NewTaskInfo(util.BuildPod("default", "both", "", v1.PodPending, v1.ResourceList{
		"cpu":             resource.MustParse("1"),
		"nvidia.com/gpu":  resource.MustParse("1"),
		"example.com/npu": resource.MustParse("2"),
	}, "pg", nil, nil))
	if got := plugin.unitResource(both); got != "example.com/npu" {
		t.Fatalf("task requesting both accelerators unit = %s, want the first configured", got)
	}

	// A CPU-only task is priced per CPU: the node with more CPUs for the
	// same price is the cheaper one, GPUs notwithstanding.
	book := NewPriceBook(bookNow, plugin.staleness)
	small := candidate("small", "spot", "8", priced("32", time.Minute))
	big := api.NewNodeInfo(util.BuildNode("big", v1.ResourceList{"cpu": resource.MustParse("128")}, nil))
	big.Node.Annotations = priced("32", time.Minute)
	scores := plugin.batchScores(book, task("cpu", ""), []*api.NodeInfo{small, big})
	if scores["big"] != 1000 || scores["small"] != 0 {
		t.Fatalf("cpu task scores = %v, want big 1000 small 0", scores)
	}
	// The same pair for a GPU task: only the node offering GPUs has a unit
	// price, the other keeps its rank score.
	scores = plugin.batchScores(book, task("gpu", "1"), []*api.NodeInfo{small, big})
	if scores["small"] != 0 || scores["big"] != 2000 {
		t.Fatalf("gpu task scores = %v, want small 0 (lone priced) big 2000 (rank)", scores)
	}
}

// Allocate scenarios: one pending GPU pod, a spot node at 4 per GPU-hour and
// an on-demand node at 2. The rank prefers spot; the prices prefer on-demand.
func allocateTiers(arguments map[string]interface{}) []conf.Tier {
	trueValue := true
	return []conf.Tier{{Plugins: []conf.PluginOption{
		{Name: conformance.PluginName, EnabledPreemptable: &trueValue},
		{Name: gang.PluginName, EnabledJobReady: &trueValue, EnabledJobPipelined: &trueValue, EnabledJobStarving: &trueValue},
		{Name: priority.PluginName, EnabledTaskOrder: &trueValue, EnabledJobOrder: &trueValue},
		{Name: PluginName, EnabledNodeOrder: &trueValue, Arguments: arguments},
		{Name: proportion.PluginName, EnabledOverused: &trueValue, EnabledAllocatable: &trueValue, EnabledQueueOrder: &trueValue},
		{Name: predicates.PluginName, EnabledPredicate: &trueValue},
	}}}
}

func allocateScenario(observedAgo time.Duration) *uthelper.TestCommonStruct {
	clusterNode := func(name, capacityType, price string) *v1.Node {
		node := util.BuildNode(name, api.BuildResourceList("64", "256G",
			api.ScalarResource{Name: "pods", Value: "110"},
			api.ScalarResource{Name: "nvidia.com/gpu", Value: "8"}),
			map[string]string{DefaultNodeLabelKey: capacityType})
		node.Annotations = map[string]string{
			OfferingPriceAnnotation:           price,
			OfferingPriceObservedAtAnnotation: time.Now().Add(-observedAgo).UTC().Format(time.RFC3339),
		}
		return node
	}
	return &uthelper.TestCommonStruct{
		Plugins: map[string]framework.PluginBuilder{
			conformance.PluginName: conformance.New,
			gang.PluginName:        gang.New,
			priority.PluginName:    priority.New,
			PluginName:             New,
			proportion.PluginName:  proportion.New,
			predicates.PluginName:  predicates.New,
		},
		Pods: []*v1.Pod{util.BuildPod("c1", "arrival", "", v1.PodPending,
			api.BuildResourceList("1", "1G", api.ScalarResource{Name: "nvidia.com/gpu", Value: "1"}), "pg-arrival", nil, nil)},
		PodGroups: []*schedulingv1beta1.PodGroup{
			util.BuildPodGroup("pg-arrival", "c1", "q1", 1, nil, schedulingv1beta1.PodGroupInqueue),
		},
		Nodes: []*v1.Node{
			clusterNode("spot", "spot", "32"),
			clusterNode("on-demand", "on-demand", "16"),
		},
		Queues:         []*schedulingv1beta1.Queue{util.BuildQueue("q1", 1, nil)},
		ExpectBindsNum: 1,
	}
}

func runAllocate(t *testing.T, test *uthelper.TestCommonStruct, arguments map[string]interface{}) {
	test.RegisterSession(allocateTiers(arguments), nil)
	defer test.Close()
	test.Run([]framework.Action{allocate.New()})
	if err := test.CheckAll(0); err != nil {
		t.Fatal(err)
	}
}

func TestAllocateFollowsRankUnlessPriceAware(t *testing.T) {
	test := allocateScenario(time.Minute)
	test.ExpectBindMap = map[string]string{"c1/arrival": "spot"}
	runAllocate(t, test, map[string]interface{}{WeightArg: 20})
}

func TestAllocatePriceAwareBindsToTheCheaperNode(t *testing.T) {
	test := allocateScenario(time.Minute)
	test.ExpectBindMap = map[string]string{"c1/arrival": "on-demand"}
	runAllocate(t, test, map[string]interface{}{WeightArg: 20, PriceAwareArg: true})
}

func TestAllocatePriceAwareFallsBackToRankOnStalePrices(t *testing.T) {
	test := allocateScenario(time.Hour)
	test.ExpectBindMap = map[string]string{"c1/arrival": "spot"}
	runAllocate(t, test, map[string]interface{}{WeightArg: 20, PriceAwareArg: true})
}

// With price scoring on, a flat capacity order no longer makes the plugin
// inert: prices alone decide.
func TestAllocatePriceAwareWithFlatOrder(t *testing.T) {
	test := allocateScenario(time.Minute)
	test.ExpectBindMap = map[string]string{"c1/arrival": "on-demand"}
	runAllocate(t, test, map[string]interface{}{WeightArg: 20, PriceAwareArg: true, OrderArg: ""})
}
