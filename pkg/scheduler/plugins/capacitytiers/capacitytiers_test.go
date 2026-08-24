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

package capacitytiers

import (
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"

	"volcano.sh/apis/pkg/apis/scheduling"
	schedulingv1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/uthelper"
	"volcano.sh/volcano/pkg/scheduler/util"
)

func TestParseTierSpec(t *testing.T) {
	now := time.Date(2026, 7, 27, 0, 0, 0, 0, time.UTC)
	earlier := now.Add(-10 * time.Minute)

	tests := []struct {
		name        string
		annotations map[string]string
		wantNil     bool
		wantErr     bool
		wantTiers   []string
		wantBudget  time.Duration
		wantCurrent string
		wantSince   time.Time
	}{
		{
			name:        "no opt-in annotation",
			annotations: map[string]string{},
			wantNil:     true,
		},
		{
			name: "valid spec with state",
			annotations: map[string]string{
				TiersAnnotation:           "reserved, spot",
				FallbackSecondsAnnotation: "120",
				CurrentTierAnnotation:     "reserved",
				TierSinceAnnotation:       earlier.Format(time.RFC3339),
			},
			wantTiers:   []string{"reserved", "spot"},
			wantBudget:  120 * time.Second,
			wantCurrent: "reserved",
			wantSince:   earlier,
		},
		{
			name:        "empty tier list is an error",
			annotations: map[string]string{TiersAnnotation: " , "},
			wantErr:     true,
		},
		{
			name:        "duplicate tiers are an error",
			annotations: map[string]string{TiersAnnotation: "spot,spot"},
			wantErr:     true,
		},
		{
			name: "malformed budget falls back to default",
			annotations: map[string]string{
				TiersAnnotation:           "reserved,spot",
				FallbackSecondsAnnotation: "not-a-number",
			},
			wantTiers:  []string{"reserved", "spot"},
			wantBudget: defaultFallbackSeconds * time.Second,
			wantSince:  now,
		},
		{
			name: "stale current tier is discarded",
			annotations: map[string]string{
				TiersAnnotation:       "reserved,spot",
				CurrentTierAnnotation: "on-demand",
				TierSinceAnnotation:   earlier.Format(time.RFC3339),
			},
			wantTiers:  []string{"reserved", "spot"},
			wantBudget: defaultFallbackSeconds * time.Second,
			wantSince:  now,
		},
		{
			name: "malformed since resets the clock",
			annotations: map[string]string{
				TiersAnnotation:       "reserved,spot",
				CurrentTierAnnotation: "spot",
				TierSinceAnnotation:   "yesterday",
			},
			wantTiers:   []string{"reserved", "spot"},
			wantBudget:  defaultFallbackSeconds * time.Second,
			wantCurrent: "spot",
			wantSince:   now,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec, err := parseTierSpec(tt.annotations, now)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got spec %+v", spec)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantNil {
				if spec != nil {
					t.Fatalf("expected nil spec, got %+v", spec)
				}
				return
			}
			if len(spec.tiers) != len(tt.wantTiers) {
				t.Fatalf("tiers = %v, want %v", spec.tiers, tt.wantTiers)
			}
			for i := range tt.wantTiers {
				if spec.tiers[i] != tt.wantTiers[i] {
					t.Fatalf("tiers = %v, want %v", spec.tiers, tt.wantTiers)
				}
			}
			if spec.budget != tt.wantBudget {
				t.Errorf("budget = %s, want %s", spec.budget, tt.wantBudget)
			}
			if spec.current != tt.wantCurrent {
				t.Errorf("current = %q, want %q", spec.current, tt.wantCurrent)
			}
			if !spec.since.Equal(tt.wantSince) {
				t.Errorf("since = %s, want %s", spec.since, tt.wantSince)
			}
		})
	}
}

func TestResolveTier(t *testing.T) {
	now := time.Date(2026, 7, 27, 0, 0, 0, 0, time.UTC)
	budget := 5 * time.Minute

	tests := []struct {
		name        string
		spec        *tierSpec
		acquiring   bool
		wantTier    string
		wantChanged bool
	}{
		{
			name:        "fresh gang enters first tier",
			spec:        &tierSpec{tiers: []string{"reserved", "spot"}, budget: budget},
			acquiring:   true,
			wantTier:    "reserved",
			wantChanged: true,
		},
		{
			name: "within budget stays",
			spec: &tierSpec{
				tiers: []string{"reserved", "spot"}, budget: budget,
				current: "reserved", since: now.Add(-time.Minute),
			},
			acquiring: true,
			wantTier:  "reserved",
		},
		{
			name: "expired budget advances",
			spec: &tierSpec{
				tiers: []string{"reserved", "spot"}, budget: budget,
				current: "reserved", since: now.Add(-6 * time.Minute),
			},
			acquiring:   true,
			wantTier:    "spot",
			wantChanged: true,
		},
		{
			name: "last tier never advances",
			spec: &tierSpec{
				tiers: []string{"reserved", "spot"}, budget: budget,
				current: "spot", since: now.Add(-time.Hour),
			},
			acquiring: true,
			wantTier:  "spot",
		},
		{
			name: "running gang freezes the clock",
			spec: &tierSpec{
				tiers: []string{"reserved", "spot"}, budget: budget,
				current: "reserved", since: now.Add(-time.Hour),
			},
			acquiring: false,
			wantTier:  "reserved",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tier, changed := resolveTier(tt.spec, tt.acquiring, now)
			if tier != tt.wantTier || changed != tt.wantChanged {
				t.Errorf("resolveTier() = (%q, %v), want (%q, %v)", tier, changed, tt.wantTier, tt.wantChanged)
			}
		})
	}
}

// sessionPredicate opens a real session over the given podgroup/pods/nodes and
// returns the tier predicate verdict for each node keyed by node name.
func sessionPredicate(t *testing.T, pg *schedulingv1.PodGroup, pods []*v1.Pod, nodes []*v1.Node) map[string]error {
	t.Helper()
	test := uthelper.TestCommonStruct{
		Name:      t.Name(),
		Plugins:   map[string]framework.PluginBuilder{PluginName: New},
		PodGroups: []*schedulingv1.PodGroup{pg},
		Pods:      pods,
		Nodes:     nodes,
		Queues:    []*schedulingv1.Queue{util.BuildQueue("q1", 1, nil)},
	}
	trueValue := true
	tiers := []conf.Tier{{
		Plugins: []conf.PluginOption{{
			Name:             PluginName,
			EnabledPredicate: &trueValue,
		}},
	}}
	ssn := test.RegisterSession(tiers, nil)
	defer test.Close()

	verdicts := map[string]error{}
	for _, job := range ssn.Jobs {
		for _, task := range job.TaskStatusIndex[api.Pending] {
			for _, node := range ssn.Nodes {
				verdicts[node.Name] = ssn.PredicateFn(task, node)
			}
			break
		}
	}
	return verdicts
}

func TestTierPredicate(t *testing.T) {
	res := api.BuildResourceList("1", "1Gi")
	reservedNode := util.BuildNode("reserved-node", api.BuildResourceList("8", "16Gi"),
		map[string]string{defaultNodeLabelKey: "reserved"})
	spotNode := util.BuildNode("spot-node", api.BuildResourceList("8", "16Gi"),
		map[string]string{defaultNodeLabelKey: "spot"})
	unlabeledNode := util.BuildNode("unlabeled-node", api.BuildResourceList("8", "16Gi"),
		map[string]string{})

	buildPG := func(annos map[string]string) *schedulingv1.PodGroup {
		return util.BuildPodGroupWithAnno("pg1", "ns1", "q1", 1, nil,
			schedulingv1.PodGroupPhase(scheduling.PodGroupInqueue), annos)
	}
	pod := util.BuildPod("ns1", "p1", "", v1.PodPending, res, "pg1", nil, nil)

	t.Run("reserved tier admits only reserved nodes", func(t *testing.T) {
		verdicts := sessionPredicate(t, buildPG(map[string]string{
			TiersAnnotation: "reserved,spot",
		}), []*v1.Pod{pod}, []*v1.Node{reservedNode, spotNode, unlabeledNode})

		if verdicts["reserved-node"] != nil {
			t.Errorf("reserved node rejected: %v", verdicts["reserved-node"])
		}
		if verdicts["spot-node"] == nil {
			t.Error("spot node admitted during reserved tier")
		}
		if verdicts["unlabeled-node"] == nil {
			t.Error("unlabeled node admitted during reserved tier")
		}
	})

	t.Run("fallen-back gang admits only spot nodes", func(t *testing.T) {
		verdicts := sessionPredicate(t, buildPG(map[string]string{
			TiersAnnotation:       "reserved,spot",
			CurrentTierAnnotation: "spot",
			TierSinceAnnotation:   time.Now().UTC().Format(time.RFC3339),
		}), []*v1.Pod{pod}, []*v1.Node{reservedNode, spotNode})

		if verdicts["spot-node"] != nil {
			t.Errorf("spot node rejected: %v", verdicts["spot-node"])
		}
		if verdicts["reserved-node"] == nil {
			t.Error("reserved node admitted after spot fallback")
		}
	})

	t.Run("expired reserved budget advances to spot", func(t *testing.T) {
		verdicts := sessionPredicate(t, buildPG(map[string]string{
			TiersAnnotation:           "reserved,spot",
			FallbackSecondsAnnotation: "60",
			CurrentTierAnnotation:     "reserved",
			TierSinceAnnotation:       time.Now().Add(-2 * time.Minute).UTC().Format(time.RFC3339),
		}), []*v1.Pod{pod}, []*v1.Node{reservedNode, spotNode})

		if verdicts["spot-node"] != nil {
			t.Errorf("spot node rejected after fallback: %v", verdicts["spot-node"])
		}
		if verdicts["reserved-node"] == nil {
			t.Error("reserved node admitted after fallback to spot")
		}
	})

	t.Run("non-tiered gang is unrestricted", func(t *testing.T) {
		verdicts := sessionPredicate(t, buildPG(nil), []*v1.Pod{pod}, []*v1.Node{reservedNode, spotNode})
		for name, err := range verdicts {
			if err != nil {
				t.Errorf("node %s rejected for non-tiered gang: %v", name, err)
			}
		}
	})

	t.Run("malformed tier list leaves gang unrestricted", func(t *testing.T) {
		verdicts := sessionPredicate(t, buildPG(map[string]string{
			TiersAnnotation: "spot,spot",
		}), []*v1.Pod{pod}, []*v1.Node{reservedNode, spotNode})
		for name, err := range verdicts {
			if err != nil {
				t.Errorf("node %s rejected for malformed tier list: %v", name, err)
			}
		}
	})
}
