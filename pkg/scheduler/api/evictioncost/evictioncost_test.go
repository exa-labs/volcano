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

package evictioncost

import (
	"encoding/json"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"volcano.sh/volcano/pkg/scheduler/api"
)

const gpu = v1.ResourceName("nvidia.com/gpu")

func TestVectors(t *testing.T) {
	raw, err := os.ReadFile("testdata/vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Vectors []struct {
			Name     string   `json:"name"`
			Now      float64  `json:"now"`
			Defaults Defaults `json:"defaults"`
			Input    Facts    `json:"input"`
			Expected float64  `json:"expected_gpu_seconds"`
		} `json:"vectors"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Vectors) == 0 {
		t.Fatal("no vectors")
	}
	for _, v := range doc.Vectors {
		if got := GPUSeconds(v.Input, v.Now, v.Defaults); math.Abs(got-v.Expected) > 1e-9 {
			t.Errorf("%s: got %v, want %v", v.Name, got, v.Expected)
		}
	}
}

func pod(name string, started time.Time, annotations map[string]string) *v1.Pod {
	grace := int64(120)
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", Annotations: annotations,
			CreationTimestamp: metav1.NewTime(started.Add(-time.Minute))},
		Spec:   v1.PodSpec{TerminationGracePeriodSeconds: &grace},
		Status: v1.PodStatus{StartTime: &metav1.Time{Time: started}},
	}
}

func task(job api.JobID, gpus float64, p *v1.Pod) *api.TaskInfo {
	return &api.TaskInfo{Job: job, Name: p.Name, Pod: p, TransactionContext: api.TransactionContext{Status: api.Running},
		Resreq: &api.Resource{ScalarResources: map[v1.ResourceName]float64{gpu: gpus * gpuMilli}}}
}

func TestParsePod(t *testing.T) {
	t0 := time.Date(2026, 10, 13, 19, 8, 12, 0, time.UTC)
	f := ParsePod(pod("a", t0, map[string]string{
		AnnotationResume:             "checkpoint",
		AnnotationStartupSeconds:     "600",
		AnnotationTerminationSeconds: "-5",
		AnnotationProductiveAt:       "2026-10-13T19:20:12Z",
		AnnotationLastCheckpoint:     "yesterday",
	}), float64(t0.Unix()+3600))
	if !f.Announced || f.Resume != ResumeCheckpoint || f.GraceSeconds != 120 || f.StartedAt != float64(t0.Unix()) {
		t.Fatalf("unexpected facts %+v", f)
	}
	if f.StartupSeconds == nil || *f.StartupSeconds != 600 || f.ProductiveAt == nil || *f.ProductiveAt != float64(t0.Unix()+720) {
		t.Fatalf("unexpected parsed values %+v", f)
	}
	if f.TerminationSeconds != nil || f.LastCheckpoint != nil {
		t.Fatalf("garbage must read as absent: %+v", f)
	}

	bare := pod("b", t0, map[string]string{AnnotationResume: "sometimes", "exa.ai/other": "1", AnnotationProductiveAt: "2026-10-13T19:00:00Z"})
	bare.Status.StartTime = nil
	bare.Spec.TerminationGracePeriodSeconds = nil
	f = ParsePod(bare, 0)
	if f.Announced || f.Resume != "" || f.GraceSeconds != 30 || f.StartedAt != float64(t0.Unix()-60) || f.ProductiveAt != nil {
		t.Fatalf("unannounced pod: unexpected facts %+v", f)
	}

	// No start and no creation time: priced as just started, not as
	// running since the epoch.
	bare.CreationTimestamp = metav1.Time{}
	now := float64(t0.Unix())
	if f = ParsePod(bare, now); f.StartedAt != now {
		t.Fatalf("unknown start: got started_at %v, want %v", f.StartedAt, now)
	}
	f.GPUs = 1
	if got := GPUSeconds(f, now, DefaultDefaults()); got != 30 {
		t.Fatalf("unknown start: got %v GPU-seconds, want the 30 s drain", got)
	}
}

func TestGangAggregation(t *testing.T) {
	t0 := time.Date(2026, 10, 13, 19, 8, 12, 0, time.UTC)
	members := []*api.TaskInfo{
		task("j", 4, pod("r0", t0, map[string]string{
			AnnotationResume: "stateless", AnnotationStartupSeconds: "100", AnnotationTerminationSeconds: "20",
			AnnotationProductiveAt: "2026-10-13T19:18:12Z", AnnotationLastCheckpoint: "2026-10-13T20:00:00Z"})),
		task("j", 4, pod("r1", t0.Add(time.Minute), map[string]string{
			AnnotationResume: "checkpoint", AnnotationStartupSeconds: "400",
			AnnotationProductiveAt: "2026-10-13T19:13:12Z", AnnotationLastCheckpoint: "2026-10-13T20:30:00Z"})),
		task("j", 0, pod("launcher", t0.Add(-time.Minute), nil)),
	}
	f, ok := Gang(members, gpu, 0)
	if !ok || !f.Announced || f.GPUs != 8 || f.StartedAt != float64(t0.Unix()-60) {
		t.Fatalf("unexpected gang facts %+v", f)
	}
	if f.Resume != ResumeCheckpoint || *f.StartupSeconds != 400 || *f.TerminationSeconds != 20 {
		t.Fatalf("declarations must take the most expensive value: %+v", f)
	}
	if *f.ProductiveAt != float64(t0.Unix()+300) || *f.LastCheckpoint != float64(t0.Add(82*time.Minute-12*time.Second).Unix()) {
		t.Fatalf("productive-at must be earliest and last-checkpoint latest: %+v", f)
	}
	if _, ok := Gang(nil, gpu, 0); ok {
		t.Fatal("empty gang must not be ok")
	}
}

func TestBookChargesEachGangOnce(t *testing.T) {
	t0 := time.Date(2026, 10, 13, 19, 8, 12, 0, time.UTC)
	now := t0.Add(time.Hour)
	a0, a1 := task("a", 1, pod("a0", t0, nil)), task("a", 1, pod("a1", t0, nil))
	b0 := task("b", 1, pod("b0", t0.Add(30*time.Minute), nil))
	jobs := map[api.JobID]*api.JobInfo{
		"a": {UID: "a", Tasks: api.TasksMap{"a0": a0, "a1": a1}},
		"b": {UID: "b", Tasks: api.TasksMap{"b0": b0}},
	}
	book := NewBook(jobs, gpu, DefaultDefaults(), now)
	// Unannounced: G x (D + S + age - S) = G x (D + age).
	if got := book.Job(a1).GPUSeconds; got != 2*(30+3600) {
		t.Fatalf("job a: got %v", got)
	}
	if book.Job(b0).Announced {
		t.Fatal("job b is unannounced")
	}
	if got, want := book.Selection([]*api.TaskInfo{a0, b0, a1}), 2*(30+3600.0)+(30+1800.0); got != want {
		t.Fatalf("selection: got %v, want %v", got, want)
	}
	orphan := task("gone", 2, pod("o", t0, nil))
	if got := book.Job(orphan).GPUSeconds; got != 2*(30+3600) {
		t.Fatalf("orphan priced alone: got %v", got)
	}
}

func TestCover(t *testing.T) {
	cpu := func(m float64) *api.Resource { return &api.Resource{MilliCPU: m} }
	ordered := []*api.TaskInfo{{Name: "x", Resreq: cpu(1000)}, {Name: "y", Resreq: cpu(1000)}, {Name: "z", Resreq: cpu(1000)}}
	if got := Cover(ordered, cpu(2500), cpu(500)); len(got) != 2 {
		t.Fatalf("got %d victims, want 2", len(got))
	}
	if got := Cover(ordered, cpu(500), cpu(500)); len(got) != 0 {
		t.Fatalf("got %d victims, want 0", len(got))
	}
}

func TestParseMode(t *testing.T) {
	for raw, want := range map[string]Mode{"": ModeOff, "off": ModeOff, "shadow": ModeShadow, "on": ModeOn, "yes": ModeOff} {
		if got := ParseMode(raw); got != want {
			t.Errorf("ParseMode(%q) = %q, want %q", raw, got, want)
		}
	}
}

// An unquoted on/off reaches the action as a YAML bool.
func TestModeOf(t *testing.T) {
	for _, c := range []struct {
		raw  interface{}
		want Mode
	}{{nil, ModeOff}, {true, ModeOn}, {false, ModeOff}, {"shadow", ModeShadow}, {"on", ModeOn}, {3, ModeOff}} {
		if got := ModeOf(c.raw); got != c.want {
			t.Errorf("ModeOf(%v) = %q, want %q", c.raw, got, c.want)
		}
	}
}

// Two orderings that evict different pods at the same cost are a tie, not
// a different decision.
func TestShadowIgnoresEqualCostTies(t *testing.T) {
	t0 := time.Date(2026, 10, 13, 19, 8, 12, 0, time.UTC)
	x, y := task("x", 1, pod("x", t0, nil)), task("y", 1, pod("y", t0, nil))
	x.UID, y.UID = "x", "y"
	book := NewBook(nil, gpu, DefaultDefaults(), t0.Add(time.Hour))
	need := &api.Resource{ScalarResources: map[v1.ResourceName]float64{gpu: gpuMilli}}
	r := book.Shadow([]*api.TaskInfo{x, y}, []*api.TaskInfo{y, x}, need, api.EmptyResource())
	if r.Differs() || r.Outcome() != "same" || r.Baseline.GPUSeconds == 0 {
		t.Fatalf("equal-cost covers: %+v", r)
	}
}

// Members already Releasing when the book opens add nothing; members the
// action marks Releasing afterwards still count.
func TestBookIgnoresMembersAlreadyReleasing(t *testing.T) {
	t0 := time.Date(2026, 10, 13, 19, 8, 12, 0, time.UTC)
	gone, kept := task("a", 1, pod("a0", t0, nil)), task("a", 1, pod("a1", t0, nil))
	gone.UID, kept.UID = "a0", "a1"
	gone.Status = api.Releasing
	jobs := map[api.JobID]*api.JobInfo{"a": {UID: "a", Tasks: api.TasksMap{"a0": gone, "a1": kept},
		TaskStatusIndex: map[api.TaskStatus]api.TasksMap{api.Releasing: {"a0": gone}, api.Running: {"a1": kept}}}}
	book := NewBook(jobs, gpu, DefaultDefaults(), t0.Add(time.Hour))
	kept.Status = api.Releasing
	if got := book.Job(kept).GPUSeconds; got != 1*(30+3600) {
		t.Fatalf("got %v, want only the member released by this action priced", got)
	}
}

// Shadow returns the victims each ordering would evict, and its log line
// names them per job with their price and source.
func TestShadowListsVictims(t *testing.T) {
	t0 := time.Date(2026, 10, 13, 19, 8, 12, 0, time.UTC)
	scratch := map[string]string{AnnotationResume: ResumeScratch}
	a0, a1 := task("a", 1, pod("a0", t0, scratch)), task("a", 1, pod("a1", t0, scratch))
	b0 := task("b", 1, pod("b0", t0, nil))
	preemptor := task("p", 1, pod("p0", t0, nil))
	a0.UID, a1.UID, b0.UID, preemptor.UID = "a0", "a1", "b0", "p0"
	jobs := map[api.JobID]*api.JobInfo{
		"a": {UID: "a", Namespace: "ns", Name: "train", Tasks: api.TasksMap{"a0": a0, "a1": a1}},
		"b": {UID: "b", Namespace: "ns", Name: "filler", Tasks: api.TasksMap{"b0": b0}},
		"p": {UID: "p", Namespace: "ns", Name: "urgent", Tasks: api.TasksMap{"p0": preemptor}},
	}
	book := NewBook(jobs, gpu, DefaultDefaults(), t0.Add(time.Hour))
	need := &api.Resource{ScalarResources: map[v1.ResourceName]float64{gpu: gpuMilli}}
	r := book.Shadow([]*api.TaskInfo{a0, b0, a1}, []*api.TaskInfo{b0, a0, a1}, need, api.EmptyResource())
	if len(r.Baseline.Victims) != 1 || r.Baseline.Victims[0] != a0 || len(r.CostAware.Victims) != 1 || r.CostAware.Victims[0] != b0 {
		t.Fatalf("victims: baseline %v, cost-aware %v", r.Baseline.Victims, r.CostAware.Victims)
	}
	// a: 2 GPUs x (30 + 3600); b, unannounced, is priced the same per GPU.
	if r.Baseline.GPUSeconds != 7260 || r.CostAware.GPUSeconds != 3630 || !r.Differs() {
		t.Fatalf("costs: %+v", r)
	}
	var lines []string
	saved := logLine
	logLine = func(line func() string) { lines = append(lines, line()) }
	defer func() { logLine = saved }()
	book.LogShadow("preempt", "normal", preemptor, r)
	want := "evictionCost shadow: action=preempt strategy=normal preemptor=ns/urgent outcome=different baseline=7260 costAware=3630" +
		" baselineVictims=[ns/train(7260, announced)] costAwareVictims=[ns/filler(3630, default)]"
	if len(lines) != 1 || lines[0] != want {
		t.Fatalf("got %q, want %q", lines, want)
	}
	r.Baseline.Node, r.CostAware.Node = "n1", "n2"
	if line := book.ShadowLine("preempt", "topology_aware", preemptor, r); !strings.Contains(line, " baselineNode=n1 costAwareNode=n2 ") {
		t.Fatalf("topology-aware line lacks the nodes: %q", line)
	}
	// Fractional costs print exactly, so a different decision never shows
	// equal numbers.
	r.CostAware.GPUSeconds = 7260.25
	if line := book.ShadowLine("preempt", "normal", preemptor, r); !strings.Contains(line, " baseline=7260 costAware=7260.25 ") {
		t.Fatalf("fractional cost rounded: %q", line)
	}
	if got := FormatPrice("ns/x", Price{GPUSeconds: 330.3}); got != "ns/x(330.3, default)" {
		t.Fatalf("FormatPrice: %q", got)
	}
}
