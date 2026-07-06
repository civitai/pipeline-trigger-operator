package v1alpha1

import (
	"reflect"
	"testing"

	pipelinev1alpha1 "github.com/jquad-group/pipeline-trigger-operator/api/v1alpha1"
	apis "github.com/jquad-group/pipeline-trigger-operator/pkg/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

// newPRTrigger builds a PipelineTrigger whose PullRequest PipelineRun template has
// a single dynamic param ("ID" -> "$.id"), seeded with the given status branches.
// This is the shape the real PullRequest path feeds into
// PullrequestSubscriber.CreatePipelineRunResource, which builds one run per branch
// in a single reconcile from this one shared template.
func newPRTrigger(branches map[string]pipelinev1alpha1.Branch) *pipelinev1alpha1.PipelineTrigger {
	var template unstructured.Unstructured
	template.SetAPIVersion("tekton.dev/v1")
	template.SetKind("PipelineRun")
	template.Object["spec"] = map[string]interface{}{
		"pipelineRef": map[string]interface{}{"name": "pr-check"},
		"params": []interface{}{
			map[string]interface{}{"name": "ID", "value": "$.id"},
		},
	}
	return &pipelinev1alpha1.PipelineTrigger{
		Spec: pipelinev1alpha1.PipelineTriggerSpec{
			Source:      pipelinev1alpha1.Source{Kind: "PullRequest", Name: "prs"},
			PipelineRun: template,
		},
		Status: pipelinev1alpha1.PipelineTriggerStatus{
			Branches: pipelinev1alpha1.Branches{Branches: branches},
		},
	}
}

func genName(t *testing.T, pr *unstructured.Unstructured) string {
	t.Helper()
	g, _, _ := unstructured.NestedString(pr.Object, "metadata", "generateName")
	return g
}

func prParamValue(t *testing.T, pr *unstructured.Unstructured, name string) string {
	t.Helper()
	params, found, err := unstructured.NestedSlice(pr.Object, "spec", "params")
	if err != nil || !found {
		t.Fatalf("spec.params not found or malformed (found=%v, err=%v)", found, err)
	}
	for _, p := range params {
		m, ok := p.(map[string]interface{})
		if !ok {
			continue
		}
		if m["name"] == name {
			v, _ := m["value"].(string)
			return v
		}
	}
	t.Fatalf("param %q not found", name)
	return ""
}

// TestPullrequestCreatePipelineRunResource_MultiBranchNoAliasing is the closest
// unit-level reproduction of the 2026-07-06 production create-storm. A PullRequest
// source with ≥2 build-eligible branches (no conditions) builds one run per branch
// in a single reconcile from ONE shared Spec.PipelineRun template. The pre-fix
// builder mutated and returned a pointer INTO that shared template, so every
// branch's run aliased a single backing object — client.Create() stamping a
// resourceVersion into the first run then made the second branch's create fail with
// "resourceVersion should not be set on objects to be created", looping the retry
// forever. Each run must be an independent deep copy with its own branch identity.
func TestPullrequestCreatePipelineRunResource_MultiBranchNoAliasing(t *testing.T) {
	pt := newPRTrigger(map[string]pipelinev1alpha1.Branch{
		"feat/aaa": {Name: "feat/aaa", Commit: "aaaaaaaa", Details: `{"id":111}`},
		"feat/bbb": {Name: "feat/bbb", Commit: "bbbbbbbb", Details: `{"id":222}`},
	})

	var sub PullrequestSubscriber
	prs := sub.CreatePipelineRunResource(pt, runtime.NewScheme())

	// One run PER branch.
	if len(prs) != 2 {
		t.Fatalf("expected 2 runs (one per build-eligible branch), got %d", len(prs))
	}

	// Distinct pointers AND distinct backing objects — neither aliases the other
	// nor the shared template.
	if prs[0] == prs[1] {
		t.Fatal("the two branch runs are the same *Unstructured pointer (aliased template) — the storm bug")
	}
	if prs[0] == &pt.Spec.PipelineRun || prs[1] == &pt.Spec.PipelineRun {
		t.Fatal("a branch run aliases the shared Spec.PipelineRun template pointer")
	}

	// Each run carries its OWN branch-derived generateName + branch labels; the two
	// runs together cover exactly the two branches (map order is nondeterministic).
	byName := map[string]*unstructured.Unstructured{}
	for _, pr := range prs {
		byName[genName(t, pr)] = pr
	}
	if len(byName) != 2 {
		t.Fatalf("expected two distinct branch-derived generateNames, got %v", keysOf(byName))
	}
	for wantGen, wantBranch := range map[string]string{"feat-aaa-": "feat-aaa", "feat-bbb-": "feat-bbb"} {
		pr, ok := byName[wantGen]
		if !ok {
			t.Fatalf("no run with generateName %q (got %v)", wantGen, keysOf(byName))
		}
		gotLabel, _, _ := unstructured.NestedString(pr.Object, "metadata", "labels", "pipeline.jquad.rocks/pr.branch.name")
		if gotLabel != wantBranch {
			t.Errorf("run %q branch-name label = %q, want %q", wantGen, gotLabel, wantBranch)
		}
	}

	// Simulate the poisoning: the API server stamps a resourceVersion into the
	// first run on Create(). It must NOT bleed onto the second run.
	prs[0].SetResourceVersion("12345")
	if rv := prs[1].GetResourceVersion(); rv != "" {
		t.Errorf("second run inherited the first run's resourceVersion %q — the runs are aliased (the storm bug)", rv)
	}

	// The shared template stays pristine so the next reconcile builds clean.
	if _, found, _ := unstructured.NestedString(pt.Spec.PipelineRun.Object, "metadata", "generateName"); found {
		t.Error("Spec.PipelineRun template was mutated (generateName leaked)")
	}
}

// TestPullrequestCreatePipelineRunResource_ParamsPerBranchNoBleed asserts dynamic
// params ($.field) resolve from EACH branch's own Details, with no cross-branch
// bleed. On the pre-fix shared-pointer builder both runs aliased one object, so the
// last branch's param value overwrote the first's — this test would see identical
// values and fail.
func TestPullrequestCreatePipelineRunResource_ParamsPerBranchNoBleed(t *testing.T) {
	pt := newPRTrigger(map[string]pipelinev1alpha1.Branch{
		"feat/aaa": {Name: "feat/aaa", Commit: "aaaaaaaa", Details: `{"id":111}`},
		"feat/bbb": {Name: "feat/bbb", Commit: "bbbbbbbb", Details: `{"id":222}`},
	})

	var sub PullrequestSubscriber
	prs := sub.CreatePipelineRunResource(pt, runtime.NewScheme())
	if len(prs) != 2 {
		t.Fatalf("expected 2 runs, got %d", len(prs))
	}

	// Collect (branch generateName -> resolved param) across ALL runs and require
	// BOTH branches' distinct values to be present. Asserting per-present-run isn't
	// enough: the pre-fix builder returns two aliases of one object AND resolves
	// $.id into the shared template on the first branch (so the second branch reads
	// the now-static value) — leaving a single genName carrying a single value. The
	// full-map equality below catches both the collapsed key set and the bled value.
	got := map[string]string{}
	for _, pr := range prs {
		got[genName(t, pr)] = prParamValue(t, pr, "ID")
	}
	want := map[string]string{"feat-aaa-": "111", "feat-bbb-": "222"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("per-branch resolved params = %v, want %v (cross-branch param bleed / aliased runs)", got, want)
	}
}

// TestPullrequestCreatePipelineRunResource_EligibilityGating asserts the
// len(Conditions)==0 gate: a branch that already has a condition is NOT rebuilt
// (no run produced, its condition untouched), while a conditionless branch is
// built and records its Unknown/Started condition on success.
func TestPullrequestCreatePipelineRunResource_EligibilityGating(t *testing.T) {
	existing := metav1.Condition{
		Type:               apis.ReconcileSuccess,
		Status:             metav1.ConditionTrue,
		Reason:             apis.ReconcileSuccessReason,
		Message:            "already built",
		LastTransitionTime: metav1.Now(),
	}
	pt := newPRTrigger(map[string]pipelinev1alpha1.Branch{
		"feat/done": {Name: "feat/done", Commit: "dddddddd", Details: `{"id":1}`, Conditions: []metav1.Condition{existing}},
		"feat/new":  {Name: "feat/new", Commit: "nnnnnnnn", Details: `{"id":2}`},
	})

	var sub PullrequestSubscriber
	prs := sub.CreatePipelineRunResource(pt, runtime.NewScheme())

	// Only the conditionless branch is built.
	if len(prs) != 1 {
		t.Fatalf("expected exactly 1 run (only the conditionless branch), got %d", len(prs))
	}
	if g := genName(t, prs[0]); g != "feat-new-" {
		t.Errorf("built run generateName = %q, want %q (built the wrong branch)", g, "feat-new-")
	}

	// The eligible branch records its Unknown/Started condition on success.
	newBranch := pt.Status.Branches.Branches["feat/new"]
	cond, found := newBranch.GetCondition(apis.ReconcileUnknown)
	if !found {
		t.Fatalf("eligible branch did not record a %q condition after a successful build", apis.ReconcileUnknown)
	}
	if cond.Status != metav1.ConditionUnknown {
		t.Errorf("eligible branch condition status = %q, want %q", cond.Status, metav1.ConditionUnknown)
	}

	// The already-conditioned branch is left untouched (not rebuilt).
	doneBranch := pt.Status.Branches.Branches["feat/done"]
	if len(doneBranch.Conditions) != 1 || doneBranch.Conditions[0].Type != apis.ReconcileSuccess {
		t.Errorf("already-conditioned branch was mutated: conditions = %+v, want the single original %q condition",
			doneBranch.Conditions, apis.ReconcileSuccess)
	}
}

func keysOf(m map[string]*unstructured.Unstructured) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
