package v1alpha1

import (
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// TestCreatePipelineRunResource_ReturnsIndependentCopy guards the single-source
// (GitRepository / ImagePolicy) half of the 2026-07-06 create-storm fix.
//
// CreatePipelineRunResource() must build on Spec.PipelineRun.DeepCopy() and never
// return a pointer aliasing the shared template. Otherwise client.Create() writing
// the API response (resourceVersion/uid/name) back into the returned object poisons
// the shared template, so the create-retry rebuilds a run that already carries a
// resourceVersion → "resourceVersion should not be set on objects to be created" →
// the retry loops forever. For each source kind this asserts:
//   - the returned pointer is NOT &Spec.PipelineRun,
//   - the template is left pristine (no generateName stamped, dynamic params NOT
//     resolved in place) so the next reconcile builds clean, and
//   - two successive builds (as a create-retry would do) are independent objects —
//     stamping a resourceVersion on one does not appear on the other.
func TestCreatePipelineRunResource_ReturnsIndependentCopy(t *testing.T) {
	newTemplate := func() unstructured.Unstructured {
		var tpl unstructured.Unstructured
		tpl.SetAPIVersion("tekton.dev/v1")
		tpl.SetKind("PipelineRun")
		tpl.Object["spec"] = map[string]interface{}{
			"pipelineRef": map[string]interface{}{"name": "build-and-push"},
			"params": []interface{}{
				map[string]interface{}{"name": "ID", "value": "$.id"},
			},
		}
		return tpl
	}

	cases := []struct {
		name         string
		newTrigger   func() *PipelineTrigger
		wantGenName  string
		wantResolved string
	}{
		{
			name: "GitRepository",
			newTrigger: func() *PipelineTrigger {
				return &PipelineTrigger{
					Spec: PipelineTriggerSpec{
						Source:      Source{Kind: "GitRepository", Name: "repo"},
						PipelineRun: newTemplate(),
					},
					Status: PipelineTriggerStatus{
						GitRepository: GitRepository{
							BranchName: "main",
							CommitId:   "abc123",
							Details:    `{"id":1163006807}`,
						},
					},
				}
			},
			wantGenName:  "main-",
			wantResolved: "1163006807",
		},
		{
			name: "ImagePolicy",
			newTrigger: func() *PipelineTrigger {
				return &PipelineTrigger{
					Spec: PipelineTriggerSpec{
						Source:      Source{Kind: "ImagePolicy", Name: "policy"},
						PipelineRun: newTemplate(),
					},
					Status: PipelineTriggerStatus{
						ImagePolicy: ImagePolicy{
							RepositoryName: "gcr.io",
							ImageName:      "repo",
							ImageVersion:   "v0.0.1",
							Details:        `{"id":42}`,
						},
					},
				}
			},
			wantGenName:  "repo-",
			wantResolved: "42",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pt := tc.newTrigger()

			run1 := pt.CreatePipelineRunResource()

			// 1. Not the shared-template pointer.
			if run1 == &pt.Spec.PipelineRun {
				t.Fatal("returned pointer aliases the shared Spec.PipelineRun template (the create-storm bug)")
			}

			// The built run resolves the dynamic param and gets a generateName.
			if got := paramValue(t, run1, "ID"); got != tc.wantResolved {
				t.Errorf("run param ID = %q, want %q", got, tc.wantResolved)
			}
			if got, _, _ := unstructured.NestedString(run1.Object, "metadata", "generateName"); got != tc.wantGenName {
				t.Errorf("run generateName = %q, want %q", got, tc.wantGenName)
			}

			// 2. Template left pristine — no generateName stamped, param still dynamic.
			if _, found, _ := unstructured.NestedString(pt.Spec.PipelineRun.Object, "metadata", "generateName"); found {
				t.Error("Spec.PipelineRun template was mutated: generateName leaked onto the shared template")
			}
			if got := paramValue(t, &pt.Spec.PipelineRun, "ID"); got != "$.id" {
				t.Errorf("template param ID was resolved in place to %q; the shared template must stay dynamic (%q)", got, "$.id")
			}

			// 3. Two successive builds (a create + its retry) are independent.
			run2 := pt.CreatePipelineRunResource()
			if run1 == run2 {
				t.Fatal("two calls to CreatePipelineRunResource returned the same pointer — a retry would rebuild the poisoned object")
			}
			run1.SetResourceVersion("999")
			if rv := run2.GetResourceVersion(); rv != "" {
				t.Errorf("second build inherited the first build's resourceVersion %q — the builds are aliased", rv)
			}
		})
	}
}

// TestCreatePipelineRunResource_MergesExistingTemplateLabels covers the
// additive-merge label branch: when the Spec.PipelineRun template ALREADY carries
// metadata.labels, the builder must merge the source-generated labels INTO a copy —
// preserving the template's own labels on the run while leaving the shared
// template's label map pristine (the DeepCopy must not let the merge bleed back).
func TestCreatePipelineRunResource_MergesExistingTemplateLabels(t *testing.T) {
	var tpl unstructured.Unstructured
	tpl.SetAPIVersion("tekton.dev/v1")
	tpl.SetKind("PipelineRun")
	tpl.SetLabels(map[string]string{"team": "platform"})
	tpl.Object["spec"] = map[string]interface{}{
		"pipelineRef": map[string]interface{}{"name": "build-and-push"},
		"params":      []interface{}{},
	}

	pt := &PipelineTrigger{
		Spec: PipelineTriggerSpec{
			Source:      Source{Kind: "GitRepository", Name: "repo"},
			PipelineRun: tpl,
		},
		Status: PipelineTriggerStatus{
			GitRepository: GitRepository{BranchName: "main", CommitId: "abc123", Details: `{"id":1}`},
		},
	}

	run := pt.CreatePipelineRunResource()

	// The run keeps the template's own label AND gains the source labels.
	runLabels, _, _ := unstructured.NestedStringMap(run.Object, "metadata", "labels")
	if runLabels["team"] != "platform" {
		t.Errorf("run dropped the template's own label: got labels %v", runLabels)
	}
	srcLabels := pt.Status.GitRepository.GenerateGitRepositoryLabelsAsHash()
	for k, v := range srcLabels {
		if runLabels[k] != v {
			t.Errorf("run missing merged source label %q=%q: got %v", k, v, runLabels)
		}
	}
	if len(runLabels) <= len(srcLabels) {
		t.Errorf("expected template label merged on top of %d source labels, got only %v", len(srcLabels), runLabels)
	}

	// The shared template's label map must be untouched (no source labels bled in).
	tplLabels, _, _ := unstructured.NestedStringMap(pt.Spec.PipelineRun.Object, "metadata", "labels")
	if len(tplLabels) != 1 || tplLabels["team"] != "platform" {
		t.Errorf("Spec.PipelineRun template labels were mutated by the merge: got %v, want {team:platform}", tplLabels)
	}
}

// paramValue returns the value of the named param in an unstructured PipelineRun's
// spec.params, failing the test if the params array is malformed.
func paramValue(t *testing.T, pr *unstructured.Unstructured, name string) string {
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
	t.Fatalf("param %q not found in spec.params", name)
	return ""
}

// --- create-storm / per-branch aliasing coverage (ported from the nilsafe
// coverage work, PR #11) --------------------------------------------------
//
// These complement the two tests above: they guard the per-branch builder's
// no-aliasing contract and the additive-merge-labels branch (including the
// ImagePolicy source kind) that the happy-path tests never exercise.

// newTemplateWithParams returns a PipelineRun template that already carries a
// spec.params array and (optionally) metadata.labels — the real trigger shape.
func newTemplateWithParams(withLabels bool) unstructured.Unstructured {
	var tpl unstructured.Unstructured
	tpl.SetAPIVersion("tekton.dev/v1")
	tpl.SetKind("PipelineRun")
	tpl.Object["spec"] = map[string]interface{}{
		"pipelineRef": map[string]interface{}{"name": "build-and-push"},
		"params": []interface{}{
			map[string]interface{}{"name": "COMMIT_SHA", "value": "$.commitId"},
		},
	}
	if withLabels {
		tpl.Object["metadata"] = map[string]interface{}{
			"labels": map[string]interface{}{"preexisting": "keep-me"},
		}
	}
	return tpl
}

// snapshot deep-copies the template's Object so we can assert it is byte-for-byte
// unchanged after a build (the aliasing guard).
func snapshot(u unstructured.Unstructured) map[string]interface{} {
	return u.DeepCopy().Object
}

// Single-source (GitRepository) builder must not mutate the shared template.
func TestCreatePipelineRunResource_DoesNotMutateSharedTemplate(t *testing.T) {
	pt := &PipelineTrigger{
		Spec: PipelineTriggerSpec{
			Source:      Source{APIVersion: "source.toolkit.fluxcd.io", Kind: "GitRepository", Name: "repo"},
			PipelineRun: newTemplateWithParams(false),
		},
		Status: PipelineTriggerStatus{
			GitRepository: GitRepository{BranchName: "main", CommitId: "abc123", Details: `{"commitId":"abc123"}`},
		},
	}
	before := snapshot(pt.Spec.PipelineRun)

	run := pt.CreatePipelineRunResource()

	if reflect.DeepEqual(run.Object, pt.Spec.PipelineRun.Object) {
		t.Fatal("built run is byte-equal to the template — expected a distinct, populated copy")
	}
	if !reflect.DeepEqual(pt.Spec.PipelineRun.Object, before) {
		t.Errorf("shared template was mutated by the builder.\n before: %#v\n after:  %#v", before, pt.Spec.PipelineRun.Object)
	}
	// Mutating the returned run must not reach back into the template (alias check).
	run.Object["spec"].(map[string]interface{})["params"] = []interface{}{}
	if !reflect.DeepEqual(pt.Spec.PipelineRun.Object, before) {
		t.Error("mutating the built run changed the shared template — the run aliases the template")
	}
}

// Per-branch builder: two branches built from ONE template must yield two
// distinct backing objects (not aliases of each other or the template), each
// carrying its own branch's params/labels, with the template left untouched.
func TestCreatePipelineRunResourceForBranch_PerBranch_NoAliasing(t *testing.T) {
	pt := &PipelineTrigger{
		Spec: PipelineTriggerSpec{
			Source:      Source{Kind: "PullRequest", Name: "repo"},
			PipelineRun: newTemplateWithParams(false),
		},
	}
	before := snapshot(pt.Spec.PipelineRun)

	branchA := Branch{Name: "feature/a", Commit: "aaa", Details: `{"commitId":"aaa"}`}
	branchB := Branch{Name: "feature/b", Commit: "bbb", Details: `{"commitId":"bbb"}`}

	runA := pt.CreatePipelineRunResourceForBranch(branchA, branchA.GenerateBranchLabelsAsHash())
	runB := pt.CreatePipelineRunResourceForBranch(branchB, branchB.GenerateBranchLabelsAsHash())

	// The two runs must be independent objects.
	if runA == runB {
		t.Fatal("both branches returned the same pointer")
	}
	labelsA := runA.Object["metadata"].(map[string]interface{})["labels"].(map[string]interface{})
	labelsB := runB.Object["metadata"].(map[string]interface{})["labels"].(map[string]interface{})
	if labelsA[pipelineTriggerLabelKey+"/pr.branch.commit"] != "aaa" {
		t.Errorf("runA has wrong commit label: %v", labelsA)
	}
	if labelsB[pipelineTriggerLabelKey+"/pr.branch.commit"] != "bbb" {
		t.Errorf("runB has wrong commit label: %v", labelsB)
	}
	// Mutating runA's labels must NOT bleed into runB (proves no shared backing map).
	labelsA["poison"] = "x"
	if _, leaked := labelsB["poison"]; leaked {
		t.Error("runB's labels aliased runA's — per-branch runs share a backing map")
	}
	// Template untouched.
	if !reflect.DeepEqual(pt.Spec.PipelineRun.Object, before) {
		t.Errorf("shared template mutated across per-branch builds:\n before: %#v\n after:  %#v", before, pt.Spec.PipelineRun.Object)
	}
}

// Additive-merge-labels path: when the template ALREADY carries metadata.labels,
// the builder must MERGE the generated labels in additively (keeping the
// preexisting ones) rather than replacing the map — and must not mutate the
// template's label map. Covers both builders' `labelsExist && labels != nil`
// branch, which the happy-path tests (label-less templates) never exercised.
func TestBuilders_AdditiveMergeLabels_PreserveExisting(t *testing.T) {
	t.Run("branch builder", func(t *testing.T) {
		pt := &PipelineTrigger{
			Spec: PipelineTriggerSpec{
				Source:      Source{Kind: "PullRequest", Name: "repo"},
				PipelineRun: newTemplateWithParams(true),
			},
		}
		branch := Branch{Name: "main", Commit: "abc", Details: `{"commitId":"abc"}`}
		run := pt.CreatePipelineRunResourceForBranch(branch, branch.GenerateBranchLabelsAsHash())

		labels := run.Object["metadata"].(map[string]interface{})["labels"].(map[string]interface{})
		if labels["preexisting"] != "keep-me" {
			t.Errorf("preexisting template label was dropped: %v", labels)
		}
		if labels[pipelineTriggerLabelKey+"/pr.branch.commit"] != "abc" {
			t.Errorf("generated branch label not merged in: %v", labels)
		}
		// Template's own label map must not have gained the branch labels.
		tplLabels := pt.Spec.PipelineRun.Object["metadata"].(map[string]interface{})["labels"].(map[string]interface{})
		if _, leaked := tplLabels[pipelineTriggerLabelKey+"/pr.branch.commit"]; leaked {
			t.Error("branch label leaked back into the shared template's label map")
		}
	})

	t.Run("single-source (GitRepository) builder", func(t *testing.T) {
		pt := &PipelineTrigger{
			Spec: PipelineTriggerSpec{
				Source:      Source{APIVersion: "source.toolkit.fluxcd.io", Kind: "GitRepository", Name: "repo"},
				PipelineRun: newTemplateWithParams(true),
			},
			Status: PipelineTriggerStatus{
				GitRepository: GitRepository{BranchName: "main", CommitId: "abc123", Details: `{"commitId":"abc123"}`},
			},
		}
		run := pt.CreatePipelineRunResource()

		labels := run.Object["metadata"].(map[string]interface{})["labels"].(map[string]interface{})
		if labels["preexisting"] != "keep-me" {
			t.Errorf("preexisting template label was dropped: %v", labels)
		}
		want := pt.Status.GitRepository.GenerateGitRepositoryLabelsAsHash()
		for k := range want {
			if _, ok := labels[k]; !ok {
				t.Errorf("generated git-repository label %q not merged in: %v", k, labels)
			}
		}
	})

	t.Run("single-source (ImagePolicy) builder", func(t *testing.T) {
		pt := &PipelineTrigger{
			Spec: PipelineTriggerSpec{
				Source:      Source{APIVersion: "image.toolkit.fluxcd.io", Kind: "ImagePolicy", Name: "policy"},
				PipelineRun: newTemplateWithParams(true),
			},
			Status: PipelineTriggerStatus{
				ImagePolicy: ImagePolicy{RepositoryName: "ghcr.io", ImageName: "foo", ImageVersion: "1.2.3", Details: `{"imageVersion":"1.2.3"}`},
			},
		}
		run := pt.CreatePipelineRunResource()

		labels := run.Object["metadata"].(map[string]interface{})["labels"].(map[string]interface{})
		if labels["preexisting"] != "keep-me" {
			t.Errorf("preexisting template label was dropped: %v", labels)
		}
		want := pt.Status.ImagePolicy.GenerateImagePolicyLabelsAsHash()
		for k := range want {
			if _, ok := labels[k]; !ok {
				t.Errorf("generated image-policy label %q not merged in: %v", k, labels)
			}
		}
	})
}
