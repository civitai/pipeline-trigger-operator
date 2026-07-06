package v1alpha1

import (
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// These tests guard the create-storm root cause (2026-07-06): both PipelineRun
// builders MUST build each run on a DeepCopy of Spec.PipelineRun and never
// mutate or alias the shared template. If the builder aliased the template,
// (a) every per-branch run in one reconcile would share one backing object and
// (b) client.Create() stamping resourceVersion/uid/name back into it would make
// the shared template un-createable, producing an unbounded create storm that
// OOM-crashlooped the manager.

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
