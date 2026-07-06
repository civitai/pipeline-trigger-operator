package v1alpha1

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// TestCreatePipelineRun_NoTemplateSpec_DoesNotPanic guards the nil-spec sharp edge
// flagged in the create-storm follow-up audit (2026-07-06).
//
// Both PipelineRun builders read the template's "spec" map and then assign
// spec["params"] = ... . If a PipelineTrigger's spec.pipelineRun template carries NO
// spec (or a wrong-typed one), the type assertion yields a nil map and the params
// assignment used to panic ("assignment to entry in nil map"). Because the reconcile
// panic is unrecovered and the create path retries, one misconfigured trigger could
// crashloop the whole manager — the same blast-radius class as the storm.
//
// After the fix the builder initializes a fresh spec map on the copy, so it never
// panics: it produces a params-only run that the Tekton API server rejects as a
// normal per-run create failure. This test asserts no panic AND that the built run
// carries a spec.params array, for both the branch path and a single-source path.
//
// Against the UNFIXED code this test panics (goroutine test failure); with the fix
// it passes.
func TestCreatePipelineRun_NoTemplateSpec_DoesNotPanic(t *testing.T) {
	// A PipelineRun template with metadata but deliberately NO spec key.
	newSpeclessTemplate := func() unstructured.Unstructured {
		var tpl unstructured.Unstructured
		tpl.SetAPIVersion("tekton.dev/v1")
		tpl.SetKind("PipelineRun")
		// no tpl.Object["spec"] — this is the whole point.
		return tpl
	}

	assertParamsArray := func(t *testing.T, run *unstructured.Unstructured) {
		t.Helper()
		if run == nil {
			t.Fatal("builder returned nil run")
		}
		spec, ok := run.Object["spec"].(map[string]interface{})
		if !ok {
			t.Fatalf("built run has no spec map: %#v", run.Object["spec"])
		}
		if _, ok := spec["params"].([]interface{}); !ok {
			t.Errorf("built run spec.params is not an array: %#v", spec["params"])
		}
	}

	t.Run("branch path", func(t *testing.T) {
		pt := &PipelineTrigger{
			Spec: PipelineTriggerSpec{
				Source:      Source{Kind: "PullRequest", Name: "repo"},
				PipelineRun: newSpeclessTemplate(),
			},
		}
		branch := Branch{Name: "main", Commit: "abc123", Details: `{"id":1}`}

		// Must not panic on the nil-map assignment.
		run := pt.CreatePipelineRunResourceForBranch(branch, map[string]string{"team": "platform"})
		assertParamsArray(t, run)
	})

	t.Run("single-source (GitRepository) path", func(t *testing.T) {
		pt := &PipelineTrigger{
			Spec: PipelineTriggerSpec{
				Source:      Source{Kind: "GitRepository", Name: "repo"},
				PipelineRun: newSpeclessTemplate(),
			},
			Status: PipelineTriggerStatus{
				GitRepository: GitRepository{
					BranchName: "main",
					CommitId:   "abc123",
					Details:    `{"id":1}`,
				},
			},
		}

		// Must not panic on the nil-map assignment.
		run := pt.CreatePipelineRunResource()
		assertParamsArray(t, run)
	})
}
