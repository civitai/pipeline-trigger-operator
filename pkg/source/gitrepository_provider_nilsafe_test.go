package v1alpha1

import (
	"testing"

	pipelinev1alpha1 "github.com/jquad-group/pipeline-trigger-operator/api/v1alpha1"
	apis "github.com/jquad-group/pipeline-trigger-operator/pkg/status"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

// ptWithCommitParam builds a PipelineTrigger whose PipelineRun has a param
// value "$.commitId" (the shape every real GitRepository trigger uses).
func ptWithCommitParam(details string) *pipelinev1alpha1.PipelineTrigger {
	var pt pipelinev1alpha1.PipelineTrigger
	pt.Spec.PipelineRun = unstructured.Unstructured{Object: map[string]interface{}{
		"spec": map[string]interface{}{
			"params": []interface{}{
				map[string]interface{}{"name": "COMMIT_SHA", "value": "$.commitId"},
			},
		},
	}}
	pt.Status.GitRepository.Details = details
	return &pt
}

// notReadyFluxGitRepository is a GitRepository with NO .status.artifact — a
// semver source before its first matching tag (the vitrine case). This is the
// exact object that crashlooped the operator: the unguarded getters assert
// status.(map)["artifact"].(map)[key] and panic on the nil artifact.
func notReadyFluxGitRepository() unstructured.Unstructured {
	return unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "source.toolkit.fluxcd.io/v1",
		"kind":       "GitRepository",
		"metadata":   map[string]interface{}{"name": "vitrine", "namespace": "flux-system"},
		// no "status" at all — the harshest not-ready shape
	}}
}

// This is THE regression test for the crashloop this whole change fixes, driven
// through the REAL production path (not the inert Details=="" shortcut).
//
// Production NEVER yields Details=="": GetGitRepository runs the nil-safe getters
// (return "" for a not-ready source) and GenerateDetails marshals an all-omitempty
// struct, so a not-ready source yields the literal Details=="{}". This test builds
// that real "{}" and drives CreatePipelineRunResource end-to-end, asserting:
//   - no panic (the getters no longer assert on the nil artifact, and json.Exists
//     no longer ajson.Must-panics — here it cleanly reports "$.commitId is null"),
//   - no PipelineRun is fired (empty result), and
//   - a clean ReconcileError condition is recorded (NOT a requeue) — which also
//     proves evaluatePipelineParams returned a NON-nil error, since the else branch
//     nil-derefs err.Error() otherwise.
func TestCreatePipelineRunResource_GitRepositoryNotReady_RealPath_NoPanicNoFire(t *testing.T) {
	pt := ptWithCommitParam("") // Details overwritten below via the real path

	// Real path: getters (nil-safe) -> "" -> GenerateDetails -> "{}".
	pt.Status.GitRepository.GetGitRepository(notReadyFluxGitRepository())
	pt.Status.GitRepository.GenerateDetails()
	if got := pt.Status.GitRepository.Details; got != "{}" {
		t.Fatalf("expected a not-ready source to marshal to Details=%q (all-omitempty), got %q — the guard under test assumes the real \"{}\" value", "{}", got)
	}

	var subscriber GitrepositorySubscriber
	prs := subscriber.CreatePipelineRunResource(pt, runtime.NewScheme())

	if len(prs) != 0 {
		t.Fatalf("expected NO PipelineRun fired for a not-ready GitRepository, got %d", len(prs))
	}
	if _, found := pt.Status.GitRepository.GetCondition(apis.ReconcileError); !found {
		t.Fatalf("expected a ReconcileError condition for a not-ready source, found none (a requeue/nil-error path would have nil-derefed err.Error())")
	}
}

// A not-Ready GitRepository (semver source with no matching tag — the vitrine
// case) leaves Details empty. evaluatePipelineParamsForGitRepository must NOT
// panic (it previously reached json.Exists("") → ajson.Must panic → operator
// crashloop) and must return (false, non-nil-error) so CreatePipelineRunResource
// records a ReconcileError condition rather than nil-derefing on err.Error().
func TestEvaluatePipelineParamsForGitRepository_NotReady_NoPanic(t *testing.T) {
	ok, err := evaluatePipelineParamsForGitRepository(ptWithCommitParam(""))
	if ok {
		t.Fatalf("expected params-correctness=false for a not-Ready source, got true (would fire an empty PipelineRun)")
	}
	if err == nil {
		t.Fatalf("expected a non-nil error for a not-Ready source; nil would nil-deref err.Error() in CreatePipelineRunResource")
	}
}

// Parity: a Ready source with resolved Details still validates params + fires.
func TestEvaluatePipelineParamsForGitRepository_Ready_Ok(t *testing.T) {
	ok, err := evaluatePipelineParamsForGitRepository(
		ptWithCommitParam(`{"commitId":"abc123","branchName":"main","repositoryName":"vitrine"}`))
	if err != nil {
		t.Fatalf("expected no error for a Ready source, got %v", err)
	}
	if !ok {
		t.Fatalf("expected params-correctness=true for a Ready source with $.commitId resolvable")
	}
}
