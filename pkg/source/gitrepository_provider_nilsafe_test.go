package v1alpha1

import (
	"testing"

	pipelinev1alpha1 "github.com/jquad-group/pipeline-trigger-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
