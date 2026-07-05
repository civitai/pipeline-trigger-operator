package v1alpha1

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// The PRIMARY original panic site: getGitRepositoryName/getBranchName/getCommitId
// did unchecked Object["status"].(map)["artifact"].(map)[key] assertions, which
// panic ("interface conversion: nil, not map[string]interface {}") the moment a
// not-Ready GitRepository (no artifact yet) is reconciled — crashlooping the whole
// operator. These assert the artifactField-guarded getters return "" without
// panicking. The process surviving the calls IS the assertion.
func TestGitRepositoryGetters_NotReady_NoPanic(t *testing.T) {
	cases := []unstructured.Unstructured{
		{Object: map[string]interface{}{}},                                                                       // no status
		{Object: map[string]interface{}{"status": map[string]interface{}{}}},                                     // status, no artifact
		{Object: map[string]interface{}{"status": map[string]interface{}{"artifact": map[string]interface{}{}}}}, // artifact, no path/revision
	}
	for i, u := range cases {
		if got := getGitRepositoryName(u); got != "" {
			t.Fatalf("case %d: getGitRepositoryName = %q, want \"\"", i, got)
		}
		if got := getBranchName(u); got != "" {
			t.Fatalf("case %d: getBranchName = %q, want \"\"", i, got)
		}
		if got := getCommitId(u); got != "" {
			t.Fatalf("case %d: getCommitId = %q, want \"\"", i, got)
		}
	}
}

// Parity: a Ready GitRepository still parses name/branch/commit from the artifact.
func TestGitRepositoryGetters_Ready_Parses(t *testing.T) {
	u := unstructured.Unstructured{Object: map[string]interface{}{
		"status": map[string]interface{}{
			"artifact": map[string]interface{}{
				"path":     "gitrepository/flux-system/vitrine/abc123.tar.gz",
				"revision": "main@sha1:abc123",
			},
		},
	}}
	if got := getGitRepositoryName(u); got != "vitrine" {
		t.Fatalf("getGitRepositoryName = %q, want \"vitrine\"", got)
	}
	if got := getBranchName(u); got != "main" {
		t.Fatalf("getBranchName = %q, want \"main\"", got)
	}
	if got := getCommitId(u); got != "abc123" {
		t.Fatalf("getCommitId = %q, want \"abc123\"", got)
	}
}
