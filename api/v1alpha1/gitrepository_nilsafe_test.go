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

// --- extended not-Ready / malformed coverage (ported from the nilsafe coverage
// work, PR #11) -------------------------------------------------------------
//
// artifactField is the low-level guard the three getters share; these exercise
// every degenerate .status / .status.artifact shape directly, plus the
// position-out-of-bounds guards for a Ready-but-malformed artifact, and the
// aggregate GetGitRepository parse path.

func TestGitRepositoryArtifactField_NotReadyShapes_ReturnEmpty(t *testing.T) {
	cases := []struct {
		name string
		obj  map[string]interface{}
	}{
		{"nil object", nil},
		{"empty object", map[string]interface{}{}},
		{"status not a map", map[string]interface{}{"status": "not-a-map"}},
		{"status empty (no artifact)", map[string]interface{}{"status": map[string]interface{}{}}},
		{"artifact not a map", map[string]interface{}{"status": map[string]interface{}{"artifact": "nope"}}},
		{"artifact present but key missing", map[string]interface{}{
			"status": map[string]interface{}{"artifact": map[string]interface{}{"other": "x"}},
		}},
		{"artifact key explicitly nil", map[string]interface{}{
			"status": map[string]interface{}{"artifact": map[string]interface{}{"path": nil}},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u := unstructured.Unstructured{Object: tc.obj}
			if got := artifactField(u, "path"); got != "" {
				t.Fatalf("artifactField(path) = %q, want \"\"", got)
			}
			if got := artifactField(u, "revision"); got != "" {
				t.Fatalf("artifactField(revision) = %q, want \"\"", got)
			}
		})
	}
}

// The three getters must survive every not-Ready shape (they call artifactField,
// which returns ""), AND the position-out-of-bounds guards must fire for a Ready
// source whose artifact strings don't split into enough parts. The process
// surviving without a panic IS part of the assertion.
func TestGitRepositoryGetters_NotReadyAndMalformed_ReturnEmpty(t *testing.T) {
	cases := []struct {
		name string
		obj  map[string]interface{}
		// getBranchName only returns "" on the not-Ready path (branchNamePosition
		// is 0, so a non-empty revision always yields parts[0]); the malformed case
		// still returns the raw revision string, so only assert it when empty.
		wantBranchEmpty bool
	}{
		{
			name:            "not-Ready (no artifact)",
			obj:             map[string]interface{}{"status": map[string]interface{}{}},
			wantBranchEmpty: true,
		},
		{
			// path splits into fewer than gitRepositoryNamePosition(=2)+1 parts;
			// revision has no commitId delimiter (":") -> commit position(=1) OOB.
			name: "malformed short artifact strings",
			obj: map[string]interface{}{
				"status": map[string]interface{}{
					"artifact": map[string]interface{}{
						"path":     "foo/bar",
						"revision": "main-no-commit-delim",
					},
				},
			},
			wantBranchEmpty: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u := unstructured.Unstructured{Object: tc.obj}
			if got := getGitRepositoryName(u); got != "" {
				t.Errorf("getGitRepositoryName = %q, want \"\"", got)
			}
			if got := getCommitId(u); got != "" {
				t.Errorf("getCommitId = %q, want \"\"", got)
			}
			if tc.wantBranchEmpty {
				if got := getBranchName(u); got != "" {
					t.Errorf("getBranchName = %q, want \"\"", got)
				}
			}
		})
	}
}

// Parity: a fully-Ready GitRepository still parses name/branch/commit through the
// aggregate GetGitRepository. This anchors the guards above against over-eager
// emptiness.
func TestGitRepositoryGetGitRepository_Ready_Parses(t *testing.T) {
	u := unstructured.Unstructured{Object: map[string]interface{}{
		"status": map[string]interface{}{
			"artifact": map[string]interface{}{
				"path":     "gitrepository/flux-system/vitrine/abc.tar.gz",
				"revision": "main@sha1:deadbeef",
			},
		},
	}}
	var gr GitRepository
	gr.GetGitRepository(u)
	if gr.RepositoryName != "vitrine" || gr.BranchName != "main" || gr.CommitId != "deadbeef" {
		t.Fatalf("GetGitRepository parsed unexpectedly: %+v", gr)
	}
}
