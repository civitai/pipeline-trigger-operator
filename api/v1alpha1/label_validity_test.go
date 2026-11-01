package v1alpha1

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/validation"
)

// Regression tests for the 2026-10-31 PipelineRun-create storm. A PR head
// branch "fix/bug-post-text-scan-raises-pg-posts-" (trailing dash) produced the
// label value "fix-bug-post-text-scan-raises-pg-posts-", which the API server
// rejects (a label value must be <= 63 chars, empty or start AND end with an
// alphanumeric). Every label the operator derives from external input must be
// valid. Validity is judged by apimachinery's own IsValidLabelValue — the same
// check the API server runs — never by a hand-written regex.

const incidentBranch = "fix/bug-post-text-scan-raises-pg-posts-"

// 70 chars, all valid label chars: invalid only because of its length.
var longBranch = "feature/" + strings.Repeat("abcdefghij", 6) + "xy"

func assertValidLabels(t *testing.T, what string, labels map[string]string) {
	t.Helper()
	for k, v := range labels {
		if errs := validation.IsValidLabelValue(v); len(errs) != 0 {
			t.Errorf("%s: label %s=%q is not a valid label value: %v", what, k, v, errs)
		}
	}
}

func TestBranchLabels_AreValidLabelValues(t *testing.T) {
	branches := []Branch{
		{Name: incidentBranch, Commit: "8932484a2017a3784608c2db429553a94f1e2f4b"},
		{Name: longBranch, Commit: "8932484a2017a3784608c2db429553a94f1e2f4b"},
		{Name: "-leading/and/trailing_", Commit: "abc"},
		{Name: "unicode/ünïcødé", Commit: "abc"},
		{Name: "///", Commit: "abc"},
	}
	for _, b := range branches {
		assertValidLabels(t, "branch "+b.Name, b.GenerateBranchLabelsAsHash())
	}
}

// The run actually sent to the API server (labels merged onto the template,
// generateName derived from the branch when the template sets none) must be
// valid too — not just the label map in isolation.
func TestBranchRun_LabelsAndGenerateNameAreValid(t *testing.T) {
	var template unstructured.Unstructured
	template.SetAPIVersion("tekton.dev/v1")
	template.SetKind("PipelineRun")
	template.Object["spec"] = map[string]interface{}{
		"pipelineRef": map[string]interface{}{"name": "pr-check"},
		"params":      []interface{}{},
	}
	pt := &PipelineTrigger{Spec: PipelineTriggerSpec{
		Source:      Source{Kind: "PullRequest", Name: "prs"},
		PipelineRun: template,
	}}
	for _, name := range []string{incidentBranch, longBranch, "Fix_Upper/Case."} {
		b := Branch{Name: name, Commit: "8932484a2017a3784608c2db429553a94f1e2f4b"}
		run := pt.CreatePipelineRunResourceForBranch(b, b.GenerateBranchLabelsAsHash())
		assertValidLabels(t, "run for "+name, run.GetLabels())
		// The API server appends 5 random chars to generateName.
		gen := run.GetGenerateName()
		if errs := validation.IsDNS1123Subdomain(gen + "abcde"); len(errs) != 0 {
			t.Errorf("run for %q: generateName %q yields an invalid name: %v", name, gen, errs)
		}
		if len(gen)+5 > validation.DNS1123LabelMaxLength {
			t.Errorf("run for %q: generateName %q too long for a 63-char name", name, gen)
		}
	}
}

func TestGitRepositoryLabels_AreValidLabelValues(t *testing.T) {
	for _, g := range []GitRepository{
		{BranchName: "feature/x", CommitId: "ae05cbcffffffffffffffffffffffffffffffff"},
		{BranchName: longBranch, CommitId: "ae05cbcffffffffffffffffffffffffffffffff"},
		{BranchName: "release-", CommitId: "ae05cbc"},
	} {
		assertValidLabels(t, "gitrepository "+g.BranchName, g.GenerateGitRepositoryLabelsAsHash())
	}
}

func TestImagePolicyLabels_AreValidLabelValues(t *testing.T) {
	for _, ip := range []ImagePolicy{
		{ImageName: "app", ImageVersion: "_underscore-start"},
		{ImageName: "app", ImageVersion: strings.Repeat("v1.2.3-rc", 14)}, // 126 chars, a legal tag
		{ImageName: "app-", ImageVersion: "1.0.0"},
	} {
		assertValidLabels(t, "imagepolicy "+ip.ImageName+":"+ip.ImageVersion, ip.GenerateImagePolicyLabelsAsHash())
	}
}

// Distinct branches must keep distinct labels after sanitising/truncation:
// SetCurrentPipelineRunName / CalculateCurrentState match runs to sources by
// label, so a collision would make one branch's run count as another's.
func TestBranchLabels_StayDistinctAfterSanitising(t *testing.T) {
	key := pipelineTriggerLabelKey + "/pr.branch.name"
	pairs := [][2]string{
		{longBranch, longBranch[:len(longBranch)-1] + "z"}, // differ only past char 63
		{incidentBranch, strings.TrimSuffix(incidentBranch, "-")},
		{"fix/a-", "fix/a_"},
	}
	for _, p := range pairs {
		a := (&Branch{Name: p[0], Commit: "c"}).GenerateBranchLabelsAsHash()[key]
		b := (&Branch{Name: p[1], Commit: "c"}).GenerateBranchLabelsAsHash()[key]
		if a == b {
			t.Errorf("branches %q and %q collide on label value %q", p[0], p[1], a)
		}
	}
}

// Invariant guard (green before AND after the fix): an already-valid value must
// be left exactly as it was, so PipelineRuns labelled before this change still
// match the reader side and are not re-created.
func TestLabels_ValidValuesUnchanged(t *testing.T) {
	b := Branch{Name: "feature/newlogin", Commit: "8932484a2017a3784608c2db429553a94f1e2f4b"}
	got := b.GenerateBranchLabelsAsHash()
	want := map[string]string{
		"pipeline.jquad.rocks/pr.branch.name":   "feature-newlogin",
		"pipeline.jquad.rocks/pr.branch.commit": "8932484a2017a3784608c2db429553a94f1e2f4b",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %q, want %q", k, got[k], v)
		}
	}
	g := GitRepository{BranchName: "main", CommitId: "ae05cbc"}
	if v := g.GenerateGitRepositoryLabelsAsHash()["pipeline.jquad.rocks/git.repository.branch.name"]; v != "main" {
		t.Errorf("git branch label = %q, want main", v)
	}
	ip := ImagePolicy{ImageName: "repo", ImageVersion: "v0.0.1"}
	if v := ip.GenerateImagePolicyLabelsAsHash()["pipeline.jquad.rocks/ip.image.version"]; v != "v0.0.1" {
		t.Errorf("image version label = %q, want v0.0.1", v)
	}
}
