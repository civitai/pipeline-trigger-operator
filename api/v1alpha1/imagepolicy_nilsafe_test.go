package v1alpha1

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// A not-Ready ImagePolicy (no .status.latestImage) must not panic in the
// getters (upstream v0.5.0 did unchecked .(map)/.(string) assertions). The
// process surviving these calls IS the assertion.
func TestImagePolicyGetters_NotReady_NoPanic(t *testing.T) {
	cases := []unstructured.Unstructured{
		{Object: map[string]interface{}{}},                                   // no status
		{Object: map[string]interface{}{"status": map[string]interface{}{}}}, // status, no latestImage
	}
	for i, u := range cases {
		if got := getRepositoryName(u); got != "" {
			t.Fatalf("case %d: getRepositoryName = %q, want \"\"", i, got)
		}
		if got := getImageName(u); got != "" {
			t.Fatalf("case %d: getImageName = %q, want \"\"", i, got)
		}
		if got := getImageVersion(u); got != "" {
			t.Fatalf("case %d: getImageVersion = %q, want \"\"", i, got)
		}
	}
}

// A Ready-but-malformed latestImage (too few delimiter parts) must hit the
// position-out-of-bounds guards and return "" rather than index out of range.
func TestImagePolicyGetters_Malformed_ReturnEmpty(t *testing.T) {
	// no "/" and no ":" -> getRepositoryName len<2 guard; getImageVersion OOB guard.
	bare := unstructured.Unstructured{Object: map[string]interface{}{
		"status": map[string]interface{}{"latestImage": "foo"},
	}}
	if got := getRepositoryName(bare); got != "" {
		t.Errorf("getRepositoryName(%q) = %q, want \"\" (len<2 guard)", "foo", got)
	}
	if got := getImageVersion(bare); got != "" {
		t.Errorf("getImageVersion(%q) = %q, want \"\" (position OOB guard)", "foo", got)
	}

	// has "/" but no ":" -> repo/name parse but version position OOB.
	noVersion := unstructured.Unstructured{Object: map[string]interface{}{
		"status": map[string]interface{}{"latestImage": "ghcr.io/foo"},
	}}
	if got := getRepositoryName(noVersion); got != "ghcr.io" {
		t.Errorf("getRepositoryName(%q) = %q, want %q", "ghcr.io/foo", got, "ghcr.io")
	}
	if got := getImageVersion(noVersion); got != "" {
		t.Errorf("getImageVersion(%q) = %q, want \"\" (no version delimiter)", "ghcr.io/foo", got)
	}
}

// Parity: a Ready ImagePolicy still parses repo/name/version from latestImage.
func TestImagePolicyGetters_Ready_Parses(t *testing.T) {
	u := unstructured.Unstructured{Object: map[string]interface{}{
		"status": map[string]interface{}{"latestImage": "ghcr.io/civitai/foo:1.2.3"},
	}}
	if got := getImageVersion(u); got == "" {
		t.Fatalf("getImageVersion returned empty for a Ready policy")
	}
	if got := getImageName(u); got == "" {
		t.Fatalf("getImageName returned empty for a Ready policy")
	}
	if got := getRepositoryName(u); got == "" {
		t.Fatalf("getRepositoryName returned empty for a Ready policy")
	}
}
