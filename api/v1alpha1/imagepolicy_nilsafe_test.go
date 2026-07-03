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
		{Object: map[string]interface{}{}},                         // no status
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
