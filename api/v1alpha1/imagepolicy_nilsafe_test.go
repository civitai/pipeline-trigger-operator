package v1alpha1

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// A not-Ready ImagePolicy (no .status.latestRef) must not panic in the getters
// (upstream did unchecked .(map)/.(string) assertions). The process surviving
// these calls IS the assertion.
func TestImagePolicyGetters_NotReady_NoPanic(t *testing.T) {
	cases := []unstructured.Unstructured{
		{Object: map[string]interface{}{}},                                                                        // no status
		{Object: map[string]interface{}{"status": map[string]interface{}{}}},                                      // status, no latestRef
		{Object: map[string]interface{}{"status": map[string]interface{}{"latestRef": map[string]interface{}{}}}}, // latestRef, no name/tag
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

// Parity: a Ready ImagePolicy still parses repo/name/version from latestRef.
// On this (newer) Flux API, .status.latestRef is a map{name,tag}: name is the
// image repository path, tag is the version.
func TestImagePolicyGetters_Ready_Parses(t *testing.T) {
	u := unstructured.Unstructured{Object: map[string]interface{}{
		"status": map[string]interface{}{
			"latestRef": map[string]interface{}{
				"name": "ghcr.io/civitai/foo",
				"tag":  "1.2.3",
			},
		},
	}}
	if got := getImageVersion(u); got != "1.2.3" {
		t.Fatalf("getImageVersion = %q, want \"1.2.3\"", got)
	}
	if got := getImageName(u); got != "foo" {
		t.Fatalf("getImageName = %q, want \"foo\"", got)
	}
	if got := getRepositoryName(u); got != "civitai" {
		t.Fatalf("getRepositoryName = %q, want \"civitai\"", got)
	}
}

// A Ready-but-malformed latestRef.name (too few "/"-delimiter parts) or a missing
// latestRef.tag must hit the position/len guards and return "" rather than panic
// or return junk. Ported from the nilsafe coverage work (PR #11) and ADAPTED to
// main's newer Flux API: nilsafe parsed a single .status.latestImage "repo/name:tag"
// string; main reads .status.latestRef{name,tag} (v1beta2), where name carries the
// repo/name path and tag carries the version. The guards under test are
// getRepositoryName's `len(parts) < 2` check and getImageVersion's missing-tag path.
func TestImagePolicyGetters_Malformed_ReturnEmpty(t *testing.T) {
	// name has no "/" (len<2 guard) and no tag key (version empty).
	bare := unstructured.Unstructured{Object: map[string]interface{}{
		"status": map[string]interface{}{"latestRef": map[string]interface{}{"name": "foo"}},
	}}
	if got := getRepositoryName(bare); got != "" {
		t.Errorf("getRepositoryName(name=%q) = %q, want \"\" (len<2 guard)", "foo", got)
	}
	if got := getImageVersion(bare); got != "" {
		t.Errorf("getImageVersion(no tag) = %q, want \"\"", got)
	}

	// name has "/" (repo/name parse) but latestRef has no tag key -> version empty.
	noVersion := unstructured.Unstructured{Object: map[string]interface{}{
		"status": map[string]interface{}{"latestRef": map[string]interface{}{"name": "ghcr.io/foo"}},
	}}
	if got := getRepositoryName(noVersion); got != "ghcr.io" {
		t.Errorf("getRepositoryName(name=%q) = %q, want %q", "ghcr.io/foo", got, "ghcr.io")
	}
	if got := getImageVersion(noVersion); got != "" {
		t.Errorf("getImageVersion(no tag) = %q, want \"\"", got)
	}
}
