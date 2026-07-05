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
		{Object: map[string]interface{}{}},                                             // no status
		{Object: map[string]interface{}{"status": map[string]interface{}{}}},           // status, no latestRef
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
