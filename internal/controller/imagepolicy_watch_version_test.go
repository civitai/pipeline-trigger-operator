package controllers

import (
	"testing"

	imagereflectorv1 "github.com/fluxcd/image-reflector-controller/api/v1"
	imagereflectorv1beta2 "github.com/fluxcd/image-reflector-controller/api/v1beta2"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

// The ImagePolicy watch must be on image.toolkit.fluxcd.io/v1. Flux 2.9 stops
// serving v1beta2; a watch still on v1beta2 then never syncs its cache and the
// manager starts no workers, so every PipelineTrigger stops firing. Both
// versions are registered here so the test reads the version the watch object
// resolves to, rather than failing on an unregistered type. Plain go test.
func TestImagePolicyWatch_IsV1(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := imagereflectorv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := imagereflectorv1beta2.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	gvk, err := apiutil.GVKForObject(imagePolicyWatchObject(), scheme)
	if err != nil {
		t.Fatal(err)
	}
	if got := gvk.String(); got != "image.toolkit.fluxcd.io/v1, Kind=ImagePolicy" {
		t.Fatalf("ImagePolicy watch GVK = %q, want %q", got, "image.toolkit.fluxcd.io/v1, Kind=ImagePolicy")
	}
}
