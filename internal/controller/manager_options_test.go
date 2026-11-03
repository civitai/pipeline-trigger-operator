package controllers

import (
	"testing"

	tektondevv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// The manager cmd/main.go starts must trim the PipelineRun informer cache.
// Without this wiring every other test stays green while the cache silently
// holds full PipelineRuns again (the memory bound is lost). Plain go test.
func TestManagerOptions_TrimsPipelineRunCache(t *testing.T) {
	opts := ManagerOptions(runtime.NewScheme(), ":0", ":0", false)
	found := 0
	for obj, byObject := range opts.Cache.ByObject {
		if _, ok := obj.(*tektondevv1.PipelineRun); !ok {
			continue
		}
		found++
		if byObject.Transform == nil {
			t.Fatal("PipelineRun cache entry has no Transform")
		}
		in := &tektondevv1.PipelineRun{
			ObjectMeta: metav1.ObjectMeta{
				Name: "run", Labels: map[string]string{"pipeline.jquad.rocks/pr.branch.commit": "c1"},
				ManagedFields: []metav1.ManagedFieldsEntry{{Manager: "tekton"}},
			},
			Spec:   tektondevv1.PipelineRunSpec{Status: tektondevv1.PipelineRunSpecStatusCancelled, PipelineRef: &tektondevv1.PipelineRef{Name: "p"}},
			Status: tektondevv1.PipelineRunStatus{PipelineRunStatusFields: tektondevv1.PipelineRunStatusFields{PipelineSpec: &tektondevv1.PipelineSpec{Description: "big"}}},
		}
		out, err := byObject.Transform(in)
		if err != nil {
			t.Fatal(err)
		}
		got, ok := out.(*tektondevv1.PipelineRun)
		if !ok {
			t.Fatalf("Transform returned %T", out)
		}
		if got.ManagedFields != nil || got.Status.PipelineSpec != nil || got.Spec.PipelineRef != nil {
			t.Errorf("PipelineRun not trimmed by the manager's cache Transform: %+v", got)
		}
		if got.Spec.Status != tektondevv1.PipelineRunSpecStatusCancelled || got.Labels["pipeline.jquad.rocks/pr.branch.commit"] != "c1" {
			t.Errorf("Transform dropped a field the dedup reads: %+v", got)
		}
	}
	if found != 1 {
		t.Fatalf("PipelineRun entries in the manager cache ByObject = %d, want 1", found)
	}
}
