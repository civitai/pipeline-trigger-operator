package controllers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	pipelinev1alpha1 "github.com/jquad-group/pipeline-trigger-operator/api/v1alpha1"
	tektondevv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// A PipelineTrigger whose pipelineRun template is not a tekton.dev group is
// unsupported: PipelineRuns are only read from the tekton.dev/v1 informer
// cache. Such a trigger must create nothing, send no PipelineRun LIST to the
// API server, and not requeue (a spec edit re-triggers the reconcile). Plain
// go test (fake client + a fake API server for the dynamic client), no envtest.
func TestReconcile_UnsupportedPipelineRunGroup(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{pipelinev1alpha1.AddToScheme, tektondevv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}

	// Fake API server for the dynamic client: serves the GitRepository source
	// and an empty PipelineRun list for any group, counting the LISTs.
	var mu sync.Mutex
	prLists := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/gitrepositories/repo"):
			_, _ = w.Write([]byte(`{"apiVersion":"source.toolkit.fluxcd.io/v1","kind":"GitRepository",
				"metadata":{"name":"repo","namespace":"default"},
				"status":{"artifact":{"revision":"main@sha1:1a2b3c4fffffffffffffffffffffffffffffffff"},
				"conditions":[{"type":"Ready","status":"True","reason":"Succeeded","message":"ok","lastTransitionTime":"2026-11-01T00:00:00Z"}]}}`))
		case strings.HasSuffix(r.URL.Path, "/pipelineruns"):
			mu.Lock()
			prLists++
			mu.Unlock()
			_, _ = w.Write([]byte(`{"apiVersion":"example.com/v1","kind":"PipelineRunList","metadata":{},"items":[]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`))
		}
	}))
	defer srv.Close()
	dyn, err := dynamic.NewForConfig(&rest.Config{Host: srv.URL})
	if err != nil {
		t.Fatal(err)
	}

	var tpl unstructured.Unstructured
	tpl.SetAPIVersion("example.com/v1")
	tpl.SetKind("PipelineRun")
	tpl.SetNamespace("default")
	tpl.SetGenerateName("run-")
	tpl.Object["spec"] = map[string]interface{}{"params": []interface{}{}}
	pt := &pipelinev1alpha1.PipelineTrigger{
		ObjectMeta: metav1.ObjectMeta{Name: "trigger", Namespace: "default"},
		Spec: pipelinev1alpha1.PipelineTriggerSpec{
			Source:      pipelinev1alpha1.Source{APIVersion: "source.toolkit.fluxcd.io/v1", Kind: "GitRepository", Name: "repo"},
			PipelineRun: tpl,
		},
	}
	creates := 0
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pt).WithStatusSubresource(pt).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				creates++
				return c.Create(ctx, obj, opts...)
			},
		}).Build()
	recorder := record.NewFakeRecorder(100)
	r := &PipelineTriggerReconciler{Client: c, DynamicClient: *dyn, Scheme: scheme, recorder: recorder}

	for i := 0; i < 3; i++ {
		res, err := r.Reconcile(context.TODO(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "trigger", Namespace: "default"}})
		if err != nil || res != (ctrl.Result{}) {
			t.Errorf("reconcile %d = (%+v, %v), want no requeue and no error (a permanent spec error must not loop)", i, res, err)
		}
	}
	if creates != 0 {
		t.Errorf("creates = %d, want 0 for an unsupported PipelineRun group", creates)
	}
	if prLists != 0 {
		t.Errorf("PipelineRun LISTs sent to the API server = %d, want 0", prLists)
	}
	warned := 0
	for len(recorder.Events) > 0 {
		if e := <-recorder.Events; strings.HasPrefix(e, "Warning") && strings.Contains(e, "example.com") {
			warned++
		}
	}
	if warned == 0 {
		t.Error("want a Warning event naming the unsupported group")
	}
}
