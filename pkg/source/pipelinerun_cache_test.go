package v1alpha1

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	pipelinev1alpha1 "github.com/jquad-group/pipeline-trigger-operator/api/v1alpha1"
	pullrequestv1alpha1 "github.com/jquad-group/pullrequest-operator/api/v1alpha1"
	tektondevv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const (
	cacheTriggerName = "trigger"
	cacheTriggerUID  = types.UID("uid-trigger-1")
	labelBranch      = "pipeline.jquad.rocks/pr.branch.name"
	labelCommit      = "pipeline.jquad.rocks/pr.branch.commit"
)

// fixtureRun builds a FULL PipelineRun as the API server returns it: the
// fields the dedup reads plus the heavy ones the cache transform must drop.
// Every run gets distinct values, so a transform that loses one field changes
// which run wins.
func fixtureRun(name string, ownerName string, ownerUID types.UID, commit string, created time.Time, specStatus tektondevv1.PipelineRunSpecStatus, succeeded corev1.ConditionStatus, reason string) *tektondevv1.PipelineRun {
	return &tektondevv1.PipelineRun{
		TypeMeta: metav1.TypeMeta{APIVersion: "tekton.dev/v1", Kind: "PipelineRun"},
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         "builds",
			UID:               types.UID("uid-" + name),
			ResourceVersion:   "rv-" + name,
			CreationTimestamp: metav1.NewTime(created),
			Labels: map[string]string{
				labelBranch:           "feature-x",
				labelCommit:           commit,
				"tekton.dev/pipeline": "pr-check",
			},
			Annotations: map[string]string{
				"keep.example/annotation":                          "kept-" + name,
				"kubectl.kubernetes.io/last-applied-configuration": "{\"huge\":\"copy\"}",
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "pipeline.jquad.rocks/v1alpha1", Kind: "PipelineTrigger",
				Name: ownerName, UID: ownerUID, Controller: ptr.To(true),
			}},
			ManagedFields: []metav1.ManagedFieldsEntry{{Manager: "tekton-controller", Operation: metav1.ManagedFieldsOperationUpdate}},
		},
		Spec: tektondevv1.PipelineRunSpec{
			Status:      specStatus,
			PipelineRef: &tektondevv1.PipelineRef{Name: "pr-check"},
			Params:      tektondevv1.Params{{Name: "commit", Value: *tektondevv1.NewStructuredValues(commit)}},
		},
		Status: tektondevv1.PipelineRunStatus{
			Status: duckv1.Status{
				ObservedGeneration: 7,
				Conditions: duckv1.Conditions{{
					Type: apis.ConditionSucceeded, Status: succeeded, Reason: reason, Message: "msg-" + name,
				}},
			},
			PipelineRunStatusFields: tektondevv1.PipelineRunStatusFields{
				PipelineSpec:    &tektondevv1.PipelineSpec{Description: "resolved pipeline spec of " + name},
				ChildReferences: []tektondevv1.ChildStatusReference{{Name: name + "-task"}},
				Results:         []tektondevv1.PipelineRunResult{{Name: "r", Value: *tektondevv1.NewStructuredValues("v")}},
				StartTime:       &metav1.Time{Time: created},
			},
		},
	}
}

// cachedView pushes every run through the cache transform and then through the
// conversion the controller hands to StartPipelineRuns — i.e. exactly what the
// dedup sees in production.
func cachedView(t *testing.T, runs ...*tektondevv1.PipelineRun) unstructured.UnstructuredList {
	t.Helper()
	list := &tektondevv1.PipelineRunList{}
	for _, r := range runs {
		out, err := TransformPipelineRunForCache(r)
		if err != nil {
			t.Fatal(err)
		}
		list.Items = append(list.Items, *out.(*tektondevv1.PipelineRun))
	}
	u, err := PipelineRunsToUnstructured(list)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestTransformPipelineRunForCache_DedupStillPicksTheRightRun(t *testing.T) {
	base := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	const commit = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	runs := []*tektondevv1.PipelineRun{
		// The expected winner: usable, owned by this trigger, right labels.
		fixtureRun("run-b-want", cacheTriggerName, cacheTriggerUID, commit, base.Add(2*time.Minute), "", corev1.ConditionTrue, "Succeeded"),
		// Newest of this trigger's runs, but spec.status Cancelled: wins if
		// spec.status is dropped.
		fixtureRun("run-a-specstatus", cacheTriggerName, cacheTriggerUID, commit, base.Add(9*time.Minute), tektondevv1.PipelineRunSpecStatusCancelled, corev1.ConditionUnknown, "Running"),
		// Newer than the winner, Succeeded condition reason cancelled: wins if
		// status.conditions are dropped.
		fixtureRun("run-c-condition", cacheTriggerName, cacheTriggerUID, commit, base.Add(5*time.Minute), "", corev1.ConditionFalse, "PipelineRunCancelled"),
		// Older but greatest name: wins on the name tie-break if
		// creationTimestamp is dropped.
		fixtureRun("run-z-older", cacheTriggerName, cacheTriggerUID, commit, base.Add(1*time.Minute), "", corev1.ConditionTrue, "Succeeded"),
		// Newest overall, same labels, another trigger's: must not match. (If
		// owner references were dropped, nothing would match at all.)
		fixtureRun("run-d-other-owner", "other-trigger", "uid-other", commit, base.Add(20*time.Minute), "", corev1.ConditionTrue, "Succeeded"),
		// Same name+UID owner, a different commit label: must never match.
		fixtureRun("run-e-other-commit", cacheTriggerName, cacheTriggerUID, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", base.Add(30*time.Minute), "", corev1.ConditionTrue, "Succeeded"),
	}
	// Positive control: on the UNtransformed objects the dedup picks run-b.
	full := &tektondevv1.PipelineRunList{}
	for _, r := range runs {
		full.Items = append(full.Items, *r.DeepCopy())
	}
	fullU, err := PipelineRunsToUnstructured(full)
	if err != nil {
		t.Fatal(err)
	}

	pt := &pipelinev1alpha1.PipelineTrigger{ObjectMeta: metav1.ObjectMeta{Name: cacheTriggerName, Namespace: "builds", UID: cacheTriggerUID}}
	pr := &unstructured.Unstructured{}
	pr.SetLabels(map[string]string{labelBranch: "feature-x", labelCommit: commit})

	for name, view := range map[string]unstructured.UnstructuredList{"full": fullU, "cached": cachedView(t, runs...)} {
		got := findExistingRun(pt, pr, view)
		if got == nil || got.GetName() != "run-b-want" {
			t.Errorf("%s view: findExistingRun = %v, want run-b-want", name, got)
		}
	}

	// isUnusableRun and SetCurrentPipelineRunStatus inputs survive per run.
	view := cachedView(t, runs...)
	byName := map[string]*unstructured.Unstructured{}
	for i := range view.Items {
		byName[view.Items[i].GetName()] = &view.Items[i]
	}
	for name, want := range map[string]bool{"run-a-specstatus": true, "run-c-condition": true, "run-b-want": false} {
		if got := isUnusableRun(byName[name]); got != want {
			t.Errorf("isUnusableRun(%s) = %v, want %v", name, got, want)
		}
	}
	conds, _, _ := unstructured.NestedSlice(byName["run-c-condition"].Object, "status", "conditions")
	if len(conds) != 1 {
		t.Fatalf("run-c conditions = %v", conds)
	}
	c := conds[0].(map[string]interface{})
	for k, want := range map[string]string{"type": "Succeeded", "status": "False", "reason": "PipelineRunCancelled", "message": "msg-run-c-condition"} {
		if c[k] != want {
			t.Errorf("run-c condition %s = %v, want %q", k, c[k], want)
		}
	}
	if got := byName["run-b-want"].GetAnnotations(); got["keep.example/annotation"] != "kept-run-b-want" {
		t.Errorf("annotations = %v, want the non-last-applied annotation kept", got)
	}
}

func TestTransformPipelineRunForCache_DropsHeavyFields(t *testing.T) {
	in := fixtureRun("run-1", cacheTriggerName, cacheTriggerUID, "c1", time.Unix(1700000000, 0), "", corev1.ConditionTrue, "Succeeded")
	orig := in.DeepCopy()
	out, err := TransformPipelineRunForCache(in)
	if err != nil {
		t.Fatal(err)
	}
	got := out.(*tektondevv1.PipelineRun)
	if got.ManagedFields != nil {
		t.Error("managedFields kept")
	}
	if _, ok := got.Annotations["kubectl.kubernetes.io/last-applied-configuration"]; ok {
		t.Error("last-applied annotation kept")
	}
	if got.Status.PipelineSpec != nil || got.Status.ChildReferences != nil || got.Status.Results != nil || got.Status.StartTime != nil {
		t.Errorf("heavy status fields kept: %+v", got.Status.PipelineRunStatusFields)
	}
	if got.Spec.PipelineRef != nil || got.Spec.Params != nil {
		t.Errorf("spec beyond spec.status kept: %+v", got.Spec)
	}
	// StatusChangePredicate compares status.Status: it must be unchanged.
	if !reflect.DeepEqual(got.Status.Status, orig.Status.Status) {
		t.Errorf("status.Status changed: %+v vs %+v", got.Status.Status, orig.Status.Status)
	}
	if got.ResourceVersion != orig.ResourceVersion || got.UID != orig.UID || got.Name != orig.Name || got.Namespace != orig.Namespace ||
		!reflect.DeepEqual(got.OwnerReferences, orig.OwnerReferences) || !reflect.DeepEqual(got.Labels, orig.Labels) ||
		!got.CreationTimestamp.Equal(&orig.CreationTimestamp) {
		t.Errorf("identity metadata changed: %+v", got.ObjectMeta)
	}
	// Idempotent: the informer may transform an already-transformed object.
	again, _ := TransformPipelineRunForCache(got)
	if !reflect.DeepEqual(again, got) {
		t.Error("transform is not idempotent")
	}
	// Other types pass through untouched.
	other := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", ManagedFields: []metav1.ManagedFieldsEntry{{Manager: "m"}}}}
	if o, _ := TransformPipelineRunForCache(other); o != other || len(other.ManagedFields) != 1 {
		t.Error("non-PipelineRun object was modified")
	}
}

// Read-after-create: the informer cache may not yet hold a run this operator
// created a moment ago. With the status rolled back (e.g. a create that timed
// out but landed), the cached list is empty, so only the live label-filtered
// re-check prevents a duplicate.
func TestStartPipelineRuns_StaleCacheDoesNotDuplicate(t *testing.T) {
	h := newCycleHarness(t, []pullrequestv1alpha1.Branch{
		{Name: "feature/a", Commit: commitA},
		{Name: "feature/b", Commit: commitB},
	}, "", nil, 0)
	if r := h.cycle(); r.Started != 2 {
		t.Fatalf("first cycle = %+v, want 2 started", r)
	}
	h.staleCache = true
	for i := 0; i < 3; i++ {
		h.pt.Status = pipelinev1alpha1.PipelineTriggerStatus{} // rollback / lost status
		if r := h.cycle(); r.Started != 0 || r.Existing != 2 || r.TransientErr != nil {
			t.Fatalf("stale-cache cycle %d = %+v, want 0 started / 2 existing", i, r)
		}
	}
	if h.creates[commitA] != 1 || h.creates[commitB] != 1 {
		t.Errorf("creates = %v, want exactly 1 per commit", h.creates)
	}
}

// F4: a failed PipelineRun lookup must not silently disable the de-dup. When
// the live re-check fails, nothing is created, the slot is rolled back
// (retried on requeue) and the error is reported as transient.
func TestStartPipelineRuns_LiveLookupFailureDoesNotCreate(t *testing.T) {
	h := newCycleHarness(t, []pullrequestv1alpha1.Branch{{Name: "feature/a", Commit: commitA}}, "", nil, 0)
	h.staleCache = true
	listErr := errors.New("injected: pipelineruns list unavailable")
	h.live = interceptor.NewClient(h.c.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			return listErr
		},
	})
	r := h.cycle()
	if r.Started != 0 || !errors.Is(r.TransientErr, listErr) {
		t.Fatalf("cycle with failing lookup = %+v, want 0 started and the list error as TransientErr", r)
	}
	if got := h.creates[commitA]; got != 0 {
		t.Fatalf("creates = %d, want 0 while the lookup fails", got)
	}
	// The slot was rolled back, so the next reconcile retries it.
	h.live = nil // lookup recovers
	if r := h.cycle(); r.Started != 1 {
		t.Fatalf("after recovery = %+v, want the run created", r)
	}
	if got := h.creates[commitA]; got != 1 {
		t.Errorf("creates = %d, want 1", got)
	}
}
