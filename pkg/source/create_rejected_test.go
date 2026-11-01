package v1alpha1

import (
	"errors"
	"testing"
	"time"

	pipelinev1alpha1 "github.com/jquad-group/pipeline-trigger-operator/api/v1alpha1"
	apis "github.com/jquad-group/pipeline-trigger-operator/pkg/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

// A permanent create rejection can be caused by the trigger's own TEMPLATE
// (e.g. a bad template label), not the source. Such a slot must be retried
// once the spec is edited (metadata.generation moves past the generation the
// rejection was recorded at) — but ONLY for the dedicated "CreateRejected"
// reason, so slots carrying the old code's generic Error ("Failed") do not
// all re-fire on the next spec edit.

func rejectedTemplate() unstructured.Unstructured {
	var tpl unstructured.Unstructured
	tpl.SetAPIVersion("tekton.dev/v1")
	tpl.SetKind("PipelineRun")
	tpl.SetNamespace("default")
	tpl.Object["spec"] = map[string]interface{}{
		"pipelineRef": map[string]interface{}{"name": "p"},
		"params":      []interface{}{},
	}
	return tpl
}

func errCond(reason string, observedGen int64) metav1.Condition {
	return metav1.Condition{
		Type: apis.ReconcileError, Status: metav1.ConditionFalse, Reason: reason,
		Message: "x", ObservedGeneration: observedGen, LastTransitionTime: metav1.Now(),
	}
}

func TestSettleFailedRun_PermanentRecordsCreateRejectedAtGeneration(t *testing.T) {
	b := pipelinev1alpha1.Branch{Name: "feature/a", Commit: "c1"}
	pt := &pipelinev1alpha1.PipelineTrigger{
		ObjectMeta: metav1.ObjectMeta{Name: "t", Namespace: "default", Generation: 3},
		Status:     pipelinev1alpha1.PipelineTriggerStatus{Branches: pipelinev1alpha1.Branches{Branches: map[string]pipelinev1alpha1.Branch{b.Name: b}}},
	}
	run := &unstructured.Unstructured{}
	run.SetLabels(b.GenerateBranchLabelsAsHash())
	PullrequestSubscriber{}.SettleFailedRun(pt, pt.Status.DeepCopy(), run, errors.New("invalid"), true)
	got := pt.Status.Branches.Branches[b.Name].Conditions
	if len(got) != 1 || got[0].Type != apis.ReconcileError || got[0].Reason != "CreateRejected" || got[0].ObservedGeneration != 3 {
		t.Errorf("want one Error condition with Reason CreateRejected at generation 3, got %+v", got)
	}

	g := &pipelinev1alpha1.PipelineTrigger{ObjectMeta: metav1.ObjectMeta{Generation: 5}}
	GitrepositorySubscriber{}.SettleFailedRun(g, g.Status.DeepCopy(), run, errors.New("invalid"), true)
	if c := g.Status.GitRepository.Conditions; len(c) != 1 || c[0].Reason != "CreateRejected" || c[0].ObservedGeneration != 5 {
		t.Errorf("gitrepository: want CreateRejected at generation 5, got %+v", c)
	}
}

func TestCreatePipelineRunResource_RetriesCreateRejectedOnlyAfterSpecChange(t *testing.T) {
	cases := []struct {
		name     string
		cond     metav1.Condition
		gen      int64
		wantRuns int
	}{
		{"CreateRejected at an older generation -> retried", errCond("CreateRejected", 1), 2, 1},
		{"CreateRejected at the current generation -> not retried", errCond("CreateRejected", 2), 2, 0},
		{"old-code generic Error at an older generation -> NOT retried", errCond(apis.ReconcileErrorReason, 1), 2, 0},
		{"generic Error with zero generation -> NOT retried", errCond(apis.ReconcileErrorReason, 0), 7, 0},
	}
	scheme := runtime.NewScheme()
	for _, c := range cases {
		// PullRequest branch slot.
		b := pipelinev1alpha1.Branch{Name: "feature/a", Commit: "c1", Conditions: []metav1.Condition{c.cond}}
		pt := &pipelinev1alpha1.PipelineTrigger{
			ObjectMeta: metav1.ObjectMeta{Name: "t", Namespace: "default", Generation: c.gen},
			Spec:       pipelinev1alpha1.PipelineTriggerSpec{Source: pipelinev1alpha1.Source{Kind: "PullRequest"}, PipelineRun: rejectedTemplate()},
			Status:     pipelinev1alpha1.PipelineTriggerStatus{Branches: pipelinev1alpha1.Branches{Branches: map[string]pipelinev1alpha1.Branch{b.Name: b}}},
		}
		if got := len(PullrequestSubscriber{}.CreatePipelineRunResource(pt, scheme)); got != c.wantRuns {
			t.Errorf("pullrequest %s: runs = %d, want %d", c.name, got, c.wantRuns)
		}
		if c.wantRuns == 1 {
			after := pt.Status.Branches.Branches[b.Name]
			if _, stillRejected := after.GetCondition(apis.ReconcileError); stillRejected {
				t.Errorf("pullrequest %s: the stale CreateRejected condition must be cleared on retry: %+v", c.name, after.Conditions)
			}
		}

		// GitRepository single slot.
		g := &pipelinev1alpha1.PipelineTrigger{
			ObjectMeta: metav1.ObjectMeta{Name: "t", Namespace: "default", Generation: c.gen},
			Spec:       pipelinev1alpha1.PipelineTriggerSpec{Source: pipelinev1alpha1.Source{Kind: "GitRepository"}, PipelineRun: rejectedTemplate()},
			Status: pipelinev1alpha1.PipelineTriggerStatus{GitRepository: pipelinev1alpha1.GitRepository{
				BranchName: "main", CommitId: "c1", Conditions: []metav1.Condition{c.cond},
			}},
		}
		if got := len(GitrepositorySubscriber{}.CreatePipelineRunResource(g, scheme)); got != c.wantRuns {
			t.Errorf("gitrepository %s: runs = %d, want %d", c.name, got, c.wantRuns)
		}

		// ImagePolicy single slot.
		ip := &pipelinev1alpha1.PipelineTrigger{
			ObjectMeta: metav1.ObjectMeta{Name: "t", Namespace: "default", Generation: c.gen},
			Spec:       pipelinev1alpha1.PipelineTriggerSpec{Source: pipelinev1alpha1.Source{Kind: "ImagePolicy"}, PipelineRun: rejectedTemplate()},
			Status: pipelinev1alpha1.PipelineTriggerStatus{ImagePolicy: pipelinev1alpha1.ImagePolicy{
				ImageName: "app", ImageVersion: "1.0.0", Conditions: []metav1.Condition{c.cond},
			}},
		}
		if got := len(ImagepolicySubscriber{}.CreatePipelineRunResource(ip, scheme)); got != c.wantRuns {
			t.Errorf("imagepolicy %s: runs = %d, want %d", c.name, got, c.wantRuns)
		}
	}
}

func ownedRun(name string, created time.Time, labels map[string]string, specStatus, succeededReason string) unstructured.Unstructured {
	var u unstructured.Unstructured
	u.SetAPIVersion("tekton.dev/v1")
	u.SetKind("PipelineRun")
	u.SetName(name)
	u.SetNamespace("default")
	u.SetCreationTimestamp(metav1.NewTime(created))
	u.SetLabels(labels)
	ctrlTrue := true
	u.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "pipeline.jquad.rocks/v1alpha1", Kind: "PipelineTrigger", Name: "trigger", Controller: &ctrlTrue}})
	if specStatus != "" {
		_ = unstructured.SetNestedField(u.Object, specStatus, "spec", "status")
	}
	if succeededReason != "" {
		_ = unstructured.SetNestedSlice(u.Object, []interface{}{map[string]interface{}{
			"type": "Succeeded", "status": "False", "reason": succeededReason, "message": "m",
		}}, "status", "conditions")
	}
	return u
}

// When several owned runs match the same revision, adoption must be
// deterministic: newest by creationTimestamp among the runs that are not
// cancelled / failed-to-start; a cancelled run only if nothing better exists.
func TestFindExistingRun_PrefersNewestNotCancelled(t *testing.T) {
	labels := map[string]string{"pipeline.jquad.rocks/pr.branch.name": "feature-a", "pipeline.jquad.rocks/pr.branch.commit": "c1"}
	pt := &pipelinev1alpha1.PipelineTrigger{ObjectMeta: metav1.ObjectMeta{Name: "trigger", Namespace: "default"}}
	pr := &unstructured.Unstructured{}
	pr.SetLabels(labels)
	t0 := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)

	list := func(items ...unstructured.Unstructured) unstructured.UnstructuredList {
		return unstructured.UnstructuredList{Items: items}
	}
	cases := []struct {
		name  string
		items unstructured.UnstructuredList
		want  string
	}{
		{"cancelled newest, two healthy -> newest healthy", list(
			ownedRun("cancelled-new", t0.Add(3*time.Minute), labels, "Cancelled", ""),
			ownedRun("ok-old", t0, labels, "", ""),
			ownedRun("ok-mid", t0.Add(time.Minute), labels, "", ""),
		), "ok-mid"},
		{"cancelled via condition reason newest -> healthy one", list(
			ownedRun("cancelled-cond", t0.Add(2*time.Minute), labels, "", "Cancelled"),
			ownedRun("ok", t0, labels, "", ""),
		), "ok"},
		{"failed-to-start newest -> healthy one", list(
			ownedRun("couldnt-start", t0.Add(2*time.Minute), labels, "", "CouldntGetPipeline"),
			ownedRun("ok", t0, labels, "", ""),
		), "ok"},
		{"an ordinary failure is a real result, newest wins", list(
			ownedRun("failed-old", t0, labels, "", "Failed"),
			ownedRun("failed-new", t0.Add(time.Minute), labels, "", "Failed"),
		), "failed-new"},
		{"only cancelled matches -> still adopted", list(
			ownedRun("cancelled-a", t0, labels, "Cancelled", ""),
			ownedRun("cancelled-b", t0.Add(time.Minute), labels, "", "PipelineRunCancelled"),
		), "cancelled-b"},
	}
	for _, c := range cases {
		got := findExistingRun(pt, pr, c.items)
		if got == nil || got.GetName() != c.want {
			name := "<nil>"
			if got != nil {
				name = got.GetName()
			}
			t.Errorf("%s: adopted %s, want %s", c.name, name, c.want)
		}
	}
}
