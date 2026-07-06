package v1alpha1

import (
	"context"
	"testing"

	pipelinev1alpha1 "github.com/jquad-group/pipeline-trigger-operator/api/v1alpha1"
	pullrequestv1alpha1 "github.com/jquad-group/pullrequest-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// GetLatestEvent is the branch-sync heart of the PullRequest source: it
// reconciles the trigger's known branch state against the upstream
// PullRequest.Status.SourceBranches each reconcile. Its individual branches
// decide whether an open PR is (re)built:
//   - a NEW branch is added (build-eligible)
//   - a CHANGED commit overwrites the tracked branch, clearing its Conditions so
//     CreatePipelineRunResource rebuilds it
//   - an UNCHANGED commit is left untouched, PRESERVING its Conditions so it is
//     NOT rebuilt (the rebuild-all trap lives here)
//   - a branch no longer present upstream is REMOVED
//   - the first-ever population takes the else-branch and seeds all branches
//
// The empty-incoming no-op guard is covered separately in
// pullrequest_emptyguard_test.go; this file covers the other five paths.

func newPRSource(name string, branches ...pullrequestv1alpha1.Branch) *pullrequestv1alpha1.PullRequest {
	return &pullrequestv1alpha1.PullRequest{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Status: pullrequestv1alpha1.PullRequestStatus{
			SourceBranches: pullrequestv1alpha1.Branches{Branches: branches},
		},
	}
}

func newTrigger(sourceName string, tracked map[string]pipelinev1alpha1.Branch) *pipelinev1alpha1.PipelineTrigger {
	return &pipelinev1alpha1.PipelineTrigger{
		ObjectMeta: metav1.ObjectMeta{Name: "trigger", Namespace: "default"},
		Spec:       pipelinev1alpha1.PipelineTriggerSpec{Source: pipelinev1alpha1.Source{Name: sourceName}},
		Status: pipelinev1alpha1.PipelineTriggerStatus{
			Branches: pipelinev1alpha1.Branches{Branches: tracked},
		},
	}
}

func runGetLatestEvent(t *testing.T, source *pullrequestv1alpha1.PullRequest, pt *pipelinev1alpha1.PipelineTrigger) bool {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := pullrequestv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(source).Build()
	var sub PullrequestSubscriber
	got, err := sub.GetLatestEvent(context.TODO(), pt, c, ctrl.Request{}, "pipeline.jquad.rocks", "v1alpha1")
	if err != nil {
		t.Fatalf("GetLatestEvent error: %v", err)
	}
	return got
}

var doneCondition = metav1.Condition{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Built", Message: "done"}

// First-ever population (else branch): no tracked branches yet -> seed all.
func TestGetLatestEvent_FirstPopulation_SeedsAllBranches(t *testing.T) {
	source := newPRSource("src",
		pullrequestv1alpha1.Branch{Name: "main", Commit: "c-main"},
		pullrequestv1alpha1.Branch{Name: "feature/x", Commit: "c-x"},
	)
	pt := newTrigger("src", nil) // no branches tracked yet

	got := runGetLatestEvent(t, source, pt)
	if !got {
		t.Error("expected gotNewEvent=true on first population")
	}
	if len(pt.Status.Branches.Branches) != 2 {
		t.Fatalf("expected 2 seeded branches, got %d", len(pt.Status.Branches.Branches))
	}
	if pt.Status.Branches.Branches["main"].Commit != "c-main" {
		t.Errorf("main not seeded correctly: %+v", pt.Status.Branches.Branches["main"])
	}
}

// Add-new-branch: an already-populated trigger gains a new upstream branch;
// the existing branch (and its Conditions) is preserved.
func TestGetLatestEvent_AddNewBranch(t *testing.T) {
	source := newPRSource("src",
		pullrequestv1alpha1.Branch{Name: "main", Commit: "c1"},
		pullrequestv1alpha1.Branch{Name: "feature/new", Commit: "c2"},
	)
	pt := newTrigger("src", map[string]pipelinev1alpha1.Branch{
		"main": {Name: "main", Commit: "c1", Conditions: []metav1.Condition{doneCondition}},
	})

	got := runGetLatestEvent(t, source, pt)
	if !got {
		t.Error("expected gotNewEvent=true when a new branch appears")
	}
	if _, ok := pt.Status.Branches.Branches["feature/new"]; !ok {
		t.Error("new branch was not added")
	}
	if len(pt.Status.Branches.Branches["main"].Conditions) != 1 {
		t.Error("existing branch lost its conditions when an unrelated branch was added")
	}
}

// Changed-commit: an existing branch whose upstream commit moved is overwritten
// with the new value, CLEARING its Conditions so it becomes build-eligible again.
func TestGetLatestEvent_ChangedCommit_ClearsConditionsAndRebuilds(t *testing.T) {
	source := newPRSource("src",
		pullrequestv1alpha1.Branch{Name: "main", Commit: "c2-new"},
	)
	pt := newTrigger("src", map[string]pipelinev1alpha1.Branch{
		"main": {Name: "main", Commit: "c1-old", Conditions: []metav1.Condition{doneCondition}},
	})

	got := runGetLatestEvent(t, source, pt)
	if !got {
		t.Error("expected gotNewEvent=true when a tracked branch's commit changes")
	}
	b := pt.Status.Branches.Branches["main"]
	if b.Commit != "c2-new" {
		t.Errorf("commit not updated: got %q want %q", b.Commit, "c2-new")
	}
	if len(b.Conditions) != 0 {
		t.Errorf("expected conditions cleared on a commit change (so it rebuilds), got %d", len(b.Conditions))
	}
}

// Unchanged-commit: a tracked branch whose commit is identical must be left
// untouched, PRESERVING its Conditions — otherwise it would be rebuilt every
// reconcile (the rebuild-all class of bug).
func TestGetLatestEvent_UnchangedCommit_PreservesConditions(t *testing.T) {
	source := newPRSource("src",
		pullrequestv1alpha1.Branch{Name: "main", Commit: "c1"},
	)
	pt := newTrigger("src", map[string]pipelinev1alpha1.Branch{
		"main": {Name: "main", Commit: "c1", Conditions: []metav1.Condition{doneCondition}},
	})

	got := runGetLatestEvent(t, source, pt)
	if got {
		t.Error("expected gotNewEvent=false when nothing changed")
	}
	if len(pt.Status.Branches.Branches["main"].Conditions) != 1 {
		t.Error("unchanged branch lost its conditions — it would be rebuilt every reconcile")
	}
}

// Remove-branch: a tracked branch no longer present upstream (PR merged/closed)
// is removed. Removal alone is not a "new event".
func TestGetLatestEvent_RemoveBranchNoLongerPresent(t *testing.T) {
	source := newPRSource("src",
		pullrequestv1alpha1.Branch{Name: "main", Commit: "c1"},
	)
	pt := newTrigger("src", map[string]pipelinev1alpha1.Branch{
		"main":  {Name: "main", Commit: "c1", Conditions: []metav1.Condition{doneCondition}},
		"stale": {Name: "stale", Commit: "c9", Conditions: []metav1.Condition{doneCondition}},
	})

	got := runGetLatestEvent(t, source, pt)
	if got {
		t.Error("expected gotNewEvent=false for a pure removal")
	}
	if _, ok := pt.Status.Branches.Branches["stale"]; ok {
		t.Error("stale branch was not removed")
	}
	if _, ok := pt.Status.Branches.Branches["main"]; !ok {
		t.Error("surviving branch was incorrectly removed")
	}
}
