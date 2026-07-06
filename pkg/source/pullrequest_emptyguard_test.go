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

// Regression test for the 2026-07-06 rebuild-all incident.
//
// The upstream pullrequest-operator polls GitHub; a failed/partial/not-yet-
// populated poll yields a PullRequest CR whose Status.SourceBranches is EMPTY.
// Before the fix, GetLatestEvent's "remove branches not in the request" loop
// deleted EVERY branch from pipelineTrigger.Status.Branches.Branches (losing each
// branch's build-eligibility Conditions); the next good poll then hit the else
// branch and refilled every branch condition-less, making every open PR
// build-eligible -> a bounded rebuild of ~all open PRs with no new commits.
//
// The fix guards the empty-incoming case as a no-op. This test asserts that an
// empty incoming source leaves known-good branch state (branches AND their
// conditions) completely untouched and reports no new event.
//
// Against the UNFIXED code this FAILS (all branches deleted, gotNewEvent==true);
// with the fix it passes.
func TestPullrequestGetLatestEvent_EmptyIncoming_PreservesBranchesAndConditions(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := pullrequestv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add pullrequest scheme: %v", err)
	}

	// Upstream PullRequest source with an EMPTY SourceBranches list (failed poll).
	source := &pullrequestv1alpha1.PullRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-source",
			Namespace: "default",
		},
		Status: pullrequestv1alpha1.PullRequestStatus{
			SourceBranches: pullrequestv1alpha1.Branches{
				Branches: []pullrequestv1alpha1.Branch{}, // empty — the failure mode
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(source).Build()

	// PipelineTrigger already tracking 2 branches, each WITH a condition (i.e.
	// already reconciled — they must NOT be rebuilt).
	condition := metav1.Condition{
		Type:    "InProgress",
		Status:  metav1.ConditionTrue,
		Reason:  "Started",
		Message: "pipeline run in progress",
	}
	pipelineTrigger := &pipelinev1alpha1.PipelineTrigger{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-trigger",
			Namespace: "default",
		},
		Spec: pipelinev1alpha1.PipelineTriggerSpec{
			Source: pipelinev1alpha1.Source{Name: "test-source"},
		},
		Status: pipelinev1alpha1.PipelineTriggerStatus{
			Branches: pipelinev1alpha1.Branches{
				Branches: map[string]pipelinev1alpha1.Branch{
					"main": {
						Name:       "main",
						Commit:     "commit-main",
						Conditions: []metav1.Condition{condition},
					},
					"feature/x": {
						Name:       "feature/x",
						Commit:     "commit-feature",
						Conditions: []metav1.Condition{condition},
					},
				},
			},
		},
	}

	var subscriber PullrequestSubscriber
	gotNewEvent, err := subscriber.GetLatestEvent(
		context.TODO(), pipelineTrigger, fakeClient, ctrl.Request{}, "pipeline.jquad.rocks", "v1alpha1")
	if err != nil {
		t.Fatalf("GetLatestEvent returned an unexpected error: %v", err)
	}

	// An empty incoming poll must be a no-op: no new event.
	if gotNewEvent {
		t.Errorf("expected gotNewEvent=false for an empty incoming SourceBranches (no-op), got true — every branch would be treated as new and rebuilt")
	}

	// Both branches must survive, untouched.
	got := pipelineTrigger.Status.Branches.Branches
	if len(got) != 2 {
		t.Fatalf("expected 2 branches preserved after an empty poll, got %d (%v) — the empty-list guard failed and the removal loop wiped known-good state", len(got), got)
	}
	for _, name := range []string{"main", "feature/x"} {
		b, found := got[name]
		if !found {
			t.Errorf("branch %q was deleted by an empty poll (must be preserved)", name)
			continue
		}
		if len(b.Conditions) != 1 {
			t.Errorf("branch %q lost its build-eligibility Conditions (got %d, want 1) — this is the rebuild-all trigger", name, len(b.Conditions))
		}
	}
}
