package v1alpha1

import (
	"context"
	"errors"
	"net/http"
	"testing"

	pipelinev1alpha1 "github.com/jquad-group/pipeline-trigger-operator/api/v1alpha1"
	apis "github.com/jquad-group/pipeline-trigger-operator/pkg/status"
	pullrequestv1alpha1 "github.com/jquad-group/pullrequest-operator/api/v1alpha1"
	tektondevv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestIsPermanentCreateError(t *testing.T) {
	gk := schema.GroupKind{Group: "tekton.dev", Kind: "PipelineRun"}
	webhookDenied := &apierrors.StatusError{ErrStatus: metav1.Status{
		// What the API server builds for a webhook denial with no code set.
		Status: metav1.StatusFailure, Code: http.StatusBadRequest,
		Message: `admission webhook "validation.webhook.pipeline.tekton.dev" denied the request: bad spec`,
	}}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"invalid label (the 2026-10-31 incident)", apierrors.NewInvalid(gk, "x", field.ErrorList{field.Invalid(field.NewPath("metadata", "labels"), "v", "bad")}), true},
		{"bad request", apierrors.NewBadRequest("bad"), true},
		{"webhook denied (400, no reason)", webhookDenied, true},
		{"webhook unreachable (the 2026-07-05 incident)", apierrors.NewInternalError(errors.New(`failed calling webhook "webhook.pipeline.tekton.dev": connect: connection refused`)), false},
		{"timeout", apierrors.NewTimeoutError("slow", 1), false},
		{"service unavailable", apierrors.NewServiceUnavailable("down"), false},
		{"conflict", apierrors.NewConflict(schema.GroupResource{Group: "tekton.dev", Resource: "pipelineruns"}, "x", errors.New("c")), false},
		{"plain network error", errors.New("dial tcp: i/o timeout"), false},
	}
	for _, c := range cases {
		if got := IsPermanentCreateError(c.err); got != c.want {
			t.Errorf("%s: IsPermanentCreateError = %v, want %v", c.name, got, c.want)
		}
	}
}

const (
	commitA   = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	commitB   = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	commitBad = "dddddddddddddddddddddddddddddddddddddddd"
)

type cycleHarness struct {
	t       *testing.T
	c       client.Client
	scheme  *runtime.Scheme
	pt      *pipelinev1alpha1.PipelineTrigger
	creates map[string]int
	// live is the reader StartPipelineRuns re-checks on a cache miss; nil
	// means the fake API server itself (h.c).
	live client.Reader
	// staleCache makes the cached list empty, as an informer that has not
	// yet observed recent creates would hand it over.
	staleCache bool
}

// newCycleHarness builds a fake client whose PipelineRun Create counts per
// pr.branch.commit label, rejects any invalid label value like the real API
// server does, and fails commit `failCommit` with failErr (failTimes<0: always).
func newCycleHarness(t *testing.T, branches []pullrequestv1alpha1.Branch, failCommit string, failErr error, failTimes int) *cycleHarness {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{pullrequestv1alpha1.AddToScheme, pipelinev1alpha1.AddToScheme, tektondevv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	h := &cycleHarness{t: t, scheme: scheme, creates: map[string]int{}}
	src := &pullrequestv1alpha1.PullRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "src", Namespace: "default"},
		Status:     pullrequestv1alpha1.PullRequestStatus{SourceBranches: pullrequestv1alpha1.Branches{Branches: branches}},
	}
	h.c = fake.NewClientBuilder().WithScheme(scheme).WithObjects(src).WithInterceptorFuncs(interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			commit := obj.GetLabels()["pipeline.jquad.rocks/pr.branch.commit"]
			h.creates[commit]++
			for k, v := range obj.GetLabels() {
				if errs := validation.IsValidLabelValue(v); len(errs) != 0 {
					return apierrors.NewInvalid(schema.GroupKind{Group: "tekton.dev", Kind: "PipelineRun"}, obj.GetName(),
						field.ErrorList{field.Invalid(field.NewPath("metadata", "labels"), v, k)})
				}
			}
			if commit == failCommit && failTimes != 0 {
				if failTimes > 0 {
					failTimes--
				}
				return failErr
			}
			return c.Create(ctx, obj, opts...)
		},
	}).Build()

	var tpl unstructured.Unstructured
	tpl.SetAPIVersion("tekton.dev/v1")
	tpl.SetKind("PipelineRun")
	tpl.SetGenerateName("pr-check-")
	// Real templates set the trigger's namespace; without it
	// SetControllerReference refuses (namespaced owner on a cluster-scoped
	// object) and the run carries no owner, which the de-dup relies on.
	tpl.SetNamespace("default")
	tpl.Object["spec"] = map[string]interface{}{
		"pipelineRef": map[string]interface{}{"name": "pr-check"},
		"params":      []interface{}{},
	}
	h.pt = &pipelinev1alpha1.PipelineTrigger{
		ObjectMeta: metav1.ObjectMeta{Name: "trigger", Namespace: "default"},
		Spec: pipelinev1alpha1.PipelineTriggerSpec{
			Source:      pipelinev1alpha1.Source{Kind: "PullRequest", Name: "src"},
			PipelineRun: tpl,
		},
	}
	return h
}

// existing lists the runs the fake API server holds, as the controller's
// ListTektonResources would hand them to StartPipelineRuns.
func (h *cycleHarness) existing() unstructured.UnstructuredList {
	h.t.Helper()
	list := unstructured.UnstructuredList{}
	list.SetAPIVersion("tekton.dev/v1")
	list.SetKind("PipelineRunList")
	if err := h.c.List(context.TODO(), &list); err != nil {
		h.t.Fatal(err)
	}
	return list
}

// cycle mirrors the controller's create block for one reconcile; the trigger
// object is reused, standing in for the status the controller persists.
func (h *cycleHarness) cycle() StartResult {
	h.t.Helper()
	ctx := context.TODO()
	sub := &PullrequestSubscriber{}
	before := h.pt.Status.DeepCopy()
	if _, err := sub.GetLatestEvent(ctx, h.pt, h.c, ctrl.Request{}, "", ""); err != nil {
		h.t.Fatal(err)
	}
	prs := sub.CreatePipelineRunResource(h.pt, h.scheme)
	existing := h.existing()
	if h.staleCache {
		existing = unstructured.UnstructuredList{}
	}
	live := h.live
	if live == nil {
		live = h.c
	}
	return StartPipelineRuns(ctx, h.c, live, sub, h.pt, prs, existing, before, ctrl.Request{}, func(string, string, string) {})
}

func TestStartPipelineRuns_PermanentErrorIsIsolatedAndNotRetried(t *testing.T) {
	invalid := apierrors.NewInvalid(schema.GroupKind{Group: "tekton.dev", Kind: "PipelineRun"}, "x",
		field.ErrorList{field.Invalid(field.NewPath("spec"), "x", "injected")})
	h := newCycleHarness(t, []pullrequestv1alpha1.Branch{
		{Name: "feature/a", Commit: commitA},
		{Name: "feature/b", Commit: commitB},
		{Name: "feature/bad", Commit: commitBad},
	}, commitBad, invalid, -1)

	first := h.cycle()
	if first.Started != 2 || first.Permanent != 1 || first.TransientErr != nil {
		t.Fatalf("first cycle = %+v, want Started=2 Permanent=1 TransientErr=nil", first)
	}
	for i := 0; i < 4; i++ {
		if r := h.cycle(); r.Started != 0 || r.Permanent != 0 || r.TransientErr != nil {
			t.Fatalf("cycle %d = %+v, want nothing attempted", i+2, r)
		}
	}
	for commit, want := range map[string]int{commitA: 1, commitB: 1, commitBad: 1} {
		if got := h.creates[commit]; got != want {
			t.Errorf("creates for %s = %d, want %d", commit[:4], got, want)
		}
	}
	bad := h.pt.Status.Branches.Branches["feature/bad"]
	if c, ok := bad.GetCondition(apis.ReconcileError); !ok || c.Status != metav1.ConditionFalse {
		t.Errorf("bad branch should carry an Error condition, got %+v", bad.Conditions)
	}
	if bad.Commit != commitBad {
		t.Errorf("bad branch must keep the advanced commit (else it is retried): %q", bad.Commit)
	}
	for _, name := range []string{"feature/a", "feature/b"} {
		b := h.pt.Status.Branches.Branches[name]
		if _, ok := b.GetCondition(apis.ReconcileError); ok {
			t.Errorf("%s must not carry the bad branch's error", name)
		}
		if b.LatestPipelineRun == "" {
			t.Errorf("%s: run name not recorded", name)
		}
	}

	msg := "a new commit on the bad branch makes it eligible again"
	src := &pullrequestv1alpha1.PullRequest{}
	if err := h.c.Get(context.TODO(), client.ObjectKey{Name: "src", Namespace: "default"}, src); err != nil {
		t.Fatal(err)
	}
	src.Status.SourceBranches.Branches[2].Commit = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	if err := h.c.Update(context.TODO(), src); err != nil {
		t.Fatal(err)
	}
	if r := h.cycle(); r.Started != 1 {
		t.Errorf("%s: got %+v", msg, r)
	}
}

func TestStartPipelineRuns_TransientErrorRetriesOnlyThatBranch(t *testing.T) {
	h := newCycleHarness(t, []pullrequestv1alpha1.Branch{
		{Name: "feature/a", Commit: commitA},
		{Name: "feature/b", Commit: commitB},
		{Name: "feature/flaky", Commit: commitBad},
	}, commitBad, apierrors.NewInternalError(errors.New("failed calling webhook")), 2)

	var results []StartResult
	for i := 0; i < 5; i++ {
		results = append(results, h.cycle())
	}
	if results[0].TransientErr == nil || results[1].TransientErr == nil {
		t.Fatalf("first two cycles must report the transient error (requeue): %+v", results[:2])
	}
	if results[2].TransientErr != nil || results[2].Started != 1 {
		t.Errorf("third cycle should create the flaky branch: %+v", results[2])
	}
	for commit, want := range map[string]int{commitA: 1, commitB: 1, commitBad: 3} {
		if got := h.creates[commit]; got != want {
			t.Errorf("creates for %s = %d, want %d", commit[:4], got, want)
		}
	}
}

// The writer (CreatePipelineRunResourceForBranch labels) and the readers
// (SetCurrentPipelineRunName, SettleFailedRun) must agree on a sanitised label:
// a run for the incident branch is created once and mapped back to its branch.
func TestStartPipelineRuns_SanitisedLabelRunIsRecognised(t *testing.T) {
	incident := "fix/bug-post-text-scan-raises-pg-posts-"
	h := newCycleHarness(t, []pullrequestv1alpha1.Branch{{Name: incident, Commit: commitA}}, "", nil, 0)
	for i := 0; i < 3; i++ {
		h.cycle()
	}
	if got := h.creates[commitA]; got != 1 {
		t.Fatalf("creates = %d, want 1", got)
	}
	list := &tektondevv1.PipelineRunList{}
	if err := h.c.List(context.TODO(), list); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("runs = %d, want 1", len(list.Items))
	}
	if got := h.pt.Status.Branches.Branches[incident].LatestPipelineRun; got != list.Items[0].Name {
		t.Errorf("LatestPipelineRun = %q, want %q (reader did not match the sanitised label)", got, list.Items[0].Name)
	}
}

func TestSettleFailedRun_GitRepository(t *testing.T) {
	newPT := func() (*pipelinev1alpha1.PipelineTrigger, *pipelinev1alpha1.PipelineTriggerStatus) {
		before := &pipelinev1alpha1.PipelineTriggerStatus{GitRepository: pipelinev1alpha1.GitRepository{
			BranchName: "main", CommitId: "old", LatestPipelineRun: "main-old",
			Conditions: []metav1.Condition{{Type: apis.ReconcileSuccess, Status: metav1.ConditionTrue}},
		}}
		pt := &pipelinev1alpha1.PipelineTrigger{Status: pipelinev1alpha1.PipelineTriggerStatus{GitRepository: pipelinev1alpha1.GitRepository{
			BranchName: "main", CommitId: "new", LatestPipelineRun: "main-old",
			Conditions: []metav1.Condition{{Type: apis.ReconcileUnknown, Status: metav1.ConditionUnknown}},
		}}}
		return pt, before
	}
	sub := GitrepositorySubscriber{}
	run := &unstructured.Unstructured{}

	pt, before := newPT()
	sub.SettleFailedRun(pt, before, run, errors.New("boom"), true)
	g := pt.Status.GitRepository
	if g.CommitId != "new" || g.LatestPipelineRun != "" || len(g.Conditions) != 1 || g.Conditions[0].Type != apis.ReconcileError {
		t.Errorf("permanent: want advanced commit, no LatestPipelineRun, only an Error condition; got %+v", g)
	}

	pt, before = newPT()
	sub.SettleFailedRun(pt, before, run, errors.New("boom"), false)
	g = pt.Status.GitRepository
	if g.CommitId != "old" || g.LatestPipelineRun != "main-old" {
		t.Errorf("transient: want slot rolled back to before; got %+v", g)
	}
	if _, ok := g.GetCondition(apis.ReconcileError); !ok {
		t.Errorf("transient: want an Error condition recorded; got %+v", g.Conditions)
	}
	if before.GitRepository.Conditions[0].Type != apis.ReconcileSuccess || len(before.GitRepository.Conditions) != 1 {
		t.Errorf("transient: snapshot was mutated (aliasing): %+v", before.GitRepository.Conditions)
	}
}

// With the branch status rolled back (or lost), an existing run owned by this
// trigger for the same branch+commit must be re-adopted, not re-created; a run
// owned by a DIFFERENT trigger must not suppress this trigger's run.
func TestStartPipelineRuns_ExistingRunDeDup(t *testing.T) {
	incident := "fix/bug-post-text-scan-raises-pg-posts-"
	h := newCycleHarness(t, []pullrequestv1alpha1.Branch{
		{Name: incident, Commit: commitA},
		{Name: "feature/b", Commit: commitB},
	}, "", nil, 0)
	if r := h.cycle(); r.Started != 2 {
		t.Fatalf("first cycle = %+v, want 2 started", r)
	}
	for i := 0; i < 3; i++ {
		h.pt.Status = pipelinev1alpha1.PipelineTriggerStatus{} // rollback
		if r := h.cycle(); r.Started != 0 || r.Existing != 2 {
			t.Fatalf("after rollback %d: %+v, want 0 started / 2 existing", i, r)
		}
	}
	if h.creates[commitA] != 1 || h.creates[commitB] != 1 {
		t.Errorf("creates = %v, want 1 each", h.creates)
	}
	list := h.existing()
	if len(list.Items) != 2 {
		t.Fatalf("want 2 runs, got %d", len(list.Items))
	}
	for i := range list.Items {
		if metav1.GetControllerOf(&list.Items[i]) == nil {
			t.Errorf("run %s has no controller owner", list.Items[i].GetName())
		}
	}
	if h.pt.Status.Branches.Branches[incident].LatestPipelineRun == "" {
		t.Error("re-adopted run not recorded as the branch's LatestPipelineRun")
	}

	// Same source, a different trigger (its runs are owned by another name):
	// it must create its own runs.
	h.pt.Name = "other-trigger"
	h.pt.Status = pipelinev1alpha1.PipelineTriggerStatus{}
	if r := h.cycle(); r.Started != 2 || r.Existing != 0 {
		t.Errorf("other trigger: %+v, want its own 2 runs created", r)
	}
}
