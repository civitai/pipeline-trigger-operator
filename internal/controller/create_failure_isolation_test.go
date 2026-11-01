package controllers

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/fluxcd/pkg/apis/meta"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	tektondevv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pipelinev1alpha1 "github.com/jquad-group/pipeline-trigger-operator/api/v1alpha1"
	opstatus "github.com/jquad-group/pipeline-trigger-operator/pkg/status"
	pullrequestv1alpha1 "github.com/jquad-group/pullrequest-operator/api/v1alpha1"
)

// Regression specs for the 2026-10-31 PipelineRun-create storm.
//
// A PR head branch ending in "-" produced an invalid label value, so the API
// server rejected that branch's PipelineRun create (422 Invalid) — a PERMANENT
// error. The create-retry path then restored the WHOLE status snapshot and
// requeued, so every requeued reconcile re-created the runs of every OTHER
// branch of the same PullRequest (their status advance had been rolled back
// too): ~1,100 PipelineRuns for 3 innocent PRs in 30 minutes. These specs drive
// the real Reconcile against envtest (real API-server label validation) and
// count creates per branch across repeated reconciles.

const (
	labelCommitKey = "pipeline.jquad.rocks/pr.branch.commit"
	isoNamespace   = "default"
)

// countingClient counts PipelineRun creates per pr.branch.commit label (each
// test branch has a distinct commit) and can inject a create error for one
// commit, so the error source is independent of label sanitising.
type countingClient struct {
	client.Client
	mu         sync.Mutex
	creates    map[string]int
	failCommit string
	// failErr builds the injected error; failTimes < 0 means "always".
	failErr   func(name string) error
	failTimes int
}

func newCountingClient(c client.Client) *countingClient {
	return &countingClient{Client: c, creates: map[string]int{}}
}

func (c *countingClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == "PipelineRun" {
		commit := u.GetLabels()[labelCommitKey]
		c.mu.Lock()
		c.creates[commit]++
		inject := c.failErr != nil && commit != "" && commit == c.failCommit && c.failTimes != 0
		if inject && c.failTimes > 0 {
			c.failTimes--
		}
		c.mu.Unlock()
		if inject {
			return c.failErr(u.GetGenerateName())
		}
	}
	return c.Client.Create(ctx, obj, opts...)
}

func (c *countingClient) count(commit string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.creates[commit]
}

func permanentInvalid(name string) error {
	return apierrors.NewInvalid(schema.GroupKind{Group: "tekton.dev", Kind: "PipelineRun"}, name,
		field.ErrorList{field.Invalid(field.NewPath("metadata", "labels"), "x", "injected permanent rejection")})
}

func transientInternal(string) error {
	return apierrors.NewInternalError(context.DeadlineExceeded)
}

var _ = Describe("PipelineRun create failures are isolated per branch", func() {
	ctx := context.Background()

	ownedRuns := func(triggerName string) []tektondevv1.PipelineRun {
		list := &tektondevv1.PipelineRunList{}
		Expect(k8sClient.List(ctx, list, client.InNamespace(isoNamespace))).To(Succeed())
		var out []tektondevv1.PipelineRun
		for i := range list.Items {
			for _, ref := range list.Items[i].GetOwnerReferences() {
				if ref.Name == triggerName {
					out = append(out, list.Items[i])
				}
			}
		}
		return out
	}

	// envtest runs no garbage collector, so owned runs survive their trigger.
	// Other specs in this suite count EVERY PipelineRun in the cluster, so
	// delete the runs these specs created (all owned by "iso-*" triggers).
	AfterEach(func() {
		isoOwned := func() []tektondevv1.PipelineRun {
			list := &tektondevv1.PipelineRunList{}
			Expect(k8sClient.List(ctx, list)).To(Succeed())
			var out []tektondevv1.PipelineRun
			for i := range list.Items {
				for _, ref := range list.Items[i].GetOwnerReferences() {
					if strings.HasPrefix(ref.Name, "iso-") {
						out = append(out, list.Items[i])
						break
					}
				}
			}
			return out
		}
		runs := isoOwned()
		for i := range runs {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &runs[i]))).To(Succeed())
		}
		Eventually(func() int { return len(isoOwned()) }, 30*time.Second, time.Second).Should(Equal(0))
	})

	createPipeline := func(name string) {
		task := &tektondevv1.Task{
			ObjectMeta: v1.ObjectMeta{Name: name + "-task", Namespace: isoNamespace},
			Spec:       tektondevv1.TaskSpec{Params: []tektondevv1.ParamSpec{{Name: "p"}}},
		}
		Expect(k8sClient.Create(ctx, task)).To(Succeed())
		pipeline := &tektondevv1.Pipeline{
			ObjectMeta: v1.ObjectMeta{Name: name, Namespace: isoNamespace},
			Spec: tektondevv1.PipelineSpec{Tasks: []tektondevv1.PipelineTask{{
				Name: name + "-task", TaskRef: &tektondevv1.TaskRef{Name: name + "-task"},
			}}},
		}
		Expect(k8sClient.Create(ctx, pipeline)).To(Succeed())
	}

	// The template sets generateName (as the incident's did), so the run NAME
	// never depends on the branch and the label is the only thing under test.
	template := func(pipelineName string) unstructured.Unstructured {
		var pr unstructured.Unstructured
		pr.SetAPIVersion("tekton.dev/v1beta1")
		pr.SetKind("PipelineRun")
		pr.SetNamespace(isoNamespace)
		pr.SetGenerateName("pr-check-")
		// The Tekton controller (absent in envtest) stamps this label on every
		// run; CalculateCurrentState includes it in the labels it matches.
		pr.SetLabels(map[string]string{"tekton.dev/pipeline": pipelineName})
		pr.Object["spec"] = map[string]interface{}{
			"pipelineRef": map[string]interface{}{"name": pipelineName},
			"params":      []interface{}{map[string]interface{}{"name": "p", "value": "v"}},
		}
		return pr
	}

	createPullRequestSource := func(name string, branches []pullrequestv1alpha1.Branch) {
		pr := &pullrequestv1alpha1.PullRequest{
			ObjectMeta: v1.ObjectMeta{Name: name, Namespace: isoNamespace},
			Spec: pullrequestv1alpha1.PullRequestSpec{
				GitProvider: pullrequestv1alpha1.GitProvider{
					Provider: "Github",
					Github: pullrequestv1alpha1.Github{
						Url: "https://github.com/example-org/svc", Owner: "example-org", Repository: "svc",
					},
				},
				TargetBranch: pullrequestv1alpha1.Branch{Name: "main"},
				Interval:     v1.Duration{},
			},
		}
		Expect(k8sClient.Create(ctx, pr)).To(Succeed())
		got := &pullrequestv1alpha1.PullRequest{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: isoNamespace}, got)).To(Succeed())
		got.Status = pullrequestv1alpha1.PullRequestStatus{
			SourceBranches: pullrequestv1alpha1.Branches{Branches: branches},
		}
		Expect(k8sClient.Status().Update(ctx, got)).To(Succeed())
	}

	createTrigger := func(name string, source pipelinev1alpha1.Source, tpl unstructured.Unstructured) {
		pt := &pipelinev1alpha1.PipelineTrigger{
			TypeMeta:   v1.TypeMeta{Kind: "PipelineTrigger", APIVersion: "pipeline.jquad.rocks/v1alpha1"},
			ObjectMeta: v1.ObjectMeta{Name: name, Namespace: isoNamespace},
			Spec:       pipelinev1alpha1.PipelineTriggerSpec{Source: source, PipelineRun: tpl},
		}
		Expect(k8sClient.Create(ctx, pt)).To(Succeed())
	}

	reconcileN := func(r *PipelineTriggerReconciler, name string, n int) []reconcile.Result {
		var results []reconcile.Result
		for i := 0; i < n; i++ {
			res, _ := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: isoNamespace}})
			results = append(results, res)
		}
		return results
	}

	getTrigger := func(name string) *pipelinev1alpha1.PipelineTrigger {
		pt := &pipelinev1alpha1.PipelineTrigger{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: isoNamespace}, pt)).To(Succeed())
		return pt
	}

	newReconciler := func(c client.Client) *PipelineTriggerReconciler {
		return &PipelineTriggerReconciler{
			Client:        c,
			DynamicClient: *dynamicClient,
			Scheme:        k8sClient.Scheme(),
			recorder:      record.NewFakeRecorder(1000),
		}
	}

	const (
		commitA   = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		commitB   = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		commitC   = "cccccccccccccccccccccccccccccccccccccccc"
		commitBad = "dddddddddddddddddddddddddddddddddddddddd"
	)

	It("a PERMANENT create error on one branch neither replays the other branches nor retries itself", func() {
		createPipeline("iso-perm-pipeline")
		createPullRequestSource("iso-perm-pr", []pullrequestv1alpha1.Branch{
			{Name: "feature/a", Commit: commitA},
			{Name: "feature/b", Commit: commitB},
			{Name: "feature/c", Commit: commitC},
			{Name: "feature/bad", Commit: commitBad},
		})
		createTrigger("iso-perm-trigger", pipelinev1alpha1.Source{
			APIVersion: "pipeline.jquad.rocks/v1alpha1", Kind: "PullRequest", Name: "iso-perm-pr",
		}, template("iso-perm-pipeline"))

		cc := newCountingClient(k8sClient)
		cc.failCommit, cc.failErr, cc.failTimes = commitBad, permanentInvalid, -1
		results := reconcileN(newReconciler(cc), "iso-perm-trigger", 5)

		By("each innocent branch got exactly ONE create across 5 reconciles")
		for _, c := range []string{commitA, commitB, commitC} {
			Expect(cc.count(c)).To(Equal(1), "creates for commit %s", c)
		}
		Expect(ownedRuns("iso-perm-trigger")).To(HaveLen(3))

		By("the permanently rejected branch was attempted once, not looped")
		Expect(cc.count(commitBad)).To(Equal(1))
		Expect(results[len(results)-1].Requeue).To(BeFalse(), "a permanent rejection must not requeue")

		By("the error is recorded on the bad branch ONLY")
		pt := getTrigger("iso-perm-trigger")
		bad := pt.Status.Branches.Branches["feature/bad"]
		_, badHasErr := bad.GetCondition(opstatus.ReconcileError)
		Expect(badHasErr).To(BeTrue())
		for _, b := range []string{"feature/a", "feature/b", "feature/c"} {
			br := pt.Status.Branches.Branches[b]
			_, hasErr := br.GetCondition(opstatus.ReconcileError)
			Expect(hasErr).To(BeFalse(), "branch %s must not carry the other branch's error", b)
			Expect(br.LatestPipelineRun).NotTo(BeEmpty(), "branch %s run name recorded", b)
		}
	})

	It("a TRANSIENT create error on one branch retries that branch only", func() {
		createPipeline("iso-trans-pipeline")
		createPullRequestSource("iso-trans-pr", []pullrequestv1alpha1.Branch{
			{Name: "feature/a", Commit: commitA},
			{Name: "feature/b", Commit: commitB},
			{Name: "feature/flaky", Commit: commitBad},
		})
		createTrigger("iso-trans-trigger", pipelinev1alpha1.Source{
			APIVersion: "pipeline.jquad.rocks/v1alpha1", Kind: "PullRequest", Name: "iso-trans-pr",
		}, template("iso-trans-pipeline"))

		cc := newCountingClient(k8sClient)
		cc.failCommit, cc.failErr, cc.failTimes = commitBad, transientInternal, 2
		results := reconcileN(newReconciler(cc), "iso-trans-trigger", 5)

		By("the transient failures requeue (retry is kept)")
		Expect(results[0].Requeue).To(BeTrue())
		Expect(results[1].Requeue).To(BeTrue())

		By("the flaky branch is retried until it succeeds, exactly one run results")
		Expect(cc.count(commitBad)).To(Equal(3))
		By("the healthy branches are NOT replayed by the flaky branch's retries")
		Expect(cc.count(commitA)).To(Equal(1))
		Expect(cc.count(commitB)).To(Equal(1))
		Expect(ownedRuns("iso-trans-trigger")).To(HaveLen(3))
		Expect(results[len(results)-1].Requeue).To(BeFalse())
	})

	It("branches with label-invalid names (the incident branch, a >63-char branch) get exactly one run, recognised on the next reconcile", func() {
		incident := "fix/bug-post-text-scan-raises-pg-posts-"
		long := "feature/" + strings.Repeat("abcdefghij", 6) + "xy"
		createPipeline("iso-label-pipeline")
		createPullRequestSource("iso-label-pr", []pullrequestv1alpha1.Branch{
			{Name: incident, Commit: commitA},
			{Name: long, Commit: commitB},
			{Name: "feature/ok", Commit: commitC},
		})
		createTrigger("iso-label-trigger", pipelinev1alpha1.Source{
			APIVersion: "pipeline.jquad.rocks/v1alpha1", Kind: "PullRequest", Name: "iso-label-pr",
		}, template("iso-label-pipeline"))

		// No injected errors: the REAL API server validates the labels.
		cc := newCountingClient(k8sClient)
		reconcileN(newReconciler(cc), "iso-label-trigger", 4)

		for _, c := range []string{commitA, commitB, commitC} {
			Expect(cc.count(c)).To(Equal(1), "creates for commit %s", c)
		}
		runs := ownedRuns("iso-label-trigger")
		Expect(runs).To(HaveLen(3))
		runByCommit := map[string]string{}
		for _, run := range runs {
			for k, v := range run.GetLabels() {
				Expect(validation.IsValidLabelValue(v)).To(BeEmpty(), "label %s=%q", k, v)
			}
			runByCommit[run.GetLabels()[labelCommitKey]] = run.GetName()
		}

		By("the reader side maps each (sanitised-label) run back to its branch")
		pt := getTrigger("iso-label-trigger")
		Expect(pt.Status.Branches.Branches[incident].LatestPipelineRun).To(Equal(runByCommit[commitA]))
		Expect(pt.Status.Branches.Branches[long].LatestPipelineRun).To(Equal(runByCommit[commitB]))
		Expect(pt.Status.Branches.Branches["feature/ok"].LatestPipelineRun).To(Equal(runByCommit[commitC]))
	})

	// The PullRequest CalculateCurrentState never reported a branch as already
	// built (it always returned false), so status was the ONLY de-dup: any path
	// that loses or rolls back a branch's status re-creates its run. An
	// existing run owned by this trigger with the same branch/commit labels must
	// prevent a second create.
	It("a branch whose status was rolled back is not re-created when its run already exists", func() {
		createPipeline("iso-dedup-pipeline")
		createPullRequestSource("iso-dedup-pr", []pullrequestv1alpha1.Branch{
			{Name: "fix/dedup-trailing-", Commit: commitA},
			{Name: "feature/b", Commit: commitB},
		})
		createTrigger("iso-dedup-trigger", pipelinev1alpha1.Source{
			APIVersion: "pipeline.jquad.rocks/v1alpha1", Kind: "PullRequest", Name: "iso-dedup-pr",
		}, template("iso-dedup-pipeline"))

		cc := newCountingClient(k8sClient)
		r := newReconciler(cc)
		reconcileN(r, "iso-dedup-trigger", 1)
		Expect(ownedRuns("iso-dedup-trigger")).To(HaveLen(2))

		By("rolling the trigger status back (as a lost/rolled-back status write would)")
		for i := 0; i < 3; i++ {
			pt := getTrigger("iso-dedup-trigger")
			pt.Status = pipelinev1alpha1.PipelineTriggerStatus{}
			Expect(k8sClient.Status().Update(ctx, pt)).To(Succeed())
			reconcileN(r, "iso-dedup-trigger", 1)
		}
		Expect(cc.count(commitA)).To(Equal(1), "branch with a sanitised label re-created")
		Expect(cc.count(commitB)).To(Equal(1))
		Expect(ownedRuns("iso-dedup-trigger")).To(HaveLen(2))

		By("the existing run is re-adopted as the branch's latest run")
		runByCommit := map[string]string{}
		for _, run := range ownedRuns("iso-dedup-trigger") {
			runByCommit[run.GetLabels()[labelCommitKey]] = run.GetName()
		}
		pt := getTrigger("iso-dedup-trigger")
		Expect(pt.Status.Branches.Branches["fix/dedup-trailing-"].LatestPipelineRun).To(Equal(runByCommit[commitA]))

		// Invariant guard (also green before the de-dup): the pullrequest
		// operator folds a label-set hash into Branch.Commit, so a new commit
		// value (new push OR label change) MUST still create a new run.
		By("a new commit value on the same branch still creates a new run")
		src := &pullrequestv1alpha1.PullRequest{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "iso-dedup-pr", Namespace: isoNamespace}, src)).To(Succeed())
		src.Status.SourceBranches.Branches[1].Commit = commitC
		Expect(k8sClient.Status().Update(ctx, src)).To(Succeed())
		reconcileN(r, "iso-dedup-trigger", 2)
		Expect(cc.count(commitC)).To(Equal(1))
		Expect(ownedRuns("iso-dedup-trigger")).To(HaveLen(3))
	})

	// A permanent rejection caused by the trigger's own template must be
	// retried once the spec is fixed (generation bump) — exactly once.
	It("a CreateRejected branch is retried exactly once after the trigger spec changes", func() {
		createPipeline("iso-gen-pipeline")
		createPullRequestSource("iso-gen-pr", []pullrequestv1alpha1.Branch{
			{Name: "feature/a", Commit: commitA},
			{Name: "feature/bad", Commit: commitBad},
		})
		createTrigger("iso-gen-trigger", pipelinev1alpha1.Source{
			APIVersion: "pipeline.jquad.rocks/v1alpha1", Kind: "PullRequest", Name: "iso-gen-pr",
		}, template("iso-gen-pipeline"))

		cc := newCountingClient(k8sClient)
		cc.failCommit, cc.failErr, cc.failTimes = commitBad, permanentInvalid, 1
		r := newReconciler(cc)
		reconcileN(r, "iso-gen-trigger", 3)
		Expect(cc.count(commitBad)).To(Equal(1))

		By("editing the trigger spec (the 'template fix') bumps metadata.generation")
		pt := getTrigger("iso-gen-trigger")
		gen := pt.GetGeneration()
		pt.Spec.PipelineRun.SetLabels(map[string]string{"tekton.dev/pipeline": "iso-gen-pipeline", "fixed": "true"})
		Expect(k8sClient.Update(ctx, pt)).To(Succeed())
		Expect(getTrigger("iso-gen-trigger").GetGeneration()).To(BeNumerically(">", gen))

		reconcileN(r, "iso-gen-trigger", 3)
		Expect(cc.count(commitBad)).To(Equal(2), "rejected branch retried exactly once after the spec change")
		Expect(cc.count(commitA)).To(Equal(1), "healthy branch must not re-run on a spec edit")
		Expect(ownedRuns("iso-gen-trigger")).To(HaveLen(2))
	})

	// The live status left by the incident: branches carrying the OLD code's
	// generic Error condition. Seed that shape and check the new code neither
	// re-fires them (also not on a spec edit) nor fails to build new commits.
	It("handles the incident's leftover status shape: old Error conditions and an existing run", func() {
		const commitOld = "0000000000000000000000000000000000000000"
		createPipeline("iso-seed-pipeline")
		createPullRequestSource("iso-seed-pr", []pullrequestv1alpha1.Branch{
			{Name: "feature/old", Commit: commitA}, // source moved past the status commit
			{Name: "feature/cur", Commit: commitB}, // source == status commit
		})
		createTrigger("iso-seed-trigger", pipelinev1alpha1.Source{
			APIVersion: "pipeline.jquad.rocks/v1alpha1", Kind: "PullRequest", Name: "iso-seed-pr",
		}, template("iso-seed-pipeline"))
		pt := getTrigger("iso-seed-trigger")

		oldErr := []v1.Condition{{
			Type: opstatus.ReconcileError, Status: v1.ConditionFalse, Reason: opstatus.ReconcileErrorReason,
			Message: "PipelineRun.tekton.dev is invalid: metadata.labels: Invalid value", ObservedGeneration: pt.GetGeneration(),
			LastTransitionTime: v1.Now(),
		}}
		pt.Status = pipelinev1alpha1.PipelineTriggerStatus{Branches: pipelinev1alpha1.Branches{Branches: map[string]pipelinev1alpha1.Branch{
			"feature/old": {Name: "feature/old", Commit: commitOld, Conditions: oldErr},
			"feature/cur": {Name: "feature/cur", Commit: commitB, Conditions: oldErr},
		}}}
		Expect(k8sClient.Status().Update(ctx, pt)).To(Succeed())

		By("a pre-existing run owned by the trigger for feature/cur@current")
		ctrlTrue := true
		existing := &tektondevv1.PipelineRun{
			ObjectMeta: v1.ObjectMeta{
				GenerateName: "pr-check-", Namespace: isoNamespace,
				Labels: map[string]string{
					"pipeline.jquad.rocks/pr.branch.name": "feature-cur",
					labelCommitKey:                        commitB,
					"tekton.dev/pipeline":                 "iso-seed-pipeline",
				},
				OwnerReferences: []v1.OwnerReference{{
					APIVersion: "pipeline.jquad.rocks/v1alpha1", Kind: "PipelineTrigger",
					Name: pt.GetName(), UID: pt.GetUID(), Controller: &ctrlTrue,
				}},
			},
			Spec: tektondevv1.PipelineRunSpec{PipelineRef: &tektondevv1.PipelineRef{Name: "iso-seed-pipeline"}},
		}
		Expect(k8sClient.Create(ctx, existing)).To(Succeed())

		cc := newCountingClient(k8sClient)
		r := newReconciler(cc)
		reconcileN(r, "iso-seed-trigger", 3)
		Expect(cc.count(commitB)).To(Equal(0), "current-commit branch with an existing run must not be re-created")
		Expect(cc.count(commitA)).To(Equal(1), "branch whose source moved builds exactly once")

		By("a spec edit must not re-fire old-code Error conditions")
		pt = getTrigger("iso-seed-trigger")
		pt.Spec.PipelineRun.SetLabels(map[string]string{"tekton.dev/pipeline": "iso-seed-pipeline", "edited": "true"})
		Expect(k8sClient.Update(ctx, pt)).To(Succeed())
		reconcileN(r, "iso-seed-trigger", 2)
		Expect(cc.count(commitB)).To(Equal(0))
		Expect(cc.count(commitA)).To(Equal(1))

		By("a new commit on the current-commit branch builds")
		src := &pullrequestv1alpha1.PullRequest{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "iso-seed-pr", Namespace: isoNamespace}, src)).To(Succeed())
		src.Status.SourceBranches.Branches[1].Commit = commitC
		Expect(k8sClient.Status().Update(ctx, src)).To(Succeed())
		reconcileN(r, "iso-seed-trigger", 2)
		Expect(cc.count(commitC)).To(Equal(1))
		Expect(ownedRuns("iso-seed-trigger")).To(HaveLen(3)) // seeded + old-branch build + new commit
	})

	It("a GitRepository on a ref with a '/' gets one run that CalculateCurrentState recognises as already running", func() {
		createPipeline("iso-git-pipeline")
		repo := &sourcev1.GitRepository{
			ObjectMeta: v1.ObjectMeta{Name: "iso-git-repo", Namespace: isoNamespace},
			Spec:       sourcev1.GitRepositorySpec{URL: "http://github.com/org/repo.git", Interval: v1.Duration{}},
		}
		Expect(k8sClient.Create(ctx, repo)).To(Succeed())
		got := &sourcev1.GitRepository{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "iso-git-repo", Namespace: isoNamespace}, got)).To(Succeed())
		got.Status = sourcev1.GitRepositoryStatus{
			Artifact: &meta.Artifact{
				Path:           "gitrepository/default/iso-git-repo/ae05cbc.tar.gz",
				URL:            "http://source-controller.local/gitrepository/default/iso-git-repo/ae05cbc.tar.gz",
				Revision:       "feature/x@sha1:ae05cbcffffffffffffffffffffffffffffffff",
				Digest:         "sha256:ae05cbcffffffffffffffffffffffffffffffff",
				LastUpdateTime: v1.Now(),
			},
			Conditions: []v1.Condition{{
				Type: "Ready", Status: v1.ConditionTrue, Reason: v1.StatusSuccess, Message: "Success",
				ObservedGeneration: 1, LastTransitionTime: v1.Now(),
			}},
		}
		Expect(k8sClient.Status().Update(ctx, got)).To(Succeed())
		createTrigger("iso-git-trigger", pipelinev1alpha1.Source{
			APIVersion: "source.toolkit.fluxcd.io/v1", Kind: "GitRepository", Name: "iso-git-repo",
		}, template("iso-git-pipeline"))

		r := newReconciler(k8sClient)
		reconcileN(r, "iso-git-trigger", 3)
		Eventually(func() int { return len(ownedRuns("iso-git-trigger")) }, 10*time.Second, time.Second).Should(Equal(1))

		By("the reader side (CalculateCurrentState label match) sees the sanitised run")
		pt := getTrigger("iso-git-trigger")
		sub := createSourceSubscriber(pt, r.DynamicClient)
		list, err := ListTektonResources(ctx, r.DynamicClient, isoNamespace, "tekton.dev", "v1beta1", "pipelineruns")
		Expect(err).NotTo(HaveOccurred())
		running, _, err := sub.CalculateCurrentState(false, "", "", ctx, pt, r.Client, list, "source.toolkit.fluxcd.io", "v1")
		Expect(err).NotTo(HaveOccurred())
		Expect(running).To(BeTrue(), "the created run must be recognised as already running")
	})
})
