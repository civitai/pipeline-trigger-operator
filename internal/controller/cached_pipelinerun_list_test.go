package controllers

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/fluxcd/pkg/apis/meta"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	tektondevv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pipelinev1alpha1 "github.com/jquad-group/pipeline-trigger-operator/api/v1alpha1"
)

// Regression specs for the 2026-10-31 OOM: every reconcile did a full
// unstructured LIST of the namespace's PipelineRuns against the API server
// (1,000+ during the backlog). They count the real HTTP requests the
// reconciler sends, so "served from the cache" is measured on the wire, not
// inferred from which client type the code calls.

// prListCounter counts PipelineRun LIST requests (GET .../pipelineruns that is
// not a watch) by whether they carry a label selector, and can fail every
// unfiltered one with a 500.
type prListCounter struct {
	mu                   sync.Mutex
	unfiltered, filtered int
	injected             int
	failUnfiltered       bool
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func (c *prListCounter) wrap(rt http.RoundTripper) http.RoundTripper {
	return roundTripFunc(func(req *http.Request) (*http.Response, error) {
		q := req.URL.Query()
		if req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/pipelineruns") && q.Get("watch") == "" {
			c.mu.Lock()
			sel := q.Get("labelSelector")
			if sel == "" {
				c.unfiltered++
			} else {
				c.filtered++
			}
			fail := c.failUnfiltered && sel == ""
			if fail {
				c.injected++
			}
			c.mu.Unlock()
			if fail {
				return &http.Response{
					StatusCode: http.StatusInternalServerError,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body: io.NopCloser(strings.NewReader(
						`{"kind":"Status","apiVersion":"v1","status":"Failure","message":"injected list failure","reason":"InternalError","code":500}`)),
					Request: req,
				}, nil
			}
		}
		return rt.RoundTrip(req)
	})
}

func (c *prListCounter) counts() (unfiltered, filtered, injected int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.unfiltered, c.filtered, c.injected
}

func (c *prListCounter) config() *rest.Config {
	cc := rest.CopyConfig(cfg)
	cc.WrapTransport = c.wrap
	return cc
}

var _ = Describe("PipelineRuns are listed from the informer cache", func() {
	ctx := context.Background()

	ownedRuns := func(triggerName string) []tektondevv1.PipelineRun {
		list := &tektondevv1.PipelineRunList{}
		Expect(k8sClient.List(ctx, list, client.InNamespace(isoNamespace))).To(Succeed())
		var out []tektondevv1.PipelineRun
		for i := range list.Items {
			if o := v1.GetControllerOf(&list.Items[i]); o != nil && o.Name == triggerName {
				out = append(out, list.Items[i])
			}
		}
		return out
	}

	// Other specs count every PipelineRun in the cluster; remove ours.
	AfterEach(func() {
		list := &tektondevv1.PipelineRunList{}
		Expect(k8sClient.List(ctx, list)).To(Succeed())
		for i := range list.Items {
			if o := v1.GetControllerOf(&list.Items[i]); o != nil && strings.HasPrefix(o.Name, "cache-") {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &list.Items[i]))).To(Succeed())
			}
		}
		Eventually(func() int {
			l := &tektondevv1.PipelineRunList{}
			Expect(k8sClient.List(ctx, l)).To(Succeed())
			n := 0
			for i := range l.Items {
				if o := v1.GetControllerOf(&l.Items[i]); o != nil && strings.HasPrefix(o.Name, "cache-") {
					n++
				}
			}
			return n
		}, 30*time.Second, time.Second).Should(Equal(0))
	})

	// gitTrigger creates a Pipeline, a ready GitRepository and a trigger on it
	// whose run template uses generateName (so the run name never dedups).
	gitTrigger := func(prefix string) string {
		pipelineName := prefix + "-pipeline"
		task := &tektondevv1.Task{
			ObjectMeta: v1.ObjectMeta{Name: pipelineName + "-task", Namespace: isoNamespace},
			Spec:       tektondevv1.TaskSpec{Params: []tektondevv1.ParamSpec{{Name: "p"}}},
		}
		Expect(k8sClient.Create(ctx, task)).To(Succeed())
		Expect(k8sClient.Create(ctx, &tektondevv1.Pipeline{
			ObjectMeta: v1.ObjectMeta{Name: pipelineName, Namespace: isoNamespace},
			Spec: tektondevv1.PipelineSpec{Tasks: []tektondevv1.PipelineTask{{
				Name: pipelineName + "-task", TaskRef: &tektondevv1.TaskRef{Name: pipelineName + "-task"},
			}}},
		})).To(Succeed())

		repoName := prefix + "-repo"
		Expect(k8sClient.Create(ctx, &sourcev1.GitRepository{
			ObjectMeta: v1.ObjectMeta{Name: repoName, Namespace: isoNamespace},
			Spec:       sourcev1.GitRepositorySpec{URL: "http://github.com/org/repo.git", Interval: v1.Duration{}},
		})).To(Succeed())
		got := &sourcev1.GitRepository{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: repoName, Namespace: isoNamespace}, got)).To(Succeed())
		got.Status = sourcev1.GitRepositoryStatus{
			Artifact: &meta.Artifact{
				Path:           "gitrepository/default/" + repoName + "/1a2b3c4.tar.gz",
				URL:            "http://source-controller.local/gitrepository/default/" + repoName + "/1a2b3c4.tar.gz",
				Revision:       "main@sha1:1a2b3c4fffffffffffffffffffffffffffffffff",
				Digest:         "sha256:1a2b3c4fffffffffffffffffffffffffffffffff",
				LastUpdateTime: v1.Now(),
			},
			Conditions: []v1.Condition{{
				Type: "Ready", Status: v1.ConditionTrue, Reason: v1.StatusSuccess, Message: "Success",
				ObservedGeneration: 1, LastTransitionTime: v1.Now(),
			}},
		}
		Expect(k8sClient.Status().Update(ctx, got)).To(Succeed())

		var tpl unstructured.Unstructured
		tpl.SetAPIVersion("tekton.dev/v1beta1")
		tpl.SetKind("PipelineRun")
		tpl.SetNamespace(isoNamespace)
		tpl.SetGenerateName(prefix + "-run-")
		tpl.SetLabels(map[string]string{"tekton.dev/pipeline": pipelineName})
		tpl.Object["spec"] = map[string]interface{}{
			"pipelineRef": map[string]interface{}{"name": pipelineName},
			"params":      []interface{}{map[string]interface{}{"name": "p", "value": "v"}},
		}
		triggerName := prefix + "-trigger"
		Expect(k8sClient.Create(ctx, &pipelinev1alpha1.PipelineTrigger{
			TypeMeta:   v1.TypeMeta{Kind: "PipelineTrigger", APIVersion: "pipeline.jquad.rocks/v1alpha1"},
			ObjectMeta: v1.ObjectMeta{Name: triggerName, Namespace: isoNamespace},
			Spec: pipelinev1alpha1.PipelineTriggerSpec{
				Source:      pipelinev1alpha1.Source{APIVersion: "source.toolkit.fluxcd.io/v1", Kind: "GitRepository", Name: repoName},
				PipelineRun: tpl,
			},
		})).To(Succeed())
		return triggerName
	}

	reconcileOnce := func(r *PipelineTriggerReconciler, name string) error {
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: isoNamespace}})
		return err
	}

	It("sends no full PipelineRun LIST to the API server across reconciles", func() {
		trigger := gitTrigger("cache-list")

		counter := &prListCounter{}
		countedCfg := counter.config()
		// A manager-equivalent cache + cache-backed client, with the cache
		// options the manager is built with (cmd/main.go).
		informers, err := cache.New(countedCfg, cache.Options{Scheme: scheme.Scheme, ByObject: CacheOptions().ByObject})
		Expect(err).NotTo(HaveOccurred())
		cacheCtx, stop := context.WithCancel(ctx)
		defer stop()
		go func() { _ = informers.Start(cacheCtx) }()
		_, err = informers.GetInformer(ctx, &tektondevv1.PipelineRun{})
		Expect(err).NotTo(HaveOccurred())
		Expect(informers.WaitForCacheSync(ctx)).To(BeTrue())
		cachedClient, err := client.New(countedCfg, client.Options{Scheme: scheme.Scheme, Cache: &client.CacheOptions{Reader: informers}})
		Expect(err).NotTo(HaveOccurred())
		countedDynamic, err := dynamic.NewForConfig(countedCfg)
		Expect(err).NotTo(HaveOccurred())

		r := &PipelineTriggerReconciler{
			Client:        cachedClient,
			DynamicClient: *countedDynamic,
			Scheme:        scheme.Scheme,
			recorder:      record.NewFakeRecorder(1000),
		}
		baseUnfiltered, baseFiltered, _ := counter.counts()

		By("the first reconcile creates the run")
		Expect(reconcileOnce(r, trigger)).To(Succeed())
		Expect(ownedRuns(trigger)).To(HaveLen(1))
		Eventually(func() int {
			l := &tektondevv1.PipelineRunList{}
			Expect(cachedClient.List(ctx, l, client.InNamespace(isoNamespace))).To(Succeed())
			n := 0
			for i := range l.Items {
				if o := v1.GetControllerOf(&l.Items[i]); o != nil && o.Name == trigger {
					n++
				}
			}
			return n
		}, 10*time.Second, 100*time.Millisecond).Should(Equal(1), "the informer must observe the created run")

		By("further reconciles see the run (from the cache) and create nothing")
		for i := 0; i < 3; i++ {
			Expect(reconcileOnce(r, trigger)).To(Succeed())
		}
		Expect(ownedRuns(trigger)).To(HaveLen(1))

		unfiltered, filtered, _ := counter.counts()
		Expect(unfiltered-baseUnfiltered).To(Equal(0), "4 reconciles must not LIST all PipelineRuns from the API server")
		Expect(filtered-baseFiltered).To(Equal(1), "exactly one label-filtered live check, for the one create")

		By("the cached copy is the trimmed one")
		live := &tektondevv1.PipelineRun{}
		run := ownedRuns(trigger)[0]
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(&run), live)).To(Succeed())
		Expect(live.ManagedFields).NotTo(BeEmpty(), "control: the API object carries managedFields")
		cached := &tektondevv1.PipelineRun{}
		Expect(cachedClient.Get(ctx, client.ObjectKeyFromObject(&run), cached)).To(Succeed())
		Expect(cached.ManagedFields).To(BeEmpty())
		Expect(cached.Spec.PipelineRef).To(BeNil())
		Expect(cached.Labels).To(Equal(live.Labels))
		Expect(cached.OwnerReferences).To(Equal(live.OwnerReferences))

		By("positive control: the counter sees the old per-reconcile LIST path")
		_, err = ListTektonResources(ctx, *countedDynamic, isoNamespace, "tekton.dev", "v1beta1", "pipelineruns")
		Expect(err).NotTo(HaveOccurred())
		after, _, _ := counter.counts()
		Expect(after - unfiltered).To(Equal(1))
	})

	// Scope: this covers ONLY Reconcile's own handling of a failed
	// listPipelineRuns (return the error, create nothing). It uses an
	// UNCACHED client (client.New), which production never does: in the
	// manager the list is served from the informer cache. Production's cached
	// F4 path — a failed live re-check before a create — is covered by
	// pkg/source TestStartPipelineRuns_LiveLookupFailureDoesNotCreate.
	It("Reconcile returns the error and creates nothing when listPipelineRuns fails (uncached client)", func() {
		trigger := gitTrigger("cache-fail")

		By("a healthy reconcile creates the run")
		healthy := &PipelineTriggerReconciler{
			Client:        k8sClient,
			DynamicClient: *dynamicClient,
			Scheme:        scheme.Scheme,
			recorder:      record.NewFakeRecorder(1000),
		}
		Expect(reconcileOnce(healthy, trigger)).To(Succeed())
		Expect(ownedRuns(trigger)).To(HaveLen(1))

		By("the trigger status is lost, so only the run list can de-dup")
		pt := &pipelinev1alpha1.PipelineTrigger{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: trigger, Namespace: isoNamespace}, pt)).To(Succeed())
		pt.Status = pipelinev1alpha1.PipelineTriggerStatus{}
		Expect(k8sClient.Status().Update(ctx, pt)).To(Succeed())

		By("every full PipelineRun LIST fails")
		counter := &prListCounter{failUnfiltered: true}
		failingCfg := counter.config()
		failingClient, err := client.New(failingCfg, client.Options{Scheme: scheme.Scheme})
		Expect(err).NotTo(HaveOccurred())
		failingDynamic, err := dynamic.NewForConfig(failingCfg)
		Expect(err).NotTo(HaveOccurred())
		r := &PipelineTriggerReconciler{
			Client:        failingClient,
			DynamicClient: *failingDynamic,
			Scheme:        scheme.Scheme,
			recorder:      record.NewFakeRecorder(1000),
		}
		var errs []error
		for i := 0; i < 3; i++ {
			errs = append(errs, reconcileOnce(r, trigger))
		}
		_, _, injected := counter.counts()
		Expect(injected).To(BeNumerically(">=", 3), "control: the injected failure must actually be hit")
		Expect(ownedRuns(trigger)).To(HaveLen(1), "a failed list must not let a duplicate through")
		for _, e := range errs {
			Expect(e).To(HaveOccurred(), "the list failure must be returned so the reconcile is retried")
		}
	})
})
