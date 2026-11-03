package v1alpha1

import (
	"context"
	"fmt"
	"strings"

	pipelinev1alpha1 "github.com/jquad-group/pipeline-trigger-operator/api/v1alpha1"
	tektondevv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// lastAppliedAnnotation holds a full copy of the object when a run was created
// with `kubectl apply`; nothing here reads it.
const lastAppliedAnnotation = "kubectl.kubernetes.io/last-applied-configuration"

// TransformPipelineRunForCache is the informer-cache transform for Tekton
// PipelineRuns. The manager keeps every PipelineRun of the cluster in its cache
// (the controller watches them), and a PipelineRun is dominated by fields this
// operator never reads: status.pipelineSpec (the fully resolved pipeline),
// status.childReferences / results / provenance, spec.pipelineSpec, params,
// workspaces, taskRunSpecs, and managedFields.
//
// It keeps exactly what the readers of the cached objects use:
//   - metadata (minus managedFields and the last-applied annotation):
//     name, namespace, uid, resourceVersion (informer bookkeeping),
//     creationTimestamp (betterRun), labels (findExistingRun, every
//     CalculateCurrentState / SetCurrentPipelineRunName HasIntersection),
//     ownerReferences (findExistingRun's controller-owner check, the
//     EnqueueRequestForOwner watch handler and the pipelineRunField index),
//     annotations and finalizers (kept; small, and dropping them would make a
//     cached run differ from the API object for any future reader);
//   - spec.status (isUnusableRun: a cancelled/stopped run);
//   - status.Status, i.e. conditions, observedGeneration and status
//     annotations (isUnusableRun, every SetCurrentPipelineRunStatus, and
//     StatusChangePredicate, which compares the whole status.Status).
//
// Objects of any other type are passed through unchanged. The function builds a
// fresh object, so the large one is released, and is idempotent.
func TransformPipelineRunForCache(obj interface{}) (interface{}, error) {
	in, ok := obj.(*tektondevv1.PipelineRun)
	if !ok {
		return obj, nil
	}
	out := &tektondevv1.PipelineRun{TypeMeta: in.TypeMeta}
	out.ObjectMeta = v1.ObjectMeta{
		Name:                       in.Name,
		GenerateName:               in.GenerateName,
		Namespace:                  in.Namespace,
		UID:                        in.UID,
		ResourceVersion:            in.ResourceVersion,
		Generation:                 in.Generation,
		CreationTimestamp:          in.CreationTimestamp,
		DeletionTimestamp:          in.DeletionTimestamp,
		DeletionGracePeriodSeconds: in.DeletionGracePeriodSeconds,
		Labels:                     in.Labels,
		OwnerReferences:            in.OwnerReferences,
		Finalizers:                 in.Finalizers,
	}
	if len(in.Annotations) > 0 {
		ann := make(map[string]string, len(in.Annotations))
		for k, v := range in.Annotations {
			if k != lastAppliedAnnotation {
				ann[k] = v
			}
		}
		out.Annotations = ann
	}
	out.Spec.Status = in.Spec.Status
	out.Status.Status = in.Status.Status
	return out, nil
}

// PipelineRunsToUnstructured converts a (cached) typed PipelineRun list into the
// unstructured list the source subscribers and StartPipelineRuns consume. An
// empty status map is dropped so that a run Tekton has not yet given a status
// looks as it does from the API (no status key).
func PipelineRunsToUnstructured(in *tektondevv1.PipelineRunList) (unstructured.UnstructuredList, error) {
	out := unstructured.UnstructuredList{}
	out.SetAPIVersion(tektondevv1.SchemeGroupVersion.String())
	out.SetKind("PipelineRunList")
	out.Items = make([]unstructured.Unstructured, 0, len(in.Items))
	for i := range in.Items {
		m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&in.Items[i])
		if err != nil {
			return unstructured.UnstructuredList{}, fmt.Errorf("converting PipelineRun %s/%s: %w", in.Items[i].Namespace, in.Items[i].Name, err)
		}
		if st, ok := m["status"].(map[string]interface{}); ok && len(st) == 0 {
			delete(m, "status")
		}
		u := unstructured.Unstructured{Object: m}
		u.SetAPIVersion(tektondevv1.SchemeGroupVersion.String())
		u.SetKind("PipelineRun")
		out.Items = append(out.Items, u)
	}
	return out, nil
}

// sourceLabels returns the pipeline.jquad.rocks/* labels of a prepared run:
// the labels findExistingRun matches on.
func sourceLabels(pr *unstructured.Unstructured) map[string]string {
	want := map[string]string{}
	for k, v := range pr.GetLabels() {
		if strings.HasPrefix(k, sourceLabelPrefix) {
			want[k] = v
		}
	}
	return want
}

// findLiveRun re-checks, against the API server, whether this trigger already
// owns a run for pr's source revision. It is called only when the (cached)
// list had no match, i.e. right before a create, and lists only the runs that
// carry pr's pipeline.jquad.rocks/* labels, so it costs one small,
// server-filtered LIST per create attempt — never one full-namespace LIST per
// reconcile. It closes the informer's read-after-create window: a run created
// moments ago (or a create that timed out but landed) may not be in the cache
// yet. A nil result with a nil error means "no such run"; an error means the
// caller must NOT create (fail closed).
func findLiveRun(ctx context.Context, live client.Reader, pt *pipelinev1alpha1.PipelineTrigger, pr *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	want := sourceLabels(pr)
	if len(want) == 0 {
		// findExistingRun can never match a run without source labels.
		return nil, nil
	}
	list := &unstructured.UnstructuredList{}
	gvk := pr.GroupVersionKind()
	gvk.Kind += "List"
	list.SetGroupVersionKind(gvk)
	if err := live.List(ctx, list, client.InNamespace(pt.Namespace), client.MatchingLabels(want)); err != nil {
		return nil, err
	}
	return findExistingRun(pt, pr, *list), nil
}
