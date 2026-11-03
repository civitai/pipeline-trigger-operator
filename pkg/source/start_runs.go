package v1alpha1

import (
	"context"

	pipelinev1alpha1 "github.com/jquad-group/pipeline-trigger-operator/api/v1alpha1"
	apis "github.com/jquad-group/pipeline-trigger-operator/pkg/status"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// IsPermanentCreateError reports whether a PipelineRun create error will fail
// identically on every retry: the API server (or an admission webhook, whose
// denials default to 400) rejected the OBJECT, not the request. Retrying such
// an error only replays the same rejection forever — the 2026-10-31 storm was
// an invalid label value retried with requeue. Transient errors (webhook
// unreachable -> 500 InternalError, timeouts, 5xx, conflicts) are NOT
// permanent and keep the retry-with-backoff behaviour of the 2026-07-05 fix.
func IsPermanentCreateError(err error) bool {
	return apierrors.IsInvalid(err) || apierrors.IsBadRequest(err)
}

// CreateRejectedReason marks a slot whose PipelineRun create was rejected
// permanently. It is distinct from the generic ReconcileErrorReason so that
// ONLY these slots are retried after a spec edit (see slotEligibleForCreate);
// slots carrying an old generic Error must not all re-fire.
const CreateRejectedReason = "CreateRejected"

// slotEligibleForCreate reports whether a source slot should get a new
// PipelineRun: eligible when it has no conditions (a new revision), or
// retry+eligible when its rejection was recorded with CreateRejectedReason at
// an OLDER trigger generation — the rejection may have come from the template,
// and the spec has since been edited, so try once more. A retry that is
// rejected again is recorded at the new generation and waits for the next edit.
func slotEligibleForCreate(conds []v1.Condition, generation int64) (retry bool, eligible bool) {
	if len(conds) == 0 {
		return false, true
	}
	for _, c := range conds {
		if c.Type == apis.ReconcileError && c.Reason == CreateRejectedReason && c.ObservedGeneration < generation {
			return true, true
		}
	}
	return false, false
}

// createFailedCondition is the condition recorded on a source slot whose
// PipelineRun create was permanently rejected. Its presence makes
// CreatePipelineRunResource skip the slot until the source moves to a new
// revision/commit (GetLatestEvent then replaces the slot condition-less) or the
// trigger spec is edited (generation moves past ObservedGeneration).
func createFailedCondition(pt *pipelinev1alpha1.PipelineTrigger, err error) v1.Condition {
	return v1.Condition{
		Type:               apis.ReconcileError,
		LastTransitionTime: v1.Now(),
		ObservedGeneration: pt.GetGeneration(),
		Reason:             CreateRejectedReason,
		Status:             v1.ConditionFalse,
		Message:            "PipelineRun create rejected permanently (retried on a new revision or a PipelineTrigger spec change): " + err.Error(),
	}
}

const sourceLabelPrefix = "pipeline.jquad.rocks/"

// findExistingRun returns the run in `existing` that this trigger already
// created for the same source revision as `pr`, or nil. A match requires
//   - the same controller owner (kind PipelineTrigger, same name, and same UID
//     when both are known), so two triggers on one source never suppress each
//     other's runs, and
//   - identical values for every pipeline.jquad.rocks/* label on `pr` (branch
//     name + commit for a PullRequest; branch + commit for a GitRepository;
//     image + version for an ImagePolicy). These come from the same
//     sanitising builders on both sides, so a sanitised run still matches.
//
// tekton.dev/pipeline is deliberately not required: Tekton's controller adds
// it some time after the create, so requiring it would miss a run created
// moments ago. The owner check is what scopes the match to this trigger (and
// thereby to its pipeline).
//
// A new commit value — a push, or the pullrequest-operator folding a changed
// label-set hash into Branch.Commit — changes pr.branch.commit and so never
// matches an older run.
//
// Several runs can match (e.g. duplicates from an earlier storm). The choice is
// deterministic: runs that are not unusable (see isUnusableRun) first, then the
// newest creationTimestamp, then the greatest name. A cancelled run is adopted
// only if no usable one exists.
func findExistingRun(pt *pipelinev1alpha1.PipelineTrigger, pr *unstructured.Unstructured, existing unstructured.UnstructuredList) *unstructured.Unstructured {
	want := sourceLabels(pr)
	if len(want) == 0 {
		return nil
	}
	var best *unstructured.Unstructured
	for i := range existing.Items {
		item := &existing.Items[i]
		owner := v1.GetControllerOf(item)
		if owner == nil || owner.Kind != "PipelineTrigger" || owner.Name != pt.GetName() {
			continue
		}
		if owner.UID != "" && pt.GetUID() != "" && owner.UID != pt.GetUID() {
			continue
		}
		labels := item.GetLabels()
		match := true
		for k, v := range want {
			if got, ok := labels[k]; !ok || got != v {
				match = false
				break
			}
		}
		if match && (best == nil || betterRun(item, best)) {
			best = item
		}
	}
	return best
}

func betterRun(a, b *unstructured.Unstructured) bool {
	if ua, ub := isUnusableRun(a), isUnusableRun(b); ua != ub {
		return !ua
	}
	ta, tb := a.GetCreationTimestamp(), b.GetCreationTimestamp()
	if !ta.Equal(&tb) {
		return tb.Before(&ta)
	}
	return a.GetName() > b.GetName()
}

// Tekton reasons for a run that was cancelled/stopped or never started (it
// failed validation/resolution before executing anything). Such a run is not
// a real result for the revision. Known-reason list: an unlisted reason is
// treated as usable, which only affects the preference order, never whether
// a run is adopted.
var unusableRunReasons = map[string]bool{
	"Cancelled": true, "PipelineRunCancelled": true, "CancelledRunFinally": true, "StoppedRunFinally": true,
	"CouldntGetPipeline": true, "CouldntGetTask": true, "PipelineValidationFailed": true,
	"PipelineInvalidGraph": true, "ParameterMissing": true, "ParameterTypeMismatch": true,
	"InvalidWorkspaceBindings": true, "CreateRunFailed": true,
}

func isUnusableRun(u *unstructured.Unstructured) bool {
	if st, _, _ := unstructured.NestedString(u.Object, "spec", "status"); unusableRunReasons[st] {
		return true
	}
	conds, _, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
	for _, c := range conds {
		m, ok := c.(map[string]interface{})
		if !ok || m["type"] != "Succeeded" {
			continue
		}
		if reason, _ := m["reason"].(string); unusableRunReasons[reason] {
			return true
		}
	}
	return false
}

// StartResult summarises one StartPipelineRuns call.
type StartResult struct {
	Started int
	// Existing counts runs NOT created because this trigger already owns a
	// run for the same source revision (see findExistingRun).
	Existing int
	// Permanent counts runs whose create was rejected permanently; their slot
	// carries an Error condition and is NOT retried for the same revision.
	Permanent int
	// TransientErr is the last transient create error, or nil. When non-nil
	// the caller must requeue: the failed slots were rolled back so the same
	// revision is seen as new again.
	TransientErr error
}

// StartPipelineRuns creates each prepared PipelineRun and settles the trigger
// status PER RUN:
//   - success: the run name is recorded on its slot (SetCurrentPipelineRunName)
//   - transient failure: ONLY that run's slot is rolled back to statusBefore so
//     the revision is retried on requeue; every other slot keeps its advance
//   - permanent failure: the slot keeps the advanced revision and gets an Error
//     condition, so it is not retried until the source changes
//
// The old behaviour rolled back the WHOLE status on any failure, so one
// branch's permanent error replayed the creates of every other branch of the
// same PullRequest on each requeue.
//
// `existing` is the (informer-cached, possibly slightly stale) PipelineRun list.
// When it has no match for a run, `live` (a reader that goes to the API server)
// is asked once, label-filtered, before creating — see findLiveRun. If that
// lookup fails the run is NOT created (fail closed: an unknown view must never
// disable the de-dup), and the slot is settled like a failed create: a
// transient error is rolled back and retried on requeue, a permanent one
// (IsPermanentCreateError, e.g. a 400 from an invalid label selector) is
// recorded as CreateRejected and not retried for the same revision.
func StartPipelineRuns(ctx context.Context, c client.Client, live client.Reader, sub SourceSubscriber, pt *pipelinev1alpha1.PipelineTrigger, prs []*unstructured.Unstructured, existing unstructured.UnstructuredList, statusBefore *pipelinev1alpha1.PipelineTriggerStatus, req ctrl.Request, event func(eventtype, reason, message string)) StartResult {
	var res StartResult
	for _, pr := range prs {
		prior := findExistingRun(pt, pr, existing)
		if prior == nil {
			var errLive error
			prior, errLive = findLiveRun(ctx, live, pt, pr)
			if errLive != nil {
				event(core.EventTypeWarning, "Error", "Not creating PipelineRun: cannot check for an existing run: "+errLive.Error())
				permanent := IsPermanentCreateError(errLive)
				sub.SettleFailedRun(pt, statusBefore, pr, errLive, permanent)
				if permanent {
					res.Permanent++
				} else {
					res.TransientErr = errLive
				}
				continue
			}
		}
		if prior != nil {
			// This trigger already created a run for exactly this source
			// revision (same pipeline.jquad.rocks/* labels): re-adopt it
			// instead of creating a duplicate. Without this, the branch status
			// was the only de-dup, and any rolled-back/lost status re-created
			// the run.
			res.Existing++
			sub.SetCurrentPipelineRunName(ctx, c, prior, prior.GetName(), pt)
			event(core.EventTypeNormal, "Info", "PipelineRun "+prior.GetName()+" already exists for this revision; not re-creating")
			continue
		}
		instanceName, _, errRun := pt.StartPipelineRun(pr, ctx, req, c)
		if errRun != nil {
			event(core.EventTypeWarning, "Error", errRun.Error())
			permanent := IsPermanentCreateError(errRun)
			sub.SettleFailedRun(pt, statusBefore, pr, errRun, permanent)
			if permanent {
				res.Permanent++
			} else {
				res.TransientErr = errRun
			}
			continue
		}
		res.Started++
		sub.SetCurrentPipelineRunName(ctx, c, pr, instanceName, pt)
		event(core.EventTypeNormal, "Info", "Started the pipeline "+instanceName+" in namespace "+pt.Namespace)
	}
	return res
}
