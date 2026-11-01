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

// createFailedCondition is the condition recorded on a source slot whose
// PipelineRun create was permanently rejected. Its presence makes
// CreatePipelineRunResource skip the slot until the source moves to a new
// revision/commit (GetLatestEvent then replaces the slot condition-less).
func createFailedCondition(pt *pipelinev1alpha1.PipelineTrigger, err error) v1.Condition {
	return v1.Condition{
		Type:               apis.ReconcileError,
		LastTransitionTime: v1.Now(),
		ObservedGeneration: pt.GetGeneration(),
		Reason:             apis.ReconcileErrorReason,
		Status:             v1.ConditionFalse,
		Message:            "PipelineRun create rejected permanently (not retried until a new revision): " + err.Error(),
	}
}

// StartResult summarises one StartPipelineRuns call.
type StartResult struct {
	Started int
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
func StartPipelineRuns(ctx context.Context, c client.Client, sub SourceSubscriber, pt *pipelinev1alpha1.PipelineTrigger, prs []*unstructured.Unstructured, statusBefore *pipelinev1alpha1.PipelineTriggerStatus, req ctrl.Request, event func(eventtype, reason, message string)) StartResult {
	var res StartResult
	for _, pr := range prs {
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
