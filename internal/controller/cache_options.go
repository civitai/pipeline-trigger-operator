package controllers

import (
	sourceApi "github.com/jquad-group/pipeline-trigger-operator/pkg/source"
	tektondevv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CacheOptions is the manager cache configuration the reconciler relies on:
// the PipelineRun informer (shared by the PipelineRun watch, the
// pipelineRunField index and listPipelineRuns) stores trimmed objects — see
// sourceApi.TransformPipelineRunForCache for what is kept and why.
func CacheOptions() cache.Options {
	return cache.Options{
		ByObject: map[client.Object]cache.ByObject{
			&tektondevv1.PipelineRun{}: {Transform: sourceApi.TransformPipelineRunForCache},
		},
	}
}
