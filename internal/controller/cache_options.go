package controllers

import (
	sourceApi "github.com/jquad-group/pipeline-trigger-operator/pkg/source"
	tektondevv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
)

// ManagerOptions builds the manager options cmd/main.go starts with, so that
// the cache wiring below is under test (TestManagerOptions_TrimsPipelineRunCache).
func ManagerOptions(scheme *runtime.Scheme, metricsAddr, probeAddr string, leaderElect bool) ctrl.Options {
	return ctrl.Options{
		Scheme:                 scheme,
		Metrics:                server.Options{BindAddress: metricsAddr},
		WebhookServer:          webhook.NewServer(webhook.Options{Port: 9443}),
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         leaderElect,
		LeaderElectionID:       "bb9e0b30.jquad.rocks",
		Cache:                  CacheOptions(),
	}
}

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
