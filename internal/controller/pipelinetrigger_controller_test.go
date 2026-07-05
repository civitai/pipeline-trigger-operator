/*
Copyright 2021.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"fmt"
	"strings"
	"time"

	imagereflectorv1 "github.com/fluxcd/image-reflector-controller/api/v1"
	"github.com/fluxcd/pkg/apis/meta"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	tektondevv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"

	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pipelinev1alpha1 "github.com/jquad-group/pipeline-trigger-operator/api/v1alpha1"
	opstatus "github.com/jquad-group/pipeline-trigger-operator/pkg/status"
	pullrequestv1alpha1 "github.com/jquad-group/pullrequest-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
)

var taskMock tektondevv1.Task
var pipelineMock tektondevv1.Pipeline
var gitRepository sourcev1.GitRepository
var imagePolicy imagereflectorv1.ImagePolicy
var pullrequest pullrequestv1alpha1.PullRequest
var pipelineRun1 unstructured.Unstructured
var pipelineRun2 unstructured.Unstructured
var pipelineRun3 unstructured.Unstructured
var pipelineTriggerReconciler *PipelineTriggerReconciler

// var _ = Describe("PipelineTrigger controller", FlakeAttempts(5), func() {
var _ = Describe("PipelineTrigger controller", func() {
	const (
		gitRepositoryName    = "git-repo-1"
		imagePolicyName      = "image-policy-1"
		pullrequestName      = "pullrequest-1"
		pipelineTriggerName0 = "pipeline-trigger-0"
		pipelineTriggerName1 = "pipeline-trigger-1"
		pipelineTriggerName2 = "pipeline-trigger-2"
		pipelineTriggerName3 = "pipeline-trigger-3"
		pipelineTriggerName4 = "pipeline-trigger-4"
		pipelineTriggerName5 = "pipeline-trigger-5"
		taskName             = "build"
		pipelineName         = "build-and-push"
		namespace            = "default"
	)

	BeforeEach(func() {
		pipelineTriggerReconciler = &PipelineTriggerReconciler{
			Client:        k8sClient,
			DynamicClient: *dynamicClient,
			Scheme:        k8sClient.Scheme(),
			recorder:      record.NewFakeRecorder(100),
		}

		pipelineRun1.SetAPIVersion("tekton.dev/v1beta1")
		pipelineRun1.SetKind("PipelineRun")
		//pipelineRun1.SetName("does-not-exist")
		pipelineRun1.SetNamespace(namespace)
		pipelineRun1.Object["spec"] = map[string]interface{}{
			"pipelineRef": map[string]interface{}{
				"name": "does-not-exist",
			},
			"params": []interface{}{
				map[string]interface{}{
					"name":  "param1",
					"value": "value1",
				},
				map[string]interface{}{
					"name":  "param2",
					"value": "value2",
				},
			},
		}
		pipelineRun2.SetAPIVersion("tekton.dev/v1beta1")
		pipelineRun2.SetKind("PipelineRun")
		//pipelineRun2.SetGenerateName(pipelineName)
		pipelineRun2.SetNamespace(namespace)
		pipelineRun2.Object["spec"] = map[string]interface{}{
			"pipelineRef": map[string]interface{}{
				"name": pipelineName,
			},
			"params": []interface{}{
				map[string]interface{}{
					"name":  "param1",
					"value": "value1",
				},
				map[string]interface{}{
					"name":  "param2",
					"value": "value2",
				},
			},
		}

		pipelineRun3.SetAPIVersion("tekton.dev/v1beta1")
		pipelineRun3.SetKind("PipelineRun")
		//pipelineRun3.SetGenerateName(pipelineName)
		pipelineRun3.SetNamespace(namespace)
		pipelineRun3.Object["spec"] = map[string]interface{}{
			"pipelineRef": map[string]interface{}{
				"name": pipelineName,
			},
			"params": []interface{}{
				map[string]interface{}{
					"name":  "param1",
					"value": "$.id",
				},
				map[string]interface{}{
					"name":  "param2",
					"value": "value2",
				},
			},
		}

		taskMock = tektondevv1.Task{
			ObjectMeta: v1.ObjectMeta{
				Name:      taskName,
				Namespace: namespace,
			},
			Spec: tektondevv1.TaskSpec{
				Params: []tektondevv1.ParamSpec{
					{
						Name: taskName,
					},
				},
			},
		}

		pipelineMock = tektondevv1.Pipeline{
			ObjectMeta: v1.ObjectMeta{
				Name:      pipelineName,
				Namespace: namespace,
			},
			Spec: tektondevv1.PipelineSpec{
				Tasks: []tektondevv1.PipelineTask{
					{
						Name: taskName,
						TaskRef: &tektondevv1.TaskRef{
							Name: taskName,
						},
					},
				},
			},
		}

		gitRepository = sourcev1.GitRepository{
			ObjectMeta: v1.ObjectMeta{
				Name:      gitRepositoryName,
				Namespace: namespace,
			},
			Spec: sourcev1.GitRepositorySpec{
				URL:      "http://github.com/org/repo.git",
				Interval: v1.Duration{},
			},
		}

		imagePolicy = imagereflectorv1.ImagePolicy{
			ObjectMeta: v1.ObjectMeta{
				Name:      imagePolicyName,
				Namespace: namespace,
			},
			Spec: imagereflectorv1.ImagePolicySpec{
				ImageRepositoryRef: meta.NamespacedObjectReference{},
				Policy:             imagereflectorv1.ImagePolicyChoice{},
			},
		}

		pullrequest = pullrequestv1alpha1.PullRequest{
			ObjectMeta: v1.ObjectMeta{
				Name:      pullrequestName,
				Namespace: namespace,
			},
			Spec: pullrequestv1alpha1.PullRequestSpec{
				GitProvider: pullrequestv1alpha1.GitProvider{
					Provider:           "Github",
					InsecureSkipVerify: true,
					Github: pullrequestv1alpha1.Github{
						Url:        "https://github.com/example-org/microservice",
						Owner:      "example-org",
						Repository: "microservice",
					},
				},
				TargetBranch: pullrequestv1alpha1.Branch{
					Name: "main",
				},
				Interval: v1.Duration{},
			},
		}

	})

	Context("PipelineTrigger fails due to different namespace in PipelineRun", func() {
		ctx := context.Background()
		It("Should not be able to create a PipelineRun", func() {

			By("Creating a PipelineTrigger")
			pipelineRunWrongPipelineTrigger := &pipelinev1alpha1.PipelineTrigger{}
			pipelineRunMock := &unstructured.Unstructured{}
			pipelineRunMock.SetAPIVersion("tekton.dev/v1beta1")
			pipelineRunMock.SetKind("PipelineRun")
			pipelineRunMock.SetName("release-pipeline")
			pipelineRunMock.SetNamespace("different-ns")
			err := k8sClient.Get(ctx, types.NamespacedName{Name: "pipelinetrigger-pr-with-wrong-ns", Namespace: namespace}, pipelineRunWrongPipelineTrigger)
			if err != nil && errors.IsNotFound(err) {
				// Let's mock our custom resource at the same way that we would
				// apply on the cluster the manifest under config/samples
				pipelineRunWrongPipelineTrigger := &pipelinev1alpha1.PipelineTrigger{
					TypeMeta: v1.TypeMeta{
						Kind:       "PipelineTrigger",
						APIVersion: "pipeline.jquad.rocks/v1alpha1",
					},
					ObjectMeta: v1.ObjectMeta{
						Name:      "pipelinetrigger-pr-with-wrong-ns",
						Namespace: namespace,
					},
					Spec: pipelinev1alpha1.PipelineTriggerSpec{
						Source: pipelinev1alpha1.Source{
							APIVersion: "source.toolkit.fluxcd.io/v1",
							Kind:       "GitRepository",
							Name:       gitRepositoryName,
						},
						PipelineRun: *pipelineRunMock,
					},
				}

				err = k8sClient.Create(ctx, pipelineRunWrongPipelineTrigger)
				Expect(err).To(Not(HaveOccurred()))
			}

			By("Checking if the PipelineTrigger was successfully created")
			Eventually(func() error {
				found := &pipelinev1alpha1.PipelineTrigger{}
				return k8sClient.Get(ctx, types.NamespacedName{Name: "pipelinetrigger-pr-with-wrong-ns", Namespace: namespace}, found)
			}, time.Minute, time.Second).Should(Succeed())

			By("Reconciling the created PipelineTrigger")
			_, err = pipelineTriggerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: "pipelinetrigger-pr-with-wrong-ns", Namespace: namespace},
			})
			Expect(err).To(Not(HaveOccurred()))

			By("Get the latest PipelineTrigger version from cluster")
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: "pipelinetrigger-pr-with-wrong-ns", Namespace: namespace}, pipelineRunWrongPipelineTrigger)
			}, time.Minute, time.Second).Should(Succeed())

			By("Checking if latest GitRepository Status Condition of PipelineTrigger instance is set to Error")
			Eventually(func() error {
				if pipelineRunWrongPipelineTrigger.Status.GitRepository.Conditions != nil && len(pipelineRunWrongPipelineTrigger.Status.GitRepository.Conditions) != 0 {
					latestStatusCondition := pipelineRunWrongPipelineTrigger.Status.GitRepository.Conditions[len(pipelineRunWrongPipelineTrigger.Status.GitRepository.Conditions)-1]
					expectedLatestStatusCondition := v1.Condition{
						Type:               "Error",
						LastTransitionTime: latestStatusCondition.LastTransitionTime,
						ObservedGeneration: latestStatusCondition.ObservedGeneration,
						Reason:             "Failed",
						Status:             v1.ConditionFalse,
						Message:            "spec.pipelineRun.metadata.namespace not supported",
					}
					if latestStatusCondition != expectedLatestStatusCondition {
						return fmt.Errorf("The latest status condition added to the PipelineTrigger instance is not as expected")
					}
				}
				return nil
			}, time.Minute, time.Second).Should(Succeed())
		})
	})

	Context("PipelineTrigger fails due to wrong API Version in spec.source", func() {
		ctx := context.Background()
		It("Should not be able to create a PipelineRun", func() {

			By("Creating a PipelineTrigger with wrong api version in spec.source")
			sourceWrongPipelineTrigger := &pipelinev1alpha1.PipelineTrigger{}
			pipelineRunMock := &unstructured.Unstructured{}
			pipelineRunMock.SetAPIVersion("tekton.dev/v1beta1")
			pipelineRunMock.SetKind("PipelineRun")
			pipelineRunMock.SetNamespace(namespace)
			err := k8sClient.Get(ctx, types.NamespacedName{Name: "source-wrong-pipelinetrigger", Namespace: namespace}, sourceWrongPipelineTrigger)
			if err != nil && errors.IsNotFound(err) {
				// Let's mock our custom resource at the same way that we would
				// apply on the cluster the manifest under config/samples
				sourceWrongPipelineTrigger := &pipelinev1alpha1.PipelineTrigger{
					TypeMeta: v1.TypeMeta{
						Kind:       "PipelineTrigger",
						APIVersion: "pipeline.jquad.rocks/v1alpha1",
					},
					ObjectMeta: v1.ObjectMeta{
						Name:      "source-wrong-pipelinetrigger",
						Namespace: namespace,
					},
					Spec: pipelinev1alpha1.PipelineTriggerSpec{
						Source: pipelinev1alpha1.Source{
							APIVersion: "source.toolkit.fluxcd.io",
							Kind:       "GitRepository",
							Name:       gitRepositoryName,
						},
						PipelineRun: *pipelineRunMock,
					},
				}

				err = k8sClient.Create(ctx, sourceWrongPipelineTrigger)
				Expect(err).To(Not(HaveOccurred()))
			}

			By("Checking if the PipelineTrigger was successfully created")
			Eventually(func() error {
				found := &pipelinev1alpha1.PipelineTrigger{}
				return k8sClient.Get(ctx, types.NamespacedName{Name: "source-wrong-pipelinetrigger", Namespace: namespace}, found)
			}, time.Minute, time.Second).Should(Succeed())

			By("Reconciling the created PipelineTrigger")
			_, err = pipelineTriggerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: "source-wrong-pipelinetrigger", Namespace: namespace},
			})
			Expect(err).To(Not(HaveOccurred()))

			By("Get the latest PipelineTrigger version from cluster")
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: "source-wrong-pipelinetrigger", Namespace: namespace}, sourceWrongPipelineTrigger)
			}, time.Minute, time.Second).Should(Succeed())

			By("Checking if latest GitRepository Status Condition of PipelineTrigger instance is set to Unknown")
			Eventually(func() error {
				if sourceWrongPipelineTrigger.Status.GitRepository.Conditions != nil && len(sourceWrongPipelineTrigger.Status.GitRepository.Conditions) != 0 {
					latestStatusCondition := sourceWrongPipelineTrigger.Status.GitRepository.Conditions[len(sourceWrongPipelineTrigger.Status.GitRepository.Conditions)-1]
					expectedLatestStatusCondition := v1.Condition{
						Type:               "Error",
						LastTransitionTime: latestStatusCondition.LastTransitionTime,
						ObservedGeneration: latestStatusCondition.ObservedGeneration,
						Reason:             "Failed",
						Status:             v1.ConditionFalse,
						Message:            "could not split the api version of the source as expected",
					}
					if latestStatusCondition != expectedLatestStatusCondition {
						return fmt.Errorf("The latest status condition added to the PipelineTrigger instance is not as expected")
					}
				}
				return nil
			}, time.Minute, time.Second).Should(Succeed())
		})
	})

	// GitRepository test cases
	Context("PipelineTrigger fails to create a PipelineRun due to missing GitRepository", func() {
		ctx := context.Background()
		It("Should not be able to create a PipelineRun", func() {

			By("Creating a Task")
			Expect(k8sClient.Create(ctx, &taskMock)).Should(Succeed())

			By("Checking if the Task was successfully created")
			Eventually(func() error {
				createdTask := &tektondevv1.Task{}
				return k8sClient.Get(ctx, types.NamespacedName{Name: taskName, Namespace: namespace}, createdTask)
			}, time.Minute, time.Second).Should(Succeed())

			By("Creating a Pipeline, referencing a single Task")
			Expect(k8sClient.Create(ctx, &pipelineMock)).Should(Succeed())

			By("Checking if the Pipeline was successfully created")
			Eventually(func() error {
				createdPipeline := &tektondevv1.Pipeline{}
				return k8sClient.Get(ctx, types.NamespacedName{Name: pipelineName, Namespace: namespace}, createdPipeline)
			}, time.Minute, time.Second).Should(Succeed())

			By("Creating a PipelineTrigger, referencing an existing Pipeline and not existing GitRepository")
			createdPipelineTrigger0 := &pipelinev1alpha1.PipelineTrigger{}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineTriggerName0, Namespace: namespace}, createdPipelineTrigger0)
			if err != nil && errors.IsNotFound(err) {
				// Let's mock our custom resource at the same way that we would
				// apply on the cluster the manifest under config/samples
				createdPipelineTrigger0 := &pipelinev1alpha1.PipelineTrigger{
					TypeMeta: v1.TypeMeta{
						Kind:       "PipelineTrigger",
						APIVersion: "pipeline.jquad.rocks/v1alpha1",
					},
					ObjectMeta: v1.ObjectMeta{
						Name:      pipelineTriggerName0,
						Namespace: namespace,
					},
					Spec: pipelinev1alpha1.PipelineTriggerSpec{
						Source: pipelinev1alpha1.Source{
							APIVersion: "source.toolkit.fluxcd.io/v1",
							Kind:       "GitRepository",
							Name:       "not-existing",
						},
						PipelineRun: pipelineRun1,
					},
				}

				err = k8sClient.Create(ctx, createdPipelineTrigger0)
				Expect(err).To(Not(HaveOccurred()))
			}

			By("Checking if the PipelineTrigger was successfully created")
			Eventually(func() error {
				found := &pipelinev1alpha1.PipelineTrigger{}
				return k8sClient.Get(ctx, types.NamespacedName{Name: pipelineTriggerName0, Namespace: namespace}, found)
			}, time.Minute, time.Second).Should(Succeed())

			By("Reconciling the created PipelineTrigger")
			_, err = pipelineTriggerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pipelineTriggerName0, Namespace: namespace},
			})
			Expect(err).To(Not(HaveOccurred()))

			By("Get the latest PipelineTrigger version from cluster")
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: pipelineTriggerName0, Namespace: namespace}, createdPipelineTrigger0)
			}, time.Minute, time.Second).Should(Succeed())

			By("Checking the latest GitRepository Status Condition added to the PipelineTrigger instance")
			Eventually(func() error {
				if createdPipelineTrigger0.Status.GitRepository.Conditions != nil && len(createdPipelineTrigger0.Status.GitRepository.Conditions) != 0 {
					latestStatusCondition := createdPipelineTrigger0.Status.GitRepository.Conditions[len(createdPipelineTrigger0.Status.GitRepository.Conditions)-1]
					expectedLatestStatusCondition := v1.Condition{
						Type:               "Error",
						Status:             "False",
						Reason:             "Failed",
						LastTransitionTime: latestStatusCondition.LastTransitionTime,
						ObservedGeneration: 1,
						Message:            fmt.Sprintf("gitrepositories.source.toolkit.fluxcd.io \"%s\" not found", createdPipelineTrigger0.Spec.Source.Name),
					}
					if latestStatusCondition != expectedLatestStatusCondition {
						return fmt.Errorf("The latest status condition added to the PipelineTrigger instance is not as expected")
					}
				}
				return nil
			}, time.Minute, time.Second).Should(Succeed())
		})
	})

	Context("PipelineTrigger creates a PipelineRun on the test cluster for GitRepository", func() {
		ctx := context.Background()
		createdGitRepository := &sourcev1.GitRepository{}
		createdPipelineRun := &tektondevv1.PipelineRun{}
		AfterEach(func() {
			By("Removing the created PipelineRun from the PipelineTrigger with the GitRepository")
			err := k8sClient.Delete(ctx, createdPipelineRun)
			Expect(err).To(Not(HaveOccurred()))
		})

		It("Should be able to create a PipelineRun custom resources", func() {

			By("Creating a GitRepository")
			Expect(k8sClient.Create(ctx, &gitRepository)).Should(Succeed())

			By("Checking if the GitRepository was successfully created")
			Eventually(func() error {
				found := &sourcev1.GitRepository{}
				return k8sClient.Get(ctx, types.NamespacedName{Name: gitRepositoryName, Namespace: namespace}, found)
			}, time.Minute, time.Second).Should(Succeed())

			By("Updating the GitRepository status")
			gitRepoStatus := sourcev1.GitRepositoryStatus{
				Artifact: &meta.Artifact{
					Path:           "gitrepository/flux-system/flux-system/dc0fd09d0915f47cbda5f235a8a9c30b2d8baa69.tar.gz",
					URL:            "http://source-controller.flux-system.svc.cluster.local./gitrepository/flux-system/flux-system/dc0fd09d0915f47cbda5f235a8a9c30b2d8baa69.tar.gz",
					Revision:       "main@sha1:dc0fd09d0915f47cbda5f235a8a9c30b2d8baa69",
					Digest:         "sha256:dc0fd09d0915f47cbda5f235a8a9c30b2d8baa69",
					LastUpdateTime: v1.Now(),
				},
				Conditions: []v1.Condition{
					{
						Type:               "Ready",
						Status:             v1.ConditionTrue,
						Reason:             v1.StatusSuccess,
						Message:            "Success",
						ObservedGeneration: 12,
						LastTransitionTime: v1.Now(),
					},
				},
			}

			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: gitRepositoryName, Namespace: namespace}, createdGitRepository)
			}, time.Minute, time.Second).Should(Succeed())

			createdGitRepository.Status = gitRepoStatus
			Expect(k8sClient.Status().Update(ctx, createdGitRepository)).Should(Succeed())

			By("Checking the latest Status Artifact added to the GitRepository instance")
			Eventually(func() error {
				if createdGitRepository.Status.Artifact != nil && len(createdGitRepository.Status.Artifact.Revision) != 0 {
					latestStatusArtifact := createdGitRepository.Status.Artifact
					expectedLatestStatusArtifact := &meta.Artifact{
						Path:     "gitrepository/flux-system/flux-system/dc0fd09d0915f47cbda5f235a8a9c30b2d8baa69.tar.gz",
						URL:      "http://source-controller.flux-system.svc.cluster.local./gitrepository/flux-system/flux-system/dc0fd09d0915f47cbda5f235a8a9c30b2d8baa69.tar.gz",
						Revision: "main@sha1:dc0fd09d0915f47cbda5f235a8a9c30b2d8baa69",
						Digest:   "sha256:dc0fd09d0915f47cbda5f235a8a9c30b2d8baa69",
					}
					if latestStatusArtifact.URL != expectedLatestStatusArtifact.URL && latestStatusArtifact.Path != expectedLatestStatusArtifact.Path && latestStatusArtifact.Revision != expectedLatestStatusArtifact.Revision {
						return fmt.Errorf("The latest status artifact added to the GitRepository instance is not as expected")
					}
				}
				return nil
			}, time.Minute, time.Second).Should(Succeed())

			By("Creating a PipelineTrigger, referencing an existing Pipeline and GitRepository")
			createdPipelineTrigger1 := &pipelinev1alpha1.PipelineTrigger{}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineTriggerName1, Namespace: namespace}, createdPipelineTrigger1)
			if err != nil && errors.IsNotFound(err) {
				// Let's mock our custom resource at the same way that we would
				// apply on the cluster the manifest under config/samples
				createdPipelineTrigger1 := &pipelinev1alpha1.PipelineTrigger{
					TypeMeta: v1.TypeMeta{
						Kind:       "PipelineTrigger",
						APIVersion: "pipeline.jquad.rocks/v1alpha1",
					},
					ObjectMeta: v1.ObjectMeta{
						Name:      pipelineTriggerName1,
						Namespace: namespace,
					},
					Spec: pipelinev1alpha1.PipelineTriggerSpec{
						Source: pipelinev1alpha1.Source{
							APIVersion: "source.toolkit.fluxcd.io/v1",
							Kind:       "GitRepository",
							Name:       gitRepositoryName,
						},
						PipelineRun: pipelineRun2,
					},
				}

				err = k8sClient.Create(ctx, createdPipelineTrigger1)
				Expect(err).To(Not(HaveOccurred()))
			}

			By("Checking if the PipelineTrigger was successfully created")
			Eventually(func() error {
				found := &pipelinev1alpha1.PipelineTrigger{}
				return k8sClient.Get(ctx, types.NamespacedName{Name: pipelineTriggerName1, Namespace: namespace}, found)
			}, time.Minute, time.Second).Should(Succeed())

			By("Reconciling the created PipelineTrigger")
			_, err = pipelineTriggerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pipelineTriggerName1, Namespace: namespace},
			})
			Expect(err).To(Not(HaveOccurred()))

			By("Get the latest PipelineTrigger version from cluster")
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: pipelineTriggerName1, Namespace: namespace}, createdPipelineTrigger1)
			}, time.Minute, time.Second).Should(Succeed())

			By("Checking if latest GitRepository Status Condition of PipelineTrigger instance is set to Unknown")
			Eventually(func() error {
				if createdPipelineTrigger1.Status.GitRepository.Conditions != nil && len(createdPipelineTrigger1.Status.GitRepository.Conditions) != 0 {
					latestStatusCondition := createdPipelineTrigger1.Status.GitRepository.Conditions[len(createdPipelineTrigger1.Status.GitRepository.Conditions)-1]
					expectedLatestStatusCondition := v1.Condition{
						Type:               "Unknown",
						LastTransitionTime: latestStatusCondition.LastTransitionTime,
						ObservedGeneration: latestStatusCondition.ObservedGeneration,
						Reason:             "Unknown",
						Status:             v1.ConditionUnknown,
						Message:            "Unknown",
					}
					if latestStatusCondition != expectedLatestStatusCondition {
						return fmt.Errorf("The latest status condition added to the PipelineTrigger instance is not as expected")
					}
				}
				return nil
			}, time.Minute, time.Second).Should(Succeed())

			By("Checking if the PipelineTrigger controller has started a single pipeline")
			Eventually(func() (int, error) {
				found := &tektondevv1.PipelineRunList{}
				err := k8sClient.List(ctx, found)
				return len(found.Items), err
			}, time.Minute, time.Second).Should(Equal(1))

			By("Checking if the PipelineTrigger controller is managing the PipelineRun")
			pipelineRuns := &tektondevv1.PipelineRunList{}
			Eventually(func() string {
				k8sClient.List(ctx, pipelineRuns)
				pipelineRun := pipelineRuns.Items[0]
				return pipelineRun.GetOwnerReferences()[0].Name
			}, time.Minute, time.Second).Should(Equal(pipelineTriggerName1))

			By("Get the latest PipelineRun version from cluster")
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: pipelineRuns.Items[0].Name, Namespace: namespace}, createdPipelineRun)
			}, time.Minute, time.Second).Should(Succeed())

			By("Updating the PipelineRun status to reason started (status: Unknown)")
			createdPipelineRun.Status.InitializeConditions(clock.RealClock{})
			Expect(k8sClient.Status().Update(ctx, createdPipelineRun)).Should(Succeed())

			// https://tekton.dev/docs/pipelines/pipelineruns/#pipelinerun-status
			By("Checking if the PipelineRun status reason was updated to reason: Started (status: Unknown)")
			Eventually(func() (string, error) {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: createdPipelineRun.Name, Namespace: namespace}, createdPipelineRun)
				return string(createdPipelineRun.Status.GetCondition(apis.ConditionSucceeded).Reason), err
			}, time.Minute, time.Second).Should(Equal("Started"))

			By("Updating the PipelineRun status to reason: Running (status: Unknown)")
			k8sClient.Get(ctx, types.NamespacedName{Name: createdPipelineRun.Name, Namespace: namespace}, createdPipelineRun)
			createdPipelineRun.Status.SetCondition(&apis.Condition{
				Type:    apis.ConditionSucceeded,
				Status:  corev1.ConditionUnknown,
				Reason:  "Running",
				Message: "Tasks Running: 1 (Failed: 0, Cancelled 0), Skipped: 0",
			})
			createdPipelineRun.Status.ObservedGeneration = 2
			Expect(k8sClient.Status().Update(ctx, createdPipelineRun)).Should(Succeed())

			By("Checking if the PipelineRun status was updated to reason: Running (status: Unknown)")
			Eventually(func() (string, error) {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: createdPipelineRun.Name, Namespace: namespace}, createdPipelineRun)
				return string(createdPipelineRun.Status.GetCondition(apis.ConditionSucceeded).Reason), err
			}, time.Minute, time.Second).Should(Equal("Running"))

			By("Reconciling the created PipelineTrigger")
			_, err = pipelineTriggerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pipelineTriggerName1, Namespace: namespace},
			})
			Expect(err).To(Not(HaveOccurred()))

			By("Get the latest PipelineTrigger version from cluster")
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: pipelineTriggerName1, Namespace: namespace}, createdPipelineTrigger1)
			}, time.Minute, time.Second).Should(Succeed())

			By("Checking if latest GitRepository Status Condition of PipelineTrigger instance is set to InProgress")
			Eventually(func() error {
				if createdPipelineTrigger1.Status.GitRepository.Conditions != nil && len(createdPipelineTrigger1.Status.GitRepository.Conditions) != 0 {
					latestStatusCondition := createdPipelineTrigger1.Status.GitRepository.Conditions[len(createdPipelineTrigger1.Status.GitRepository.Conditions)-1]
					expectedLatestStatusCondition := v1.Condition{
						Type:               "InProgress",
						LastTransitionTime: latestStatusCondition.LastTransitionTime,
						ObservedGeneration: latestStatusCondition.ObservedGeneration,
						Reason:             "InProgress",
						Status:             v1.ConditionUnknown,
						Message:            "Progressing",
					}
					if latestStatusCondition != expectedLatestStatusCondition {
						return fmt.Errorf("The latest status condition added to the PipelineTrigger instance is not as expected")
					}
				}
				return nil
			}, time.Minute, time.Second).Should(Succeed())

			By("Updating the PipelineRun status to reason: Succeeded (status: True)")
			k8sClient.Get(ctx, types.NamespacedName{Name: createdPipelineRun.Name, Namespace: namespace}, createdPipelineRun)
			createdPipelineRun.Status.SetCondition(&apis.Condition{
				Type:    apis.ConditionSucceeded,
				Status:  corev1.ConditionTrue,
				Reason:  "Succeeded",
				Message: "Tasks Completed: 1 (Failed: 0, Cancelled 0), Skipped: 0",
			})
			createdPipelineRun.Status.ObservedGeneration = 2
			Expect(k8sClient.Status().Update(ctx, createdPipelineRun)).Should(Succeed())

			By("Checking if the PipelineRun status was updated to reason: Succeeded (status: True)")
			Eventually(func() (corev1.ConditionStatus, error) {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: createdPipelineRun.Name, Namespace: namespace}, createdPipelineRun)
				return createdPipelineRun.Status.GetCondition(apis.ConditionSucceeded).Status, err
			}, time.Minute, time.Second).Should(Equal(corev1.ConditionTrue))

			By("Checking if the PipelineTrigger LatestPipelineRun is correctly set")
			Eventually(func() (string, error) {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineTriggerName1, Namespace: namespace}, createdPipelineTrigger1)
				return createdPipelineTrigger1.Status.GitRepository.LatestPipelineRun, err
			}, time.Minute, time.Second).Should(ContainSubstring("main"))

			By("Reconciling the created PipelineTrigger")
			_, err = pipelineTriggerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pipelineTriggerName1, Namespace: namespace},
			})
			Expect(err).To(Not(HaveOccurred()))

			By("Checking if the PipelineTrigger status is updated to succeeded when the corresponding PipelineRun is completed")
			Eventually(func() (string, error) {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineTriggerName1, Namespace: namespace}, createdPipelineTrigger1)
				cond, _ := createdPipelineTrigger1.Status.GitRepository.GetCondition(opstatus.ReconcileSuccess)
				return cond.Type, err
			}, time.Minute, time.Second).Should(ContainSubstring(opstatus.ReconcileSuccess))

			By("Checking if the PipelineTrigger status conditions = PipelineRuns status changes")
			Eventually(func() (int, error) {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineTriggerName1, Namespace: namespace}, createdPipelineTrigger1)
				return len(createdPipelineTrigger1.Status.GitRepository.Conditions), err
			}, time.Minute, time.Second).Should(Equal(3))

		})
	})

	// ImagePolicy test cases
	Context("PipelineTrigger fails to create a PipelineRun due to missing ImagePolicy", func() {
		ctx := context.Background()
		It("Should not be able to create a PipelineRun", func() {

			By("Creating a PipelineTrigger, referencing an existing Pipeline and not existing ImagePolicy")
			createdPipelineTrigger2 := &pipelinev1alpha1.PipelineTrigger{}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineTriggerName2, Namespace: namespace}, createdPipelineTrigger2)
			if err != nil && errors.IsNotFound(err) {
				// Let's mock our custom resource at the same way that we would
				// apply on the cluster the manifest under config/samples
				createdPipelineTrigger2 := &pipelinev1alpha1.PipelineTrigger{
					TypeMeta: v1.TypeMeta{
						Kind:       "PipelineTrigger",
						APIVersion: "pipeline.jquad.rocks/v1alpha1",
					},
					ObjectMeta: v1.ObjectMeta{
						Name:      pipelineTriggerName2,
						Namespace: namespace,
					},
					Spec: pipelinev1alpha1.PipelineTriggerSpec{
						Source: pipelinev1alpha1.Source{
							APIVersion: "image.toolkit.fluxcd.io/v1beta2",
							Kind:       "ImagePolicy",
							Name:       "not-existing",
						},
						PipelineRun: pipelineRun1,
					},
				}

				err = k8sClient.Create(ctx, createdPipelineTrigger2)
				Expect(err).To(Not(HaveOccurred()))
			}

			By("Checking if the PipelineTrigger was successfully created")
			Eventually(func() error {
				found := &pipelinev1alpha1.PipelineTrigger{}
				return k8sClient.Get(ctx, types.NamespacedName{Name: pipelineTriggerName2, Namespace: namespace}, found)
			}, time.Minute, time.Second).Should(Succeed())

			By("Reconciling the created PipelineTrigger")
			_, err = pipelineTriggerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pipelineTriggerName2, Namespace: namespace},
			})
			Expect(err).To(Not(HaveOccurred()))

			By("Get the latest PipelineTrigger version from cluster")
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: pipelineTriggerName2, Namespace: namespace}, createdPipelineTrigger2)
			}, time.Minute, time.Second).Should(Succeed())

			By("Checking the latest ImagePolicy Status Condition added to the PipelineTrigger instance")
			Eventually(func() error {
				if createdPipelineTrigger2.Status.ImagePolicy.Conditions != nil && len(createdPipelineTrigger2.Status.ImagePolicy.Conditions) != 0 {
					latestStatusCondition := createdPipelineTrigger2.Status.ImagePolicy.Conditions[len(createdPipelineTrigger2.Status.ImagePolicy.Conditions)-1]
					expectedLatestStatusCondition := v1.Condition{
						Type:               "Error",
						Status:             "False",
						Reason:             "Failed",
						LastTransitionTime: latestStatusCondition.LastTransitionTime,
						ObservedGeneration: 1,
						Message:            fmt.Sprintf("imagepolicies.image.toolkit.fluxcd.io \"%s\" not found", createdPipelineTrigger2.Spec.Source.Name)}
					if latestStatusCondition != expectedLatestStatusCondition {
						return fmt.Errorf("The latest status condition added to the PipelineTrigger instance is not as expected")
					}
				}
				return nil
			}, time.Minute, time.Second).Should(Succeed())
		})
	})

	Context("PipelineTrigger creates a PipelineRun on the test cluster for ImagePolicy", func() {
		ctx := context.Background()
		createdImagePolicy := &imagereflectorv1.ImagePolicy{}
		createdPipelineRun := &tektondevv1.PipelineRun{}
		AfterEach(func() {
			By("Removing the created PipelineRun from the PipelineTrigger with the ImagePolicy")
			err := k8sClient.Delete(ctx, createdPipelineRun)
			Expect(err).To(Not(HaveOccurred()))
		})
		It("Should be able to create a PipelineRun custom resources", func() {

			By("Creating a ImagePolicy")
			Expect(k8sClient.Create(ctx, &imagePolicy)).Should(Succeed())

			By("Checking if the ImagePolicy was successfully created")
			Eventually(func() error {
				found := &imagereflectorv1.ImagePolicy{}
				return k8sClient.Get(ctx, types.NamespacedName{Name: imagePolicyName, Namespace: namespace}, found)
			}, time.Minute, time.Second).Should(Succeed())

			By("Updating the ImagePolicy status")
			imagePolicyStatus := imagereflectorv1.ImagePolicyStatus{
				LatestRef: &imagereflectorv1.ImageRef{
					Name: "ghcr.io/test/test",
					Tag:  "v0.0.1",
				},
				Conditions: []v1.Condition{
					{
						Type:               "Ready",
						Status:             v1.ConditionTrue,
						Reason:             "ReconciliationSucceeded",
						Message:            "Latest image tag for 'ghcr.io/test/test' resolved to: v0.0.1",
						LastTransitionTime: v1.Now(),
					},
				},
			}

			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: imagePolicyName, Namespace: namespace}, createdImagePolicy)
			}, time.Minute, time.Second).Should(Succeed())

			createdImagePolicy.Status = imagePolicyStatus
			Expect(k8sClient.Status().Update(ctx, createdImagePolicy)).Should(Succeed())

			By("Checking the latest Status Artifact added to the ImagePolicy instance")
			Eventually(func() error {
				if createdImagePolicy.Status.Conditions != nil && len(createdImagePolicy.Status.Conditions) != 0 {
					latestStatusCondition := createdImagePolicy.Status.Conditions[len(createdImagePolicy.Status.Conditions)-1]
					expectedLatestStatusCondition := v1.Condition{
						Type:               "Ready",
						Status:             v1.ConditionTrue,
						Reason:             "ReconciliationSucceeded",
						LastTransitionTime: latestStatusCondition.LastTransitionTime,
						ObservedGeneration: latestStatusCondition.ObservedGeneration,
						Message:            "Latest image tag for 'ghcr.io/test/test' resolved to: v0.0.1",
					}
					if latestStatusCondition != expectedLatestStatusCondition {
						return fmt.Errorf("The latest status artifact added to the ImagePolicy instance is not as expected")
					}
				}
				return nil
			}, time.Minute, time.Second).Should(Succeed())

			By("Creating a PipelineTrigger, referencing an existing Pipeline and ImagePolicy")
			createdPipelineTrigger3 := &pipelinev1alpha1.PipelineTrigger{}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineTriggerName3, Namespace: namespace}, createdPipelineTrigger3)
			if err != nil && errors.IsNotFound(err) {
				// Let's mock our custom resource at the same way that we would
				// apply on the cluster the manifest under config/samples
				createdPipelineTrigger3 := &pipelinev1alpha1.PipelineTrigger{
					TypeMeta: v1.TypeMeta{
						Kind:       "PipelineTrigger",
						APIVersion: "pipeline.jquad.rocks/v1alpha1",
					},
					ObjectMeta: v1.ObjectMeta{
						Name:      pipelineTriggerName3,
						Namespace: namespace,
					},
					Spec: pipelinev1alpha1.PipelineTriggerSpec{
						Source: pipelinev1alpha1.Source{
							APIVersion: "image.toolkit.fluxcd.io/v1beta2",
							Kind:       "ImagePolicy",
							Name:       imagePolicyName,
						},
						PipelineRun: pipelineRun2,
					},
				}

				err = k8sClient.Create(ctx, createdPipelineTrigger3)
				Expect(err).To(Not(HaveOccurred()))
			}

			By("Checking if the PipelineTrigger was successfully created")
			Eventually(func() error {
				found := &pipelinev1alpha1.PipelineTrigger{}
				return k8sClient.Get(ctx, types.NamespacedName{Name: pipelineTriggerName3, Namespace: namespace}, found)
			}, time.Minute, time.Second).Should(Succeed())

			By("Reconciling the created PipelineTrigger")
			_, err = pipelineTriggerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pipelineTriggerName3, Namespace: namespace},
			})
			Expect(err).To(Not(HaveOccurred()))

			By("Get the latest PipelineTrigger version from cluster")
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: pipelineTriggerName3, Namespace: namespace}, createdPipelineTrigger3)
			}, time.Minute, time.Second).Should(Succeed())

			By("Checking if latest ImagePolicy Status Condition of PipelineTrigger instance is set to Unknown")
			Eventually(func() error {
				if createdPipelineTrigger3.Status.ImagePolicy.Conditions != nil && len(createdPipelineTrigger3.Status.ImagePolicy.Conditions) != 0 {
					latestStatusCondition := createdPipelineTrigger3.Status.ImagePolicy.Conditions[len(createdPipelineTrigger3.Status.ImagePolicy.Conditions)-1]
					expectedLatestStatusCondition := v1.Condition{
						Type:               "Unknown",
						LastTransitionTime: latestStatusCondition.LastTransitionTime,
						ObservedGeneration: latestStatusCondition.ObservedGeneration,
						Reason:             "Unknown",
						Status:             v1.ConditionUnknown,
						Message:            "Unknown",
					}
					if latestStatusCondition != expectedLatestStatusCondition {
						return fmt.Errorf("The latest status condition added to the PipelineTrigger instance is not as expected")
					}
				}
				return nil
			}, time.Minute, time.Second).Should(Succeed())

			By("Checking if the PipelineTrigger controller has started a single pipeline")
			Eventually(func() (int, error) {
				found := &tektondevv1.PipelineRunList{}
				err := k8sClient.List(ctx, found)
				return len(found.Items), err
			}, time.Minute, time.Second).Should(Equal(1))

			By("Checking if the PipelineTrigger controller is managing the PipelineRun")
			pipelineRuns := &tektondevv1.PipelineRunList{}
			Eventually(func() string {
				k8sClient.List(ctx, pipelineRuns)
				pipelineRun := pipelineRuns.Items[0]
				return pipelineRun.GetOwnerReferences()[0].Name
			}, time.Minute, time.Second).Should(Equal(pipelineTriggerName3))

			By("Get the latest PipelineRun version from cluster")
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: pipelineRuns.Items[0].Name, Namespace: namespace}, createdPipelineRun)
			}, time.Minute, time.Second).Should(Succeed())

			By("Updating the PipelineRun status to reason started (status: Unknown)")
			createdPipelineRun.Status.InitializeConditions(clock.RealClock{})
			Expect(k8sClient.Status().Update(ctx, createdPipelineRun)).Should(Succeed())

			// https://tekton.dev/docs/pipelines/pipelineruns/#pipelinerun-status
			By("Checking if the PipelineRun status reason was updated to reason: Started (status: Unknown)")
			Eventually(func() (string, error) {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: createdPipelineRun.Name, Namespace: namespace}, createdPipelineRun)
				return string(createdPipelineRun.Status.GetCondition(apis.ConditionSucceeded).Reason), err
			}, time.Minute, time.Second).Should(Equal("Started"))

			By("Updating the PipelineRun status to reason: Running (status: Unknown)")
			k8sClient.Get(ctx, types.NamespacedName{Name: createdPipelineRun.Name, Namespace: namespace}, createdPipelineRun)
			createdPipelineRun.Status.SetCondition(&apis.Condition{
				Type:    apis.ConditionSucceeded,
				Status:  corev1.ConditionUnknown,
				Reason:  "Running",
				Message: "Tasks Running: 1 (Failed: 0, Cancelled 0), Skipped: 0",
			})
			createdPipelineRun.Status.ObservedGeneration = 2
			Expect(k8sClient.Status().Update(ctx, createdPipelineRun)).Should(Succeed())

			By("Checking if the PipelineRun status was updated to reason: Running (status: Unknown)")
			Eventually(func() (string, error) {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: createdPipelineRun.Name, Namespace: namespace}, createdPipelineRun)
				return string(createdPipelineRun.Status.GetCondition(apis.ConditionSucceeded).Reason), err
			}, time.Minute, time.Second).Should(Equal("Running"))

			By("Reconciling the created PipelineTrigger")
			_, err = pipelineTriggerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pipelineTriggerName3, Namespace: namespace},
			})
			Expect(err).To(Not(HaveOccurred()))

			By("Get the latest PipelineTrigger version from cluster")
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: pipelineTriggerName3, Namespace: namespace}, createdPipelineTrigger3)
			}, time.Minute, time.Second).Should(Succeed())

			By("Checking if latest ImagePolicy Status Condition of PipelineTrigger instance is set to InProgress")
			Eventually(func() error {
				if createdPipelineTrigger3.Status.ImagePolicy.Conditions != nil && len(createdPipelineTrigger3.Status.ImagePolicy.Conditions) != 0 {
					latestStatusCondition := createdPipelineTrigger3.Status.ImagePolicy.Conditions[len(createdPipelineTrigger3.Status.ImagePolicy.Conditions)-1]
					expectedLatestStatusCondition := v1.Condition{
						Type:               "InProgress",
						LastTransitionTime: latestStatusCondition.LastTransitionTime,
						ObservedGeneration: latestStatusCondition.ObservedGeneration,
						Reason:             "InProgress",
						Status:             v1.ConditionUnknown,
						Message:            "Progressing",
					}
					if latestStatusCondition != expectedLatestStatusCondition {
						return fmt.Errorf("The latest status condition added to the PipelineTrigger instance is not as expected")
					}
				}
				return nil
			}, time.Minute, time.Second).Should(Succeed())

			By("Updating the PipelineRun status to reason: Succeeded (status: True)")
			k8sClient.Get(ctx, types.NamespacedName{Name: createdPipelineRun.Name, Namespace: namespace}, createdPipelineRun)
			createdPipelineRun.Status.SetCondition(&apis.Condition{
				Type:    apis.ConditionSucceeded,
				Status:  corev1.ConditionTrue,
				Reason:  "Succeeded",
				Message: "Tasks Completed: 1 (Failed: 0, Cancelled 0), Skipped: 0",
			})
			createdPipelineRun.Status.ObservedGeneration = 2
			Expect(k8sClient.Status().Update(ctx, createdPipelineRun)).Should(Succeed())

			By("Checking if the PipelineRun status was updated to reason: Succeeded (status: True)")
			Eventually(func() (corev1.ConditionStatus, error) {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: createdPipelineRun.Name, Namespace: namespace}, createdPipelineRun)
				return createdPipelineRun.Status.GetCondition(apis.ConditionSucceeded).Status, err
			}, time.Minute, time.Second).Should(Equal(corev1.ConditionTrue))

			By("Checking if the PipelineTrigger LatestPipelineRun is correctly set")
			Eventually(func() (string, error) {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineTriggerName3, Namespace: namespace}, createdPipelineTrigger3)
				return createdPipelineTrigger3.Status.ImagePolicy.LatestPipelineRun, err
			}, time.Minute, time.Second).Should(ContainSubstring("test"))

			By("Reconciling the created PipelineTrigger")
			_, err = pipelineTriggerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pipelineTriggerName3, Namespace: namespace},
			})
			Expect(err).To(Not(HaveOccurred()))

			By("Checking if the PipelineTrigger status is updated to succeeded when the corresponding PipelineRun is completed")
			Eventually(func() (string, error) {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineTriggerName3, Namespace: namespace}, createdPipelineTrigger3)
				cond, _ := createdPipelineTrigger3.Status.ImagePolicy.GetCondition(opstatus.ReconcileSuccess)
				return cond.Type, err
			}, time.Minute, time.Second).Should(ContainSubstring(opstatus.ReconcileSuccess))

			By("Checking if the PipelineTrigger status conditions = PipelineRuns status changes")
			Eventually(func() (int, error) {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineTriggerName3, Namespace: namespace}, createdPipelineTrigger3)
				return len(createdPipelineTrigger3.Status.ImagePolicy.Conditions), err
			}, time.Minute, time.Second).Should(Equal(3))

		})
	})

	// PullRequest test cases
	Context("PipelineTrigger fails to create a PipelineRun due to missing PullRequest", func() {
		ctx := context.Background()
		It("Should not be able to create a PipelineRun", func() {

			By("Creating a PipelineTrigger, referencing an existing Pipeline and not existing PullRequest")
			createdPipelineTrigger4 := &pipelinev1alpha1.PipelineTrigger{}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineTriggerName4, Namespace: namespace}, createdPipelineTrigger4)
			if err != nil && errors.IsNotFound(err) {
				// Let's mock our custom resource at the same way that we would
				// apply on the cluster the manifest under config/samples
				createdPipelineTrigger4 := &pipelinev1alpha1.PipelineTrigger{
					TypeMeta: v1.TypeMeta{
						Kind:       "PipelineTrigger",
						APIVersion: "pipeline.jquad.rocks/v1alpha1",
					},
					ObjectMeta: v1.ObjectMeta{
						Name:      pipelineTriggerName4,
						Namespace: namespace,
					},
					Spec: pipelinev1alpha1.PipelineTriggerSpec{
						Source: pipelinev1alpha1.Source{
							APIVersion: "pipeline.jquad.rocks/v1alpha1",
							Kind:       "PullRequest",
							Name:       "not-existing",
						},
						PipelineRun: pipelineRun1,
					},
				}

				err = k8sClient.Create(ctx, createdPipelineTrigger4)
				Expect(err).To(Not(HaveOccurred()))
			}

			By("Checking if the PipelineTrigger was successfully created")
			Eventually(func() error {
				found := &pipelinev1alpha1.PipelineTrigger{}
				return k8sClient.Get(ctx, types.NamespacedName{Name: pipelineTriggerName4, Namespace: namespace}, found)
			}, time.Minute, time.Second).Should(Succeed())

			By("Reconciling the created PipelineTrigger")
			_, err = pipelineTriggerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pipelineTriggerName4, Namespace: namespace},
			})
			Expect(err).To(Not(HaveOccurred()))

			By("Get the latest PipelineTrigger version from cluster")
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: pipelineTriggerName4, Namespace: namespace}, createdPipelineTrigger4)
			}, time.Minute, time.Second).Should(Succeed())

			By("Checking the latest PullRequest Status Condition added to the PipelineTrigger instance")
			Eventually(func() error {
				if createdPipelineTrigger4.Status.Branches.Branches["null"].Conditions != nil && len(createdPipelineTrigger4.Status.Branches.Branches["null"].Conditions) != 0 {
					latestStatusCondition := createdPipelineTrigger4.Status.Branches.Branches["null"].Conditions[len(createdPipelineTrigger4.Status.Branches.Branches["null"].Conditions)-1]
					expectedLatestStatusCondition := v1.Condition{
						Type:               "Error",
						Status:             "False",
						Reason:             "Failed",
						LastTransitionTime: latestStatusCondition.LastTransitionTime,
						ObservedGeneration: 1,
						Message:            fmt.Sprintf("pullrequests.pipeline.jquad.rocks \"%s\" not found", createdPipelineTrigger4.Spec.Source.Name)}
					if latestStatusCondition != expectedLatestStatusCondition {
						return fmt.Errorf("The latest status condition added to the PipelineTrigger instance is not as expected")
					}
				}
				return nil
			}, time.Minute, time.Second).Should(Succeed())
		})
	})

	Context("PipelineTrigger creates a PipelineRun on the test cluster for PullRequest", func() {
		ctx := context.Background()
		createdPullrequest := &pullrequestv1alpha1.PullRequest{}
		createdPipelineRun := &tektondevv1.PipelineRun{}
		AfterEach(func() {
			By("Removing the created PipelineRun from the PipelineTrigger with the PullRequest")
			err := k8sClient.Delete(ctx, createdPipelineRun)
			Expect(err).To(Not(HaveOccurred()))
		})
		It("Should be able to create a PipelineRun custom resources", func() {

			By("Creating a PullRequest")
			Expect(k8sClient.Create(ctx, &pullrequest)).Should(Succeed())

			By("Checking if the PullRequest was successfully created")
			Eventually(func() error {
				found := &pullrequestv1alpha1.PullRequest{}
				return k8sClient.Get(ctx, types.NamespacedName{Name: pullrequestName, Namespace: namespace}, found)
			}, time.Minute, time.Second).Should(Succeed())

			By("Updating the PullRequest status")
			pullrequestStatus := pullrequestv1alpha1.PullRequestStatus{
				SourceBranches: pullrequestv1alpha1.Branches{
					Branches: []pullrequestv1alpha1.Branch{
						{
							Name:    "feature-branch-test",
							Commit:  "8932484a2017a3784608c2db429553a94f1e2f4b",
							Details: "{\"id\":1163006807}",
						},
					},
				},
				Conditions: []v1.Condition{
					{
						Type:               "Success",
						Status:             v1.ConditionTrue,
						Reason:             "Succeded",
						Message:            "Success",
						LastTransitionTime: v1.Now(),
					},
				},
			}

			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: pullrequestName, Namespace: namespace}, createdPullrequest)
			}, time.Minute, time.Second).Should(Succeed())

			createdPullrequest.Status = pullrequestStatus
			Expect(k8sClient.Status().Update(ctx, createdPullrequest)).Should(Succeed())

			By("Checking the latest Status Artifact added to the PullRequest instance")
			Eventually(func() error {
				if createdPullrequest.Status.SourceBranches.Branches[0].Name != "" && len(createdPullrequest.Status.SourceBranches.Branches[0].Name) != 0 {
					latestStatusCondition := createdPullrequest.Status.SourceBranches.Branches[len(createdPullrequest.Status.SourceBranches.Branches)-1]
					expectedLatestStatusCondition := pullrequestv1alpha1.Branch{
						Name:    "feature-branch-test",
						Commit:  "8932484a2017a3784608c2db429553a94f1e2f4b",
						Details: "{\"id\":1163006807}",
					}
					if latestStatusCondition.Name != expectedLatestStatusCondition.Name && latestStatusCondition.Details != expectedLatestStatusCondition.Details && latestStatusCondition.Commit != expectedLatestStatusCondition.Commit {
						return fmt.Errorf("The latest status artifact added to the PullRequest instance is not as expected")
					}
				}
				return nil
			}, time.Minute, time.Second).Should(Succeed())

			By("Creating a PipelineTrigger, referencing an existing Pipeline and PullRequest")
			createdPipelineTrigger5 := &pipelinev1alpha1.PipelineTrigger{}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineTriggerName5, Namespace: namespace}, createdPipelineTrigger5)
			if err != nil && errors.IsNotFound(err) {
				// Let's mock our custom resource at the same way that we would
				// apply on the cluster the manifest under config/samples
				createdPipelineTrigger5 := &pipelinev1alpha1.PipelineTrigger{
					TypeMeta: v1.TypeMeta{
						Kind:       "PipelineTrigger",
						APIVersion: "pipeline.jquad.rocks/v1alpha1",
					},
					ObjectMeta: v1.ObjectMeta{
						Name:      pipelineTriggerName5,
						Namespace: namespace,
					},
					Spec: pipelinev1alpha1.PipelineTriggerSpec{
						Source: pipelinev1alpha1.Source{
							APIVersion: "pipeline.jquad.rocks/v1alpha1",
							Kind:       "PullRequest",
							Name:       pullrequestName,
						},
						PipelineRun: pipelineRun3,
					},
				}

				err = k8sClient.Create(ctx, createdPipelineTrigger5)
				Expect(err).To(Not(HaveOccurred()))
			}

			By("Checking if the PipelineTrigger was successfully created")
			Eventually(func() error {
				found := &pipelinev1alpha1.PipelineTrigger{}
				return k8sClient.Get(ctx, types.NamespacedName{Name: pipelineTriggerName5, Namespace: namespace}, found)
			}, time.Minute, time.Second).Should(Succeed())

			By("Reconciling the created PipelineTrigger")
			_, err = pipelineTriggerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pipelineTriggerName5, Namespace: namespace},
			})
			Expect(err).To(Not(HaveOccurred()))

			By("Get the latest PipelineTrigger version from cluster")
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: pipelineTriggerName5, Namespace: namespace}, createdPipelineTrigger5)
			}, time.Minute, time.Second).Should(Succeed())

			By("Checking if latest PullRequest Status Condition of PipelineTrigger instance is set to Unknown")
			Eventually(func() error {
				if createdPipelineTrigger5.Status.Branches.Branches["feature-branch-test"].Conditions != nil && len(createdPipelineTrigger5.Status.Branches.Branches["feature-branch-test"].Conditions) != 0 {
					latestStatusCondition := createdPipelineTrigger5.Status.Branches.Branches["feature-branch-test"].Conditions[len(createdPipelineTrigger5.Status.Branches.Branches["feature-branch-test"].Conditions)-1]
					expectedLatestStatusCondition := v1.Condition{
						Type:               "Unknown",
						LastTransitionTime: latestStatusCondition.LastTransitionTime,
						ObservedGeneration: latestStatusCondition.ObservedGeneration,
						Reason:             "Unknown",
						Status:             v1.ConditionUnknown,
						Message:            "Unknown",
					}
					if latestStatusCondition != expectedLatestStatusCondition {
						return fmt.Errorf("The latest status condition added to the PipelineTrigger instance is not as expected")
					}
				}
				return nil
			}, time.Minute, time.Second).Should(Succeed())

			By("Checking if the PipelineTrigger controller has started a single pipeline")
			Eventually(func() (int, error) {
				found := &tektondevv1.PipelineRunList{}
				err := k8sClient.List(ctx, found)
				return len(found.Items), err
			}, time.Minute, time.Second).Should(Equal(1))

			By("Checking if the params of the created PipelineRun were correctly resolved")
			Eventually(func() error {
				expectedString := "1163006807"
				found := &tektondevv1.PipelineRunList{}
				err := k8sClient.List(ctx, found)
				if found.Items[0].Spec.Params[0].Value.StringVal != expectedString {
					return fmt.Errorf("The $.id param was not correctly resolved")
				}
				return err
			}, time.Minute, time.Second).Should(Succeed())

			By("Checking if the PipelineTrigger controller is managing the PipelineRun")
			pipelineRuns := &tektondevv1.PipelineRunList{}
			Eventually(func() string {
				k8sClient.List(ctx, pipelineRuns)
				pipelineRun := pipelineRuns.Items[0]
				return pipelineRun.GetOwnerReferences()[0].Name
			}, time.Minute, time.Second).Should(Equal(pipelineTriggerName5))

			By("Get the latest PipelineRun version from cluster")
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: pipelineRuns.Items[0].Name, Namespace: namespace}, createdPipelineRun)
			}, time.Minute, time.Second).Should(Succeed())

			By("Updating the PipelineRun status to reason started (status: Unknown)")
			createdPipelineRun.Status.InitializeConditions(clock.RealClock{})
			Expect(k8sClient.Status().Update(ctx, createdPipelineRun)).Should(Succeed())

			// https://tekton.dev/docs/pipelines/pipelineruns/#pipelinerun-status
			By("Checking if the PipelineRun status reason was updated to reason: Started (status: Unknown)")
			Eventually(func() (string, error) {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: createdPipelineRun.Name, Namespace: namespace}, createdPipelineRun)
				return string(createdPipelineRun.Status.GetCondition(apis.ConditionSucceeded).Reason), err
			}, time.Minute, time.Second).Should(Equal("Started"))

			By("Updating the PipelineRun status to reason: Running (status: Unknown)")
			k8sClient.Get(ctx, types.NamespacedName{Name: createdPipelineRun.Name, Namespace: namespace}, createdPipelineRun)
			createdPipelineRun.Status.SetCondition(&apis.Condition{
				Type:    apis.ConditionSucceeded,
				Status:  corev1.ConditionUnknown,
				Reason:  "Running",
				Message: "Tasks Running: 1 (Failed: 0, Cancelled 0), Skipped: 0",
			})
			createdPipelineRun.Status.ObservedGeneration = 2
			Expect(k8sClient.Status().Update(ctx, createdPipelineRun)).Should(Succeed())

			By("Checking if the PipelineRun status was updated to reason: Running (status: Unknown)")
			Eventually(func() (string, error) {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: createdPipelineRun.Name, Namespace: namespace}, createdPipelineRun)
				return string(createdPipelineRun.Status.GetCondition(apis.ConditionSucceeded).Reason), err
			}, time.Minute, time.Second).Should(Equal("Running"))

			By("Reconciling the created PipelineTrigger")
			_, err = pipelineTriggerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pipelineTriggerName5, Namespace: namespace},
			})
			Expect(err).To(Not(HaveOccurred()))

			By("Get the latest PipelineTrigger version from cluster")
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: pipelineTriggerName5, Namespace: namespace}, createdPipelineTrigger5)
			}, time.Minute, time.Second).Should(Succeed())

			By("Checking if latest PullRequest Status Condition of PipelineTrigger instance is set to InProgress")
			Eventually(func() error {
				if createdPipelineTrigger5.Status.Branches.Branches["feature-branch-test"].Conditions != nil && len(createdPipelineTrigger5.Status.Branches.Branches["feature-branch-test"].Conditions) != 0 {
					latestStatusCondition := createdPipelineTrigger5.Status.Branches.Branches["feature-branch-test"].Conditions[len(createdPipelineTrigger5.Status.Branches.Branches["feature-branch-test"].Conditions)-1]
					expectedLatestStatusCondition := v1.Condition{
						Type:               "InProgress",
						LastTransitionTime: latestStatusCondition.LastTransitionTime,
						ObservedGeneration: latestStatusCondition.ObservedGeneration,
						Reason:             "InProgress",
						Status:             v1.ConditionUnknown,
						Message:            "Progressing",
					}
					if latestStatusCondition != expectedLatestStatusCondition {
						return fmt.Errorf("The latest status condition added to the PipelineTrigger instance is not as expected")
					}
				}
				return nil
			}, time.Minute, time.Second).Should(Succeed())

			By("Updating the PipelineRun status to reason: Succeeded (status: True)")
			k8sClient.Get(ctx, types.NamespacedName{Name: createdPipelineRun.Name, Namespace: namespace}, createdPipelineRun)
			createdPipelineRun.Status.SetCondition(&apis.Condition{
				Type:    apis.ConditionSucceeded,
				Status:  corev1.ConditionTrue,
				Reason:  "Succeeded",
				Message: "Tasks Completed: 1 (Failed: 0, Cancelled 0), Skipped: 0",
			})
			createdPipelineRun.Status.ObservedGeneration = 2
			Expect(k8sClient.Status().Update(ctx, createdPipelineRun)).Should(Succeed())

			By("Checking if the PipelineRun status was updated to reason: Succeeded (status: True)")
			Eventually(func() (corev1.ConditionStatus, error) {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: createdPipelineRun.Name, Namespace: namespace}, createdPipelineRun)
				return createdPipelineRun.Status.GetCondition(apis.ConditionSucceeded).Status, err
			}, time.Minute, time.Second).Should(Equal(corev1.ConditionTrue))

			By("Checking if the PipelineTrigger LatestPipelineRun is correctly set")
			Eventually(func() (string, error) {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineTriggerName5, Namespace: namespace}, createdPipelineTrigger5)
				return createdPipelineTrigger5.Status.Branches.Branches["feature-branch-test"].LatestPipelineRun, err
			}, time.Minute, time.Second).Should(ContainSubstring("feature-branch-test"))

			By("Reconciling the created PipelineTrigger")
			_, err = pipelineTriggerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pipelineTriggerName5, Namespace: namespace},
			})
			Expect(err).To(Not(HaveOccurred()))

			/*
				By("Checking if the PipelineTrigger status is updated to succeeded when the corresponding PipelineRun is completed")
				Eventually(func() (string, error) {
					err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineTriggerName4, Namespace: namespace}, createdPipelineTrigger4)
					cond, _ := createdPipelineTrigger4.Status.Branches.Get.Branches["feature-branch-test"].GetCondition(opstatus.ReconcileSuccess)
					return cond.Type, err
				}, time.Minute, time.Second).Should(ContainSubstring(opstatus.ReconcileSuccess))
			*/

			By("Checking if the PipelineTrigger status conditions = PipelineRuns status changes")
			Eventually(func() (int, error) {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineTriggerName5, Namespace: namespace}, createdPipelineTrigger5)
				return len(createdPipelineTrigger5.Status.Branches.Branches["feature-branch-test"].Conditions), err
			}, time.Minute, time.Second).Should(Equal(3))

		})
	})

	// Regression test for the 2026-07-05 incident: a transient Tekton admission
	// webhook outage rejected PipelineRun creates, yet the operator advanced the
	// PipelineTrigger status to the new revision and returned without requeueing,
	// permanently stranding the build. The fix keeps a PipelineRun-create failure
	// retryable: status is NOT advanced, no success event is emitted, and the
	// reconcile requeues so it retries once the webhook recovers.
	Context("PipelineTrigger retries when the Tekton admission webhook rejects the PipelineRun create", func() {
		ctx := context.Background()

		const (
			webhookGitRepositoryName = "git-repo-webhook-retry"
			webhookPipelineName      = "build-and-push-webhook-retry"
			webhookTaskName          = "build-webhook-retry"
			webhookTriggerName       = "pipeline-trigger-webhook-retry"
			webhookConfigName        = "reject-tekton-pipelineruns-webhook-retry"
		)

		// drainEvents non-blockingly collects everything currently in a
		// FakeRecorder's channel.
		drainEvents := func(rec *record.FakeRecorder) []string {
			var events []string
			for {
				select {
				case e := <-rec.Events:
					events = append(events, e)
				default:
					return events
				}
			}
		}

		// ownedPipelineRuns counts the PipelineRuns owned by the given trigger.
		ownedPipelineRuns := func(triggerName string) int {
			list := &tektondevv1.PipelineRunList{}
			_ = k8sClient.List(ctx, list)
			count := 0
			for i := range list.Items {
				for _, ref := range list.Items[i].GetOwnerReferences() {
					if ref.Name == triggerName {
						count++
					}
				}
			}
			return count
		}

		It("Should not advance status and should requeue so the build is retried", func() {
			By("Creating a Task and Pipeline referenced by the PipelineRun")
			webhookTask := &tektondevv1.Task{
				ObjectMeta: v1.ObjectMeta{Name: webhookTaskName, Namespace: namespace},
				Spec: tektondevv1.TaskSpec{
					Params: []tektondevv1.ParamSpec{{Name: webhookTaskName}},
				},
			}
			Expect(k8sClient.Create(ctx, webhookTask)).Should(Succeed())

			webhookPipeline := &tektondevv1.Pipeline{
				ObjectMeta: v1.ObjectMeta{Name: webhookPipelineName, Namespace: namespace},
				Spec: tektondevv1.PipelineSpec{
					Tasks: []tektondevv1.PipelineTask{{
						Name:    webhookTaskName,
						TaskRef: &tektondevv1.TaskRef{Name: webhookTaskName},
					}},
				},
			}
			Expect(k8sClient.Create(ctx, webhookPipeline)).Should(Succeed())

			By("Creating a GitRepository with a Ready revision")
			webhookGitRepository := &sourcev1.GitRepository{
				ObjectMeta: v1.ObjectMeta{Name: webhookGitRepositoryName, Namespace: namespace},
				Spec: sourcev1.GitRepositorySpec{
					URL:      "http://github.com/org/repo.git",
					Interval: v1.Duration{},
				},
			}
			Expect(k8sClient.Create(ctx, webhookGitRepository)).Should(Succeed())

			createdWebhookGitRepository := &sourcev1.GitRepository{}
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: webhookGitRepositoryName, Namespace: namespace}, createdWebhookGitRepository)
			}, time.Minute, time.Second).Should(Succeed())

			createdWebhookGitRepository.Status = sourcev1.GitRepositoryStatus{
				Artifact: &meta.Artifact{
					Path:           "gitrepository/flux-system/flux-system/ae05cbcffffffffffffffffffffffffffffffff.tar.gz",
					URL:            "http://source-controller.flux-system.svc.cluster.local./gitrepository/flux-system/flux-system/ae05cbc.tar.gz",
					Revision:       "main@sha1:ae05cbcffffffffffffffffffffffffffffffff",
					Digest:         "sha256:ae05cbcffffffffffffffffffffffffffffffff",
					LastUpdateTime: v1.Now(),
				},
				Conditions: []v1.Condition{{
					Type:               "Ready",
					Status:             v1.ConditionTrue,
					Reason:             v1.StatusSuccess,
					Message:            "Success",
					ObservedGeneration: 1,
					LastTransitionTime: v1.Now(),
				}},
			}
			Expect(k8sClient.Status().Update(ctx, createdWebhookGitRepository)).Should(Succeed())

			By("Creating a PipelineTrigger referencing the Pipeline and GitRepository")
			webhookPipelineRun := &unstructured.Unstructured{}
			webhookPipelineRun.SetAPIVersion("tekton.dev/v1beta1")
			webhookPipelineRun.SetKind("PipelineRun")
			webhookPipelineRun.SetNamespace(namespace)
			webhookPipelineRun.Object["spec"] = map[string]interface{}{
				"pipelineRef": map[string]interface{}{"name": webhookPipelineName},
				"params": []interface{}{
					map[string]interface{}{"name": "param1", "value": "value1"},
					map[string]interface{}{"name": "param2", "value": "value2"},
				},
			}

			webhookTrigger := &pipelinev1alpha1.PipelineTrigger{
				TypeMeta:   v1.TypeMeta{Kind: "PipelineTrigger", APIVersion: "pipeline.jquad.rocks/v1alpha1"},
				ObjectMeta: v1.ObjectMeta{Name: webhookTriggerName, Namespace: namespace},
				Spec: pipelinev1alpha1.PipelineTriggerSpec{
					Source: pipelinev1alpha1.Source{
						APIVersion: "source.toolkit.fluxcd.io/v1",
						Kind:       "GitRepository",
						Name:       webhookGitRepositoryName,
					},
					PipelineRun: *webhookPipelineRun,
				},
			}
			Expect(k8sClient.Create(ctx, webhookTrigger)).Should(Succeed())
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: webhookTriggerName, Namespace: namespace}, &pipelinev1alpha1.PipelineTrigger{})
			}, time.Minute, time.Second).Should(Succeed())

			By("Installing a failing ValidatingWebhookConfiguration that rejects PipelineRun creates")
			webhookConfig := &admissionregistrationv1.ValidatingWebhookConfiguration{
				ObjectMeta: v1.ObjectMeta{Name: webhookConfigName},
				Webhooks: []admissionregistrationv1.ValidatingWebhook{{
					Name: "reject-pipelineruns.pipeline-trigger-operator.test",
					ClientConfig: admissionregistrationv1.WebhookClientConfig{
						// Nothing listens here -> connection refused, mirroring
						// the incident's "failed calling webhook ... connect:
						// connection refused".
						URL: ptr.To("https://127.0.0.1:1/reject"),
					},
					Rules: []admissionregistrationv1.RuleWithOperations{{
						Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create},
						Rule: admissionregistrationv1.Rule{
							APIGroups:   []string{"tekton.dev"},
							APIVersions: []string{"*"},
							Resources:   []string{"pipelineruns"},
							Scope:       ptr.To(admissionregistrationv1.AllScopes),
						},
					}},
					FailurePolicy:           ptr.To(admissionregistrationv1.Fail),
					SideEffects:             ptr.To(admissionregistrationv1.SideEffectClassNone),
					AdmissionReviewVersions: []string{"v1"},
					TimeoutSeconds:          ptr.To(int32(5)),
				}},
			}
			Expect(k8sClient.Create(ctx, webhookConfig)).Should(Succeed())
			// Guarantee removal even if the spec fails, so the broken webhook
			// cannot leak into other specs that create PipelineRuns.
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, webhookConfig)
			})

			By("Waiting until the webhook is active (PipelineRun creates are rejected)")
			Eventually(func() bool {
				probe := webhookPipelineRun.DeepCopy()
				probe.SetName("webhook-probe-active")
				err := k8sClient.Create(ctx, probe)
				if err == nil {
					_ = k8sClient.Delete(ctx, probe)
					return false
				}
				return strings.Contains(err.Error(), "failed calling webhook")
			}, time.Minute, time.Second).Should(BeTrue())

			By("Reconciling while the webhook is down")
			fakeRec := record.NewFakeRecorder(100)
			pipelineTriggerReconciler.recorder = fakeRec
			result, reconcileErr := pipelineTriggerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: webhookTriggerName, Namespace: namespace},
			})

			By("Checking the reconcile requeues (is retryable) rather than silently succeeding")
			Expect(result.Requeue || reconcileErr != nil).To(BeTrue(),
				"reconcile must requeue or error when the PipelineRun create fails")

			By("Checking no PipelineRun was created for the trigger")
			Expect(ownedPipelineRuns(webhookTriggerName)).To(Equal(0))

			By("Checking the status was NOT advanced (no LatestPipelineRun recorded)")
			strandedCheck := &pipelinev1alpha1.PipelineTrigger{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: webhookTriggerName, Namespace: namespace}, strandedCheck)).Should(Succeed())
			Expect(strandedCheck.Status.GitRepository.LatestPipelineRun).To(BeEmpty())

			By("Checking no 'Started the pipeline' success event was emitted")
			for _, e := range drainEvents(fakeRec) {
				Expect(e).ShouldNot(ContainSubstring("Started the pipeline"))
			}

			By("Recovering the webhook (deleting the failing configuration)")
			Expect(k8sClient.Delete(ctx, webhookConfig)).Should(Succeed())
			Eventually(func() bool {
				probe := webhookPipelineRun.DeepCopy()
				probe.SetName("webhook-probe-recovered")
				err := k8sClient.Create(ctx, probe)
				if err == nil {
					_ = k8sClient.Delete(ctx, probe)
					return true
				}
				return false
			}, time.Minute, time.Second).Should(BeTrue())

			By("Reconciling after recovery -> the SAME revision is retried and a PipelineRun is created")
			Eventually(func() int {
				_, _ = pipelineTriggerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: webhookTriggerName, Namespace: namespace},
				})
				return ownedPipelineRuns(webhookTriggerName)
			}, time.Minute, time.Second).Should(Equal(1))

			By("Cleaning up the created PipelineRun")
			list := &tektondevv1.PipelineRunList{}
			Expect(k8sClient.List(ctx, list)).Should(Succeed())
			for i := range list.Items {
				for _, ref := range list.Items[i].GetOwnerReferences() {
					if ref.Name == webhookTriggerName {
						_ = k8sClient.Delete(ctx, &list.Items[i])
					}
				}
			}
		})
	})

})
