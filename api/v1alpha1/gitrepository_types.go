package v1alpha1

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const (
	gitRepositoryNamePosition int    = 2
	branchNamePosition        int    = 0
	commitIdPosition          int    = 1
	repositoryNameDelimeter   string = "/"
	revisionDelimiter         string = "@"
	commitIdDelimeter         string = ":"
)

type GitRepository struct {
	// +kubebuilder:validation:Optional
	BranchName string `json:"branchName,omitempty"`

	// +kubebuilder:validation:Optional
	CommitId string `json:"commitId,omitempty"`

	// +kubebuilder:validation:Optional
	RepositoryName string `json:"repositoryName,omitempty"`

	// +kubebuilder:validation:Optional
	LatestPipelineRun string `json:"latestPipelineRun,omitempty"`

	Details string `json:"details,omitempty"`

	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

func (currentGitRepository *GitRepository) GenerateGitRepositoryLabelsAsHash() map[string]string {
	labels := make(map[string]string)

	labels[pipelineTriggerLabelKey+"/"+"git.repository.branch.name"] = currentGitRepository.BranchName
	labels[pipelineTriggerLabelKey+"/"+"git.repository.branch.commit"] = currentGitRepository.CommitId

	return labels
}

func (currentGitRepository *GitRepository) GenerateGitRepositoryLabelsAsString() string {
	label :=
		pipelineTriggerLabelKey + "/" + "git.repository.branch.name=" + currentGitRepository.BranchName + "," +
			pipelineTriggerLabelKey + "/" + "git.repository.branch.commit=" + currentGitRepository.CommitId

	return label
}

func (currentGitRepository *GitRepository) Equals(newGitRepository GitRepository) bool {
	if currentGitRepository.BranchName == newGitRepository.BranchName && currentGitRepository.RepositoryName == newGitRepository.RepositoryName && currentGitRepository.CommitId == newGitRepository.CommitId {
		return true
	} else {
		return false
	}
}

// artifactField safely reads .status.artifact[key] from a Flux GitRepository as
// a string. It returns "" when the GitRepository is not Ready yet — i.e. when
// status or artifact is absent (a new semver source with no matching tag, or a
// source still being reconciled). Upstream does unchecked type assertions here
// (Object["status"].(map)["artifact"].(map)[key]), which panic with
// "interface conversion: nil, not map[string]interface{}" the moment such a
// not-ready GitRepository is reconciled — and because a reconcile panic is
// unrecovered, it crashes the whole manager and CrashLoopBackOffs the operator,
// blocking every PipelineTrigger. This nil-safe accessor is the civitai patch.
func artifactField(fluxGitRepository unstructured.Unstructured, key string) string {
	status, ok := fluxGitRepository.Object["status"].(map[string]interface{})
	if !ok {
		return ""
	}
	artifact, ok := status["artifact"].(map[string]interface{})
	if !ok {
		return ""
	}
	value, ok := artifact[key]
	if !ok || value == nil {
		return ""
	}
	return fmt.Sprintf("%v", value)
}

func getGitRepositoryName(fluxGitRepository unstructured.Unstructured) string {
	repositoryPathStr := artifactField(fluxGitRepository, "path")
	if repositoryPathStr == "" {
		return ""
	}
	parts := strings.Split(repositoryPathStr, repositoryNameDelimeter)
	if gitRepositoryNamePosition >= len(parts) {
		return ""
	}
	return parts[gitRepositoryNamePosition]
}

func getBranchName(fluxGitRepository unstructured.Unstructured) string {
	repositoryRevisionStr := artifactField(fluxGitRepository, "revision")
	if repositoryRevisionStr == "" {
		return ""
	}
	parts := strings.Split(repositoryRevisionStr, revisionDelimiter)
	if branchNamePosition >= len(parts) {
		return ""
	}
	return parts[branchNamePosition]
}

func getCommitId(fluxGitRepository unstructured.Unstructured) string {
	repositoryCommitIdStr := artifactField(fluxGitRepository, "revision")
	if repositoryCommitIdStr == "" {
		return ""
	}
	parts := strings.Split(repositoryCommitIdStr, commitIdDelimeter)
	if commitIdPosition >= len(parts) {
		return ""
	}
	return parts[commitIdPosition]
}

func (gitRepository *GitRepository) GetGitRepository(fluxGitRepository unstructured.Unstructured) {
	gitRepository.RepositoryName = getGitRepositoryName(fluxGitRepository)
	gitRepository.BranchName = getBranchName(fluxGitRepository)
	gitRepository.CommitId = getCommitId(fluxGitRepository)
}

func (gitRepository *GitRepository) AddOrReplaceCondition(c metav1.Condition) {
	found := false
	for i, condition := range gitRepository.Conditions {
		if c.Type == condition.Type {
			gitRepository.Conditions[i] = c
			found = true
		}
	}
	if !found {
		gitRepository.Conditions = append(gitRepository.Conditions, c)
	}
}

func (gitRepository *GitRepository) GetCondition(conditionType string) (metav1.Condition, bool) {
	for _, condition := range gitRepository.Conditions {
		if condition.Type == conditionType {
			return condition, true
		}
	}
	return metav1.Condition{}, false
}

// GetLastCondition retruns the last condition based on the condition timestamp. if no condition is present it return false.
func (gitRepository *GitRepository) GetLastCondition() metav1.Condition {
	if len(gitRepository.Conditions) == 0 {
		return metav1.Condition{}
	}
	//we need to make a copy of the slice
	copiedConditions := []metav1.Condition{}
	for _, condition := range gitRepository.Conditions {
		ccondition := condition.DeepCopy()
		copiedConditions = append(copiedConditions, *ccondition)
	}
	sort.Slice(copiedConditions, func(i, j int) bool {
		return copiedConditions[i].LastTransitionTime.Before(&copiedConditions[j].LastTransitionTime)
	})
	return copiedConditions[len(copiedConditions)-1]
}

func (gitRepository *GitRepository) Rewrite() string {
	// Replaces branch names from feature/newlogin to feature-newlogin
	return strings.ReplaceAll(gitRepository.BranchName, "/", "-")
}

func (gitRepository *GitRepository) GenerateDetails() {
	tempGitRepository := &GitRepository{
		BranchName:     gitRepository.BranchName,
		CommitId:       gitRepository.CommitId,
		RepositoryName: gitRepository.RepositoryName,
	}
	data, _ := json.Marshal(tempGitRepository)
	gitRepository.Details = string(data)
}
