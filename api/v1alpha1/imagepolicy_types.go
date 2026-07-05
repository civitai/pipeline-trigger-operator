package v1alpha1

import (
	"encoding/json"
	"sort"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const (
	repositoryNamePosition int    = 1
	imageNamePosition      int    = 0
	imageVersionPosition   int    = 1
	imageNameDelimeter     string = "/"
	imageVersionDelimeter  string = ":"
)

type ImagePolicy struct {

	// +kubebuilder:validation:Optional
	RepositoryName string `json:"repositoryName,omitempty"`

	// +kubebuilder:validation:Optional
	ImageName string `json:"imageName,omitempty"`

	// +kubebuilder:validation:Optional
	ImageVersion string `json:"imageVersion,omitempty"`

	// +kubebuilder:validation:Optional
	LatestPipelineRun string `json:"latestPipelineRun,omitempty"`

	Details string `json:"details,omitempty"`

	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

func (currentImagePolicy *ImagePolicy) GenerateImagePolicyLabelsAsHash() map[string]string {
	labels := make(map[string]string)

	labels[pipelineTriggerLabelKey+"/"+"ip.image.name"] = currentImagePolicy.ImageName
	labels[pipelineTriggerLabelKey+"/"+"ip.image.version"] = currentImagePolicy.ImageVersion

	return labels
}

func (currentImagePolicy *ImagePolicy) GenerateImagePolicyLabelsAsString() string {
	label :=
		pipelineTriggerLabelKey + "/" + "ip.image.name=" + currentImagePolicy.ImageName + "," +
			pipelineTriggerLabelKey + "/" + "ip.image.version=" + currentImagePolicy.ImageVersion

	return label
}

func (currentImagePolicy *ImagePolicy) Equals(newImagePolicy ImagePolicy) bool {
	if currentImagePolicy.RepositoryName == newImagePolicy.RepositoryName && currentImagePolicy.ImageName == newImagePolicy.ImageName && currentImagePolicy.ImageVersion == newImagePolicy.ImageVersion {
		return true
	} else {
		return false
	}
}

func (imagePolicy *ImagePolicy) GetImagePolicy(fluxImagePolicy unstructured.Unstructured) {
	imagePolicy.RepositoryName = getRepositoryName(fluxImagePolicy)
	imagePolicy.ImageName = getImageName(fluxImagePolicy)
	imagePolicy.ImageVersion = getImageVersion(fluxImagePolicy)
}

// latestRefField safely reads .status.latestRef[key] from a Flux ImagePolicy as
// a string, returning "" when the policy is not Ready (no latestRef resolved
// yet). Upstream does unchecked assertions here
// (Object["status"].(map)["latestRef"].(map)[key].(string)) — same class of
// crash as the GitRepository getters: the moment a not-Ready ImagePolicy is
// reconciled, the nil.(map) assertion panics, the unrecovered reconcile panic
// crashes the manager, and the operator CrashLoopBackOffs cluster-wide.
// Currently LATENT (the cluster has zero ImagePolicy-sourced PipelineTriggers)
// but nil-guarded for parity so an ImagePolicy trigger can never reintroduce
// the crashloop. This is the civitai patch.
func latestRefField(fluxImagePolicy unstructured.Unstructured, key string) string {
	status, ok := fluxImagePolicy.Object["status"].(map[string]interface{})
	if !ok {
		return ""
	}
	latestRef, ok := status["latestRef"].(map[string]interface{})
	if !ok {
		return ""
	}
	value, ok := latestRef[key].(string)
	if !ok {
		return ""
	}
	return value
}

func getRepositoryName(fluxImagePolicy unstructured.Unstructured) string {
	imageName := latestRefField(fluxImagePolicy, "name")
	if imageName == "" {
		return ""
	}
	parts := strings.Split(imageName, imageNameDelimeter)
	if len(parts) < 2 {
		return ""
	}
	return parts[len(parts)-2]
}

func getImageName(fluxImagePolicy unstructured.Unstructured) string {
	imageName := latestRefField(fluxImagePolicy, "name")
	if imageName == "" {
		return ""
	}
	parts := strings.Split(imageName, imageNameDelimeter)
	return parts[len(parts)-1]
}

func getImageVersion(fluxImagePolicy unstructured.Unstructured) string {
	return latestRefField(fluxImagePolicy, "tag")
}

func (imagePolicy *ImagePolicy) AddOrReplaceCondition(c metav1.Condition) {
	found := false
	for i, condition := range imagePolicy.Conditions {
		if c.Type == condition.Type {
			imagePolicy.Conditions[i] = c
			found = true
		}
	}
	if !found {
		imagePolicy.Conditions = append(imagePolicy.Conditions, c)
	}
}

func (imagePolicy *ImagePolicy) GetCondition(conditionType string) (metav1.Condition, bool) {
	for _, condition := range imagePolicy.Conditions {
		if condition.Type == conditionType {
			return condition, true
		}
	}
	return metav1.Condition{}, false
}

// GetLastCondition retruns the last condition based on the condition timestamp. if no condition is present it return false.
func (imagePolicy *ImagePolicy) GetLastCondition() metav1.Condition {
	if len(imagePolicy.Conditions) == 0 {
		return metav1.Condition{}
	}
	//we need to make a copy of the slice
	copiedConditions := []metav1.Condition{}
	for _, condition := range imagePolicy.Conditions {
		ccondition := condition.DeepCopy()
		copiedConditions = append(copiedConditions, *ccondition)
	}
	sort.Slice(copiedConditions, func(i, j int) bool {
		return copiedConditions[i].LastTransitionTime.Before(&copiedConditions[j].LastTransitionTime)
	})
	return copiedConditions[len(copiedConditions)-1]
}

func (imagePolicy *ImagePolicy) Rewrite() string {
	// Replaces branch names from feature/newlogin to feature-newlogin
	return strings.ReplaceAll(imagePolicy.ImageName, ":", "-")
}

func (imagePolicy *ImagePolicy) GenerateDetails() {
	tempImagePolicy := &ImagePolicy{
		ImageName:      imagePolicy.ImageName,
		RepositoryName: imagePolicy.RepositoryName,
		ImageVersion:   imagePolicy.ImageVersion,
	}
	data, _ := json.Marshal(tempImagePolicy)
	imagePolicy.Details = string(data)
}
