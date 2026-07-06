package v1alpha1

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// TestCreatePipelineRunResource_ReturnsIndependentCopy guards the single-source
// (GitRepository / ImagePolicy) half of the 2026-07-06 create-storm fix.
//
// CreatePipelineRunResource() must build on Spec.PipelineRun.DeepCopy() and never
// return a pointer aliasing the shared template. Otherwise client.Create() writing
// the API response (resourceVersion/uid/name) back into the returned object poisons
// the shared template, so the create-retry rebuilds a run that already carries a
// resourceVersion → "resourceVersion should not be set on objects to be created" →
// the retry loops forever. For each source kind this asserts:
//   - the returned pointer is NOT &Spec.PipelineRun,
//   - the template is left pristine (no generateName stamped, dynamic params NOT
//     resolved in place) so the next reconcile builds clean, and
//   - two successive builds (as a create-retry would do) are independent objects —
//     stamping a resourceVersion on one does not appear on the other.
func TestCreatePipelineRunResource_ReturnsIndependentCopy(t *testing.T) {
	newTemplate := func() unstructured.Unstructured {
		var tpl unstructured.Unstructured
		tpl.SetAPIVersion("tekton.dev/v1")
		tpl.SetKind("PipelineRun")
		tpl.Object["spec"] = map[string]interface{}{
			"pipelineRef": map[string]interface{}{"name": "build-and-push"},
			"params": []interface{}{
				map[string]interface{}{"name": "ID", "value": "$.id"},
			},
		}
		return tpl
	}

	cases := []struct {
		name         string
		newTrigger   func() *PipelineTrigger
		wantGenName  string
		wantResolved string
	}{
		{
			name: "GitRepository",
			newTrigger: func() *PipelineTrigger {
				return &PipelineTrigger{
					Spec: PipelineTriggerSpec{
						Source:      Source{Kind: "GitRepository", Name: "repo"},
						PipelineRun: newTemplate(),
					},
					Status: PipelineTriggerStatus{
						GitRepository: GitRepository{
							BranchName: "main",
							CommitId:   "abc123",
							Details:    `{"id":1163006807}`,
						},
					},
				}
			},
			wantGenName:  "main-",
			wantResolved: "1163006807",
		},
		{
			name: "ImagePolicy",
			newTrigger: func() *PipelineTrigger {
				return &PipelineTrigger{
					Spec: PipelineTriggerSpec{
						Source:      Source{Kind: "ImagePolicy", Name: "policy"},
						PipelineRun: newTemplate(),
					},
					Status: PipelineTriggerStatus{
						ImagePolicy: ImagePolicy{
							RepositoryName: "gcr.io",
							ImageName:      "repo",
							ImageVersion:   "v0.0.1",
							Details:        `{"id":42}`,
						},
					},
				}
			},
			wantGenName:  "repo-",
			wantResolved: "42",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pt := tc.newTrigger()

			run1 := pt.CreatePipelineRunResource()

			// 1. Not the shared-template pointer.
			if run1 == &pt.Spec.PipelineRun {
				t.Fatal("returned pointer aliases the shared Spec.PipelineRun template (the create-storm bug)")
			}

			// The built run resolves the dynamic param and gets a generateName.
			if got := paramValue(t, run1, "ID"); got != tc.wantResolved {
				t.Errorf("run param ID = %q, want %q", got, tc.wantResolved)
			}
			if got, _, _ := unstructured.NestedString(run1.Object, "metadata", "generateName"); got != tc.wantGenName {
				t.Errorf("run generateName = %q, want %q", got, tc.wantGenName)
			}

			// 2. Template left pristine — no generateName stamped, param still dynamic.
			if _, found, _ := unstructured.NestedString(pt.Spec.PipelineRun.Object, "metadata", "generateName"); found {
				t.Error("Spec.PipelineRun template was mutated: generateName leaked onto the shared template")
			}
			if got := paramValue(t, &pt.Spec.PipelineRun, "ID"); got != "$.id" {
				t.Errorf("template param ID was resolved in place to %q; the shared template must stay dynamic (%q)", got, "$.id")
			}

			// 3. Two successive builds (a create + its retry) are independent.
			run2 := pt.CreatePipelineRunResource()
			if run1 == run2 {
				t.Fatal("two calls to CreatePipelineRunResource returned the same pointer — a retry would rebuild the poisoned object")
			}
			run1.SetResourceVersion("999")
			if rv := run2.GetResourceVersion(); rv != "" {
				t.Errorf("second build inherited the first build's resourceVersion %q — the builds are aliased", rv)
			}
		})
	}
}

// paramValue returns the value of the named param in an unstructured PipelineRun's
// spec.params, failing the test if the params array is malformed.
func paramValue(t *testing.T, pr *unstructured.Unstructured, name string) string {
	t.Helper()
	params, found, err := unstructured.NestedSlice(pr.Object, "spec", "params")
	if err != nil || !found {
		t.Fatalf("spec.params not found or malformed (found=%v, err=%v)", found, err)
	}
	for _, p := range params {
		m, ok := p.(map[string]interface{})
		if !ok {
			continue
		}
		if m["name"] == name {
			v, _ := m["value"].(string)
			return v
		}
	}
	t.Fatalf("param %q not found in spec.params", name)
	return ""
}
