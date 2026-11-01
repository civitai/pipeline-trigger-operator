package v1alpha1

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/validation"
)

func TestSanitizeLabelValue(t *testing.T) {
	long := strings.Repeat("abcdefghij", 7) // 70 chars, valid chars, too long
	cases := []struct {
		raw  string
		want string // exact expected value (pinned literal, not derived)
	}{
		// Already valid: returned unchanged (compat with existing runs' labels).
		{"", ""},
		{"main", "main"},
		{"feature-newlogin", "feature-newlogin"},
		{"8932484a2017a3784608c2db429553a94f1e2f4b", "8932484a2017a3784608c2db429553a94f1e2f4b"},
		// Invalid: base + "-" + sha256(raw)[:8].
		// Literal hashes (printf %s <raw> | sha256sum | cut -c1-8), so a change
		// to the hash input or length is caught.
		{"fix-bug-post-text-scan-raises-pg-posts-", "fix-bug-post-text-scan-raises-pg-posts-f618f4fb"},
		{"feature/x", "feature-x-217d2bf5"},
		{"_tag", "tag-" + shortHash("_tag")},
		{long, long[:54] + "-" + shortHash(long)},
		// Nothing alphanumeric survives: hash only.
		{"///", shortHash("///")},
		{"ü", shortHash("ü")},
	}
	for _, c := range cases {
		got := SanitizeLabelValue(c.raw)
		if got != c.want {
			t.Errorf("SanitizeLabelValue(%q) = %q, want %q", c.raw, got, c.want)
		}
		if errs := validation.IsValidLabelValue(got); len(errs) != 0 {
			t.Errorf("SanitizeLabelValue(%q) = %q is invalid: %v", c.raw, got, errs)
		}
		// Deterministic and idempotent: the reader re-deriving it gets the same.
		if again := SanitizeLabelValue(got); again != got {
			t.Errorf("not idempotent: %q -> %q", got, again)
		}
	}
}

// Truncation boundary: the base must be cut so that the 63-char limit holds
// for every input length around it, and a cut that lands on a '-' must still
// end alphanumeric.
func TestSanitizeLabelValue_LengthBoundaries(t *testing.T) {
	for n := 50; n <= 130; n++ {
		raw := strings.Repeat("a", n-1) + "-" // always invalid (trailing dash)
		got := SanitizeLabelValue(raw)
		if errs := validation.IsValidLabelValue(got); len(errs) != 0 {
			t.Errorf("len %d: %q invalid: %v", n, got, errs)
		}
		raw2 := strings.Repeat("ab-", n/3+1)[:n]
		got2 := SanitizeLabelValue(raw2)
		if errs := validation.IsValidLabelValue(got2); len(errs) != 0 {
			t.Errorf("len %d: %q invalid: %v", n, got2, errs)
		}
	}
}

func TestGenerateNamePrefix(t *testing.T) {
	cases := map[string]string{
		"feat-aaa":              "feat-aaa-",
		"Fix_Upper.":            "fix-upper-",
		"-lead":                 "lead-",
		"":                      "run-",
		"///":                   "run-",
		strings.Repeat("x", 80): strings.Repeat("x", 56) + "-",
	}
	for raw, want := range cases {
		got := GenerateNamePrefix(raw)
		if got != want {
			t.Errorf("GenerateNamePrefix(%q) = %q, want %q", raw, got, want)
		}
		if errs := validation.IsDNS1123Label(got + "abcde"); len(errs) != 0 {
			t.Errorf("GenerateNamePrefix(%q) = %q gives invalid name: %v", raw, got, errs)
		}
	}
}
