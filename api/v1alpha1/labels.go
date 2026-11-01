package v1alpha1

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	// labelHashLen is the number of hex chars of sha256(raw) appended to a label
	// value that had to be modified to become valid.
	labelHashLen = 8
	// labelBaseMaxLen leaves room for "-" + hash inside the 63-char limit.
	labelBaseMaxLen = validation.LabelValueMaxLength - 1 - labelHashLen

	// generateNameBaseMaxLen: the API server truncates a generateName base to
	// 58 chars before appending its 5 random chars; keep ours (incl. the
	// trailing "-") within that so the truncation never cuts mid-sanitisation.
	generateNameBaseMaxLen = 57
	generateNameFallback   = "run"
)

func isAlnum(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
}

func isLabelChar(r rune) bool {
	return isAlnum(r) || r == '-' || r == '_' || r == '.'
}

func trimNonAlnum(s string) string {
	return strings.TrimFunc(s, func(r rune) bool { return !isAlnum(r) })
}

func shortHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])[:labelHashLen]
}

// SanitizeLabelValue turns an externally-derived string (a branch name, commit,
// image name/tag, ...) into a valid Kubernetes label value.
//
//   - A value that is ALREADY valid is returned unchanged, so the labels on
//     PipelineRuns created before this function existed keep matching the
//     reader side (CalculateCurrentState / SetCurrentPipelineRunName), which
//     derives its expected labels through this same function.
//   - Otherwise every disallowed char becomes '-', the result is truncated so
//     that "-" + an 8-hex sha256 of the ORIGINAL value fits in 63 chars,
//     leading/trailing non-alphanumerics are stripped, and the hash is
//     appended. The hash keeps two distinct inputs that sanitise/truncate to
//     the same prefix (e.g. two >63-char branches sharing 54 leading chars, or
//     "a-" vs "a") distinct, which the "already ran" matching depends on.
//   - If nothing alphanumeric survives, the hash alone is returned: valid,
//     non-empty and deterministic. (An empty label value is valid too, but it
//     would make every such input collide and so defeat de-dup.)
//
// The result is deterministic: writer and reader always compute the same value.
func SanitizeLabelValue(raw string) string {
	if len(validation.IsValidLabelValue(raw)) == 0 {
		return raw
	}
	var b strings.Builder
	for _, r := range raw {
		if isLabelChar(r) {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	base := b.String() // ASCII only from here on, so byte slicing is safe
	if len(base) > labelBaseMaxLen {
		base = base[:labelBaseMaxLen]
	}
	base = trimNonAlnum(base)
	if base == "" {
		return shortHash(raw)
	}
	return base + "-" + shortHash(raw)
}

// GenerateNamePrefix turns an externally-derived string into a metadata.
// generateName prefix (DNS-1123 subdomain chars, lowercase, alphanumeric
// start, ending in "-"). Used only when the PipelineRun template sets neither
// name nor generateName. The random suffix the API server appends keeps names
// unique, so no hash is needed here.
func GenerateNamePrefix(raw string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(raw) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	base := strings.TrimLeft(b.String(), "-")
	if len(base) > generateNameBaseMaxLen-1 {
		base = base[:generateNameBaseMaxLen-1]
	}
	base = strings.TrimRight(base, "-")
	if base == "" {
		base = generateNameFallback
	}
	return base + "-"
}
