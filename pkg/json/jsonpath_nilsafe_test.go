package json

import "testing"

// A not-Ready Flux source (GitRepository with no artifact yet) yields an empty
// Details string. Exists/EvalExpr previously called ajson.Must on it, which
// PANICS ("unexpected end of file") — and an unrecovered reconcile panic
// crashloops the whole operator. These assert the accessors now return an error
// instead of panicking. (The test process surviving the call IS the assertion.)

func TestExistsEmptyJSONReturnsErrorNotPanic(t *testing.T) {
	ok, err := Exists("", "$.commitId")
	if ok {
		t.Fatalf("Exists on empty JSON should be false")
	}
	if err == nil {
		t.Fatalf("Exists on empty JSON should return an error, got nil")
	}
}

func TestExistsMalformedJSONReturnsErrorNotPanic(t *testing.T) {
	if _, err := Exists("{not-json", "$.commitId"); err == nil {
		t.Fatalf("Exists on malformed JSON should return an error, got nil")
	}
}

func TestEvalExprEmptyJSONReturnsErrorNotPanic(t *testing.T) {
	if _, err := EvalExpr("", "$.commitId"); err == nil {
		t.Fatalf("EvalExpr on empty JSON should return an error, got nil")
	}
}

// Parity: a Ready source (valid Details JSON) still resolves the path.
func TestExistsReadyJSONStillWorks(t *testing.T) {
	ok, err := Exists(`{"commitId":"abc123","branchName":"main"}`, "$.commitId")
	if err != nil {
		t.Fatalf("Exists on valid JSON returned error: %v", err)
	}
	if !ok {
		t.Fatalf("Exists should find $.commitId in valid JSON")
	}
}
