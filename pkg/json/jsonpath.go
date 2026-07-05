package json

import (
	"errors"
	"strings"

	"github.com/spyzhov/ajson"
)

// EvalExpr / Exists previously used ajson.Must, which PANICS on invalid or
// empty JSON. An empty Details string (a not-Ready source) therefore crashed the
// whole reconcile — and, unrecovered, the entire operator. Return the unmarshal
// error instead so callers can handle a not-ready/malformed source gracefully.
func EvalExpr(json string, expr string) (string, error) {
	root, err := ajson.Unmarshal([]byte(json))
	if err != nil {
		return "", err
	}
	result, err := ajson.Eval(root, expr)
	if err != nil {
		return "", err
	}
	return result.String(), nil
}

func Exists(json string, expr string) (bool, error) {
	root, err := ajson.Unmarshal([]byte(json))
	if err != nil {
		return false, err
	}
	result, err := ajson.Eval(root, expr)
	if err != nil {
		return false, err
	}
	if strings.Contains(result.String(), "null") {
		return false, errors.New("Expression " + expr + " evaluates to null.")
	}
	return true, nil
}
