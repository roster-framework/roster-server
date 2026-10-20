// Package contracts contains the single shared vocabulary. It has no transport dependencies.
package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"time"
)

type Data = map[string]any
type Schema = json.RawMessage
type Ref struct {
	Namespace string `json:"namespace"`
	ID        string `json:"id"`
}

func (r Ref) Key() string { return r.Namespace + "/" + r.ID }

var validID = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,99}$`)

func ValidID(s string) bool { return validID.MatchString(s) }
func (r Ref) Valid() bool   { return ValidID(r.Namespace) && ValidID(r.ID) }
func ID(parts ...string) string {
	b, _ := json.Marshal(parts)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func Now() time.Time { return time.Now().UTC() }
func Clone[T any](value T) T {
	b, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	var out T
	if err = json.Unmarshal(b, &out); err != nil {
		panic(err)
	}
	return out
}

type Fault struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *Fault) Error() string       { return e.Code + ": " + e.Message }
func Err(code, message string) error { return &Fault{code, message} }
func AsFault(err error) *Fault {
	var f *Fault
	if errors.As(err, &f) {
		return f
	}
	return &Fault{"internal", "Operation failed"}
}
