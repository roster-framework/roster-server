// Package contracts contains the single shared vocabulary. It has no transport dependencies.
package contracts

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	out, err := CloneJSON(value)
	if err != nil {
		panic(err)
	}
	return out
}

// CloneJSON makes one lossless JSON round-trip, preserving arbitrary-precision
// numbers in interface values as json.Number. Callers handling untrusted input
// must apply CheckJSONNumbers at that input boundary before cloning.
func CloneJSON[T any](value T) (T, error) {
	var zero T
	b, err := json.Marshal(value)
	if err != nil {
		return zero, err
	}
	if err = CheckJSONNumbers(b); err != nil {
		return zero, err
	}
	var out T
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if err = dec.Decode(&out); err != nil {
		return zero, err
	}
	if err = requireJSONEOF(dec); err != nil {
		return zero, err
	}
	return out, nil
}

const (
	maxJSONMantissaDigits = 1024
	maxJSONExponent       = 1024
)

// CheckJSONNumbers validates one complete JSON value and bounds each decimal
// token without converting numbers to machine or arbitrary-precision values.
func CheckJSONNumbers(raw json.RawMessage) error {
	if !json.Valid(raw) {
		return errors.New("invalid JSON value")
	}
	for i := 0; i < len(raw); {
		if raw[i] == '"' {
			i++
			for i < len(raw) && raw[i] != '"' {
				if raw[i] == '\\' {
					i += 2
				} else {
					i++
				}
			}
			i++
			continue
		}
		if raw[i] != '-' && (raw[i] < '0' || raw[i] > '9') {
			i++
			continue
		}
		start := i
		for i < len(raw) && !jsonNumberDelimiter(raw[i]) {
			i++
		}
		if err := checkJSONNumber(string(raw[start:i])); err != nil {
			return err
		}
	}
	return nil
}

func jsonNumberDelimiter(b byte) bool {
	switch b {
	case ',', ']', '}', ' ', '\t', '\r', '\n':
		return true
	default:
		return false
	}
}

func checkJSONNumber(number string) error {
	exponentAt := len(number)
	for i := 0; i < len(number); i++ {
		if number[i] == 'e' || number[i] == 'E' {
			exponentAt = i
			break
		}
	}
	mantissaDigits := 0
	for i := 0; i < exponentAt; i++ {
		if number[i] >= '0' && number[i] <= '9' {
			mantissaDigits++
			if mantissaDigits > maxJSONMantissaDigits {
				return fmt.Errorf("JSON mantissa exceeds %d digits", maxJSONMantissaDigits)
			}
		}
	}
	exponent := 0
	if exponentAt < len(number) {
		i := exponentAt + 1
		if i < len(number) && (number[i] == '+' || number[i] == '-') {
			i++
		}
		for ; i < len(number); i++ {
			digit := int(number[i] - '0')
			if exponent > maxJSONExponent/10 || exponent == maxJSONExponent/10 && digit > maxJSONExponent%10 {
				return fmt.Errorf("JSON exponent exceeds %d", maxJSONExponent)
			}
			exponent = exponent*10 + digit
		}
	}
	return nil
}

func requireJSONEOF(dec *json.Decoder) error {
	if _, err := dec.Token(); err != io.EOF {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return fmt.Errorf("invalid trailing JSON: %w", err)
	}
	return nil
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
