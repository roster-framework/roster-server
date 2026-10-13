package session

import (
	"errors"
	"regexp"
)

var ErrNotFound = errors.New("session not found")
var ErrConflict = errors.New("session version conflict")
var ErrUnsupported = errors.New("memory operation unsupported")
var ErrExpired = errors.New("original log history retired")
var ErrUnavailable = errors.New("session evidence unavailable")
var idPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,99}$`)

func ValidID(id string) bool { return idPattern.MatchString(id) }

type Algorithm struct {
	ID      string `json:"id"`
	Version string `json:"version"`
}
