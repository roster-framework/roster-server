// Package authority owns the single immutable admission policy used by Core.
package authority

import (
	"bytes"
	"encoding/json"
	"io"
	"reflect"

	c "github.com/roster-framework/roster-server/core/internal/contracts"
)

const MaxPolicyBytes = 64 << 10

type Principal struct {
	Namespace string `json:"namespace"`
	ID        string `json:"id"`
}

type Grant struct {
	Principal  Principal `json:"principal"`
	Action     string    `json:"action"`
	Target     Principal `json:"target"`
	Operations []string  `json:"operations"`
}

type policyFile struct {
	Schema string  `json:"schema"`
	Grants []Grant `json:"grants"`
}

type permission struct {
	principal string
	action    string
	target    string
	operation string
}

type Service struct{ grants map[permission]struct{} }

type shutdownCapability struct {
	owner    *Service
	instance any
}

func (cap shutdownCapability) AuthorizeAbort(instance any, target c.Ref, operation string) error {
	if cap.owner == nil || !sameInstance(cap.instance, instance) || target.Namespace != "nodes" || !target.Valid() || !c.ValidID(operation) {
		return c.Err("forbidden", "Admission is not authorized")
	}
	return nil
}

// Empty returns the fail-closed policy used when no policy file is supplied.
func Empty() *Service { return &Service{grants: map[permission]struct{}{}} }

// ShutdownCapability returns an instance-bound capability for trusted host
// cleanup. The caller cannot choose an origin or use it for invoke admission.
func (s *Service) ShutdownCapability(instance any) c.HostShutdownCapability {
	return shutdownCapability{owner: s, instance: instance}
}

func sameInstance(expected, actual any) bool {
	e, a := reflect.ValueOf(expected), reflect.ValueOf(actual)
	return e.IsValid() && a.IsValid() && e.Type() == a.Type() && e.Kind() == reflect.Ptr && a.Kind() == reflect.Ptr && e.Pointer() == a.Pointer()
}

// New validates grants and returns an immutable policy value.
func New(grants []Grant) (*Service, error) {
	if len(grants) > 512 {
		return nil, c.Err("invalid_input", "Authority policy exceeds grant limit")
	}
	permissions := make(map[permission]struct{})
	for _, grant := range grants {
		principal := c.Ref{Namespace: grant.Principal.Namespace, ID: grant.Principal.ID}
		target := c.Ref{Namespace: grant.Target.Namespace, ID: grant.Target.ID}
		if !principal.Valid() || (principal.Namespace != "operators" && principal.Namespace != "nodes") || principal.Namespace == "operators" && principal.ID != "local" || target.Namespace != "nodes" || !target.Valid() {
			return nil, c.Err("invalid_input", "Invalid authority principal or target")
		}
		if grant.Action != "invoke" && grant.Action != "abort" || len(grant.Operations) < 1 || len(grant.Operations) > 64 {
			return nil, c.Err("invalid_input", "Invalid authority action or operation list")
		}
		seen := make(map[string]bool, len(grant.Operations))
		for _, operation := range grant.Operations {
			if !c.ValidID(operation) || seen[operation] {
				return nil, c.Err("invalid_input", "Invalid or duplicate authority operation")
			}
			seen[operation] = true
			key := permission{principal: principal.Key(), action: grant.Action, target: target.ID, operation: operation}
			if _, exists := permissions[key]; exists {
				return nil, c.Err("invalid_input", "Duplicate authority grant")
			}
			permissions[key] = struct{}{}
		}
	}
	return &Service{grants: permissions}, nil
}

// Parse validates a strict, bounded roster-authority/1 JSON policy.
func Parse(data []byte) (*Service, error) {
	if len(data) == 0 || len(data) > MaxPolicyBytes || !jsonValueHasUniqueKeys(data) {
		return nil, c.Err("invalid_input", "Invalid authority policy")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var file policyFile
	if err := decoder.Decode(&file); err != nil {
		return nil, c.Err("invalid_input", "Invalid authority policy")
	}
	if err := decoder.Decode(new(any)); err != io.EOF || file.Schema != "roster-authority/1" || file.Grants == nil {
		return nil, c.Err("invalid_input", "Invalid authority policy")
	}
	return New(file.Grants)
}

func (s *Service) Authorize(origin c.AdmissionOrigin, action string, target c.Ref, operation string) error {
	if s == nil || !validOrigin(origin) || action != "invoke" && action != "abort" || target.Namespace != "nodes" || !target.Valid() || !c.ValidID(operation) {
		return c.Err("forbidden", "Admission is not authorized")
	}
	root := permission{principal: origin.Root.Key(), action: action, target: target.ID, operation: operation}
	if _, ok := s.grants[root]; !ok {
		return c.Err("forbidden", "Admission is not authorized")
	}
	if origin.Actor != origin.Root {
		actor := permission{principal: origin.Actor.Key(), action: action, target: target.ID, operation: operation}
		if _, ok := s.grants[actor]; !ok {
			return c.Err("forbidden", "Admission is not authorized")
		}
	}
	return nil
}

func validOrigin(origin c.AdmissionOrigin) bool {
	return c.ValidAdmissionOrigin(origin)
}

// jsonValueHasUniqueKeys rejects duplicate object names at every nesting level.
func jsonValueHasUniqueKeys(data []byte) bool {
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	var value func(int) bool
	value = func(depth int) bool {
		if depth > 64 {
			return false
		}
		token, err := d.Token()
		if err != nil {
			return false
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return true
		}
		switch delim {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				key, err := d.Token()
				if err != nil {
					return false
				}
				name, ok := key.(string)
				if !ok || seen[name] {
					return false
				}
				seen[name] = true
				if !value(depth + 1) {
					return false
				}
			}
			end, err := d.Token()
			return err == nil && end == json.Delim('}')
		case '[':
			for d.More() {
				if !value(depth + 1) {
					return false
				}
			}
			end, err := d.Token()
			return err == nil && end == json.Delim(']')
		default:
			return false
		}
	}
	if !value(0) {
		return false
	}
	_, err := d.Token()
	return err == io.EOF
}
