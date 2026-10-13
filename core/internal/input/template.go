// Package input owns the one data-only mapping used by subscriptions and conversations.
package input

import (
	c "github.com/roster-framework/roster-server/core/internal/contracts"
	"strconv"
	"strings"
)

func Render(t c.InputTemplate, payload c.Data) (c.Data, error) {
	out := c.Clone(t.Values)
	if out == nil {
		out = c.Data{}
	}
	for key, path := range t.Pointers {
		if key == "" {
			return nil, c.Err("invalid_input", "Input mapping key is empty")
		}
		value, err := Pointer(payload, path)
		if err != nil {
			return nil, err
		}
		out[key] = c.Clone(value)
	}
	return out, nil
}
func Pointer(value any, path string) (any, error) {
	if path == "" {
		return value, nil
	}
	if !strings.HasPrefix(path, "/") {
		return nil, c.Err("invalid_input", "Mapping must be a JSON Pointer")
	}
	for _, part := range strings.Split(path[1:], "/") {
		for i := 0; i < len(part); i++ {
			if part[i] == '~' && (i+1 >= len(part) || part[i+1] != '0' && part[i+1] != '1') {
				return nil, c.Err("invalid_input", "Invalid JSON Pointer escape")
			}
			if part[i] == '~' {
				i++
			}
		}
		part = strings.ReplaceAll(strings.ReplaceAll(part, "~1", "/"), "~0", "~")
		switch current := value.(type) {
		case map[string]any:
			var ok bool
			value, ok = current[part]
			if !ok {
				return nil, c.Err("invalid_input", "JSON Pointer field not found")
			}
		case []any:
			index, err := strconv.Atoi(part)
			if err != nil || index < 0 || index >= len(current) || len(part) > 1 && part[0] == '0' {
				return nil, c.Err("invalid_input", "Invalid JSON Pointer index")
			}
			value = current[index]
		default:
			return nil, c.Err("invalid_input", "JSON Pointer traverses a scalar")
		}
	}
	return value, nil
}
