package session

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

// OperationalRecord recognizes reserved, validated diagnostic facts. It never
// guesses from ordinary note text. Raw records and usage queries remain intact.
func OperationalRecord(e Entry) (bool, error) {
	if _, found, err := DecodeModelUsage(e); found {
		return true, err
	}
	if e.Kind != Note {
		return false, nil
	}
	switch e.ID {
	case "engine.started", "engine.returned", "engine.interrupted":
	default:
		return false, nil
	}
	var fact struct {
		Protocol   string `json:"protocol"`
		Phase      string `json:"phase"`
		DurationMS *int64 `json:"duration_ms"`
	}
	d := json.NewDecoder(bytes.NewBufferString(e.Text))
	d.DisallowUnknownFields()
	if d.Decode(&fact) != nil || !errors.Is(d.Decode(new(any)), io.EOF) ||
		fact.Protocol != "roster-engine-observation/1" || e.ID != "engine."+fact.Phase ||
		fact.DurationMS == nil || *fact.DurationMS < 0 || (fact.Phase == "started" && *fact.DurationMS != 0) || e.Validate() != nil {
		return true, errors.New("invalid engine observation")
	}
	return true, nil
}
