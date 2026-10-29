package binding

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"partner-hub/internal/model"
	"strings"
	"unicode"
	"unicode/utf8"
)

var ErrInvalidEvent = errors.New("invalid binding event")

func DecodeEvent(body []byte) (model.BindingEvent, error) {
	var zero model.BindingEvent
	if len(body) > 8192 || !utf8.Valid(body) {
		return zero, ErrInvalidEvent
	}
	allowed := map[string]bool{"schemaVersion": true, "eventId": true, "eventType": true, "platformId": true, "clientId": true, "platformUid": true, "occurredAt": true}
	d := json.NewDecoder(bytes.NewReader(body))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return zero, ErrInvalidEvent
	}
	seen := map[string]bool{}
	for d.More() {
		token, err = d.Token()
		if err != nil {
			return zero, ErrInvalidEvent
		}
		key, ok := token.(string)
		if !ok || !allowed[key] || seen[key] {
			return zero, ErrInvalidEvent
		}
		seen[key] = true
		var value json.RawMessage
		if d.Decode(&value) != nil || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return zero, ErrInvalidEvent
		}
	}
	if _, err = d.Token(); err != nil {
		return zero, ErrInvalidEvent
	}
	if _, err = d.Token(); err != io.EOF || len(seen) != 7 {
		return zero, ErrInvalidEvent
	}
	var e model.BindingEvent
	if json.Unmarshal(body, &e) != nil || ValidateEvent(e) != nil {
		return zero, ErrInvalidEvent
	}
	return e, nil
}

func ValidateEvent(e model.BindingEvent) error {
	if e.SchemaVersion != 1 || (e.EventType != "BIND" && e.EventType != "UNBIND") || (e.PlatformID != "PLATFORM_A" && e.PlatformID != "PLATFORM_B") || !validUUID(e.EventID) || !validBindingText(e.ClientID) || !validBindingText(e.PlatformUID) {
		return ErrInvalidEvent
	}
	return nil
}

func validUUID(s string) bool {
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return false
	}
	_, err := hex.DecodeString(strings.ReplaceAll(s, "-", ""))
	return err == nil
}
func validBindingText(s string) bool {
	if !utf8.ValidString(s) || strings.TrimSpace(s) == "" || utf8.RuneCountInString(s) > 128 {
		return false
	}
	for _, c := range s {
		if unicode.IsControl(c) {
			return false
		}
	}
	return true
}
