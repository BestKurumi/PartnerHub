package binding_test

import (
	"errors"
	"strings"
	"testing"

	"partner-hub/internal/binding"
)

const eventJSON = `{"schemaVersion":1,"eventId":"e84b51f2-198b-49c1-a28d-8debf1620061","eventType":"BIND","platformId":"PLATFORM_A","clientId":"00001","platformUid":"00009","occurredAt":1792630800123}`

// Catches coercion/defaults, unknown identities and poisoned events altering relations.
func TestBindingDecode(t *testing.T) {
	for _, body := range []string{eventJSON, strings.Replace(strings.Replace(eventJSON, "BIND", "UNBIND", 1), "PLATFORM_A", "PLATFORM_B", 1)} {
		e, err := binding.DecodeEvent([]byte(body))
		if err != nil {
			t.Fatal(err)
		}
		if e.SchemaVersion != 1 || e.ClientID != "00001" || e.PlatformUID != "00009" || e.OccurredAt != 1792630800123 {
			t.Fatalf("event lost values: %+v", e)
		}
	}
}

func TestBindingRejectsPoison(t *testing.T) {
	for name, body := range map[string]string{
		"version":          strings.Replace(eventJSON, `"schemaVersion":1`, `"schemaVersion":2`, 1),
		"unknown platform": strings.Replace(eventJSON, "PLATFORM_A", "PLATFORM_C", 1),
		"unknown type":     strings.Replace(eventJSON, `"BIND"`, `"UPDATE"`, 1),
		"bad uuid":         strings.Replace(eventJSON, "e84b51f2-198b-49c1-a28d-8debf1620061", "oops", 1),
		"number ID":        strings.Replace(eventJSON, `"00001"`, `1`, 1),
		"null":             strings.Replace(eventJSON, `"00009"`, `null`, 1),
		"blank":            strings.Replace(eventJSON, `"00009"`, `" "`, 1),
		"long ID":          strings.Replace(eventJSON, "00009", strings.Repeat("界", 129), 1),
		"control":          strings.Replace(eventJSON, "00009", `a\n`, 1),
		"duplicate":        strings.TrimSuffix(eventJSON, "}") + `,"event\u0049d":"e84b51f2-198b-49c1-a28d-8debf1620061"}`,
		"extra":            strings.TrimSuffix(eventJSON, "}") + `,"extra":1}`,
		"missing":          strings.Replace(eventJSON, `"occurredAt":1792630800123`, `"other":0`, 1),
		"fractional":       strings.Replace(eventJSON, "1792630800123", "1.1", 1),
		"large integer":    strings.Replace(eventJSON, "1792630800123", "9223372036854775808", 1),
		"invalid utf8":     strings.Replace(eventJSON, "00009", string([]byte{0xff}), 1),
		"multiple":         eventJSON + `{}`,
		"oversize":         eventJSON + strings.Repeat(" ", 8192),
	} {
		t.Run(name, func(t *testing.T) {
			e, err := binding.DecodeEvent([]byte(body))
			if !errors.Is(err, binding.ErrInvalidEvent) || e.EventID != "" {
				t.Fatalf("poison accepted: %+v %v", e, err)
			}
		})
	}
}
