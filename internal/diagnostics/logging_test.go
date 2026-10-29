package diagnostics_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"partner-hub/internal/diagnostics"
	"partner-hub/internal/observability"
)

// Arbitrary text, errors, group attributes and With attributes cannot bypass
// the fixed message/field allowlist in either diagnostic or downstream output.
func TestLogHandlerKeepsSafeOperationalFieldsWithoutRawMessages(t *testing.T) {
	b := diagnostics.NewBuffer("hub-1")
	var out bytes.Buffer
	l := slog.New(diagnostics.NewLogHandler(observability.NewLogger(&out).Handler(), b))
	l.With("password", "secret-with", "msgId", "27c71118-a387-4f56-bd4b-99dcdb15c9d3").WithGroup("secret-group").Error("secret-raw-message", "error", "secret-error", "traceId", "known-trace", "payload", "secret-payload")
	l.Warn("binding fetch unavailable", "partition", 2)
	l.Info("first delivery result", "outcome", "SUCCESS")
	s := b.Snapshot(diagnostics.Query{Limit: 100})
	if len(s.Items) != 2 || s.Items[0].Kind != "binding_fetch_unavailable" || s.Items[1].Kind != "runtime_error" || s.Items[1].MsgID != "27c71118-a387-4f56-bd4b-99dcdb15c9d3" || s.Items[1].TraceID != "known-trace" {
		t.Fatalf("events=%+v", s.Items)
	}
	encoded, _ := json.Marshal(s)
	for _, secret := range []string{"secret-with", "secret-raw-message", "secret-error", "secret-group", "secret-payload"} {
		if strings.Contains(string(encoded), secret) || strings.Contains(out.String(), secret) {
			t.Fatalf("secret leaked: %s", secret)
		}
	}
	if !strings.Contains(out.String(), "runtime_error") {
		t.Fatal("downstream logger was bypassed")
	}
}
