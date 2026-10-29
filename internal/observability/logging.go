package observability

import (
	"context"
	"io"
	"log/slog"
)

// NewLogger emits structured JSON. Only scalar operational and correlation
// fields can cross this boundary; payload, UID, credentials and raw errors are
// intentionally excluded, including attributes attached through With.
func NewLogger(w io.Writer) *slog.Logger {
	return slog.New(safeHandler{next: slog.NewJSONHandler(w, nil)})
}

type safeHandler struct{ next slog.Handler }

func (h safeHandler) Enabled(ctx context.Context, l slog.Level) bool { return h.next.Enabled(ctx, l) }
func safeAttr(a slog.Attr) (slog.Attr, bool) {
	switch a.Key {
	case "msgId", "deliveryId", "traceId", "platform", "outcome", "kind", "status", "instance", "address", "partition", "count", "duration":
	default:
		return slog.Attr{}, false
	}
	a.Value = a.Value.Resolve()
	switch a.Value.Kind() {
	case slog.KindString:
		if len(a.Value.String()) > 256 {
			return slog.Attr{}, false
		}
	case slog.KindInt64, slog.KindUint64, slog.KindFloat64, slog.KindBool, slog.KindDuration:
	default:
		return slog.Attr{}, false
	}
	return a, true
}
func (h safeHandler) Handle(ctx context.Context, r slog.Record) error {
	clean := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	r.Attrs(func(a slog.Attr) bool {
		if a, ok := safeAttr(a); ok {
			clean.AddAttrs(a)
		}
		return true
	})
	return h.next.Handle(ctx, clean)
}
func (h safeHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	safe := make([]slog.Attr, 0, len(attrs))
	for _, a := range attrs {
		if a, ok := safeAttr(a); ok {
			safe = append(safe, a)
		}
	}
	return safeHandler{next: h.next.WithAttrs(safe)}
}

// Groups cannot be used to hide arbitrary nested application payloads.
func (h safeHandler) WithGroup(string) slog.Handler { return h }
