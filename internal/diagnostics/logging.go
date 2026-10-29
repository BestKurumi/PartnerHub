package diagnostics

import (
	"context"
	"log/slog"
)

type logHandler struct {
	next   slog.Handler
	buffer *Buffer
	attrs  []slog.Attr
}

// NewLogHandler records WARN/ERROR with fixed messages, then forwards a
// sanitized record to the existing safe JSON handler. Unknown log text is
// replaced even downstream; arbitrary error strings are never forwarded.
func NewLogHandler(next slog.Handler, b *Buffer) slog.Handler {
	return logHandler{next: next, buffer: b}
}
func (h logHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= slog.LevelWarn || h.next != nil && h.next.Enabled(ctx, level)
}
func (h logHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	copied := append([]slog.Attr{}, h.attrs...)
	for _, a := range attrs {
		if a, ok := logAttr(a); ok {
			copied = append(copied, a)
		}
	}
	h.attrs = copied
	return h
}
func (h logHandler) WithGroup(string) slog.Handler { return h }
func logAttr(a slog.Attr) (slog.Attr, bool) {
	a.Value = a.Value.Resolve()
	switch a.Key {
	case "requestId", "msgId", "traceId", "deliveryId", "platform", "outcome", "status":
		if a.Value.Kind() != slog.KindString {
			return slog.Attr{}, false
		}
		a.Value = slog.StringValue(cleanText(a.Value.String(), 128))
		return a, true
	case "partition", "count", "duration":
		switch a.Value.Kind() {
		case slog.KindInt64, slog.KindUint64, slog.KindDuration:
			return a, true
		}
	}
	return slog.Attr{}, false
}
func (h logHandler) Handle(ctx context.Context, r slog.Record) error {
	kind := "runtime_info"
	if r.Level >= slog.LevelError {
		kind = "runtime_error"
	} else if r.Level >= slog.LevelWarn {
		kind = "runtime_warning"
	}
	switch r.Message {
	case "binding fetch unavailable":
		kind = "binding_fetch_unavailable"
	case "binding persistence or commit unconfirmed; partition paused":
		kind = "binding_commit_unconfirmed"
	case "binding cache invalidation failed; TTL convergence required":
		kind = "binding_cache_invalidation_failed"
	case "platform2 fetch unavailable":
		kind = "platform2_fetch_unavailable"
	case "platform2 persistence/commit unconfirmed; partition paused":
		kind = "platform2_commit_unconfirmed"
	case "trade handler panic":
		kind = "ingress_panic"
	case "hub stopped":
		kind = "hub_stopped"
	case "first delivery result":
		kind = "first_delivery_result"
	case "recovery result":
		kind = "recovery_result"
	}
	attrs := append([]slog.Attr{}, h.attrs...)
	r.Attrs(func(a slog.Attr) bool {
		if a, ok := logAttr(a); ok {
			attrs = append(attrs, a)
		}
		return true
	})
	f := Fields{}
	for _, a := range attrs {
		if a.Value.Kind() != slog.KindString {
			continue
		}
		v := a.Value.String()
		switch a.Key {
		case "requestId":
			f.RequestID = v
		case "msgId":
			f.MsgID = v
		case "traceId":
			f.TraceID = v
		case "deliveryId":
			f.DeliveryID = v
		case "platform":
			f.Platform = v
		case "outcome":
			f.Outcome = v
		case "status":
			f.Status = v
		}
	}
	f = cleanFields(f)
	if r.Level >= slog.LevelWarn {
		if _, ok := events[kind]; !ok {
			if r.Level >= slog.LevelError {
				kind = "runtime_error"
			} else {
				kind = "runtime_warning"
			}
		}
		level := "WARN"
		if r.Level >= slog.LevelError {
			level = "ERROR"
		}
		h.buffer.record(kind, level, f)
	}
	clean := slog.NewRecord(r.Time, r.Level, kind, r.PC)
	for _, a := range attrs {
		switch a.Key {
		case "requestId":
			a.Value = slog.StringValue(f.RequestID)
		case "msgId":
			a.Value = slog.StringValue(f.MsgID)
		case "traceId":
			a.Value = slog.StringValue(f.TraceID)
		case "deliveryId":
			a.Value = slog.StringValue(f.DeliveryID)
		case "platform":
			a.Value = slog.StringValue(f.Platform)
		case "outcome":
			a.Value = slog.StringValue(f.Outcome)
		case "status":
			a.Value = slog.StringValue(f.Status)
		}
		if a.Value.Kind() != slog.KindString || a.Value.String() != "" {
			clean.AddAttrs(a)
		}
	}
	if h.next != nil && h.next.Enabled(ctx, r.Level) {
		return h.next.Handle(ctx, clean)
	}
	return nil
}
