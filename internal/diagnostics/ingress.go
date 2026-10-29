package diagnostics

import "partner-hub/internal/ingress"

func ObserveIngress(b *Buffer) func(ingress.HTTPObservation) {
	return func(o ingress.HTTPObservation) {
		b.Record(o.Kind, Fields{RequestID: o.RequestID, MsgID: o.MsgID, TraceID: o.TraceID, DeliveryID: o.DeliveryID, Platform: o.Platform, Outcome: o.Outcome, Status: o.Status})
	}
}
