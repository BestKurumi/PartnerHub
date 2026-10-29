package main

import (
	"testing"
	"time"
)

// Catches treating unavailable DB rows as zero latency and recovery timestamps as first success.
func TestObservationKeepsMissingAndFailedFirstFacts(t *testing.T) {
	now := time.Date(2026, 10, 23, 1, 0, 0, 0, time.UTC)
	rows := makeInputs(2, "client")
	applyObservation(&rows[0], Observation{Received: now, Stored: now.Add(time.Second), Platform: "PLATFORM_A", Outcome: "UNKNOWN", Started: true, Finished: tp(now.Add(2 * time.Second)), Recovery: tp(now.Add(9 * time.Second))})
	applyObservation(&rows[0], Observation{Received: now, Stored: now.Add(time.Second), Platform: "PLATFORM_B", Outcome: "SUCCESS", Started: true, Finished: tp(now.Add(3 * time.Second))})
	a := summarize(rows).Platforms["PLATFORM_A"]
	if rows[1].ReceivedAt != nil || a.Failed != 1 || a.Missing != 1 || a.Eligible || a.P99MS != nil || a.Recovery.Success != 1 {
		t.Fatalf("observation lost cohort or first fact: %+v", a)
	}
}

func TestVisibilityUsesActualFirstObservation(t *testing.T) {
	now := time.Date(2026, 10, 23, 1, 0, 0, 0, time.UTC)
	submitted := now.Add(-time.Second)
	r := makeInputs(1, "client")[0]
	r.SubmittedAt = &submitted
	observeVisible(&r, now)
	observeVisible(&r, now.Add(time.Second))
	if r.ObservedVisibleAt == nil || !r.ObservedVisibleAt.Equal(now) {
		t.Fatal("first visibility observation overwritten")
	}
	got := summarize([]Input{r}).Visibility
	if !got.Eligible || got.P99MS == nil || *got.P99MS != 1000 {
		t.Fatalf("visibility must use local submit time: %+v", got)
	}
}

func TestRecoveredRouteAttemptOneNeverBecomesOriginalFirstDelivery(t *testing.T) {
	now := time.Now().UTC()
	row := makeInputs(1, "client")[0]
	applyObservation(&row, Observation{Received: now, Stored: now.Add(time.Second), Platform: "PLATFORM_A", FirstOutcome: "NOT_ATTEMPTED", Outcome: "SUCCESS", FirstLeaseOwner: "worker", Started: true, Finished: tp(now.Add(2 * time.Second))})
	p := summarize([]Input{row}).Platforms["PLATFORM_A"]
	if p.Success != 0 || p.Eligible || row.Targets["PLATFORM_A"].FirstOutcome != "NOT_ATTEMPTED" || p.Recovery.Success != 1 {
		t.Fatalf("route recovery promoted into first delivery: %+v", p)
	}
	// A peer that really succeeded in original HTTP202 partial handoff stays first.
	applyObservation(&row, Observation{Received: now, Stored: now.Add(time.Second), Platform: "PLATFORM_B", FirstOutcome: "SUCCESS", Outcome: "SUCCESS", Started: true, Finished: tp(now.Add(time.Second))})
	b := summarize([]Input{row}).Platforms["PLATFORM_B"]
	if b.Success != 1 || b.Recovery.Success != 0 {
		t.Fatalf("partial first peer lost: %+v", b)
	}
}
