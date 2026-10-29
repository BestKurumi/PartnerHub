package main

import (
	"testing"
	"time"
)

// Catches omitting rejected/timeout/missing inputs and promoting recovery into first delivery.
func TestSummaryKeepsAllInputsAndSeparatesRecovery(t *testing.T) {
	now := time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC)
	rows := []Input{
		{Disposition: "accepted", ReceivedAt: &now, StoredAt: tp(now.Add(time.Second)), Targets: map[string]Target{"PLATFORM_A": {FirstOutcome: "SUCCESS", SendStarted: true, FirstFinished: tp(now.Add(2 * time.Second))}, "PLATFORM_B": {FirstOutcome: "SUCCESS", SendStarted: true, FirstFinished: tp(now.Add(3 * time.Second))}}},
		{Disposition: "accepted", ReceivedAt: &now, Targets: map[string]Target{"PLATFORM_A": {FirstOutcome: "UNKNOWN", SendStarted: true, FirstFinished: tp(now.Add(time.Second)), RecoveredAt: tp(now.Add(9 * time.Second))}}},
		{Disposition: "http_rejected"}, {Disposition: "transport_error"}, {Disposition: "local_rejected"}, {Disposition: "planned"},
	}
	got := summarize(rows)
	if got.Planned != 6 || got.Submitted != 4 || got.HTTPRejected != 1 || got.TransportErrors != 1 || got.LocalRejected != 1 || got.NotSubmitted != 1 || got.Unrecorded != 4 {
		t.Fatalf("denominator lost: %+v", got)
	}
	a := got.Platforms["PLATFORM_A"]
	if a.Success != 1 || a.Failed != 1 || a.Missing != 4 || a.Eligible || a.P99MS != nil || a.SuccessP99MS == nil || *a.SuccessP99MS != 2000 || a.Coverage != float64(1)/6 {
		t.Fatalf("first delivery selected successes: %+v", a)
	}
	if a.Recovery.Success != 1 || a.Recovery.SuccessP99MS == nil || *a.Recovery.SuccessP99MS != 9000 {
		t.Fatalf("recovery mixed with first: %+v", a)
	}
	if got.Archive.Success != 1 || got.Archive.Eligible || got.Archive.P99MS != nil {
		t.Fatalf("archive omissions hidden: %+v", got.Archive)
	}
}
func tp(t time.Time) *time.Time { return &t }

func TestAllCohortP99RequiresEverySuccessfulStartedTarget(t *testing.T) {
	now := time.Now()
	rows := make([]Input, 2)
	for i := range rows {
		rows[i] = Input{Disposition: "accepted", ReceivedAt: &now, Targets: map[string]Target{"PLATFORM_A": {FirstOutcome: "SUCCESS", SendStarted: true, FirstFinished: tp(now.Add(time.Duration(i+1) * time.Second))}, "PLATFORM_B": {FirstOutcome: "SUCCESS", SendStarted: true, FirstFinished: tp(now.Add(time.Second))}}}
	}
	a := summarize(rows).Platforms["PLATFORM_A"]
	if !a.Eligible || a.P99MS == nil || *a.P99MS != 2000 {
		t.Fatalf("complete cohort unavailable: %+v", a)
	}
	rows[1].Targets["PLATFORM_A"] = Target{FirstOutcome: "SUCCESS", SendStarted: false, FirstFinished: tp(now.Add(time.Second))}
	if summarize(rows).Platforms["PLATFORM_A"].Eligible {
		t.Fatal("unsent target counted successful")
	}
}
