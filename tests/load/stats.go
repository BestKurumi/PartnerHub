package main

import (
	"math"
	"sort"
	"time"
)

type Target struct {
	FirstOutcome  string     `json:"first_outcome"`
	SendStarted   bool       `json:"send_started"`
	FirstFinished *time.Time `json:"first_finished_at"`
	RecoveredAt   *time.Time `json:"recovered_at"`
}
type Input struct {
	OfferedAt         *time.Time        `json:"offered_at"`
	ObservedVisibleAt *time.Time        `json:"observed_visible_at"`
	MsgID             string            `json:"msg_id"`
	TraceID           string            `json:"trace_id"`
	ClientID          string            `json:"client_id"`
	PlannedAt         time.Time         `json:"planned_at"`
	SubmittedAt       *time.Time        `json:"submitted_at"`
	HTTPFinishedAt    *time.Time        `json:"http_finished_at"`
	HTTPStatus        int               `json:"http_status"`
	Disposition       string            `json:"disposition"`
	Bytes             int               `json:"bytes"`
	ReceivedAt        *time.Time        `json:"received_at"`
	StoredAt          *time.Time        `json:"stored_at"`
	Targets           map[string]Target `json:"targets"`
}
type Latency struct {
	Success      int      `json:"success"`
	Missing      int      `json:"missing"`
	Eligible     bool     `json:"eligible"`
	P99MS        *float64 `json:"all_cohort_p99_ms"`
	SuccessP99MS *float64 `json:"successful_samples_p99_ms"`
	Coverage     float64  `json:"coverage"`
}
type Platform struct {
	Latency
	Failed   int     `json:"failed"`
	Recovery Latency `json:"recovery"`
}
type Summary struct {
	HTTPUnconfirmed  int                 `json:"http_unconfirmed"`
	InvalidResponses int                 `json:"invalid_responses"`
	Planned          int                 `json:"planned"`
	Submitted        int                 `json:"submitted"`
	HTTPRejected     int                 `json:"http_rejected"`
	TransportErrors  int                 `json:"transport_errors"`
	LocalRejected    int                 `json:"local_rejected"`
	NotSubmitted     int                 `json:"not_submitted"`
	Unrecorded       int                 `json:"unrecorded"`
	Platforms        map[string]Platform `json:"platforms"`
	HTTP             Latency             `json:"http_response"`
	Visibility       Latency             `json:"visibility_upper_bound_from_submit"`
	Archive          Latency             `json:"db_insert_from_ingress"`
}

func summarize(rows []Input) Summary {
	s := Summary{Planned: len(rows), Platforms: map[string]Platform{}}
	archive, visible, http := []float64{}, []float64{}, []float64{}
	for _, r := range rows {
		switch r.Disposition {
		case "http_unconfirmed":
			s.Submitted++
			s.HTTPUnconfirmed++
		case "invalid_response":
			s.Submitted++
			s.InvalidResponses++
		case "accepted":
			s.Submitted++
		case "http_rejected":
			s.Submitted++
			s.HTTPRejected++
		case "transport_error":
			s.Submitted++
			s.TransportErrors++
		case "local_rejected":
			s.LocalRejected++
		default:
			s.NotSubmitted++
		}
		if r.ReceivedAt == nil {
			s.Unrecorded++
		}
		if d, ok := elapsed(r.SubmittedAt, r.ObservedVisibleAt); ok {
			visible = append(visible, d)
		}
		if d, ok := elapsed(r.SubmittedAt, r.HTTPFinishedAt); ok {
			http = append(http, d)
		}
		if d, ok := elapsed(r.ReceivedAt, r.StoredAt); ok {
			archive = append(archive, d)
		}
	}
	s.Archive = latency(archive, len(rows))
	s.Visibility = latency(visible, len(rows))
	s.HTTP = latency(http, len(rows))
	for _, id := range []string{"PLATFORM_A", "PLATFORM_B"} {
		first, recovery := []float64{}, []float64{}
		failed := 0
		for _, r := range rows {
			t, ok := r.Targets[id]
			if !ok {
				continue
			}
			if t.FirstOutcome != "" && t.FirstOutcome != "SUCCESS" {
				failed++
			}
			if d, ok := elapsed(r.ReceivedAt, t.FirstFinished); ok && t.FirstOutcome == "SUCCESS" && t.SendStarted {
				first = append(first, d)
			}
			if d, ok := elapsed(r.ReceivedAt, t.RecoveredAt); ok {
				recovery = append(recovery, d)
			}
		}
		p := Platform{Latency: latency(first, len(rows)), Failed: failed, Recovery: latency(recovery, len(rows))}
		p.Missing -= failed
		s.Platforms[id] = p
	}
	return s
}
func elapsed(a, b *time.Time) (float64, bool) {
	if a == nil || b == nil || b.Before(*a) {
		return 0, false
	}
	return float64(b.Sub(*a)) / float64(time.Millisecond), true
}
func latency(values []float64, n int) Latency {
	l := Latency{Success: len(values), Missing: n - len(values)}
	if n > 0 {
		l.Coverage = float64(len(values)) / float64(n)
		l.Eligible = len(values) == n
	}
	if len(values) > 0 {
		sort.Float64s(values)
		v := values[int(math.Ceil(float64(len(values))*.99))-1]
		l.SuccessP99MS = &v
		if l.Eligible {
			l.P99MS = &v
		}
	}
	return l
}
