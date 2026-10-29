package ops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// A nil value is an observed gap, never an invented zero latency or rate.
type Point struct {
	Time  time.Time `json:"time"`
	Value *float64  `json:"value"`
}
type TimeSeries struct {
	Labels map[string]string `json:"labels"`
	Points []Point           `json:"points"`
}
type Trend struct {
	Key    string       `json:"key"`
	Status string       `json:"status"`
	Reason string       `json:"reason,omitempty"`
	Start  time.Time    `json:"start"`
	End    time.Time    `json:"end"`
	Step   int          `json:"step"`
	Series []TimeSeries `json:"series"`
}
type cachedTrend struct {
	trend   Trend
	fetched time.Time
	failed  bool
}

func historyWindow(window string) (time.Duration, int) {
	switch window {
	case "1h":
		return time.Hour, 60
	case "6h":
		return 6 * time.Hour, 300
	default:
		return 15 * time.Minute, 15
	}
}
func historyDefinitions(window string) []definition {
	var out []definition
	for _, d := range definitions(window) {
		if d.key == "ingress_rate" || d.key == "first_rate" || d.key == "first_p99" {
			out = append(out, d)
		}
	}
	return out
}

// History is sampled by the bounded background worker pool. Browser reads never
// issue queries and cannot supply PromQL, endpoints, ranges or arbitrary steps.
func (s *Service) sampleHistory(ctx context.Context) {
	if s.endpoint == "" || ctx.Err() != nil {
		return
	}
	type job struct {
		window string
		d      definition
	}
	queue := make(chan job)
	var wg sync.WaitGroup
	now := time.Now().UTC()
	for range s.workers {
		wg.Go(func() {
			for j := range queue {
				if ctx.Err() != nil {
					return
				}
				duration, step := historyWindow(j.window)
				tr, err := s.queryHistory(ctx, j.d.expression, now.Add(-duration), now, step)
				tr.Key = j.d.key
				s.mu.Lock()
				key := j.window + "/" + j.d.key
				old := s.histories[key]
				if err == nil {
					old = cachedTrend{trend: tr, fetched: time.Now().UTC()}
				} else {
					old.failed = true
				}
				s.histories[key] = old
				s.mu.Unlock()
			}
		})
	}
	for _, window := range windows {
		for _, d := range historyDefinitions(window) {
			select {
			case queue <- job{window, d}:
			case <-ctx.Done():
				close(queue)
				wg.Wait()
				return
			}
		}
	}
	close(queue)
	wg.Wait()
}
func (s *Service) queryHistory(ctx context.Context, expression string, start, end time.Time, step int) (Trend, error) {
	out := Trend{Start: start, End: end, Step: step, Series: []TimeSeries{}}
	ctx, cancel := context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()
	u, _ := url.Parse(s.endpoint)
	u.Path = strings.TrimSuffix(u.Path, "/query") + "/query_range"
	q := u.Query()
	q.Set("query", expression)
	q.Set("start", strconv.FormatFloat(float64(start.UnixMilli())/1000, 'f', 3, 64))
	q.Set("end", strconv.FormatFloat(float64(end.UnixMilli())/1000, 'f', 3, 64))
	q.Set("step", strconv.Itoa(step))
	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return out, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return out, errors.New("history query failed")
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, s.config.MaxBytes+1))
	if err != nil || int64(len(body)) > s.config.MaxBytes {
		return out, errors.New("history response limit")
	}
	var data struct {
		Status string
		Data   struct {
			ResultType string
			Result     []struct {
				Metric map[string]string
				Values [][]json.RawMessage
			}
		}
	}
	if json.Unmarshal(body, &data) != nil || data.Status != "success" || data.Data.ResultType != "matrix" || data.Data.Result == nil || len(data.Data.Result) > min(32, s.config.MaxSeries) {
		return out, errors.New("invalid history matrix")
	}
	for _, row := range data.Data.Result {
		if len(row.Values) > 121 || len(row.Metric) > 16 {
			return out, errors.New("history series limit")
		}
		if job := row.Metric["job"]; job != "" && job != "hub" {
			return out, errors.New("unexpected history source")
		}
		series := TimeSeries{Labels: map[string]string{}, Points: []Point{}}
		for key, label := range row.Metric {
			switch key {
			case "status", "platform", "outcome", "job", "instance":
				if len(label) > 128 {
					return out, errors.New("history label limit")
				}
				series.Labels[key] = label
			}
		}
		previous := math.Inf(-1)
		for _, pair := range row.Values {
			if len(pair) != 2 {
				return out, errors.New("invalid history point")
			}
			var ts float64
			var raw string
			if json.Unmarshal(pair[0], &ts) != nil || json.Unmarshal(pair[1], &raw) != nil || math.IsNaN(ts) || math.IsInf(ts, 0) || ts <= previous || ts < float64(start.UnixMilli())/1000-0.001 || ts > float64(end.UnixMilli())/1000+0.001 {
				return out, errors.New("invalid history timestamp")
			}
			value, err := strconv.ParseFloat(raw, 64)
			if err != nil {
				return out, fmt.Errorf("invalid history value")
			}
			point := Point{Time: time.UnixMilli(int64(ts * 1000)).UTC()}
			if !math.IsNaN(value) && !math.IsInf(value, 0) {
				point.Value = &value
			}
			series.Points = append(series.Points, point)
			previous = ts
		}
		out.Series = append(out.Series, series)
	}
	return out, nil
}

// Caller holds s.mu.RLock. Deep copies keep a returned snapshot immutable.
func (s *Service) historySnapshotLocked(window string) []Trend {
	out := []Trend{}
	for _, d := range historyDefinitions(window) {
		c := s.histories[window+"/"+d.key]
		tr := c.trend
		tr.Key = d.key
		tr.Status = "unavailable"
		tr.Reason = "not_sampled"
		tr.Series = []TimeSeries{}
		hasValue := false
		for _, series := range c.trend.Series {
			copySeries := TimeSeries{Labels: map[string]string{}, Points: []Point{}}
			for k, v := range series.Labels {
				copySeries.Labels[k] = v
			}
			for _, point := range series.Points {
				p := point
				if p.Value != nil {
					value := *p.Value
					p.Value = &value
					hasValue = true
				}
				copySeries.Points = append(copySeries.Points, p)
			}
			tr.Series = append(tr.Series, copySeries)
		}
		if !c.fetched.IsZero() {
			tr.Reason = "no_samples"
		}
		if hasValue {
			tr.Status = "ok"
			tr.Reason = ""
		}
		if c.failed {
			tr.Reason = "query_failed"
			if hasValue {
				tr.Status = "stale"
			}
		} else if !c.fetched.IsZero() && time.Since(c.fetched) > s.config.StaleAfter {
			tr.Reason = "expired"
			if hasValue {
				tr.Status = "stale"
			}
		}
		out = append(out, tr)
	}
	return out
}
