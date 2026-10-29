package ops

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestHistoricalChartRetainsGapsFailureAndNeverProbesOnRead(t *testing.T) {
	var calls atomic.Int64
	var down atomic.Bool
	now := time.Now().Truncate(time.Second).UTC()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/api/v1/query_range" {
			t.Errorf("unexpected endpoint: %s", r.URL.Path)
		}
		if r.URL.Query().Get("step") == "" || r.URL.Query().Get("start") == "" || r.URL.Query().Get("end") == "" {
			t.Error("unbounded history query")
		}
		if down.Load() {
			http.Error(w, "down", 503)
			return
		}
		fmt.Fprintf(w, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"status":"200"},"values":[[%d,"0"],[%d,"NaN"],[%d,"2.5"]]}]}}`, now.Add(-30*time.Second).Unix(), now.Add(-15*time.Second).Unix(), now.Unix())
	}))
	defer server.Close()
	s, _ := New(Config{PrometheusURL: server.URL})
	s.sampleHistory(context.Background())
	read := func() Trend {
		t.Helper()
		out := decode(t, s, "")
		for _, tr := range out.Trends {
			if tr.Key == "ingress_rate" {
				return tr
			}
		}
		t.Fatal("missing ingress history")
		return Trend{}
	}
	tr := read()
	if tr.Status != "ok" || len(tr.Series) != 1 || len(tr.Series[0].Points) != 3 {
		t.Fatalf("history %+v", tr)
	}
	pts := tr.Series[0].Points
	if pts[0].Value == nil || *pts[0].Value != 0 || pts[1].Value != nil || pts[2].Value == nil || math.Abs(*pts[2].Value-2.5) > 1e-9 {
		t.Fatalf("missing gap or zero: %+v", pts)
	}
	count := calls.Load()
	read()
	if calls.Load() != count {
		t.Fatal("read triggered Prometheus query")
	}
	down.Store(true)
	s.sampleHistory(context.Background())
	tr = read()
	if tr.Status != "stale" || tr.Reason != "query_failed" || len(tr.Series[0].Points) != 3 {
		t.Fatalf("lost failed history: %+v", tr)
	}
	raw, _ := json.Marshal(tr)
	if strings.Contains(string(raw), "NaN") {
		t.Fatal("non JSON number")
	}
}

func TestHistoryRejectsUnexpectedSeriesAndMalformedPoints(t *testing.T) {
	for _, payload := range []string{
		`{"metric":{},"values":[[1,"bogus"]]}`,
		`{"metric":{"job":"mock-platform1"},"values":[[1,"1"]]}`,
		`{"metric":{},"values":[[2,"1"],[1,"2"]]}`,
	} {
		t.Run(payload, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintf(w, `{"status":"success","data":{"resultType":"matrix","result":[%s]}}`, payload)
			}))
			defer srv.Close()
			s, _ := New(Config{PrometheusURL: srv.URL})
			_, err := s.queryHistory(context.Background(), "up", time.Unix(0, 0), time.Unix(60, 0), 15)
			if err == nil {
				t.Fatal("invalid history accepted")
			}
		})
	}
}

func TestHistoryHonorsConfiguredPrometheusProxyPrefix(t *testing.T) {
	var path string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[]}}`)
	}))
	defer server.Close()
	s, _ := New(Config{PrometheusURL: server.URL + "/prometheus"})
	_, err := s.queryHistory(context.Background(), "up", time.Now().Add(-time.Minute), time.Now(), 15)
	if err != nil {
		t.Fatal(err)
	}
	if path != "/prometheus/api/v1/query_range" {
		t.Fatalf("lost configured base prefix: %s", path)
	}
}
