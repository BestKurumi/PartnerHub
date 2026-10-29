package ops

import (
	"testing"
	"time"
)

// Shared observations must survive a second instance whose first sample failed.
// That instance has never published a valid resource value.
func TestSharedResourcePreservesSuccessfulPeerAndHistory(t *testing.T) {
	for _, historical := range []bool{false, true} {
		t.Run(map[bool]string{false: "healthy", true: "historical"}[historical], func(t *testing.T) {
			s, _ := New(Config{})
			now := time.Now().UTC()
			up := 1.
			age := 1.
			if historical {
				up = 0
				age = 300
			}
			s.cache["15m/mysql_connections"] = cached{fetched: now, samples: []Sample{{Value: 7, SampledAt: now, Labels: map[string]string{}}}}
			s.cache["15m/resource_up"] = cached{fetched: now, samples: []Sample{{Value: up, SampledAt: now, Labels: map[string]string{"job": "hub", "instance": "hub-1", "kind": "mysql"}}, {Value: 0, SampledAt: now, Labels: map[string]string{"job": "hub", "instance": "hub-2", "kind": "mysql"}}}}
			s.cache["15m/resource_age"] = cached{fetched: now, samples: []Sample{{Value: age, SampledAt: now, Labels: map[string]string{"job": "hub", "instance": "hub-1", "kind": "mysql"}}, {Value: float64(now.Unix()), SampledAt: now, Labels: map[string]string{"job": "hub", "instance": "hub-2", "kind": "mysql"}}}}
			got := metric(t, s.snapshot("15m"), "mysql_connections")
			if len(got.Samples) != 1 || got.Samples[0].Value != 7 || got.Status == "unavailable" {
				t.Fatalf("peer erased known data: %+v", got)
			}
			if historical && got.Status != "stale" {
				t.Fatalf("history looks fresh: %+v", got)
			}
		})
	}
}

// Match both job and instance: independently scraped Mock roles may have the
// same instance label, and a never-initialized peer's zero is not a receipt.
func TestReceiptsHealthIsCorrelatedPerSource(t *testing.T) {
	for _, historical := range []bool{false, true} {
		t.Run(map[bool]string{false: "healthy", true: "historical"}[historical], func(t *testing.T) {
			s, _ := New(Config{})
			now := time.Now().UTC()
			up := 1.
			age := 1.
			if historical {
				up = 0
				age = 300
			}
			good := map[string]string{"job": "mock-platform1", "instance": "shared-address"}
			never := map[string]string{"job": "mock-platform2", "instance": "shared-address"}
			got := Metric{Status: "ok", Samples: []Sample{{Value: 7, SampledAt: now, Labels: good}, {Value: 0, SampledAt: now, Labels: never}}}
			health := cached{fetched: now, samples: []Sample{{Value: up, SampledAt: now, Labels: good}, {Value: 0, SampledAt: now, Labels: never}}}
			ageCache := cached{fetched: now, samples: []Sample{{Value: age, SampledAt: now, Labels: good}, {Value: float64(now.Unix()), SampledAt: now, Labels: never}}}
			s.applyDependencyHealth(&got, health, ageCache, "")
			if len(got.Samples) != 1 || got.Samples[0].Value != 7 || got.Samples[0].Labels["job"] != "mock-platform1" || got.Status == "unavailable" {
				t.Fatalf("source mismatch or healthy receipt erased: %+v", got)
			}
			if historical && got.Status != "stale" {
				t.Fatalf("history looks fresh: %+v", got)
			}
		})
	}
}

func TestAllNeverObservedResourcesAndReceiptsDoNotInventZero(t *testing.T) {
	s, _ := New(Config{})
	now := time.Now().UTC()
	labels := map[string]string{"job": "hub", "instance": "hub-1", "kind": "mysql"}
	s.cache["15m/mysql_connections"] = cached{fetched: now, samples: []Sample{{Value: 0, SampledAt: now, Labels: map[string]string{}}}}
	s.cache["15m/resource_up"] = cached{fetched: now, samples: []Sample{{Value: 0, SampledAt: now, Labels: labels}}}
	s.cache["15m/resource_age"] = cached{fetched: now, samples: []Sample{{Value: float64(now.Unix()), SampledAt: now, Labels: labels}}}
	got := metric(t, s.snapshot("15m"), "mysql_connections")
	if got.Status != "unavailable" || len(got.Samples) != 0 {
		t.Fatalf("never-observed placeholder became zero: %+v", got)
	}
}
