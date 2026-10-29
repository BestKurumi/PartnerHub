package observability_test

import (
	"github.com/prometheus/client_golang/prometheus"
	"partner-hub/internal/observability"
	"testing"
)

func TestResourceConfiguredKafkaClientStarts(t *testing.T) {
	resources, err := observability.NewResources(observability.ResourceConfig{Brokers: []string{"kafka:9092"}}, prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("valid deployment Kafka config prevents startup: %v", err)
	}
	defer resources.Close()
}
