package main

import (
	"database/sql"
	"errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"net/http"
	"os"
	"partner-hub/internal/mock/portal"
	"strings"
)

func withPortal(platform string, db *sql.DB, base http.Handler) (http.Handler, func(), error) {
	token, hub, brokers := os.Getenv("DEMO_TOKEN"), os.Getenv("HUB_TEST_URL"), os.Getenv("MOCK_KAFKA_BROKERS")
	if token == "" || hub == "" || brokers == "" {
		return nil, nil, errors.New("DEMO_TOKEN, HUB_TEST_URL and MOCK_KAFKA_BROKERS required for local portal")
	}
	producer, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.MaxBufferedRecords(32), kgo.MaxBufferedBytes(256<<10))
	if err != nil {
		return nil, nil, err
	}
	p, err := portal.NewHandler(portal.Config{PlatformID: platform, Token: token, HubURL: hub}, db, producer)
	if err != nil {
		producer.Close()
		return nil, nil, err
	}
	inner := http.NewServeMux()
	inner.Handle("/", p)
	if platform == "PLATFORM_A" {
		inner.Handle("/notifications/", base)
	} else {
		inner.Handle("/demo/consumer", base)
	}
	prefix := "/mock/p1/"
	if platform == "PLATFORM_B" {
		prefix = "/mock/p2/"
	}
	outer := http.NewServeMux()
	outer.Handle(prefix, http.StripPrefix(strings.TrimSuffix(prefix, "/"), inner))
	outer.Handle("/", inner)
	return outer, producer.Close, nil
}
