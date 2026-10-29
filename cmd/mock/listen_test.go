package main

import (
	"strings"
	"testing"
)

func TestMockContainerListenRequiresExplicitOptIn(t *testing.T) {
	for _, service := range []string{"platform1", "platform2", "upstream"} {
		for _, tc := range []struct {
			allow, address string
			rejected       bool
		}{
			{"", ":18081", true},
			{"false", "0.0.0.0:18081", true},
			{"true", ":18081", false},
			{"true", "0.0.0.0:18081", false},
			{"true", "[::]:18081", false},
			{"true", "192.0.2.1:18081", true},
			{"true", "localhost:18081", true},
			{"true", "127.0.0.1:18081", false},
			{"", "127.0.0.1:18081", false},
		} {
			t.Run(service+"/"+tc.allow+"/"+tc.address, func(t *testing.T) {
				t.Setenv("DEMO_ALLOW_CONTAINER_LISTEN", tc.allow)
				t.Setenv("PLATFORM1_DSN", "")
				t.Setenv("PLATFORM2_DSN", "")
				t.Setenv("DEMO_TOKEN", "")
				err := run([]string{service, "-listen", tc.address})
				if err == nil {
					t.Fatal("missing dependencies must reject startup")
				}
				rejected := strings.Contains(err.Error(), "listen")
				if rejected != tc.rejected {
					t.Fatalf("listen rejection=%v want %v: %v", rejected, tc.rejected, err)
				}
			})
		}
	}
}
