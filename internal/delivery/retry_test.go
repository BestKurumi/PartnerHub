package delivery

import (
	"testing"
	"time"
)

func TestRetryExponentialJitter(t *testing.T) {
	for _, tc := range []struct {
		attempt   int
		low, high time.Duration
	}{
		{1, 500 * time.Millisecond, time.Second},
		{2, time.Second, 2 * time.Second},
		{3, 2 * time.Second, 4 * time.Second},
		{4, 4 * time.Second, 8 * time.Second},
		{5, 8 * time.Second, 16 * time.Second},
		{6, 15 * time.Second, 30 * time.Second},
		{1000000, 15 * time.Second, 30 * time.Second},
		{0, 500 * time.Millisecond, time.Second},
		{-1, 500 * time.Millisecond, time.Second},
	} {
		var first time.Duration
		var varied bool
		for i := 0; i < 128; i++ {
			delay := RetryDelay(tc.attempt)
			if delay < tc.low || delay > tc.high {
				t.Fatalf("attempt %d: delay %v outside [%v,%v]", tc.attempt, delay, tc.low, tc.high)
			}
			if i == 0 {
				first = delay
			} else if delay != first {
				varied = true
			}
		}
		if !varied {
			t.Errorf("attempt %d: jitter never varies", tc.attempt)
		}
	}
}
