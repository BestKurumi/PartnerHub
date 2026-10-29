package delivery

import (
	"math/rand/v2"
	"time"
)

const MaxCycleAttempts = 5

// RetryDelay schedules a failed attempt within its exponential jitter window.
func RetryDelay(cycleAttempt int) time.Duration {
	cap := time.Second
	// Clamp before shifting to avoid overflow, including corrupt/large counts.
	if cycleAttempt >= 6 {
		cap = 30 * time.Second
	} else if cycleAttempt > 1 {
		cap <<= cycleAttempt - 1
	}
	// Database timestamps have millisecond precision; microsecond input keeps
	// both jitter endpoints bounded without a floating-point conversion.
	half := cap.Microseconds() / 2
	return time.Duration(half+rand.Int64N(half+1)) * time.Microsecond
}
