package platform2

import "testing"

// Catches treating no committed offset as consumed, or hiding retention gaps
// behind a negative/zero lag while the consumer is stopped.
func TestConsumerLagOffsetBoundaries(t *testing.T) {
	for _, tt := range []struct {
		name                     string
		start, end, commit, want int64
		bad                      bool
	}{
		{"paused committed", 2, 19, 8, 11, false},
		{"new group uses retained start", 8, 19, -1, 11, false},
		{"caught up", 2, 19, 19, 0, false},
		{"retention gap", 8, 19, 7, 0, true},
		{"beyond end", 8, 19, 20, 0, true},
		{"invalid broker offset", 8, 7, 8, 0, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := offsetLag(tt.start, tt.end, tt.commit)
			if (err != nil) != tt.bad || got != tt.want {
				t.Fatalf("lag=%d err=%v want=%d error=%v", got, err, tt.want, tt.bad)
			}
		})
	}
}
