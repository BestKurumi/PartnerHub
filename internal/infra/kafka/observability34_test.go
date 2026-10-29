package kafka

import (
	"errors"
	"github.com/twmb/franz-go/pkg/kadm"
	"testing"
)

func TestBindingLagRejectsInvalidCommitBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name               string
		commit, start, end int64
		startErr           error
		wantErr            bool
		want               int64
	}{
		{"below retained start", 9, 10, 20, nil, true, 0}, {"past log end", 21, 10, 20, nil, true, 0}, {"no commits uses retained start", -1, 10, 20, nil, false, 10}, {"valid commit", 12, 10, 20, nil, false, 8}, {"missing start", 12, 10, 20, errors.New("unavailable"), true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := kadm.GroupMemberLag{Commit: kadm.Offset{At: tc.commit}, Start: kadm.ListedOffset{Offset: tc.start, Err: tc.startErr}, End: kadm.ListedOffset{Offset: tc.end}, Lag: max(0, tc.end-tc.commit)}
			got, err := validateBindingPartitionLag(p)
			if (err != nil) != tc.wantErr || (!tc.wantErr && got != tc.want) {
				t.Fatalf("invalid boundary observation: lag=%d error=%v", got, err)
			}
		})
	}
}
func TestBindingLagKnownGapIsUnhealthy(t *testing.T) {
	c := &BindingConsumer{}
	c.gap.Store(true)
	if err := c.observationAvailable(); err == nil {
		t.Fatal("known retention gap reported healthy")
	}
}
