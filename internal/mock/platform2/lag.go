package platform2

import (
	"context"
	"errors"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

// Lag samples the configured group's committed next offsets for this consumer's
// configured topic only. It reuses the existing bounded Kafka client and runs
// independently of polling/pause. The endpoint exposes only the cached result.
func (c *Consumer) Lag(ctx context.Context) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	admin := kadm.NewClient(c.client)
	commits, err := admin.FetchOffsetsForTopics(ctx, c.group, c.topic)
	if err != nil {
		return 0, err
	}
	starts, err := admin.ListStartOffsets(ctx, c.topic)
	if err != nil {
		return 0, err
	}
	if err = starts.Error(); err != nil {
		return 0, err
	}
	ends, err := admin.ListEndOffsets(ctx, c.topic)
	if err != nil {
		return 0, err
	}
	if err = ends.Error(); err != nil {
		return 0, err
	}
	if len(ends[c.topic]) == 0 {
		return 0, errors.New("platform2 lag topic unavailable")
	}
	var total int64
	for partition, end := range ends[c.topic] {
		start, ok := starts.Lookup(c.topic, partition)
		if !ok {
			return 0, ErrRetentionGap
		}
		commit, ok := commits.Lookup(c.topic, partition)
		if !ok {
			return 0, errors.New("platform2 lag offset unavailable")
		}
		lag, err := offsetLag(start.Offset, end.Offset, commit.At)
		if err != nil {
			return 0, err
		}
		total += lag
	}
	return total, nil
}
func offsetLag(start, end, committed int64) (int64, error) {
	if start < 0 || end < start {
		return 0, ErrRetentionGap
	}
	if committed < 0 {
		committed = start
	}
	if committed < start || committed > end {
		return 0, ErrRetentionGap
	}
	return end - committed, nil
}
