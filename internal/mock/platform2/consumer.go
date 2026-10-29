package platform2

import (
	"context"
	"errors"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

type ConsumerConfig struct {
	Brokers      []string
	Group, Topic string
	Paused       bool
}

var ErrRetentionGap = errors.New("platform2 consumer retention gap")

type Consumer struct {
	client  *kgo.Client
	store   Store
	topic   string
	group   string
	work    sync.Mutex
	paused  atomic.Bool
	closing atomic.Bool
	gap     atomic.Bool
	once    sync.Once
}

func NewConsumer(config ConsumerConfig, store Store) (*Consumer, error) {
	if len(config.Brokers) == 0 || config.Group == "" || config.Topic == "" || store == nil {
		return nil, errors.New("invalid platform2 consumer configuration")
	}
	c := &Consumer{store: store, topic: config.Topic, group: config.Group}
	c.paused.Store(config.Paused)
	var client *kgo.Client
	client, err := kgo.NewClient(kgo.SeedBrokers(config.Brokers...), kgo.ConsumerGroup(config.Group), kgo.ConsumeTopics(config.Topic), kgo.DisableAutoCommit(), kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()), kgo.ConsumeResetOffset(kgo.NoResetOffset()), kgo.BlockRebalanceOnPoll(), kgo.FetchMaxBytes(1<<20), kgo.FetchMaxPartitionBytes(1<<20), kgo.FetchMaxWait(100*time.Millisecond), kgo.RebalanceTimeout(20*time.Second), kgo.SessionTimeout(10*time.Second),
		kgo.AdjustFetchOffsetsFn(func(ctx context.Context, offsets map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
			check, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			admin := kadm.NewClient(client)
			starts, err := admin.ListStartOffsets(check, config.Topic)
			if err != nil {
				return nil, err
			}
			if err = starts.Error(); err != nil {
				return nil, err
			}
			ends, err := admin.ListEndOffsets(check, config.Topic)
			if err != nil {
				return nil, err
			}
			if err = ends.Error(); err != nil {
				return nil, err
			}
			for topic, partitions := range offsets {
				for partition, offset := range partitions {
					at := offset.EpochOffset().Offset
					if at < 0 {
						continue
					}
					start, ok := starts.Lookup(topic, partition)
					if !ok {
						return nil, ErrRetentionGap
					}
					end, ok := ends.Lookup(topic, partition)
					if !ok {
						return nil, ErrRetentionGap
					}
					if at < start.Offset || at > end.Offset {
						c.gap.Store(true)
						return nil, ErrRetentionGap
					}
				}
			}
			return offsets, nil
		}))
	if err != nil {
		return nil, err
	}
	c.client = client
	if config.Paused {
		client.PauseFetchTopics(config.Topic)
	}
	return c, nil
}
func (c *Consumer) Run(ctx context.Context) error {
	defer c.Close()
	pausedPartitions := map[int32]time.Time{}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if c.closing.Load() {
			return kgo.ErrClientClosed
		}
		if c.gap.Load() {
			return ErrRetentionGap
		}
		for p, until := range pausedPartitions {
			if !time.Now().Before(until) && !c.paused.Load() {
				c.client.ResumeFetchPartitions(map[string][]int32{c.topic: {p}})
				delete(pausedPartitions, p)
			}
		}
		poll, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		fetches := c.client.PollRecords(poll, 1)
		cancel()
		if fetches.IsClientClosed() {
			return kgo.ErrClientClosed
		}
		var fatal error
		fetches.EachError(func(_ string, _ int32, err error) {
			if errors.Is(err, kerr.OffsetOutOfRange) {
				fatal = ErrRetentionGap
			} else if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
				slog.Warn("platform2 fetch unavailable")
			}
		})
		if fatal != nil || c.gap.Load() {
			c.client.AllowRebalance()
			return ErrRetentionGap
		}
		for _, record := range fetches.Records() {
			c.work.Lock()
			if c.closing.Load() {
				c.work.Unlock()
				return kgo.ErrClientClosed
			}
			if c.paused.Load() {
				c.client.SetOffsets(map[string]map[int32]kgo.EpochOffset{record.Topic: {record.Partition: {Epoch: record.LeaderEpoch, Offset: record.Offset}}})
				c.work.Unlock()
				continue
			}
			save, cancel := context.WithTimeout(ctx, time.Second)
			receipt, decodeErr := DecodeRecord(record)
			var err error
			if decodeErr == nil {
				err = c.store.Save(save, receipt)
			} else {
				err = c.store.SaveInvalid(save, InvalidRecord{record.Topic, record.Partition, record.Offset, record.Value})
			}
			cancel()
			if err == nil && !c.closing.Load() {
				commit, cancel := context.WithTimeout(ctx, time.Second)
				err = c.client.CommitRecords(commit, record)
				cancel()
			}
			if err != nil && !c.closing.Load() {
				c.client.SetOffsets(map[string]map[int32]kgo.EpochOffset{record.Topic: {record.Partition: {Epoch: record.LeaderEpoch, Offset: record.Offset}}})
				c.client.PauseFetchPartitions(map[string][]int32{record.Topic: {record.Partition}})
				pausedPartitions[record.Partition] = time.Now().Add(200 * time.Millisecond)
				slog.Warn("platform2 persistence/commit unconfirmed; partition paused")
			}
			c.work.Unlock()
		}
		c.client.AllowRebalance()
	}
}
func (c *Consumer) Close() { c.closing.Store(true); c.once.Do(c.client.CloseAllowingRebalance) }

// Return only after any current Save/commit has settled. A successful pause is
// therefore an observable boundary after which receipt counts remain unchanged.
func (c *Consumer) SetPaused(paused bool) {
	c.work.Lock()
	defer c.work.Unlock()
	c.paused.Store(paused)
	if paused {
		c.client.PauseFetchTopics(c.topic)
	} else {
		c.client.ResumeFetchTopics(c.topic)
	}
}
func (c *Consumer) Paused() bool { return c.paused.Load() }
