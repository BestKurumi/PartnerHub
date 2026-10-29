package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"partner-hub/internal/binding"
	"partner-hub/internal/model"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

var ErrBindingGap = errors.New("binding consumer retention gap")

type BindingConfig struct {
	Brokers      []string
	Group, Topic string
}
type BindingConsumer struct {
	group      string
	client     *kgo.Client
	service    *binding.Service
	repository binding.Repository
	gap        atomic.Bool
	closing    atomic.Bool
	closeOnce  sync.Once
}

func NewBindingConsumer(config BindingConfig, service *binding.Service, repository binding.Repository) (*BindingConsumer, error) {
	if len(config.Brokers) == 0 || config.Group == "" || config.Topic == "" || service == nil || repository == nil {
		return nil, errors.New("invalid binding consumer configuration")
	}
	c := &BindingConsumer{service: service, repository: repository, group: config.Group}
	var client *kgo.Client
	var err error
	client, err = kgo.NewClient(kgo.SeedBrokers(config.Brokers...), kgo.ConsumerGroup(config.Group), kgo.ConsumeTopics(config.Topic), kgo.DisableAutoCommit(), kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()), kgo.ConsumeResetOffset(kgo.NoResetOffset()), kgo.BlockRebalanceOnPoll(), kgo.FetchMaxBytes(1<<20), kgo.FetchMaxPartitionBytes(1<<20), kgo.FetchMaxWait(100*time.Millisecond), kgo.RebalanceTimeout(20*time.Second), kgo.SessionTimeout(10*time.Second),
		kgo.OnPartitionsAssigned(func(ctx context.Context, cl *kgo.Client, assigned map[string][]int32) {
			cl.ResumeFetchPartitions(assigned)
		}),
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
			for topic, ps := range offsets {
				for partition, offset := range ps {
					at := offset.EpochOffset().Offset
					if at < 0 {
						continue
					}
					start, ok := starts.Lookup(topic, partition)
					if !ok {
						return nil, errors.New("missing binding start offset")
					}
					end, ok := ends.Lookup(topic, partition)
					if !ok {
						return nil, errors.New("missing binding end offset")
					}
					if at < start.Offset || at > end.Offset {
						c.gap.Store(true)
						return nil, ErrBindingGap
					}
				}
			}
			return offsets, nil
		}))
	if err != nil {
		return nil, err
	}
	c.client = client
	return c, nil
}
func (c *BindingConsumer) Run(ctx context.Context) error {
	// A stopped worker must leave its group before reporting that it stopped;
	// otherwise a successor waits for a rebalance/session timeout.
	defer c.Close()
	paused := map[int32]time.Time{}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if c.closing.Load() {
			return kgo.ErrClientClosed
		}
		if c.gap.Load() {
			return ErrBindingGap
		}
		for partition, until := range paused {
			if !time.Now().Before(until) {
				c.client.ResumeFetchPartitions(map[string][]int32{c.topic(): {partition}})
				delete(paused, partition)
			}
		}
		poll, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		fetches := c.client.PollRecords(poll, 1)
		cancel()
		if fetches.IsClientClosed() {
			return kgo.ErrClientClosed
		}
		var fatal error
		fetches.EachError(func(topic string, partition int32, err error) {
			if errors.Is(err, kerr.OffsetOutOfRange) {
				fatal = ErrBindingGap
			} else if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
				slog.Warn("binding fetch unavailable", "partition", partition)
			}
		})
		if fatal != nil || c.gap.Load() {
			c.client.AllowRebalance()
			return ErrBindingGap
		}
		for _, record := range fetches.Records() {
			process, cancel := context.WithTimeout(ctx, time.Second)
			err := c.process(process, record)
			cancel()
			if c.closing.Load() {
				return kgo.ErrClientClosed
			}
			if err == nil {
				commit, cancel := context.WithTimeout(ctx, time.Second)
				err = c.client.CommitRecords(commit, record)
				cancel()
			}
			if err != nil {
				// Rewind only while this poll owns its generation; never retain an
				// old generation's record for later commit. DB replay is harmless.
				c.client.SetOffsets(map[string]map[int32]kgo.EpochOffset{record.Topic: {record.Partition: {Epoch: record.LeaderEpoch, Offset: record.Offset}}})
				c.client.PauseFetchPartitions(map[string][]int32{record.Topic: {record.Partition}})
				paused[record.Partition] = time.Now().Add(200 * time.Millisecond)
				slog.Warn("binding persistence or commit unconfirmed; partition paused", "partition", record.Partition)
			}
		}
		c.client.AllowRebalance()
	}
}
func (c *BindingConsumer) process(ctx context.Context, r *kgo.Record) error {
	position := model.BindingPosition{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset}
	event, err := binding.DecodeEvent(r.Value)
	if err != nil {
		return c.repository.SaveAnomaly(ctx, model.BindingAnomaly{Position: position, Reason: "INVALID_BINDING_EVENT", Payload: r.Value})
	}
	if !bytes.Equal(r.Key, BindingKey(event.ClientID, event.PlatformID)) {
		return c.repository.SaveAnomaly(ctx, model.BindingAnomaly{Position: position, EventID: event.EventID, Reason: "INVALID_BINDING_KEY", Payload: r.Value})
	}
	event.Position = position
	_, err = c.service.Apply(ctx, event)
	return err
}
func (c *BindingConsumer) topic() string { return c.client.GetConsumeTopics()[0] }
func (c *BindingConsumer) Close() {
	c.closing.Store(true)
	c.closeOnce.Do(c.client.CloseAllowingRebalance)
}
func BindingKey(clientID, platformID string) []byte {
	body, err := json.Marshal([2]string{clientID, platformID})
	if err != nil {
		panic(fmt.Sprintf("encode binding key: %v", err))
	}
	return body
}

// CheckReady checks broker/topic availability and known retention gaps without
// requiring partition ownership (an idle member can be healthy).
func (c *BindingConsumer) CheckReady(ctx context.Context) error {
	if c.gap.Load() || c.closing.Load() {
		return errors.New("binding consumer unavailable")
	}
	check, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	topics, err := kadm.NewClient(c.client).ListTopics(check, "customer.binding.events", "platform2.trade.notifications")
	if err != nil {
		return errors.New("Kafka unavailable")
	}
	for _, name := range []string{"customer.binding.events", "platform2.trade.notifications"} {
		topic, ok := topics[name]
		if !ok || topic.Err != nil || len(topic.Partitions) == 0 {
			return errors.New("Kafka topic unavailable")
		}
	}
	return nil
}

// BindingLag observes committed offsets for this fixed group and binding topic.
// It is invoked by the bounded background sampler, never from /metrics.
func (c *BindingConsumer) BindingLag(ctx context.Context) (int64, error) {
	if err := c.observationAvailable(); err != nil {
		return 0, err
	}
	groups, err := kadm.NewClient(c.client).Lag(ctx, c.group)
	if err != nil {
		return 0, err
	}
	group, ok := groups[c.group]
	if !ok {
		return 0, errors.New("binding group observation missing")
	}
	if err = group.Error(); err != nil {
		return 0, err
	}
	partitions, ok := group.Lag[c.topic()]
	if !ok || len(partitions) == 0 {
		return 0, errors.New("binding topic lag unavailable")
	}
	var total int64
	for _, p := range partitions {
		lag, err := validateBindingPartitionLag(p)
		if err != nil {
			return 0, err
		}
		total += lag
	}
	if err := c.observationAvailable(); err != nil {
		return 0, err
	}
	return total, nil
}

func validateBindingPartitionLag(p kadm.GroupMemberLag) (int64, error) {
	if p.Err != nil || p.Start.Err != nil || p.End.Err != nil || p.Start.Offset < 0 || p.End.Offset < p.Start.Offset {
		return 0, errors.New("binding partition offsets unavailable")
	}
	at := p.Commit.At
	if at == -1 {
		at = p.Start.Offset
	} else if at < p.Start.Offset || at > p.End.Offset {
		return 0, ErrBindingGap
	}
	return p.End.Offset - at, nil
}
func (c *BindingConsumer) observationAvailable() error {
	if c.gap.Load() {
		return ErrBindingGap
	}
	if c.closing.Load() {
		return errors.New("binding consumer unavailable")
	}
	return nil
}
