//go:build integration

package integration_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"partner-hub/internal/binding"
	hubmysql "partner-hub/internal/infra/mysql"
	"partner-hub/internal/model"
)

// Simulate loss of confirmation after the actual database transaction commits.
// This is a persistence boundary fixture, not an alternative consumer.
type committedBindingGate struct {
	binding.Repository
	eventID string
	entered chan struct{}
	once    sync.Once
}

func (r *committedBindingGate) Apply(ctx context.Context, event model.BindingEvent) (model.BindingResult, error) {
	result, err := r.Repository.Apply(ctx, event)
	if err == nil && event.EventID == r.eventID {
		r.once.Do(func() { close(r.entered) })
		<-ctx.Done()
		return model.BindingResult{}, ctx.Err()
	}
	return result, err
}

type replayBindingObserver struct {
	binding.Repository
	eventID  string
	observed chan bool
	once     sync.Once
}

func (r *replayBindingObserver) Apply(ctx context.Context, event model.BindingEvent) (model.BindingResult, error) {
	result, err := r.Repository.Apply(ctx, event)
	if err == nil && event.EventID == r.eventID {
		state, loadErr := r.Repository.Load(ctx, event.ClientID)
		r.once.Do(func() { r.observed <- loadErr == nil && len(state.Targets) == 0 && result.Replayed })
	}
	return result, err
}

// A former generation may have committed DB state but lost its Kafka commit.
// Replaying that older BIND must not resurrect a subsequently applied UNBIND.
func TestBindingsShutdownAfterDBCommitDoesNotReviveUnboundUID(t *testing.T) {
	db := bindingDB(t)
	producer, admin, broker := kafkaTestClient(t)
	topic := createBindingTopic(t, admin)
	group := "it-shutdown-" + topic
	client := "it-shutdown-" + model.NewID()
	repo := hubmysql.NewBindings(db)
	first := bindingEvent(client, 1301, "BIND", "PLATFORM_A", "departed-uid")
	first.EventID = model.NewID()
	firstRecord := produceBinding(t, producer, topic, 0, first)
	gate := &committedBindingGate{Repository: repo, eventID: first.EventID, entered: make(chan struct{})}
	_, stop, done := startBindingConsumer(t, broker, group, topic, gate)
	select {
	case <-gate.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("initial DB transaction did not commit")
	}
	last := bindingEvent(client, 1302, "UNBIND", "PLATFORM_A", "departed-uid")
	last.EventID = model.NewID()
	lastRecord := produceBinding(t, producer, topic, 0, last)
	last.Position = model.BindingPosition{Topic: topic, Partition: 0, Offset: lastRecord.Offset}
	if result, err := repo.Apply(context.Background(), last); err != nil || result.Outcome != "APPLIED" {
		t.Fatalf("newer unbind not saved: %+v %v", result, err)
	}
	stop()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("former owner exit: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("former consumer exceeded exit budget")
	}
	if offset := committed(admin, group, topic, 0); offset > firstRecord.Offset {
		t.Fatal("unconfirmed former generation advanced offset")
	}
	observer := &replayBindingObserver{Repository: repo, eventID: first.EventID, observed: make(chan bool, 1)}
	_, _, successor := startBindingConsumer(t, broker, group, topic, observer)
	select {
	case safe := <-observer.observed:
		if !safe {
			t.Fatal("replayed BIND revived old UID or was reapplied")
		}
	case err := <-successor:
		t.Fatalf("successor stopped: %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("successor did not replay former generation")
	}
	await(t, func() bool { return committed(admin, group, topic, 0) == lastRecord.Offset+1 })
	var events, revision int
	if err := db.QueryRow("SELECT COUNT(*) FROM binding_event WHERE client_id=?", client).Scan(&events); err != nil || events != 2 {
		t.Fatalf("event replay changed durable identities: %d %v", events, err)
	}
	if err := db.QueryRow("SELECT revision FROM binding_current WHERE client_id=? AND platform_id='PLATFORM_A'", client).Scan(&revision); err != nil || revision != 2 {
		t.Fatalf("replay modified tombstone revision: %d %v", revision, err)
	}
	t.Log("former generation left unconfirmed offset; successor replay preserved UNBOUND tombstone/revision and advanced the contiguous durable offset")
}
