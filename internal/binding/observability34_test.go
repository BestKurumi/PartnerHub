package binding_test

import (
	"context"
	"errors"
	"partner-hub/internal/binding"
	"partner-hub/internal/model"
	"testing"
)

func TestBindingObservationActualSourceFailureAndCacheHit(t *testing.T) {
	r := binding.NewResolver(lookupRepository{load: func(context.Context, string) (model.BindingSnapshot, error) {
		return model.BindingSnapshot{}, errors.New("private db error")
	}}, &snapshotCache{})
	source := []string{}
	cache := []string{}
	r.ObserveSource = func(s string) { source = append(source, s) }
	r.ObserveCache = func(s string) { cache = append(cache, s) }
	_, err := r.Resolve(context.Background(), "customer")
	if err == nil || len(source) != 1 || source[0] != "error" || len(cache) == 0 {
		t.Fatalf("source/cache observation missing: %v %v %v", source, cache, err)
	}
}
