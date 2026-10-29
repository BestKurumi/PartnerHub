package runtime

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
)

// Lifecycle is the outer admission boundary. Requests admitted before draining
// keep their original service budget; no new business request enters afterward.
type Lifecycle struct {
	next     http.Handler
	draining atomic.Bool
}

func (l *Lifecycle) BeginDrain() { l.draining.Store(true) }
func (l *Lifecycle) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if l.draining.Load() && r.URL.Path != "/livez" {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"DRAINING","acceptance":"REJECTED"}`))
		return
	}
	l.next.ServeHTTP(w, r)
}

// Shutdown shares one deadline across request drain, worker join, final archive
// flush and client close. A non-cooperating Close cannot hold the process alive.
func (l *Lifecycle) Shutdown(ctx context.Context, server *http.Server, workersDone <-chan struct{}, flush func(context.Context) error, closeClients func() error) error {
	l.BeginDrain()
	finish := make(chan error, 1)
	go func() {
		if err := server.Shutdown(ctx); err != nil {
			finish <- err
			return
		}
		select {
		case <-workersDone:
		case <-ctx.Done():
			finish <- ctx.Err()
			return
		}
		var flushErr error
		if ctx.Err() == nil && flush != nil {
			flushErr = flush(ctx)
		}
		if err := ctx.Err(); err != nil {
			finish <- err
			return
		}
		var closeErr error
		if closeClients != nil {
			closeErr = closeClients()
		}
		finish <- errors.Join(flushErr, closeErr)
	}()
	select {
	case err := <-finish:
		return err
	case <-ctx.Done():
		// Close active sockets without turning its implementation into an unbounded
		// wait. The caller cancels in-flight worker work and exits on this error.
		go server.Close()
		return ctx.Err()
	}
}
