package mysql

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

func TestPoolIsolationAndBudgets(t *testing.T) {
	p, e := NewPools("hub:password@tcp(127.0.0.1:1)/partner_hub")
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	seen := map[*sql.DB]bool{}
	total := 0
	for _, entry := range []struct {
		db  *sql.DB
		max int
	}{{p.Bindings, 4}, {p.Handoff, 6}, {p.Archive, 3}, {p.Recovery, 7}, {p.Query, 4}} {
		if entry.db == nil || seen[entry.db] {
			t.Fatal("logical pools share sql.DB")
		}
		seen[entry.db] = true
		if n := entry.db.Stats().MaxOpenConnections; n != entry.max {
			t.Fatalf("pool budget=%d want=%d", n, entry.max)
		}
		total += entry.db.Stats().MaxOpenConnections
	}
	if total != 24 {
		t.Fatalf("total connections=%d", total)
	}
	if e = p.Close(); e != nil {
		t.Fatal(e)
	}
	for db := range seen {
		if e = db.PingContext(context.Background()); e == nil || !strings.Contains(e.Error(), "closed") {
			t.Fatalf("pool not closed: %v", e)
		}
	}
}
func TestPoolsRejectUnsafeDSNWithoutSecret(t *testing.T) {
	for _, dsn := range []string{"root:secret@tcp(localhost)/partner_hub", "hub:secret@tcp(localhost)/other", "hub:secret@tcp(localhost)/partner_hub?multiStatements=true", "hub:secret@tcp(localhost)/partner_hub?unknown=secret"} {
		p, e := NewPools(dsn)
		if e == nil {
			p.Close()
			t.Fatal("unsafe DSN accepted")
		}
		if strings.Contains(e.Error(), "secret") {
			t.Fatal("error leaked password")
		}
	}
}
