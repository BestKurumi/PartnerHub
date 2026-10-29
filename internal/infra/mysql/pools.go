package mysql

import (
	"database/sql"
	"errors"
	driver "github.com/go-sql-driver/mysql"
	"time"
)

// Pools separates five bounded classes while retaining reusable connections.
// Two hub instances use 48 connections; deployment reserves at least 80 in total.
type Pools struct{ Bindings, Handoff, Archive, Recovery, Query *sql.DB }

func NewPools(dsn string) (*Pools, error) {
	c, e := driver.ParseDSN(dsn)
	if e != nil || c.User != "hub" || c.DBName != "partner_hub" || c.MultiStatements || len(c.Params) > 0 {
		return nil, errors.New("invalid independent hub database configuration")
	}
	c.ParseTime = true
	c.Timeout = time.Second
	c.ReadTimeout = 2 * time.Second
	c.WriteTimeout = 2 * time.Second
	p := &Pools{}
	for _, entry := range []struct {
		dst **sql.DB
		max int
	}{{&p.Bindings, 4}, {&p.Handoff, 6}, {&p.Archive, 3}, {&p.Recovery, 7}, {&p.Query, 4}} {
		db, e := sql.Open("mysql", c.FormatDSN())
		if e != nil {
			_ = p.Close()
			return nil, errors.New("cannot create hub database pools")
		}
		db.SetMaxOpenConns(entry.max)
		db.SetMaxIdleConns(entry.max)
		db.SetConnMaxLifetime(5 * time.Minute)
		db.SetConnMaxIdleTime(time.Minute)
		*entry.dst = db
	}
	return p, nil
}
func (p *Pools) Close() error {
	if p == nil {
		return nil
	}
	var errs []error
	for _, db := range []*sql.DB{p.Bindings, p.Handoff, p.Archive, p.Recovery, p.Query} {
		if db != nil {
			errs = append(errs, db.Close())
		}
	}
	return errors.Join(errs...)
}
