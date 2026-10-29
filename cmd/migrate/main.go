// migrate is a one-shot deployment step. Hub processes never create schemas or
// reset data. Existing users retain their passwords; a mismatch fails migration.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	driver "github.com/go-sql-driver/mysql"
	"io"
	"net"
	"os"
	"os/signal"
	hubmysql "partner-hub/internal/infra/mysql"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if e := run(ctx, os.Args[1:], os.Getenv); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
	fmt.Println("database migrations completed")
}
func run(parent context.Context, args []string, getenv func(string) string) error {
	fs := flag.NewFlagSet("migrate", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dir := fs.String("dir", "deploy/mysql", "migration root directory")
	kafka := fs.Bool("kafka", false, "also create missing native demo Kafka topics")
	if e := fs.Parse(args); e != nil || fs.NArg() != 0 {
		return errors.New("usage: migrate --dir deploy/mysql [-kafka]")
	}
	brokers := getenv("HUB_KAFKA_BROKERS")
	if *kafka && brokers == "" {
		return errors.New("HUB_KAFKA_BROKERS required")
	}
	env := map[string]string{}
	for _, key := range []string{"MYSQL_ADDR", "MYSQL_ROOT_PASSWORD", "MYSQL_PASSWORD", "PLATFORM1_DB_PASSWORD", "PLATFORM2_DB_PASSWORD"} {
		v := getenv(key)
		if v == "" || strings.ContainsRune(v, 0) {
			return errors.New(key + " required")
		}
		env[key] = v
	}
	if _, _, e := net.SplitHostPort(env["MYSQL_ADDR"]); e != nil {
		return errors.New("MYSQL_ADDR must be host:port")
	}
	ctx, cancel := context.WithTimeout(parent, 60*time.Second)
	defer cancel()
	if *kafka {
		if err := bootstrapKafka(ctx, brokers); err != nil {
			return err
		}
	}
	root, e := openDB("root", env["MYSQL_ROOT_PASSWORD"], env["MYSQL_ADDR"], "")
	if e != nil {
		return e
	}
	root.SetMaxOpenConns(1)
	identities := []struct{ schema, user, password, dir string }{{"partner_hub", "hub", env["MYSQL_PASSWORD"], "migrations"}, {"mock_platform1", "mock_p1", env["PLATFORM1_DB_PASSWORD"], "mock-platform1"}, {"mock_platform2", "mock_p2", env["PLATFORM2_DB_PASSWORD"], "mock-platform2"}}
	for _, identity := range identities {
		if _, e = root.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+identity.schema+" CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin"); e != nil {
			root.Close()
			return safeError(ctx, "schema bootstrap failed")
		}
		if _, e = root.ExecContext(ctx, "CREATE USER IF NOT EXISTS '"+identity.user+"'@'%' IDENTIFIED BY ?", identity.password); e != nil {
			root.Close()
			return safeError(ctx, "identity bootstrap failed")
		}
		if _, e = root.ExecContext(ctx, "GRANT ALL PRIVILEGES ON "+identity.schema+".* TO '"+identity.user+"'@'%'"); e != nil {
			root.Close()
			return safeError(ctx, "schema grant failed")
		}
	}
	root.Close()
	for _, identity := range identities {
		db, e := openDB(identity.user, identity.password, env["MYSQL_ADDR"], identity.schema)
		if e != nil {
			return e
		}
		db.SetMaxOpenConns(1)
		e = hubmysql.ApplyMigrations(ctx, db, filepath.Join(*dir, identity.dir))
		db.Close()
		if e != nil {
			return e
		}
	}
	return nil
}

func bootstrapKafka(ctx context.Context, brokers string) error {
	client, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...))
	if err != nil {
		return errors.New("invalid Kafka configuration")
	}
	defer client.Close()
	check, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	retention := "604800000"
	topics := []string{"customer.binding.events", "platform2.trade.notifications"}
	responses, err := kadm.NewClient(client).CreateTopics(check, 3, 1, map[string]*string{"retention.ms": &retention}, topics...)
	if err != nil {
		return errors.New("Kafka topic initialization failed; check broker address and CREATE permission")
	}
	for _, topic := range topics {
		response, exists := responses[topic]
		if !exists || (response.Err != nil && !errors.Is(response.Err, kerr.TopicAlreadyExists)) {
			return errors.New("Kafka topic initialization failed; existing topics and data are preserved")
		}
	}
	return nil
}
func openDB(user, password, addr, schema string) (*sql.DB, error) {
	c := driver.NewConfig()
	c.User = user
	c.Passwd = password
	c.Net = "tcp"
	c.Addr = addr
	c.DBName = schema
	c.ParseTime = true
	// MySQL account DDL does not accept prepared-statement placeholders.
	// The driver safely escapes and interpolates values for this bootstrap-only
	// connection; runtime application pools retain prepared parameters.
	c.InterpolateParams = true
	c.Timeout = time.Second
	c.ReadTimeout = 2 * time.Second
	c.WriteTimeout = 2 * time.Second
	db, e := sql.Open("mysql", c.FormatDSN())
	if e != nil {
		return nil, errors.New("invalid migration database configuration")
	}
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(time.Minute)
	return db, nil
}
func safeError(ctx context.Context, message string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return errors.New(message)
}
