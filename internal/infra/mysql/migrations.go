package mysql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	sqldriver "database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

const migrationLock = "CONCAT('ph-migrate:',LEFT(SHA2(DATABASE(),256),48))"

type migration struct {
	name, checksum string
	statements     []string
}

// ApplyMigrations holds a schema-specific MySQL advisory lock on the same
// connection as every DDL and version write. MySQL DDL is not transactional:
// failed files stay unrecorded and must themselves be additive/rerunnable.
func ApplyMigrations(parent context.Context, db *sql.DB, dir string) error {
	ctx, cancel := context.WithTimeout(parent, 60*time.Second)
	defer cancel()
	if db == nil {
		return errors.New("migration database required")
	}
	files, e := readMigrations(dir)
	if e != nil {
		return e
	}
	conn, e := db.Conn(ctx)
	if e != nil {
		return migrationError(ctx, "connect", e)
	}
	defer conn.Close()
	for {
		var acquired sql.NullInt64
		e = conn.QueryRowContext(ctx, "SELECT GET_LOCK("+migrationLock+",0)").Scan(&acquired)
		if e != nil {
			return migrationError(ctx, "acquire migration lock", e)
		}
		if acquired.Valid && acquired.Int64 == 1 {
			break
		}
		if !acquired.Valid {
			return errors.New("migration lock unavailable")
		}
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	defer func() {
		cleanup, c := context.WithTimeout(context.Background(), time.Second)
		defer c()
		var released sql.NullInt64
		if e := conn.QueryRowContext(cleanup, "SELECT RELEASE_LOCK("+migrationLock+")").Scan(&released); e != nil || !released.Valid || released.Int64 != 1 {
			_ = conn.Raw(func(any) error { return sqldriver.ErrBadConn })
		}
	}()
	if _, e = conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS hub_schema_migration (version VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY, checksum CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL, applied_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)) ENGINE=InnoDB`); e != nil {
		return migrationError(ctx, "prepare version table", e)
	}
	applied := map[string]string{}
	rows, e := conn.QueryContext(ctx, "SELECT version,checksum FROM hub_schema_migration")
	if e != nil {
		return migrationError(ctx, "read versions", e)
	}
	for rows.Next() {
		var name, sum string
		if e = rows.Scan(&name, &sum); e != nil {
			rows.Close()
			return migrationError(ctx, "read version", e)
		}
		applied[name] = sum
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return migrationError(ctx, "read versions", e)
	}
	expected := map[string]bool{}
	for _, file := range files {
		expected[file.name] = true
		if checksum, ok := applied[file.name]; ok && checksum != file.checksum {
			return fmt.Errorf("migration checksum mismatch: %s", file.name)
		}
	}
	for name := range applied {
		if !expected[name] {
			return errors.New("applied migration missing from manifest directory")
		}
	}
	for _, file := range files {
		if _, ok := applied[file.name]; ok {
			continue
		}
		for _, statement := range file.statements {
			if _, e = conn.ExecContext(ctx, statement); e != nil {
				return migrationError(ctx, "execute "+file.name, e)
			}
		}
		if _, e = conn.ExecContext(ctx, "INSERT INTO hub_schema_migration(version,checksum) VALUES (?,?)", file.name, file.checksum); e != nil {
			return migrationError(ctx, "record "+file.name, e)
		}
	}
	return nil
}
func migrationError(ctx context.Context, action string, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return errors.New("migration " + action + " failed")
}
func readMigrations(dir string) ([]migration, error) {
	entries, e := os.ReadDir(dir)
	if e != nil {
		return nil, errors.New("cannot read migration directory")
	}
	valid := regexp.MustCompile(`^[0-9]{3}_[a-z0-9_]+\.sql$`)
	var files []migration
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		if !valid.MatchString(entry.Name()) || !entry.Type().IsRegular() {
			return nil, errors.New("invalid migration file")
		}
		info, e := entry.Info()
		if e != nil || info.Size() > 1<<20 {
			return nil, errors.New("migration file exceeds 1MiB")
		}
		raw, e := os.ReadFile(filepath.Join(dir, entry.Name()))
		if e != nil {
			return nil, errors.New("cannot read migration file")
		}
		statements, e := splitMigrationSQL(string(raw))
		if e != nil {
			return nil, e
		}
		sum := sha256.Sum256(raw)
		files = append(files, migration{entry.Name(), hex.EncodeToString(sum[:]), statements})
	}
	if len(files) == 0 {
		return nil, errors.New("migration directory contains no SQL files")
	}
	sort.Slice(files, func(i, j int) bool { return files[i].name < files[j].name })
	return files, nil
}

// The checked-in additive migrations contain prepared statements, quoted
// semicolons and comments. Split only terminators outside those constructs;
// reject unfinished quotes/comments before executing any DDL.
func splitMigrationSQL(raw string) ([]string, error) {
	var out []string
	var b strings.Builder
	var quote byte
	line, block := false, false
	for i := 0; i < len(raw); i++ {
		c := raw[i]
		if line {
			if c == '\n' {
				line = false
				b.WriteByte('\n')
			}
			continue
		}
		if block {
			if c == '*' && i+1 < len(raw) && raw[i+1] == '/' {
				block = false
				i++
				b.WriteByte(' ')
			}
			continue
		}
		if quote != 0 {
			b.WriteByte(c)
			if c == '\\' && i+1 < len(raw) {
				i++
				b.WriteByte(raw[i])
				continue
			}
			if c == quote {
				if i+1 < len(raw) && raw[i+1] == quote {
					i++
					b.WriteByte(raw[i])
				} else {
					quote = 0
				}
			}
			continue
		}
		if c == '\'' || c == '"' || c == '`' {
			quote = c
			b.WriteByte(c)
			continue
		}
		if c == '#' || c == '-' && i+2 < len(raw) && raw[i+1] == '-' && (raw[i+2] == ' ' || raw[i+2] == '\t' || raw[i+2] == '\n') {
			line = true
			continue
		}
		if c == '/' && i+1 < len(raw) && raw[i+1] == '*' {
			block = true
			i++
			continue
		}
		if c == ';' {
			if s := strings.TrimSpace(b.String()); s != "" {
				out = append(out, s)
			}
			b.Reset()
			continue
		}
		b.WriteByte(c)
	}
	if quote != 0 || block {
		return nil, errors.New("unterminated migration SQL")
	}
	if s := strings.TrimSpace(b.String()); s != "" {
		out = append(out, s)
	}
	if len(out) == 0 {
		return nil, errors.New("empty migration SQL")
	}
	return out, nil
}
