// Package fmt provides the `spirit fmt` subcommand, which canonicalizes
// CREATE TABLE .sql files by round-tripping them through a real MySQL server.
//
// MySQL normalizes many SQL constructs internally. For example:
//   - BOOLEAN becomes TINYINT(1)
//   - SERIAL becomes BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE
//   - NVARCHAR becomes VARCHAR with utf8mb3 charset
//
// These normalizations cause spurious diffs when comparing schema files against
// a live database. `spirit fmt` solves this by applying each .sql file to a
// local MySQL server, reading back the canonical form via SHOW CREATE TABLE,
// and updating the file if it differs.
//
// Modelled on `go fmt`: if changes were made, the filename is printed to stdout.
// If no changes were needed, nothing is printed.
//
// The connection is opened with dbconn, so it uses the same session settings
// as every other spirit connection, with STRICT_TRANS_TABLES and
// NO_ENGINE_SUBSTITUTION added to spirit's sql_mode (NO_AUTO_VALUE_ON_ZERO).
// Without them MySQL accepts some invalid definitions by changing them: an
// index that is too long becomes a prefix index, a VARCHAR that is too long
// becomes a TEXT type, and an unknown engine becomes InnoDB. fmt would then
// write a different table into the file. The zero-date checks (NO_ZERO_DATE,
// NO_ZERO_IN_DATE) stay off, as in spirit, so DEFAULT '0000-00-00' is
// accepted. TLS is PREFERRED: it is used when the server supports it.
package fmt

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/parser/format"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/utils"
)

// FmtCmd is the Kong CLI struct for the fmt command.
// It canonicalizes CREATE TABLE .sql files by round-tripping them through MySQL,
// under formatSQLMode.
type FmtCmd struct {
	// Files is the list of .sql files (or directories) to format.
	Files []string `arg:"" help:"SQL files or directories to format. Statements run in strict mode with NO_ENGINE_SUBSTITUTION, so a definition MySQL would change is an error, but zero dates such as DEFAULT '0000-00-00' are accepted." type:"path"`

	// MySQL connection options
	Host     string `name:"host" help:"MySQL server host:port. TLS is used when the server supports it" default:"127.0.0.1:3306" env:"MYSQL_SERVER"`
	Username string `name:"username" help:"MySQL username" default:"root" env:"MYSQL_USER"`
	Password string `name:"password" help:"MySQL password" default:"" env:"MYSQL_PASSWORD"`
	Database string `name:"database" help:"Temporary database to use for formatting" default:"spirit_fmt" env:"MYSQL_DATABASE"`
}

// formatSQLMode is added to spirit's session sql_mode (NO_AUTO_VALUE_ON_ZERO)
// for the formatting session. Without these modes MySQL accepts some invalid
// definitions by changing them (a too-long index becomes a prefix index, a
// too-long VARCHAR a TEXT type, an unknown engine InnoDB), and fmt would write
// the changed table back into the file.
const formatSQLMode = "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"

// session is the connection the formatting statements run on: a *sql.Conn
// from formatSession.
type session interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// formatSession pins one connection of db and adds formatSQLMode to its
// sql_mode. The connection is pinned because a SET SESSION on the pool would
// be lost when the pool replaces the connection.
func formatSession(ctx context.Context, db *sql.DB) (*sql.Conn, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, "SET SESSION sql_mode = CONCAT(@@SESSION.sql_mode, ',', ?)", formatSQLMode); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to set sql_mode: %w", err)
	}
	return conn, nil
}

// Run executes the fmt command. It is called by Kong.
func (cmd *FmtCmd) Run() error {
	ctx := context.Background()

	// Collect all .sql files from the arguments.
	files, err := collectSQLFiles(cmd.Files)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("no .sql files found")
	}

	// Connect to MySQL without a database first so we can create the
	// working database if it doesn't exist yet.
	dbConfig := dbconn.NewDBConfig()
	dbConfig.MaxOpenConnections = 1
	bootstrapDB, err := dbconn.NewContext(ctx, buildDSN(cmd.Username, cmd.Password, cmd.Host, ""), dbConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to MySQL at %s: %w", cmd.Host, err)
	}
	defer utils.CloseAndLog(bootstrapDB)

	// Ensure the working database exists.
	if _, err := bootstrapDB.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", sqlescape.EscapeIdentifier(cmd.Database))); err != nil {
		return fmt.Errorf("failed to create database %s: %w", cmd.Database, err)
	}

	// Now connect to the working database.
	db, err := dbconn.NewContext(ctx, buildDSN(cmd.Username, cmd.Password, cmd.Host, cmd.Database), dbConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to MySQL at %s/%s: %w", cmd.Host, cmd.Database, err)
	}
	defer utils.CloseAndLog(db)
	conn, err := formatSession(ctx, db)
	if err != nil {
		return err
	}
	defer utils.CloseAndLog(conn)

	// Process each file.
	for _, path := range files {
		changed, err := formatFile(ctx, conn, path)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		if changed {
			fmt.Println(path)
		}
	}
	return nil
}

// formatFile canonicalizes a single .sql file. It returns true if the file was
// modified.
func formatFile(ctx context.Context, db session, path string) (bool, error) {
	original, err := os.ReadFile(path)
	if err != nil {
		return false, fmt.Errorf("failed to read file: %w", err)
	}

	canonical, err := canonicalize(ctx, db, string(original))
	if err != nil {
		return false, err
	}

	if canonical == string(original) {
		return false, nil
	}

	if err := os.WriteFile(path, []byte(canonical), 0o644); err != nil {
		return false, fmt.Errorf("failed to write file: %w", err)
	}
	return true, nil
}

// parseAndPrepare parses the SQL, validates it is exactly one CREATE TABLE
// statement, and returns the table name, SQL to execute, and whether MySQL's
// instance-specific AUTO_INCREMENT counter must be reset after creation. If the
// statement includes a database qualifier (e.g., CREATE TABLE mydb.mytable),
// it is stripped so the table is created in the connected working database.
// This also prevents SQL injection since only parsed CREATE TABLE statements
// are accepted.
func parseAndPrepare(createSQL string) (tableName string, execSQL string, resetAutoIncrement bool, err error) {
	ct, err := statement.ParseCreateTable(createSQL)
	if err != nil {
		return "", "", false, fmt.Errorf("not a valid CREATE TABLE statement: %w", err)
	}

	tableName = ct.TableName
	for _, opt := range ct.Raw.Options {
		if opt.Tp == ast.TableOptionAutoIncrement {
			resetAutoIncrement = true
			break
		}
	}

	// Restore only when stripping a schema qualifier. In the common case the
	// exact input is sent to MySQL, including every literal and table option.
	if ct.Raw.Table.Schema.L != "" {
		ct.Raw.Table.Schema = ast.CIStr{}
		var sb strings.Builder
		rCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
		if err := ct.Raw.Restore(rCtx); err != nil {
			return "", "", false, fmt.Errorf("failed to restore CREATE TABLE: %w", err)
		}
		execSQL = sb.String()
	} else {
		execSQL = createSQL
	}

	return tableName, execSQL, resetAutoIncrement, nil
}

// canonicalize takes a CREATE TABLE statement, applies it to MySQL, reads it
// back via SHOW CREATE TABLE, and returns the canonical form with a trailing
// newline. The AUTO_INCREMENT table option is stripped because it is
// instance-specific and not meaningful in schema files.
//
// The SQL is first parsed to validate it is a single CREATE TABLE statement.
// Any database qualifier is stripped. This prevents SQL injection since only
// valid CREATE TABLE statements are executed against the MySQL server.
func canonicalize(ctx context.Context, db session, createStmt string) (string, error) {
	tableName, execSQL, resetAutoIncrement, err := parseAndPrepare(createStmt)
	if err != nil {
		return "", err
	}

	// Drop any existing table with this name in our temporary database.
	if _, err := db.ExecContext(ctx, sqlescape.MustEscapeSQL("DROP TABLE IF EXISTS %n", tableName)); err != nil {
		return "", fmt.Errorf("failed to drop existing table: %w", err)
	}

	// Apply the CREATE TABLE. Since we've validated it's a pure CREATE TABLE
	// (with any schema qualifier stripped), it creates the table in the
	// connected database (our temporary working database).
	if _, err := db.ExecContext(ctx, execSQL); err != nil {
		return "", fmt.Errorf("failed to create table: %w", err)
	}
	// Keep all table options and literals intact while applying the input. An
	// empty table reports no instance-specific counter after resetting it to
	// the initial value, so SHOW CREATE produces the desired canonical schema.
	if resetAutoIncrement {
		if _, err := db.ExecContext(ctx, sqlescape.MustEscapeSQL("ALTER TABLE %n AUTO_INCREMENT = 1", tableName)); err != nil {
			return "", fmt.Errorf("failed to reset AUTO_INCREMENT counter: %w", err)
		}
	}

	// Read back the canonical form via SHOW CREATE TABLE.
	var tbl, canonical string
	if err := db.QueryRowContext(ctx, sqlescape.MustEscapeSQL("SHOW CREATE TABLE %n", tableName)).Scan(&tbl, &canonical); err != nil {
		return "", fmt.Errorf("failed to read back table: %w", err)
	}

	// Drop the table now that we have the canonical form.
	if _, err := db.ExecContext(ctx, sqlescape.MustEscapeSQL("DROP TABLE IF EXISTS %n", tableName)); err != nil {
		return "", fmt.Errorf("failed to drop table: %w", err)
	}

	// Ensure a trailing newline.
	canonical = strings.TrimRight(canonical, "\n") + "\n"
	return canonical, nil
}

// collectSQLFiles expands the given paths into a list of .sql files.
// Paths can be individual files or directories (non-recursive).
// Non-.sql files are silently skipped so that shell globs like
// `spirit fmt *` work without erroring on metadata files.
func collectSQLFiles(paths []string) ([]string, error) {
	var files []string
	for _, p := range paths {
		info, err := os.Stat(p)
		if err != nil {
			return nil, fmt.Errorf("cannot access %s: %w", p, err)
		}
		if info.IsDir() {
			entries, err := os.ReadDir(p)
			if err != nil {
				return nil, fmt.Errorf("cannot read directory %s: %w", p, err)
			}
			for _, entry := range entries {
				if entry.IsDir() {
					continue
				}
				if strings.HasSuffix(strings.ToLower(entry.Name()), ".sql") {
					files = append(files, filepath.Join(p, entry.Name()))
				}
			}
		} else if strings.HasSuffix(strings.ToLower(p), ".sql") {
			files = append(files, p)
		}
	}
	return files, nil
}

// buildDSN constructs a MySQL DSN from the given parameters. It formats the
// DSN with the driver rather than by concatenation, which escapes the
// database name: a '?' or '%' in it would otherwise be read as the start of
// the driver parameters or as an escape.
func buildDSN(username, password, host, database string) string {
	cfg := mysql.NewConfig()
	cfg.User = username
	cfg.Passwd = password
	cfg.Net = "tcp"
	cfg.Addr = host
	cfg.DBName = database
	return cfg.FormatDSN()
}
