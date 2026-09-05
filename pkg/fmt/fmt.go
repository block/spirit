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
package fmt

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/parser/format"
	_ "github.com/go-sql-driver/mysql"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/utils"
)

// FmtCmd is the Kong CLI struct for the fmt command.
// It canonicalizes CREATE TABLE .sql files by round-tripping them through MySQL.
type FmtCmd struct {
	// Files is the list of .sql files (or directories) to format.
	Files []string `arg:"" help:"SQL files or directories to format." type:"path"`

	// MySQL connection options
	Host     string `name:"host" help:"MySQL server host:port" default:"127.0.0.1:3306" env:"MYSQL_SERVER"`
	Username string `name:"username" help:"MySQL username" default:"root" env:"MYSQL_USER"`
	Password string `name:"password" help:"MySQL password" default:"" env:"MYSQL_PASSWORD"`
	Database string `name:"database" help:"Temporary database to use for formatting" default:"spirit_fmt" env:"MYSQL_DATABASE"`
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
	bootstrapDSN := buildDSN(cmd.Username, cmd.Password, cmd.Host, "")
	bootstrapDB, err := sql.Open("mysql", bootstrapDSN)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer utils.CloseAndLog(bootstrapDB)
	if err := bootstrapDB.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to connect to MySQL at %s: %w", cmd.Host, err)
	}

	// Ensure the working database exists.
	if _, err := bootstrapDB.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", sqlescape.EscapeIdentifier(cmd.Database))); err != nil {
		return fmt.Errorf("failed to create database %s: %w", cmd.Database, err)
	}

	// Now connect to the working database.
	dsn := buildDSN(cmd.Username, cmd.Password, cmd.Host, cmd.Database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer utils.CloseAndLog(db)
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to connect to MySQL at %s/%s: %w", cmd.Host, cmd.Database, err)
	}

	// Process each file.
	for _, path := range files {
		changed, err := formatFile(ctx, db, path)
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
func formatFile(ctx context.Context, db *sql.DB, path string) (bool, error) {
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
// statement, and returns the table name and the SQL to execute. If the
// statement includes a database qualifier (e.g., CREATE TABLE mydb.mytable),
// it is stripped so the table is created in the connected working database.
// This also prevents SQL injection since only parsed CREATE TABLE statements
// are accepted.
func parseAndPrepare(createSQL string) (tableName string, execSQL string, err error) {
	ct, err := statement.ParseCreateTable(createSQL)
	if err != nil {
		return "", "", fmt.Errorf("not a valid CREATE TABLE statement: %w", err)
	}

	tableName = ct.TableName

	// Remove only the instance-specific table counter. Matching SQL text also
	// matches defaults, comments and quoted identifiers, changing the schema.
	optionCount := len(ct.Raw.Options)
	ct.Raw.Options = slices.DeleteFunc(ct.Raw.Options, func(opt *ast.TableOption) bool {
		return opt.Tp == ast.TableOptionAutoIncrement
	})

	// Restore only when stripping a schema qualifier or table counter;
	// otherwise preserve the original SQL for MySQL to canonicalize.
	if ct.Raw.Table.Schema.L != "" || len(ct.Raw.Options) != optionCount {
		ct.Raw.Table.Schema = ast.CIStr{}
		var sb strings.Builder
		rCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
		if err := ct.Raw.Restore(rCtx); err != nil {
			return "", "", fmt.Errorf("failed to restore CREATE TABLE: %w", err)
		}
		execSQL = sb.String()
	} else {
		execSQL = createSQL
	}

	return tableName, execSQL, nil
}

// canonicalize takes a CREATE TABLE statement, applies it to MySQL, reads it
// back via SHOW CREATE TABLE, and returns the canonical form with a trailing
// newline. The AUTO_INCREMENT table option is stripped because it is
// instance-specific and not meaningful in schema files.
//
// The SQL is first parsed to validate it is a single CREATE TABLE statement.
// Any database qualifier is stripped. This prevents SQL injection since only
// valid CREATE TABLE statements are executed against the MySQL server.
func canonicalize(ctx context.Context, db *sql.DB, createStmt string) (string, error) {
	tableName, execSQL, err := parseAndPrepare(createStmt)
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

	// Read back the canonical form via SHOW CREATE TABLE.
	var tbl, canonical string
	if err := db.QueryRowContext(ctx, sqlescape.MustEscapeSQL("SHOW CREATE TABLE %n", tableName)).Scan(&tbl, &canonical); err != nil {
		return "", fmt.Errorf("failed to read back table: %w", err)
	}

	// Drop the table now that we have the canonical form.
	if _, err := db.ExecContext(ctx, sqlescape.MustEscapeSQL("DROP TABLE IF EXISTS %n", tableName)); err != nil {
		return "", fmt.Errorf("failed to drop table: %w", err)
	}

	// The table counter was removed before CREATE, so SHOW CREATE needs no
	// textual substitutions. Ensure a trailing newline.
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

// buildDSN constructs a MySQL DSN from the given parameters.
func buildDSN(username, password, host, database string) string {
	if password != "" {
		return fmt.Sprintf("%s:%s@tcp(%s)/%s", username, password, host, database)
	}
	return fmt.Sprintf("%s@tcp(%s)/%s", username, host, database)
}
