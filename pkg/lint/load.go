package lint

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/block/mysql"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
)

// LoadSchemaFromDSN connects to a MySQL server and retrieves all CREATE TABLE
// statements from the connected database, parsed into structured CreateTable objects.
func LoadSchemaFromDSN(ctx context.Context, dsn string) ([]*statement.CreateTable, error) {
	db, err := sql.Open("block-mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	defer utils.CloseAndLog(db)
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	rawTables, err := table.LoadSchemaFromDB(ctx, db)
	if err != nil {
		return nil, err
	}
	var tables []*statement.CreateTable
	for _, raw := range rawTables {
		ct, err := statement.ParseCreateTable(raw.Schema)
		if err != nil {
			return nil, fmt.Errorf("failed to parse CREATE TABLE for %s: %w", raw.Name, err)
		}
		tables = append(tables, ct)
	}
	return tables, nil
}

// LoadSchemaFromDir reads all .sql files from a directory and parses them as
// CREATE TABLE statements. Each file should contain exactly one CREATE TABLE statement.
func LoadSchemaFromDir(dir string) ([]*statement.CreateTable, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory %s: %w", dir, err)
	}

	var tables []*statement.CreateTable
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasSuffix(strings.ToLower(entry.Name()), ".sql") {
			continue
		}

		path := filepath.Join(dir, entry.Name())
		content, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("failed to read %s: %w", path, err)
		}

		ct, err := statement.ParseCreateTable(string(content))
		if err != nil {
			return nil, fmt.Errorf("failed to parse %s: %w", path, err)
		}
		tables = append(tables, ct)
	}

	return tables, nil
}
