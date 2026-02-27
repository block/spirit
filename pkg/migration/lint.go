package migration

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/lint"
	"github.com/block/spirit/pkg/statement"
)

var (
	// defaultLinterSettings holds settings for linters where we know we want to override linter
	// system defaults or ensure specific behavior.
	defaultLinterSettings = map[string]map[string]string{
		"invisible_index_before_drop": {
			"raiseError": "false",
		},
	}
)

func (r *Runner) lint(ctx context.Context) error {
	var createTables []*statement.CreateTable
	var alterTables []*statement.AbstractStatement
	config := lint.Config{
		Enabled:  make(map[string]bool),
		Settings: defaultLinterSettings,
	}

	if err := printLinters(config); err != nil {
		return err
	}

	for _, change := range r.changes {
		// Collect ALTER TABLE statements and the CREATE TABLEs for the tables they reference
		if change.stmt.IsAlterTable() {
			alterTables = append(alterTables, change.stmt)

			ct, err := r.getCreateTable(ctx, change.stmt.Schema, change.stmt.Table)
			if err != nil {
				return err
			}
			createTables = append(createTables, ct)
		}

		// If the migration creates a table, we need to collect that CREATE TABLE as well
		if change.stmt.IsCreateTable() {
			ct, err := change.stmt.ParseCreateTable()
			if err != nil {
				return err
			}
			createTables = append(createTables, ct)
		}
	}

	var errs []error

	violations, err := lint.RunLinters(createTables, alterTables, config)
	if err != nil {
		errs = append(errs, err)
	}

	for _, v := range violations {
		if v.Severity == lint.SeverityError {
			errs = append(errs, errors.New(v.String()))
		}
		fmt.Println(v)
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (r *Runner) getCreateTable(ctx context.Context, db string, tbl string) (*statement.CreateTable, error) {
	// Escape backticks in db and tbl names to be extra pedantic
	db = strings.ReplaceAll(db, "`", "``")
	tbl = strings.ReplaceAll(tbl, "`", "``")
	sql := fmt.Sprintf("show create table `%s`.`%s`", db, tbl)

	row := r.db.QueryRowContext(ctx, sql)
	var createTable string
	if err := row.Scan(&tbl, &createTable); err != nil {
		return nil, err
	}
	stmt, err := statement.ParseCreateTable(createTable)
	if err != nil {
		return nil, err
	}
	return stmt, nil
}

func printLinters(config lint.Config) error {
	fmt.Printf("Linting is enabled with the following linters:\n")
	linters := lint.List()
	for _, linterName := range linters {
		l, err := lint.Get(linterName)
		// Skip linters that are not enabled
		if !config.IsEnabled(linterName) {
			continue
		}

		if err != nil {
			return err
		}
		fmt.Printf(" + %s", l)
		if cfg, ok := config.Settings[linterName]; ok {
			fmt.Print(" (with config")
			for k, v := range cfg {
				fmt.Printf(" %s=%s", k, v)
			}
			fmt.Print(") ")
		}
		fmt.Println()
	}
	return nil
}
