package lint

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/block/spirit/pkg/statement"
)

// StatementSource represents a single source of SQL statements.
type StatementSource struct {
	// Origin describes where this SQL came from
	// For files: "file:" + file path (e.g., "file:migrations/001.sql")
	// For stdin: "stdin"
	// For command-line: "cmdline"
	Origin string

	// SQL contains the actual SQL content
	SQL string
}

// resolveStatement takes a single --statement argument and returns one or more StatementSources.
// - Inline SQL → 1 StatementSource with Origin="cmdline"
// - "-" (stdin) → 1 StatementSource with Origin="stdin"
// - "file:path.sql" → 1 StatementSource with Origin="file:path.sql"
// - "file:dir/" → N StatementSources (one per .sql file in directory, recursively)
// - "file:*.sql" → N StatementSources (one per matching file)
func resolveStatement(arg string) ([]StatementSource, error) {
	// Check for stdin
	if arg == "-" {
		content, err := io.ReadAll(os.Stdin)
		if err != nil {
			return nil, fmt.Errorf("failed to read from stdin: %w", err)
		}

		return []StatementSource{{
			Origin: "stdin",
			SQL:    string(content),
		}}, nil
	}

	// Check for file: prefix
	if strings.HasPrefix(arg, "file:") {
		path := strings.TrimPrefix(arg, "file:")

		// Check if it's a glob pattern (contains wildcard characters)
		if strings.ContainsAny(path, "*?[]") {
			return resolveGlob(path)
		}

		// Try to stat the path to determine if it's a file or directory
		info, err := os.Stat(path)
		if err != nil {
			return nil, fmt.Errorf("failed to access %s: %w", path, err)
		}

		if info.IsDir() {
			return resolveDirectory(path)
		}

		return resolveFile(path)
	}

	// Default to command-line SQL
	return []StatementSource{{
		Origin: "cmdline",
		SQL:    arg,
	}}, nil
}

// resolveFile reads a single SQL file and returns a StatementSource
func resolveFile(path string) ([]StatementSource, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", path, err)
	}

	return []StatementSource{{
		Origin: "file:" + path,
		SQL:    string(content),
	}}, nil
}

// resolveDirectory recursively finds all .sql files in a directory and returns StatementSources
func resolveDirectory(dir string) ([]StatementSource, error) {
	var sources []StatementSource

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories and non-.sql files
		if info.IsDir() || !strings.HasSuffix(strings.ToLower(path), ".sql") {
			return nil
		}

		content, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("failed to read file %s: %w", path, err)
		}

		sources = append(sources, StatementSource{
			Origin: "file:" + path,
			SQL:    string(content),
		})

		return nil
	})
	if err != nil {
		return nil, err
	}

	if len(sources) == 0 {
		return nil, fmt.Errorf("no .sql files found in directory: %s", dir)
	}

	return sources, nil
}

// resolveGlob expands a glob pattern and returns StatementSources for all matching files
func resolveGlob(pattern string) ([]StatementSource, error) {
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid glob pattern %s: %w", pattern, err)
	}

	if len(matches) == 0 {
		return nil, fmt.Errorf("no files matched glob pattern: %s", pattern)
	}

	var sources []StatementSource

	for _, path := range matches {
		// Skip directories
		info, err := os.Stat(path)
		if err != nil {
			return nil, fmt.Errorf("failed to stat file %s: %w", path, err)
		}

		if info.IsDir() {
			continue
		}

		content, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("failed to read file %s: %w", path, err)
		}

		sources = append(sources, StatementSource{
			Origin: "file:" + path,
			SQL:    string(content),
		})
	}

	if len(sources) == 0 {
		return nil, fmt.Errorf("glob pattern matched only directories: %s", pattern)
	}

	return sources, nil
}

// parseStatementSource parses a single StatementSource and extracts CREATE TABLE and ALTER TABLE statements.
// Returns the parsed statements and any error encountered.
// Note: Due to limitations in statement.New(), a single source cannot contain both CREATE TABLE and ALTER TABLE statements.
func parseStatementSource(source StatementSource) ([]*statement.CreateTable, []*statement.AbstractStatement, error) {
	sql := strings.TrimSpace(source.SQL)
	if sql == "" {
		return nil, nil, nil // Empty source is OK
	}

	var (
		createTables    []*statement.CreateTable
		alterStatements []*statement.AbstractStatement
	)

	// Parse all statements
	stmts, err := statement.New(sql)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse %s: %w", source.Origin, err)
	}

	// Categorize statements
	for _, stmt := range stmts {
		if stmt.IsAlterTable() {
			alterStatements = append(alterStatements, stmt)
		} else {
			// It's a CREATE TABLE, parse into structured format
			ct, err := statement.ParseCreateTable(stmt.Statement)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to parse CREATE TABLE from %s: %w", source.Origin, err)
			}

			createTables = append(createTables, ct)
		}
	}

	return createTables, alterStatements, nil
}

// Lint is the struct for the lint command
type Lint struct {
	Statement []string `help:"CREATE TABLE and ALTER TABLE statements to lint" sep:"none"`
	Linters   []string `help:"Specific linters to run (default: all)" default:"all"`
	Config    []string `help:"Individual linter configuration properties"`
}

func (l *Lint) Run() error {
	var (
		allCreateTables    []*statement.CreateTable
		allAlterStatements []*statement.AbstractStatement
		lintConfig         Config
	)

	if len(l.Statement) == 0 {
		return errors.New("must specify at least one statement to lint")
	}

	// Resolve all statement arguments into sources
	var sources []StatementSource

	for _, arg := range l.Statement {
		s, err := resolveStatement(arg)
		if err != nil {
			return err
		}

		sources = append(sources, s...)
	}

	// Parse each source
	for _, source := range sources {
		createTables, alterStatements, err := parseStatementSource(source)
		if err != nil {
			return err
		}

		if len(createTables) == 0 && len(alterStatements) == 0 {
			fmt.Fprintf(os.Stderr, "Warning: no valid statements found in %s, skipping\n", source.Origin)
			continue // No valid statements in this source
		}

		allCreateTables = append(allCreateTables, createTables...)
		allAlterStatements = append(allAlterStatements, alterStatements...)
	}

	// Run linters
	violations, err := RunLinters(allCreateTables, allAlterStatements, lintConfig)
	if err != nil {
		return fmt.Errorf("failed to run linters: %w", err)
	}

	if len(violations) == 0 {
		fmt.Println("No lint violations found")
		return nil
	}

	for _, v := range violations {
		fmt.Println(v.String())
	}

	return errors.New("lint violations found")
}
