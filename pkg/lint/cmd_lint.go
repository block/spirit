package lint

import (
	"context"
	"fmt"
	"os"
	"regexp"

	"github.com/block/spirit/pkg/statement"
)

// LintCmd is the Kong CLI struct for the lint command.
// It lints an entire schema without requiring a target.
type LintCmd struct {
	// Source of existing schema (exactly one required)
	SourceDSN string `help:"MySQL DSN for existing schema. TLS is attempted by default; add tls=false to the DSN to disable it" xor:"source" required:"" env:"MYSQL_DSN"`
	SourceDir string `help:"Directory of CREATE TABLE .sql files for existing schema" xor:"source" required:"" type:"existingdir"`

	// Filtering
	IgnoreTables string `help:"Regex pattern of table names to ignore" default:""`
}

// Run executes the lint command. It is called by Kong.
func (cmd *LintCmd) Run() error {
	ctx := context.Background()

	// 1. Load source schema
	source, err := loadSource(ctx, cmd.SourceDSN, cmd.SourceDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading source schema: %s\n", err)
		os.Exit(2)
	}

	// 2. Build config — lint everything, no LintOnlyChanges
	config, err := buildIgnoreTablesConfig(cmd.IgnoreTables, source)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error building config: %s\n", err)
		os.Exit(2)
	}

	// 3. Run linters with no changes (lint entire schema)
	violations, err := RunLinters(source, nil, config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error running linters: %s\n", err)
		os.Exit(2)
	}

	// 4. Print violations
	printViolations(violations)

	// 5. Exit code
	if HasErrors(violations) {
		os.Exit(1)
	}

	return nil
}

// loadSource loads the existing schema from either a DSN or a directory.
func loadSource(ctx context.Context, dsn, dir string) ([]*statement.CreateTable, error) {
	if dsn != "" {
		return LoadSchemaFromDSN(ctx, dsn)
	}
	return LoadSchemaFromDir(dir)
}

// buildIgnoreTablesConfig constructs a Config with IgnoreTables populated
// from a regex pattern matched against the source schema.
func buildIgnoreTablesConfig(pattern string, source []*statement.CreateTable) (Config, error) {
	config := Config{}

	if pattern != "" {
		re, err := regexp.Compile(pattern)
		if err != nil {
			return Config{}, fmt.Errorf("invalid --ignore-tables regex %q: %w", pattern, err)
		}

		config.IgnoreTables = make(map[string]bool)
		for _, ct := range source {
			if re.MatchString(ct.TableName) {
				config.IgnoreTables[ct.TableName] = true
			}
		}
	}

	return config, nil
}
