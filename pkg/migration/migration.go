// Package migration contains the logic for running online schema changes.
package migration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/block/spirit/pkg/check"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"github.com/pingcap/tidb/pkg/parser"
)

var (
	ErrMismatchedAlter = errors.New("alter statement in checkpoint table does not match the alter statement specified here")
)

type Migration struct {
	Host                 string        `name:"host" help:"Hostname" optional:"" default:"127.0.0.1:3306"`
	Username             string        `name:"username" help:"User" optional:"" default:"spirit"`
	Password             string        `name:"password" help:"Password" optional:"" default:"spirit"`
	Database             string        `name:"database" help:"Database" optional:"" default:"test"`
	Table                string        `name:"table" help:"Table" optional:""`
	Alter                string        `name:"alter" help:"The alter statement to run on the table" optional:""`
	Threads              int           `name:"threads" help:"Number of concurrent threads for copy and checksum tasks" optional:"" default:"4"`
	TargetChunkTime      time.Duration `name:"target-chunk-time" help:"The target copy time for each chunk" optional:"" default:"500ms"`
	Checksum             bool          `name:"checksum" help:"Checksum new table before final cut-over" optional:"" default:"true"`
	ReplicaDSN           string        `name:"replica-dsn" help:"A DSN for a replica which (if specified) will be used for lag checking." optional:""`
	ReplicaMaxLag        time.Duration `name:"replica-max-lag" help:"The maximum lag allowed on the replica before the migration throttles." optional:"" default:"120s"`
	LockWaitTimeout      time.Duration `name:"lock-wait-timeout" help:"The DDL lock_wait_timeout required for checksum and cutover" optional:"" default:"30s"`
	SkipDropAfterCutover bool          `name:"skip-drop-after-cutover" help:"Keep old table after completing cutover" optional:"" default:"false"`
	DeferCutOver         bool          `name:"defer-cutover" help:"Defer cutover (and checksum) until sentinel table is dropped" optional:"" default:"false"`
	ForceKill            bool          `name:"force-kill" help:"Kill long-running transactions in order to acquire metadata lock (MDL) at checksum and cutover time" optional:"" default:"false"`
	Strict               bool          `name:"strict" help:"Exit on --alter mismatch when incomplete migration is detected" optional:"" default:"false"`
	Statement            string        `name:"statement" help:"The SQL statement to run (replaces --table and --alter)" optional:"" default:""`
	// TLS Configuration
	TLSMode            string `name:"tls-mode" help:"TLS connection mode (case insensitive): DISABLED, PREFERRED (default), REQUIRED, VERIFY_CA, VERIFY_IDENTITY" optional:"" default:"PREFERRED"`
	TLSCertificatePath string `name:"tls-ca" help:"Path to custom TLS CA certificate file" optional:""`

	// Experimental features
	// These are no longer hidden, we document them.
	EnableExperimentalMultiTableSupport bool `name:"enable-experimental-multi-table-support" help:"Allow multiple alter statements to run concurrently and cutover together" optional:"" default:"false"`
	EnableExperimentalBufferedCopy      bool `name:"enable-experimental-buffered-copy" help:"Use the experimental buffered copier/repl applier based on the DBLog algorithm" optional:"" default:"false"`

	// Hidden options for now (supports more obscure cash/sq usecases)
	InterpolateParams bool `name:"interpolate-params" help:"Enable interpolate params for DSN" optional:"" default:"false" hidden:""`
	// Used for tests so we can concurrently execute without issues even though
	// the sentinel name is shared. Basically it will be true here, but false
	// in the tests unless we set it explicitly true.
	RespectSentinel bool `name:"respect-sentinel" help:"Look for sentinel table to exist and block if it does" optional:"" default:"true" hidden:""`
}

func (m *Migration) Run() error {
	migration, err := NewRunner(m)
	if err != nil {
		return err
	}
	defer migration.Close()
	if err := migration.runChecks(context.TODO(), check.ScopePreRun); err != nil {
		return err
	}
	if err := migration.Run(context.TODO()); err != nil {
		return err
	}
	return nil
}

// normalizeOptions does some validation and sets defaults.
// for example, it validates that only --statement or --table and --alter are specified,
// and when --statement is not specified, it generates it
// so the rest of the code can use --statement as the canonical
// source of truth for what's happening.
func (m *Migration) normalizeOptions() (stmts []*statement.AbstractStatement, err error) {
	if m.TargetChunkTime == 0 {
		m.TargetChunkTime = table.ChunkerDefaultTarget
	}
	if m.Threads == 0 {
		m.Threads = 4
	}
	if m.ReplicaMaxLag == 0 {
		m.ReplicaMaxLag = 120 * time.Second
	}
	if m.Host == "" {
		return nil, errors.New("host is required")
	}
	if !strings.Contains(m.Host, ":") {
		m.Host = fmt.Sprintf("%s:%d", m.Host, 3306)
	}
	if m.Database == "" {
		return nil, errors.New("database/schema name is required")
	}
	if m.Statement != "" { // statement is specified
		if m.Table != "" || m.Alter != "" {
			return nil, errors.New("only --statement or --table and --alter can be specified")
		}
		// extract the table and alter from the statement.
		// if it is a CREATE INDEX statement, we rewrite it to an alter statement.
		// This also returns the StmtNode.
		stmts, err = statement.New(m.Statement)
		if err != nil {
			// The error could be a parser error, or it might be something
			// specific like mixed ALTER + non alter statements.
			return nil, err
		}
		for _, stmt := range stmts {
			if stmt.Schema != "" && stmt.Schema != m.Database {
				return nil, errors.New("schema name in statement (`schema`.`table`) does not match --database")
			}
			stmt.Schema = m.Database
		}
	} else { // --alter and --table are specified
		if m.Table == "" {
			return nil, errors.New("table name is required")
		}
		if m.Alter == "" {
			return nil, errors.New("alter statement is required")
		}
		// Trim whitespace and remove trailing semicolon. Without this, the attemptInstantDDL and attemptInplaceDDL functions will fail.
		m.Alter = strings.TrimSpace(m.Alter)
		m.Alter = strings.TrimSuffix(m.Alter, ";")
		fullStatement := fmt.Sprintf("ALTER TABLE `%s` %s", m.Table, m.Alter)
		m.Statement = fullStatement // used in resume from checkpoint
		p := parser.New()
		stmtNodes, _, err := p.Parse(fullStatement, "", "")
		if err != nil {
			return nil, errors.New("could not parse SQL statement: " + fullStatement)
		}
		stmts = append(stmts, &statement.AbstractStatement{
			Schema:    m.Database,
			Table:     m.Table,
			Alter:     m.Alter,
			Statement: fullStatement,
			StmtNode:  &stmtNodes[0],
		})
	}

	if len(stmts) > 1 && !m.EnableExperimentalMultiTableSupport {
		return nil, errors.New("multiple statements detected. To enable this experimental feature, please specify --enable-experimental-multi-table-support")
	}
	return stmts, err
}
