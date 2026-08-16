// Package migration contains the logic for running online schema changes.
package migration

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/block/spirit/pkg/checksum"
	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/migration/check"
	"github.com/block/spirit/pkg/parser"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
)

// defaultWriteThreads must match the `default:"4"` kong tag on
// Migration.WriteThreads, so a programmatic caller that leaves the field unset
// lands on the same value the CLI does. Move keeps its own copy against its own
// tag (pkg/move), since the two flags are independently defaulted.
const defaultWriteThreads = 4

var (
	defaultHost     = "127.0.0.1"
	defaultPort     = 3306
	defaultUsername = "spirit"
	defaultPassword = "spirit"
	defaultDatabase = "test"
	defaultTLSMode  = "PREFERRED"
)

type Migration struct {
	Host         string  `name:"host" help:"Hostname" optional:""`
	Username     string  `name:"username" help:"User" optional:""`
	Password     *string `name:"password" help:"Password" optional:""`
	Database     string  `name:"database" help:"Database" optional:""`
	ConfFile     string  `name:"conf" help:"MySQL conf file" optional:"" type:"existingfile"`
	Table        string  `name:"table" help:"Table" optional:""`
	Alter        string  `name:"alter" help:"The alter statement to run on the table" optional:""`
	Threads      int     `name:"threads" help:"Number of concurrent threads for copy and checksum tasks. Ignored when --enable-experimental-autoscaling engages" optional:"" default:"4"`
	WriteThreads int     `name:"write-threads" help:"Number of concurrent apply (write) threads. Ignored when --enable-experimental-autoscaling engages" optional:"" default:"4"`

	// EnableExperimentalAutoscaling turns on dynamic thread scaling driven by
	// throttler feedback. When it engages (an Aurora target with at least
	// autoscale.MinVCPUs) it takes over both thread counts: Threads and
	// WriteThreads are ignored, and each pool's starting size and ceiling are
	// derived from the instance instead — see the override in
	// setupCopierCheckerAndReplClient and autoscale.ReadBounds. See issue #831.
	EnableExperimentalAutoscaling bool `name:"enable-experimental-autoscaling" help:"EXPERIMENTAL: size the copy, apply and checksum thread pools from the instance and scale them on throttler feedback. Overrides --threads and --write-threads. Requires an Aurora target" optional:"" default:"false"`
	// TargetChunkTime sizes chunks for the time-based signal: the checksum
	// (server-side CRC). The copier ignores it and sizes chunks by an
	// in-memory byte budget (table.DefaultTargetChunkBytes), because its
	// fed-back time measures read + applier-queue-wait + write/commit — a
	// signal that is size-independent under backpressure and collapses the
	// chunk size.
	TargetChunkTime time.Duration `name:"target-chunk-time" help:"Target time per chunk for the checksum. The copier sizes chunks by memory instead; see --target-chunk-size." optional:"" default:"500ms"`
	// TargetChunkSize is the in-memory byte budget the copier sizes each copy
	// chunk against (the memory signal; see table.DefaultTargetChunkBytes and
	// pkg/table/README.md). A zero value means "use the default"
	// (normalizeOptions fills it in), so callers that construct Migration
	// programmatically don't have to set it.
	// The Kong default below must stay equal to table.DefaultTargetChunkBytes.
	TargetChunkSize      uint64        `name:"target-chunk-size" help:"In-memory byte budget per copy chunk (in bytes)" optional:"" default:"16777216"`
	ReplicaDSN           string        `name:"replica-dsn" help:"DSN(s) for replica(s) used for lag checking. Multiple replicas can be comma-separated; Spirit throttles on the slowest." optional:""`
	ReplicaMaxLag        time.Duration `name:"replica-max-lag" help:"The maximum lag allowed on the replica before the migration throttles. If lag becomes unobservable (lag polling keeps failing) the migration pauses (fails closed) until polling recovers; remove --replica-dsn to proceed without lag protection." optional:"" default:"120s"`
	LockWaitTimeout      time.Duration `name:"lock-wait-timeout" help:"The DDL lock_wait_timeout required for checksum and cutover" optional:"" default:"30s"`
	SkipDropAfterCutover bool          `name:"skip-drop-after-cutover" help:"Keep old table after completing cutover" optional:"" default:"false"`
	DeferCutOver         bool          `name:"defer-cutover" help:"Defer cutover (and checksum) until sentinel table is dropped" optional:"" default:"false"`
	SkipForceKill        bool          `name:"skip-force-kill" help:"Disable killing long-running transactions in order to acquire metadata lock (MDL) at checksum and cutover time" optional:"" default:"false"`
	Statement            string        `name:"statement" help:"The SQL statement to run (replaces --table and --alter)" optional:"" default:""`

	// TLS Configuration
	TLSMode            string `name:"tls-mode" help:"TLS connection mode (case insensitive): DISABLED, PREFERRED (default), REQUIRED, VERIFY_CA, VERIFY_IDENTITY" optional:""`
	TLSCertificatePath string `name:"tls-ca" help:"Path to custom TLS CA certificate file" optional:""`

	CheckpointMaxAge     time.Duration `name:"checkpoint-max-age" help:"Maximum age of a checkpoint before refusing to resume from it" optional:"" default:"168h"`
	ChecksumYieldTimeout time.Duration `name:"checksum-yield-timeout" help:"Maximum duration for a single checksum pass before yielding to release long-running REPEATABLE READ transactions (reduces InnoDB HLL growth)" optional:"" default:"24h"`

	// MaxCommitLatency throttles when observed commit latency exceeds this
	// threshold. Currently auto-enabled only on Aurora (auto-detected); the
	// default 100ms is intentionally a high upper bound to only cut the most
	// extreme tail latencies. See issue #468.
	MaxCommitLatency time.Duration `name:"max-commit-latency" help:"Throttle when average commit latency exceeds this threshold (currently only auto-enabled on Aurora)" optional:"" default:"100ms"`

	// Hidden options for now (supports more obscure cash/sq usecases)
	InterpolateParams bool `name:"interpolate-params" help:"Enable interpolate params for DSN" optional:"" default:"false" hidden:""`
	// Used for tests so we can concurrently execute without issues even though
	// the sentinel name is shared. Basically it will be true here, but false
	// in the tests unless we set it explicitly true.
	RespectSentinel bool `name:"respect-sentinel" help:"Look for sentinel table to exist and block if it does" optional:"" default:"true" hidden:""`

	// useTestCutover is a test-only cutover
	useTestCutover   bool
	useTestThrottler bool
}

// Validate is called by Kong after parsing to reject invalid flag values.
// Zero values mean "use the default" (normalizeOptions fills them in), so they
// are not rejected here; only explicitly-negative or otherwise invalid values
// are caught. There are currently no cross-flag combination checks.
func (m *Migration) Validate() error {
	if m.Threads < 0 {
		return fmt.Errorf("--threads must be non-negative, got %d", m.Threads)
	}
	if m.WriteThreads < 0 {
		return fmt.Errorf("--write-threads must be non-negative, got %d", m.WriteThreads)
	}
	if m.TargetChunkTime < 0 {
		return fmt.Errorf("--target-chunk-time must be non-negative, got %s", m.TargetChunkTime)
	}
	if m.ReplicaMaxLag < 0 {
		return fmt.Errorf("--replica-max-lag must be non-negative, got %s", m.ReplicaMaxLag)
	}
	if m.CheckpointMaxAge < 0 {
		return fmt.Errorf("--checkpoint-max-age must be non-negative, got %s", m.CheckpointMaxAge)
	}
	return nil
}

func (m *Migration) Run() error {
	migration, err := NewRunner(m)
	if err != nil {
		return err
	}
	defer utils.CloseAndLog(migration)
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
	if m.TargetChunkSize == 0 {
		m.TargetChunkSize = table.DefaultTargetChunkBytes
	}
	if m.Threads == 0 {
		m.Threads = 4
	}
	// A non-positive WriteThreads is filled in rather than rejected, matching
	// Threads above. Zero used to mean "auto-size from the instance", so anyone
	// who adopted that opt-in would otherwise see their apply pool quietly drop
	// from the instance vCPU count to 4 — warn, and name the flag that replaced
	// it. (Kong's default is 4, so a literal 0 was either passed explicitly or
	// left unset by a programmatic caller.)
	if m.WriteThreads <= 0 {
		if m.WriteThreads == 0 {
			slog.Default().Warn("--write-threads 0 no longer means auto-size; using the default. Pass --enable-experimental-autoscaling for instance-derived thread counts",
				"write_threads", defaultWriteThreads)
		}
		m.WriteThreads = defaultWriteThreads
	}
	if m.ReplicaMaxLag == 0 {
		m.ReplicaMaxLag = 120 * time.Second
	}
	if m.CheckpointMaxAge == 0 {
		m.CheckpointMaxAge = 7 * 24 * time.Hour // 7 days
	}
	if m.ChecksumYieldTimeout == 0 {
		m.ChecksumYieldTimeout = checksum.DefaultYieldTimeout
	}

	if err := m.normalizeConnectionOptions(); err != nil {
		return nil, err
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
		fullStatement := fmt.Sprintf("ALTER TABLE %s %s", sqlescape.EscapeIdentifier(m.Table), m.Alter)
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
	return stmts, err
}

func (m *Migration) normalizeConnectionOptions() error {
	confParams, err := newConfParams(m.ConfFile)
	if err != nil {
		return err
	}
	if m.Host == "" {
		m.Host = confParams.GetHost()
	}
	if !strings.Contains(m.Host, ":") {
		hostAndPort := fmt.Sprintf("%s:%d", m.Host, confParams.GetPort())
		m.Host = hostAndPort
	}
	if m.Username == "" {
		m.Username = confParams.GetUser()
	}
	if m.Password == nil {
		pw := confParams.GetPassword()
		m.Password = &pw
	}
	if m.Database == "" {
		m.Database = confParams.GetDatabase()
	}
	if m.TLSMode == "" {
		m.TLSMode = confParams.GetTLSMode()
	}
	if m.TLSCertificatePath == "" {
		m.TLSCertificatePath = confParams.GetTLSCA()
	}
	return nil
}
