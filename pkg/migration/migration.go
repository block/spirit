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
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/flags"
	"github.com/block/spirit/pkg/migration/check"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/throttler"
	"github.com/block/spirit/pkg/utils"
)

// defaultMaxConnections is the pool size a programmatic caller that leaves
// MaxConnections unset gets, the same as the CLI default (see flags.Common).
const defaultMaxConnections = flags.DefaultMaxConnections

var (
	defaultHost     = "127.0.0.1"
	defaultPort     = 3306
	defaultUsername = "spirit"
	defaultPassword = "spirit"
	defaultDatabase = "test"
	defaultTLSMode  = "PREFERRED"
)

type Migration struct {
	Host     string  `name:"host" help:"Hostname" optional:""`
	Username string  `name:"username" help:"User" optional:""`
	Password *string `name:"password" help:"Password" optional:""`
	Database string  `name:"database" help:"Database" optional:""`
	ConfFile string  `name:"conf" help:"MySQL conf file" optional:"" type:"existingfile"`

	// Common holds the flags shared with move and sync: thread counts,
	// --max-connections, --max-commit-latency, autoscaling, lock timeouts and
	// TLS. For a migration --max-connections is the size of the main pool (see
	// the MaxOpenConnections assignment in Runner.Run): the thread ceilings
	// bound how far the copier scales its own workers and can exceed it, in
	// which case workers contend for connections instead of each being
	// guaranteed one, which costs throughput and nothing else. Ask for more
	// than the server can spare and the copy does not slow down, it dies on
	// `Error 1040: Too many connections`. Validate rejects a value too small
	// for the migration to finish on; see minPoolSize.
	flags.Common
	flags.Cutover

	EnableExperimentalLocklessChecksum bool `name:"enable-experimental-lockless-checksum" help:"EXPERIMENTAL: verify with optimistic reads and retries instead of checksum locks and long-lived snapshots. Cutover locking is unchanged." default:"false"`

	ReplicaDSN           string        `name:"replica-dsn" help:"DSN(s) for replica(s) used for lag checking. Multiple replicas can be comma-separated; Spirit throttles on the slowest." optional:""`
	ReplicaMaxLag        time.Duration `name:"replica-max-lag" help:"The maximum lag allowed on the replica before the migration throttles. If lag becomes unobservable (lag polling keeps failing) the migration pauses (fails closed) until polling recovers; remove --replica-dsn to proceed without lag protection." optional:"" default:"120s"`
	SkipDropAfterCutover bool          `name:"skip-drop-after-cutover" help:"Keep old table after completing cutover" optional:"" default:"false"`
	Statement            string        `name:"statement" help:"The SQL statement to run" required:""`

	ChecksumYieldTimeout time.Duration `name:"checksum-yield-timeout" help:"Maximum duration for a single checksum pass before yielding to release long-running REPEATABLE READ transactions (reduces InnoDB HLL growth)" optional:"" default:"24h"`

	// useTestCutover is a test-only cutover
	useTestCutover bool
	// testThrottler is a test-only copier throttler (see WithTestThrottler).
	testThrottler throttler.Throttler
}

// minPoolSize is the smallest --max-connections a migration can complete on.
//
// The cutover sets the number: it needs the LOCK TABLES connection, the RENAME
// TABLE connection and the flush threads, and unlike every other phase it
// cannot trade connections for time — below the minimum it does not run slower,
// it cannot run. See CutOver.Run, which holds the same number and will raise a
// pool that arrives under it.
//
// Nothing else gets a say. The copy, the checksum and the drain all queue on a
// small pool and finish eventually, so a low --max-connections is the operator
// asking for a slow migration, which is theirs to ask for.
const minPoolSize = dbconn.MinMigrationPoolSize

// Validate is called by Kong after parsing to reject invalid flag values.
// Zero values mean "use the default" (normalizeOptions fills them in), so they
// are not rejected here; only explicitly-negative or otherwise invalid values
// are caught.
//
// Cross-flag checks include ForceKillAfter and MaxConnections. The latter is here
// because it has nowhere else to be: the pool is set to that number verbatim
// and never recomputed, so a number too small to work is a migration that
// stalls somewhere in the middle rather than one that fails at startup.
func (m *Migration) Validate() error {
	if err := m.Common.Validate(); err != nil {
		return err
	}
	if err := m.Cutover.Validate(); err != nil {
		return err
	}
	if m.ReplicaMaxLag < 0 {
		return fmt.Errorf("--replica-max-lag must be non-negative, got %s", m.ReplicaMaxLag)
	}
	return dbconn.ValidateMaxConnections(m.MaxConnections, m.ValidationThreads(), minChecksumPhaseReserve)
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
// --statement is the only way to describe the change, and it is the canonical
// source of truth for the rest of the code.
func (m *Migration) normalizeOptions() (stmts []*statement.AbstractStatement, err error) {
	if err := m.Common.Validate(); err != nil {
		return nil, err
	}
	if err := m.Cutover.Validate(); err != nil {
		return nil, err
	}
	m.WarnZeroWriteThreads(slog.Default())
	m.Normalize()
	if m.ReplicaMaxLag == 0 {
		m.ReplicaMaxLag = 120 * time.Second
	}
	if m.ChecksumYieldTimeout == 0 {
		m.ChecksumYieldTimeout = checksum.DefaultYieldTimeout
	}

	if err := m.normalizeConnectionOptions(); err != nil {
		return nil, err
	}

	if m.Statement == "" {
		return nil, errors.New("--statement is required")
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
