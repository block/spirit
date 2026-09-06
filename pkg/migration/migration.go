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
	"github.com/block/spirit/pkg/migration/check"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
)

// defaultWriteThreads must match the `default:"4"` kong tag on
// Migration.WriteThreads, so a programmatic caller that leaves the field unset
// lands on the same value the CLI does. Move keeps its own copy against its own
// tag (pkg/move), since the two flags are independently defaulted.
const defaultWriteThreads = 4

// defaultThreads must match the `default:"4"` kong tag on Migration.Threads, for
// the same reason defaultWriteThreads does. Validate reads it because it runs
// before normalizeOptions, so a programmatic caller's zero has to be resolved to
// the number the migration will actually use before it can be checked.
const defaultThreads = 4

// defaultMaxConnections must match the `default:"128"` kong tag on
// Migration.MaxConnections, for the same reason defaultWriteThreads does: a
// programmatic caller that leaves the field unset must land where the CLI does.
// This one matters more than most, because it is the pool size itself rather
// than a knob on one: a programmatic caller that landed somewhere else would be
// running with a differently-sized pool than the same migration from the CLI.
const defaultMaxConnections = 128

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
	Threads      int     `name:"threads" help:"Number of concurrent threads for copy and checksum tasks. Ignored when --enable-experimental-autoscaling engages" optional:"" default:"4"`
	WriteThreads int     `name:"write-threads" help:"Number of concurrent apply (write) threads. Ignored when --enable-experimental-autoscaling engages" optional:"" default:"4"`

	// MaxConnections is the size of the main connection pool, set verbatim and
	// never recomputed (see the MaxOpenConnections assignment in Runner.Run).
	//
	// The connections it spends are the server's max_connections, shared with
	// the production workload, so this is spirit's claim on someone else's
	// budget rather than a description of what spirit could use. Ask for more
	// than the server can spare and the copy does not slow down, it dies on
	// `Error 1040: Too many connections`.
	//
	// The thread ceilings bound how far the copier scales its own workers and
	// can exceed this. When they do, the workers contend for connections instead
	// of each being guaranteed one, which costs throughput and nothing else.
	//
	// Zero means "use the default" (normalizeOptions fills it in), matching
	// Threads and WriteThreads. Negative is rejected by Validate, as is any
	// value too small for the migration to finish on; see minPoolSize.
	MaxConnections int `name:"max-connections" help:"Size of the main connection pool. Copier, applier and flush workers all share it, and contend for connections rather than each being guaranteed one" optional:"" default:"128"`

	// EnableExperimentalAutoscaling turns on dynamic thread scaling driven by
	// throttler feedback. When it engages (an Aurora target with at least
	// autoscale.MinVCPUs) it takes over both thread counts: Threads and
	// WriteThreads are ignored, and each pool's starting size and ceiling are
	// derived from the instance instead — see the override in
	// setupCopierCheckerAndReplClient and autoscale.ReadBounds. See issue #831.
	EnableExperimentalAutoscaling bool `name:"enable-experimental-autoscaling" help:"EXPERIMENTAL: size the copy, apply and checksum thread pools from the instance and scale them on throttler feedback. Overrides --threads and --write-threads. Requires an Aurora target" optional:"" default:"false"`
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
	Statement            string        `name:"statement" help:"The SQL statement to run" required:""`

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
const minPoolSize = 5

// Validate is called by Kong after parsing to reject invalid flag values.
// Zero values mean "use the default" (normalizeOptions fills them in), so they
// are not rejected here; only explicitly-negative or otherwise invalid values
// are caught.
//
// The cross-flag check on MaxConnections is the exception, and it is here
// because it has nowhere else to be: the pool is set to that number verbatim
// and never recomputed, so a number too small to work is a migration that
// stalls somewhere in the middle rather than one that fails at startup.
func (m *Migration) Validate() error {
	if m.Threads < 0 {
		return fmt.Errorf("--threads must be non-negative, got %d", m.Threads)
	}
	if m.WriteThreads < 0 {
		return fmt.Errorf("--write-threads must be non-negative, got %d", m.WriteThreads)
	}
	if m.ReplicaMaxLag < 0 {
		return fmt.Errorf("--replica-max-lag must be non-negative, got %s", m.ReplicaMaxLag)
	}
	if m.CheckpointMaxAge < 0 {
		return fmt.Errorf("--checkpoint-max-age must be non-negative, got %s", m.CheckpointMaxAge)
	}
	if m.MaxConnections < 0 {
		return fmt.Errorf("--max-connections must be non-negative, got %d", m.MaxConnections)
	}
	if m.MaxConnections > 0 {
		if m.MaxConnections < minPoolSize {
			return fmt.Errorf("--max-connections must be at least %d for the cutover to run, got %d",
				minPoolSize, m.MaxConnections)
		}
		// The checksum's read transactions each pin a connection for the whole
		// phase whether or not a worker has one checked out, so the pool has to
		// hold them *plus* everything that has to keep running alongside them:
		// the checksum's own off-pool queries, the control plane, and the drain
		// (see checksumPhaseReserve). A pool that only just fits the
		// transactions is not a slow checksum, it is a checksum during which the
		// drain cannot check out a connection at all.
		//
		// Threads is read through defaultThreads because zero here means "use
		// the default" and normalizeOptions has not run yet — validating the 0
		// would accept a pool that the 4 it becomes cannot run on.
		threads := m.Threads
		if threads == 0 {
			threads = defaultThreads
		}
		if pinned := threads + minChecksumPhaseReserve; m.MaxConnections < pinned {
			return fmt.Errorf("--max-connections (%d) is below what the checksum phase needs: %d pinned read transactions plus %d reserved for off-pool queries, the control plane and the drain; use at least %d, or lower --threads",
				m.MaxConnections, threads, minChecksumPhaseReserve, pinned)
		}
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
// --statement is the only way to describe the change, and it is the canonical
// source of truth for the rest of the code.
func (m *Migration) normalizeOptions() (stmts []*statement.AbstractStatement, err error) {
	if m.TargetChunkSize == 0 {
		m.TargetChunkSize = table.DefaultTargetChunkBytes
	}
	if m.Threads == 0 {
		m.Threads = defaultThreads
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
	if m.MaxConnections == 0 {
		m.MaxConnections = defaultMaxConnections
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
