// Package flags holds the command-line flags shared by the migrate, move and
// sync commands.
//
// The three commands used to declare these separately, with defaults, help
// text and validation that drifted apart (move's --threads defaulted to 2, the
// others to 4; only migrate had the TLS flags). Common is embedded in each
// command's Kong struct, so each flag, its default and its validation are
// declared once. Cutover holds the flags of the two commands that end in a
// cutover (migrate and move); sync runs continuously and has none.
//
// The autoscaling setup that turns these flags into thread counts is
// concurrency.Engage.
package flags

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
)

// The defaults below must match the `default:` Kong tags on Common, so a
// programmatic caller that leaves a field unset (Normalize) lands on the same
// value the CLI does.
const (
	DefaultThreads        = 4
	DefaultWriteThreads   = 4
	DefaultMaxConnections = dbconn.DefaultMaxConnections

	DefaultCheckpointMaxAge = 7 * 24 * time.Hour
)

// Common is the configuration shared by migrate, move and sync. It is embedded
// (anonymously) in each command's Kong struct, so its fields are promoted:
// m.Threads keeps working, but a composite literal must name the embedded
// struct, e.g. migration.Migration{Common: flags.Common{Threads: 8}}.
//
// Zero thread counts, connection counts and chunk sizes mean "use the default" (Normalize fills
// them in); negative ones are rejected by Validate. A zero MaxCommitLatency is
// not replaced: it disables the commit-latency throttler.
type Common struct {
	// Threads is the number of read workers: the copier's read side and the
	// checksum's workers.
	Threads int `name:"threads" help:"Number of concurrent threads for copy and checksum tasks. Replaced on Aurora (by autoscaling, or by small-instance mode below 4 vCPUs) unless --skip-autoscaling is set" optional:"" default:"4"`
	// WriteThreads is the number of apply (write) workers, per target.
	WriteThreads int `name:"write-threads" help:"Number of concurrent apply (write) threads per target. Replaced on Aurora (by autoscaling, or by small-instance mode below 4 vCPUs) unless --skip-autoscaling is set" optional:"" default:"4"`

	// MaxConnections is the size of each connection pool spirit opens to a
	// source or target server, set verbatim and never recomputed. Its
	// connections are the server's max_connections, shared with the
	// production workload, so it is spirit's claim on someone else's budget
	// and spirit does not derive it from anything: worker counts never grow it.
	MaxConnections int `name:"max-connections" help:"Size of each connection pool. Copier, applier and flush workers all share it, and contend for connections rather than each being guaranteed one" optional:"" default:"128"`

	// TargetChunkSize is the in-memory byte budget the copier sizes each copy
	// chunk against (the memory signal; see table.DefaultTargetChunkBytes and
	// pkg/table/README.md). Zero means "use the default" (Normalize fills it in).
	TargetChunkSize uint64 `name:"target-chunk-size" help:"In-memory byte budget per copy chunk (in bytes). Lowered to 1 MiB in small-instance mode (a small Aurora target, unless --skip-autoscaling is set)" optional:"" default:"16777216"`

	// MaxCommitLatency throttles when a target's average commit latency exceeds
	// this threshold. Auto-enabled only on Aurora targets; zero disables it.
	// See issue #468. It also decides whether autoscaling may grow write
	// threads past their start on a redo-aware signal (see
	// throttler.ResolveMaxWriteThreads).
	MaxCommitLatency time.Duration `name:"max-commit-latency" help:"Throttle when average commit latency exceeds this threshold (currently only auto-enabled on Aurora)" optional:"" default:"100ms"`

	// SkipAutoscaling turns off dynamic thread scaling driven by the targets'
	// Aurora load signal, which is on by default. When autoscaling engages
	// (concurrency.Engage) it takes over both thread counts: Threads and
	// WriteThreads are replaced with instance-derived starting sizes, and each
	// pool scales between bounds derived from the instance. An Aurora target
	// below autoscale.MinVCPUs gets small-instance mode instead, which also
	// replaces both counts. It only engages on Aurora; on other servers this
	// flag has no effect. See issue #831.
	SkipAutoscaling bool `name:"skip-autoscaling" help:"Do not size the copy, apply and checksum thread pools from the instance or scale them on throttler feedback, and use --threads and --write-threads instead. Autoscaling only engages on Aurora, so this flag has no effect elsewhere. It also disables small-instance mode (one thread per pool and 1 MiB chunks on an Aurora target with fewer than 4 vCPUs)" optional:"" default:"false"`

	// ThreadsAreBaseline declares that Threads and WriteThreads are the
	// caller's baseline for servers autoscaling does not engage on, not a load
	// cap. It is for programmatic callers that embed spirit and pass their own
	// non-default counts on every run. Autoscaling and small-instance mode
	// still replace the counts on Aurora; this only silences the warning that
	// they did, which otherwise fires on every Aurora run and names a CLI flag
	// (--skip-autoscaling) such a caller may not expose. A caller whose counts
	// are a cap sets SkipAutoscaling instead. It is not a CLI flag: a
	// non-default count passed on the command line is an explicit choice.
	ThreadsAreBaseline bool `kong:"-"`

	// CheckpointMaxAge is the oldest checkpoint a run will resume from. Its
	// age is the time since the checkpoint row was last written, i.e. how long
	// the previous run has been stopped. What happens to a checkpoint that is
	// too old is per-command: migrate starts fresh, move and sync fail (their
	// targets are not empty) and point at --force or a larger value. Zero
	// means the default (Normalize fills it in).
	CheckpointMaxAge time.Duration `name:"checkpoint-max-age" help:"Maximum age of a checkpoint before refusing to resume from it" optional:"" default:"168h"`

	// InterpolateParams sets the driver's interpolateParams on every
	// connection: client-side placeholder interpolation instead of
	// server-side prepared statements.
	InterpolateParams bool `name:"interpolate-params" help:"Enable interpolate params for DSN" optional:"" default:"false" hidden:""`

	// TLS Configuration. Empty keeps the connection config's own value
	// (dbconn's PREFERRED default, or what a conf file supplied), and a DSN's
	// own tls= parameter takes precedence over both.
	TLSMode            string `name:"tls-mode" help:"TLS connection mode (case insensitive): DISABLED, PREFERRED (default), REQUIRED, VERIFY_CA, VERIFY_IDENTITY" optional:""`
	TLSCertificatePath string `name:"tls-ca" help:"Path to custom TLS CA certificate file" optional:""`
}

// Validate rejects explicitly negative thread counts and durations. Zero
// values are accepted and mean "use the default". Each command validates MaxConnections itself, because the smallest
// usable pool depends on what the command runs on it
// (dbconn.ValidateMaxConnections vs dbconn.ValidateConnectionLimit).
func (c *Common) Validate() error {
	if c.Threads < 0 {
		return fmt.Errorf("--threads must be non-negative, got %d", c.Threads)
	}
	if c.WriteThreads < 0 {
		return fmt.Errorf("--write-threads must be non-negative, got %d", c.WriteThreads)
	}
	// The throttler treats a non-positive latency as "disabled" and Normalize
	// a zero age as "use the default", so a negative one would silently become
	// a different setting.
	if c.MaxCommitLatency < 0 {
		return fmt.Errorf("--max-commit-latency must be non-negative (0 disables it), got %s", c.MaxCommitLatency)
	}
	if c.CheckpointMaxAge < 0 {
		return fmt.Errorf("--checkpoint-max-age must be non-negative, got %s", c.CheckpointMaxAge)
	}
	return nil
}

// ValidationThreads is the read-thread count a run will start with, for
// validating MaxConnections before Normalize has run.
func (c *Common) ValidationThreads() int {
	if c.Threads == 0 {
		return DefaultThreads
	}
	return c.Threads
}

// Normalize fills in the defaults for zero counts, so a programmatic caller
// that leaves a field unset gets what the CLI does.
func (c *Common) Normalize() {
	if c.Threads <= 0 {
		c.Threads = DefaultThreads
	}
	if c.WriteThreads <= 0 {
		c.WriteThreads = DefaultWriteThreads
	}
	if c.MaxConnections == 0 {
		c.MaxConnections = DefaultMaxConnections
	}
	if c.TargetChunkSize == 0 {
		c.TargetChunkSize = table.DefaultTargetChunkBytes
	}
	if c.CheckpointMaxAge == 0 {
		c.CheckpointMaxAge = DefaultCheckpointMaxAge
	}
}

// WarnZeroWriteThreads warns when WriteThreads is zero, before Normalize
// replaces it with the default. In migrate and move a zero used to mean
// "auto-size from the instance", so anyone who relied on that would
// otherwise see their apply pool quietly drop from the instance vCPU count to
// the default. Sync always treated zero as the default, so it does not call
// this. (Kong's default is non-zero, so a literal 0 was either passed
// explicitly or left unset by a programmatic caller.)
func (c *Common) WarnZeroWriteThreads(logger *slog.Logger) {
	if c.WriteThreads != 0 {
		return
	}
	if logger == nil {
		logger = slog.Default()
	}
	logger.Warn("--write-threads 0 no longer means auto-size; using the default. On Aurora, thread counts are derived from the instance unless --skip-autoscaling is set",
		"write_threads", DefaultWriteThreads)
}

// ApplyTo copies the connection-level flags onto a connection config: the pool
// size, TLS and parameter interpolation. Zero or empty values leave the config's own
// value alone, so a programmatic caller that never set one keeps dbconn's
// default. Callers that want a different pool size for a dedicated pool (a
// monitor, a replica) override MaxOpenConnections afterwards.
func (c *Common) ApplyTo(config *dbconn.DBConfig) {
	if c.MaxConnections > 0 {
		config.MaxOpenConnections = c.MaxConnections
	}
	if c.InterpolateParams {
		config.InterpolateParams = true
	}
	if c.TLSMode != "" {
		config.TLSMode = c.TLSMode
	}
	if c.TLSCertificatePath != "" {
		config.TLSCertificatePath = c.TLSCertificatePath
	}
}
