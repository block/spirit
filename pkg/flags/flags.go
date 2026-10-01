// Package flags holds the command-line flags shared by the migrate, move and
// sync commands.
//
// The three commands used to declare these separately, with defaults, help
// text and validation that drifted apart (move's --threads defaulted to 2, the
// others to 4; only migrate had --lock-wait-timeout, --force-kill-after and the
// TLS flags). Common is embedded in each command's Kong struct, so each flag,
// its default and its validation are declared once.
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
	Threads int `name:"threads" help:"Number of concurrent threads for copy and checksum tasks. Ignored when --enable-experimental-autoscaling engages" optional:"" default:"4"`
	// WriteThreads is the number of apply (write) workers, per target.
	WriteThreads int `name:"write-threads" help:"Number of concurrent apply (write) threads per target. Ignored when --enable-experimental-autoscaling engages" optional:"" default:"4"`

	// MaxConnections is the size of each connection pool spirit opens to a
	// source or target server, set verbatim and never recomputed. Its
	// connections are the server's max_connections, shared with the
	// production workload, so it is spirit's claim on someone else's budget
	// and spirit does not derive it from anything: worker counts never grow it.
	MaxConnections int `name:"max-connections" help:"Size of each connection pool. Copier, applier and flush workers all share it, and contend for connections rather than each being guaranteed one" optional:"" default:"128"`

	// TargetChunkSize is the in-memory byte budget the copier sizes each copy
	// chunk against (the memory signal; see table.DefaultTargetChunkBytes and
	// pkg/table/README.md). Zero means "use the default" (Normalize fills it in).
	TargetChunkSize uint64 `name:"target-chunk-size" help:"In-memory byte budget per copy chunk (in bytes)" optional:"" default:"16777216"`

	// MaxCommitLatency throttles when a target's average commit latency exceeds
	// this threshold. Auto-enabled only on Aurora targets; zero disables it.
	// See issue #468. It also decides whether autoscaling may grow write
	// threads past their start on a redo-aware signal (see
	// throttler.ResolveMaxWriteThreads).
	MaxCommitLatency time.Duration `name:"max-commit-latency" help:"Throttle when average commit latency exceeds this threshold (currently only auto-enabled on Aurora)" optional:"" default:"100ms"`

	// EnableExperimentalAutoscaling turns on dynamic thread scaling driven by
	// the targets' Aurora load signal. When it engages (concurrency.Engage) it
	// takes over both thread counts: Threads and WriteThreads are replaced with
	// instance-derived starting sizes, and each pool scales between bounds
	// derived from the instance. See issue #831.
	EnableExperimentalAutoscaling bool `name:"enable-experimental-autoscaling" help:"EXPERIMENTAL: size the copy, apply and checksum thread pools from the instance and scale them on throttler feedback. Overrides --threads and --write-threads. Requires an Aurora target" optional:"" default:"false"`

	// ForceKillAfter and LockWaitTimeout bound how long spirit's DDL and table
	// locks wait on the workload, and when spirit kills the transactions
	// blocking them. A zero ForceKillAfter means 90% of LockWaitTimeout; a zero
	// LockWaitTimeout keeps dbconn's default.
	ForceKillAfter  time.Duration `name:"force-kill-after" help:"Delay before killing transactions blocking DDL or table locks; 0 uses 90% of lock-wait-timeout" optional:"" default:"0s"`
	LockWaitTimeout time.Duration `name:"lock-wait-timeout" help:"The DDL lock_wait_timeout required for checksum and cutover" optional:"" default:"30s"`

	// TLS Configuration. Empty keeps the connection config's own value
	// (dbconn's PREFERRED default, or what a conf file supplied), and a DSN's
	// own tls= parameter takes precedence over both.
	TLSMode            string `name:"tls-mode" help:"TLS connection mode (case insensitive): DISABLED, PREFERRED (default), REQUIRED, VERIFY_CA, VERIFY_IDENTITY" optional:""`
	TLSCertificatePath string `name:"tls-ca" help:"Path to custom TLS CA certificate file" optional:""`
}

// Validate rejects explicitly negative thread counts and durations, and a
// ForceKillAfter that leaves no time to acquire a lock. Zero counts are
// accepted and mean "use the default". Each command validates MaxConnections itself, because the smallest
// usable pool depends on what the command runs on it
// (dbconn.ValidateMaxConnections vs dbconn.ValidateConnectionLimit).
func (c *Common) Validate() error {
	if c.Threads < 0 {
		return fmt.Errorf("--threads must be non-negative, got %d", c.Threads)
	}
	if c.WriteThreads < 0 {
		return fmt.Errorf("--write-threads must be non-negative, got %d", c.WriteThreads)
	}
	// ApplyTo and the throttler treat a non-positive duration as "use the
	// default" and "disabled" respectively, so a negative one would silently
	// become a different setting.
	if c.LockWaitTimeout < 0 {
		return fmt.Errorf("--lock-wait-timeout must be non-negative, got %s", c.LockWaitTimeout)
	}
	if c.MaxCommitLatency < 0 {
		return fmt.Errorf("--max-commit-latency must be non-negative (0 disables it), got %s", c.MaxCommitLatency)
	}
	config := dbconn.NewDBConfig()
	c.ApplyTo(config)
	return config.ValidateForceKillAfter()
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
//
// A zero WriteThreads is warned about: it used to mean "auto-size from the
// instance", so anyone who adopted that opt-in would otherwise see their apply
// pool quietly drop from the instance vCPU count to the default. (Kong's
// default is non-zero, so a literal 0 was either passed explicitly or left
// unset by a programmatic caller.)
func (c *Common) Normalize(logger *slog.Logger) {
	if logger == nil {
		logger = slog.Default()
	}
	if c.Threads <= 0 {
		c.Threads = DefaultThreads
	}
	if c.WriteThreads <= 0 {
		if c.WriteThreads == 0 {
			logger.Warn("--write-threads 0 no longer means auto-size; using the default. Pass --enable-experimental-autoscaling for instance-derived thread counts",
				"write_threads", DefaultWriteThreads)
		}
		c.WriteThreads = DefaultWriteThreads
	}
	if c.MaxConnections == 0 {
		c.MaxConnections = DefaultMaxConnections
	}
	if c.TargetChunkSize == 0 {
		c.TargetChunkSize = table.DefaultTargetChunkBytes
	}
}

// ApplyTo copies the connection-level flags onto a connection config: the pool
// size, the lock timeouts and TLS. Zero or empty values leave the config's own
// value alone, so a programmatic caller that never set one keeps dbconn's
// default. Callers that want a different pool size for a dedicated pool (a
// monitor, a replica) override MaxOpenConnections afterwards.
func (c *Common) ApplyTo(config *dbconn.DBConfig) {
	if c.MaxConnections > 0 {
		config.MaxOpenConnections = c.MaxConnections
	}
	if c.LockWaitTimeout > 0 {
		config.LockWaitTimeout = int(c.LockWaitTimeout.Seconds())
	}
	config.ForceKillAfter = c.ForceKillAfter
	if c.TLSMode != "" {
		config.TLSMode = c.TLSMode
	}
	if c.TLSCertificatePath != "" {
		config.TLSCertificatePath = c.TLSCertificatePath
	}
}
