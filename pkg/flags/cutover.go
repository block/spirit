package flags

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/block/spirit/pkg/dbconn"
)

// Cutover is the configuration shared by the commands that end in a cutover:
// migrate and move. Sync runs continuously, takes no table locks and has no
// sentinel, so it does not embed it. Like Common it is embedded anonymously,
// so a composite literal must name it, e.g.
// move.Move{Cutover: flags.Cutover{DeferCutOver: true}}.
type Cutover struct {
	// ForceKillAfter and LockWaitTimeout bound how long spirit's DDL and table
	// locks wait on the workload, and when spirit kills the transactions
	// blocking them. A zero ForceKillAfter means 90% of LockWaitTimeout; a zero
	// LockWaitTimeout keeps dbconn's default.
	ForceKillAfter  time.Duration `name:"force-kill-after" help:"Delay before killing transactions blocking DDL or table locks; 0 uses 90% of lock-wait-timeout" optional:"" default:"0s"`
	LockWaitTimeout time.Duration `name:"lock-wait-timeout" help:"The DDL lock_wait_timeout required for checksum and cutover" optional:"" default:"30s"`

	// DeferCutOver creates the sentinel table before the copy, so the run
	// blocks before cutover (running a continuous checksum) until an operator
	// drops it.
	DeferCutOver bool `name:"defer-cutover" help:"Defer cutover (and continuous checksum) until the sentinel table is dropped" optional:"" default:"false"`
	// IgnoreSentinel lets the run cut over while a sentinel table it did not
	// create exists. By default (the zero value, for the CLI and for
	// programmatic callers alike) a run blocks before cutover while any
	// sentinel exists, so an operator can hold a cutover by creating one. It
	// never overrides DeferCutOver: see WaitsOnSentinel. Tests set it so they
	// can run concurrently despite the shared sentinel name.
	IgnoreSentinel bool `name:"ignore-sentinel" help:"Cut over even while a sentinel table exists, unless --defer-cutover is set" optional:"" default:"false" hidden:""`
	// DeprecatedRespectSentinel keeps the removed hidden --respect-sentinel
	// parsing on the CLI: --respect-sentinel=false means --ignore-sentinel.
	// It is a pointer so that an unset flag is distinguishable from false.
	// Go callers set IgnoreSentinel instead. Remove in a later release.
	DeprecatedRespectSentinel *bool `name:"respect-sentinel" help:"Deprecated: use --ignore-sentinel" optional:"" hidden:""`
}

// WaitsOnSentinel reports whether the run blocks before cutover while the
// sentinel table exists. DeferCutOver overrides IgnoreSentinel: a run that
// created a sentinel and then ignored it would cut over without the deferral
// the caller asked for.
func (c *Cutover) WaitsOnSentinel() bool {
	return c.DeferCutOver || !c.ignoresSentinel()
}

func (c *Cutover) ignoresSentinel() bool {
	if c.DeprecatedRespectSentinel != nil {
		return !*c.DeprecatedRespectSentinel
	}
	return c.IgnoreSentinel
}

// Validate rejects a negative LockWaitTimeout (ApplyTo would silently keep
// the default), a ForceKillAfter that leaves no time to acquire a lock, and
// --respect-sentinel combined with --ignore-sentinel.
func (c *Cutover) Validate() error {
	if c.DeprecatedRespectSentinel != nil && c.IgnoreSentinel {
		return errors.New("--respect-sentinel is deprecated and cannot be combined with --ignore-sentinel")
	}
	if c.LockWaitTimeout < 0 {
		return fmt.Errorf("--lock-wait-timeout must be non-negative, got %s", c.LockWaitTimeout)
	}
	config := dbconn.NewDBConfig()
	c.ApplyTo(config)
	return config.ValidateForceKillAfter()
}

// WarnDeprecated logs once per run when the deprecated --respect-sentinel is
// set. It is separate from Validate because Validate runs more than once.
func (c *Cutover) WarnDeprecated(logger *slog.Logger) {
	if c.DeprecatedRespectSentinel != nil {
		logger.Warn("--respect-sentinel is deprecated and will be removed: a sentinel is respected by default; use --ignore-sentinel instead of --respect-sentinel=false")
	}
}

// ApplyTo copies the lock timeouts onto a connection config. A zero
// LockWaitTimeout leaves the config's own value alone.
func (c *Cutover) ApplyTo(config *dbconn.DBConfig) {
	if c.LockWaitTimeout > 0 {
		config.LockWaitTimeout = int(c.LockWaitTimeout.Seconds())
	}
	config.ForceKillAfter = c.ForceKillAfter
}
