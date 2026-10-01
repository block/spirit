package flags

import (
	"fmt"
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
	// RespectSentinel makes the run block before cutover while a sentinel
	// table exists, including one it did not create. It is true on the CLI.
	// A programmatic caller's zero value ignores a sentinel it did not ask
	// for (tests use this to run concurrently despite the shared sentinel
	// name) but never one it did: see WaitsOnSentinel.
	RespectSentinel bool `name:"respect-sentinel" help:"Look for sentinel table to exist and block if it does" optional:"" default:"true" hidden:""`
}

// WaitsOnSentinel reports whether the run blocks before cutover while the
// sentinel table exists. DeferCutOver implies it: a run that created a
// sentinel and then ignored it would cut over without the deferral the caller
// asked for.
func (c *Cutover) WaitsOnSentinel() bool {
	return c.DeferCutOver || c.RespectSentinel
}

// Validate rejects a negative LockWaitTimeout (ApplyTo would silently keep
// the default) and a ForceKillAfter that leaves no time to acquire a lock.
func (c *Cutover) Validate() error {
	if c.LockWaitTimeout < 0 {
		return fmt.Errorf("--lock-wait-timeout must be non-negative, got %s", c.LockWaitTimeout)
	}
	config := dbconn.NewDBConfig()
	c.ApplyTo(config)
	return config.ValidateForceKillAfter()
}

// ApplyTo copies the lock timeouts onto a connection config. A zero
// LockWaitTimeout leaves the config's own value alone.
func (c *Cutover) ApplyTo(config *dbconn.DBConfig) {
	if c.LockWaitTimeout > 0 {
		config.LockWaitTimeout = int(c.LockWaitTimeout.Seconds())
	}
	config.ForceKillAfter = c.ForceKillAfter
}
