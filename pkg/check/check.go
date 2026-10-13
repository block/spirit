// Package check holds the checks spirit's runners make before and while they
// change data: configuration, privileges, and refusals of tables and
// statements spirit does not support.
//
// Every check declares two things: the phases of a run it belongs to (Phase,
// when it runs) and the runtimes it applies to (Runtime, which runner runs
// it). RunChecks runs the checks that match both, so a check that only applies
// to one runner is never run by another, and a check that does apply fails
// rather than passes when the runner does not supply what it needs.
package check

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
)

// Runtime is the runner a check applies to. A check registers for every
// runtime it applies to; RunChecks runs it only for those.
type Runtime uint8

const (
	// Migration is a schema change of one or more tables (pkg/migration).
	Migration Runtime = 1 << iota
	// Move is a move of tables to other servers (pkg/move).
	Move
	// Sync is a continuous sync of a schema (pkg/datasync).
	Sync
)

// Phase is when in a run a check runs. A check registers for every phase it
// runs in.
type Phase uint8

const (
	PhasePreRun    Phase = 1 << 0
	PhasePreflight Phase = 1 << 1
	PhasePostSetup Phase = 1 << 2
	PhaseCutover   Phase = 1 << 3
	// PhaseResume runs when a move resumes from a checkpoint, in place of
	// PhasePostSetup: an object can be created on a source or a target
	// between runs, so the checks of the setup are repeated.
	PhaseResume Phase = 1 << 4
	// phaseTesting is for this package's tests only.
	phaseTesting Phase = 1 << 5
	// PhaseStatement marks checks a caller can run ahead of an apply to learn
	// that Spirit will refuse a statement. Most are preflight checks that also
	// run as part of a migration; a check registered only here predicts, from
	// the statement, a refusal a later phase makes for every statement it
	// refuses. Callers run them via
	// RunChecks with Resources.Change.Statement set and, optionally,
	// Resources.Change.Table — the table's current metadata, which widens coverage to
	// the checks that compare the statement against the existing column
	// definitions. Neither needs a database connection: Table can be built
	// from the table's DDL with statement.CreateTable.ToTableInfo. A check
	// tagged with this phase must tolerate every Resources field except
	// Change.Statement being unset.
	//
	// A failure here is a refusal the caller can report as certain, so the
	// phase only carries checks that no earlier stage can bypass on any
	// server. The migration runner runs this phase itself, once it has loaded
	// the tables and before it attempts MySQL's native DDL, so it refuses
	// every statement these checks refuse. Spirit attempts that native DDL —
	// ALGORITHM=INSTANT, then a safe-INPLACE subset — before it runs
	// preflight checks, and MySQL decides what that completes, which varies
	// with the server version and the table. A preflight check the native DDL
	// may complete (dropadd, rename) is deliberately excluded: claiming those
	// as refusals would report failure for an apply that succeeds, and the
	// runner, running them here, would then make it fail.
	//
	// That exclusion only ever under-reports, which is the safe direction:
	// passing these checks is not a promise Spirit will accept the statement,
	// only a failure is a claim. An excluded check still refuses at preflight
	// whenever the native attempt does not take the statement — Spirit skips
	// the attempt altogether for a multi-table change, and an older server
	// rejects shapes a newer one completes instantly. Checks that need a live
	// connection (existing foreign keys, triggers, privileges, ...) are not in
	// this phase either: they run at preflight, and the foreign key and
	// trigger checks run again before and during the cutover, to catch one
	// created while the migration runs.
	//
	// A caller that reports these refusals somewhere other than its own logs
	// judges the text by reading the checks that produce it, and a check added
	// to the phase is picked up without any change on the caller's side. Pin
	// the membership with Checks to keep that judgement attached to the
	// checks it was made about.
	PhaseStatement Phase = 1 << 6
	// PhaseCutoverLocked runs while the cutover holds its table lock, after
	// the final flush and immediately before the RENAME. LOCK TABLES ... WRITE
	// keeps out any DDL that needs a metadata lock on the table, so a check
	// here sees the state the RENAME acts on. The runner stops acting on
	// schema-change notifications once the cutover starts, so this is the
	// only check of anything created while the cutover waits for its lock.
	// A refusal (an error that matches ErrRefused, see refuse) fails the
	// cutover without a retry. Any other error is retried like any other
	// cutover failure, so a check must return ErrRefused for a permanent
	// condition, or it takes the table lock again on every retry. Keep these
	// checks to fast reads: they run while the application's writes to the
	// table are blocked.
	//
	// A move runs this phase while its forward cutover holds the table locks
	// on every source, after the final flush and before the traffic switch
	// and the source rename. A refusal leaves the source live.
	PhaseCutoverLocked Phase = 1 << 7
)

// Endpoint is one MySQL server a run reads from or writes to, at one schema.
type Endpoint struct {
	DB *sql.DB
	// Config is the connection's configuration. Config.DBName is the schema.
	Config *mysql.Config
	// Tables are the tables of this endpoint the run acts on.
	Tables []*table.TableInfo
}

// Change is the table and statement of one change in a migration. The
// migration runner runs its checks once per change.
type Change struct {
	Table     *table.TableInfo
	NewTable  *table.TableInfo // the altered shadow table; set from PhasePostSetup onwards
	Statement *statement.AbstractStatement
}

// Options are the settings of the run that checks validate or depend on.
type Options struct {
	Threads       int
	ReplicaMaxLag time.Duration
	// ExperimentalForeignKeys is --enable-experimental-foreign-keys: a table
	// with foreign keys of its own is supported on MySQL 9.7 and later.
	ExperimentalForeignKeys bool
	// MoveEverything is true when a move was given no explicit table list,
	// so every table in each source database is moved. The
	// source_schema_consistency check then also requires an identical table
	// *set* across all sources: when moving everything, an extra or missing
	// table on one shard is a drift error; when only a named subset is
	// moved, tables outside that subset are ignored.
	MoveEverything bool
}

// Credentials are the connection settings PhasePreRun checks connect with:
// the run has no connection of its own yet.
type Credentials struct {
	Host               string
	Username           string
	Password           string
	TLSMode            string
	TLSCertificatePath string
}

// Resources is what a run gives its checks. Runtime is required; which of the
// other fields are set depends on the runtime and the phase.
type Resources struct {
	// Runtime is the runner running the checks. RunChecks runs only the
	// checks registered for it.
	Runtime Runtime
	// Sources are the servers the run reads from. A migration has exactly
	// one: the server it alters the table on.
	Sources []Endpoint
	// Targets are the servers a move or a sync writes to.
	Targets []applier.Target
	// SourceTables are the first source's tables. They stand for every
	// source where only their reported definition matters; Sources[i].Tables
	// holds each source's own (Move and Sync).
	SourceTables []*table.TableInfo
	// CheckpointTable is the checkpoint table the run writes on Targets[0]
	// (Move and Sync).
	CheckpointTable string
	// Replicas are the migration's replicas, for the replica checks.
	Replicas []*sql.DB
	// Change is the migration's table and statement (Migration only).
	Change      Change
	Options     Options
	Credentials Credentials

	// phase is the phase the checks are running under, set by RunChecks. A
	// check that tolerates a missing resource for an external caller reads it
	// to tell that case from a migration which failed to supply the resource.
	phase Phase
}

type check struct {
	name     string
	run      func(context.Context, Resources, *slog.Logger) error
	phases   Phase
	runtimes Runtime
}

var (
	checks map[string]check
	lock   sync.Mutex
)

// register registers c. A check's name is its identity: registering a second
// check under a name already taken panics, so two checks can never shadow
// each other.
func register(c check) {
	lock.Lock()
	defer lock.Unlock()
	if checks == nil {
		checks = make(map[string]check)
	}
	if _, ok := checks[c.name]; ok {
		panic(fmt.Sprintf("check %q is registered twice", c.name))
	}
	if c.phases == 0 || c.runtimes == 0 {
		panic(fmt.Sprintf("check %q is registered without a phase or a runtime", c.name))
	}
	checks[c.name] = c
}

// RunChecks runs every check registered for both r.Runtime and phase, except
// those named in exclude. r.Runtime must be exactly one runtime: a check that
// branches on the runtime reads it as the runner it is running for. Every
// name in exclude must be a registered check. Checks run in name order so that a run failing more
// than one check always reports the same error.
//
// logger may be nil: checks log as they run, and a caller classifying a
// statement without a logger to hand must get a verdict rather than a panic.
func RunChecks(ctx context.Context, r Resources, logger *slog.Logger, phase Phase, exclude ...string) error {
	if r.Runtime == 0 {
		return errors.New("check: Resources.Runtime is not set")
	}
	if !r.Runtime.valid() {
		return fmt.Errorf("check: Resources.Runtime %d is not exactly one of Migration, Move or Sync", r.Runtime)
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	r.phase = phase
	lock.Lock()
	registered := maps.Clone(checks)
	lock.Unlock()
	// An exclusion that names no check would silently run the check it was
	// meant to skip once that check is renamed.
	for _, name := range exclude {
		if _, ok := registered[name]; !ok {
			return fmt.Errorf("check: cannot exclude %q: no check is registered under that name", name)
		}
	}
	for _, name := range namesFor(registered, r.Runtime, phase) {
		if slices.Contains(exclude, name) {
			continue
		}
		if err := registered[name].run(ctx, r, logger); err != nil {
			return err
		}
	}
	return nil
}

// Checks reports the names of the checks registered for runtime and phase, in
// the order RunChecks runs them.
//
// Phase membership is a curated set, not an accident of registration: each
// phase states what a check in it must tolerate and what its failure entitles
// the caller to conclude. A caller relying on that contract can pin the set it
// was written against, so a check added to the phase later has to be judged
// against the contract rather than inherit a verdict made about its
// predecessors.
func Checks(runtime Runtime, phase Phase) []string {
	lock.Lock()
	registered := maps.Clone(checks)
	lock.Unlock()
	return namesFor(registered, runtime, phase)
}

// namesFor returns the names of the checks in registered that belong to
// runtime and phase, sorted so RunChecks and Checks agree on the order.
func namesFor(registered map[string]check, runtime Runtime, phase Phase) []string {
	names := make([]string, 0, len(registered))
	for name, c := range registered {
		if c.phases&phase == 0 || c.runtimes&runtime == 0 {
			continue
		}
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// sourceDB returns the connection to the first source, or nil when there is
// none: an external PhaseStatement caller supplies no connection, and a check
// that tolerates that reads a nil connection as "not supplied".
func (r Resources) sourceDB() *sql.DB {
	if len(r.Sources) == 0 {
		return nil
	}
	return r.Sources[0].DB
}

// schema returns the endpoint's schema, or "" when it has no configuration.
func (e Endpoint) schema() string {
	if e.Config == nil {
		return ""
	}
	return e.Config.DBName
}

// sourceError attributes err to source i. A migration has a single source, so
// its errors are returned unchanged; a move names the source.
func (r Resources) sourceError(i int, err error) error {
	if r.Runtime == Migration {
		return err
	}
	return fmt.Errorf("source %d: %w", i, err)
}

// tableError attributes err to the source table named table.
func (r Resources) tableError(table string, err error) error {
	return fmt.Errorf("table '%s' cannot be %s: %w", table, r.Runtime.participle(), err)
}

// valid reports whether rt is exactly one of the runtimes.
func (rt Runtime) valid() bool {
	return rt == Migration || rt == Move || rt == Sync
}

// noun names the runtime's operation in an error message ("run a migration").
func (rt Runtime) noun() string {
	switch rt {
	case Migration:
		return "migration"
	case Move:
		return "move"
	case Sync:
		return "sync"
	}
	return "spirit operation"
}

// participle completes "cannot be ..." in an error message ("cannot be moved").
func (rt Runtime) participle() string {
	switch rt {
	case Migration:
		return "migrated"
	case Move:
		return "moved"
	case Sync:
		return "synced"
	}
	return "processed"
}
