// Package check provides various configuration and health checks
// that can be run for move operations.
package check

import (
	"context"
	"database/sql"
	"log/slog"
	"slices"
	"sync"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/table"
	"github.com/go-sql-driver/mysql"
)

// ScopeFlag scopes a check
type ScopeFlag uint8

const (
	ScopeNone ScopeFlag = iota
	ScopePreRun
	ScopePreflight
	ScopePostSetup
	ScopeResume
)

// SourceResource holds per-source connection state for checks.
type SourceResource struct {
	DB     *sql.DB
	Config *mysql.Config
	DSN    string
}

// Resources contains the resources needed for move checks
type Resources struct {
	Sources        []SourceResource
	Targets        []applier.Target
	SourceTables   []*table.TableInfo
	CreateSentinel bool
	// GTID, when true, opts the move into the experimental GTID-based change
	// source. The configuration check uses this to additionally validate
	// gtid_mode and enforce_gtid_consistency on every source.
	GTID bool
	// MoveEverything is true when no explicit table list was supplied (i.e.
	// move.SourceTables is empty), so every table in each source database is
	// being moved. The source_schema_consistency check uses this to decide
	// whether to also require an identical table *set* across all sources: when
	// moving everything, an extra/missing table on one shard is a drift error;
	// when only a named subset is moved, tables outside that subset are ignored.
	MoveEverything bool
}

type check struct {
	callback func(context.Context, Resources, *slog.Logger) error
	scope    ScopeFlag
}

var (
	checks map[string]check
	lock   sync.Mutex
)

// registerCheck registers a check (callback func) and a scope (aka time) that it is expected to be run
func registerCheck(name string, callback func(context.Context, Resources, *slog.Logger) error, scope ScopeFlag) {
	lock.Lock()
	defer lock.Unlock()
	if checks == nil {
		checks = make(map[string]check)
	}
	checks[name] = check{callback: callback, scope: scope}
}

// RunChecks runs all checks that are registered for the given scope
func RunChecks(ctx context.Context, r Resources, logger *slog.Logger, scope ScopeFlag) error {
	return RunChecksExcluding(ctx, r, logger, scope)
}

// RunChecksExcluding runs all checks registered for the given scope except
// those named in exclude. The runner's --force recovery path uses it to
// re-run the post-setup checks minus the target-state check before wiping
// the target: wiping only cures target-side state, so a failure in any other
// (source-side) check must surface before the target is destroyed. New
// checks are deliberately included by default — excluding too little only
// blocks a wipe, excluding too much could green-light one.
func RunChecksExcluding(ctx context.Context, r Resources, logger *slog.Logger, scope ScopeFlag, exclude ...string) error {
	for name, check := range checks {
		if check.scope != scope || slices.Contains(exclude, name) {
			continue
		}
		err := check.callback(ctx, r, logger)
		if err != nil {
			return err
		}
	}
	return nil
}
