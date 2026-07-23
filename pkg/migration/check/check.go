// Package check provides various configuration and health checks
// that can be run against a sql.DB connection.
package check

import (
	"context"
	"database/sql"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
)

// ScopeFlag scopes a check
type ScopeFlag uint8

const (
	ScopeNone        ScopeFlag = 0
	ScopePreRun      ScopeFlag = 1 << 0
	ScopePreflight   ScopeFlag = 1 << 1
	ScopePostSetup   ScopeFlag = 1 << 2
	ScopeCutover     ScopeFlag = 1 << 3
	ScopePostCutover ScopeFlag = 1 << 4
	ScopeTesting     ScopeFlag = 1 << 5
	// ScopeStatement marks preflight checks that classify the ALTER statement
	// itself: they decide from the parsed statement alone whether Spirit
	// refuses to run it, never touching a database connection. Callers can run
	// them via RunChecks with only Resources.Statement set (Table is optional
	// and widens coverage when present) to determine up front that a statement
	// would be refused — for example, a planning tool classifying DDL before
	// an apply. Passing these checks is not a promise Spirit will accept the
	// statement: checks that need a live connection (existing foreign keys,
	// triggers, privileges, ...) still run only at preflight. A check tagged
	// with this scope must tolerate every Resources field except Statement
	// being unset.
	ScopeStatement ScopeFlag = 1 << 6
)

type Resources struct {
	DB                   *sql.DB
	Replicas             []*sql.DB
	Table                *table.TableInfo
	Statement            *statement.AbstractStatement
	TargetChunkTime      time.Duration
	Threads              int
	ReplicaMaxLag        time.Duration
	SkipDropAfterCutover bool
	ForceKill            bool
	// The following resources are only used by the
	// pre-run checks
	Host               string
	Username           string
	Password           string
	TLSMode            string
	TLSCertificatePath string
	// GTID, when true, opts the migration into the experimental GTID-based
	// change source. The configuration check uses this to additionally
	// validate gtid_mode and enforce_gtid_consistency on the source.
	GTID bool
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

// RunChecks runs all checks that are registered for the given scope.
// Checks run in name order so that a statement failing more than one
// check always reports the same error.
func RunChecks(ctx context.Context, r Resources, logger *slog.Logger, scope ScopeFlag) error {
	lock.Lock()
	registered := maps.Clone(checks)
	lock.Unlock()
	for _, name := range slices.Sorted(maps.Keys(registered)) {
		check := registered[name]
		if check.scope&scope == 0 {
			continue
		}
		err := check.callback(ctx, r, logger)
		if err != nil {
			return err
		}
	}
	return nil
}
