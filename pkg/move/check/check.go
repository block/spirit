// Package check provides various configuration and health checks
// that can be run for move operations.
package check

import (
	"context"
	"database/sql"
	"log/slog"
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
	for _, check := range checks {
		if check.scope != scope {
			continue
		}
		err := check.callback(ctx, r, logger)
		if err != nil {
			return err
		}
	}
	return nil
}
