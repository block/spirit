// Package check provides various configuration and health checks
// that can be run against a sql.DB connection.
package check

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"github.com/siddontang/loggers"
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
)

type Resources struct {
	DB                   *sql.DB
	Replica              *sql.DB
	Table                *table.TableInfo
	Statement            *statement.AbstractStatement
	TargetChunkTime      time.Duration
	Threads              int
	ReplicaMaxLag        time.Duration
	SkipDropAfterCutover bool
	ForceKill            bool
	// The following resources are only used by the
	// pre-run checks
	Host                     string
	Username                 string
	Password                 string
	TLSMode                  string
	TLSCertificatePath       string
	ExperimentalBufferedCopy bool
	Checksum                 bool
}

type check struct {
	callback func(context.Context, Resources, loggers.Advanced) error
	scope    ScopeFlag
}

var (
	checks map[string]check
	lock   sync.Mutex
)

// registerCheck registers a check (callback func) and a scope (aka time) that it is expected to be run
func registerCheck(name string, callback func(context.Context, Resources, loggers.Advanced) error, scope ScopeFlag) {
	lock.Lock()
	defer lock.Unlock()
	if checks == nil {
		checks = make(map[string]check)
	}
	checks[name] = check{callback: callback, scope: scope}
}

// RunChecks runs all checks that are registered for the given scope
func RunChecks(ctx context.Context, r Resources, logger loggers.Advanced, scope ScopeFlag) error {
	for _, check := range checks {
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
