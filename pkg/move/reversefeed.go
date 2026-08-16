package move

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
)

// ReverseFeed implements the data plane of the "reverse-window" move: after a
// forward cutover, it tails the (former) target's binlog and applies changes
// back to the (former) source in CHANGE-ONLY mode — no copy, no checksum — so
// the source stays current and the move can be rolled back for a bounded
// window. It is the mirror of the forward move: the former targets become the
// reverse sources and the former source becomes the reverse target.
//
// It reuses change.Source exactly as the forward move does; the only new idea
// is direction + "change-only" (SetWatermarkOptimization(false), started from a
// position captured at cutover). Because the applier is idempotent (REPLACE
// INTO / DELETE by PK), starting slightly early replays no-ops, so the cutover
// position handoff does not need to be exact.
//
// This type is deliberately decoupled from the forward Runner and its cutover
// so it can be exercised in isolation (see reversefeed_test.go): construct it
// against two already-consistent databases, drive writes on the source side,
// and assert the target converges.

// ReverseSource is one reverse source: a former move target (shard S) whose
// binlog is tailed to keep the former move source (U) current.
//
// DB's default database MUST be this source's schema. table.TableInfo.SetInfo
// resolves columns via information_schema WHERE table_schema=DATABASE() — the
// connection's default database, not the SchemaName argument — so a connection
// defaulting to the wrong schema silently loads the wrong table's definition,
// surfacing only at apply time as a fatal column-count mismatch.
type ReverseSource struct {
	DB       *sql.DB            // connection to S (default DB == its schema)
	Addr     string             // host:port for the binlog syncer
	User     string             // binlog syncer user
	Password string             // binlog syncer password
	Tables   []*table.TableInfo // S-side tables to watch, built on DB
	// Position is the opaque change.Source position to resume from (captured at
	// cutover). Empty means start from the source's current head. Its encoding
	// also selects the change-source coordinate scheme, exactly like a
	// checkpoint resume (see change.NewAutoClient): a GTID set resumes through
	// the GTID client (and requires the server to still have GTIDs enabled), a
	// file:offset position through the binlog client, and the empty head-start
	// case probes the server so the scheme matches what a cutover capture on
	// that server would have produced.
	Position string
}

// ReverseFeedConfig configures a ReverseFeed.
type ReverseFeedConfig struct {
	Sources []ReverseSource
	// Target is the U side when the former move source was a single database.
	// Target.DB's default database MUST be U's schema (see ReverseSource.DB).
	// Mutually exclusive with Targets.
	Target applier.Target
	// Targets is the U side when the former move source was SHARDED: one entry
	// per former source shard, each with its Vitess-style key range set. Rows
	// are routed by the WATCHED table's sharding metadata, so every
	// ReverseSource table must have ShardingColumn and HashFunc set (the source
	// keyspace's vindex) — NewReverseFeed fails otherwise. Each Targets[i].DB's
	// default database MUST be that shard's schema (see ReverseSource.DB).
	// Mutually exclusive with Target.
	Targets []applier.Target
	// TargetTables maps each watched (reverse-source) table NAME to the U-side
	// TableInfo it is written to, built on Target.DB (with Targets, on any one
	// shard: the schemas are identical and the name is unqualified, so each
	// shard's own connection determines where the write lands). It is a map,
	// not a slice, because the names can differ: after a forward cutover the
	// source tables are renamed to their _old form, so a watched "t1" is
	// written to "t1_old".
	TargetTables map[string]*table.TableInfo

	Logger        *slog.Logger
	DBConfig      *dbconn.DBConfig
	Threads       int           // applier write threads; 0 => default (4)
	FlushInterval time.Duration // 0 => change.DefaultFlushInterval
}

// ReverseFeed is a running change-only reverse feed: one change.Source per
// ReverseSource, all sharing a single applier that writes to U (mirrors the
// forward move, where N sources share one applier).
type ReverseFeed struct {
	sources  []ReverseSource
	clients  []change.Source
	appl     applier.Applier
	logger   *slog.Logger
	flushInt time.Duration

	// A fatal error on any feed (schema change or stream failure) means U can no
	// longer be trusted for rollback. We capture it once and close fatalCh so
	// Run wakes immediately and the caller can degrade to complete-forward
	// rather than silently letting U drift.
	fatalOnce sync.Once
	fatalCh   chan struct{}
	fatalMu   sync.Mutex
	fatalErr  error
}

// NewReverseFeed wires the feeds and their shared applier. It does not open any
// binlog stream — call Start or Run for that — but it does query each source
// server once: change.NewAutoClient selects (and validates) the change-source
// coordinate scheme per source, so e.g. a GTID-set Position on a server that no
// longer has GTIDs enabled fails here with a clear error rather than as a
// stream failure at Start.
func NewReverseFeed(ctx context.Context, cfg ReverseFeedConfig) (_ *ReverseFeed, err error) {
	if len(cfg.Sources) == 0 {
		return nil, errors.New("reverse feed: at least one source is required")
	}
	if len(cfg.Targets) > 0 && cfg.Target.DB != nil {
		return nil, errors.New("reverse feed: Target and Targets are mutually exclusive")
	}
	if len(cfg.Targets) == 0 && cfg.Target.DB == nil {
		return nil, errors.New("reverse feed: target DB must be non-nil")
	}
	if len(cfg.TargetTables) == 0 {
		return nil, errors.New("reverse feed: at least one target table is required")
	}
	if len(cfg.Targets) > 0 {
		for ti := range cfg.Targets {
			if cfg.Targets[ti].DB == nil {
				return nil, fmt.Errorf("reverse feed: target %d DB must be non-nil", ti)
			}
		}
		// Sharded reverse target: routing decisions come from the watched
		// tables' sharding metadata, so its absence would strand every row —
		// fail here rather than fatally at first apply.
		for si, src := range cfg.Sources {
			for _, t := range src.Tables {
				if t.ShardingColumn == "" || t.HashFunc == nil {
					return nil, fmt.Errorf("reverse feed: source %d table %q has no sharding metadata; a sharded reverse target requires ShardingColumn and HashFunc on every watched table", si, t.TableName)
				}
			}
		}
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	dbCfg := cfg.DBConfig
	if dbCfg == nil {
		dbCfg = dbconn.NewDBConfig()
	}
	flushInt := cfg.FlushInterval
	if flushInt <= 0 {
		flushInt = change.DefaultFlushInterval
	}
	threads := cfg.Threads
	if threads <= 0 {
		threads = 4
	}

	// One applier writing to U, shared across all source feeds. With a sharded
	// U (Targets), the applier routes each row to the shard whose key range
	// contains its hash — the exact mirror of the forward move's fan-out.
	applCfg := &applier.ApplierConfig{
		DBConfig: dbCfg,
		Logger:   logger,
		Threads:  threads,
	}
	var appl applier.Applier
	if len(cfg.Targets) > 0 {
		appl, err = applier.NewShardedApplier(cfg.Targets, applCfg)
	} else {
		appl, err = applier.NewSingleTargetApplier(cfg.Target, applCfg)
	}
	if err != nil {
		return nil, fmt.Errorf("reverse feed: create applier: %w", err)
	}

	rf := &ReverseFeed{
		sources:  cfg.Sources,
		appl:     appl,
		logger:   logger,
		flushInt: flushInt,
		fatalCh:  make(chan struct{}),
	}
	// If wiring the per-source feeds fails partway, tear down the feeds already
	// created so we don't leak their binlog syncer goroutines and connections.
	// The applier is inert until Start (nothing to stop) and does not own
	// Target.DB, so closing the clients is the whole cleanup.
	defer func() {
		if err != nil {
			rf.Close()
		}
	}()

	for si, src := range cfg.Sources {
		if src.DB == nil {
			return nil, fmt.Errorf("reverse feed: source %d DB must be non-nil", si)
		}
		if len(src.Tables) == 0 {
			return nil, fmt.Errorf("reverse feed: source %d has no tables", si)
		}
		clientCfg := &change.ClientConfig{
			Logger:     logger,
			ServerID:   change.NewServerID(),
			DBConfig:   dbCfg,
			CancelFunc: rf.onFatal,
		}
		// The captured start position's own encoding selects the change
		// source implementation: it was captured by targetCurrentPosition in
		// this server's auto-detected coordinate scheme (GTID when the server
		// has GTIDs enabled), and Start hands it back via StartFromPosition,
		// so classifying it here guarantees the round-trip parses. Routing
		// through NewAutoClient (rather than classifying locally) adds the
		// same validation a checkpoint resume gets — a GTID position on a
		// server that lost GTIDs is a purpose-built error now, not a stream
		// failure later — and makes the empty head-start case (Position "",
		// see ReverseSource) probe the server instead of silently defaulting
		// to the file+offset scheme.
		client, cerr := change.NewAutoClient(ctx, src.DB, src.Addr, src.User, src.Password, appl, clientCfg, src.Position)
		if cerr != nil {
			return nil, fmt.Errorf("reverse feed: source %d: %w", si, cerr)
		}
		// Track the client now so the deferred cleanup closes it — and every
		// earlier client — if a later subscription or source fails to wire up.
		rf.clients = append(rf.clients, client)
		for _, sTbl := range src.Tables {
			uTbl, ok := cfg.TargetTables[sTbl.TableName]
			if !ok {
				return nil, fmt.Errorf("reverse feed: source %d has no target table for %q", si, sTbl.TableName)
			}
			chunker, cerr := table.NewChunker(sTbl, table.ChunkerConfig{NewTable: uTbl, Logger: logger})
			if cerr != nil {
				return nil, fmt.Errorf("reverse feed: chunker for %q: %w", sTbl.TableName, cerr)
			}
			if aerr := client.AddSubscription(sTbl, uTbl, chunker); aerr != nil {
				return nil, fmt.Errorf("reverse feed: subscribe %q: %w", sTbl.TableName, aerr)
			}
		}
	}
	return rf, nil
}

// onFatal records the first fatal feed condition and wakes Run. It returns true
// (we always act on it: the window degrades to complete-forward).
func (rf *ReverseFeed) onFatal(reason change.FatalReason) bool {
	rf.fatalOnce.Do(func() {
		rf.fatalMu.Lock()
		rf.fatalErr = fmt.Errorf("reverse feed hit a fatal condition (%s); rollback is no longer safe", reason)
		rf.fatalMu.Unlock()
		rf.logger.Error("reverse feed fatal; source can no longer be trusted for rollback", "reason", reason.String())
		close(rf.fatalCh)
	})
	return true
}

// Err returns the first fatal feed error, if any.
func (rf *ReverseFeed) Err() error {
	rf.fatalMu.Lock()
	defer rf.fatalMu.Unlock()
	return rf.fatalErr
}

// Start opens every feed (StartFromPosition when a Position is set, else from
// current head), switches it to change-only mode, and begins periodic flush.
func (rf *ReverseFeed) Start(ctx context.Context) error {
	for i, client := range rf.clients {
		var err error
		if rf.sources[i].Position != "" {
			err = client.StartFromPosition(ctx, rf.sources[i].Position)
		} else {
			err = client.Start(ctx)
		}
		if err != nil {
			// A partial start must not leave earlier feeds running in the
			// background (leaking goroutines/connections and still applying
			// changes). Close is idempotent, so the caller's own Close is fine.
			rf.Close()
			return fmt.Errorf("reverse feed: start source %d: %w", i, err)
		}
		// Change-only: apply every change regardless of copy watermark, since
		// there is no copier. This is what the forward move disables at cutover.
		if err := client.SetWatermarkOptimization(ctx, false); err != nil {
			rf.Close()
			return fmt.Errorf("reverse feed: disable watermark optimization on source %d: %w", i, err)
		}
		client.StartPeriodicFlush(ctx, rf.flushInt)
	}
	return nil
}

// Flush drains all feeds synchronously (e.g. before a health check or a reverse
// cutover, so U reflects everything written to the sources so far).
func (rf *ReverseFeed) Flush(ctx context.Context) error {
	for i, client := range rf.clients {
		if err := client.Flush(ctx); err != nil {
			return fmt.Errorf("reverse feed: flush source %d: %w", i, err)
		}
	}
	return nil
}

// Positions returns each source's current safe-to-resume position, in the same
// order as the configured sources. Intended for the caller's checkpoint so the
// window can resume in reverse mode after a restart.
func (rf *ReverseFeed) Positions() []string {
	out := make([]string, len(rf.clients))
	for i, client := range rf.clients {
		out[i] = client.Position()
	}
	return out
}

// Run opens the feeds and holds the rollback window for the given duration,
// keeping U current. It returns:
//   - nil when the window elapses normally (after a final flush);
//   - ctx.Err() if the context is cancelled;
//   - a fatal error if any feed dies (rollback is then unsafe and the caller
//     must complete-forward).
//
// Run does not Close the feeds; the caller does that after deciding the
// terminal action (complete-forward or roll back), since a reverse cutover
// needs the feeds flushed one last time first.
func (rf *ReverseFeed) Run(ctx context.Context, window time.Duration) error {
	if err := rf.Start(ctx); err != nil {
		return err
	}
	timer := time.NewTimer(window)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-rf.fatalCh:
		return rf.Err()
	case <-timer.C:
	}
	// Window elapsed normally: final drain so U reflects everything written to
	// the sources during the window before the caller retires it.
	return rf.Flush(ctx)
}

// Close stops periodic flush and closes all feeds. Safe to call more than once.
// The applier is never Started (the subscription apply path is synchronous), so
// there is nothing to Stop on it.
func (rf *ReverseFeed) Close() {
	for _, client := range rf.clients {
		client.StopPeriodicFlush()
		client.Close()
	}
}
