package move

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/contract"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"golang.org/x/sync/errgroup"
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
	// Position is the opaque change.Source position to resume from: the one
	// captured at cutover, or on a resumed window the feed's flushed position
	// as last checkpointed during the window (see
	// reverseWindow.checkpointPositions). Empty means start from the source's
	// current head. Its encoding
	// also selects the change-source coordinate scheme, exactly like a
	// checkpoint resume (see change.NewAutoClient): a GTID set resumes through
	// the GTID client (and requires the server to still have GTIDs enabled), a
	// file:offset position through the binlog client, and the empty head-start
	// case probes the server so the scheme matches what a cutover capture on
	// that server would have produced.
	Position string
}

// DefaultReverseFeedMemoryBudget is the total, across every subscription of a
// reverse feed, of the change feed's per-subscription soft limits: 4 GiB.
//
// The change feed bounds each subscription (one per watched table per source)
// separately, at change.DefaultSubscriptionSoftLimitBytes. A reverse feed has
// one change client per move target, each subscribing to every moved table, so
// with per-subscription defaults its ceiling grows as targets × tables: a move
// with 128 target shards and 17 tables has 2,176 subscriptions, a ceiling of
// over 500 GiB, and the reverse feed's catch-up after a resume, when every
// client is behind its server at once, is exactly when buffers fill. The
// budget is divided evenly across the subscriptions instead (see
// reverseSubscriptionSoftLimitBytes).
//
// 4 GiB is 16 × change.DefaultSubscriptionSoftLimitBytes (256 MiB): a move
// with up to 16 subscriptions keeps exactly the buffering it had without a
// budget, and a larger one is bounded at a figure that leaves room for the
// rest of the process in an 8–16 GiB container.
const DefaultReverseFeedMemoryBudget int64 = 4 << 30

// minReverseSubscriptionSoftLimitBytes is the smallest per-subscription soft
// limit a budget is divided into. Below it the reader would park every few
// hundred changes, and each park costs a flush round trip. When the floor
// binds, the feed's ceiling is subscriptions × this floor, above the budget;
// NewReverseFeed logs it.
const minReverseSubscriptionSoftLimitBytes int64 = 1 << 20

// DefaultReverseFeedEventCacheCount is the number of parsed binlog events each
// reverse-feed client's streamer may hold ahead of the reader (see
// change.ClientConfig.EventCacheCount). go-mysql's default, 10240, is per
// client, so it too scales with the number of targets — and those events are
// held outside the subscription soft limits, filling while the reader is
// behind or parked. 256 events still keeps the network reader well ahead of
// the row decoder.
const DefaultReverseFeedEventCacheCount = 256

// reverseSubscriptionSoftLimitBytes divides a reverse feed's total memory
// budget evenly across its subscriptions, returning the per-subscription soft
// limit to set as change.ClientConfig.SubscriptionSoftLimitBytes.
//
// budget is ReverseFeedConfig.MemoryBudget: zero selects
// DefaultReverseFeedMemoryBudget, and a negative value opts out of the total
// budget, returning 0 so the change feed applies its own per-subscription
// default. The share is clamped to at most that default — a budget never
// raises a subscription's limit above what it would have without one — and to
// at least minReverseSubscriptionSoftLimitBytes.
func reverseSubscriptionSoftLimitBytes(budget int64, subscriptions int) int64 {
	if budget < 0 {
		return 0
	}
	if budget == 0 {
		budget = DefaultReverseFeedMemoryBudget
	}
	share := budget / int64(max(subscriptions, 1))
	return min(max(share, minReverseSubscriptionSoftLimitBytes), change.DefaultSubscriptionSoftLimitBytes)
}

// reverseFeedLimits are a ReverseFeedConfig's memory settings, resolved to
// what every source's change.ClientConfig is given.
type reverseFeedLimits struct {
	budget          int64 // the resolved total budget; meaningless when softLimitBytes is 0
	subscriptions   int   // across every source
	softLimitBytes  int64 // change.ClientConfig.SubscriptionSoftLimitBytes; 0 = the change feed's default
	eventCacheCount int   // change.ClientConfig.EventCacheCount; 0 = go-mysql's default
}

// resolveReverseFeedLimits resolves cfg's MemoryBudget and EventCacheCount.
// Every source subscribes to its own tables, so the subscriptions the budget
// is divided across are the sum over the sources.
func resolveReverseFeedLimits(cfg ReverseFeedConfig) reverseFeedLimits {
	l := reverseFeedLimits{budget: cmp.Or(cfg.MemoryBudget, DefaultReverseFeedMemoryBudget)}
	for _, src := range cfg.Sources {
		l.subscriptions += len(src.Tables)
	}
	l.softLimitBytes = reverseSubscriptionSoftLimitBytes(cfg.MemoryBudget, l.subscriptions)
	switch {
	case cfg.EventCacheCount == 0:
		l.eventCacheCount = DefaultReverseFeedEventCacheCount
	case cfg.EventCacheCount > 0:
		l.eventCacheCount = cfg.EventCacheCount
	}
	return l
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

	// MemoryBudget is the total, in bytes, of the soft limits of every
	// subscription across every source: each subscription's limit is this
	// divided by the number of subscriptions (see
	// reverseSubscriptionSoftLimitBytes). 0 => DefaultReverseFeedMemoryBudget;
	// negative => no total budget, each subscription at the change feed's own
	// per-subscription default.
	//
	// The limit is soft in the change feed's sense: a subscription at its
	// limit parks the binlog reader of its source until a flush applies part
	// of the buffer, so a small budget slows the feed but loses no change.
	// The streamers' parsed-event caches (EventCacheCount) are held outside
	// it.
	MemoryBudget int64
	// EventCacheCount is each source client's streamer event cache (see
	// change.ClientConfig.EventCacheCount). 0 =>
	// DefaultReverseFeedEventCacheCount; negative => go-mysql's default.
	EventCacheCount int
}

// ReverseFeed is a running change-only reverse feed: one change.Source per
// ReverseSource, all sharing a single applier that writes to U (mirrors the
// forward move, where N sources share one applier).
type ReverseFeed struct {
	sources  []ReverseSource
	clients  []change.Source
	logger   *slog.Logger
	flushInt time.Duration

	// A fatal condition on any feed stops it, and U stops receiving the
	// targets' writes. We record it so the caller, which polls Err and
	// Restartable, can act on it rather than silently letting U drift: restart
	// the feed from its last checkpointed position when the condition left the
	// watched tables unchanged (a failed flush or stream), or degrade to
	// complete-forward when it did not. Several feeds (or one feed, more than
	// once) can report: the first reason is kept unless a later one is not
	// restartable, which always wins — one unsafe condition anywhere makes the
	// whole feed unsafe to restart.
	fatalMu     sync.Mutex
	fatalErr    error
	fatalReason contract.FatalReason
}

// NewReverseFeed wires the feeds and their shared applier. It does not open any
// binlog stream — call Start for that — but it does query each source
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
	targets := cfg.Targets
	if len(targets) == 0 {
		targets = []applier.Target{cfg.Target}
	}
	appl, err := applier.New(targets, applCfg)
	if err != nil {
		return nil, fmt.Errorf("reverse feed: create applier: %w", err)
	}

	limits := resolveReverseFeedLimits(cfg)
	if limits.softLimitBytes > 0 {
		logger.Info("reverse feed memory budget",
			"budget_bytes", limits.budget,
			"subscriptions", limits.subscriptions,
			"subscription_soft_limit_bytes", limits.softLimitBytes,
			"event_cache_count", limits.eventCacheCount)
		if ceiling := limits.softLimitBytes * int64(limits.subscriptions); ceiling > limits.budget {
			logger.Warn("reverse feed memory budget is below the per-subscription minimum; buffers may exceed it",
				"budget_bytes", limits.budget,
				"subscriptions", limits.subscriptions,
				"subscription_soft_limit_bytes", limits.softLimitBytes,
				"ceiling_bytes", ceiling)
		}
	}

	rf := &ReverseFeed{
		sources:  cfg.Sources,
		logger:   logger,
		flushInt: flushInt,
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
			// The feed's share of the memory budget: zero (a negative
			// budget) leaves the change feed's per-subscription default.
			SubscriptionSoftLimitBytes: limits.softLimitBytes,
			EventCacheCount:            limits.eventCacheCount,
			// Heartbeats are how the progress log tells a quiet target from
			// a stream that has stopped delivering; see reverseSourceIdle.
			HeartbeatPeriod: reverseFeedHeartbeatPeriod,
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

// onFatal records a fatal feed condition for Err and Restartable. The first
// condition is kept unless a later one is not restartable: an unsafe condition
// (a schema change, XA, a wrapped LogPos) replaces an earlier restartable one,
// and is never replaced itself, so a flush error on one feed cannot hide a
// schema change on another. It returns true: the feed stops either way, and the
// window decides whether to restart it or complete forward.
func (rf *ReverseFeed) onFatal(reason contract.FatalReason) bool {
	restartable := reason.PreservesCheckpoint()
	rf.fatalMu.Lock()
	record := rf.fatalErr == nil || (!restartable && rf.fatalReason.PreservesCheckpoint())
	if record {
		rf.fatalReason = reason
		if restartable {
			rf.fatalErr = fmt.Errorf("reverse feed stopped on a fatal condition (%s); it can be restarted from its last checkpointed position", reason)
		} else {
			rf.fatalErr = fmt.Errorf("reverse feed hit a fatal condition (%s); rollback is no longer safe", reason)
		}
	}
	rf.fatalMu.Unlock()
	if record {
		rf.logger.Error("reverse feed fatal", "reason", reason.String(), "restartable", restartable)
	}
	return true
}

// Err returns the recorded fatal feed error, if any (see onFatal for which one).
func (rf *ReverseFeed) Err() error {
	rf.fatalMu.Lock()
	defer rf.fatalMu.Unlock()
	return rf.fatalErr
}

// Restartable reports whether the feed stopped on a fatal condition that left
// the watched tables unchanged — a failed flush (after the applier's own
// retries: for example the reverse target refusing new connections) or a failed
// stream. Such a condition does not make the source untrustworthy: the failed
// changes were not applied, the feed's flushed position did not advance past
// them, and a new feed started from the last checkpointed position replays
// them (the apply is idempotent). It is false while the feed is healthy, and
// for a schema change, XA or a wrapped LogPos, after which rollback is no
// longer safe.
func (rf *ReverseFeed) Restartable() bool {
	rf.fatalMu.Lock()
	defer rf.fatalMu.Unlock()
	return rf.fatalErr != nil && rf.fatalReason.PreservesCheckpoint()
}

// Start opens every feed (StartFromPosition when a Position is set, else from
// current head), switches it to change-only mode, and begins periodic flush.
func (rf *ReverseFeed) Start(ctx context.Context) error {
	// Start opens the sources one after another, and each open is a few round
	// trips to its server, so with many sources it takes minutes. It does not
	// wait for any source to catch up: that happens after it returns, and the
	// reverse window's progress log reports it.
	started := time.Now()
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
	rf.logger.Info("reverse feed started; catching up from each source's start position",
		"sources", len(rf.clients), "took", time.Since(started).Round(time.Millisecond).String())
	return nil
}

// Flush drains all feeds synchronously (e.g. before a health check or a reverse
// cutover, so U reflects everything written to the sources so far). The feeds
// are drained at the same time, as their periodic flushes already run through
// the shared applier at the same time.
func (rf *ReverseFeed) Flush(ctx context.Context) error {
	g, gctx := errgroup.WithContext(ctx)
	for i, client := range rf.clients {
		g.Go(func() error {
			if err := client.Flush(gctx); err != nil {
				return fmt.Errorf("reverse feed: flush source %d: %w", i, err)
			}
			return nil
		})
	}
	return g.Wait()
}

// PausePeriodicFlush stops every feed's periodic flush and waits for any
// periodic flush in progress to end. A reverse cutover calls it before it
// locks the targets: a flush of its own taken under the locks would otherwise
// wait, without regard to its context, for a periodic flush already running.
func (rf *ReverseFeed) PausePeriodicFlush() {
	for _, client := range rf.clients {
		client.StopPeriodicFlush()
	}
}

// ResumePeriodicFlush restarts the periodic flush PausePeriodicFlush stopped.
// It is a no-op on a closed feed.
func (rf *ReverseFeed) ResumePeriodicFlush(ctx context.Context) {
	for _, client := range rf.clients {
		client.StartPeriodicFlush(ctx, rf.flushInt)
	}
}

// AllChangesFlushed reports whether every feed has applied its whole buffer.
//
// Flush returning nil is not the same question, and callers that are about to
// discard the buffer must ask this one instead. A drain may decline to finish —
// lock contention it could not resolve in its budget, or a drain cut short to
// bound how long it holds the flush mutex — and reports that by leaving the
// changes buffered rather than by erroring, because for the periodic flusher
// "try again next tick" is the right response and failing the whole change is
// not. Flush's own loop compounds it: it exits once the backlog is merely
// *trivial*, not empty, so a residual below that threshold returns nil by
// design.
//
// Mirrors the forward cutover's check in cutover.go, which pairs the two calls
// for exactly this reason.
func (rf *ReverseFeed) AllChangesFlushed() bool {
	for _, client := range rf.clients {
		if !client.AllChangesFlushed() {
			return false
		}
	}
	return true
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

// sourceProgress returns each source's progress, in the same order as the configured
// sources, for the reverse window's periodic progress log. It does no server
// I/O: every figure is the feed's in-memory state.
func (rf *ReverseFeed) sourceProgress() []reverseSourceProgress {
	out := make([]reverseSourceProgress, len(rf.clients))
	for i, client := range rf.clients {
		out[i] = reverseSourceProgress{Stats: client.FeedStats(), Pending: client.GetDeltaLen()}
	}
	return out
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
