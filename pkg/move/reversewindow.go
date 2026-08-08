package move

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/checkpoint"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/move/check"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
)

// Checkpoint phase values for a reverse-window move. The empty string ("") is
// the copy phase — the only value migration/datasync ever write, and what a
// normal move writes right up to (and including) its cutover.
const (
	phaseReverseWindow = "reverse_window" // forward cutover done, reverse feed live
	phaseReverting     = "reverting"      // reverse cutover under way
)

// reverseWindowPollInterval is how often the window loop checks for a revert
// request, feed death, or the deadline. A var so tests can shorten it.
var reverseWindowPollInterval = 1 * time.Second

// targetCurrentPosition reads target tgt's current head position in the change
// feed's coordinate scheme — matching the mode the reverse feed will use
// (r.move.EnableExperimentalGTID) — so the captured string round-trips through
// StartFromPosition. It uses a short-lived, unstarted Source purely for the
// Source.CurrentPosition read (the applier is unused on that path, hence nil),
// because at cutover the reverse feed itself cannot exist yet: its target-side
// _old tables are created by the rename that immediately follows this hook.
func targetCurrentPosition(ctx context.Context, r *Runner, tgt *applier.Target) (string, error) {
	cfg := change.NewClientDefaultConfig()
	cfg.Logger = r.logger
	cfg.DBConfig = r.dbConfig
	var src change.Source
	if r.move.EnableExperimentalGTID {
		src = change.NewGTIDClient(tgt.DB, tgt.Config.Addr, tgt.Config.User, tgt.Config.Passwd, nil, cfg)
	} else {
		src = change.NewBinlogClient(tgt.DB, tgt.Config.Addr, tgt.Config.User, tgt.Config.Passwd, nil, cfg)
	}
	defer src.Close()
	return src.CurrentPosition(ctx)
}

// captureReverseWindow runs as the cutover postSwitch hook (under the source
// lock, after the traffic switch, before the source rename). It records each
// target's current position — the reverse feeds' start points — and persists
// that the move has entered its reverse window. The background checkpoint
// dumper has already been stopped, so this write is authoritative.
func captureReverseWindow(ctx context.Context, r *Runner) error {
	positions := make(map[string]string, len(r.targets))
	for i := range r.targets {
		pos, err := targetCurrentPosition(ctx, r, &r.targets[i])
		if err != nil {
			return fmt.Errorf("capture reverse-feed start position for target %d: %w", i, err)
		}
		positions[targetKey(r.targets[i])] = pos
	}
	r.reversePositions = positions
	r.cutoverAt = time.Now()

	posJSON, err := json.Marshal(positions)
	if err != nil {
		return fmt.Errorf("marshal reverse positions: %w", err)
	}
	// Persist that the move has entered its reverse window. Pre-flight and
	// pre-cutover already guaranteed no revert marker was present up to here, so
	// any _spirit_move_revert that appears on targets[0] from now on is a genuine
	// operator revert request for THIS window — never dropped as "stale".
	return r.checkpointTbl().Write(ctx, checkpoint.Record{
		Position:  string(posJSON),
		Phase:     phaseReverseWindow,
		CutoverAt: r.cutoverAt,
	})
}

// reverseWindow drives the post-cutover reverse window: it stands up a
// change-only reverse feed (targets → the source's _old tables) so the source
// stays current, then holds until the window elapses (complete forward), a
// revert is requested (reverse cutover), or the feed dies (complete forward —
// rollback is no longer safe). It is a separate type so the Runner's methods
// stay in runner.go.
type reverseWindow struct {
	r    *Runner
	feed *ReverseFeed
	// watched[i] is target i's real-name tables, used to lock and then retire
	// the targets during a reverse cutover.
	watched [][]*table.TableInfo
}

func newReverseWindow(r *Runner) *reverseWindow { return &reverseWindow{r: r} }

// run holds the window and performs the terminal action. It owns the feed's
// lifecycle.
func (w *reverseWindow) run(ctx context.Context) error {
	if err := w.buildFeed(ctx); err != nil {
		return err
	}
	defer w.feed.Close() // idempotent; the terminal actions also close explicitly
	if err := w.feed.Start(ctx); err != nil {
		return fmt.Errorf("reverse window: start feed: %w", err)
	}

	r := w.r
	deadline := r.cutoverAt.Add(r.move.ReverseWindow)
	// Where an operator creates the revert marker to trigger a rollback:
	// targets[0]'s host and database.
	revertLoc := r.targets[0].Config.Addr + "." + r.targets[0].Config.DBName
	r.logger.Info(fmt.Sprintf("reverse window open; watching for a table named %s to be created on %s (create it to roll back)",
		revertMarkerName, revertLoc),
		"window", r.move.ReverseWindow, "deadline", deadline, "reverse_sources", len(r.targets))

	return r.status.Do(status.ReverseWindow, func() error {
		ticker := time.NewTicker(reverseWindowPollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				if ferr := w.feed.Err(); ferr != nil {
					r.logger.Error("reverse feed died mid-window; completing forward (rollback no longer safe)", "error", ferr)
					return w.completeForward(ctx)
				}
				requested, err := w.revertRequested(ctx)
				if err != nil {
					// A transient read error should not abandon the window; log and
					// retry on the next tick.
					r.logger.Warn("could not read revert flag; continuing window", "error", err)
				} else if requested {
					r.logger.Info(fmt.Sprintf("revert triggered: detected a table named %s on %s; rolling back to source",
						revertMarkerName, revertLoc))
					return w.reverseCutover(ctx)
				}
				if !time.Now().Before(deadline) {
					r.logger.Info("reverse window elapsed; completing forward")
					return w.completeForward(ctx)
				}
			}
		}
	})
}

// buildFeed constructs the reverse feed: reverse sources are the former targets
// (watched under their real names); the reverse target is the source, written
// to its _old tables (the forward cutover renamed source real → _old). With a
// sharded source (an N:M move), the reverse target is the set of source shards
// and each row is routed by the SOURCE keyspace's sharding metadata, which is
// attached to the watched tables here.
func (w *reverseWindow) buildFeed(ctx context.Context) error {
	r := w.r
	src := &r.sources[0] // canonical source: table names and the _old TableInfos
	sharded := len(r.sources) > 1

	// The _old mapping TableInfos are built on sources[0]. Their names are
	// unqualified, so with a sharded source the same mapping serves every
	// shard — each shard's own connection determines the database written to
	// (the shards' schemas are identical by the move's own invariant).
	targetTables := make(map[string]*table.TableInfo, len(src.tables))
	for _, t := range src.tables {
		oldName := check.CutoverOldName(t.TableName)
		oldTbl := table.NewTableInfo(src.db, src.config.DBName, oldName)
		if err := oldTbl.SetInfo(ctx); err != nil {
			return fmt.Errorf("reverse window: load renamed source table %q: %w", oldName, err)
		}
		targetTables[t.TableName] = oldTbl
	}

	// Reverse rows are routed to a source shard by the SOURCE keyspace's vindex,
	// so the provider is asked about the source schema (the canonical
	// sources[0]) — NOT the target shard whose binlog carries the row. The
	// answer is per-table, not per-shard, so resolve it once here. No metadata
	// for a moved table means its rows cannot be routed — fail loudly rather
	// than let the window open with an unroutable feed.
	type shardingMeta struct {
		column string
		hash   table.HashFunc
	}
	var sharding map[string]shardingMeta
	if sharded {
		sharding = make(map[string]shardingMeta, len(src.tables))
		for _, t := range src.tables {
			col, hashFn, err := r.move.ReverseShardingProvider.GetShardingMetadata(src.config.DBName, t.TableName)
			if err != nil {
				return fmt.Errorf("reverse window: sharding metadata for table %q: %w", t.TableName, err)
			}
			if col == "" || hashFn == nil {
				return fmt.Errorf("reverse window: no sharding metadata for table %q; a sharded source requires every moved table to have a sharding key to route reverse writes", t.TableName)
			}
			sharding[t.TableName] = shardingMeta{column: col, hash: hashFn}
		}
	}

	sources := make([]ReverseSource, 0, len(r.targets))
	w.watched = make([][]*table.TableInfo, len(r.targets))
	for i := range r.targets {
		tgt := &r.targets[i]
		// The reverse feed MUST resume from the position captured at cutover. A
		// missing/empty entry (e.g. a corrupted or partial checkpoint on resume)
		// would otherwise fall back to the target's current head, silently
		// skipping post-cutover writes and making rollback unsafe — so fail loudly.
		pos, ok := r.reversePositions[targetKey(*tgt)]
		if !ok || pos == "" {
			return fmt.Errorf("reverse window: no captured start position for target %d (%s); refusing to start the reverse feed, which would miss post-cutover writes and make rollback unsafe", i, targetKey(*tgt))
		}
		watched := make([]*table.TableInfo, 0, len(src.tables))
		for _, t := range src.tables {
			wt := table.NewTableInfo(tgt.DB, tgt.Config.DBName, t.TableName)
			if err := wt.SetInfo(ctx); err != nil {
				return fmt.Errorf("reverse window: load target table %q on shard %d: %w", t.TableName, i, err)
			}
			if sharded {
				wt.ShardingColumn = sharding[t.TableName].column
				wt.HashFunc = sharding[t.TableName].hash
			}
			watched = append(watched, wt)
		}
		w.watched[i] = watched
		sources = append(sources, ReverseSource{
			DB:       tgt.DB,
			Addr:     tgt.Config.Addr,
			User:     tgt.Config.User,
			Password: tgt.Config.Passwd,
			Tables:   watched,
			Position: pos,
		})
	}

	cfg := ReverseFeedConfig{
		Sources:      sources,
		TargetTables: targetTables,
		Logger:       r.logger,
		DBConfig:     r.dbConfig,
		Threads:      r.move.WriteThreads,
		GTID:         r.move.EnableExperimentalGTID,
	}
	if sharded {
		revTargets := make([]applier.Target, len(r.sources))
		for i := range r.sources {
			revTargets[i] = applier.Target{
				DB:       r.sources[i].db,
				Config:   r.sources[i].config,
				KeyRange: r.sources[i].keyRange,
			}
		}
		cfg.Targets = revTargets
	} else {
		cfg.Target = applier.Target{DB: src.db}
	}

	feed, err := NewReverseFeed(cfg)
	if err != nil {
		return err
	}
	w.feed = feed
	return nil
}

func (w *reverseWindow) revertRequested(ctx context.Context) (bool, error) {
	return revertMarkerExists(ctx, w.r.targets[0].DB)
}

// completeForward reaches the same terminal state as a normal move: the source
// tables are already renamed to _old (left in place, as after any move) and the
// checkpoint is dropped. The reverse feed is stopped.
func (w *reverseWindow) completeForward(ctx context.Context) error {
	w.feed.Close()
	if err := dropRevertMarker(ctx, w.r.targets[0].DB); err != nil {
		return fmt.Errorf("reverse window: drop revert marker on complete-forward: %w", err)
	}
	if err := w.r.checkpointTbl().Drop(ctx); err != nil {
		return fmt.Errorf("reverse window: drop checkpoint on complete-forward: %w", err)
	}
	w.r.logger.Info("reverse window complete; move finalized forward (source retired)")
	return nil
}

// reverseCutover rolls the move back to the source. It mirrors the forward
// cutover with roles swapped, plus one asymmetry: the source tables sit under
// their _old names (on every source shard) and must be un-retired before
// traffic returns to them.
func (w *reverseWindow) reverseCutover(ctx context.Context) error {
	r := w.r

	// Record that we are rolling back (best-effort; for a future resume).
	if err := r.checkpointTbl().Write(ctx, checkpoint.Record{Phase: phaseReverting, CutoverAt: r.cutoverAt}); err != nil {
		r.logger.Warn("could not persist reverting phase; continuing", "error", err)
	}

	// Clear any stale _revert tables on the targets before the retire (step 5)
	// renames each target table to its _revert form — a leftover from a prior
	// reverse cutover would otherwise collide. Done before locking, since the
	// leftovers are not in the lock set (DROP under LOCK TABLES is disallowed for
	// unlocked tables).
	if err := r.dropStaleRevertTables(ctx); err != nil {
		return err
	}

	// 1. Lock the reverse sources (former targets) to freeze their writes.
	var locks []*dbconn.TableLock
	closeLocks := func() {
		for _, l := range locks {
			utils.CloseAndLogWithContext(ctx, l)
		}
	}
	for i := range r.targets {
		lock, err := dbconn.NewTableLock(ctx, r.targets[i].DB, w.watched[i], r.dbConfig, r.logger)
		if err != nil {
			closeLocks()
			return fmt.Errorf("reverse cutover: lock target %d: %w", i, err)
		}
		locks = append(locks, lock)
	}
	defer closeLocks()

	// 2. Flush the reverse feed so the source's _old tables reflect every target
	//    write, then stop it (no more writes to _old during the renames below).
	if err := w.feed.Flush(ctx); err != nil {
		return fmt.Errorf("reverse cutover: final flush: %w", err)
	}
	w.feed.Close()

	// 3. Un-retire the source: rename its _old tables back to their real names
	//    (on every source shard) so it can serve again. The feed is stopped, so
	//    nothing writes them.
	for si := range r.sources {
		s := &r.sources[si]
		for _, t := range r.sourceTables {
			oldName := check.CutoverOldName(t.TableName)
			if err := dbconn.Exec(ctx, s.db, "RENAME TABLE %n TO %n", oldName, t.TableName); err != nil {
				return fmt.Errorf("reverse cutover: un-retire source %d table %q: %w", si, t.TableName, err)
			}
		}
	}

	// 4. Switch traffic back to the source.
	if r.reverseCutoverFunc != nil {
		if err := r.reverseCutoverFunc(ctx); err != nil {
			return fmt.Errorf("reverse cutover: traffic switch failed: %w", err)
		}
	}

	// 5. Retire the former targets to their _revert form under their lock —
	//    fencing straggler writes after the switch, mirroring the forward
	//    cutover's source rename. _revert (not _old) marks these as revert
	//    artifacts, so a later move can safely drop them.
	for i := range r.targets {
		for _, t := range r.sourceTables {
			revertName := check.RevertRetiredName(t.TableName)
			stmt := sqlescape.MustEscapeSQL("RENAME TABLE %n TO %n", t.TableName, revertName)
			if err := locks[i].ExecUnderLock(ctx, stmt); err != nil {
				return fmt.Errorf("reverse cutover: retire target %d table %q: %w", i, t.TableName, err)
			}
		}
	}

	// 6. Drop the revert marker and the checkpoint.
	if err := dropRevertMarker(ctx, r.targets[0].DB); err != nil {
		return fmt.Errorf("reverse cutover: drop revert marker: %w", err)
	}
	if err := r.checkpointTbl().Drop(ctx); err != nil {
		return fmt.Errorf("reverse cutover: drop checkpoint: %w", err)
	}
	r.logger.Info("reverse cutover complete; move rolled back to source")
	return nil
}
