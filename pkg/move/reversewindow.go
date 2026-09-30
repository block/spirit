package move

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
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
	phaseReverting     = "reverting"      // reverse cutover crossed its first ownership-moving rename
	// phaseReverseFinalized means every former target has been retired, so
	// ownership is definitively back on the source. Only idempotent cleanup
	// (drop the revert marker, drop the checkpoint) can still be outstanding,
	// which is what makes a resume from this phase safe to retry.
	phaseReverseFinalized = "reverse_finalized"
)

// reverseWindowPollInterval is how often the window loop checks for a revert
// request, feed death, or the deadline. A var so tests can shorten it.
var reverseWindowPollInterval = 1 * time.Second

// reverseFeedFlushInterval is the reverse feed's periodic flush interval. A var
// so tests can shorten it.
var reverseFeedFlushInterval = change.DefaultFlushInterval

// targetCurrentPosition reads target tgt's current head position in the change
// feed's coordinate scheme — auto-detected from that target server (GTIDs when
// it has them enabled), the same way NewReverseFeed later classifies the
// captured string, so it round-trips through StartFromPosition. It uses a
// short-lived, unstarted Source purely for the Source.CurrentPosition read
// (the applier is unused on that path, hence nil), because at cutover the
// reverse feed itself cannot exist yet: its target-side _old tables are
// created by the rename that immediately follows this hook.
func targetCurrentPosition(ctx context.Context, r *Runner, tgt *applier.Target) (string, error) {
	cfg := change.NewClientDefaultConfig()
	cfg.Logger = r.logger
	cfg.DBConfig = r.dbConfig
	src, err := change.NewAutoClient(ctx, tgt.DB, tgt.Config.Addr, tgt.Config.User, tgt.Config.Passwd, nil, cfg, "")
	if err != nil {
		return "", err
	}
	defer src.Close()
	return src.CurrentPosition(ctx)
}

// captureReverseWindow runs under the source locks after the final forward
// flush, before switching traffic. They are the reverse feeds' start points,
// so writes committed during the switch are included when those feeds start.
// On a safe cutover retry they are recaptured after the new final flush.
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
	return nil
}

// persistReverseWindow runs after the traffic switch and before retiring the
// source. The background checkpoint dumper is already stopped, so this write
// is authoritative. The window duration starts when the switch completes.
func persistReverseWindow(ctx context.Context, r *Runner) error {
	r.cutoverAt = time.Now()
	posJSON, err := json.Marshal(r.reversePositions)
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
	// persistPhase, dropMarker and dropCheckpoint are the durable side effects
	// of a reverse cutover, injectable so the ownership-boundary and
	// finalize-retry behavior can be tested without a live topology.
	persistPhase   func(ctx context.Context, phase string) error
	dropMarker     func(ctx context.Context) error
	dropCheckpoint func(ctx context.Context) error
	// writeCheckpoint persists the window's periodic position checkpoint,
	// injectable so the loop's handling of a failed write can be tested
	// without a live topology.
	writeCheckpoint func(ctx context.Context, rec checkpoint.Record) error
}

func newReverseWindow(r *Runner) *reverseWindow {
	return &reverseWindow{
		r: r,
		persistPhase: func(ctx context.Context, phase string) error {
			return r.checkpointTbl().Write(ctx, checkpoint.Record{Phase: phase, CutoverAt: r.cutoverAt})
		},
		dropMarker:     func(ctx context.Context) error { return dropRevertMarker(ctx, r.targets[0].DB) },
		dropCheckpoint: func(ctx context.Context) error { return r.checkpointTbl().Drop(ctx) },
		writeCheckpoint: func(ctx context.Context, rec checkpoint.Record) error {
			return r.checkpointTbl().Write(ctx, rec)
		},
	}
}

// run holds the window and performs the terminal action. It owns the feed's
// lifecycle.
func (w *reverseWindow) run(ctx context.Context) error {
	// Both the fresh cutover and a resume enter the window here, and neither
	// runs a check scope on the way. The reverse feeds write to the retired
	// _old tables, and a reverse cutover puts them back into service, so a
	// trigger or event in the source schema that can write to them is
	// refused before the feeds start (see
	// check.ReverseWindowSchemaObjectsError). Traffic stays on the target, and
	// a re-run resumes the window once the objects are dropped.
	if err := w.checkSourceSchemaObjects(ctx); err != nil {
		return fmt.Errorf("reverse window: %w", err)
	}
	if err := w.buildFeed(ctx); err != nil {
		return err
	}
	defer w.feed.Close() // idempotent; the terminal actions also close explicitly
	if err := w.feed.Start(ctx); err != nil {
		return fmt.Errorf("reverse window: start feed: %w", err)
	}
	return w.hold(ctx)
}

// hold runs the window loop over a started feed until the window elapses, a
// revert is requested, the feed dies, or ctx is cancelled, and performs the
// terminal action.
func (w *reverseWindow) hold(ctx context.Context) error {
	r := w.r
	deadline := r.cutoverAt.Add(r.move.ReverseWindow)
	// Where an operator creates the revert marker to trigger a rollback:
	// targets[0]'s host and database.
	revertLoc := r.targets[0].Config.Addr + "." + r.targets[0].Config.DBName
	r.logger.Info(fmt.Sprintf("reverse window open; watching for a table named %s to be created on %s (create it to roll back)",
		revertMarkerName, revertLoc),
		"window", r.move.ReverseWindow, "deadline", deadline, "reverse_sources", len(r.targets))

	return r.status.DoContext(ctx, status.ReverseWindow, func() error {
		ticker := time.NewTicker(reverseWindowPollInterval)
		defer ticker.Stop()
		checkpointTicker := time.NewTicker(status.CheckpointDumpInterval)
		defer checkpointTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-checkpointTicker.C:
				err := w.checkpointPositions(ctx)
				if err == nil {
					continue
				}
				if ctx.Err() != nil {
					// Same as the forward dumper on shutdown: the write was cut
					// short because we are stopping, so stop.
					if errors.Is(err, checkpoint.ErrWriteAbandoned) {
						r.logger.Warn("checkpoint write abandoned during shutdown", "error", err)
					}
					return ctx.Err()
				}
				if !errors.Is(err, checkpoint.ErrWriteNotSent) &&
					(errors.Is(err, checkpoint.ErrWriteAbandoned) || dbconn.IsOutcomeUnknown(err)) {
					// The REPLACE reached the server and its outcome is unknown:
					// Write abandoned it, or the connection was lost after it
					// was sent. It may still commit, and would then overwrite a
					// terminal action taken from here on (the reverse cutover's
					// phase writes, complete-forward's checkpoint drop), so end
					// the window instead, as the forward dumper aborts the move
					// on a failed write. Every write in this phase, the late one
					// included, records phase reverse_window, so a re-run
					// resumes the window.
					return status.FatalAbort(fmt.Errorf("reverse window: %w", err))
				}
				// The write failed without leaving a REPLACE that can commit
				// later: it was never sent (ErrWriteNotSent, even on a lost
				// connection), or the server answered with an error. The row
				// still holds an earlier position the feeds can resume from,
				// and ending the window over it would stop keeping the source
				// current, so retry on the next tick.
				r.logger.Warn("could not checkpoint reverse feed positions; will retry", "error", err)
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

// checkpointPositions persists the reverse feeds' flushed positions, so a
// restart resumes each feed from what it had already applied rather than from
// the cutover. Without it the checkpoint keeps the cutover positions for the
// whole window: a restart re-reads the window's binlog, and cannot resume at
// all once a target has purged it. It runs on the window loop's goroutine, so
// it cannot interleave with a reverse cutover's phase writes or a
// complete-forward's checkpoint drop.
func (w *reverseWindow) checkpointPositions(ctx context.Context) error {
	r := w.r
	if w.feed.Err() != nil {
		return nil // the window is about to complete forward; leave the row alone
	}
	feedPositions := w.feed.Positions()
	positions := make(map[string]string, len(feedPositions))
	for i, pos := range feedPositions {
		if pos == "" {
			return nil // nothing resumable observed yet; keep the previous row
		}
		positions[targetKey(r.targets[i])] = pos
	}
	if maps.Equal(positions, r.reversePositions) {
		return nil
	}
	posJSON, err := json.Marshal(positions)
	if err != nil {
		return fmt.Errorf("marshal reverse positions: %w", err)
	}
	if err := w.writeCheckpoint(ctx, checkpoint.Record{
		Position:  string(posJSON),
		Phase:     phaseReverseWindow,
		CutoverAt: r.cutoverAt,
	}); err != nil {
		return err
	}
	r.reversePositions = positions
	return nil
}

// checkSourceSchemaObjects refuses when a source schema holds a trigger or an
// event. See check.ReverseWindowSchemaObjectsError.
func (w *reverseWindow) checkSourceSchemaObjects(ctx context.Context) error {
	return check.ReverseWindowSchemaObjectsError(ctx, w.r.checkResources().Sources)
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
		// The reverse feed MUST resume from the position captured at cutover, or
		// from a later one checkpointed during the window. A missing/empty entry
		// (e.g. a corrupted or partial checkpoint on resume) would otherwise fall
		// back to the target's current head, silently skipping post-cutover
		// writes and making rollback unsafe — so fail loudly.
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
		Sources:       sources,
		TargetTables:  targetTables,
		Logger:        r.logger,
		DBConfig:      r.dbConfig,
		Threads:       r.reverseWriteThreads,
		FlushInterval: reverseFeedFlushInterval,
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

	feed, err := NewReverseFeed(ctx, cfg)
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
	if err := w.dropMarker(ctx); err != nil {
		return fmt.Errorf("reverse window: drop revert marker on complete-forward: %w", err)
	}
	if err := w.dropCheckpoint(ctx); err != nil {
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
	//
	// Both calls, in this order, and neither is optional. Close discards the
	// buffer rather than flushing it, and the renames below put the source's
	// _old tables back into service — so a change still buffered here is a
	// target-era write that is silently lost. A nil error from Flush does not
	// rule that out: a drain can decline to finish and report it by leaving the
	// changes buffered, and Flush's loop exits on a *trivial* backlog rather
	// than an empty one. AllChangesFlushed is the question that matters, and
	// asking it is what the forward cutover already does (cutover.go).
	if err := w.feed.Flush(ctx); err != nil {
		return fmt.Errorf("reverse cutover: final flush: %w", err)
	}
	if !w.feed.AllChangesFlushed() {
		return fmt.Errorf("reverse cutover: %w; refusing to discard buffered target writes",
			change.ErrChangesNotFlushed)
	}
	w.feed.Close()

	// Check the source schema one last time before the _old tables go back
	// into service: the reverse feed is drained, and a trigger or event
	// created during the window could have written to them outside it, or (a
	// trigger on an _old table) would go live with them. Views, procedures
	// and functions are not refused (see
	// check.ReverseWindowSchemaObjectsError). This path holds no lock on the
	// source tables, so it does not keep DDL out while it runs. Fail closed:
	// nothing has moved ownership yet, the phase is still reverse_window, and
	// a re-run resumes the window once the objects are dropped.
	if err := w.checkSourceSchemaObjects(ctx); err != nil {
		return fmt.Errorf("reverse cutover: %w", err)
	}

	// The mirror of the forward cutover's carryAutoIncrementsToTargets: ids the
	// targets issued during the window and have since deleted never reached
	// the source's _old tables, so without this the source would issue them
	// again once traffic returns. The targets are locked and the feed is
	// drained, so neither side's counter can move now.
	if err := w.carryAutoIncrementsToSource(ctx); err != nil {
		return fmt.Errorf("reverse cutover: %w", err)
	}

	// Persist the ownership boundary immediately before the first rename that
	// moves ownership, and fail closed if it cannot be written. Everything up
	// to here is reversible; from the next statement on, a crash leaves the
	// move half-rolled-back, and a resume that could not read phaseReverting
	// would restart the window as though traffic were still on the target.
	if err := w.persistPhase(ctx, phaseReverting); err != nil {
		return fmt.Errorf("reverse cutover: persist reverting phase: %w", err)
	}

	// 3. Un-retire the source: rename its _old tables back to their real names
	//    (on every source shard) so it can serve again. The feed is stopped, so
	//    nothing writes them. The renames move ownership, so a started rename
	//    runs to completion even if ctx is cancelled: a cancelled one could
	//    still commit on the server, and the client could not tell.
	for si := range r.sources {
		s := &r.sources[si]
		for _, t := range r.sourceTables {
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("reverse cutover: un-retire source %d table %q: %w", si, t.TableName, err)
			}
			if err := w.unretireSourceTable(ctx, s.db, t.TableName); err != nil {
				return fmt.Errorf("reverse cutover: un-retire source %d table %q: %w", si, t.TableName, err)
			}
			r.durableMutation.Store(true)
		}
	}

	// 4. Switch traffic back to the source.
	if err := w.runReverseCutoverCallback(ctx); err != nil {
		return err
	}

	// 5. Retire the former targets to their _revert form under their lock —
	//    fencing straggler writes after the switch, mirroring the forward
	//    cutover's source rename. _revert (not _old) marks these as revert
	//    artifacts, so a later move can safely drop them. Traffic is back on
	//    the source, so the renames run even if ctx is cancelled
	//    (context.WithoutCancel): a stopped one would leave a target serving.
	for i := range r.targets {
		for _, t := range r.sourceTables {
			revertName := check.RevertRetiredName(t.TableName)
			stmt := sqlescape.MustEscapeSQL("RENAME TABLE %n TO %n", t.TableName, revertName)
			if err := locks[i].ExecUnderLock(context.WithoutCancel(ctx), stmt); err != nil {
				if dbconn.IsOutcomeUnknown(err) {
					return fmt.Errorf("%w: reverse cutover: retire target %d table %q, outcome unknown: %w",
						status.ErrOwnershipAmbiguous, i, t.TableName, err)
				}
				return fmt.Errorf("reverse cutover: retire target %d table %q: %w", i, t.TableName, err)
			}
		}
	}

	// Ownership is back on the source. Record it even if ctx is cancelled: a
	// cancelled write would leave the checkpoint at phaseReverting, and the
	// next run would treat the finished rollback as ownership-ambiguous.
	finalizeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), postCutoverCleanupTimeout)
	defer cancel()
	return w.finalizeReverse(finalizeCtx)
}

// unretireSourceTable renames one source table from its _old name back to its
// real name. The rename runs on a context detached from ctx's cancellation and
// bounded by DBConfig.StatementCompletionTimeout. A lost connection or an
// expired bound leaves its outcome unknown, reported as ErrOwnershipAmbiguous.
func (w *reverseWindow) unretireSourceTable(ctx context.Context, db *sql.DB, tableName string) error {
	renameCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), w.r.dbConfig.StatementCompletionTimeout())
	defer cancel()
	err := dbconn.Exec(renameCtx, db, "RENAME TABLE %n TO %n", check.CutoverOldName(tableName), tableName)
	if err != nil && (renameCtx.Err() != nil || dbconn.IsOutcomeUnknown(err)) {
		return fmt.Errorf("%w: outcome unknown: %w", status.ErrOwnershipAmbiguous, err)
	}
	return err
}

// carryAutoIncrementsToSource raises each of the source's retired (_old)
// tables' AUTO_INCREMENT counters to at least the highest counter among the
// targets. See carryAutoIncrement.
func (w *reverseWindow) carryAutoIncrementsToSource(ctx context.Context) error {
	r := w.r
	for _, t := range r.sourceTables {
		from := make([]autoIncrementTable, len(r.targets))
		for i, target := range r.targets {
			from[i] = autoIncrementTable{db: target.DB, schema: target.Config.DBName, name: t.TableName}
		}
		to := make([]autoIncrementTable, len(r.sources))
		for i, src := range r.sources {
			to[i] = autoIncrementTable{db: src.db, schema: src.config.DBName, name: check.CutoverOldName(t.TableName)}
		}
		if err := carryAutoIncrement(ctx, r.logger, from, to); err != nil {
			return fmt.Errorf("carry AUTO_INCREMENT of %s back to the source: %w", t.TableName, err)
		}
	}
	return nil
}

func (w *reverseWindow) runReverseCutoverCallback(ctx context.Context) error {
	r := w.r
	var result CutoverResult
	var err error
	switch {
	case r.reverseCutoverResultFunc != nil:
		result, err = r.reverseCutoverResultFunc(ctx)
	case r.reverseCutoverFunc != nil:
		err = r.reverseCutoverFunc(ctx)
		if err != nil {
			result.OwnershipAmbiguous = true
		}
	}
	if result.DurableMutation {
		r.durableMutation.Store(true)
	}
	if result.OwnershipAmbiguous {
		r.terminalOwnership.Store(uint32(status.WorkflowTerminalOwnershipAmbiguous))
	}
	if err == nil && result.OwnershipAmbiguous {
		err = status.ErrOwnershipAmbiguous
	}
	if err == nil {
		return nil
	}
	switch {
	case result.DurableMutation && result.OwnershipAmbiguous:
		return errors.Join(
			status.ErrDurableMutation,
			fmt.Errorf("%w: reverse cutover traffic switch failed: %w", status.ErrOwnershipAmbiguous, err),
		)
	case result.DurableMutation:
		return fmt.Errorf("%w: reverse cutover traffic switch failed: %w", status.ErrDurableMutation, err)
	case result.OwnershipAmbiguous:
		return fmt.Errorf("%w: reverse cutover traffic switch failed: %w", status.ErrOwnershipAmbiguous, err)
	default:
		return fmt.Errorf("reverse cutover: traffic switch failed: %w", err)
	}
}

// finalizeReverse records that ownership is definitively back on the source,
// then performs the cleanup that may still fail. Once every former target has
// been retired no remaining step can move ownership again, so persisting the
// phase first turns the rest into idempotent work a resume can simply repeat.
func (w *reverseWindow) finalizeReverse(ctx context.Context) error {
	r := w.r
	if err := w.persistPhase(ctx, phaseReverseFinalized); err != nil {
		return fmt.Errorf("reverse cutover: persist finalized phase: %w", err)
	}
	r.durableMutation.Store(true)
	r.terminalOwnership.Store(uint32(status.WorkflowTerminalOwnershipReverseFinalized))
	// 6. Drop the revert marker and the checkpoint.
	if err := w.dropMarker(ctx); err != nil {
		return fmt.Errorf("reverse cutover: drop revert marker: %w", err)
	}
	if err := w.dropCheckpoint(ctx); err != nil {
		return fmt.Errorf("reverse cutover: drop checkpoint: %w", err)
	}
	r.logger.Info("reverse cutover complete; move rolled back to source")
	return nil
}
