package move

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/check"
	"github.com/block/spirit/pkg/checkpoint"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"golang.org/x/sync/errgroup"
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

// defaultReverseWindowPollInterval is how often the window loop checks for a
// revert request, feed death, or the deadline. Tests shorten it per Runner
// (tunables.reverseWindowPollInterval).
const defaultReverseWindowPollInterval = 1 * time.Second

// defaultReverseFeedFlushInterval is the reverse feed's periodic flush
// interval. Tests shorten it per Runner (tunables.reverseFeedFlushInterval).
const defaultReverseFeedFlushInterval = change.DefaultFlushInterval

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
// On a safe cutover retry they are recaptured after the new final flush. The
// targets are read at the same time, since this runs under the locks.
func captureReverseWindow(ctx context.Context, r *Runner) error {
	read := make([]string, len(r.targets))
	g, gctx := errgroup.WithContext(ctx)
	for i := range r.targets {
		g.Go(func() error {
			pos, err := targetCurrentPosition(gctx, r, &r.targets[i])
			if err != nil {
				return fmt.Errorf("capture reverse-feed start position for target %d: %w", i, err)
			}
			read[i] = pos
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	positions := make(map[string]string, len(r.targets))
	for i, pos := range read {
		positions[targetKey(r.targets[i])] = pos
	}
	r.reversePositions = positions
	return nil
}

// persistReverseWindow runs after the traffic switch and before retiring the
// source. The background checkpoint dumper is already stopped, so this write
// is authoritative. The window duration starts when the switch completes.
func persistReverseWindow(ctx context.Context, r *Runner) error {
	// Hold checkpointMu so no periodic dump is mid-write, and mark the row as
	// owned by the reverse window so none can follow (see DumpCheckpoint).
	r.checkpointMu.Lock()
	defer r.checkpointMu.Unlock()
	r.cutoverAt = time.Now()
	posJSON, err := json.Marshal(r.reversePositions)
	if err != nil {
		return fmt.Errorf("marshal reverse positions: %w", err)
	}
	// Persist that the move has entered its reverse window. Pre-flight and
	// pre-cutover already guaranteed no revert marker was present up to here, so
	// any _spirit_move_revert that appears on targets[0] from now on is a genuine
	// operator revert request for THIS window — never dropped as "stale".
	if err := r.checkpointTbl().Write(ctx, checkpoint.Record{
		Position:  string(posJSON),
		Phase:     phaseReverseWindow,
		CutoverAt: r.cutoverAt,
	}); err != nil {
		return err
	}
	r.checkpointOwnedByReverseWindow = true
	return nil
}

// reverseWindow drives the post-cutover reverse window: it stands up a
// change-only reverse feed (targets → the source's _old tables) so the source
// stays current, then holds until the window elapses (complete forward), a
// revert is requested (reverse cutover), or the feed dies on a condition after
// which rollback is no longer safe (complete forward). A feed that stops on a
// failed flush or stream is restarted from its last checkpointed position
// instead, and the window stays open. It is a separate type so the Runner's
// methods stay in runner.go.
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
	// readRevertMarker and restart, when non-nil, replace the window loop's
	// revert-marker read (revertMarkerExists on targets[0]) and its restart
	// of a stopped feed (restartFeed), so the loop's handling of a stopped
	// feed can be tested without live binlog streams.
	readRevertMarker func(ctx context.Context) (bool, error)
	restart          func(ctx context.Context) error
}

func newReverseWindow(r *Runner) *reverseWindow {
	return &reverseWindow{
		r: r,
		persistPhase: func(ctx context.Context, phase string) error {
			return r.checkpointTbl().Write(ctx, checkpoint.Record{Phase: phase, CutoverAt: r.cutoverAt})
		},
		dropMarker:     func(ctx context.Context) error { return dropRevertMarker(ctx, r.targets[0].DB, r.logger) },
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
	// A closure, not `defer w.feed.Close()`: restartFeed replaces w.feed, and
	// the feed to close on the way out is the one running then.
	defer func() { w.feed.Close() }() // idempotent; the terminal actions also close explicitly
	if err := w.feed.Start(ctx); err != nil {
		return fmt.Errorf("reverse window: start feed: %w", err)
	}
	return w.hold(ctx)
}

// hold runs the window loop over a started feed until the window elapses, a
// revert is requested, the feed dies on a condition that makes rollback unsafe,
// or ctx is cancelled, and performs the terminal action. A feed that stops on a
// restartable condition (see ReverseFeed.Restartable) is restarted, with
// backoff, for as long as the window is open; a revert requested meanwhile
// runs once the feed is running again.
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
		ticker := time.NewTicker(cmp.Or(r.tunables.reverseWindowPollInterval, defaultReverseWindowPollInterval))
		defer ticker.Stop()
		checkpointTicker := time.NewTicker(status.CheckpointDumpInterval)
		defer checkpointTicker.Stop()
		// The runner's status block stops at cutover, so this is the only
		// periodic report of how far behind the reverse feed is.
		progressTicker := time.NewTicker(cmp.Or(r.tunables.reverseFeedProgressInterval, defaultReverseFeedProgressInterval))
		defer progressTicker.Stop()
		progress := newReverseFeedProgress(w.sourceLabels(), time.Now())
		// failedReverts counts reverse cutover attempts that failed before
		// ownership moved (see errReverseCutoverAbandoned). After one, the
		// next attempt waits until retryRevertAt, so a failing attempt does
		// not lock the targets again on the next tick.
		failedReverts := 0
		// deferredReverts counts attempts deferred because the source was not
		// yet up to date (errReverseFeedNotCaughtUp); they take no lock and
		// do not count against DBConfig.MaxRetries. For logs.
		deferredReverts := 0
		var retryRevertAt time.Time
		revertBackoff := cmp.Or(r.tunables.reverseCutoverInitialBackoff, defaultReverseCutoverInitialBackoff)
		// Restarts of a stopped feed are spaced out by retryRestartAt, which
		// every attempt sets, failed or not: a restart that succeeds can stop
		// again on its first flush (the cause is often still there), and
		// restarting on every tick would re-read every target's binlog from
		// the checkpoint each time. restartBackoff doubles with each attempt
		// and goes back to its initial value only once a restarted feed has
		// stayed up for restartHealthyAfter — two flush intervals, so it has
		// flushed at least once. failedRestarts counts attempts that
		// failed since the last one that succeeded, and restarts the
		// successful ones since the backoff was last reset; both are for logs.
		failedRestarts, restarts := 0, 0
		var retryRestartAt, restartedAt time.Time
		initialRestartBackoff := cmp.Or(r.tunables.reverseFeedRestartInitialBackoff, defaultReverseFeedRestartInitialBackoff)
		maxRestartBackoff := cmp.Or(r.tunables.reverseFeedRestartMaxBackoff, defaultReverseFeedRestartMaxBackoff)
		restartHealthyAfter := 2 * cmp.Or(r.tunables.reverseFeedFlushInterval, defaultReverseFeedFlushInterval)
		restartBackoff := initialRestartBackoff
		// revertPendingLogged is set once a revert requested while the feed
		// is stopped has been logged, so it is logged once per stop.
		revertPendingLogged := false
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-progressTicker.C:
				if ferr := w.feed.Err(); ferr != nil {
					progress.logStopped(r.logger, ferr, w.feed.Restartable())
					continue
				}
				now := time.Now()
				progress.log(r.logger, now, progress.report(now, w.feed.sourceProgress()))
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
					// (Write abandons only once ctx is done, caught above; that arm is defence in depth.)
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
					if !w.feed.Restartable() {
						r.logger.Error("reverse feed died mid-window; completing forward (rollback no longer safe)", "error", ferr)
						return w.completeForward(ctx)
					}
					// The feed stopped without the watched tables changing:
					// typically the source refused the flush's writes for a
					// while (connection limits, a failover). The source is
					// behind, not wrong, so keep the window open and restart
					// the feed — the window's deadline still bounds it.
					//
					// A revert requested meanwhile cannot run: the reverse
					// cutover drains the feed, and the source is missing the
					// target writes the stopped feed did not apply. It runs
					// on the first tick after a restart succeeds (its drain
					// then applies the backlog under the target locks); until
					// then it is logged, once per stop.
					requested, rerr := w.revertRequested(ctx)
					revertPending := rerr == nil && requested
					if revertPending && !revertPendingLogged {
						r.logger.Warn(fmt.Sprintf("revert requested (a table named %s exists on %s), but the reverse feed is stopped and the source is behind the targets; the revert runs once the feed is running again",
							revertMarkerName, revertLoc), "deadline", deadline, "error", ferr.Error())
						revertPendingLogged = true
					}
					if !time.Now().Before(deadline) {
						if revertPending {
							r.logger.Error(fmt.Sprintf("reverse window elapsed while the reverse feed was stopped; discarding the pending revert request (%s on %s) and completing forward: the source is behind the targets and cannot take traffic back",
								revertMarkerName, revertLoc), "error", ferr.Error())
						} else {
							r.logger.Error("reverse window elapsed while the reverse feed was stopped; completing forward", "error", ferr.Error())
						}
						return w.completeForward(ctx)
					}
					if time.Now().Before(retryRestartAt) {
						continue
					}
					restart := w.restart
					if restart == nil {
						restart = w.restartFeed
					}
					err := restart(ctx)
					if errors.Is(err, errReverseFeedNotRestartable) {
						r.logger.Error("reverse feed died mid-window; completing forward (rollback no longer safe)", "error", w.feed.Err())
						return w.completeForward(ctx)
					}
					if err != nil && ctx.Err() != nil {
						return ctx.Err()
					}
					retryIn := restartBackoff
					retryRestartAt = time.Now().Add(retryIn)
					restartBackoff = min(restartBackoff*2, maxRestartBackoff)
					if err != nil {
						failedRestarts++
						r.logger.Warn("could not restart the reverse feed; the reverse window stays open and will try again",
							"attempt", failedRestarts, "retry_in", retryIn, "deadline", deadline, "error", err.Error())
						continue
					}
					restarts++
					restartedAt = time.Now()
					revertPendingLogged = false
					// The replacement feed resumes from the checkpoint, so its
					// catch-up starts over: track it from scratch rather than
					// compare its positions with the stopped feed's.
					progress = newReverseFeedProgress(w.sourceLabels(), restartedAt)
					r.logger.Info("reverse feed restarted from its last checkpointed position; the reverse window stays open",
						"stopped_on", ferr.Error(), "restarts", restarts, "failed_attempts", failedRestarts,
						"next_restart_no_sooner_than", retryIn)
					failedRestarts = 0
					continue
				}
				if !restartedAt.IsZero() && time.Since(restartedAt) >= restartHealthyAfter {
					r.logger.Info("restarted reverse feed has stayed up; resetting the restart backoff",
						"up_for", time.Since(restartedAt).Round(time.Second), "restarts", restarts)
					restarts = 0
					restartedAt = time.Time{}
					restartBackoff = initialRestartBackoff
					retryRestartAt = time.Time{}
				}
				if time.Now().Before(retryRevertAt) {
					continue
				}
				requested, err := w.revertRequested(ctx)
				if err != nil {
					// A transient read error should not abandon the window; log and
					// retry on the next tick.
					r.logger.Warn("could not read revert flag; continuing window", "error", err)
				} else if requested {
					r.logger.Info(fmt.Sprintf("revert triggered: detected a table named %s on %s; rolling back to source",
						revertMarkerName, revertLoc))
					err := w.reverseCutover(ctx)
					if err == nil {
						return nil
					}
					if errors.Is(err, errReverseFeedNotCaughtUp) && ctx.Err() == nil {
						// No lock was taken and nothing moved; the source is
						// behind the targets. Wait for the feed, up to the
						// deadline — the same bound as a revert requested
						// while the feed is stopped.
						if !time.Now().Before(deadline) {
							r.logger.Error(fmt.Sprintf("reverse window elapsed before the reverse feed brought the source up to date; discarding the pending revert request (%s on %s) and completing forward: the source is behind the targets and cannot take traffic back",
								revertMarkerName, revertLoc), "error", err.Error())
							return w.completeForward(ctx)
						}
						deferredReverts++
						r.logger.Warn("revert deferred: the reverse feed has not brought the source up to date; no lock was taken, and the reverse window will try again",
							"deferrals", deferredReverts, "retry_in", revertBackoff, "deadline", deadline, "error", err.Error())
						retryRevertAt = time.Now().Add(revertBackoff)
						revertBackoff = min(revertBackoff*2, cmp.Or(r.tunables.reverseCutoverMaxBackoff, defaultReverseCutoverMaxBackoff))
						continue
					}
					failedReverts++
					if !w.mayRetryReverseCutover(ctx, err, failedReverts) {
						return err
					}
					// The marker is still there, so a tick after the backoff
					// tries again.
					r.logger.Warn("reverse cutover attempt failed before ownership moved; the reverse window stays open and will try again",
						"attempt", failedReverts, "max_retries", r.dbConfig.MaxRetries, "retry_in", revertBackoff, "error", err.Error())
					retryRevertAt = time.Now().Add(revertBackoff)
					revertBackoff = min(revertBackoff*2, cmp.Or(r.tunables.reverseCutoverMaxBackoff, defaultReverseCutoverMaxBackoff))
					continue
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
		// The feed is stopped. Leave the row alone: a restarted feed resumes
		// from it, and a feed that cannot be restarted completes forward.
		return nil
	}
	feedPositions := w.feed.Positions()
	positions := make(map[string]string, len(feedPositions))
	for i, pos := range feedPositions {
		if pos == "" {
			return nil // nothing resumable observed yet; keep the previous row
		}
		positions[targetKey(r.targets[i])] = pos
	}
	// Skip the write only when no feed position moved. In practice that
	// excludes targets[0]: the previous checkpoint REPLACE is in its binlog and
	// advances its feed's position, so even an idle window writes once per
	// interval.
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
		FlushInterval: cmp.Or(r.tunables.reverseFeedFlushInterval, defaultReverseFeedFlushInterval),
		MemoryBudget:  r.move.ReverseFeedMemoryBudget,
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

// restartFeed replaces a feed that stopped on a restartable condition with a
// new one started from the last checkpointed positions (r.reversePositions:
// the cutover capture, or a later flushed position written by
// checkpointPositions). The stopped feed's flushed position did not advance
// past the changes it failed to apply, and the checkpointed one is no later
// than that, so the new feed replays them; the apply is idempotent (REPLACE /
// DELETE by primary key), the same contract a resumed window relies on. On
// failure w.feed is left as the stopped feed, so the next tick tries again.
//
// Closing the stopped feed waits for its other sources' streams, and one of
// them can report a condition after which rollback is no longer safe (a schema
// change) while it stops. That condition overrides the restartable one (see
// ReverseFeed.onFatal), so the verdict is read again after Close, and
// errReverseFeedNotRestartable returned rather than starting a new feed over
// it.
func (w *reverseWindow) restartFeed(ctx context.Context) error {
	stopped := w.feed
	stopped.Close()
	if !stopped.Restartable() {
		return errReverseFeedNotRestartable
	}
	if err := w.buildFeed(ctx); err != nil {
		w.feed = stopped
		return err
	}
	return w.startReplacementFeed(ctx, stopped)
}

// startReplacementFeed starts w.feed, built by restartFeed to replace
// stopped. Start launches the sources one at a time, so when a later source
// fails to start, an earlier one has already streamed and may have reported a
// condition after which rollback is no longer safe; Start closes the new feed
// either way. That verdict is kept — w.feed stays the new feed and
// errReverseFeedNotRestartable sends the window to complete forward — rather
// than discarded by putting back the stopped feed, whose own verdict is only
// restartable. Otherwise the stopped feed goes back in place, and its Err
// makes the next tick try again.
func (w *reverseWindow) startReplacementFeed(ctx context.Context, stopped *ReverseFeed) error {
	err := w.feed.Start(ctx)
	if err == nil {
		return nil
	}
	if w.feed.Err() != nil && !w.feed.Restartable() {
		return errReverseFeedNotRestartable
	}
	w.feed = stopped
	return fmt.Errorf("reverse window: restart feed: %w", err)
}

// errReverseFeedNotRestartable is restartFeed's report that the stopped feed
// recorded a condition after which rollback is no longer safe while it was
// being closed; the window completes forward.
var errReverseFeedNotRestartable = errors.New("reverse feed is no longer restartable")

// sourceLabels names each reverse source for the progress log, in feed order
// (the feed has one source per target): its host:port/database.
func (w *reverseWindow) sourceLabels() []string {
	labels := make([]string, len(w.r.targets))
	for i, tgt := range w.r.targets {
		labels[i] = tgt.Config.Addr + "/" + tgt.Config.DBName
	}
	return labels
}

func (w *reverseWindow) revertRequested(ctx context.Context) (bool, error) {
	if w.readRevertMarker != nil {
		return w.readRevertMarker(ctx)
	}
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
func (w *reverseWindow) reverseCutover(ctx context.Context) (err error) {
	r := w.r

	// Clear any stale _revert tables on the targets before the retire (step 5)
	// renames each target table to its _revert form — a leftover from a prior
	// reverse cutover would otherwise collide. Done before locking, since the
	// leftovers are not in the lock set (DROP under LOCK TABLES is disallowed for
	// unlocked tables).
	if err := r.dropStaleRevertTables(ctx); err != nil {
		return abandonReverseCutover(err)
	}

	// Stop the periodic flush before locking, and restart it if this attempt
	// is abandoned with the feed still open (see PausePeriodicFlush). Waiting
	// here for a periodic flush in progress blocks nothing: the targets are
	// not locked yet.
	w.feed.PausePeriodicFlush()
	feedClosed := false
	defer func() {
		if !feedClosed {
			w.feed.ResumePeriodicFlush(ctx)
		}
	}()

	// Bring the source up to date before taking any lock. A feed that was
	// just restarted, or that is still replaying a backlog, may not be able to
	// apply its changes yet: the source can still be refusing them (the
	// condition that stopped the feed), or the backlog can take longer to
	// apply than the target locks may be held. Finding that out under the
	// locks would block the targets' writes for each attempt and use up the
	// attempts the window allows. Found out here, it costs no lock and no
	// attempt (see errReverseFeedNotCaughtUp): the revert waits, bounded by
	// the window's deadline. The drain under the locks (step 2) is then short.
	if err := w.preDrainFeed(ctx); err != nil {
		return abandonReverseCutover(err)
	}

	timing := newLockTiming(r.logger, "reverse cutover")
	defer func() { timing.log(err) }()

	// 1. Lock the reverse sources (former targets) to freeze their writes, all
	//    at the same time (see lockAll).
	locks, err := lockAll(ctx, len(r.targets), func(ctx context.Context, i int) (*dbconn.TableLock, error) {
		lock, err := dbconn.NewTableLock(ctx, r.targets[i].DB, w.watched[i], r.dbConfig, r.logger)
		if err != nil {
			return nil, fmt.Errorf("reverse cutover: lock target %d: %w", i, err)
		}
		return lock, nil
	})
	if err != nil {
		return abandonReverseCutover(err)
	}
	defer closeLocks(ctx, locks)
	timing.locked()

	// Everything up to the reverting phase write is bounded by the cutover
	// lock timeout, as the forward cutover bounds its work before the switch.
	// None of it moves ownership, so an attempt past the bound releases the
	// locks and leaves the window open for another (see hold).
	lockTimeout := r.move.cutoverLockTimeout()
	lockedCtx, cancelLocked := context.WithTimeout(ctx, lockTimeout)
	defer cancelLocked()
	beforePersist := func(err error) error {
		return abandonReverseCutover(lockTimeoutError(ctx, lockedCtx, lockTimeout, err))
	}
	if hook := w.r.tunables.reverseCutoverUnderLockHook; hook != nil {
		if err := hook(lockedCtx); err != nil {
			return beforePersist(err)
		}
	}

	// Check the source schema one last time before the _old tables go back
	// into service: a trigger or event created during the window could have
	// written to them outside the reverse feed, or (a trigger on an _old
	// table) would go live with them. Views, procedures and functions are not
	// refused (see check.ReverseWindowSchemaObjectsError). This path holds no
	// lock on the source tables, so it does not keep DDL out while it runs.
	// Fail closed: nothing has moved ownership yet, the phase is still
	// reverse_window, and a re-run resumes the window once the objects are
	// dropped.
	if err := w.checkSourceSchemaObjects(lockedCtx); err != nil {
		return beforePersist(fmt.Errorf("reverse cutover: %w", err))
	}
	timing.step("checks")

	// 2. Flush the reverse feed so the source's _old tables reflect every target
	//    write, then stop it (no more writes to _old during the renames below).
	//    The feed stays open until here, so an attempt that fails before this
	//    point leaves it running and the window can try again.
	if err := w.drainFeed(lockedCtx); err != nil {
		return beforePersist(err)
	}
	if err := lockedCtx.Err(); err != nil {
		return beforePersist(err)
	}
	w.feed.Close()
	feedClosed = true
	timing.step("flush")

	// The mirror of the forward cutover's carryAutoIncrementsToTargets: ids the
	// targets issued during the window and have since deleted never reached
	// the source's _old tables, so without this the source would issue them
	// again once traffic returns. The targets are locked and the feed is
	// drained, so neither side's counter can move now.
	//
	// It runs after the feed is closed: its ALTER TABLE on an _old table is a
	// schema change the feed would stop on where the source and a target
	// share a server. The window cannot retry a failure here (the feed is
	// gone), so the carry is retried in place, under the locks (see
	// retryAutoIncrementCarry). If every attempt fails, the run fails with the
	// phase still reverse_window, and a re-run resumes the window.
	if err := retryAutoIncrementCarry(ctx, r, lockTimeout, w.carryAutoIncrementsToSource); err != nil {
		return fmt.Errorf("reverse cutover: %w", err)
	}
	timing.step("auto_increment")

	// Persist the ownership boundary immediately before the first rename that
	// moves ownership, and fail closed if it cannot be written. Everything up
	// to here is reversible; from the next statement on, a crash leaves the
	// move half-rolled-back, and a resume that could not read phaseReverting
	// would restart the window as though traffic were still on the target.
	if err := w.persistPhase(ctx, phaseReverting); err != nil {
		return fmt.Errorf("reverse cutover: persist reverting phase: %w", err)
	}
	timing.step("persist_phase")

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
			r.lifecycle.MarkDurableMutation()
		}
	}
	timing.step("unretire")

	// 4. Switch traffic back to the source. The source serves under its real
	//    names now, and the target locks are what keeps stragglers off the
	//    targets, so a callback that failed cleanly is retried here, under
	//    the locks, rather than leaving the move half rolled back.
	if err := w.switchTrafficToSource(ctx); err != nil {
		return err
	}
	timing.switchedTraffic()

	// 5. Retire the former targets to their _revert form under their lock —
	//    fencing straggler writes after the switch, mirroring the forward
	//    cutover's source rename. _revert (not _old) marks these as revert
	//    artifacts, so a later move can safely drop them. Traffic is back on
	//    the source, so the renames run even if ctx is cancelled
	//    (context.WithoutCancel): a stopped one would leave a target serving.
	if err := w.retireTargets(context.WithoutCancel(ctx), locks); err != nil {
		return err
	}
	timing.step("retire")

	// Ownership is back on the source. Record it even if ctx is cancelled: a
	// cancelled write would leave the checkpoint at phaseReverting, and the
	// next run would treat the finished rollback as ownership-ambiguous.
	finalizeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), postCutoverCleanupTimeout)
	defer cancel()
	return w.finalizeReverse(finalizeCtx)
}

// defaultReverseFeedRestartInitialBackoff and
// defaultReverseFeedRestartMaxBackoff bound the wait between attempts to
// restart a reverse feed that stopped on a restartable condition. Tests shorten
// them per Runner (tunables.reverseFeedRestartInitialBackoff /
// reverseFeedRestartMaxBackoff).
const (
	defaultReverseFeedRestartInitialBackoff = 2 * time.Second
	defaultReverseFeedRestartMaxBackoff     = 30 * time.Second
)

// defaultReverseCutoverInitialBackoff and defaultReverseCutoverMaxBackoff
// bound the wait between reverse cutover attempts. Tests shorten them per
// Runner (tunables.reverseCutoverInitialBackoff / reverseCutoverMaxBackoff).
const (
	defaultReverseCutoverInitialBackoff = 2 * time.Second
	defaultReverseCutoverMaxBackoff     = 10 * time.Second
)

// retryAutoIncrementCarry runs carry, and runs it again, up to
// DBConfig.MaxRetries attempts, when it fails while ctx is live. Each attempt
// is bounded by timeout. The carry only raises AUTO_INCREMENT counters, so
// repeating it is safe, and it runs after the reverse feed is closed, where a
// failure would end the run instead of leaving the window open.
func retryAutoIncrementCarry(ctx context.Context, r *Runner, timeout time.Duration, carry func(ctx context.Context) error) error {
	var err error
	for attempt := 1; attempt <= max(r.dbConfig.MaxRetries, 1); attempt++ {
		if attempt > 1 {
			r.logger.Warn("retrying the AUTO_INCREMENT carry of a reverse cutover while still holding target table locks",
				"attempt", attempt, "max_retries", r.dbConfig.MaxRetries, "error", err.Error())
			select {
			case <-ctx.Done():
				return errors.Join(err, ctx.Err())
			case <-time.After(cmp.Or(r.tunables.renameRetryWait, defaultRenameRetryWait)):
			}
		}
		err = carryAttempt(ctx, timeout, carry)
		if err == nil || ctx.Err() != nil {
			return err
		}
	}
	return err
}

func carryAttempt(ctx context.Context, timeout time.Duration, carry func(ctx context.Context) error) error {
	attemptCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	err := carry(attemptCtx)
	if err != nil && ctx.Err() == nil && errors.Is(attemptCtx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("%w: the AUTO_INCREMENT carry did not finish within %s (--cutover-lock-timeout): %w", errCutoverLockTimeout, timeout, err)
	}
	return err
}

// mayRetryReverseCutover reports whether the window may try a reverse cutover
// again after its failedReverts-th failed attempt failed with err: the attempt
// moved no ownership (errReverseCutoverAbandoned), no check refused it (a
// retry would be refused for the same reason), ctx is live, and fewer than
// DBConfig.MaxRetries attempts have failed.
func (w *reverseWindow) mayRetryReverseCutover(ctx context.Context, err error, failedReverts int) bool {
	return errors.Is(err, errReverseCutoverAbandoned) &&
		!errors.Is(err, check.ErrRefused) &&
		ctx.Err() == nil &&
		failedReverts < w.r.dbConfig.MaxRetries
}

// errReverseFeedNotCaughtUp marks a reverse cutover attempt abandoned before it
// took any lock because the reverse feed could not bring the source up to date
// (see preDrainFeed). The source is behind, not wrong: the window waits and
// tries again without counting the attempt against DBConfig.MaxRetries.
var errReverseFeedNotCaughtUp = errors.New("the reverse feed has not brought the source up to date")

// preDrainFeed flushes the reverse feed without holding any lock, bounded by
// the cutover lock timeout: a backlog that takes longer than that to apply
// would not fit under the locks either.
func (w *reverseWindow) preDrainFeed(ctx context.Context) error {
	timeout := w.r.move.cutoverLockTimeout()
	drainCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	err := w.feed.Flush(drainCtx)
	if err == nil {
		err = w.feed.Err() // a feed that stopped meanwhile has not applied its changes
	}
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(drainCtx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("reverse cutover: %w: flushing the backlog took longer than %s (--cutover-lock-timeout): %w", errReverseFeedNotCaughtUp, timeout, err)
	}
	return fmt.Errorf("reverse cutover: %w: %w", errReverseFeedNotCaughtUp, err)
}

// errReverseCutoverAbandoned marks a reverse cutover attempt that failed before
// it wrote the reverting phase. Nothing has moved ownership: the targets still
// serve and the reverse feed is still running, so the window may try again.
var errReverseCutoverAbandoned = errors.New("reverse cutover abandoned before ownership moved")

func abandonReverseCutover(err error) error {
	return fmt.Errorf("%w: %w", errReverseCutoverAbandoned, err)
}

// drainFeed flushes the reverse feed and confirms that nothing is left
// buffered. A nil error from Flush does not rule that out: a drain can decline
// to finish and report it by leaving the changes buffered, and Flush's loop
// exits on a *trivial* backlog rather than an empty one. AllChangesFlushed is
// the question that matters, and asking it is what the forward cutover already
// does (cutover.go).
func (w *reverseWindow) drainFeed(ctx context.Context) error {
	if err := w.feed.Flush(ctx); err != nil {
		return fmt.Errorf("reverse cutover: final flush: %w", err)
	}
	if !w.feed.AllChangesFlushed() {
		return fmt.Errorf("reverse cutover: %w; refusing to discard buffered target writes",
			change.ErrChangesNotFlushed)
	}
	// A feed can stop between the window loop's last look and here: a
	// periodic flush that fails, or a stream that breaks, while this attempt
	// is locking the targets. Its changes are then neither applied nor
	// buffered, so an empty buffer does not mean the source is current.
	if err := w.feed.Err(); err != nil {
		return fmt.Errorf("reverse cutover: the reverse feed stopped: %w", err)
	}
	return nil
}

// switchTrafficToSource runs the reverse cutover callback, and runs it again,
// up to DBConfig.MaxRetries attempts, when it fails without reporting a
// durable mutation or ambiguous ownership: the callback changed nothing, so
// trying again cannot make the routing worse.
func (w *reverseWindow) switchTrafficToSource(ctx context.Context) error {
	r := w.r
	var err error
	for attempt := 1; attempt <= max(r.dbConfig.MaxRetries, 1); attempt++ {
		if attempt > 1 {
			r.logger.Warn("retrying the reverse cutover traffic switch while still holding target table locks",
				"attempt", attempt, "max_retries", r.dbConfig.MaxRetries, "error", err.Error())
			select {
			case <-ctx.Done():
				return errors.Join(err, ctx.Err())
			case <-time.After(cmp.Or(r.tunables.renameRetryWait, defaultRenameRetryWait)):
			}
		}
		err = w.runReverseCutoverCallback(ctx)
		if err == nil || errors.Is(err, status.ErrDurableMutation) || errors.Is(err, status.ErrOwnershipAmbiguous) {
			return err
		}
	}
	return err
}

// retireTargets renames every former target's tables to their _revert names,
// one RENAME TABLE per target covering all of its tables, so a target is never
// left partially retired. The targets are renamed at the same time. A rename
// the server refused is retried under the lock, as the forward cutover retries
// its source rename. A rename whose outcome is unknown is not: a retry cannot
// tell whether it already ran, so it is reported as ErrOwnershipAmbiguous.
func (w *reverseWindow) retireTargets(ctx context.Context, locks []*dbconn.TableLock) error {
	r := w.r
	fragments := make([]string, 0, len(r.sourceTables))
	for _, t := range r.sourceTables {
		fragments = append(fragments, sqlescape.MustEscapeSQL("%n TO %n", t.TableName, check.RevertRetiredName(t.TableName)))
	}
	stmt := "RENAME TABLE " + strings.Join(fragments, ", ")

	errs := make([]error, len(locks))
	var wg sync.WaitGroup
	for i := range locks {
		wg.Go(func() {
			started := time.Now()
			errs[i] = w.retireTarget(ctx, i, locks[i], stmt)
			// The retire step lasts as long as the slowest target.
			attrs := []any{"target", i, "addr", r.targets[i].Config.Addr, "database", r.targets[i].Config.DBName,
				"duration", time.Since(started)}
			if errs[i] != nil {
				r.logger.Warn("reverse cutover: failed to retire a former target", append(attrs, "error", errs[i].Error())...)
				return
			}
			r.logger.Warn("reverse cutover: retired a former target", attrs...)
		})
	}
	wg.Wait()
	// Every target's outcome is kept: recovery needs to know which targets
	// are definitely still unretired, not only which one is ambiguous.
	// errors.Is still finds ErrOwnershipAmbiguous in the joined error.
	return errors.Join(errs...)
}

func (w *reverseWindow) retireTarget(ctx context.Context, i int, lock *dbconn.TableLock, stmt string) error {
	r := w.r
	var err error
	for attempt := 1; attempt <= max(r.dbConfig.MaxRetries, 1); attempt++ {
		if attempt > 1 {
			r.logger.Warn("retrying the retire of a former target while still holding its table locks",
				"target", i, "attempt", attempt, "max_retries", r.dbConfig.MaxRetries, "error", err.Error())
			time.Sleep(cmp.Or(r.tunables.renameRetryWait, defaultRenameRetryWait))
		}
		err = lock.ExecUnderLock(ctx, stmt)
		if err == nil {
			return nil
		}
		if dbconn.IsOutcomeUnknown(err) {
			return fmt.Errorf("%w: reverse cutover: retire target %d, outcome unknown: %w",
				status.ErrOwnershipAmbiguous, i, err)
		}
	}
	return fmt.Errorf("reverse cutover: retire target %d after %d attempts under lock: %w",
		i, max(r.dbConfig.MaxRetries, 1), err)
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
		r.lifecycle.MarkDurableMutation()
	}
	if result.OwnershipAmbiguous {
		r.lifecycle.SetTerminalOwnership(status.WorkflowTerminalOwnershipAmbiguous)
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
	r.lifecycle.MarkDurableMutation()
	r.lifecycle.SetTerminalOwnership(status.WorkflowTerminalOwnershipReverseFinalized)
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
