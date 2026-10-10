package change

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/go-mysql-org/go-mysql/replication"
)

// feedCore is the state and behaviour shared by the two binlog-backed
// change.Source implementations, binlogClient (file+offset positions) and
// gtidClient (GTID positions). Both embed it, so every method here exists
// once instead of once per client.
//
// What lives here is everything that does not depend on how a client names
// its resume position: the subscription set and how it is built, DDL
// filtering, the fatal-error callback, Stop/Close teardown, flush
// bookkeeping (FlushResidual, the FeedStats flush fields), the periodic
// flush goroutine, and the row dispatch that cooperates with the parker.
//
// What stays on each client is everything that does: the buffered/flushed
// position fields and their accessors, the client's own flush (which
// snapshots and advances the position), blockWait, Start/recreateStreamer,
// readStream and event decoding. An embedded struct cannot call the
// embedding type's methods, so the shared methods that need the client's
// flush or BlockWait take them as arguments (see flushFunc), and each
// client keeps a one-line Source method that passes its own.
type feedCore struct {
	// mu protects the flush bookkeeping fields below, the syncer /
	// cancelFunc pair, and — on the embedding client — the position fields
	// (binlogClient: bufferedPos / flushedPos and the cached
	// binlogStatusStmt; gtidClient: bufferedGTID / flushedGTID and the
	// pending GTID), plus the streamer the client's Start and
	// recreateStreamer replace alongside syncer. It lives here so both the
	// shared methods and the client-specific ones take the same lock.
	// Subscriptions live in subs with its own RWMutex. Named (not embedded)
	// so the lock surface stays package-internal: sync.Mutex is not
	// re-entrant, and exposing public Lock/Unlock on an external API invites
	// accidental self-deadlocks from a caller that doesn't know what's
	// already held.
	mu sync.Mutex

	username string
	password string

	// syncer is created by the client's Start and recreateStreamer under mu,
	// and closed by Close once the reader has exited.
	syncer *replication.BinlogSyncer

	applier applier.Applier

	// subs owns the table-keyed subscription set and its own lock. See
	// subscriptionRegistry. The client mutex above does NOT cover map
	// access; reach the subscriptions only through these methods.
	subs *subscriptionRegistry

	// parker backs VerifyRowAtNextChange: it holds the reader between events
	// and owns the single armed watch. See park.go.
	parker RowParker

	// callerCancelFunc is an optional callback that is called when a DDL
	// change is detected on a subscribed table, or when a fatal stream
	// error occurs; the FatalReason distinguishes the two so the caller
	// can decide whether persisted resume state must be invalidated. The
	// caller is expected to handle cancellation and cleanup in this
	// callback. It returns true if the error was acted upon (i.e. the
	// caller actually cancelled), or false if it was ignored (e.g.
	// because the migration is already past cutover).
	callerCancelFunc func(FatalReason) bool
	ddlFilterSchema  string
	ddlFilterTables  map[string]struct{}

	// stopped is set once by Stop and read by the stream reader on every event
	// it would otherwise deliver. Atomic because the two are different
	// goroutines. Distinct from isClosed, which tears the reader down.
	stopped atomic.Bool

	serverID uint32 // server ID for the binlog reader

	// flushResidual is the pending-change count observed at the end of the
	// most recent flush, and flushCount how many flushes have completed. Both
	// guarded by mu. See Source.FlushResidual.
	flushResidual int
	flushCount    int

	// lastFlush* describe the most recently completed flush, for FeedStats.
	// Guarded by mu. Recorded for every flush, not just the periodic one, so
	// the status block's "flushed X ago" answers "when did the position last
	// advance?" rather than "when did the ticker last fire?".
	lastFlushAt       time.Time
	lastFlushDuration time.Duration
	lastFlushRows     int
	// lastFlushComplete is that flush's allChangesFlushed result: whether
	// every subscription drained everything it held. Flush reads it, alongside
	// each subscription's LastDrainHitBudget, to tell a backlog it can work
	// through from one it cannot — see backlogWorthDraining.
	lastFlushComplete bool

	// periodicFlushLock protects the cancel/done pair below. The cancel
	// signals the periodic-flush goroutine to exit; the done channel is
	// closed by the goroutine on its way out, so StopPeriodicFlush can
	// wait until the goroutine has fully exited before returning. This
	// matters because StartPeriodicFlush is allowed to be called again
	// after Stop — without the done-wait, an old goroutine could still
	// be live when a new one starts, briefly doubling up.
	periodicFlushLock   sync.Mutex
	periodicFlushCancel context.CancelFunc
	periodicFlushDone   chan struct{}

	cancelFunc func()
	isClosed   atomic.Bool
	logger     *slog.Logger
	streamWG   sync.WaitGroup // tracks readStream goroutine for proper cleanup

	// subscriptionSoftLimitBytes is the per-subscription byte cap passed
	// to bufferedMap.softLimitBytes on construction. Zero disables the
	// cap. See DefaultSubscriptionSoftLimitBytes.
	subscriptionSoftLimitBytes int64

	// subscriptionSoftLimitChanges is the per-subscription change-count
	// cap, applied alongside the byte cap. Zero disables it. See
	// DefaultSubscriptionSoftLimitChanges.
	subscriptionSoftLimitChanges int

	// flushConcurrency is the map-mode flush batch concurrency passed
	// to each subscription on construction. See DefaultFlushConcurrency.
	flushConcurrency int

	// batchSize is the map-mode flush batch size passed to each
	// subscription on construction. It travels with flushConcurrency:
	// the two together set the rows a drain has in flight. See
	// DefaultBatchSize.
	batchSize int

	// underLoad is ClientConfig.UnderLoad, handed to every subscription so the
	// drain can narrow itself when the target is loaded. Nil disables it.
	underLoad func() bool

	// flushRequests receives the subscription that parked on its soft
	// memory limit. runPeriodicFlush selects on it and flushes that
	// subscription first — the all-subscription pass visits the
	// registry in nondeterministic order, and draining another
	// saturated subscription first would leave the binlog reader parked
	// for that entire drain. Buffered (cap 1); only one subscription
	// can be parked at a time (the single reader goroutine is what
	// parks), so requests never queue behind each other.
	flushRequests chan Subscription
}

// flushFunc is a client's own low-level flush (binlogClient.flush /
// gtidClient.flush): it drains every subscription and, if all of them drained
// fully, advances that client's flushed position. The shared flush loops below
// take it as an argument because an embedded feedCore cannot call the
// embedding client's methods.
type flushFunc func(ctx context.Context, underLock bool, locks []*dbconn.TableLock) error

// configure fills in the shared fields from a client constructor's arguments,
// resolving the soft-limit defaults: zero selects the package default and a
// negative value is an explicit opt-out (stored as zero, which disables the
// cap). c must be the zero value embedded in a freshly allocated client.
func (c *feedCore) configure(username, password string, appl applier.Applier, config *ClientConfig) {
	softLimit := config.SubscriptionSoftLimitBytes
	if softLimit == 0 {
		softLimit = DefaultSubscriptionSoftLimitBytes
	} else if softLimit < 0 {
		softLimit = 0 // explicit opt-out
	}
	softLimitChanges := config.SubscriptionSoftLimitChanges
	if softLimitChanges == 0 {
		softLimitChanges = DefaultSubscriptionSoftLimitChanges
	} else if softLimitChanges < 0 {
		softLimitChanges = 0 // explicit opt-out
	}
	c.username = username
	c.password = password
	c.logger = config.Logger
	c.subs = newSubscriptionRegistry()
	c.callerCancelFunc = config.CancelFunc
	c.ddlFilterSchema = config.DDLFilterSchema
	c.ddlFilterTables = toSet(config.DDLFilterTables)
	c.serverID = config.ServerID
	c.applier = appl
	c.subscriptionSoftLimitBytes = softLimit
	c.subscriptionSoftLimitChanges = softLimitChanges
	c.flushConcurrency = config.resolveFlushConcurrency()
	c.batchSize = config.resolveBatchSize()
	c.underLoad = config.UnderLoad
	c.flushRequests = make(chan Subscription, 1)
}

// AddSubscription adds a new subscription.
// Returns an error if a subscription already exists for the given table.
// Satisfies Source interface.
func (c *feedCore) AddSubscription(currentTable, newTable *table.TableInfo, chunker table.MappedChunker) error {
	subKey := encodeSchemaTable(currentTable.SchemaName, currentTable.TableName)
	// Build the buffered subscription via the shared public constructor so the
	// in-tree clients and out-of-tree change.Source implementations
	// (e.g. a VStream source) construct it the same way. The bufferedMap
	// transparently handles a non-memory-comparable PK: once the watermark
	// optimizations are disabled (copy done, checksum about to start) it acts
	// like a FIFO queue, which is required because of collation edge cases
	// (A == a on the server, but not in our map).
	sub, err := NewBufferedSubscription(BufferedSubscriptionConfig{
		CurrentTable:     currentTable,
		NewTable:         newTable,
		Applier:          c.applier,
		Chunker:          chunker,
		Logger:           c.logger,
		SoftLimitBytes:   c.subscriptionSoftLimitBytes,
		SoftLimitChanges: c.subscriptionSoftLimitChanges,
		FlushRequest:     c.flushRequests,
		FlushConcurrency: c.flushConcurrency,
		BatchSize:        c.batchSize,
		UnderLoad:        c.underLoad,
	})
	if err != nil {
		return fmt.Errorf("could not build subscription for table %s.%s: %w", currentTable.SchemaName, currentTable.TableName, err)
	}
	if !c.subs.Add(subKey, sub) {
		return fmt.Errorf("subscription already exists for table %s.%s", currentTable.SchemaName, currentTable.TableName)
	}
	return nil
}

// GetDeltaLen returns the total number of changes
// that are pending across all subscriptions.
// Satisfies Source interface.
func (c *feedCore) GetDeltaLen() int {
	deltaLen := 0
	for _, subscription := range c.subs.Snapshot() {
		deltaLen += subscription.Length()
	}
	return deltaLen
}

// buildSyncerConfig returns the BinlogSyncerConfig used by both clients'
// Start. Shared so the decode options below cannot drift between the two;
// TestSyncerConfigDecodeOptions still asserts them for both.
func (c *feedCore) buildSyncerConfig(host string, port uint16) replication.BinlogSyncerConfig {
	return replication.BinlogSyncerConfig{
		ServerID: c.serverID,
		Flavor:   "mysql",
		Host:     host,
		Port:     port,
		User:     c.username,
		Password: c.password,
		// Wrapped so go-mysql's per-rotation INFO line does not dominate the
		// log; we report rotations on the status block instead. See
		// syncerQuietMessages.
		Logger: newDemotingLogger(c.logger, syncerQuietMessages),
		// Render JSON columns directly from the JSONB byte stream in the
		// same textual form MySQL produces from SELECT json_col. The
		// default decoder goes through Go intermediate values + json.Marshal
		// and loses type tags — whole-number JSONB_DOUBLEs collapse to
		// JSON INTEGER, JSONB_OPAQUE/NEWDECIMAL collapses to JSON STRING —
		// which corrupts the JSON binary when the row is replayed into
		// the _new table and breaks the CRC32 checksum on every retry.
		// See replication/json_mysql_text.go in the go-mysql fork for
		// the renderer.
		RenderJSONAsMySQLText: true,
		// Decode TIMESTAMP values into UTC wall-clock strings. go-mysql
		// stores the epoch via time.Unix (local time) and, when this is
		// left nil, formats it in the spirit *process's* local timezone.
		// Every connection the applier uses is pinned to time_zone='+00:00'
		// (see dbconn), so a local-time string written back over a UTC
		// session shifts the stored value by the process's UTC offset —
		// silently corrupting TIMESTAMP columns on any host whose TZ isn't
		// UTC. Pinning the decoder to UTC keeps the binlog replay path
		// consistent with the UTC-pinned copier connections.
		TimestampStringLocation: time.UTC,
		// Decode row images only for subscribed tables. During the copy the
		// binlog is dominated by spirit's own writes to the _new table; see
		// newRowsEventDecodeFunc.
		RowsEventDecodeFunc: newRowsEventDecodeFunc(c.subs, &c.stopped),
	}
}

// processDDLNotification cancels the client if the DDL matches our filter criteria.
// By default, only exact schema.table matches against subscriptions trigger cancellation.
// If ddlFilterSchema is set, any DDL in that schema triggers cancellation instead.
// If ddlFilterTables is also set (alongside ddlFilterSchema), only DDL on those
// specific tables within the schema triggers cancellation — this is used for partial
// moves where only a subset of tables from a schema are being moved.
func (c *feedCore) processDDLNotification(schema, table string) {
	c.processDDL(schema, table, false)
}

// processDDLTables notifies processDDL of every table a DDL event names.
func (c *feedCore) processDDLTables(info queryEventInfo) {
	for i, ddlTable := range info.tables {
		c.processDDL(ddlTable.schema, ddlTable.table, i == 0 && info.foreignKeysOnly)
	}
}

// processDDL is processDDLNotification, except that with foreignKeysOnly (an
// ALTER of the table that only adds or drops foreign keys) a subscription's
// new table does not match. Such an ALTER changes no column and no row: the
// migration's experimental foreign key support adds the table's foreign keys
// to the new table under the cutover lock, and drops them again if the
// attempt fails, so a run resumed after a failed attempt reads those ALTERs
// after its checkpoint. The migration refuses a foreign key on the new table
// outside the cutover lock on its own.
func (c *feedCore) processDDL(schema, table string, foreignKeysOnly bool) {
	if c.stopped.Load() {
		// Post-cutover, where spirit's own RENAME TABLE is the DDL we would
		// otherwise be reporting on ourselves. See Source.Stop.
		return
	}
	if c.ddlFilterSchema != "" {
		// Schema-level filtering: cancel on DDL in the specified schema.
		if schema != c.ddlFilterSchema {
			return
		}
		// If ddlFilterTables is set, further narrow to only those tables.
		if len(c.ddlFilterTables) > 0 {
			if _, ok := c.ddlFilterTables[table]; !ok {
				return
			}
		}
	} else {
		// Check if the schema.table matches any of our subscriptions.
		// Tables() is a pure accessor and needs no further locking.
		matchFound := false
		for _, sub := range c.subs.Snapshot() {
			for i, tsub := range sub.Tables() { // currentTable, newTable
				if tsub == nil {
					// Defensive: in-tree subscriptions never emit nil
					// entries (bufferedMap.Tables omits a nil newTable),
					// but the interface can't guarantee it for other
					// implementations, and a DDL notification must never
					// crash the stream reader.
					continue
				}
				if tsub.SchemaName == schema && tsub.TableName == table && (i == 0 || !foreignKeysOnly) {
					matchFound = true
					break
				}
			}
			if matchFound {
				break
			}
		}
		if !matchFound {
			return
		}
	}
	if c.fatalError(FatalReasonSchemaChange) {
		c.logger.Error("table definition changed, cancelling operation", "schema", schema, "table", table)
	}
}

// fatalError is called from within the readStream goroutine when a truly fatal
// condition occurs, with reason distinguishing DDL on a watched table
// (FatalReasonSchemaChange) from stream failures such as an unrecoverable
// stream error, minimal RBR detection, or a fatal rows event error
// (FatalReasonStreamError). It returns true if the caller acknowledged the
// error (i.e. the cancel function was called and acted upon).
//
// IMPORTANT: This method must NOT call Close() because Close() calls
// streamWG.Wait(), which would deadlock since readStream is the caller.
func (c *feedCore) fatalError(reason FatalReason) bool {
	if c.callerCancelFunc != nil {
		return c.callerCancelFunc(reason)
	}
	return false
}

// Stop satisfies Source. The reader goroutine keeps running — Close owns
// teardown — but stops delivering events to subscriptions, which is what makes
// it cheap enough to call inside cutover's lock window.
func (c *feedCore) Stop() {
	if c.stopped.Swap(true) {
		return
	}
	c.logger.Debug("change stream stopped; further events will not be dispatched")
}

// Close satisfies Source: it stops the reader and the periodic flush and
// waits for both to exit.
func (c *feedCore) Close() {
	c.isClosed.Store(true)

	// Read cancelFunc under c.mu — Start() writes it under the same lock.
	// We must not hold c.mu across streamWG.Wait() below: readStream
	// itself acquires c.mu from inside its loop (binlogClient:
	// setBufferedPos, recreateStreamer; gtidClient: setBufferedGTID,
	// promotePendingGTID, recreateStreamer), and holding the lock during
	// Wait would deadlock an in-flight lock acquisition there.
	c.mu.Lock()
	cancel := c.cancelFunc
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}

	// Wake any subscription parked on backpressure. Without this, readStream
	// can be stuck inside processRowsEvent → HasChanged on the soft-limit
	// cond and never observe the ctx cancel — streamWG.Wait() would block
	// forever.
	for _, sub := range c.subs.Snapshot() {
		sub.Close()
	}

	// Wait for the readStream goroutine to exit cleanly. This prevents
	// goroutine leaks detected by goleak in tests.
	// Join the independently cancellable writer too. Both background loops
	// must finish before Close returns; neither join depends on the other.
	c.StopPeriodicFlush()

	c.streamWG.Wait()

	// streamWG.Wait has returned, so readStream has exited and c.syncer
	// is no longer raced by it. Close is not expected to run concurrently
	// with Start() — the caller's sequenced-before edge (Start returned →
	// Close called) makes Start's write of c.syncer visible here without
	// further synchronization.
	if c.syncer != nil {
		c.syncer.Close()
		c.syncer = nil
	}
}

// flushUnderTableLock implements Source.FlushUnderTableLock for both clients;
// flush and blockWait are the client's own flush and BlockWait.
//
// It is a final flush under an exclusive table lock using the connection
// that holds a write lock. Because flushing generates binary log events,
// we actually want to call flush *twice*:
//   - The first time flushes the pending changes to the new table.
//   - We then ensure that we have all the binary log changes read from the server.
//   - The second time reads through the changes generated by the first flush
//     and updates the in memory applied position to match the server's position.
//     This is required so the position is updated for the c.AllChangesFlushed() check.
//
// For the GTID client the second pass is what makes the resume coordinate
// cover the GTIDs the first flush itself generated.
func (c *feedCore) flushUnderTableLock(ctx context.Context, locks []*dbconn.TableLock, flush flushFunc, blockWait func(context.Context) error) error {
	if len(locks) == 0 {
		// Flushing "under lock" without any lock would silently execute the
		// statements outside the locks the caller believes are held.
		return errors.New("FlushUnderTableLock requires at least one table lock")
	}
	if err := flush(ctx, true, locks); err != nil {
		return err
	}
	// Wait for the changes flushed to be received.
	if err := blockWait(ctx); err != nil {
		return err
	}
	// Do a final flush
	return flush(ctx, true, locks)
}

// recordFlush captures what this flush left behind (for FlushResidual) and
// how long it took on how many changes (for FeedStats). Recorded whether or
// not every change could be flushed: a flush that could not drain everything
// is exactly the case a caller watching for a feed losing ground needs to see.
//
// GetDeltaLen takes no lock of its own, so it is called before acquiring c.mu.
func (c *feedCore) recordFlush(start time.Time, batch int, complete bool) {
	residual := c.GetDeltaLen()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.flushResidual = residual
	c.flushCount++
	c.lastFlushAt = time.Now()
	c.lastFlushDuration = time.Since(start)
	c.lastFlushRows = batch
	c.lastFlushComplete = complete
}

// lastFlushWasComplete reports whether the most recent flush drained every
// subscription. Flush uses it to decide whether re-draining a backlog
// immediately would make progress or just re-defer the same keys.
func (c *feedCore) lastFlushWasComplete() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastFlushComplete
}

// FlushResidual satisfies Source.
func (c *feedCore) FlushResidual() (int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.flushResidual, c.flushCount
}

// flushUntilTrivial implements Source.Flush for both clients; flush and
// blockWait are the client's own flush and BlockWait, and readerName names the
// reader in the BlockWait-failure log line ("binlog" / "GTID").
//
// It empties the changeset in a loop until the amount of changes is considered "trivial".
// The loop is required, because changes continue to be added while the flush is occurring.
func (c *feedCore) flushUntilTrivial(ctx context.Context, flush flushFunc, blockWait func(context.Context) error, readerName string) error {
	for {
		// Repeat in a loop until the changeset length is trivial
		subs := c.subs.Snapshot()
		parks := watchParks(subs)
		if err := flush(ctx, false, nil); err != nil {
			return err
		}
		// Skip the wait entirely while the reader is not keeping up: draining
		// is both productive and the precondition for the wait ever
		// succeeding. See backlogWorthDraining — this is the case the comment
		// below used to describe as merely "a lot to do", which turned out to
		// cost 30s of idling per drain.
		//
		// pending is sampled once, so the logged figure is the one the branch
		// was decided on rather than a second reading taken next to it.
		pending := c.GetDeltaLen()
		redrainCanProgress := c.lastFlushWasComplete() || drainHitBudget(subs)
		if backlogWorthDraining(pending, parks.readerWasBlocked(subs), redrainCanProgress) {
			c.logger.Debug("reader is not keeping up, draining again instead of waiting on it",
				"pending", pending)
			continue
		}
		// BlockWait to ensure we've read everything from the server
		// into our buffer. This can timeout, in which case we start
		// a new loop. Typically a timeout occurs when we resume from a checkpoint
		// and move from the copy phase to the apply phase, and there's
		// actually a lot to do!
		if err := blockWait(ctx); err != nil {
			c.logger.Warn("error waiting for "+readerName+" reader to catch up", "error", err)
			// Check if the error is due to context cancellation
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return ctx.Err()
			}
			continue
		}
		//  If it doesn't timeout, we ensure the deltas
		// are low, and then we can break. Otherwise we continue
		// with a new loop.
		if c.GetDeltaLen() < binlogTrivialThreshold {
			break
		}
	}
	// Flush one more time, since after BlockWait()
	// there might be more changes.
	return flush(ctx, false, nil)
}

// StopPeriodicFlush stops the periodic flush goroutine started by
// StartPeriodicFlush and blocks until that goroutine has fully exited.
// Safe to call when no periodic flush is running (no-op).
// Satisfies Source interface.
func (c *feedCore) StopPeriodicFlush() {
	c.periodicFlushLock.Lock()
	cancel := c.periodicFlushCancel
	done := c.periodicFlushDone
	c.periodicFlushCancel = nil
	c.periodicFlushDone = nil
	c.periodicFlushLock.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	<-done
}

// startPeriodicFlush implements Source.StartPeriodicFlush for both clients;
// flush is the client's own flush, and changesetName names what is flushed in
// the loop's log lines ("binary log" / "GTID changeset").
//
// It starts a goroutine that periodically flushes the changeset, used by the
// migrator to advance the resume position.
// Registration of the cancel/done pair happens synchronously in the
// caller's goroutine before the loop is spawned, so a follow-up
// StopPeriodicFlush is guaranteed to observe the registration. Callers
// MUST NOT prefix with `go` — the loop is spawned internally.
//
// Calling Start while a flush is already running or after Close is a no-op.
func (c *feedCore) startPeriodicFlush(ctx context.Context, interval time.Duration, flush flushFunc, changesetName string) {
	c.periodicFlushLock.Lock()
	if c.isClosed.Load() {
		c.periodicFlushLock.Unlock()
		c.logger.Debug("ignoring periodic flush start on a closed client")
		return
	}
	if c.periodicFlushCancel != nil {
		c.periodicFlushLock.Unlock()
		return
	}
	flushCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	c.periodicFlushCancel = cancel
	c.periodicFlushDone = done
	c.periodicFlushLock.Unlock()

	go c.runPeriodicFlush(flushCtx, interval, done, flush, changesetName)
}

func (c *feedCore) runPeriodicFlush(ctx context.Context, interval time.Duration, done chan struct{}, flush flushFunc, changesetName string) {
	defer close(done)
	startMsg := "starting periodic flush of " + changesetName
	errMsg := "error flushing " + changesetName
	finishMsg := "finished periodic flush of " + changesetName
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		trigger := "interval"
		select {
		case <-ctx.Done():
			return
		case parked := <-c.flushRequests:
			// A subscription parked on its soft memory limit. Flush now:
			// the parked reader stalls binlog ingestion, and waiting out
			// the remainder of the interval burns retention headroom.
			// Flush the parked subscription first — the all-subscription
			// pass below visits the registry in nondeterministic order,
			// and draining another saturated subscription first would
			// leave the reader parked for that entire drain. The full
			// pass still runs afterwards for position advancement.
			trigger = "soft-limit-park"
			if _, err := parked.Flush(ctx, false, nil); err != nil {
				if periodicFlushStopping(ctx) {
					return
				}
				c.logger.Error("error flushing parked subscription", "error", err)
				if c.fatalError(FatalReasonFlushError) {
					return
				}
			}
		case <-ticker.C:
		}
		startLoop := time.Now()
		c.logger.Debug(startMsg, "trigger", trigger)
		// The periodic flush does not respect the throttler since we want to advance the binlog position
		// we allow this to run, and then expect that if it is under load the throttler
		// will kick in and slow down the copy-rows.
		if err := flush(ctx, false, nil); err != nil {
			if periodicFlushStopping(ctx) {
				return
			}
			c.logger.Error(errMsg, "error", err)
			// The failed changes stay buffered and the flushed position
			// stays where it is, so every later pass would fail the same
			// way while the checkpoint falls further behind the binlog
			// retention window. Stop the caller rather than carry on.
			if c.fatalError(FatalReasonFlushError) {
				return
			}
		}
		// Debug, not Info: the runner reports the same information (when the
		// last flush was, how long it took, how many rows) on its periodic
		// status block, and this loop runs often enough that logging it here
		// was one of the top contributors to log volume (#329).
		c.logger.Debug(finishMsg, "total-duration", time.Since(startLoop).String(), "trigger", trigger)
	}
}

// SetWatermarkOptimization sets both high and low watermark optimizations
// for all subscriptions. This should be disabled before checksum/cutover to
// ensure all changes are flushed regardless of watermark position.
//
// Each subscription may drain its outgoing store on the toggle (see
// bufferedMap.SetWatermarkOptimization), so this can fail with the drain
// error. If one subscription fails, subsequent subscriptions are not
// touched and the caller should treat the operation as not-yet-applied.
//
// Subscriptions are toggled against a snapshot so a long-running drain on
// one subscription doesn't block processRowsEvent from finding
// subscriptions for unrelated tables.
func (c *feedCore) SetWatermarkOptimization(ctx context.Context, newVal bool) error {
	for _, sub := range c.subs.Snapshot() {
		if err := sub.SetWatermarkOptimization(ctx, newVal); err != nil {
			return err
		}
	}
	return nil
}

// dispatchRow delivers one row change to its subscription, cooperating with any
// armed verification (see park.go). All three steps are ordered, and each one
// is wrong anywhere else:
//
//   - RowParker.Watch runs first, so a rewrite of the watched row is counted
//     before it
//     can be buffered, and therefore before any flush could carry it;
//   - the change is buffered next, so the flush the verification runs puts it on
//     the target;
//   - the reader parks before the verification is released, so nothing past this
//     event is admitted while the target is read.
//
// The rest of this event still dispatches behind the park, which is what the
// rewrite count covers; no later event does.
func (c *feedCore) dispatchRow(sub Subscription, tbl *table.TableInfo, key, image []any, deleted bool) {
	watched := c.parker.Watch(tbl, key, image, deleted)
	sub.HasChanged(key, image, deleted)
	watched.Release()
}
