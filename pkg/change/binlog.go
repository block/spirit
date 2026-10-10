package change

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

// Compile-time assertion that the binlog-backed Client satisfies Source.
var _ Source = (*binlogClient)(nil)

// binlogClient is the change.Source that resumes by (binlog-file, offset).
// NewAutoClient selects it when the source server does not have GTIDs
// enabled (or when resuming from a file:offset checkpoint); otherwise it
// selects gtidClient. Everything that does not depend on the position
// encoding lives on the embedded feedCore, shared with gtidClient.
type binlogClient struct {
	// feedCore holds the state and methods shared with gtidClient,
	// including mu, which also guards this type's position fields
	// (bufferedPos / flushedPos), streamer, and the cached binlogStatusStmt.
	feedCore

	host string

	cfg      replication.BinlogSyncerConfig
	streamer *replication.BinlogStreamer

	// The DB connection is used for queries like SHOW MASTER STATUS
	db               *sql.DB
	dbConfig         *dbconn.DBConfig
	binlogStatusStmt string // cached: "SHOW MASTER STATUS" or "SHOW BINARY LOG STATUS"

	bufferedPos mysql.Position // buffered position
	flushedPos  mysql.Position // safely written to new table

	// rotations counts binlog rotations followed by the reader. See
	// FeedStats.Rotations.
	rotations atomic.Int64

	// lastEventTime is the source's own wall-clock timestamp on the newest
	// binlog event the reader has seen, as unix seconds. Written by
	// recordEventTime from the read loop, read back by eventTime; see
	// FeedStats.BufferedEventAt.
	lastEventTime atomic.Int64

	flushedBinlogs atomic.Int64 // stall-triggered rotations reported as FeedStats.ForcedRotations
}

// NewBinlogClient constructs the binlog-backed change.Source. The
// returned Source talks to MySQL via go-mysql's BinlogSyncer; future
// alternative sources (e.g. VStream) will live behind their own
// constructors. config.Applier is required.
func NewBinlogClient(db *sql.DB, host string, username, password string, appl applier.Applier, config *ClientConfig) Source {
	if config.DBConfig == nil {
		config.DBConfig = dbconn.NewDBConfig() // default DB config
	}
	c := &binlogClient{
		db:       db,
		dbConfig: config.DBConfig,
		host:     host,
	}
	c.configure(username, password, appl, config)
	return c
}

// setBufferedPos updates the in-memory position that all changes have
// been read but not necessarily flushed. The update is monotonic:
// a position that compares less-than-or-equal to the current
// bufferedPos is silently dropped.
//
// The monotonicity matters because recreateStreamer restarts the
// binlog dump at position 4 of the current bufferedPos.Name, and
// MySQL prefaces every binlog dump with a synthetic RotateEvent whose
// `event.Position` is 4. Without the guard, that synthetic rotate
// would drag bufferedPos back to {file, 4}, and a flush that ran
// before subsequent events caught the position back up would publish
// the rewound value into flushedPos — silently regressing the
// checkpoint and forcing a large re-read on the next resume.
func (c *binlogClient) setBufferedPos(pos mysql.Position) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if pos.Compare(c.bufferedPos) <= 0 {
		return
	}
	c.bufferedPos = pos
}

// getBufferedPos returns the buffered position under a mutex.
func (c *binlogClient) getBufferedPos() mysql.Position {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bufferedPos
}

// shouldSkipReplayedEvent reports whether a replayed RowsEvent must not be
// re-delivered. An event whose end position is at or below the live
// bufferedPos is already represented (applied to the target, or held in the
// map with its latest image), so re-buffering it could regress a key to a
// stale image. No replay flag is needed: bufferedPos is monotonic and the
// live stream always runs ahead of it, so only a replay compares <=. The
// (file, pos) Compare is rotation-safe.
//
// "The live stream always runs ahead of it" is what stops holding once a
// binlog file grows past 4GiB: wrapped positions make live events compare
// at or below the frozen bufferedPos, and this function would classify
// every one of them as a replay. readStream refuses the stream before any
// such event gets here — see logPosTracker and errLogPosWrapped.
func shouldSkipReplayedEvent(eventPos, bufferedPos mysql.Position) bool {
	return eventPos.Compare(bufferedPos) <= 0
}

// AllChangesFlushed returns true if all buffered changes across all
// subscriptions have been flushed to the target tables.
// Satisfies Source interface.
func (c *binlogClient) AllChangesFlushed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.bufferedPos.Compare(c.flushedPos) > 0 {
		c.logger.Warn("Binlog reader info flushed-pos buffered-pos. Discrepancies could be due to modifications on other tables.", "flushed-pos", c.flushedPos, "buffered-pos", c.bufferedPos)
	}
	// Safe to call c.subs.Snapshot() and subscription.Length() while
	// holding c.Lock — each uses a different mutex and neither calls back
	// into the Client.
	for _, subscription := range c.subs.Snapshot() {
		if subscription.Length() > 0 {
			return false
		}
	}
	return true
}

// Position satisfies Source.
//
// It returns the safe-flushed binlog position encoded as
// "<binlog-file>:<offset>". Returns "" when no position has been
// observed yet, signaling that a fresh Start is required.
func (c *binlogClient) Position() string {
	c.mu.Lock()
	pos := c.flushedPos
	c.mu.Unlock()
	if pos.Name == "" {
		return ""
	}
	return formatBinlogPosition(pos)
}

// StartFromPosition satisfies Source.
//
// It primes flushedPos from the opaque position string previously
// returned by Position(), then begins streaming as Start would.
func (c *binlogClient) StartFromPosition(ctx context.Context, pos string) error {
	if pos == "" {
		return errors.New("StartFromPosition: empty position; use Start instead for a fresh start")
	}
	// Parse failures are wrapped with ErrPositionNotFound, mirroring the
	// GTID client: an unparseable position (e.g. a GTID-set checkpoint fed
	// to this client directly, bypassing NewAutoClient's classification)
	// can never become resumable by retrying, so callers should treat it
	// the same as a purged binlog and start fresh.
	parsed, err := parseBinlogPositionString(pos)
	if err != nil {
		return fmt.Errorf("%w: StartFromPosition: %w", ErrPositionNotFound, err)
	}
	c.mu.Lock()
	c.flushedPos = parsed
	c.mu.Unlock()
	return c.Start(ctx)
}

func (c *binlogClient) getCurrentBinlogPosition(ctx context.Context) (mysql.Position, error) {
	// We rotate the binary log before we start, so we can always safely just resume
	// by reopening the binary log file at Position 4. This is required to get the table map.
	// Why we need to recreate the syncer just after it is created is a mystery to me, but
	// we seem to have this issue in tests sometimes.
	if _, err := c.db.ExecContext(ctx, `FLUSH BINARY LOGS`); err != nil {
		return mysql.Position{}, fmt.Errorf("failed to flush binary logs: %w", err)
	}
	var binlogFile, fake string
	var binlogPos uint32
	// On the first call, try SHOW MASTER STATUS (works on MySQL 8.0, the most common version)
	// and fall back to SHOW BINARY LOG STATUS (MySQL 8.2+). Cache whichever succeeds
	// so subsequent calls don't waste a round-trip.
	if c.binlogStatusStmt == "" {
		err := c.db.QueryRowContext(ctx, "SHOW MASTER STATUS").Scan(&binlogFile, &binlogPos, &fake, &fake, &fake)
		if err == nil {
			c.binlogStatusStmt = "SHOW MASTER STATUS"
		} else {
			err = c.db.QueryRowContext(ctx, "SHOW BINARY LOG STATUS").Scan(&binlogFile, &binlogPos, &fake, &fake, &fake)
			if err == nil {
				c.binlogStatusStmt = "SHOW BINARY LOG STATUS"
			} else {
				return mysql.Position{}, err
			}
		}
	} else {
		err := c.db.QueryRowContext(ctx, c.binlogStatusStmt).Scan(&binlogFile, &binlogPos, &fake, &fake, &fake)
		if err != nil {
			return mysql.Position{}, err
		}
	}
	return mysql.Position{
		Name: binlogFile,
		Pos:  binlogPos,
	}, nil
}

// CurrentPosition satisfies Source. See the interface doc for how it differs
// from Position (in-memory feed progress vs a live server read). It delegates to
// getCurrentBinlogPosition, so it FLUSHes first — the returned position is a
// fresh binlog-file boundary (offset 4), and a feed later resuming from it
// starts at a clean file, avoiding the table-map-on-a-mid-file-offset quirk —
// and shares that helper's cached SHOW MASTER STATUS / SHOW BINARY LOG STATUS
// statement rather than duplicating the fallback and paying an extra round-trip.
func (c *binlogClient) CurrentPosition(ctx context.Context) (string, error) {
	pos, err := c.getCurrentBinlogPosition(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to read current binlog position: %w", err)
	}
	return formatBinlogPosition(pos), nil
}

// newRowsEventDecodeFunc returns the RowsEventDecodeFunc both clients install
// on their syncer: decode a rows event's header (cheap — fixed-size fields and
// a table-map lookup that resolves the schema/table name), then skip decoding
// the row images entirely unless the table has a subscription.
//
// This is where most of the stream's volume goes during a migration: spirit's
// own INSERTs into the _new table dominate the binlog while the copy runs, and
// every one of them used to be fully decoded — every column of every row,
// including JSON rendering — only for processRowsEvent to drop the event by
// table name. On a fast copy the stream cannot keep up, and the gap is repaid
// after the copy as a long catch-up phase that is ~all no-ops. Skipping the
// row-image decode leaves header parsing and network transfer as the only
// per-event costs for unsubscribed tables. (canal's table filter uses the
// same hook for the same reason.)
//
// Correctness leans on the Source lifecycle (construct → AddSubscription* →
// Start): the subscription set is complete before the first event is decoded,
// so a decode-time check is equivalent to processRowsEvent's consumption-time
// check, just earlier. processRowsEvent enforces this with a hard error if a
// subscribed table's event arrives undecoded (Rows == nil — DecodeData always
// allocates, so nil is unambiguous). Skipped events still advance the stream
// position: the header, including LogPos, is parsed as usual.
//
// The stopped flag extends the same shortcut past cutover: Stop() means no
// subscription's TableInfo is valid anymore and processRowsEvent drops
// everything, so there is nothing worth decoding.
//
// The registry has its own lock (this runs on the syncer's parse goroutine,
// not readStream). A DecodeHeader error is returned unchanged — the default
// Decode path would surface the identical error.
func newRowsEventDecodeFunc(subs *subscriptionRegistry, stopped *atomic.Bool) func(*replication.RowsEvent, []byte) error {
	return func(e *replication.RowsEvent, data []byte) error {
		pos, err := e.DecodeHeader(data)
		if err != nil {
			return err
		}
		if stopped.Load() {
			return nil
		}
		if _, ok := subs.Get(encodeSchemaTable(string(e.Table.Schema), string(e.Table.Table))); !ok {
			return nil // no subscription: leave e.Rows nil, the row images are never read
		}
		return e.DecodeData(pos, data)
	}
}

// Start initializes the binlog syncer and spawns the binlog reader
// goroutine. Returns once the reader is running; the stream itself
// continues until Close is called or ctx is cancelled.
// Satisfies Source interface.
func (c *binlogClient) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	host, portStr, err := net.SplitHostPort(c.host)
	if err != nil {
		return fmt.Errorf("failed to parse host: %w", err)
	}
	// convert portStr to a uint16
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return fmt.Errorf("failed to parse port: %w", err)
	}
	c.cfg = c.buildSyncerConfig(host, uint16(port))

	// Apply TLS configuration using the same infrastructure as main database connections
	if c.dbConfig != nil {
		tlsConfig, err := dbconn.GetTLSConfigForBinlog(c.dbConfig, host)
		if err != nil {
			return fmt.Errorf("failed to configure TLS for binlog connection: %w", err)
		}
		c.cfg.TLSConfig = tlsConfig
	}
	// Determine where to start the sync from.
	// We default from what the current position is right
	// now, but for resume cases we just need to check that the
	// position is resumable.
	if c.flushedPos.Name == "" {
		c.flushedPos, err = c.getCurrentBinlogPosition(ctx)
		if err != nil {
			return fmt.Errorf("failed to get binlog position, check binary is enabled: %w", err)
		}
	} else {
		impossible, err := binlogPositionIsImpossible(ctx, c.db, c.flushedPos.Name)
		if err != nil {
			return fmt.Errorf("could not verify binlog position: %w", err)
		}
		if impossible {
			return fmt.Errorf("%w: binlog %q is no longer on the server", ErrPositionNotFound, c.flushedPos.Name)
		}
	}
	c.bufferedPos = c.flushedPos // set buffered to the initial flushed value
	c.syncer = replication.NewBinlogSyncer(c.cfg)
	c.streamer, err = c.syncer.StartSync(c.flushedPos)
	if err != nil {
		// Close the syncer we just created so its internal goroutines exit
		// even if the caller discards the Client without calling Close.
		c.syncer.Close()
		c.syncer = nil
		return fmt.Errorf("failed to start binlog streamer: %w", err)
	}
	// Start the binlog reader in a go routine, using a context with cancel.
	// Write the cancel function to c.cancelFunc
	ctx, c.cancelFunc = context.WithCancel(ctx)
	c.streamWG.Add(1)
	go c.readStream(ctx)
	return nil
}

// recreateStreamer recreates the binlog streamer from position 4 of the
// current bufferedPos file. Used by readStream's error path to recover
// from transient stream-level read errors. Position 4 is the start of a
// binlog file; restarting there guarantees the syncer sees the
// FormatDescriptionEvent and any TableMapEvents needed to decode
// subsequent RowsEvents — MySQL does not re-send TableMaps from earlier
// in the file when serving a mid-position dump, so restarting mid-file
// would leave the parser unable to decode rows.
//
// Re-reading from position 4 replays events we already processed;
// readStream skips re-delivering any RowsEvent at or below the live
// bufferedPos (see shouldSkipReplayedEvent), so the replay cannot regress a
// key to a stale image. Events above bufferedPos are new and safe to
// re-buffer because the applier is idempotent (REPLACE INTO).
func (c *binlogClient) recreateStreamer() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.logger.Info("recreateStreamer called",
		"buffered_position", c.bufferedPos,
		"flushed_position", c.flushedPos,
		"syncer_exists", c.syncer != nil,
		"streamer_exists", c.streamer != nil)

	// Close the existing syncer completely
	// Since we can't do anything with it.
	if c.syncer != nil {
		c.syncer.Close()
	}

	newStartPos := mysql.Position{
		Name: c.bufferedPos.Name,
		Pos:  4, // Binlog files always start at position 4
	}
	c.logger.Info("Recreating streamer from file start",
		"file", c.bufferedPos.Name,
		"previous_position", c.bufferedPos.Pos,
		"new_start_position", newStartPos,
	)

	c.syncer = replication.NewBinlogSyncer(c.cfg)
	var err error
	c.streamer, err = c.syncer.StartSync(newStartPos)
	if err != nil {
		c.logger.Error("Failed to start binlog streamer in recreateStreamer",
			"error", err,
			"position", newStartPos,
			"config", fmt.Sprintf("host=%s:%d user=%s", c.cfg.Host, c.cfg.Port, c.cfg.User))
		return fmt.Errorf("failed to start binlog streamer: %w", err)
	}
	return nil
}

// readStream continuously reads the binlog stream. It is usually called in a go routine.
// It will read the stream until the context is closed
// *and* it continues on any errors
func (c *binlogClient) readStream(ctx context.Context) {
	defer c.streamWG.Done() // Signal completion when goroutine exits

	c.mu.Lock()
	currentLogName := c.flushedPos.Name
	startPos := c.flushedPos // Copy while holding lock
	c.mu.Unlock()

	consecutiveErrors := 0
	recreateAttempts := 0
	backoffDuration := initialBackoffDuration
	lastErrorTime := time.Time{}
	var recentErrors []string // Track recent errors for debugging
	// Watches for the 4-byte LogPos wrapping past 4GiB within one file,
	// which would make every later event compare below bufferedPos and be
	// discarded as a replay. See logPosTracker and errLogPosWrapped.
	var logPos logPosTracker

	c.logger.Debug("readStream started for binlog position", "position", startPos, "log_name", currentLogName)

	for {
		// Check if context is done before processing
		select {
		case <-ctx.Done():
			c.logger.Debug("readStream context cancelled", "error", ctx.Err())
			return // stop processing
		default:
		}

		var ev *replication.BinlogEvent
		var err error

		// If streamer is nil (such as after a failed recreation), treat it as an error
		// This will then trigger the recreation
		if c.streamer == nil {
			err = errors.New("binlog streamer is nil, cannot read events")
		} else {
			// Read the next event from the stream
			ev, err = c.streamer.GetEvent(ctx)
		}

		if err != nil {
			// We only stop processing for context cancelled errors.
			if errors.Is(err, context.Canceled) || ctx.Err() != nil || c.isClosed.Load() {
				return // stop processing
			}

			consecutiveErrors++
			currentTime := time.Now()

			// Track recent errors for debugging (keep last 20)
			errorMsg := fmt.Sprintf("[%s] %v", currentTime.Format("15:04:05.000"), err)
			recentErrors = append(recentErrors, errorMsg)
			if len(recentErrors) > 20 {
				recentErrors = recentErrors[1:]
			}

			c.logger.Error("error reading binlog stream", "consecutive_errors", consecutiveErrors, "error", err, "current_position", c.getBufferedPos())

			// If we've had too many consecutive errors, try to recreate the streamer
			if consecutiveErrors >= maxConsecutiveErrors {
				recreateAttempts++

				// Get current state information for debugging
				currentPos := c.getBufferedPos()

				c.logger.Warn("Too many consecutive errors, attempting to recreate streamer",
					"consecutive_errors", consecutiveErrors,
					"attempt", recreateAttempts,
					"max_attempts", maxRecreateAttempts,
					"current_position", currentPos,
					"backoff_duration", backoffDuration)

				// Check if we've exceeded the maximum number of recreation attempts
				if recreateAttempts >= maxRecreateAttempts {
					c.logger.Error("failed to recreate binlog streamer, giving up",
						"total_attempts", recreateAttempts,
						"current_position", currentPos,
						"start_position", startPos,
						"recent_errors", recentErrors,
						"is_closed", c.isClosed.Load())

					c.fatalError(FatalReasonStreamError)
					return
				}

				// Apply exponential backoff
				if currentTime.Sub(lastErrorTime) < backoffDuration {
					c.logger.Info("Backing off before recreating streamer", "duration", backoffDuration.String())
					backoffTimer := time.NewTimer(backoffDuration)
					select {
					case <-ctx.Done():
						backoffTimer.Stop()
						return
					case <-backoffTimer.C:
					}
				}

				// Try to recreate the streamer
				if recreateErr := c.recreateStreamer(); recreateErr != nil {
					c.logger.Error("Failed to recreate streamer", "error", recreateErr)

					// Increase backoff duration for next attempt
					backoffDuration *= backoffMultiplier
					if backoffDuration > maxBackoffDuration {
						backoffDuration = maxBackoffDuration
					}
				} else {
					// Successfully recreated, reset counters
					consecutiveErrors = 0
					recreateAttempts = 0
					backoffDuration = initialBackoffDuration
				}
				lastErrorTime = currentTime
			}

			// Short sleep before retrying
			retryTimer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-ctx.Done():
				retryTimer.Stop()
				return
			case <-retryTimer.C:
			}
			continue
		}

		// Reset error counters on successful read
		if consecutiveErrors > 0 {
			c.logger.Info("Binlog stream recovered after consecutive errors", "consecutive_errors", consecutiveErrors)
			consecutiveErrors = 0
			backoffDuration = initialBackoffDuration
		}

		if ev == nil {
			continue
		}
		// Hold here if a verification has parked the reader. The event is
		// already read but not acted on, so nothing is consumed and lost;
		// dispatch resumes with it once the gate opens. See park.go.
		if err := c.parker.Wait(ctx); err != nil {
			return
		}
		// Stamp before the switch, not inside it: RotateEvent `continue`s out
		// below, and one call site per client is what keeps the two clients
		// from drifting on this. The published position is at most one
		// transaction behind the event stamped here.
		recordEventTime(&c.lastEventTime, ev.Header.Timestamp)
		// Check for LogPos wraparound before the event is acted on, so no
		// row event from beyond the wrap is ever buffered: past the wrap
		// the replay-skip guard below cannot tell a live event from a
		// replayed one, and the position we would checkpoint is no longer
		// a coordinate we could resume from.
		if logPos.observe(ev) {
			c.logger.Error("fatal error reading binlog stream", "error", errLogPosWrapped,
				"file", currentLogName,
				"event_log_pos", ev.Header.LogPos,
				"previous_log_pos", logPos.last,
				"buffered_position", c.getBufferedPos())
			c.fatalError(FatalReasonLogPosWrapped)
			return
		}
		// Handle the event.
		switch event := ev.Event.(type) {
		case *replication.RotateEvent:
			// Rotate event, update the current log name.
			// Count only real rotations: the server sends the rotate event
			// from the binlog followed by an artificial one carrying the
			// same position, and recreateStreamer re-opens the current file
			// with another synthetic rotate. Comparing against the file we
			// are already reading collapses all of those to one count.
			if string(event.NextLogName) != currentLogName {
				c.rotations.Add(1)
			}
			currentLogName = string(event.NextLogName)
			// Positions restart in the file we are rotating into, and the
			// server opens every dump (including recreateStreamer's) with
			// an artificial rotate — so this is also what keeps a replay
			// from position 4 from looking like LogPos wraparound.
			logPos.rotated()
			// For RotateEvent, we must use event.Position (the position in the NEW log)
			// not ev.Header.LogPos (which is the position in the OLD log).
			// Update position immediately and skip the generic position update at the end.
			c.setBufferedPos(mysql.Position{
				Name: currentLogName,
				Pos:  uint32(event.Position),
			})
			continue
		case *replication.RowsEvent:
			// Skip events already buffered/applied: a post-recreateStreamer
			// replay must not re-buffer them and regress a key to a stale
			// image. See shouldSkipReplayedEvent.
			eventPos := mysql.Position{Name: currentLogName, Pos: ev.Header.LogPos}
			if shouldSkipReplayedEvent(eventPos, c.getBufferedPos()) {
				c.logger.Debug("skipping replayed rows event at or below the buffered position",
					"event_position", eventPos)
				continue
			}
			if err = c.processRowsEvent(ev, event); err != nil {
				c.logger.Error("fatal error processing binlog rows event", "error", err)
				c.fatalError(FatalReasonStreamError)
				return
			}
		case *replication.QueryEvent:
			info, err := parseQueryEvent(string(event.Schema), string(event.Query))
			if err != nil {
				// An unparseable statement may use a SQL mode or syntax newer
				// than the parser. Do not log the query: it may contain data.
				c.logger.Error("Skipping query that was unable to parse", "file", currentLogName, "pos", ev.Header.LogPos)
				continue
			}
			// Any XA statement fails the stream: spirit does not support
			// XA workloads. An XA transaction's row events are binlogged
			// at XA PREPARE time, before its outcome is known — applying
			// them treats the prepare as a commit, and a later XA ROLLBACK
			// has no binlog representation that could undo them. "XA START"
			// opens the group ahead of its row events, so failing here
			// guarantees none of them are ever buffered, let alone flushed.
			// See the matching guard in the GTID client's processQueryEvent
			// for the full rationale and group shape.
			if info.xa {
				c.logger.Error("fatal error processing binlog query event", "error", errXAUnsupported)
				c.fatalError(FatalReasonUnsupportedXA)
				return
			}
			// Query event, check if it is a DDL statement,
			// in which case we need to notify the caller.
			c.processDDLTables(info)
		case *replication.TransactionPayloadEvent:
			// binlog_transaction_compression=ON wraps an entire transaction —
			// the BEGIN QueryEvent, TableMapEvents, row events and the
			// XIDEvent — in one compressed payload event; only the GTID event
			// stays outside. go-mysql has already decompressed and re-parsed
			// the inner events into event.Events (with our decode options
			// inherited), so we process them exactly as if they had arrived
			// uncompressed. Letting this fall through to the default case
			// would silently drop every change in the transaction.
			//
			// Inner event headers hold offsets into the uncompressed
			// transaction cache, NOT file positions, so they must never feed
			// position tracking: the replay-skip check below covers the whole
			// payload using the outer end position, and bufferedPos advances
			// only via the outer header in the generic update after the
			// switch — once every inner event has been buffered. That makes
			// replay all-or-nothing, which is safe because bufferedPos only
			// ever lands on whole-payload boundaries.
			eventPos := mysql.Position{Name: currentLogName, Pos: ev.Header.LogPos}
			if shouldSkipReplayedEvent(eventPos, c.getBufferedPos()) {
				c.logger.Debug("skipping replayed transaction payload event at or below the buffered position",
					"event_position", eventPos)
				continue
			}
			if err = c.processTransactionPayload(event, eventPos); err != nil {
				c.logger.Error("fatal error processing binlog transaction payload event", "error", err)
				c.fatalError(fatalReasonForStreamError(err))
				return
			}
		case *replication.GTIDEvent,
			*replication.TableMapEvent,
			*replication.XIDEvent,
			*replication.FormatDescriptionEvent,
			*replication.PreviousGTIDsEvent:
			// Known stream-housekeeping events. We don't act on them here; the
			// position is still advanced via the LogPos update below. They are
			// listed explicitly (rather than handled by the default case) so the
			// default can keep logging genuinely unknown event types — a future
			// row-event variant we don't recognize could otherwise cause silent
			// data loss.
		case *replication.GenericEvent:
			// Event types without a dedicated go-mysql decoder surface as
			// GenericEvent; the header carries the real type. An
			// XA_PREPARE_LOG_EVENT terminates an XA transaction's first
			// binlog group (it is also how the server logs
			// `XA COMMIT ... ONE PHASE`), and spirit does not support XA
			// workloads. The QueryEvent guard above already fails the
			// stream at the group's opening "XA START", before any of its
			// row events are buffered, so this branch is defense in depth
			// in case a future server version reshapes the group.
			if ev.Header.EventType == replication.XA_PREPARE_LOG_EVENT {
				c.logger.Error("fatal error processing binlog stream", "error", errXAUnsupported)
				c.fatalError(FatalReasonUnsupportedXA)
				return
			}
			c.logger.Debug("Received unknown event type", "type", ev.Header.EventType.String())
		default:
			c.logger.Debug("Received unknown event type", "type", fmt.Sprintf("%T", ev.Event))
		}
		// Update the buffered position under a mutex. Some events
		// (FormatDescriptionEvent and similar housekeeping events) have
		// LogPos=0 and don't represent a real position. setBufferedPos
		// itself enforces monotonicity, so we don't filter further here.
		if ev.Header.LogPos > 0 {
			c.setBufferedPos(mysql.Position{
				Name: currentLogName,
				Pos:  ev.Header.LogPos,
			})
		}
	}
}

// processRowsEvent processes a RowsEvent. It looks up the subscription
// for the event's table and dispatches per-row HasChanged calls.
//
//   - If there is no subscription, the event is ignored.
//   - Otherwise we call HasChanged for each affected key.
//
// The subscription lookup goes through c.subs (its own RWMutex); c.Lock
// is not held here. That's load-bearing for backpressure: bufferedMap.
// HasChanged can park on its own condition variable when the buffer is
// full, and a c.Lock held across the park would block c.flush() — the
// very flush that drains the buffer and would wake the parker.
//
// We require binlog_row_image=FULL on the source. With FULL each row
// (before and after image alike) contains every column, so PK extraction
// works the same way for all event types and no reconstruction is needed.
// If a MINIMAL image slips through we error out.
func (c *binlogClient) processRowsEvent(ev *replication.BinlogEvent, e *replication.RowsEvent) error {
	if c.stopped.Load() {
		// Post-cutover. The subscription's TableInfo no longer describes the
		// table this event's name resolves to, so decoding it would fail on a
		// row image that is perfectly valid for the table that produced it.
		// See Source.Stop.
		return nil
	}
	subName := encodeSchemaTable(string(e.Table.Schema), string(e.Table.Table))
	sub, ok := c.subs.Get(subName)
	if !ok {
		return nil // ignore event, it could be to a _new table.
	}
	if e.Rows == nil {
		// The decode-time filter (newRowsEventDecodeFunc) skipped this event's
		// row images because the table had no subscription when the event was
		// parsed — yet one exists now. That means AddSubscription was called
		// after Start, violating the Source lifecycle, and silently treating
		// the event as empty would lose rows. DecodeData always allocates
		// e.Rows, so nil cannot be a legitimately decoded event.
		return fmt.Errorf("rows event for subscribed table %s arrived with undecoded rows: subscriptions must be added before Start (see Source lifecycle)", subName)
	}

	if isMinimalRowImage(e) {
		return fmt.Errorf("received a minimal RBR event for table %s.%s, but we require binlog_row_image=FULL on the source server", string(e.Table.Schema), string(e.Table.Table))
	}

	eventType := parseEventType(ev.Header.EventType)
	if eventType == eventTypeUnknown {
		// Hard-fail, mirroring the minimal-row-image check above. go-mysql
		// parses several rows-event subtypes we don't recognize into a
		// plain *replication.RowsEvent — PARTIAL_UPDATE_ROWS_EVENT, which
		// the server emits once binlog_row_value_options=PARTIAL_JSON is
		// set, is the live one (the preflight check reads the global, and
		// the global can change afterwards). Such an event still carries
		// row changes for a table we are subscribed to, so dropping it with
		// only an error log loses them silently; they would surface, if at
		// all, as a checksum mismatch at the end of the run.
		return fmt.Errorf("%w for table %s.%s", unsupportedRowsEventError(ev.Header.EventType), string(e.Table.Schema), string(e.Table.Table))
	}

	tbl := sub.Tables()[0]

	// Decode ENUM ordinals / SET bitmasks back to their string form and
	// re-pad BINARY(N) values (MySQL strips trailing 0x00 from the row
	// image) before we hand the row image to the subscription. Without
	// this the applier would insert ENUM/SET integers as literal values
	// on migrated columns, and replay short BINARY values into targets
	// that don't re-pad (e.g. VARBINARY). Padding must happen before
	// PrimaryKeyValues below so binary PK keys match what a SELECT
	// returns. See TableInfo.DecodeBinlogRow.
	if tbl.NeedsBinlogRowDecoding() {
		for _, row := range e.Rows {
			if err := tbl.DecodeBinlogRow(row); err != nil {
				return fmt.Errorf("decoding binlog row for %s.%s: %w", tbl.SchemaName, tbl.TableName, err)
			}
		}
	}

	if eventType == eventTypeUpdate {
		// UPDATE events always carry before/after image pairs.
		immutableOrdinal := sub.ImmutableColumnOrdinal()
		for i := 0; i < len(e.Rows); i += 2 {
			beforeRow := e.Rows[i]
			afterRow := e.Rows[i+1]

			beforeKey, err := tbl.PrimaryKeyValues(beforeRow)
			if err != nil {
				return err
			}
			afterKey, err := tbl.PrimaryKeyValues(afterRow)
			if err != nil {
				return err
			}

			// Sharded operations track changes by PRIMARY KEY only, so an
			// UPDATE to the sharding (vindex) column would leave a stale
			// copy of the row on its old shard. The subscription declares
			// the column immutable and we fail the stream fatally instead.
			if err := checkImmutableColumn(tbl, immutableOrdinal, beforeRow, afterRow, beforeKey); err != nil {
				return err
			}

			if pkChanged(beforeKey, afterKey) {
				c.dispatchRow(sub, tbl, beforeKey, nil, true)      // delete old PK
				c.dispatchRow(sub, tbl, afterKey, afterRow, false) // insert new PK
			} else {
				c.dispatchRow(sub, tbl, beforeKey, afterRow, false)
			}
		}
		return nil
	}

	// INSERT and DELETE: one row per entry.
	for _, row := range e.Rows {
		key, err := tbl.PrimaryKeyValues(row)
		if err != nil {
			return err
		}
		switch eventType { //nolint:exhaustive
		case eventTypeInsert:
			c.dispatchRow(sub, tbl, key, row, false)
		case eventTypeDelete:
			c.dispatchRow(sub, tbl, key, nil, true)
		default:
			// Unreachable today: eventTypeUnknown is rejected above and
			// eventTypeUpdate returned earlier. Kept as a hard error so a
			// future eventType addition cannot silently drop rows here.
			return fmt.Errorf("%w for table %s.%s", unsupportedRowsEventError(ev.Header.EventType), string(e.Table.Schema), string(e.Table.Table))
		}
	}
	return nil
}

// processTransactionPayload processes the events decompressed from a
// TransactionPayloadEvent (binlog_transaction_compression=ON, settable
// per-session by any client regardless of the global value the preflight
// checks). The payload carries the whole transaction except its GTID
// event: the BEGIN QueryEvent, TableMapEvents, row events and the XIDEvent
// terminator. RowsEvents dispatch to subscriptions and QueryEvents go
// through DDL detection, mirroring their uncompressed equivalents in
// readStream. payloadPos is the outer event's end position and is used
// only for log messages — inner headers hold transaction-cache offsets
// that must not be mistaken for file positions.
func (c *binlogClient) processTransactionPayload(e *replication.TransactionPayloadEvent, payloadPos mysql.Position) error {
	for _, inner := range e.Events {
		switch innerEvent := inner.Event.(type) {
		case *replication.RowsEvent:
			// No per-event replay check here: readStream already skipped the
			// whole payload if its outer end position was at or below
			// bufferedPos, and bufferedPos never lands inside a payload.
			if err := c.processRowsEvent(inner, innerEvent); err != nil {
				return err
			}
		case *replication.QueryEvent:
			info, err := parseQueryEvent(string(innerEvent.Schema), string(innerEvent.Query))
			if err != nil {
				c.logger.Error("Skipping query inside transaction payload that was unable to parse",
					"file", payloadPos.Name, "pos", payloadPos.Pos)
				continue
			}
			// XA statements fail the payload before any of its row events
			// are buffered — see the guard in readStream's QueryEvent case.
			// A compressed XA prepare group opens with an inner "XA START"
			// QueryEvent, so this fires ahead of the group's RowsEvents.
			if info.xa {
				return errXAUnsupported
			}
			// Usually the transaction's BEGIN, which parses cleanly and
			// yields no DDL tables. Unparseable statements are skipped the
			// same way readStream skips them.
			c.processDDLTables(info)
		case *replication.TableMapEvent, *replication.XIDEvent:
			// Housekeeping inside the payload. The TableMapEvents were
			// already consumed by go-mysql's inner parser to decode the
			// RowsEvents above; position tracking advances via the outer
			// event only.
		case *replication.GenericEvent:
			// An inner XA_PREPARE_LOG_EVENT terminates a compressed XA
			// prepare group. The inner "XA START" QueryEvent above already
			// fails the payload before its row events are buffered; this is
			// defense in depth, mirroring readStream's GenericEvent case.
			if inner.Header.EventType == replication.XA_PREPARE_LOG_EVENT {
				return errXAUnsupported
			}
			c.logger.Debug("Received unknown event type inside transaction payload", "type", inner.Header.EventType.String())
		default:
			// Same rationale as readStream's default case: log genuinely
			// unknown inner event types so a future row-event variant can't
			// cause silent data loss without a trace.
			c.logger.Debug("Received unknown event type inside transaction payload", "type", fmt.Sprintf("%T", inner.Event))
		}
	}
	return nil
}

// FlushUnderTableLock satisfies Source. See feedCore.flushUnderTableLock for
// the two-pass flush it performs.
func (c *binlogClient) FlushUnderTableLock(ctx context.Context, locks []*dbconn.TableLock) error {
	return c.flushUnderTableLock(ctx, locks, c.flush, c.BlockWait)
}

// Flush satisfies Source. See feedCore.flushUntilTrivial.
func (c *binlogClient) Flush(ctx context.Context) error {
	return c.flushUntilTrivial(ctx, c.flush, c.BlockWait, "binlog")
}

// StartPeriodicFlush satisfies Source. See feedCore.startPeriodicFlush;
// callers MUST NOT prefix with `go`.
func (c *binlogClient) StartPeriodicFlush(ctx context.Context, interval time.Duration) {
	c.startPeriodicFlush(ctx, interval, c.flush, "binary log")
}

// flush is a low level flush, that asks all of the subscriptions to flush
// Some of these will flush a delta map, others will flush a queue.
//
// Note: we yield the lock early because otherwise no new events can be sent
// to the subscriptions while we are flushing.
// This means that the actual buffered position might be slightly ahead by
// the end of the flush. That's OK, we only set the flushed position to the known
// safe buffered position taken at the start.
func (c *binlogClient) flush(ctx context.Context, underLock bool, locks []*dbconn.TableLock) error {
	// Sampled before the flush starts: this is the batch size the flush is
	// about to work through. GetDeltaLen takes no lock of its own, so it is
	// called before acquiring c.mu.
	start := time.Now()
	batch := c.GetDeltaLen()
	c.mu.Lock()
	newFlushedPos := c.bufferedPos
	c.mu.Unlock()
	var allChangesFlushed = true
	for _, subscription := range c.subs.Snapshot() {
		flushed, err := subscription.Flush(ctx, underLock, locks)
		if err != nil {
			return err
		}
		if !flushed {
			allChangesFlushed = false
		}
	}
	// If there is a scenario where a key couldn't be flushed because it wasn't
	// below the watermark, then we need to skip advancing the checkpoint.
	// TODO: This could lead to a starvation issue under contention where
	// the checkpoint never advances. The longterm fix for this is that we
	// would need to track the minimum binlog position that applies to a key.
	// We could then advance up to just below the lowest key that couldn't be flushed.
	// This is a little bit complicated, so for now we just accept that in some
	// high contention scenarios the binlog position in the checkpoint
	// won't advance.
	// Another potential fix is that we disable the belowLowWatermark optimization
	// for these high contention cases. But that's not a great solution either,
	// because the low watermark optimization helps a lot in these cases because
	// it reduces contention between the copier and the repl applier.
	if allChangesFlushed {
		c.mu.Lock()
		// Monotonic, mirroring setBufferedPos: if two flushes were ever to
		// overlap, the later-finishing one could hold an older snapshot of
		// bufferedPos, and storing it unconditionally would regress the
		// resume position. Every current caller serializes flushes, so this
		// guards the invariant rather than fixing a live bug.
		if newFlushedPos.Compare(c.flushedPos) > 0 {
			c.flushedPos = newFlushedPos
		}
		c.mu.Unlock()
	}
	c.recordFlush(start, batch, allChangesFlushed)
	return nil
}

// FeedStats satisfies Source, so the runner can fold the feed's
// activity into the binlog row of its periodic status block.
func (c *binlogClient) FeedStats() FeedStats {
	// Collected before c.mu is taken: these lock each subscription, and the
	// subscriptions take c.mu on their flush paths.
	var stats FeedStats
	stats.MergeSubscriptions(c.subs.Snapshot())

	c.mu.Lock()
	defer c.mu.Unlock()
	stats.LastFlushAt = c.lastFlushAt
	stats.LastFlushDuration = c.lastFlushDuration
	stats.LastFlushRows = c.lastFlushRows
	stats.Rotations = c.rotations.Load()
	stats.ForcedRotations = c.flushedBinlogs.Load()
	stats.BufferedEventAt = eventTime(&c.lastEventTime)
	// Already under c.mu, which is what guards bufferedPos.
	if c.bufferedPos.Name != "" {
		stats.BufferedPosition = formatBinlogPosition(c.bufferedPos)
	}
	return stats
}

// BlockWait blocks until all changes are *buffered*.
// i.e. the server's current position is 1234, but our buffered position
// is only 100. We need to read all the events until we reach >= 1234.
// We do not need to guarantee that they are flushed though, so
// you need to call Flush() to do that. This call times out!
// The default timeout is 10 seconds, after which an error will be returned.
// Satisfies Source interface.
func (c *binlogClient) BlockWait(ctx context.Context) error {
	return c.blockWait(ctx, DefaultTimeout)
}

// blockWait accepts a budget so timeout diagnostics can be exercised without a
// thirty-second test or mutation of shared configuration.
func (c *binlogClient) blockWait(ctx context.Context, timeout time.Duration) error {
	targetPos, err := c.getCurrentBinlogPosition(ctx)
	if err != nil {
		return err
	}
	// Info only when there is actually a gap to close. Flush() calls BlockWait
	// in a loop until the delta count is trivial, and most of those calls find
	// the buffered position already at or past the target — at Info those
	// printed several times per second during applyChangeset, every one of
	// them reporting target_position == current_position and so carrying no
	// information (#329). A real wait still says so at Info, which is the
	// reading this line is kept for.
	bufferedPos := c.getBufferedPos()
	logCatchUp := c.logger.Debug
	if bufferedPos.Compare(targetPos) < 0 {
		logCatchUp = c.logger.Info
	}
	logCatchUp("waiting to catch up to source position", "target_position", targetPos, "current_position", bufferedPos)
	timer := time.NewTimer(timeout)
	defer timer.Stop() // Ensure timer is always stopped to prevent goroutine leak

	prevPos := c.getBufferedPos()
	stalls := blockWaitStalls{}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return fmt.Errorf("timed out waiting to catch up to source position: %v, current position is: %v, started at: %v; %s", targetPos, c.getBufferedPos(), bufferedPos, catchUpDiagnostics(c.subs.Snapshot()))
		default:
			currPos := c.getBufferedPos()
			if stalls.observe(prevPos, currPos) {
				c.logger.Debug("buffered position has not advanced, flushing binary logs")
				if err := dbconn.Exec(ctx, c.db, "FLUSH BINARY LOGS"); err != nil {
					return err
				}
				c.flushedBinlogs.Add(1)
			}
			prevPos = currPos

			if c.getBufferedPos().Compare(targetPos) >= 0 {
				return nil // we are up to date!
			}

			// We are not caught up yet, so we need to wait.
			time.Sleep(blockWaitSleep)
		}
	}
}

// formatBinlogPosition encodes a mysql.Position as the opaque string
// returned by binlogClient.Position(). The format is "<binlog-file>:<offset>".
func formatBinlogPosition(p mysql.Position) string {
	return p.Name + ":" + strconv.FormatUint(uint64(p.Pos), 10)
}

// parseBinlogPositionString is the inverse of formatBinlogPosition.
// It splits on the LAST ':' so binlog file names that happen to contain
// a ':' (unusual but possible) round-trip cleanly. Returns an error if
// the offset portion does not parse as a uint32.
func parseBinlogPositionString(s string) (mysql.Position, error) {
	idx := strings.LastIndex(s, ":")
	if idx <= 0 || idx == len(s)-1 {
		return mysql.Position{}, fmt.Errorf("malformed position %q: expected <binlog-file>:<offset>", s)
	}
	name := s[:idx]
	offsetStr := s[idx+1:]
	offset, err := strconv.ParseUint(offsetStr, 10, 32)
	if err != nil {
		return mysql.Position{}, fmt.Errorf("malformed position %q: offset is not a uint32: %w", s, err)
	}
	return mysql.Position{Name: name, Pos: uint32(offset)}, nil
}

// VerifyRowAtNextChange satisfies Source. The parker owns the ordering; all
// this supplies is the inner drain, which must not be Flush (see
// RowParker.Verify).
func (c *binlogClient) VerifyRowAtNextChange(ctx context.Context, watch RowWatch, verify RowVerifier) error {
	return c.parker.Verify(ctx, watch, verify,
		func(ctx context.Context) error { return c.flush(ctx, false, nil) },
		c.AllChangesFlushed)
}
