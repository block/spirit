package repl

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

type Client struct {
	// mu protects position fields (bufferedPos / flushedPos), the
	// streamer / syncer / cancelFunc tuple, and the cached
	// binlogStatusStmt. Subscriptions live in c.subs with its own
	// RWMutex. Named (not embedded) so the lock surface stays
	// package-internal: sync.Mutex is not re-entrant, and exposing
	// public Lock/Unlock on an external API invites accidental
	// self-deadlocks from a caller that doesn't know what's already held.
	mu sync.Mutex

	host     string
	username string
	password string

	cfg      replication.BinlogSyncerConfig
	syncer   *replication.BinlogSyncer
	streamer *replication.BinlogStreamer

	// The DB connection is used for queries like SHOW MASTER STATUS
	db               *sql.DB
	applier          applier.Applier
	dbConfig         *dbconn.DBConfig
	binlogStatusStmt string // cached: "SHOW MASTER STATUS" or "SHOW BINARY LOG STATUS"

	// subs owns the table-keyed subscription set and its own lock. See
	// subscriptionRegistry. The Client mutex above does NOT cover map
	// access; reach the subscriptions only through these methods.
	subs *subscriptionRegistry

	// callerCancelFunc is an optional callback that is called when a DDL
	// change is detected on a subscribed table, or when a fatal stream
	// error occurs. The caller is expected to handle cancellation and
	// cleanup in this callback. It returns true if the error was acted
	// upon (i.e. the caller actually cancelled), or false if it was
	// ignored (e.g. because the migration is already past cutover).
	callerCancelFunc func() bool
	ddlFilterSchema  string
	ddlFilterTables  map[string]struct{}

	serverID    uint32         // server ID for the binlog reader
	bufferedPos mysql.Position // buffered position
	flushedPos  mysql.Position // safely written to new table

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

	flushedBinlogs atomic.Int64 // for testing binlog flushing frequency
}

// NewClient creates a new Client instance.
// config.Applier is required!
func NewClient(db *sql.DB, host string, username, password string, appl applier.Applier, config *ClientConfig) *Client {
	if config.DBConfig == nil {
		config.DBConfig = dbconn.NewDBConfig() // default DB config
	}
	softLimit := config.SubscriptionSoftLimitBytes
	if softLimit == 0 {
		softLimit = DefaultSubscriptionSoftLimitBytes
	} else if softLimit < 0 {
		softLimit = 0 // explicit opt-out
	}
	return &Client{
		db:                         db,
		dbConfig:                   config.DBConfig,
		host:                       host,
		username:                   username,
		password:                   password,
		logger:                     config.Logger,
		subs:                       newSubscriptionRegistry(),
		callerCancelFunc:           config.CancelFunc,
		ddlFilterSchema:            config.DDLFilterSchema,
		ddlFilterTables:            toSet(config.DDLFilterTables),
		serverID:                   config.ServerID,
		applier:                    appl,
		subscriptionSoftLimitBytes: softLimit,
	}
}

// AddSubscription adds a new subscription.
// Returns an error if a subscription already exists for the given table.
func (c *Client) AddSubscription(currentTable, newTable *table.TableInfo, chunker table.MappedChunker) error {
	subKey := encodeSchemaTable(currentTable.SchemaName, currentTable.TableName)
	// If the PK is not memory comparable we still use the buffered map, but it
	// needs to know that when the watermark optimizations are disabled
	// (i.e. we've finished copying and we're about to start checksum),
	// that it needs to act like a FIFO queue. This is a requirement because of edge
	// cases caused by collations since A == a, but in our map they would
	// not compare as equal.
	if !c.subs.AddBuffered(subKey, &bufferedMap{
		table:                currentTable,
		newTable:             newTable,
		changes:              make(map[string]bufferedChange),
		c:                    c,
		chunker:              chunker,
		applier:              c.applier,
		pkIsMemoryComparable: currentTable.PrimaryKeyIsMemoryComparable() == nil,
		softLimitBytes:       c.subscriptionSoftLimitBytes,
	}) {
		return fmt.Errorf("subscription already exists for table %s.%s", currentTable.SchemaName, currentTable.TableName)
	}
	return nil
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
// the rewound value via SetFlushedPos — silently regressing the
// checkpoint and forcing a large re-read on the next resume.
func (c *Client) setBufferedPos(pos mysql.Position) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if pos.Compare(c.bufferedPos) <= 0 {
		return
	}
	c.bufferedPos = pos
}

// getBufferedPos returns the buffered position under a mutex.
func (c *Client) getBufferedPos() mysql.Position {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bufferedPos
}

// SetFlushedPos updates the known safe position that all changes have been flushed.
// It is used for resuming from a checkpoint.
func (c *Client) SetFlushedPos(pos mysql.Position) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.flushedPos = pos
}

func (c *Client) AllChangesFlushed() bool {
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

func (c *Client) GetBinlogApplyPosition() mysql.Position {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.flushedPos
}

// GetDeltaLen returns the total number of changes
// that are pending across all subscriptions.
func (c *Client) GetDeltaLen() int {
	deltaLen := 0
	for _, subscription := range c.subs.Snapshot() {
		deltaLen += subscription.Length()
	}
	return deltaLen
}

func (c *Client) getCurrentBinlogPosition(ctx context.Context) (mysql.Position, error) {
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

// Run initializes the binlog syncer and starts the binlog reader.
// It returns an error if the initialization fails.
func (c *Client) Run(ctx context.Context) error {
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
	c.cfg = replication.BinlogSyncerConfig{
		ServerID: c.serverID,
		Flavor:   "mysql",
		Host:     host,
		Port:     uint16(port),
		User:     c.username,
		Password: c.password,
		Logger:   c.logger,
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
	}

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
			return errors.New("binlog position is impossible, the source may have already purged it")
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
// Re-reading events from position 4 is safe because the applier is
// idempotent: it uses REPLACE INTO so re-applying any already-applied
// event simply re-inserts the same row image, and re-applying an
// out-of-order event transiently sets the destination to that row's
// older state, which a subsequent binlog event (in this or a later
// flush) will correct. The eventual-consistency property holds in both
// map mode and queue mode — see UpsertRows in pkg/applier/single_target.go.
func (c *Client) recreateStreamer() error {
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
func (c *Client) readStream(ctx context.Context) {
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

					c.fatalError()
					return
				}

				// Apply exponential backoff
				if currentTime.Sub(lastErrorTime) < backoffDuration {
					c.logger.Info("Backing off before recreating streamer", "duration", backoffDuration)
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
		// Handle the event.
		switch event := ev.Event.(type) {
		case *replication.RotateEvent:
			// Rotate event, update the current log name.
			currentLogName = string(event.NextLogName)
			// For RotateEvent, we must use event.Position (the position in the NEW log)
			// not ev.Header.LogPos (which is the position in the OLD log).
			// Update position immediately and skip the generic position update at the end.
			c.setBufferedPos(mysql.Position{
				Name: currentLogName,
				Pos:  uint32(event.Position),
			})
			continue
		case *replication.RowsEvent:
			// Rows event, check if there are any active subscriptions
			// for it, and pass it to the subscription.
			if err = c.processRowsEvent(ev, event); err != nil {
				c.logger.Error("fatal error processing binlog rows event", "error", err)
				c.fatalError()
				return
			}
		case *replication.QueryEvent:
			// Query event, check if it is a DDL statement,
			// in which case we need to notify the caller.
			ddlTables, err := extractTablesFromDDLStmts(string(event.Schema), string(event.Query))
			if err != nil {
				// The parser does not understand all syntax.
				// For example, it won't parse [CREATE|DROP] TRIGGER statements *or*
				// ALTER USER x IDENTIFIED WITH x RETAIN CURRENT PASSWORD
				// This behavior is copied from canal:
				// https://github.com/go-mysql-org/go-mysql/blob/ee9447d96b48783abb05ab76a12501e5f1161e47/canal/sync.go#L144C1-L150C1
				// We can't print the statement because it could contain user-data.
				// We instead rely on file + pos being useful.
				c.logger.Error("Skipping query that was unable to parse", "file", currentLogName, "pos", ev.Header.LogPos)
				continue
			}
			for _, ddlTable := range ddlTables {
				c.processDDLNotification(ddlTable.schema, ddlTable.table)
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

// processDDLNotification cancels the client if the DDL matches our filter criteria.
// By default, only exact schema.table matches against subscriptions trigger cancellation.
// If ddlFilterSchema is set, any DDL in that schema triggers cancellation instead.
// If ddlFilterTables is also set (alongside ddlFilterSchema), only DDL on those
// specific tables within the schema triggers cancellation — this is used for partial
// moves where only a subset of tables from a schema are being moved.
func (c *Client) processDDLNotification(schema, table string) {
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
			for _, tsub := range sub.Tables() { // currentTable, newTable
				if tsub.SchemaName == schema && tsub.TableName == table {
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
	if c.fatalError() {
		c.logger.Error("table definition changed, cancelling operation", "schema", schema, "table", table)
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
func (c *Client) processRowsEvent(ev *replication.BinlogEvent, e *replication.RowsEvent) error {
	subName := encodeSchemaTable(string(e.Table.Schema), string(e.Table.Table))
	sub, ok := c.subs.Get(subName)
	if !ok {
		return nil // ignore event, it could be to a _new table.
	}

	if isMinimalRowImage(e) {
		return fmt.Errorf("received a minimal RBR event for table %s.%s, but we require binlog_row_image=FULL on the source server", string(e.Table.Schema), string(e.Table.Table))
	}

	tbl := sub.Tables()[0]
	eventType := parseEventType(ev.Header.EventType)

	if eventType == eventTypeUpdate {
		// UPDATE events always carry before/after image pairs.
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

			if pkChanged(beforeKey, afterKey) {
				sub.HasChanged(beforeKey, nil, true)      // delete old PK
				sub.HasChanged(afterKey, afterRow, false) // insert new PK
			} else {
				sub.HasChanged(beforeKey, afterRow, false)
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
			sub.HasChanged(key, row, false)
		case eventTypeDelete:
			sub.HasChanged(key, nil, true)
		default:
			c.logger.Error("unknown event type", "type", ev.Header.EventType)
		}
	}
	return nil
}

// fatalError is called from within the readStream goroutine when a truly fatal
// stream error occurs (e.g. unrecoverable stream error, minimal RBR detection,
// or a fatal rows event error). It returns true if the caller acknowledged the
// error (i.e. the cancel function was called and acted upon).
//
// IMPORTANT: This method must NOT call Close() because Close() calls
// streamWG.Wait(), which would deadlock since readStream is the caller.
func (c *Client) fatalError() bool {
	if c.callerCancelFunc != nil {
		return c.callerCancelFunc()
	}
	return false
}

func (c *Client) Close() {
	c.isClosed.Store(true)

	// Read cancelFunc under c.Lock — Run() writes it under the same lock.
	// We must not hold c.Lock across streamWG.Wait() below: readStream
	// itself acquires c.Lock from inside its loop (setBufferedPos,
	// recreateStreamer), and holding the lock during Wait would deadlock
	// an in-flight lock acquisition there.
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
	c.streamWG.Wait()

	// streamWG.Wait has returned, so readStream has exited and c.syncer
	// is no longer raced by it. Close is not expected to run concurrently
	// with Run() — the caller's sequenced-before edge (Run returned →
	// Close called) makes Run's write of c.syncer visible here without
	// further synchronization.
	if c.syncer != nil {
		c.syncer.Close()
		c.syncer = nil
	}
}

// FlushUnderTableLock is a final flush under an exclusive table lock using the connection
// that holds a write lock. Because flushing generates binary log events,
// we actually want to call flush *twice*:
//   - The first time flushes the pending changes to the new table.
//   - We then ensure that we have all the binary log changes read from the server.
//   - The second time reads through the changes generated by the first flush
//     and updates the in memory applied position to match the server's position.
//     This is required to satisfy the binlog position is updated for the c.AllChangesFlushed() check.
func (c *Client) FlushUnderTableLock(ctx context.Context, lock *dbconn.TableLock) error {
	if err := c.flush(ctx, true, lock); err != nil {
		return err
	}
	// Wait for the changes flushed to be received.
	if err := c.BlockWait(ctx); err != nil {
		return err
	}
	// Do a final flush
	return c.flush(ctx, true, lock)
}

// Flush is a low level flush, that asks all of the subscriptions to flush
// Some of these will flush a delta map, others will flush a queue.
//
// Note: we yield the lock early because otherwise no new events can be sent
// to the subscriptions while we are flushing.
// This means that the actual buffered position might be slightly ahead by
// the end of the flush. That's OK, we only set the flushed position to the known
// safe buffered position taken at the start.
func (c *Client) flush(ctx context.Context, underLock bool, lock *dbconn.TableLock) error {
	c.mu.Lock()
	newFlushedPos := c.bufferedPos
	c.mu.Unlock()
	var allChangesFlushed = true
	for _, subscription := range c.subs.Snapshot() {
		flushed, err := subscription.Flush(ctx, underLock, lock)
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
		c.SetFlushedPos(newFlushedPos)
	}
	return nil
}

// Flush empties the changeset in a loop until the amount of changes is considered "trivial".
// The loop is required, because changes continue to be added while the flush is occurring.
func (c *Client) Flush(ctx context.Context) error {
	for {
		// Repeat in a loop until the changeset length is trivial
		if err := c.flush(ctx, false, nil); err != nil {
			return err
		}
		// BlockWait to ensure we've read everything from the server
		// into our buffer. This can timeout, in which case we start
		// a new loop. Typically a timeout occurs when we resume from a checkpoint
		// and move from the copy phase to the apply phase, and there's
		// actually a lot to do!
		if err := c.BlockWait(ctx); err != nil {
			c.logger.Warn("error waiting for binlog reader to catch up", "error", err)
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
	return c.flush(ctx, false, nil)
}

// StopPeriodicFlush stops the periodic flush goroutine started by
// StartPeriodicFlush and blocks until that goroutine has fully exited.
// Safe to call when no periodic flush is running (no-op).
func (c *Client) StopPeriodicFlush() {
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

// StartPeriodicFlush starts a goroutine that periodically flushes the
// binlog changeset, used by the migrator to advance the binlog position.
// Registration of the cancel/done pair happens synchronously in the
// caller's goroutine before the loop is spawned, so a follow-up
// StopPeriodicFlush is guaranteed to observe the registration. Callers
// MUST NOT prefix with `go` — the loop is spawned internally.
//
// Calling Start while a flush is already running is a no-op.
func (c *Client) StartPeriodicFlush(ctx context.Context, interval time.Duration) {
	c.periodicFlushLock.Lock()
	if c.periodicFlushCancel != nil {
		c.periodicFlushLock.Unlock()
		return
	}
	flushCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	c.periodicFlushCancel = cancel
	c.periodicFlushDone = done
	c.periodicFlushLock.Unlock()

	go c.runPeriodicFlush(flushCtx, interval, done)
}

func (c *Client) runPeriodicFlush(ctx context.Context, interval time.Duration, done chan struct{}) {
	defer close(done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			startLoop := time.Now()
			c.logger.Debug("starting periodic flush of binary log")
			// The periodic flush does not respect the throttler since we want to advance the binlog position
			// we allow this to run, and then expect that if it is under load the throttler
			// will kick in and slow down the copy-rows.
			if err := c.flush(ctx, false, nil); err != nil {
				c.logger.Error("error flushing binary log", "error", err)
			}
			c.logger.Info("finished periodic flush of binary log", "total-duration", time.Since(startLoop))
		}
	}
}

// BlockWait blocks until all changes are *buffered*.
// i.e. the server's current position is 1234, but our buffered position
// is only 100. We need to read all the events until we reach >= 1234.
// We do not need to guarantee that they are flushed though, so
// you need to call Flush() to do that. This call times out!
// The default timeout is 10 seconds, after which an error will be returned.
func (c *Client) BlockWait(ctx context.Context) error {
	targetPos, err := c.getCurrentBinlogPosition(ctx)
	if err != nil {
		return err
	}
	c.logger.Info("waiting to catch up to source position", "target_position", targetPos, "current_position", c.getBufferedPos())
	timer := time.NewTimer(DefaultTimeout)
	defer timer.Stop() // Ensure timer is always stopped to prevent goroutine leak

	prevPos := c.getBufferedPos()
	first := true
	stallCount := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return fmt.Errorf("timed out waiting to catch up to source position: %v, current position is: %v", targetPos, c.getBufferedPos())
		default:
			currPos := c.getBufferedPos()
			if currPos.Compare(prevPos) <= 0 && !first {
				// Position hasn't advanced. Only flush after multiple consecutive
				// stalls to avoid unnecessary flushes when the binlog syncer is
				// just slightly behind (e.g., under CI load). getCurrentBinlogPosition
				// already flushes once at the start, so a brief stall is expected.
				stallCount++
				if stallCount >= blockWaitStallThreshold {
					c.logger.Debug("buffered position has not advanced, flushing binary logs")
					if err := dbconn.Exec(ctx, c.db, "FLUSH BINARY LOGS"); err != nil {
						return err // it could be context cancelled, return it
					}
					c.flushedBinlogs.Add(1)
					stallCount = 0
				}
			} else {
				stallCount = 0
			}
			prevPos = currPos
			first = false

			if c.getBufferedPos().Compare(targetPos) >= 0 {
				return nil // we are up to date!
			}

			// We are not caught up yet, so we need to wait.
			time.Sleep(blockWaitSleep)
		}
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
func (c *Client) SetWatermarkOptimization(ctx context.Context, newVal bool) error {
	for _, sub := range c.subs.Snapshot() {
		if err := sub.SetWatermarkOptimization(ctx, newVal); err != nil {
			return err
		}
	}
	return nil
}
