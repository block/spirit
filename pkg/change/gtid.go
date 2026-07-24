package change

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
)

// Compile-time assertion that the GTID-backed Client satisfies Source.
var _ Source = (*gtidClient)(nil)

// gtidClient is an experimental change.Source that uses MySQL GTIDs as the
// resume coordinate instead of (binlog-file, offset). It is a parallel
// implementation to binlogClient; nothing is shared with it directly so the
// binlog client can be kept untouched while this one matures.
//
// Wire protocol: COM_BINLOG_DUMP_GTID via go-mysql's
// BinlogSyncer.StartSyncGTID. The server requires gtid_mode=ON and
// enforce_gtid_consistency=ON; a non-GTID source will error out at Start.
type gtidClient struct {
	mu sync.Mutex

	host     string
	username string
	password string

	cfg      replication.BinlogSyncerConfig
	syncer   *replication.BinlogSyncer
	streamer *replication.BinlogStreamer

	db       *sql.DB
	applier  applier.Applier
	dbConfig *dbconn.DBConfig

	subs *subscriptionRegistry

	callerCancelFunc func(FatalReason) bool
	ddlFilterSchema  string
	ddlFilterTables  map[string]struct{}

	serverID uint32

	// bufferedGTID is everything we have seen from the stream and either
	// fully decoded into subscriptions (committed transactions) or that
	// the server told us was already part of its executed set when we
	// connected. flushedGTID is the subset that has been safely written
	// to the new table. Both are *MysqlGTIDSet; never nil after Start.
	//
	// pendingSID/pendingGNO carry the GTID of the in-progress transaction
	// (set on GTIDEvent, applied to bufferedGTID by promotePendingGTID
	// when the transaction's binlog group ends). This matters because
	// resume positions advance per-transaction: a GTID must only enter
	// the resume set after its full row-event stream has been buffered,
	// otherwise a crash mid-transaction would silently skip its tail on
	// the next run. See promotePendingGTID for the group shapes.
	bufferedGTID mysql.GTIDSet
	flushedGTID  mysql.GTIDSet
	pendingSID   []byte
	pendingGNO   int64

	periodicFlushLock   sync.Mutex
	periodicFlushCancel context.CancelFunc
	periodicFlushDone   chan struct{}

	cancelFunc func()
	isClosed   atomic.Bool
	logger     *slog.Logger
	streamWG   sync.WaitGroup

	subscriptionSoftLimitBytes int64
}

// NewGTIDClient constructs the GTID-backed change.Source. It mirrors
// NewBinlogClient: config.Applier (passed via appl) is required.
//
// EXPERIMENTAL. See docs in pkg/migration and pkg/move for the --gtid
// flag.
func NewGTIDClient(db *sql.DB, host string, username, password string, appl applier.Applier, config *ClientConfig) Source {
	if config.DBConfig == nil {
		config.DBConfig = dbconn.NewDBConfig()
	}
	softLimit := config.SubscriptionSoftLimitBytes
	if softLimit == 0 {
		softLimit = DefaultSubscriptionSoftLimitBytes
	} else if softLimit < 0 {
		softLimit = 0
	}
	return &gtidClient{
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

// AddSubscription satisfies Source.
func (c *gtidClient) AddSubscription(currentTable, newTable *table.TableInfo, chunker table.MappedChunker) error {
	subKey := encodeSchemaTable(currentTable.SchemaName, currentTable.TableName)
	sub, err := NewBufferedSubscription(BufferedSubscriptionConfig{
		CurrentTable:   currentTable,
		NewTable:       newTable,
		Applier:        c.applier,
		Chunker:        chunker,
		Logger:         c.logger,
		SoftLimitBytes: c.subscriptionSoftLimitBytes,
	})
	if err != nil {
		return fmt.Errorf("could not build subscription for table %s.%s: %w", currentTable.SchemaName, currentTable.TableName, err)
	}
	if !c.subs.Add(subKey, sub) {
		return fmt.Errorf("subscription already exists for table %s.%s", currentTable.SchemaName, currentTable.TableName)
	}
	return nil
}

// getCurrentGTIDSet reads the source's @@GLOBAL.gtid_executed and parses
// it into a *MysqlGTIDSet. Returned set is never nil — an empty
// gtid_executed parses to an empty set, which is what a brand-new
// server (no transactions yet) reports.
func (c *gtidClient) getCurrentGTIDSet(ctx context.Context) (mysql.GTIDSet, error) {
	var gtidStr string
	if err := c.db.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&gtidStr); err != nil {
		return nil, fmt.Errorf("failed to read @@GLOBAL.gtid_executed (is gtid_mode=ON?): %w", err)
	}
	gset, err := mysql.ParseMysqlGTIDSet(normalizeGTIDString(gtidStr))
	if err != nil {
		return nil, fmt.Errorf("failed to parse @@GLOBAL.gtid_executed %q: %w", gtidStr, err)
	}
	return gset, nil
}

// getPurgedGTIDSet reads @@GLOBAL.gtid_purged. A GTID set we want to
// resume from must be a superset of gtid_purged; if not, the source has
// dropped binary logs containing changes we need.
func (c *gtidClient) getPurgedGTIDSet(ctx context.Context) (mysql.GTIDSet, error) {
	var gtidStr string
	if err := c.db.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_purged").Scan(&gtidStr); err != nil {
		return nil, fmt.Errorf("failed to read @@GLOBAL.gtid_purged: %w", err)
	}
	gset, err := mysql.ParseMysqlGTIDSet(normalizeGTIDString(gtidStr))
	if err != nil {
		return nil, fmt.Errorf("failed to parse @@GLOBAL.gtid_purged %q: %w", gtidStr, err)
	}
	return gset, nil
}

// validateResumeGTIDSet checks that a checkpointed GTID set is still a
// usable resume coordinate on this server, wrapping failures with
// ErrPositionNotFound so callers discard the checkpoint and start fresh.
// Two containments must hold. flushed must cover purged: any purged GTID
// missing from flushed is a change we still need whose binary log the
// server has dropped. executed must contain flushed: any flushed GTID
// missing from executed is a transaction this server never executed —
// the server's GTID history has regressed relative to the checkpoint
// (restore from an earlier backup/PITR, or failover to a replica that
// lagged the server the checkpoint was written against). The protocol
// does not catch the regression case for us: COM_BINLOG_DUMP_GTID treats
// the requested set as "already applied" and never streams transactions
// it does not know about, so resuming would silently skip every change
// the checkpoint wrongly records as applied.
func validateResumeGTIDSet(flushed, executed, purged mysql.GTIDSet) error {
	if !flushed.Contain(purged) {
		return fmt.Errorf("%w: requested GTID set does not cover @@GLOBAL.gtid_purged (purged=%s, requested=%s)",
			ErrPositionNotFound, purged.String(), flushed.String())
	}
	if !executed.Contain(flushed) {
		return fmt.Errorf("%w: @@GLOBAL.gtid_executed does not contain the requested GTID set; the server never executed transactions the checkpoint records as applied (failover or restore from backup?), so the migration must restart fresh (executed=%s, requested=%s)",
			ErrPositionNotFound, executed.String(), flushed.String())
	}
	return nil
}

// normalizeGTIDString strips whitespace (including embedded newlines, which
// MySQL injects when gtid_executed contains many UUID groups) before parse.
func normalizeGTIDString(s string) string {
	return strings.Map(func(r rune) rune {
		if r == ' ' || r == '\n' || r == '\r' || r == '\t' {
			return -1
		}
		return r
	}, s)
}

// hasPrefixFold reports whether s begins with prefix, matched
// case-insensitively (strings.HasPrefix + strings.EqualFold).
func hasPrefixFold(s, prefix string) bool {
	return len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix)
}

// setBufferedGTID adds gtid to bufferedGTID under c.mu. The set grows
// monotonically — there is no "rewind" in GTID semantics.
func (c *gtidClient) setBufferedGTID(sid []byte, gno int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	u, err := uuid.FromBytes(sid)
	if err != nil {
		c.logger.Error("failed to decode SID bytes from GTIDEvent", "error", err)
		return
	}
	if err := c.bufferedGTID.Update(u.String() + ":" + strconv.FormatInt(gno, 10)); err != nil {
		c.logger.Error("failed to update buffered GTID set", "error", err)
	}
}

// getBufferedGTID returns a clone of the buffered set under c.mu so the
// caller can compare without racing with concurrent updates.
func (c *gtidClient) getBufferedGTID() mysql.GTIDSet {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bufferedGTID.Clone()
}

// promotePendingGTID moves the pending {SID,GNO} stashed by the most
// recent GTIDEvent into bufferedGTID and clears the pending state. It is
// a no-op when there is no pending GTID (e.g. right after a reconnect).
//
// This must be called when — and only when — the current transaction's
// binlog group ends: XIDEvent (InnoDB commit), a COMMIT or ROLLBACK
// QueryEvent (non-transactional engines / mixed-engine rollbacks), or a
// standalone statement that is its own transaction (DDL, statements the
// TiDB parser cannot parse such as CREATE TRIGGER or stored procedures).
// Every GTIDEvent the server streams corresponds to an entry in its
// gtid_executed, so any path that drops a pending GTID instead of
// promoting it leaves bufferedGTID permanently behind gtid_executed and
// wedges BlockWait/Flush forever.
//
// The "only when" direction matters just as much: promoting before the
// group's row events have been buffered lets a concurrent flush() publish
// the GTID as a resume coordinate too early; a crash then resumes past it
// and its row events are silently lost. This is why a mid-group QueryEvent
// must not promote. Regular transactions have no mid-group QueryEvents
// besides BEGIN and the SAVEPOINT family ("SAVEPOINT `x`", plus
// "ROLLBACK TO `x`" in mixed-engine transactions). XA transactions log
// mid-group QueryEvents too ("XA START"/"XA END"), but any XA statement
// now fails the stream outright instead of adjusting pending-GTID state —
// see the XA guard in processQueryEvent.
func (c *gtidClient) promotePendingGTID() {
	c.mu.Lock()
	pendingSID := c.pendingSID
	pendingGNO := c.pendingGNO
	c.pendingSID = nil
	c.pendingGNO = 0
	c.mu.Unlock()
	if len(pendingSID) > 0 {
		c.setBufferedGTID(pendingSID, pendingGNO)
	}
}

// AllChangesFlushed satisfies Source. True when bufferedGTID and
// flushedGTID are equal — i.e. every GTID we have observed has had its
// row events applied to the target.
func (c *gtidClient) AllChangesFlushed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.flushedGTID.Contain(c.bufferedGTID) {
		c.logger.Warn("GTID reader flushed set does not yet contain buffered set",
			"flushed", c.flushedGTID.String(), "buffered", c.bufferedGTID.String())
	}
	for _, subscription := range c.subs.Snapshot() {
		if subscription.Length() > 0 {
			return false
		}
	}
	return true
}

// Position satisfies Source. The opaque string is the GTID set
// representation (e.g. "uuid:1-5,otheruuid:1-3"). Returns "" if no
// position has been observed yet, signaling that a fresh Start is
// required.
func (c *gtidClient) Position() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.flushedGTID == nil || c.flushedGTID.IsEmpty() {
		return ""
	}
	return c.flushedGTID.String()
}

// CurrentPosition satisfies Source. See the interface doc for how it differs
// from Position (in-memory feed progress vs a live server read). In GTID mode
// it reads the server's @@GLOBAL.gtid_executed — no FLUSH and no running feed
// required — and returns it in the same encoding Position uses, so it round
// trips through StartFromPosition.
func (c *gtidClient) CurrentPosition(ctx context.Context) (string, error) {
	set, err := c.getCurrentGTIDSet(ctx)
	if err != nil {
		return "", err
	}
	return set.String(), nil
}

// StartFromPosition satisfies Source. It primes flushedGTID from the
// previously-returned opaque string, validates it is still resumable
// against the server's GTID state (see validateResumeGTIDSet), then
// begins streaming as Start would.
//
// Parse failures are wrapped with ErrPositionNotFound. This matters
// because the most likely real-world parse failure is an operator
// resuming a legacy file:offset checkpoint (binlog.000123:4567)
// against the GTID client (or vice-versa). Without the wrap the
// generic parse error falls through pkg/migration's strict-mode
// classifier and silently restarts from scratch, losing checkpoint
// progress; with the wrap the strict-mode caller sees it as
// status.ErrBinlogNotFound and aborts loudly.
func (c *gtidClient) StartFromPosition(ctx context.Context, pos string) error {
	if pos == "" {
		return errors.New("StartFromPosition: empty position; use Start instead for a fresh start")
	}
	parsed, err := mysql.ParseMysqlGTIDSet(normalizeGTIDString(pos))
	if err != nil {
		return fmt.Errorf("%w: StartFromPosition: cannot parse %q as a GTID set (legacy file:offset checkpoint?): %w", ErrPositionNotFound, pos, err)
	}
	c.mu.Lock()
	c.flushedGTID = parsed
	c.mu.Unlock()
	return c.Start(ctx)
}

// buildSyncerConfig returns the BinlogSyncerConfig used by Start. Split
// out (mirroring binlogClient.buildSyncerConfig) so tests can assert the
// decode options below stay in sync between the two clients.
func (c *gtidClient) buildSyncerConfig(host string, port uint16) replication.BinlogSyncerConfig {
	return replication.BinlogSyncerConfig{
		ServerID: c.serverID,
		Flavor:   "mysql",
		Host:     host,
		Port:     port,
		User:     c.username,
		Password: c.password,
		Logger:   c.logger,
		// Render JSON the same way the binlog client does — see the
		// rationale on NewBinlogClient.
		RenderJSONAsMySQLText: true,
		// Decode TIMESTAMP values in UTC the same way the binlog client
		// does — see the rationale on NewBinlogClient. Without this, the
		// decoder uses the process's local timezone while the applier
		// writes over time_zone='+00:00' connections, silently shifting
		// stored TIMESTAMP values on any non-UTC host.
		TimestampStringLocation: time.UTC,
	}
}

// Start satisfies Source. On a fresh start it reads @@GLOBAL.gtid_executed
// and begins streaming from there; on a resume (flushedGTID already
// primed by StartFromPosition) it validates the position against the
// server's GTID state (see validateResumeGTIDSet) before connecting.
func (c *gtidClient) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	host, portStr, err := net.SplitHostPort(c.host)
	if err != nil {
		return fmt.Errorf("failed to parse host: %w", err)
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return fmt.Errorf("failed to parse port: %w", err)
	}
	c.cfg = c.buildSyncerConfig(host, uint16(port))
	if c.dbConfig != nil {
		tlsConfig, err := dbconn.GetTLSConfigForBinlog(c.dbConfig, host)
		if err != nil {
			return fmt.Errorf("failed to configure TLS for binlog connection: %w", err)
		}
		c.cfg.TLSConfig = tlsConfig
	}

	// Determine the starting GTID set. On fresh start, this is
	// gtid_executed; on resume, the caller has primed flushedGTID and
	// we validate it against the server's current GTID state before
	// connecting.
	if c.flushedGTID == nil || c.flushedGTID.IsEmpty() {
		c.flushedGTID, err = c.getCurrentGTIDSet(ctx)
		if err != nil {
			return fmt.Errorf("failed to read current GTID set, is gtid_mode=ON?: %w", err)
		}
	} else {
		executed, err := c.getCurrentGTIDSet(ctx)
		if err != nil {
			return fmt.Errorf("could not verify GTID position: %w", err)
		}
		purged, err := c.getPurgedGTIDSet(ctx)
		if err != nil {
			return fmt.Errorf("could not verify GTID position: %w", err)
		}
		if err := validateResumeGTIDSet(c.flushedGTID, executed, purged); err != nil {
			return err
		}
	}
	c.bufferedGTID = c.flushedGTID.Clone()
	c.syncer = replication.NewBinlogSyncer(c.cfg)
	// Clone to avoid data race
	c.streamer, err = c.syncer.StartSyncGTID(c.flushedGTID.Clone())
	if err != nil {
		c.syncer.Close()
		c.syncer = nil
		return fmt.Errorf("failed to start GTID binlog streamer: %w", err)
	}
	ctx, c.cancelFunc = context.WithCancel(ctx)
	c.streamWG.Add(1)
	go c.readStream(ctx)
	return nil
}

// recreateStreamer reconnects the syncer at the current bufferedGTID
// after a transient stream-level failure. Unlike the file/offset client
// there is no "restart at beginning of current file" gymnastic — GTID
// resume is naturally transaction-aligned, so re-asking for everything
// after bufferedGTID gets us a clean stream with all needed TableMaps.
func (c *gtidClient) recreateStreamer() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.logger.Info("recreateStreamer called",
		"buffered_gtid", c.bufferedGTID.String(),
		"flushed_gtid", c.flushedGTID.String(),
		"syncer_exists", c.syncer != nil,
		"streamer_exists", c.streamer != nil)

	if c.syncer != nil {
		c.syncer.Close()
	}
	resumeFrom := c.bufferedGTID.Clone()
	c.syncer = replication.NewBinlogSyncer(c.cfg)
	var err error
	c.streamer, err = c.syncer.StartSyncGTID(resumeFrom)
	if err != nil {
		c.logger.Error("Failed to start GTID binlog streamer in recreateStreamer",
			"error", err,
			"gtid", resumeFrom.String(),
			"config", fmt.Sprintf("host=%s:%d user=%s", c.cfg.Host, c.cfg.Port, c.cfg.User))
		return fmt.Errorf("failed to start GTID binlog streamer: %w", err)
	}
	// Drop any half-buffered transaction state — the new stream restarts
	// at a transaction boundary so any pending {SID,GNO} from before the
	// reconnect would be wrong to apply to a later XIDEvent.
	c.pendingSID = nil
	c.pendingGNO = 0
	return nil
}

// readStream is the GTID equivalent of binlogClient.readStream. The shape
// is intentionally the same: a long-running loop that decodes events,
// dispatches RowsEvent to subscriptions, surfaces DDL via the cancel
// callback, and advances the resume coordinate per-transaction.
func (c *gtidClient) readStream(ctx context.Context) {
	defer c.streamWG.Done()

	consecutiveErrors := 0
	recreateAttempts := 0
	backoffDuration := initialBackoffDuration
	lastErrorTime := time.Time{}
	var recentErrors []string

	c.logger.Debug("readStream started for GTID position", "gtid", c.getBufferedGTID().String())

	for {
		select {
		case <-ctx.Done():
			c.logger.Debug("readStream context cancelled", "error", ctx.Err())
			return
		default:
		}

		var ev *replication.BinlogEvent
		var err error

		if c.streamer == nil {
			err = errors.New("GTID streamer is nil, cannot read events")
		} else {
			ev, err = c.streamer.GetEvent(ctx)
		}

		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil || c.isClosed.Load() {
				return
			}
			consecutiveErrors++
			currentTime := time.Now()

			errorMsg := fmt.Sprintf("[%s] %v", currentTime.Format("15:04:05.000"), err)
			recentErrors = append(recentErrors, errorMsg)
			if len(recentErrors) > 20 {
				recentErrors = recentErrors[1:]
			}

			c.logger.Error("error reading GTID stream", "consecutive_errors", consecutiveErrors, "error", err, "current_gtid", c.getBufferedGTID().String())

			if consecutiveErrors >= maxConsecutiveErrors {
				recreateAttempts++
				c.logger.Warn("Too many consecutive errors, attempting to recreate GTID streamer",
					"consecutive_errors", consecutiveErrors,
					"attempt", recreateAttempts,
					"max_attempts", maxRecreateAttempts,
					"backoff_duration", backoffDuration)

				if recreateAttempts >= maxRecreateAttempts {
					c.logger.Error("failed to recreate GTID streamer, giving up",
						"total_attempts", recreateAttempts,
						"recent_errors", recentErrors,
						"is_closed", c.isClosed.Load())
					c.fatalError(FatalReasonStreamError)
					return
				}

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

				if recreateErr := c.recreateStreamer(); recreateErr != nil {
					c.logger.Error("Failed to recreate GTID streamer", "error", recreateErr)
					backoffDuration *= backoffMultiplier
					if backoffDuration > maxBackoffDuration {
						backoffDuration = maxBackoffDuration
					}
				} else {
					consecutiveErrors = 0
					recreateAttempts = 0
					backoffDuration = initialBackoffDuration
				}
				lastErrorTime = currentTime
			}

			retryTimer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-ctx.Done():
				retryTimer.Stop()
				return
			case <-retryTimer.C:
			}
			continue
		}

		if consecutiveErrors > 0 {
			c.logger.Info("GTID stream recovered after consecutive errors", "consecutive_errors", consecutiveErrors)
			consecutiveErrors = 0
			backoffDuration = initialBackoffDuration
		}

		if ev == nil {
			continue
		}
		switch event := ev.Event.(type) {
		case *replication.GTIDEvent:
			// The server emits a GTIDEvent at the start of every
			// transaction. Stash its {SID,GNO} so we can promote it to
			// bufferedGTID only after the matching XIDEvent.
			c.mu.Lock()
			c.pendingSID = append(c.pendingSID[:0], event.SID...)
			c.pendingGNO = event.GNO
			c.mu.Unlock()
		case *replication.XIDEvent:
			// Transaction commit (InnoDB). Promote the pending GTID
			// into bufferedGTID — only now is it safe to resume past it.
			c.promotePendingGTID()
		case *replication.RowsEvent:
			if err = c.processRowsEvent(ev, event); err != nil {
				c.logger.Error("fatal error processing GTID rows event", "error", err)
				c.fatalError(FatalReasonStreamError)
				return
			}
		case *replication.QueryEvent:
			if err = c.processQueryEvent(event); err != nil {
				c.logger.Error("fatal error processing GTID query event", "error", err)
				c.fatalError(FatalReasonStreamError)
				return
			}
		case *replication.TransactionPayloadEvent:
			// binlog_transaction_compression=ON wraps the whole transaction
			// (BEGIN QueryEvent, TableMapEvents, row events, XIDEvent) in one
			// compressed payload event; only the GTID event stays outside.
			// go-mysql has already decompressed and re-parsed the inner
			// events, so dispatch them exactly as if they had arrived
			// uncompressed. Letting this fall through to the default case
			// would silently drop the transaction's changes AND leave its
			// pending GTID unpromoted (the XIDEvent is inside the payload),
			// wedging BlockWait/Flush forever.
			if err = c.processTransactionPayload(event); err != nil {
				c.logger.Error("fatal error processing GTID transaction payload event", "error", err)
				c.fatalError(FatalReasonStreamError)
				return
			}
		case *replication.TableMapEvent,
			*replication.FormatDescriptionEvent,
			*replication.PreviousGTIDsEvent,
			*replication.RotateEvent:
			// Stream housekeeping events. Position tracking advances via
			// GTIDEvent/XIDEvent above, not via these.
		case *replication.GenericEvent:
			// Event types without a dedicated go-mysql decoder surface as
			// GenericEvent; the header carries the real type. The one we
			// must act on is XA_PREPARE_LOG_EVENT: it terminates an XA
			// transaction's first binlog group (it is also how the server
			// logs `XA COMMIT ... ONE PHASE`), and spirit does not support
			// XA workloads — see the guard in processQueryEvent. That guard
			// already fails the stream at the group's opening "XA START"
			// QueryEvent, before any of its row events are buffered, so
			// this branch is defense in depth in case a future server
			// version reshapes the group.
			if ev.Header.EventType == replication.XA_PREPARE_LOG_EVENT {
				c.logger.Error("fatal error processing GTID stream", "error", errXAUnsupported)
				c.fatalError(FatalReasonStreamError)
				return
			}
			c.logger.Debug("Received unknown event type", "type", ev.Header.EventType.String())
		default:
			c.logger.Debug("Received unknown event type", "type", fmt.Sprintf("%T", ev.Event))
		}
	}
}

// processQueryEvent handles a QueryEvent, whether read directly from the
// stream or decompressed from a transaction payload. Transaction-control
// statements adjust the pending-GTID state, XA statements fail the stream
// (spirit does not support XA workloads; see the guard below), and
// everything else goes through DDL extraction. See promotePendingGTID for
// the group shapes that dictate which statements promote and which must
// leave the pending GTID pending. A returned error is fatal: the caller
// must tear the stream down without buffering anything further.
func (c *gtidClient) processQueryEvent(event *replication.QueryEvent) error {
	// A "BEGIN" QueryEvent inside a transaction is not DDL — skip
	// it cheaply rather than handing it to the parser. The pending
	// GTID must stay pending: the transaction's row events have not
	// been buffered yet.
	q := strings.TrimSpace(string(event.Query))
	if strings.EqualFold(q, "BEGIN") {
		return nil
	}
	// MySQL also logs SAVEPOINT statements as QueryEvents in the
	// *middle* of a row-format transaction (verified against MySQL
	// 8.0: GTIDEvent → Query(BEGIN) → row events →
	// Query("SAVEPOINT `sp1`") → more row events → XIDEvent).
	// "ROLLBACK TO `sp1`" appears mid-group the same way when
	// non-transactional writes after the savepoint prevent the
	// server from simply truncating its binlog cache. Neither
	// terminates the group — it still ends at the XIDEvent or
	// COMMIT/ROLLBACK QueryEvent that follows — so, exactly like
	// BEGIN and "XA START"/"XA END", the pending GTID must stay
	// pending: falling through to the parser path below would
	// promote on both of its branches, letting a concurrent flush
	// publish the GTID as a resume coordinate before the rest of
	// the transaction's row events are buffered. A crash before the
	// next flush would then resume past the transaction and
	// silently lose its tail. The server rewrites these statements
	// with backtick-quoted identifiers ("ROLLBACK TO `sp1`" — no
	// SAVEPOINT keyword), so keyword prefix matches are exact.
	// "ROLLBACK TO " is matched here, above the terminator check
	// below, so it can never be taken for a transaction-ending
	// ROLLBACK; "RELEASE SAVEPOINT " is matched defensively (MySQL
	// does not binlog it today) since releasing a savepoint never
	// ends a transaction either.
	if hasPrefixFold(q, "SAVEPOINT ") || hasPrefixFold(q, "ROLLBACK TO ") || hasPrefixFold(q, "RELEASE SAVEPOINT ") {
		return nil
	}
	// COMMIT/ROLLBACK QueryEvents end a transaction that involved a
	// non-transactional engine (these get a QueryEvent terminator
	// instead of an XIDEvent; a logged ROLLBACK is the mixed-engine
	// case where the non-transactional writes survived the rollback).
	// Either way the server has recorded the GTID in gtid_executed
	// and we have buffered all of the transaction's row events, so
	// promote — exactly as the XIDEvent path does. Skipping the
	// promotion here would wedge BlockWait forever.
	if strings.EqualFold(q, "COMMIT") || strings.EqualFold(q, "ROLLBACK") {
		c.promotePendingGTID()
		return nil
	}
	// Any XA statement fails the stream: spirit does not support XA
	// workloads. An XA transaction's first binlog group is written in
	// one piece at XA PREPARE time (verified against MySQL 8.0):
	//
	//	GTIDEvent(g1) → Query("XA START x") → row events →
	//	Query("XA END x") → XA_PREPARE_LOG_EVENT
	//
	// with the terminal XA COMMIT or XA ROLLBACK arriving any amount
	// of time later as a QueryEvent under its own GTID (g2), with no
	// row events. The row events are therefore streamed before the
	// transaction's outcome is known: buffering and flushing them
	// treats the prepare as a commit, and a later XA ROLLBACK has no
	// binlog representation that could undo them — the target would
	// diverge permanently, detectable only by checksum. Rather than
	// track prepared XIDs and buffer until the outcome, refuse the
	// workload. Failing on "XA START" — before any of the group's row
	// events — guarantees none of them are ever buffered, let alone
	// flushed. A terminal XA COMMIT / XA ROLLBACK with no preceding
	// "XA START" in-stream means the transaction was prepared before
	// we connected: its row events were never streamed and may postdate
	// the copier's snapshot of their chunk, so an XA COMMIT outcome
	// could silently lose them — refuse those too. (`XA COMMIT ... ONE
	// PHASE`, though committed atomically, is likewise refused: at
	// "XA START" time the one-phase outcome is unknowable.) The pending
	// GTID is deliberately left unpromoted so the resume coordinate
	// stays before the XA group.
	if isXAStatement(q) {
		return errXAUnsupported
	}
	ddlTables, err := extractTablesFromDDLStmts(string(event.Schema), string(event.Query))
	if err != nil {
		// The TiDB parser does not understand all syntax (CREATE/DROP
		// TRIGGER, certain ALTER USER variants, etc.) — these are
		// expected misses, not bugs. We include the parser error and
		// the schema so an operator can diagnose unexpected payloads,
		// but deliberately omit the query itself: it can contain user
		// data and ends up in logs. (Same rationale as the binlog
		// client.)
		c.logger.Error("Skipping query that was unable to parse",
			"error", err,
			"schema", string(event.Schema),
			"gtid", c.getBufferedGTID().String())
		// The statement was still a complete server transaction with
		// its own GTID — promote it even though we could not parse
		// it, otherwise bufferedGTID falls permanently behind
		// gtid_executed and BlockWait/Flush never complete. Note the
		// schema filter only applies after parsing, so *any*
		// unparseable statement on the server (e.g. a stored
		// procedure deploy in an unrelated schema) takes this path.
		c.promotePendingGTID()
		return nil
	}
	// MySQL emits a synthetic GTID for DDL statements too, but the
	// DDL is its own transaction (no XIDEvent). Promote any pending
	// GTID now so a DDL-as-last-event still ends up in the resume
	// set. This is best-effort — if the caller cancels on DDL we
	// won't actually resume, but the position is consistent for
	// non-cancelling filters.
	c.promotePendingGTID()
	for _, ddlTable := range ddlTables {
		c.processDDLNotification(ddlTable.schema, ddlTable.table)
	}
	return nil
}

// processTransactionPayload processes the events decompressed from a
// TransactionPayloadEvent (binlog_transaction_compression=ON, settable
// per-session by any client regardless of the global value the preflight
// checks). The payload carries the whole transaction except its GTID
// event, so the pending {SID,GNO} stashed by the preceding (uncompressed)
// GTIDEvent is promoted here, by the inner group terminator — XIDEvent or
// COMMIT/ROLLBACK QueryEvent — after the transaction's row events have
// been buffered, keeping promotePendingGTID's per-transaction resume
// contract intact.
func (c *gtidClient) processTransactionPayload(e *replication.TransactionPayloadEvent) error {
	for _, inner := range e.Events {
		switch innerEvent := inner.Event.(type) {
		case *replication.XIDEvent:
			// Transaction commit (InnoDB), delivered inside the payload.
			c.promotePendingGTID()
		case *replication.RowsEvent:
			if err := c.processRowsEvent(inner, innerEvent); err != nil {
				return err
			}
		case *replication.QueryEvent:
			if err := c.processQueryEvent(innerEvent); err != nil {
				return err
			}
		case *replication.TableMapEvent:
			// Already consumed by go-mysql's inner parser to decode the
			// RowsEvents above.
		case *replication.GenericEvent:
			// XA transactions are compressed too (verified against MySQL
			// 8.0.43): the first binlog group — XA START, row events, XA END,
			// XA_PREPARE_LOG_EVENT — arrives inside a payload, with only the
			// terminal XA COMMIT / XA ROLLBACK QueryEvent outside under its
			// own GTID. Spirit does not support XA workloads: the inner
			// "XA START" QueryEvent above already fails the payload before
			// any of its row events are buffered, so this branch — the
			// XA_PREPARE_LOG_EVENT surfacing as a GenericEvent — is defense
			// in depth, mirroring readStream's GenericEvent case.
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

// processDDLNotification mirrors binlogClient.processDDLNotification.
func (c *gtidClient) processDDLNotification(schema, table string) {
	if c.ddlFilterSchema != "" {
		if schema != c.ddlFilterSchema {
			return
		}
		if len(c.ddlFilterTables) > 0 {
			if _, ok := c.ddlFilterTables[table]; !ok {
				return
			}
		}
	} else {
		matchFound := false
		for _, sub := range c.subs.Snapshot() {
			for _, tsub := range sub.Tables() {
				if tsub == nil {
					// Defensive: in-tree subscriptions never emit nil
					// entries (bufferedMap.Tables omits a nil newTable),
					// but the interface can't guarantee it for other
					// implementations, and a DDL notification must never
					// crash the stream reader.
					continue
				}
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
	if c.fatalError(FatalReasonSchemaChange) {
		c.logger.Error("table definition changed, cancelling operation", "schema", schema, "table", table)
	}
}

// processRowsEvent mirrors binlogClient.processRowsEvent.
func (c *gtidClient) processRowsEvent(ev *replication.BinlogEvent, e *replication.RowsEvent) error {
	subName := encodeSchemaTable(string(e.Table.Schema), string(e.Table.Table))
	sub, ok := c.subs.Get(subName)
	if !ok {
		return nil
	}

	if isMinimalRowImage(e) {
		return fmt.Errorf("received a minimal RBR event for table %s.%s, but we require binlog_row_image=FULL on the source server", string(e.Table.Schema), string(e.Table.Table))
	}

	tbl := sub.Tables()[0]
	eventType := parseEventType(ev.Header.EventType)

	// Decode ENUM/SET integers and re-pad BINARY(N) values before key
	// extraction and buffering — see the matching block in binlog.go's
	// processRowsEvent and TableInfo.DecodeBinlogRow.
	if tbl.NeedsBinlogRowDecoding() {
		for _, row := range e.Rows {
			if err := tbl.DecodeBinlogRow(row); err != nil {
				return fmt.Errorf("decoding binlog row for %s.%s: %w", tbl.SchemaName, tbl.TableName, err)
			}
		}
	}

	if eventType == eventTypeUpdate {
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
			// See the matching block in binlog.go's processRowsEvent: an
			// UPDATE to the declared-immutable sharding column must fail
			// the stream fatally.
			if err := checkImmutableColumn(tbl, immutableOrdinal, beforeRow, afterRow, beforeKey); err != nil {
				return err
			}
			if pkChanged(beforeKey, afterKey) {
				sub.HasChanged(beforeKey, nil, true)
				sub.HasChanged(afterKey, afterRow, false)
			} else {
				sub.HasChanged(beforeKey, afterRow, false)
			}
		}
		return nil
	}

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

// fatalError mirrors binlogClient.fatalError; see the doc comment there.
func (c *gtidClient) fatalError(reason FatalReason) bool {
	if c.callerCancelFunc != nil {
		return c.callerCancelFunc(reason)
	}
	return false
}

// GetDeltaLen satisfies Source.
func (c *gtidClient) GetDeltaLen() int {
	deltaLen := 0
	for _, subscription := range c.subs.Snapshot() {
		deltaLen += subscription.Length()
	}
	return deltaLen
}

func (c *gtidClient) Close() {
	c.isClosed.Store(true)

	c.mu.Lock()
	cancel := c.cancelFunc
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}

	for _, sub := range c.subs.Snapshot() {
		sub.Close()
	}

	c.streamWG.Wait()

	if c.syncer != nil {
		c.syncer.Close()
		c.syncer = nil
	}
}

// FlushUnderTableLock satisfies Source. Same two-pass dance as the
// binlog client: flush the in-flight delta, wait for the events the
// flush itself generated to be ingested, then flush again so the
// resume coordinate covers those.
func (c *gtidClient) FlushUnderTableLock(ctx context.Context, locks []*dbconn.TableLock) error {
	if len(locks) == 0 {
		// Flushing "under lock" without any lock would silently execute the
		// statements outside the locks the caller believes are held.
		return errors.New("FlushUnderTableLock requires at least one table lock")
	}
	if err := c.flush(ctx, true, locks); err != nil {
		return err
	}
	if err := c.BlockWait(ctx); err != nil {
		return err
	}
	return c.flush(ctx, true, locks)
}

func (c *gtidClient) flush(ctx context.Context, underLock bool, locks []*dbconn.TableLock) error {
	c.mu.Lock()
	newFlushedGTID := c.bufferedGTID.Clone()
	c.mu.Unlock()
	allChangesFlushed := true
	for _, subscription := range c.subs.Snapshot() {
		flushed, err := subscription.Flush(ctx, underLock, locks)
		if err != nil {
			return err
		}
		if !flushed {
			allChangesFlushed = false
		}
	}
	if allChangesFlushed {
		c.mu.Lock()
		// Monotonic, mirroring the binlog client's flushedPos guard: if two
		// flushes were ever to overlap, the later-finishing one could hold
		// an older (smaller) snapshot of bufferedGTID, and storing it
		// unconditionally would regress the resume coordinate. bufferedGTID
		// only ever grows, so any two snapshots are ordered by containment;
		// skip the store when the current flushed set already contains the
		// candidate. Every current caller serializes flushes, so this
		// guards the invariant rather than fixing a live bug.
		if c.flushedGTID == nil || !c.flushedGTID.Contain(newFlushedGTID) {
			c.flushedGTID = newFlushedGTID
		}
		c.mu.Unlock()
	}
	return nil
}

// Flush satisfies Source. Same shape as binlogClient.Flush.
func (c *gtidClient) Flush(ctx context.Context) error {
	for {
		if err := c.flush(ctx, false, nil); err != nil {
			return err
		}
		if err := c.BlockWait(ctx); err != nil {
			c.logger.Warn("error waiting for GTID reader to catch up", "error", err)
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return ctx.Err()
			}
			continue
		}
		if c.GetDeltaLen() < binlogTrivialThreshold {
			break
		}
	}
	return c.flush(ctx, false, nil)
}

// StopPeriodicFlush satisfies Source.
func (c *gtidClient) StopPeriodicFlush() {
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

// StartPeriodicFlush satisfies Source.
func (c *gtidClient) StartPeriodicFlush(ctx context.Context, interval time.Duration) {
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

func (c *gtidClient) runPeriodicFlush(ctx context.Context, interval time.Duration, done chan struct{}) {
	defer close(done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			startLoop := time.Now()
			c.logger.Debug("starting periodic flush of GTID changeset")
			if err := c.flush(ctx, false, nil); err != nil {
				c.logger.Error("error flushing GTID changeset", "error", err)
			}
			c.logger.Info("finished periodic flush of GTID changeset", "total-duration", time.Since(startLoop).String())
		}
	}
}

// BlockWait satisfies Source. Reads the source's @@GLOBAL.gtid_executed
// and waits until our buffered set is a superset of it.
func (c *gtidClient) BlockWait(ctx context.Context) error {
	targetGTID, err := c.getCurrentGTIDSet(ctx)
	if err != nil {
		return err
	}
	c.logger.Info("waiting to catch up to source GTID", "target", targetGTID.String(), "current", c.getBufferedGTID().String())
	timer := time.NewTimer(DefaultTimeout)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return fmt.Errorf("timed out waiting to catch up to source GTID: %s, current: %s", targetGTID.String(), c.getBufferedGTID().String())
		default:
			if c.getBufferedGTID().Contain(targetGTID) {
				return nil
			}
			time.Sleep(blockWaitSleep)
		}
	}
}

// SetWatermarkOptimization satisfies Source.
func (c *gtidClient) SetWatermarkOptimization(ctx context.Context, newVal bool) error {
	for _, sub := range c.subs.Snapshot() {
		if err := sub.SetWatermarkOptimization(ctx, newVal); err != nil {
			return err
		}
	}
	return nil
}
