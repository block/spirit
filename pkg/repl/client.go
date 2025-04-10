// Package repl contains binary log subscription functionality.
package repl

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cashapp/spirit/pkg/dbconn"
	"github.com/cashapp/spirit/pkg/table"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/siddontang/loggers"
	"github.com/sirupsen/logrus"
)

const (
	binlogTrivialThreshold = 10000
	// DefaultBatchSize is the number of rows in each batched REPLACE/DELETE statement.
	// Larger is better, but we need to keep the run-time of the statement well below
	// dbconn.maximumLockTime so that it doesn't prevent copy-row tasks from failing.
	// Since on some of our Aurora tables with out-of-cache workloads only copy ~300 rows per second,
	// we probably shouldn't set this any larger than about 1K. It will also use
	// multiple-flush-threads, which should help it group commit and still be fast.
	// This is only used as an initial starting value. It will auto-scale based on the DefaultTargetBatchTime.
	DefaultBatchSize = 1000
	// minBatchSize is the minimum batch size that we will allow the targetBatchSize to be.
	minBatchSize = 5
	// DefaultTargetBatchTime is the target time for flushing REPLACE/DELETE statements.
	DefaultTargetBatchTime = time.Millisecond * 500

	// DefaultFlushInterval is the time that the client will flush all binlog changes to disk.
	// Longer values require more memory, but permit more merging.
	// I expect we will change this to 1hr-24hr in the future.
	DefaultFlushInterval = 30 * time.Second
	// DefaultTimeout is how long BlockWait is supposed to wait before returning errors.
	DefaultTimeout = 10 * time.Second
)

type Client struct {
	sync.Mutex
	host     string
	username string
	password string

	syncer   *replication.BinlogSyncer
	streamer *replication.BinlogStreamer

	// The DB connection is used for queries like SHOW MASTER STATUS
	// or flushing changes in subscriptions.
	db       *sql.DB
	dbConfig *dbconn.DBConfig

	// subscriptions is a map of tables that are actively
	// watching for changes on. The key is schemaName.tableName.
	// each subscription has its own set of changes.
	subscriptions map[string]*subscription

	// onDDL is a channel that is used to notify of
	// any schema changes. It will send any changes,
	// and the caller is expected to filter it.
	onDDL chan string

	serverID    uint32         // server ID for the binlog reader
	bufferedPos mysql.Position // buffered position
	flushedPos  mysql.Position // safely written to new table

	statisticsLock  sync.Mutex
	targetBatchTime time.Duration
	targetBatchSize int64 // will auto-adjust over time, use atomic to read/set
	timingHistory   []time.Duration
	concurrency     int

	isMySQL84 bool

	// The periodic flush lock is just used for ensuring only one periodic flush runs at a time,
	// and when we disable it, no more periodic flushes will run. The actual flushing is protected
	// by a lower level lock (sync.Mutex on Client)
	periodicFlushLock    sync.Mutex
	periodicFlushEnabled bool

	cancelFunc func()
	logger     loggers.Advanced
}

// NewClient creates a new Client instance.
func NewClient(db *sql.DB, host string, username, password string, config *ClientConfig) *Client {
	return &Client{
		db:              db,
		dbConfig:        dbconn.NewDBConfig(),
		host:            host,
		username:        username,
		password:        password,
		logger:          config.Logger,
		targetBatchTime: config.TargetBatchTime,
		targetBatchSize: DefaultBatchSize, // initial starting value.
		concurrency:     config.Concurrency,
		subscriptions:   make(map[string]*subscription),
		onDDL:           config.OnDDL,
		serverID:        config.ServerID,
	}
}

type ClientConfig struct {
	TargetBatchTime time.Duration
	Concurrency     int
	Logger          loggers.Advanced
	OnDDL           chan string
	ServerID        uint32
}

// NewServerID randomizes the server ID to avoid conflicts with other binlog readers.
// This uses the same logic as canal:
func NewServerID() uint32 {
	return uint32(rand.New(rand.NewSource(time.Now().Unix())).Intn(1000)) + 1001
}

// NewClientDefaultConfig returns a default config for the copier.
func NewClientDefaultConfig() *ClientConfig {
	return &ClientConfig{
		Concurrency:     4,
		TargetBatchTime: DefaultTargetBatchTime,
		Logger:          logrus.New(),
		OnDDL:           nil,
		ServerID:        NewServerID(),
	}
}

// AddSubscription adds a new subscription.
// Returns an error if a subscription already exists for the given table.
func (c *Client) AddSubscription(currentTable, newTable *table.TableInfo, keyAboveCopierCallback func(interface{}) bool) error {
	c.Lock()
	defer c.Unlock()

	subKey := EncodeSchemaTable(currentTable.SchemaName, currentTable.TableName)
	if _, exists := c.subscriptions[subKey]; exists {
		return fmt.Errorf("subscription already exists for table %s.%s", currentTable.SchemaName, currentTable.TableName)
	}

	c.subscriptions[subKey] = &subscription{
		table:                  currentTable,
		newTable:               newTable,
		deltaMap:               make(map[string]bool),
		c:                      c,
		keyAboveCopierCallback: keyAboveCopierCallback,
	}
	return nil
}

// setBufferedPos updates the in-memory position that all changes have been read
// but not necessarily flushed.
func (c *Client) setBufferedPos(pos mysql.Position) {
	c.Lock()
	defer c.Unlock()
	c.bufferedPos = pos
}

// getBufferedPos returns the buffered position under a mutex.
func (c *Client) getBufferedPos() mysql.Position {
	c.Lock()
	defer c.Unlock()
	return c.bufferedPos
}

// SetFlushedPos updates the known safe position that all changes have been flushed.
// It is used for resuming from a checkpoint.
func (c *Client) SetFlushedPos(pos mysql.Position) {
	c.Lock()
	defer c.Unlock()
	c.flushedPos = pos
}

func (c *Client) AllChangesFlushed() bool {
	c.Lock()
	defer c.Unlock()
	// We check if the buffered position is ahead of the flushed position.
	// We have a mutex, so we can read safely.
	if c.bufferedPos.Compare(c.flushedPos) > 0 {
		c.logger.Warnf("Binlog reader info flushed-pos=%v buffered-pos=%v. Discrepancies could be due to modifications on other tables.", c.flushedPos, c.bufferedPos)
	}
	// We check if all subscriptions have flushed their changes.
	for _, subscription := range c.subscriptions {
		if subscription.getDeltaLen() > 0 {
			return false
		}
	}
	return true
}

func (c *Client) GetBinlogApplyPosition() mysql.Position {
	c.Lock()
	defer c.Unlock()
	return c.flushedPos
}

// GetDeltaLen returns the total number of changes
// that are pending across all subscriptions.
// Acquires the client lock for thread safety.
func (c *Client) GetDeltaLen() int {
	c.Lock()
	defer c.Unlock()
	deltaLen := 0
	for _, subscription := range c.subscriptions {
		deltaLen += subscription.getDeltaLen()
	}
	return deltaLen
}

func (c *Client) getCurrentBinlogPosition() (mysql.Position, error) {
	var binlogFile, fake string
	var binlogPos uint32
	var binlogPosStmt = "SHOW MASTER STATUS"
	if c.isMySQL84 {
		binlogPosStmt = "SHOW BINARY LOG STATUS"
	}
	err := c.db.QueryRow(binlogPosStmt).Scan(&binlogFile, &binlogPos, &fake, &fake, &fake)
	if err != nil {
		return mysql.Position{}, err
	}
	return mysql.Position{
		Name: binlogFile,
		Pos:  binlogPos,
	}, nil
}

func (c *Client) Run(ctx context.Context) (err error) {
	c.Lock()
	defer c.Unlock()

	host, portStr, err := net.SplitHostPort(c.host)
	if err != nil {
		return fmt.Errorf("failed to parse host: %w", err)
	}
	// convert portStr to a uint16
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return fmt.Errorf("failed to parse port: %w", err)
	}
	cfg := replication.BinlogSyncerConfig{
		ServerID: c.serverID,
		Flavor:   "mysql",
		Host:     host,
		Port:     uint16(port),
		User:     c.username,
		Password: c.password,
		Logger:   NewLogWrapper(c.logger),
	}
	if dbconn.IsRDSHost(c.host) {
		cfg.TLSConfig = dbconn.NewTLSConfig()
	}
	if cfg.TLSConfig != nil {
		// Set the ServerName so that the TLS config can verify the server's certificate.
		cfg.TLSConfig.ServerName = host
	}
	if dbconn.IsMySQL84(c.db) { // handle MySQL 8.4
		c.isMySQL84 = true
	}
	// Determine where to start the sync from.
	// We default from what the current position is right
	// now, but for resume cases we just need to check that the
	// position is resumable.
	if c.flushedPos.Name == "" {
		c.flushedPos, err = c.getCurrentBinlogPosition()
		if err != nil {
			return errors.New("failed to get binlog position, check binary is enabled")
		}
	} else if c.binlogPositionIsImpossible() {
		return errors.New("binlog position is impossible, the source may have already purged it")
	}
	c.syncer = replication.NewBinlogSyncer(cfg)
	c.streamer, err = c.syncer.StartSync(c.flushedPos)
	if err != nil {
		return fmt.Errorf("failed to start binlog streamer: %w", err)
	}
	// Start the binlog reader in a go routine, using a context with cancel.
	// Write the cancel function to c.cancelFunc
	ctx, c.cancelFunc = context.WithCancel(ctx)
	go c.readStream(ctx)
	return nil
}

// readStream continuously reads the binlog stream. It is usually called in a go routine.
// It will read the stream until the context is closed, or an error occurs.
func (c *Client) readStream(ctx context.Context) {
	c.Lock()
	currentLogName := c.flushedPos.Name
	c.Unlock()
	for {
		// Read the next event from the stream.
		// if we get an error, return. it is probably a context cancel.
		ev, err := c.streamer.GetEvent(ctx)
		if err != nil {
			return // stopping reader
		}
		if ev == nil {
			continue
		}
		// Handle the event.
		switch ev.Event.(type) {
		case *replication.RotateEvent:
			// Rotate event, update the flushed position.
			rotateEvent := ev.Event.(*replication.RotateEvent)
			currentLogName = string(rotateEvent.NextLogName)
		case *replication.RowsEvent:
			// Rows event, check if there are any active subscriptions
			// for it, and pass it to the subscription.
			if err = c.processRowsEvent(ev, ev.Event.(*replication.RowsEvent)); err != nil {
				panic("could not process events")
			}
		case *replication.QueryEvent:
			// Query event, check if it is a DDL statement,
			// in which case we need to notify the caller.
			queryEvent := ev.Event.(*replication.QueryEvent)
			tables, err := extractTablesFromDDLStmts(string(queryEvent.Schema), string(queryEvent.Query))
			if err != nil {
				panic("could not process statement") // could not parse DDL stmts, need to error
			}
			for _, table := range tables {
				c.processDDLNotification(table)
			}
		default:
			// Unsure how to handle this event.
		}
		// Update the buffered position
		// under a mutex.
		c.setBufferedPos(mysql.Position{
			Name: currentLogName,
			Pos:  ev.Header.LogPos,
		})
	}
}

func (c *Client) processDDLNotification(table string) {
	c.Lock()
	defer c.Unlock()
	if c.onDDL == nil {
		return // no one is listening for DDL events
	}
	c.onDDL <- table
}

// processRowsEvent processes a RowsEvent. It will search all active
// subscriptions to find one that matches the event's table:
//
//   - If there is no subscription, the event will be ignored.
//   - If there is, it will call the subscription's keyHasChanged method
//     with the PK that has been changed.
//
// We acquire a mutex when processing row events because we don't want a new subscription
// to be added (uses mutex) and we miss processing for rows on it.
func (c *Client) processRowsEvent(ev *replication.BinlogEvent, e *replication.RowsEvent) error {
	c.Lock()
	defer c.Unlock()

	subName := EncodeSchemaTable(string(e.Table.Schema), string(e.Table.Table))
	sub, ok := c.subscriptions[subName]
	if !ok {
		return nil // ignore event, it could be to a _new table.
	}
	eventType := parseEventType(ev.Header.EventType)
	var i = 0
	for _, row := range e.Rows {
		if eventType == eventTypeUpdate {
			// For update events there are always before and after images (i.e. e.Rows is always in pairs.)
			// We only need to capture one of the events, and since in MINIMAL RBR row
			// image the PK is only included in the before, we chose that one.
			i++
			if i%2 == 0 {
				continue
			}
		}
		key, err := sub.table.PrimaryKeyValues(row)
		if err != nil {
			return err
		}
		if len(key) == 0 {
			return fmt.Errorf("no primary key found for row: %#v", row)
		}
		switch eventType {
		case eventTypeInsert, eventTypeUpdate:
			sub.keyHasChanged(key, false)
		case eventTypeDelete:
			sub.keyHasChanged(key, true)
		default:
			c.logger.Errorf("unknown event type: %v", ev.Header.EventType)
		}
	}
	return nil
}

func (c *Client) binlogPositionIsImpossible() bool {
	rows, err := c.db.Query("SHOW BINARY LOGS")
	if err != nil {
		return true // if we can't get the logs, its already impossible
	}
	defer rows.Close()
	var logname, size, encrypted string
	for rows.Next() {
		if err := rows.Scan(&logname, &size, &encrypted); err != nil {
			return true
		}
		if logname == c.flushedPos.Name {
			return false // We just need presence of the log file for success
		}
	}
	if rows.Err() != nil {
		return true // can't determine.
	}
	return true
}

func (c *Client) Close() {
	if c.syncer != nil {
		c.syncer.Close()
	}
	if c.cancelFunc != nil {
		c.cancelFunc()
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
	c.Lock()
	newFlushedPos := c.bufferedPos
	c.Unlock()

	for _, subscription := range c.subscriptions {
		if err := subscription.flush(ctx, underLock, lock); err != nil {
			return err
		}
	}
	// Update the position that has been flushed.
	c.SetFlushedPos(newFlushedPos)
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
		// a new loop.
		if err := c.BlockWait(ctx); err != nil {
			c.logger.Warnf("error waiting for binlog reader to catch up: %v", err)
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

// StopPeriodicFlush disables the periodic flush, also guaranteeing
// when it returns there is no current flush running
func (c *Client) StopPeriodicFlush() {
	c.periodicFlushLock.Lock()
	defer c.periodicFlushLock.Unlock()
	c.periodicFlushEnabled = false
}

// StartPeriodicFlush starts a loop that periodically flushes the binlog changeset.
// This is used by the migrator to ensure the binlog position is advanced.
func (c *Client) StartPeriodicFlush(ctx context.Context, interval time.Duration) {
	c.periodicFlushLock.Lock()
	c.periodicFlushEnabled = true
	c.periodicFlushLock.Unlock()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.periodicFlushLock.Lock()
			// At some point before cutover we want to disable th periodic flush.
			// The migrator will do this by calling StopPeriodicFlush()
			if !c.periodicFlushEnabled {
				c.periodicFlushLock.Unlock()
				return
			}
			startLoop := time.Now()
			c.logger.Debug("starting periodic flush of binary log")
			// The periodic flush does not respect the throttler since we want to advance the binlog position
			// we allow this to run, and then expect that if it is under load the throttler
			// will kick in and slow down the copy-rows.
			if err := c.flush(ctx, false, nil); err != nil {
				c.logger.Errorf("error flushing binary log: %v", err)
			}
			c.periodicFlushLock.Unlock()
			c.logger.Infof("finished periodic flush of binary log: total-duration=%v batch-size=%d",
				time.Since(startLoop),
				atomic.LoadInt64(&c.targetBatchSize),
			)
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
	targetPos, err := c.getCurrentBinlogPosition()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, DefaultTimeout)
	defer cancel()
	for ctx.Err() == nil { // end when the timeout is reached.
		// Inject some noise into the binlog stream
		// This is to ensure that we are past the current position
		// without an off-by-one error
		if err := dbconn.Exec(ctx, c.db, "FLUSH BINARY LOGs"); err != nil {
			break // error flushing binary logs
		}
		// fetch the bufferPos under a mutex.
		if c.getBufferedPos().Compare(targetPos) >= 0 {
			return nil // we are up to date!
		}
		// We are not caught up yet, so we need to wait.
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("timed out waiting for buffered position to catch up to target position")
}

// feedback provides feedback on the apply time of changesets.
// We use this to refine the targetBatchSize. This is a little bit
// different for feedback for the copier, because:
//
//  1. frequently the batches will not be full.
//  2. feedback is (at least currently) global to all subscriptions,
//     and does not take into account that inserting into a 2 col table
//     with 0 indexes is much faster than inserting into a 10 col table with 5 indexes.
//
// We still need to use a p90-like mechanism though,
// because the rows being changed are by definition more likely to be hotspots.
// Hotspots == Lock Contention. This is one of the exact reasons why we are
// chunking in the first place. The probability that the applier can cause
// impact on OLTP workloads is much higher than the copier.
func (c *Client) feedback(numberOfKeys int, d time.Duration) {
	c.statisticsLock.Lock()
	defer c.statisticsLock.Unlock()
	if numberOfKeys == 0 {
		return // can't calculate anything, just return
	}
	// For the p90-like mechanism rather than storing all the previous
	// durations, because the numberOfKeys is variable we instead store
	// the timePerKey. We then adjust the targetBatchSize based on this.
	// This creates some skew because small batches will have a higher
	// timePerKey, which can create a back log. Which results in a smaller
	// timePerKey. So at least the skew *should* be self-correcting. This
	// has not yet been proven though.
	timePerKey := d / time.Duration(numberOfKeys)
	c.timingHistory = append(c.timingHistory, timePerKey)

	// If we have enough feedback re-evaluate the target batch size
	// based on the p90 timePerKey.
	if len(c.timingHistory) >= 10 {
		timePerKey := table.LazyFindP90(c.timingHistory)
		newBatchSize := int64(float64(c.targetBatchTime) / float64(timePerKey))
		if newBatchSize < minBatchSize {
			newBatchSize = minBatchSize
		}
		atomic.StoreInt64(&c.targetBatchSize, newBatchSize)
		c.timingHistory = nil // reset
	}
}

// SetKeyAboveWatermarkOptimization sets the key above watermark optimization
// for all subscriptions. In future this should become obsolete!
func (c *Client) SetKeyAboveWatermarkOptimization(newVal bool) {
	c.Lock()
	defer c.Unlock()

	for _, sub := range c.subscriptions {
		sub.setKeyAboveWatermarkOptimization(newVal)
	}
}

func (c *Client) SetDDLNotificationChannel(ch chan string) {
	c.Lock()
	defer c.Unlock()
	c.onDDL = ch
}
