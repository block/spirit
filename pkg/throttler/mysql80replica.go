package throttler

import (
	"errors"
	"sync/atomic"
	"time"
)

type MySQL80Replica struct {
	Repl
	isClosed atomic.Bool
}

// MySQL8LagQuery is a query that is used to get the lag between the source and the replica.
// The implementation is described in https://github.com/block/spirit/issues/286
// It uses performance_schema instead of a heartbeat injection or seconds_behind_source.
const MySQL8LagQuery = `WITH applier_latency AS (
	SELECT MAX(TIMESTAMPDIFF(MICROSECOND, APPLYING_TRANSACTION_IMMEDIATE_COMMIT_TIMESTAMP, NOW())/1000 ) AS applier_latency_ms
	FROM performance_schema.replication_applier_status_by_worker
   ), queue_latency AS (
	SELECT MIN(
	CASE
	 WHEN
	  LAST_QUEUED_TRANSACTION = 'ANONYMOUS' OR
	  LAST_APPLIED_TRANSACTION = 'ANONYMOUS' OR
	  GTID_SUBTRACT(LAST_QUEUED_TRANSACTION, LAST_APPLIED_TRANSACTION) = ''
	 THEN 0
	  ELSE
	  TIMESTAMPDIFF(MICROSECOND, LAST_APPLIED_TRANSACTION_IMMEDIATE_COMMIT_TIMESTAMP, NOW(3))/1000
	END
   ) AS queue_latency_ms,
   IF(MIN(TIMESTAMPDIFF(MINUTE, LAST_QUEUED_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP, NOW()))>1,'IDLE','ACTIVE') as queue_status
   FROM performance_schema.replication_applier_status_by_worker w
   JOIN performance_schema.replication_connection_status s ON s.channel_name = w.channel_name
   )
   SELECT IFNULL(IF(queue_status='IDLE',0,CEIL(GREATEST(applier_latency_ms, queue_latency_ms))),0) as lagMs FROM applier_latency, queue_latency
`

var _ Throttler = &MySQL80Replica{}

// Open starts the lag monitor. This is not gh-ost. The lag monitor is primitive
// because the requirement is only for DR, and not for up-to-date read-replicas.
// Because chunk-sizes are typically 500ms, getting fine-grained metrics is not realistic.
// We only check the replica every 5 seconds, and typically allow up to 120s
// of replica lag, which is a lot.
func (l *MySQL80Replica) Open() error {
	if err := l.UpdateLag(); err != nil {
		return err
	}
	go func() {
		ticker := time.NewTicker(loopInterval)
		defer ticker.Stop()
		for range ticker.C {
			if l.isClosed.Load() {
				return
			}
			if err := l.UpdateLag(); err != nil {
				l.logger.Errorf("error getting lag: %s", err.Error())
			}
		}
	}()
	return nil
}

func (l *MySQL80Replica) Close() error {
	l.isClosed.Store(true)
	return nil
}

// UpdateLag is a MySQL 8.0+ implementation of lag that is a better approximation than "seconds_behind_source".
// It requires performance_schema to be enabled.
func (l *MySQL80Replica) UpdateLag() error {
	var newLagValue int64
	if err := l.replica.QueryRow(MySQL8LagQuery).Scan(&newLagValue); err != nil {
		return errors.New("could not check replication lag, check that this is a MySQL 8.0 replica, and that performance_schema is enabled")
	}
	atomic.StoreInt64(&l.currentLagInMs, newLagValue)
	if l.IsThrottled() {
		l.logger.Warnf("replication delayed, throttling in progress. lag: %v tolerance: %v",
			atomic.LoadInt64(&l.currentLagInMs), l.lagTolerance)
	}
	return nil
}
