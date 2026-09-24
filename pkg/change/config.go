package change

import (
	"fmt"
	"log/slog"

	"github.com/block/spirit/pkg/dbconn"
)

// FatalReason tells the caller's CancelFunc why the change client hit a fatal
// condition, so the caller can decide which of its state (if any) must be
// invalidated before cancelling.
type FatalReason int

const (
	// FatalReasonSchemaChange means DDL was detected on a watched table.
	// Persisted resume state (checkpoints) describes the table's old
	// definition, so a caller that keeps such state must invalidate it:
	// resuming against the changed table could corrupt data.
	FatalReasonSchemaChange FatalReason = iota
	// FatalReasonStreamError means the change stream itself failed fatally
	// (streamer recreation attempts exhausted, or a row event could not be
	// processed). The watched tables are not known to have changed, so
	// persisted resume state remains valid and a retry can resume from it.
	FatalReasonStreamError
)

// String implements fmt.Stringer for logging.
func (f FatalReason) String() string {
	switch f {
	case FatalReasonSchemaChange:
		return "schema-change"
	case FatalReasonStreamError:
		return "stream-error"
	default:
		return fmt.Sprintf("unknown-fatal-reason(%d)", int(f))
	}
}

type ClientConfig struct {
	Logger   *slog.Logger
	ServerID uint32
	DBConfig *dbconn.DBConfig // Database configuration including TLS settings

	// CancelFunc is an optional callback from the caller (e.g. migration or move runner).
	// It is called when a DDL change is detected on a subscribed table
	// (FatalReasonSchemaChange), or when a fatal stream error occurs, such as
	// minimal RBR detection or exhausted streamer recreation attempts
	// (FatalReasonStreamError). The caller is expected to handle cancellation
	// and cleanup, using reason to decide whether persisted resume state
	// (e.g. a checkpoint) must be invalidated (schema change) or is still
	// safe to resume from (stream error).
	// It returns true if the error was acted upon (caller actually cancelled),
	// or false if it was ignored (e.g. because the caller is already past cutover).
	CancelFunc func(reason FatalReason) bool

	// DDLFilterSchema, when set, broadens DDL detection to cancel on any DDL change
	// in the specified schema, rather than only on exact table matches against subscriptions.
	// This is used by the move runner to detect DDL on any table in the source database.
	DDLFilterSchema string

	// DDLFilterTables, when set alongside DDLFilterSchema, narrows the schema-level
	// DDL detection to only the specified table names. This is used for partial moves
	// where only specific tables from a schema are being moved — DDL on unrelated
	// tables in the same schema should not trigger cancellation.
	// If empty (and DDLFilterSchema is set), all tables in the schema trigger cancellation.
	DDLFilterTables []string

	// SubscriptionSoftLimitBytes overrides DefaultSubscriptionSoftLimitBytes
	// for new subscriptions. Set to a negative value to disable the cap
	// entirely (HasChanged will never block on memory). Zero (the
	// zero-value default) means use DefaultSubscriptionSoftLimitBytes.
	SubscriptionSoftLimitBytes int64

	// SubscriptionSoftLimitChanges overrides
	// DefaultSubscriptionSoftLimitChanges for new subscriptions: the cap on
	// pending change *count* before HasChanged parks, applied alongside
	// SubscriptionSoftLimitBytes. Set to a negative value to disable the cap
	// entirely. Zero (the zero-value default) means use
	// DefaultSubscriptionSoftLimitChanges.
	SubscriptionSoftLimitChanges int

	// FlushConcurrency overrides DefaultFlushConcurrency for new
	// subscriptions: the maximum number of applier batches a map-mode
	// flush keeps in flight concurrently. Set to a negative value to
	// force serial flushing. Zero (the zero-value default) means use
	// DefaultFlushConcurrency.
	FlushConcurrency int

	// BatchSize overrides DefaultBatchSize for new subscriptions: the
	// maximum number of rows one map-mode flush batch renders into a
	// single statement. Zero (the zero-value default) means use
	// DefaultBatchSize; a negative value is clamped to one row per
	// statement.
	//
	// This travels with FlushConcurrency rather than being set on its
	// own, because the two together decide how many rows a drain has in
	// flight. See autoscale.FlushBounds, which is what sets both when
	// the migration runner sizes them from the instance.
	BatchSize int

	// UnderLoad reports whether the target is currently loaded enough that the
	// flush should narrow. Nil (the zero value) means no signal, and the drain
	// runs at its configured width exactly as it did before this existed.
	//
	// This is the change feed's only view of server load, and it exists because
	// the feed was previously the one write path with no such view at all. The
	// flush is deliberately not throttled — the binlog position has to keep
	// advancing or the migration loses its retention window — and the original
	// reasoning was that the copier would absorb the load on its behalf. That
	// held while the flush was a fixed 8 batches wide. Once the width became
	// instance-derived (up to 32) the absorbing side kept shedding while the
	// widened side never did, so under sustained load the copier would shed to
	// almost nothing while the flush stayed at full width and the total barely
	// moved. See bufferedMap.adaptFlushLoad.
	//
	// It is a func rather than a throttler because the change feed has no
	// business importing one, and because the migration runner swaps its
	// throttler during setup — a value captured at construction would be the
	// wrong one.
	UnderLoad func() bool
}

// resolveFlushConcurrency normalizes the FlushConcurrency knob for the
// clients: 0 (the zero value) means DefaultFlushConcurrency, negative
// means an explicit opt-out to a serial drain. Both clients resolve
// through this so the same ClientConfig can never produce different
// concurrency semantics per client type.
func (c *ClientConfig) resolveFlushConcurrency() int {
	if c.FlushConcurrency == 0 {
		return DefaultFlushConcurrency
	}
	if c.FlushConcurrency < 0 {
		return 1 // explicit opt-out: serial
	}
	return c.FlushConcurrency
}

// resolveBatchSize normalizes the BatchSize knob for the clients, on the
// same 0-means-default footing as resolveFlushConcurrency. Negative is
// clamped to a single row rather than treated as an opt-out, because
// there is no such thing as a batch of no rows — the smallest meaningful
// request is a statement per row.
func (c *ClientConfig) resolveBatchSize() int {
	if c.BatchSize == 0 {
		return DefaultBatchSize
	}
	return max(1, c.BatchSize)
}

// NewClientDefaultConfig returns a default config for the copier.
func NewClientDefaultConfig() *ClientConfig {
	return &ClientConfig{
		Logger:   slog.Default(),
		ServerID: NewServerID(),
	}
}
