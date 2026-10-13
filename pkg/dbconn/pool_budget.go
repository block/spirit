package dbconn

import "fmt"

// DefaultMaxConnections is the main pool default for migrations, moves and
// continuous syncs. Monitor and advisory-lock connections use separate dedicated pools.
const DefaultMaxConnections = 128

// MinMigrationPoolSize is the smallest --max-connections a migration can
// complete on. It covers the connections that cutover cannot serialize.
//
// The cutover sets the number: it needs the LOCK TABLES connection, the RENAME
// TABLE connection and the flush threads, and unlike every other phase it
// cannot trade connections for time — below the minimum it does not run slower,
// it cannot run. See migration's CutOver.Run, which holds the same number and
// will raise a pool that arrives under it.
//
// Nothing else gets a say. The copy, the checksum and the drain all queue on a
// small pool and finish eventually, so a low --max-connections is the operator
// asking for a slow migration, which is theirs to ask for.
const MinMigrationPoolSize = 5

// ValidateMaxConnections validates an explicit pool budget against
// pinned checksum readers and runner-specific headroom. Zero is unresolved and
// is accepted so callers can apply their defaults after validation.
func ValidateMaxConnections(maxConnections, readers, reserve int) error {
	if err := ValidateConnectionLimit(maxConnections); err != nil {
		return err
	}
	if maxConnections == 0 {
		return nil
	}
	if maxConnections < MinMigrationPoolSize {
		return fmt.Errorf("--max-connections must be at least %d for the cutover to run, got %d", MinMigrationPoolSize, maxConnections)
	}
	if maxConnections < readers+reserve {
		return fmt.Errorf("--max-connections (%d) is below what the checksum phase needs: %d pinned read transactions plus %d reserved for off-pool queries, the control plane and the drain; use at least %d, or lower --threads", maxConnections, readers, reserve, readers+reserve)
	}
	return nil
}

// ReadBoundsForPool fits BOTH bounds of a reader pool without growing the
// connection pool. Fitting just the ceiling fails because consumers floor it
// back to the start (see checksum.NewChecker and the copier's
// resolveReadCeiling). The legacy SingleChecker also pre-opens a snapshot
// transaction per ceiling slot under one table lock (initConnPool); the
// lockless checker borrows a connection per read. Migration, move and sync
// all fit with it. The caller supplies its lifecycle-specific reserve. holds is how many
// connections one reader takes from the busiest pool at once: one for a
// migration, more for a move whose source and target share a handle. Values
// below one mean one. Unresolved connection limits pass through; a small budget
// retains one reader so it can progress.
func ReadBoundsForPool(start, ceiling, maxConnections, reserve, holds int) (int, int) {
	if maxConnections <= 0 {
		return start, ceiling
	}
	fit := max(1, (maxConnections-reserve)/max(1, holds))
	return min(start, fit), min(ceiling, fit)
}

// ValidateConnectionLimit rejects negative limits, which database/sql would
// otherwise interpret as unlimited. Zero means the runner should apply its
// default.
func ValidateConnectionLimit(limit int) error {
	if limit < 0 {
		return fmt.Errorf("--max-connections must be non-negative, got %d", limit)
	}
	return nil
}
