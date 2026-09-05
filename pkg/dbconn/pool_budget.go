package dbconn

import "fmt"

// DefaultMaxConnections is the main pool default for finite migrations and
// moves. Monitor and advisory-lock connections use separate dedicated pools.
const DefaultMaxConnections = 128

// MinMigrationPoolSize covers the connections that cutover cannot serialize.
const MinMigrationPoolSize = 5

// ValidateMaxConnections validates an explicit finite-run pool budget against
// pinned checksum readers and runner-specific headroom. Zero is unresolved and
// is accepted so callers can apply their defaults after validation.
func ValidateMaxConnections(maxConnections, readers, reserve int) error {
	if maxConnections < 0 {
		return fmt.Errorf("--max-connections must be non-negative, got %d", maxConnections)
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
// connection pool. Checksums pre-open a snapshot transaction per ceiling slot:
// fitting just the ceiling fails because consumers floor it back to the start.
// The caller supplies its lifecycle-specific reserve. Unresolved connection
// limits pass through; a small budget retains one reader so it can progress.
func ReadBoundsForPool(start, ceiling, maxConnections, reserve int) (int, int) {
	if maxConnections <= 0 {
		return start, ceiling
	}
	fit := max(1, maxConnections-reserve)
	return min(start, fit), min(ceiling, fit)
}
