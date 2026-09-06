package move

import (
	"database/sql"
	"fmt"
)

// As in migration, reserve checksum repair/prefetch, checkpoint/flush polling,
// at least one statistics query, and a drain connection. The runtime reserve
// adds the remaining per-table statistics queries on each source pool.
const minChecksumPhaseReserve = 6

func (r *Runner) fitReadThreadsToPools() error {
	if r.move.MaxConnections <= 0 {
		return nil
	}
	reserve := minChecksumPhaseReserve + max(0, len(r.sourceTables)-1)
	// Usually each source/target owns a distinct *sql.DB, even when hosts are
	// shared. Programmatic callers may reuse a handle across targets, however:
	// DistributedChecker then pins multiple snapshot pools on that handle.
	uses := make(map[*sql.DB]int)
	for _, source := range r.sources {
		uses[source.db]++
	}
	for _, target := range r.targets {
		uses[target.DB]++
	}
	copies := 1
	for db, n := range uses {
		if db != nil {
			copies = max(copies, n)
		}
	}
	reserve = max(reserve, copies+2) // snapshot creation holds a lock per occurrence
	if r.move.MaxConnections < 2*copies+1 {
		return fmt.Errorf("--max-connections (%d) cannot hold %d checksum snapshot pools and their locks on a shared connection pool", r.move.MaxConnections, copies)
	}
	// Preserve at least one reader, matching migration; advisory control-plane
	// queries may queue when the requested budget cannot cover all headroom.
	available := max(1, (r.move.MaxConnections-reserve)/copies)
	start := min(r.move.Threads, available)
	if start != r.move.Threads {
		r.logger.Info("fitting read threads to the connection pool", "threads", start, "max_connections", r.move.MaxConnections, "reserved", reserve)
	}
	r.move.Threads = start
	return nil
}
