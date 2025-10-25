package table

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// multiChunker wraps multiple chunkers and distributes Next() calls
// to the chunker that has made the least progress
type multiChunker struct {
	sync.Mutex
	chunkers map[string]Chunker // map of table name to chunker for quick lookup
	isOpen   bool
}

var _ Chunker = &multiChunker{}

// NewMultiChunker creates a new multi-chunker that wraps multiple chunkers
func NewMultiChunker(c ...Chunker) Chunker {
	if len(c) == 0 {
		return nil
	}
	if len(c) == 1 {
		return c[0]
	}
	chunkers := make(map[string]Chunker, len(c))
	for _, chunker := range c {
		tables := chunker.Tables()
		if len(tables) == 0 {
			continue
		}
		// By convention the first table is the "current" table
		table := tables[0]
		chunkers[table.TableName] = chunker
	}
	return &multiChunker{
		chunkers: chunkers,
	}
}

// Open opens all wrapped chunkers
func (m *multiChunker) Open() error {
	m.Lock()
	defer m.Unlock()

	if m.isOpen {
		return errors.New("multi-chunker is already open")
	}

	// Open each of the child chunkers.
	for _, chunker := range m.chunkers {
		if err := chunker.Open(); err != nil {
			return fmt.Errorf("failed to open child chunker: %w", err)
		}
	}
	m.isOpen = true
	return nil
}

// IsRead returns true if all chunkers are read
func (m *multiChunker) IsRead() bool {
	m.Lock()
	defer m.Unlock()

	for _, chunker := range m.chunkers {
		if !chunker.IsRead() {
			return false
		}
	}
	return true
}

// Close closes all wrapped chunkers
func (m *multiChunker) Close() error {
	m.Lock()
	defer m.Unlock()

	var errs []error
	for name, chunker := range m.chunkers {
		if err := chunker.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close chunker %s: %w", name, err))
		}
	}
	m.isOpen = false
	if err := errors.Join(errs...); err != nil {
		return err
	}
	return nil
}

// Next returns the next chunk from the chunker that has made the least progress (by percentage)
func (m *multiChunker) Next() (*Chunk, error) {
	m.Lock()
	defer m.Unlock()

	if !m.isOpen {
		return nil, ErrTableNotOpen
	}

	// Find the chunker with the least progress (lowest percentage)
	// Only consider chunkers that are not complete (IsRead() == false)
	var selectedChunker Chunker
	var minProgressPercent float64
	var maxTotalRows uint64
	var hasActiveChunkers = false
	var firstCandidate = true

	for _, chunker := range m.chunkers {
		if chunker.IsRead() {
			continue // skip chunkers that are done
		}

		hasActiveChunkers = true
		rowsCopied, _, totalRowsExpected := chunker.Progress()

		// Calculate progress percentage
		var progressPercent float64
		if totalRowsExpected > 0 {
			progressPercent = float64(rowsCopied) / float64(totalRowsExpected)
		} else {
			progressPercent = 0.0
		}

		// If it's our first candidate, we select it.
		// Then we could potentially pick a different candidate as we range through
		// the chunkers. We'll pick a better candidate if:
		// - It has a lower progress percentage
		// - If percentages are equal, it has more total rows expected
		if firstCandidate || progressPercent < minProgressPercent ||
			(progressPercent == minProgressPercent && totalRowsExpected > maxTotalRows) {
			minProgressPercent = progressPercent
			maxTotalRows = totalRowsExpected
			selectedChunker = chunker
			firstCandidate = false
		}
	}

	// If no active chunkers are available, all are done
	if !hasActiveChunkers || selectedChunker == nil {
		return nil, ErrTableIsRead
	}
	return selectedChunker.Next()
}

// Feedback forwards feedback to the appropriate chunker based on the chunk's table
func (m *multiChunker) Feedback(chunk *Chunk, duration time.Duration, actualRows uint64) {
	m.Lock()
	defer m.Unlock()

	// Find the chunker that handles this table
	for _, chunker := range m.chunkers {
		tables := chunker.Tables()
		for _, table := range tables {
			if table == chunk.Table {
				chunker.Feedback(chunk, duration, actualRows)
				return
			}
		}
	}
}

// KeyAboveHighWatermark currently not supported for multi-chunker
// The interface would need to be changed to accept (table, key) to route properly
// We work around this by using the child chunkers in repl.AddSubscription() directly.
func (m *multiChunker) KeyAboveHighWatermark(_ any) bool {
	return false
}

// KeyBelowLowWatermark currently not supported for multi-chunker
// We need to return true so callers don't block flushing.
func (m *multiChunker) KeyBelowLowWatermark(_ any) bool {
	return true
}

// Progress returns aggregate progress across all chunkers
func (m *multiChunker) Progress() (uint64, uint64, uint64) {
	m.Lock()
	defer m.Unlock()

	var totalRowsCopied, totalChunksCopied, totalRowsExpected uint64

	for _, chunker := range m.chunkers {
		rowsCopied, chunksCopied, rowsExpected := chunker.Progress()
		totalRowsCopied += rowsCopied
		totalChunksCopied += chunksCopied
		totalRowsExpected += rowsExpected
	}

	return totalRowsCopied, totalChunksCopied, totalRowsExpected
}

// OpenAtWatermark for multi-chunker. We deserialize the watermarks
// into a map, and then call OpenAtWatermark on each child chunker
// with the corresponding watermark. If a table doesn't have a watermark
// (because it wasn't ready when the checkpoint was saved), we call Open()
// instead to start from scratch.
func (m *multiChunker) OpenAtWatermark(watermark string) error {
	m.Lock()
	defer m.Unlock()

	watermarks := make(map[string]string, len(m.chunkers))
	if err := json.Unmarshal([]byte(watermark), &watermarks); err != nil {
		return fmt.Errorf("could not parse multi-chunker watermark: %w", err)
	}

	// Open each chunker - either at watermark if available, or from scratch if not
	for tableName, chunker := range m.chunkers {
		if tableWatermark, hasWatermark := watermarks[tableName]; hasWatermark {
			// Table has a watermark, resume from checkpoint
			if err := chunker.OpenAtWatermark(tableWatermark); err != nil {
				return fmt.Errorf("could not open chunker for table %q at watermark: %w", tableName, err)
			}
		} else {
			// Table doesn't have a watermark (wasn't ready when checkpoint was saved)
			// Start from scratch
			if err := chunker.Open(); err != nil {
				return fmt.Errorf("could not open chunker for table %q from scratch: %w", tableName, err)
			}
		}
	}

	// Set isOpen to true after successfully opening all child chunkers
	m.isOpen = true
	return nil
}

// GetLowWatermark calls GetLowWatermark on all the child chunkers
// and then merges them into a single watermark string.
// For multi-table migrations, we include watermarks for all tables, but skip
// tables that return errors (not ready yet) to avoid blocking checkpoint writing.
func (m *multiChunker) GetLowWatermark() (string, error) {
	watermarks := make(map[string]string, len(m.chunkers))

	for _, chunker := range m.chunkers {
		watermark, err := chunker.GetLowWatermark()
		if err != nil {
			// If this chunker's watermark isn't ready yet, skip it from the checkpoint
			// This prevents one unready table from blocking checkpoint writing for all tables
			// On recovery, tables without watermarks will start from scratch via Open()
			continue
		}
		tbl := chunker.Tables()[0]
		watermarks[tbl.TableName] = watermark
	}
	// We have to serialize the map to a string.
	json, err := json.Marshal(watermarks)
	if err != nil {
		return "", err
	}
	return string(json), nil
}

// Tables returns all tables from all chunkers
// By convention the first table is always the one being migrated.
func (m *multiChunker) Tables() []*TableInfo {
	m.Lock()
	defer m.Unlock()

	var allTables []*TableInfo
	seen := make(map[*TableInfo]bool)

	for _, chunker := range m.chunkers {
		tables := chunker.Tables()
		for _, table := range tables {
			if !seen[table] {
				allTables = append(allTables, table)
				seen[table] = true
			}
		}
	}
	return allTables
}
