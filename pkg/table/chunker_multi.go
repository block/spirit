package table

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// multiChunker wraps multiple chunkers and distributes Next() calls
// to the chunker that has made the least progress
type multiChunker struct {
	sync.Mutex
	chunkers []Chunker
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
	return &multiChunker{
		chunkers: c,
	}
}

// Open opens all wrapped chunkers
func (m *multiChunker) Open() error {
	m.Lock()
	defer m.Unlock()

	if m.isOpen {
		return errors.New("multi-chunker is already open")
	}

	// For now, The child chunkers are already open.
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
	for i, chunker := range m.chunkers {
		if err := chunker.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close chunker %d: %w", i, err))
		}
	}

	m.isOpen = false

	if len(errs) > 0 {
		return fmt.Errorf("errors closing chunkers: %v", errs)
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
	var selectedChunker Chunker
	var minProgressPercent = 2.0 // Start higher than 100% so any real progress is selected
	var maxTotalRows uint64 = 0
	var hasActiveChunkers = false

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

		// Select chunker with lowest progress percentage
		// If percentages are equal (including both 0), prefer the one with more total rows
		if progressPercent < minProgressPercent ||
			(progressPercent == minProgressPercent && totalRowsExpected > maxTotalRows) {
			minProgressPercent = progressPercent
			maxTotalRows = totalRowsExpected
			selectedChunker = chunker
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
// The interface will need to be changed to accept (table, key) to route properly
func (m *multiChunker) KeyAboveHighWatermark(key any) bool {
	// TODO: Interface needs to be changed to KeyAboveHighWatermark(table, key)
	// to properly route to the correct underlying chunker
	return false
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

// OpenAtWatermark is not yet implemented for multi-chunker
func (m *multiChunker) OpenAtWatermark(watermark string, datum Datum, rowsCopied uint64) error {
	return errors.New("OpenAtWatermark not yet implemented for multi-chunker")
}

// GetLowWatermark is not yet implemented for multi-chunker
func (m *multiChunker) GetLowWatermark() (string, error) {
	return "", errors.New("GetLowWatermark not yet implemented for multi-chunker")
}

// Tables returns all tables from all chunkers
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
