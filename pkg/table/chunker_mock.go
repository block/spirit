package table

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// MockChunker provides a controllable chunker for testing multi-chunker behavior
type MockChunker struct {
	mu sync.Mutex

	// Configuration
	tableName string
	totalRows uint64
	chunkSize uint64
	tableInfo *TableInfo

	// State
	isOpen          bool
	currentPosition uint64
	isComplete      bool

	// Control behavior
	openError      error
	closeError     error
	nextError      error
	watermarkError error

	// Tracking
	feedbackCalls []FeedbackCall
	nextCalls     int
	progressCalls int
}

type FeedbackCall struct {
	Chunk      *Chunk
	Duration   time.Duration
	ActualRows uint64
	Timestamp  time.Time
}

// NewMockChunker creates a new mock chunker for testing
func NewMockChunker(tableName string, totalRows uint64) *MockChunker {
	tableInfo := &TableInfo{
		TableName:  tableName,
		SchemaName: "test",
	}

	return &MockChunker{
		tableName:     tableName,
		totalRows:     totalRows,
		chunkSize:     1000, // default chunk size
		tableInfo:     tableInfo,
		feedbackCalls: make([]FeedbackCall, 0),
	}
}

// Configuration methods
func (m *MockChunker) SetOpenError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.openError = err
}

func (m *MockChunker) SetCloseError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closeError = err
}

func (m *MockChunker) SetNextError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextError = err
}

func (m *MockChunker) SetWatermarkError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.watermarkError = err
}

func (m *MockChunker) SetChunkSize(size uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.chunkSize = size
}

// Test helper methods
func (m *MockChunker) SimulateProgress(percentage float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if percentage >= 1.0 {
		m.currentPosition = m.totalRows
		m.isComplete = true
	} else {
		m.currentPosition = uint64(float64(m.totalRows) * percentage)
	}
}

func (m *MockChunker) MarkAsComplete() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currentPosition = m.totalRows
	m.isComplete = true
}

func (m *MockChunker) GetFeedbackCalls() []FeedbackCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]FeedbackCall, len(m.feedbackCalls))
	copy(result, m.feedbackCalls)
	return result
}

func (m *MockChunker) GetCallCounts() (next, progress int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.nextCalls, m.progressCalls
}

// Chunker interface implementation
func (m *MockChunker) Open() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.openError != nil {
		return m.openError
	}

	if m.isOpen {
		return errors.New("mock chunker is already open")
	}

	m.isOpen = true
	return nil
}

func (m *MockChunker) IsRead() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.isRead()
}

func (m *MockChunker) isRead() bool {
	return m.isComplete || m.currentPosition >= m.totalRows
}

func (m *MockChunker) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closeError != nil {
		return m.closeError
	}

	m.isOpen = false
	return nil
}

func (m *MockChunker) Next() (*Chunk, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.nextCalls++

	if m.nextError != nil {
		return nil, m.nextError
	}

	if !m.isOpen {
		return nil, ErrTableNotOpen
	}

	if m.isRead() {
		return nil, ErrTableIsRead
	}

	// Create a chunk
	_ = fmt.Sprintf("%s_chunk_%d", m.tableName, m.nextCalls)
	chunk := &Chunk{
		Table:     m.tableInfo,
		ChunkSize: m.chunkSize,
		LowerBound: &Boundary{
			Value:     []Datum{NewDatum(m.currentPosition, unsignedType)},
			Inclusive: true,
		},
		UpperBound: &Boundary{
			Value:     []Datum{NewDatum(m.currentPosition+m.chunkSize, unsignedType)},
			Inclusive: false,
		},
	}

	// Advance position
	m.currentPosition += m.chunkSize
	if m.currentPosition >= m.totalRows {
		m.currentPosition = m.totalRows
		m.isComplete = true
	}

	return chunk, nil
}

func (m *MockChunker) Feedback(chunk *Chunk, duration time.Duration, actualRows uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.feedbackCalls = append(m.feedbackCalls, FeedbackCall{
		Chunk:      chunk,
		Duration:   duration,
		ActualRows: actualRows,
		Timestamp:  time.Now(),
	})
}

// KeyAboveHighWatermark returns true if the given key is above the current watermark
// It returns FALSE in cases that are difficult to determine (e.g. non-numeric keys)
func (m *MockChunker) KeyAboveHighWatermark(key any) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Convert key to uint64 for comparison
	var keyPos uint64
	switch v := key.(type) {
	case int:
		keyPos = uint64(v)
	case uint64:
		keyPos = v
	case int64:
		keyPos = uint64(v)
	default:
		// For non-numeric keys, return false (always process)
		return false
	}

	// Key is above high watermark if it's greater than current position
	return keyPos > m.currentPosition
}

// KeyBelowLowWatermark returns true if the given key is below the current low watermark
// It returns TRUE in cases that are difficult to determine (e.g. non-numeric keys)
func (m *MockChunker) KeyBelowLowWatermark(key any) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	// For string keys (hashed keys), we need to handle differently
	// Since we can't easily convert hash back to position, return true
	// to allow processing (this matches the behavior of other chunkers
	// that return true to not block flushing when not supported)
	if _, ok := key.(string); ok {
		return true
	}

	// Convert key to uint64 for comparison
	var keyPos uint64
	switch v := key.(type) {
	case int:
		keyPos = uint64(v)
	case uint64:
		keyPos = v
	case int64:
		keyPos = uint64(v)
	default:
		// For non-numeric keys, return true (allow processing)
		return true
	}

	// Key is below low watermark if it's less than current position
	// This means the copier has already passed this key
	return keyPos < m.currentPosition
}

func (m *MockChunker) Progress() (uint64, uint64, uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.progressCalls++

	// Return: rowsRead, chunksCopied, totalRowsExpected
	return m.currentPosition, uint64(m.nextCalls), m.totalRows
}

func (m *MockChunker) OpenAtWatermark(watermark string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.watermarkError != nil {
		return m.watermarkError
	}

	if m.isOpen {
		return errors.New("mock chunker is already open")
	}

	// Simple watermark parsing - just extract position
	var pos uint64
	if err := json.Unmarshal([]byte(watermark), &pos); err != nil {
		return fmt.Errorf("invalid watermark: %w", err)
	}

	m.currentPosition = pos
	m.isOpen = true
	return nil
}

func (m *MockChunker) GetLowWatermark() (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.watermarkError != nil {
		return "", m.watermarkError
	}

	// Simple watermark - just return current position
	watermark, err := json.Marshal(m.currentPosition)
	if err != nil {
		return "", err
	}

	return string(watermark), nil
}

func (m *MockChunker) Tables() []*TableInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	return []*TableInfo{m.tableInfo}
}
