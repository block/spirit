package table

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMultiChunker(t *testing.T) {
	t.Run("EmptyChunkers", func(t *testing.T) {
		chunker := NewMultiChunker()
		assert.Nil(t, chunker)
	})

	t.Run("SingleChunker", func(t *testing.T) {
		mock := NewMockChunker("table1", 1000)
		chunker := NewMultiChunker(mock)
		assert.Equal(t, mock, chunker)
	})

	t.Run("MultipleChunkers", func(t *testing.T) {
		mock1 := NewMockChunker("table1", 1000)
		mock2 := NewMockChunker("table2", 2000)
		chunker := NewMultiChunker(mock1, mock2)
		assert.IsType(t, &multiChunker{}, chunker)

		multiChunker := chunker.(*multiChunker)
		assert.Len(t, multiChunker.chunkers, 2)
		assert.Contains(t, multiChunker.chunkers, "table1")
		assert.Contains(t, multiChunker.chunkers, "table2")
	})
}

func TestMultiChunkerLifecycle(t *testing.T) {
	mock1 := NewMockChunker("table1", 1000)
	mock2 := NewMockChunker("table2", 2000)
	chunker := NewMultiChunker(mock1, mock2).(*multiChunker)

	t.Run("Open", func(t *testing.T) {
		err := chunker.Open()
		assert.NoError(t, err)
		assert.True(t, chunker.isOpen)

		// Double open should fail
		err = chunker.Open()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already open")
	})

	t.Run("OpenError", func(t *testing.T) {
		mock3 := NewMockChunker("table3", 1000)
		mock4 := NewMockChunker("table4", 1000)
		mock3.SetOpenError(errors.New("open failed"))
		chunker2 := NewMultiChunker(mock3, mock4).(*multiChunker)

		err := chunker2.Open()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to open child chunker")
	})

	t.Run("Close", func(t *testing.T) {
		err := chunker.Close()
		assert.NoError(t, err)
		assert.False(t, chunker.isOpen)
	})

	t.Run("CloseError", func(t *testing.T) {
		mock3 := NewMockChunker("table3", 1000)
		mock4 := NewMockChunker("table4", 1000)
		mock3.SetCloseError(errors.New("close failed"))
		chunker2 := NewMultiChunker(mock3, mock4).(*multiChunker)
		err := chunker2.Open()
		require.NoError(t, err)

		err = chunker2.Close()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to close chunker")
	})
}

func TestMultiChunkerProgressBasedSelection(t *testing.T) {
	mock1 := NewMockChunker("table1", 1000)
	mock2 := NewMockChunker("table2", 2000)
	mock3 := NewMockChunker("table3", 500)

	chunker := NewMultiChunker(mock1, mock2, mock3)
	require.NoError(t, chunker.Open())
	defer chunker.Close()

	t.Run("SelectLowestProgress", func(t *testing.T) {
		// Set different progress levels
		mock1.SimulateProgress(0.5) // 50%
		mock2.SimulateProgress(0.1) // 10% - should be selected
		mock3.SimulateProgress(0.9) // 90%

		chunk, err := chunker.Next()
		assert.NoError(t, err)
		assert.Equal(t, "table2", chunk.Table.TableName)
	})

	t.Run("SelectLargestWhenEqualProgress", func(t *testing.T) {
		// Reset progress to equal levels
		mock1.SimulateProgress(0.0) // 0%, 1000 rows
		mock2.SimulateProgress(0.0) // 0%, 2000 rows - should be selected (largest)
		mock3.SimulateProgress(0.0) // 0%, 500 rows

		chunk, err := chunker.Next()
		assert.NoError(t, err)
		assert.Equal(t, "table2", chunk.Table.TableName)
	})

	t.Run("SkipCompletedChunkers", func(t *testing.T) {
		// Mark some chunkers as complete
		mock1.MarkAsComplete()
		mock3.MarkAsComplete()
		mock2.SimulateProgress(0.5) // Only this one is active

		chunk, err := chunker.Next()
		assert.NoError(t, err)
		assert.Equal(t, "table2", chunk.Table.TableName)
	})

	t.Run("AllChunkersComplete", func(t *testing.T) {
		mock1.MarkAsComplete()
		mock2.MarkAsComplete()
		mock3.MarkAsComplete()

		_, err := chunker.Next()
		assert.ErrorIs(t, err, ErrTableIsRead)
	})
}

func TestMultiChunkerIsRead(t *testing.T) {
	mock1 := NewMockChunker("table1", 1000)
	mock2 := NewMockChunker("table2", 2000)
	chunker := NewMultiChunker(mock1, mock2).(*multiChunker)

	t.Run("NotReadWhenSomeActive", func(t *testing.T) {
		mock1.MarkAsComplete()
		mock2.SimulateProgress(0.5) // Still active

		assert.False(t, chunker.IsRead())
	})

	t.Run("ReadWhenAllComplete", func(t *testing.T) {
		mock1.MarkAsComplete()
		mock2.MarkAsComplete()

		assert.True(t, chunker.IsRead())
	})
}

func TestMultiChunkerProgress(t *testing.T) {
	mock1 := NewMockChunker("table1", 1000)
	mock2 := NewMockChunker("table2", 2000)
	chunker := NewMultiChunker(mock1, mock2).(*multiChunker)

	// Simulate some progress
	mock1.SimulateProgress(0.5) // 500 rows
	mock2.SimulateProgress(0.3) // 600 rows

	// Simulate some chunks processed
	mock1.nextCalls = 5
	mock2.nextCalls = 3

	rowsCopied, chunksCopied, totalRows := chunker.Progress()

	assert.Equal(t, uint64(1100), rowsCopied) // 500 + 600
	assert.Equal(t, uint64(8), chunksCopied)  // 5 + 3
	assert.Equal(t, uint64(3000), totalRows)  // 1000 + 2000
}

func TestMultiChunkerFeedbackRouting(t *testing.T) {
	mock1 := NewMockChunker("table1", 1000)
	mock2 := NewMockChunker("table2", 2000)
	chunker := NewMultiChunker(mock1, mock2).(*multiChunker)
	require.NoError(t, chunker.Open())
	defer chunker.Close()

	// Get chunks from different tables
	mock1.SimulateProgress(0.0)
	mock2.SimulateProgress(1.0) // Complete table2 so table1 is selected

	chunk1, err := chunker.Next()
	require.NoError(t, err)
	assert.Equal(t, "table1", chunk1.Table.TableName)

	// Provide feedback
	duration := 100 * time.Millisecond
	actualRows := uint64(500)
	chunker.Feedback(chunk1, duration, actualRows)

	// Check that feedback was routed to the correct chunker
	feedback1 := mock1.GetFeedbackCalls()
	feedback2 := mock2.GetFeedbackCalls()

	assert.Len(t, feedback1, 1)
	assert.Empty(t, feedback2)

	assert.Equal(t, chunk1, feedback1[0].Chunk)
	assert.Equal(t, duration, feedback1[0].Duration)
	assert.Equal(t, actualRows, feedback1[0].ActualRows)
}

func TestMultiChunkerWatermarkHandling(t *testing.T) {
	mock1 := NewMockChunker("table1", 1000)
	mock2 := NewMockChunker("table2", 2000)
	chunker := NewMultiChunker(mock1, mock2).(*multiChunker)

	t.Run("GetLowWatermark", func(t *testing.T) {
		// Simulate some progress
		mock1.SimulateProgress(0.3) // position 300
		mock2.SimulateProgress(0.5) // position 1000

		watermark, err := chunker.GetLowWatermark()
		assert.NoError(t, err)

		// Parse the watermark
		var watermarks map[string]string
		err = json.Unmarshal([]byte(watermark), &watermarks)
		assert.NoError(t, err)

		assert.Contains(t, watermarks, "table1")
		assert.Contains(t, watermarks, "table2")

		// Check individual watermarks
		assert.Equal(t, "300", watermarks["table1"])
		assert.Equal(t, "1000", watermarks["table2"])
	})

	t.Run("OpenAtWatermark", func(t *testing.T) {
		// Create watermark
		watermarks := map[string]string{
			"table1": "300",
			"table2": "1000",
		}
		watermarkJSON, err := json.Marshal(watermarks)
		require.NoError(t, err)

		err = chunker.OpenAtWatermark(string(watermarkJSON))
		assert.NoError(t, err)
		assert.True(t, chunker.isOpen)

		// Verify chunkers were opened at correct positions
		progress1, _, _ := mock1.Progress()
		progress2, _, _ := mock2.Progress()
		assert.Equal(t, uint64(300), progress1)
		assert.Equal(t, uint64(1000), progress2)
	})

	t.Run("OpenAtWatermarkError", func(t *testing.T) {
		// Invalid JSON
		err := chunker.OpenAtWatermark("invalid json")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "could not parse multi-chunker watermark")
	})

	t.Run("OpenAtWatermarkMissingTable", func(t *testing.T) {
		// Create fresh mock chunkers for this test to avoid state from previous tests
		mock3 := NewMockChunker("table1", 1000)
		mock4 := NewMockChunker("table2", 2000)

		// Missing table in watermark
		watermarks := map[string]string{
			"table1": "300",
			// table2 missing
		}
		watermarkJSON, err := json.Marshal(watermarks)
		require.NoError(t, err)

		chunker2 := NewMultiChunker(mock3, mock4).(*multiChunker)
		err = chunker2.OpenAtWatermark(string(watermarkJSON))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "could not find chunker for table")
	})
}

func TestMultiChunkerTables(t *testing.T) {
	mock1 := NewMockChunker("table1", 1000)
	mock2 := NewMockChunker("table2", 2000)
	mock3 := NewMockChunker("table1", 500) // Duplicate table name

	chunker := NewMultiChunker(mock1, mock2, mock3).(*multiChunker)

	tables := chunker.Tables()

	// Should have unique tables
	tableNames := make(map[string]bool)
	for _, table := range tables {
		tableNames[table.TableName] = true
	}

	assert.Len(t, tableNames, 2) // Only unique table names
	assert.Contains(t, tableNames, "table1")
	assert.Contains(t, tableNames, "table2")
}

func TestMultiChunkerKeyAboveHighWatermark(t *testing.T) {
	mock1 := NewMockChunker("table1", 1000)
	mock2 := NewMockChunker("table2", 1000)
	chunker := NewMultiChunker(mock1, mock2).(*multiChunker)

	// Should always return false (not supported)
	result := chunker.KeyAboveHighWatermark("any_key")
	assert.False(t, result)
}

func TestMultiChunkerErrorHandling(t *testing.T) {
	t.Run("NextOnClosedChunker", func(t *testing.T) {
		mock1 := NewMockChunker("table1", 1000)
		mock2 := NewMockChunker("table2", 1000)
		chunker := NewMultiChunker(mock1, mock2).(*multiChunker)

		_, err := chunker.Next()
		assert.ErrorIs(t, err, ErrTableNotOpen)
	})

	t.Run("NextError", func(t *testing.T) {
		mock1 := NewMockChunker("table1", 1000)
		mock2 := NewMockChunker("table2", 1000)
		mock1.SetNextError(errors.New("next failed"))
		chunker := NewMultiChunker(mock1, mock2).(*multiChunker)
		require.NoError(t, chunker.Open())
		defer chunker.Close()

		_, err := chunker.Next()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "next failed")
	})

	t.Run("WatermarkError", func(t *testing.T) {
		mock1 := NewMockChunker("table1", 1000)
		mock2 := NewMockChunker("table2", 1000)
		mock1.SetWatermarkError(errors.New("watermark failed"))
		chunker := NewMultiChunker(mock1, mock2).(*multiChunker)

		_, err := chunker.GetLowWatermark()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "watermark failed")
	})
}

func TestMultiChunkerDeterministicBehavior(t *testing.T) {
	// This test ensures that the multi-chunker behaves deterministically
	// given the same input conditions

	createTestScenario := func() (*multiChunker, *MockChunker, *MockChunker, *MockChunker) {
		mock1 := NewMockChunker("table1", 1000)
		mock2 := NewMockChunker("table2", 2000)
		mock3 := NewMockChunker("table3", 500)

		chunker := NewMultiChunker(mock1, mock2, mock3).(*multiChunker)
		require.NoError(t, chunker.Open())

		return chunker, mock1, mock2, mock3
	}

	t.Run("ConsistentSelectionOrder", func(t *testing.T) {
		// Run the same scenario multiple times and ensure consistent results
		results := make([][]string, 3)

		for i := range 3 {
			chunker, mock1, mock2, mock3 := createTestScenario()
			defer chunker.Close()

			// Set identical progress states
			mock1.SimulateProgress(0.1) // 10%
			mock2.SimulateProgress(0.5) // 50%
			mock3.SimulateProgress(0.3) // 30%

			var selectedTables []string
			for j := range 3 {
				chunk, err := chunker.Next()
				if err != nil {
					break
				}
				selectedTables = append(selectedTables, chunk.Table.TableName)

				// Advance the selected chunker's progress
				switch chunk.Table.TableName {
				case "table1":
					mock1.SimulateProgress(0.1 + float64(j+1)*0.2)
				case "table2":
					mock2.SimulateProgress(0.5 + float64(j+1)*0.1)
				case "table3":
					mock3.SimulateProgress(0.3 + float64(j+1)*0.15)
				}
			}

			results[i] = selectedTables
		}

		// All runs should produce the same selection order
		for i := 1; i < len(results); i++ {
			assert.Equal(t, results[0], results[i], "Selection order should be deterministic")
		}
	})
}
