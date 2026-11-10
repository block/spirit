package table

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewMultiChunker(t *testing.T) {
	t.Run("EmptyChunkers", func(t *testing.T) {
		chunker := NewMultiChunker()
		require.Nil(t, chunker)
	})

	t.Run("SingleChunker", func(t *testing.T) {
		mock := NewMockChunker("table1", 1000)
		chunker := NewMultiChunker(mock)
		require.Equal(t, mock, chunker)
	})

	t.Run("MultipleChunkers", func(t *testing.T) {
		mock1 := NewMockChunker("table1", 1000)
		mock2 := NewMockChunker("table2", 2000)
		chunker := NewMultiChunker(mock1, mock2)
		require.IsType(t, &multiChunker{}, chunker)

		multiChunker := chunker.(*multiChunker)
		require.Len(t, multiChunker.chunkers, 2)
		require.Contains(t, multiChunker.chunkers, "test.table1")
		require.Contains(t, multiChunker.chunkers, "test.table2")
	})
}

func TestMultiChunkerLifecycle(t *testing.T) {
	mock1 := NewMockChunker("table1", 1000)
	mock2 := NewMockChunker("table2", 2000)
	chunker := NewMultiChunker(mock1, mock2).(*multiChunker)

	t.Run("Open", func(t *testing.T) {
		err := chunker.Open()
		require.NoError(t, err)
		require.True(t, chunker.isOpen)

		// Double open should fail
		err = chunker.Open()
		require.Error(t, err)
		require.ErrorIs(t, err, ErrChunkerAlreadyOpen)
	})

	t.Run("OpenError", func(t *testing.T) {
		mock3 := NewMockChunker("table3", 1000)
		mock4 := NewMockChunker("table4", 1000)
		mock3.SetOpenError(errors.New("open failed"))
		chunker2 := NewMultiChunker(mock3, mock4).(*multiChunker)

		err := chunker2.Open()
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to open child chunker")
	})

	t.Run("Close", func(t *testing.T) {
		err := chunker.Close()
		require.NoError(t, err)
		require.False(t, chunker.isOpen)
	})

	t.Run("CloseError", func(t *testing.T) {
		mock3 := NewMockChunker("table3", 1000)
		mock4 := NewMockChunker("table4", 1000)
		mock3.SetCloseError(errors.New("close failed"))
		chunker2 := NewMultiChunker(mock3, mock4).(*multiChunker)
		err := chunker2.Open()
		require.NoError(t, err)

		err = chunker2.Close()
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to close chunker")
	})
}

func TestMultiChunkerProgressBasedSelection(t *testing.T) {
	mock1 := NewMockChunker("table1", 1000)
	mock2 := NewMockChunker("table2", 2000)
	mock3 := NewMockChunker("table3", 500)

	chunker := NewMultiChunker(mock1, mock2, mock3)
	require.NoError(t, chunker.Open())
	defer func() {
		if err := chunker.Close(); err != nil {
			t.Logf("failed to close chunker: %v", err)
		}
	}()

	t.Run("SelectLowestProgress", func(t *testing.T) {
		// Set different progress levels
		mock1.SimulateProgress(0.5) // 50%
		mock2.SimulateProgress(0.1) // 10% - should be selected
		mock3.SimulateProgress(0.9) // 90%

		chunk, err := chunker.Next()
		require.NoError(t, err)
		require.Equal(t, "table2", chunk.Table.TableName)
	})

	t.Run("SelectLargestWhenEqualProgress", func(t *testing.T) {
		// Reset progress to equal levels
		mock1.SimulateProgress(0.0) // 0%, 1000 rows
		mock2.SimulateProgress(0.0) // 0%, 2000 rows - should be selected (largest)
		mock3.SimulateProgress(0.0) // 0%, 500 rows

		chunk, err := chunker.Next()
		require.NoError(t, err)
		require.Equal(t, "table2", chunk.Table.TableName)
	})

	t.Run("SkipCompletedChunkers", func(t *testing.T) {
		// Mark some chunkers as complete
		mock1.MarkAsComplete()
		mock3.MarkAsComplete()
		mock2.SimulateProgress(0.5) // Only this one is active

		chunk, err := chunker.Next()
		require.NoError(t, err)
		require.Equal(t, "table2", chunk.Table.TableName)
	})

	t.Run("AllChunkersComplete", func(t *testing.T) {
		mock1.MarkAsComplete()
		mock2.MarkAsComplete()
		mock3.MarkAsComplete()

		_, err := chunker.Next()
		require.ErrorIs(t, err, ErrTableIsRead)
	})
}

func TestMultiChunkerIsRead(t *testing.T) {
	mock1 := NewMockChunker("table1", 1000)
	mock2 := NewMockChunker("table2", 2000)
	chunker := NewMultiChunker(mock1, mock2).(*multiChunker)

	t.Run("NotReadWhenSomeActive", func(t *testing.T) {
		mock1.MarkAsComplete()
		mock2.SimulateProgress(0.5) // Still active

		require.False(t, chunker.IsRead())
	})

	t.Run("ReadWhenAllComplete", func(t *testing.T) {
		mock1.MarkAsComplete()
		mock2.MarkAsComplete()

		require.True(t, chunker.IsRead())
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

	require.Equal(t, uint64(1100), rowsCopied) // 500 + 600
	require.Equal(t, uint64(8), chunksCopied)  // 5 + 3
	require.Equal(t, uint64(3000), totalRows)  // 1000 + 2000
}

func TestMultiChunkerFeedbackRouting(t *testing.T) {
	mock1 := NewMockChunker("table1", 1000)
	mock2 := NewMockChunker("table2", 2000)
	chunker := NewMultiChunker(mock1, mock2).(*multiChunker)
	require.NoError(t, chunker.Open())
	defer func() {
		if err := chunker.Close(); err != nil {
			t.Logf("failed to close chunker: %v", err)
		}
	}()

	// Get chunks from different tables
	mock1.SimulateProgress(0.0)
	mock2.SimulateProgress(1.0) // Complete table2 so table1 is selected

	chunk1, err := chunker.Next()
	require.NoError(t, err)
	require.Equal(t, "table1", chunk1.Table.TableName)

	// Provide feedback
	duration := 100 * time.Millisecond
	actualRows := uint64(500)
	chunker.Feedback(chunk1, duration, actualRows)

	// Check that feedback was routed to the correct chunker
	feedback1 := mock1.GetFeedbackCalls()
	feedback2 := mock2.GetFeedbackCalls()

	require.Len(t, feedback1, 1)
	require.Empty(t, feedback2)

	require.Equal(t, chunk1, feedback1[0].Chunk)
	require.Equal(t, duration, feedback1[0].Duration)
	require.Equal(t, actualRows, feedback1[0].ActualRows)
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
		require.NoError(t, err)

		// Parse the watermark
		var watermarks map[string]string
		err = json.Unmarshal([]byte(watermark), &watermarks)
		require.NoError(t, err)

		require.Contains(t, watermarks, "test.table1")
		require.Contains(t, watermarks, "test.table2")

		// Check individual watermarks
		require.Equal(t, "300", watermarks["test.table1"])
		require.Equal(t, "1000", watermarks["test.table2"])
	})

	t.Run("OpenAtWatermark", func(t *testing.T) {
		// Create watermark
		watermarks := map[string]string{
			"test.table1": "300",
			"test.table2": "1000",
		}
		watermarkJSON, err := json.Marshal(watermarks)
		require.NoError(t, err)

		err = chunker.OpenAtWatermark(string(watermarkJSON))
		require.NoError(t, err)
		require.True(t, chunker.isOpen)

		// Verify chunkers were opened at correct positions
		progress1, _, _ := mock1.Progress()
		progress2, _, _ := mock2.Progress()
		require.Equal(t, uint64(300), progress1)
		require.Equal(t, uint64(1000), progress2)
	})

	t.Run("OpenAtWatermarkError", func(t *testing.T) {
		// Invalid JSON
		err := chunker.OpenAtWatermark("invalid json")
		require.Error(t, err)
		require.Contains(t, err.Error(), "could not parse multi-chunker watermark")
	})

	t.Run("OpenAtWatermarkMissingTable", func(t *testing.T) {
		// Create fresh mock chunkers for this test to avoid state from previous tests
		mock3 := NewMockChunker("table1", 1000)
		mock4 := NewMockChunker("table2", 2000)

		// Missing table in watermark - this should now succeed with our fix
		watermarks := map[string]string{
			"test.table1": "300",
			// "test.table2" missing - should start from scratch
		}
		watermarkJSON, err := json.Marshal(watermarks)
		require.NoError(t, err)

		chunker2 := NewMultiChunker(mock3, mock4).(*multiChunker)
		err = chunker2.OpenAtWatermark(string(watermarkJSON))
		require.NoError(t, err, "Should handle missing table watermarks gracefully")

		// Verify table1 was opened at watermark position
		progress1, _, _ := mock3.Progress()
		require.Equal(t, uint64(300), progress1, "table1 should resume from watermark position")

		// Verify table2 was opened from scratch (position 0)
		progress2, _, _ := mock4.Progress()
		require.Equal(t, uint64(0), progress2, "table2 should start from scratch when no watermark")
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

	require.Len(t, tableNames, 2) // Only unique table names
	require.Contains(t, tableNames, "table1")
	require.Contains(t, tableNames, "table2")
}

func TestMultiChunkerErrorHandling(t *testing.T) {
	t.Run("NextOnClosedChunker", func(t *testing.T) {
		mock1 := NewMockChunker("table1", 1000)
		mock2 := NewMockChunker("table2", 1000)
		chunker := NewMultiChunker(mock1, mock2).(*multiChunker)

		_, err := chunker.Next()
		require.ErrorIs(t, err, ErrTableNotOpen)
	})

	t.Run("NextError", func(t *testing.T) {
		mock1 := NewMockChunker("table1", 2000) // Make this chunker larger so it gets selected
		mock2 := NewMockChunker("table2", 1000)
		mock1.SetNextError(errors.New("next failed"))
		chunker := NewMultiChunker(mock1, mock2).(*multiChunker)
		require.NoError(t, chunker.Open())
		defer func() {
			if err := chunker.Close(); err != nil {
				t.Logf("failed to close chunker: %v", err)
			}
		}()

		_, err := chunker.Next()
		require.Error(t, err)
		require.Contains(t, err.Error(), "next failed")
	})

	t.Run("WatermarkError", func(t *testing.T) {
		mock1 := NewMockChunker("table1", 2000)
		mock2 := NewMockChunker("table2", 1000)
		mock1.SetWatermarkError(errors.New("watermark failed"))
		chunker := NewMultiChunker(mock1, mock2).(*multiChunker)

		// With our fix, this should succeed and skip the errored chunker
		watermark, err := chunker.GetLowWatermark()
		require.NoError(t, err, "Should skip chunker with watermark error")

		// Parse the watermark to verify only table2 is included
		var watermarks map[string]string
		err = json.Unmarshal([]byte(watermark), &watermarks)
		require.NoError(t, err)

		require.NotContains(t, watermarks, "test.table1", "Should skip table1 due to watermark error")
		require.Contains(t, watermarks, "test.table2", "Should include table2 watermark")
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
			defer func() {
				if err := chunker.Close(); err != nil {
					t.Logf("failed to close chunker: %v", err)
				}
			}()

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
			require.Equal(t, results[0], results[i], "Selection order should be deterministic")
		}
	})
}

func TestMultiChunkerReset(t *testing.T) {
	// Create mock chunkers with different sizes
	mock1 := NewMockChunker("table1", 1000)
	mock2 := NewMockChunker("table2", 2000)
	mock3 := NewMockChunker("table3", 500)

	chunker := NewMultiChunker(mock1, mock2, mock3).(*multiChunker)

	// Test that Reset() fails when chunker is not open
	err := chunker.Reset()
	require.Error(t, err)
	require.ErrorIs(t, err, ErrChunkerNotOpen)

	// Open the chunker
	require.NoError(t, chunker.Open())

	// Capture initial state after opening
	initialRowsCopied1, initialChunksCopied1, _ := mock1.Progress()
	initialRowsCopied2, initialChunksCopied2, _ := mock2.Progress()
	initialRowsCopied3, initialChunksCopied3, _ := mock3.Progress()

	// Process some chunks to change the state
	mock1.SimulateProgress(0.1) // 10%
	mock2.SimulateProgress(0.5) // 50%
	mock3.SimulateProgress(0.3) // 30%

	// Get chunks from different tables
	chunk1, err := chunker.Next() // Should select table1 (lowest progress)
	require.NoError(t, err)
	require.Equal(t, "table1", chunk1.Table.TableName)

	// Advance table1's progress so table3 becomes lowest
	mock1.SimulateProgress(0.4)
	chunk2, err := chunker.Next() // Should select table3 (now lowest progress)
	require.NoError(t, err)
	require.Equal(t, "table3", chunk2.Table.TableName)

	// Give feedback to change state
	chunker.Feedback(chunk1, time.Second, 100)
	chunker.Feedback(chunk2, time.Second, 50)

	// Verify state has changed
	currentRowsCopied1, currentChunksCopied1, _ := mock1.Progress()
	currentRowsCopied2, currentChunksCopied2, _ := mock2.Progress()
	currentRowsCopied3, currentChunksCopied3, _ := mock3.Progress()

	require.Greater(t, currentRowsCopied1, initialRowsCopied1)
	require.Greater(t, currentChunksCopied1, initialChunksCopied1)
	require.Greater(t, currentRowsCopied3, initialRowsCopied3)
	require.Greater(t, currentChunksCopied3, initialChunksCopied3)
	require.Greater(t, currentRowsCopied2, initialRowsCopied2)   // in the mock: rows it based on progress.
	require.Equal(t, initialChunksCopied2, currentChunksCopied2) // chunks is based on actual

	// Verify feedback was recorded
	feedback1 := mock1.GetFeedbackCalls()
	feedback3 := mock3.GetFeedbackCalls()
	require.Len(t, feedback1, 1)
	require.Len(t, feedback3, 1)

	// Verify watermark exists
	watermark, err := chunker.GetLowWatermark()
	require.NoError(t, err)
	require.NotEmpty(t, watermark)

	// Now reset the chunker
	err = chunker.Reset()
	require.NoError(t, err)

	// Verify all child chunkers are reset to initial values
	resetRowsCopied1, resetChunksCopied1, _ := mock1.Progress()
	resetRowsCopied2, resetChunksCopied2, _ := mock2.Progress()
	resetRowsCopied3, resetChunksCopied3, _ := mock3.Progress()

	require.Equal(t, initialRowsCopied1, resetRowsCopied1, "mock1 rowsCopied should be reset to initial value")
	require.Equal(t, initialChunksCopied1, resetChunksCopied1, "mock1 chunksCopied should be reset to initial value")
	require.Equal(t, initialRowsCopied2, resetRowsCopied2, "mock2 rowsCopied should be reset to initial value")
	require.Equal(t, initialChunksCopied2, resetChunksCopied2, "mock2 chunksCopied should be reset to initial value")
	require.Equal(t, initialRowsCopied3, resetRowsCopied3, "mock3 rowsCopied should be reset to initial value")
	require.Equal(t, initialChunksCopied3, resetChunksCopied3, "mock3 chunksCopied should be reset to initial value")

	// Verify every child was reset once
	require.Equal(t, 1, mock1.ResetCount(), "mock1 should be reset once")
	require.Equal(t, 1, mock2.ResetCount(), "mock2 should be reset once")
	require.Equal(t, 1, mock3.ResetCount(), "mock3 should be reset once")

	// Verify that after reset, the chunker produces the same selection behavior
	// Reset progress to same initial state
	mock1.SimulateProgress(0.1) // 10%
	mock2.SimulateProgress(0.5) // 50%
	mock3.SimulateProgress(0.3) // 30%

	resetChunk1, err := chunker.Next() // Should select table1 (lowest progress) again
	require.NoError(t, err)
	require.Equal(t, chunk1.Table.TableName, resetChunk1.Table.TableName, "First chunk after reset should be from same table as original")

	// Advance table1's progress so table3 becomes lowest (same as before)
	mock1.SimulateProgress(0.4)
	resetChunk2, err := chunker.Next() // Should select table3 again
	require.NoError(t, err)
	require.Equal(t, chunk2.Table.TableName, resetChunk2.Table.TableName, "Second chunk after reset should be from same table as original")

	// Test aggregate progress after reset
	totalRowsCopied, totalChunksCopied, totalRowsExpected := chunker.Progress()
	require.Equal(t, uint64(1900), totalRowsCopied)
	require.Equal(t, uint64(2), totalChunksCopied)    // 1 + 0 + 1 = 2 chunks processed
	require.Equal(t, uint64(3500), totalRowsExpected) // 1000 + 2000 + 500 = 3500

	// Test that reset works with child chunker errors
	mock1.SetNextError(errors.New("mock error"))

	// Reset should still work even if child chunkers have errors configured
	err = chunker.Reset()
	require.NoError(t, err)

	// Clear the error and verify normal operation resumes
	mock1.SetNextError(nil)
	mock1.SimulateProgress(0.0) // Make table1 lowest progress
	mock2.SimulateProgress(0.5)
	mock3.SimulateProgress(0.3)

	finalChunk, err := chunker.Next()
	require.NoError(t, err)
	require.Equal(t, "table1", finalChunk.Table.TableName)

	// Test reset with one child chunker reset error
	mock2.SetNextError(errors.New("reset would fail but we don't call Next"))

	// Mock chunker doesn't have a way to make Reset() fail, but let's test the error path
	// by creating a scenario where one child chunker is in a bad state
	mock2.MarkAsComplete() // This changes state

	err = chunker.Reset()
	require.NoError(t, err) // Should still succeed

	// Verify the completed chunker was reset (no longer complete)
	require.False(t, mock2.isRead(), "mock2 should not be read after reset")
	require.NoError(t, chunker.Close())
}

// TestMultiChunkerSelectionRegressions tests for specific bugs that were fixed
// in the multi-chunker selection logic to prevent regressions.
func TestMultiChunkerSelectionRegressions(t *testing.T) {
	t.Run("RegressionHighProgressNotStarved", func(t *testing.T) {
		// Regression test for: High progress tables getting starved by lower progress tables
		// This was the core issue causing migrations to stall at 95%+ completion

		mock1 := NewMockChunker("table1", 1000) // High progress table
		mock2 := NewMockChunker("table2", 2000) // Lower progress table
		chunker := NewMultiChunker(mock1, mock2)
		require.NoError(t, chunker.Open())
		defer func() {
			if err := chunker.Close(); err != nil {
				t.Logf("failed to close chunker: %v", err)
			}
		}()

		// Simulate the stalling scenario: table1 at 95.34%, table2 at 90%
		mock1.SimulateProgress(0.9534) // 95.34% - this was getting starved
		mock2.SimulateProgress(0.90)   // 90% - this was always being selected

		// The chunker should select table2 (90%) because it has lower progress
		// This is correct behavior - we want to prioritize the table that's furthest behind
		chunk, err := chunker.Next()
		require.NoError(t, err)
		require.Equal(t, "table2", chunk.Table.TableName, "Should select table with lowest progress (90%)")

		// Advance table2's progress past table1
		mock2.SimulateProgress(0.96) // Now table2 is at 96%

		// Now table1 (95.34%) should be selected as it has lower progress
		chunk, err = chunker.Next()
		require.NoError(t, err)
		require.Equal(t, "table1", chunk.Table.TableName, "Should now select table1 as it has lower progress")
	})

	t.Run("RegressionFirstChunkerBias", func(t *testing.T) {
		// Regression test for: First chunker always being selected due to minProgressPercent = 2.0 bug

		mock1 := NewMockChunker("table1", 1000) // This would always be selected due to bug
		mock2 := NewMockChunker("table2", 2000) // This would never be selected
		mock3 := NewMockChunker("table3", 500)  // This would never be selected
		chunker := NewMultiChunker(mock1, mock2, mock3)
		require.NoError(t, chunker.Open())
		defer func() {
			if err := chunker.Close(); err != nil {
				t.Logf("failed to close chunker: %v", err)
			}
		}()

		// Set table1 to high progress, others to low progress
		mock1.SimulateProgress(0.95) // 95% - should NOT be selected
		mock2.SimulateProgress(0.10) // 10% - should be selected (lowest)
		mock3.SimulateProgress(0.20) // 20%

		chunk, err := chunker.Next()
		require.NoError(t, err)
		require.Equal(t, "table2", chunk.Table.TableName, "Should select table with lowest progress, not first table")

		// Advance table2 past table3
		mock2.SimulateProgress(0.25) // Now 25%

		// table3 should now be selected (20% < 25%)
		chunk, err = chunker.Next()
		require.NoError(t, err)
		require.Equal(t, "table3", chunk.Table.TableName, "Should select table3 as it now has lowest progress")
	})

	t.Run("RegressionNilSelectedChunker", func(t *testing.T) {
		// Regression test for: selectedChunker remaining nil when first chunkers are IsRead() == true

		mock1 := NewMockChunker("table1", 1000)
		mock2 := NewMockChunker("table2", 2000)
		mock3 := NewMockChunker("table3", 500)
		chunker := NewMultiChunker(mock1, mock2, mock3)
		require.NoError(t, chunker.Open())
		defer func() {
			if err := chunker.Close(); err != nil {
				t.Logf("failed to close chunker: %v", err)
			}
		}()

		// Mark first two chunkers as complete
		mock1.MarkAsComplete()
		mock2.MarkAsComplete()
		mock3.SimulateProgress(0.50) // Only this one is active

		// Should successfully select the only active chunker
		chunk, err := chunker.Next()
		require.NoError(t, err)
		require.Equal(t, "table3", chunk.Table.TableName, "Should select the only active chunker")
		require.NotNil(t, chunk, "Chunk should not be nil")
	})

	t.Run("RegressionAllChunkersComplete", func(t *testing.T) {
		// Regression test for: Proper handling when all chunkers become complete

		mock1 := NewMockChunker("table1", 1000)
		mock2 := NewMockChunker("table2", 2000)
		chunker := NewMultiChunker(mock1, mock2)
		require.NoError(t, chunker.Open())
		defer func() {
			if err := chunker.Close(); err != nil {
				t.Logf("failed to close chunker: %v", err)
			}
		}()

		// Mark all chunkers as complete
		mock1.MarkAsComplete()
		mock2.MarkAsComplete()

		// Should return ErrTableIsRead when all chunkers are complete
		_, err := chunker.Next()
		require.ErrorIs(t, err, ErrTableIsRead, "Should return ErrTableIsRead when all chunkers are complete")
	})

	t.Run("RegressionEqualProgressLargestTable", func(t *testing.T) {
		// Regression test for: When progress is equal, select table with most total rows

		mock1 := NewMockChunker("table1", 500)  // Smaller table
		mock2 := NewMockChunker("table2", 2000) // Larger table - should be selected
		mock3 := NewMockChunker("table3", 1000) // Medium table
		chunker := NewMultiChunker(mock1, mock2, mock3)
		require.NoError(t, chunker.Open())
		defer func() {
			if err := chunker.Close(); err != nil {
				t.Logf("failed to close chunker: %v", err)
			}
		}()

		// Set all to equal progress (0%)
		mock1.SimulateProgress(0.0)
		mock2.SimulateProgress(0.0)
		mock3.SimulateProgress(0.0)

		chunk, err := chunker.Next()
		require.NoError(t, err)
		require.Equal(t, "table2", chunk.Table.TableName, "Should select largest table when progress is equal")
	})

	t.Run("RegressionWatermarkErrorHandling", func(t *testing.T) {
		// Regression test for: Watermark errors should not block checkpoint writing for other tables

		mock1 := NewMockChunker("table1", 1000)
		mock2 := NewMockChunker("table2", 2000)
		mock3 := NewMockChunker("table3", 500)
		chunker := NewMultiChunker(mock1, mock2, mock3)

		// Simulate table2 having a watermark error (not ready)
		mock2.SetWatermarkError(errors.New("watermark not ready"))

		// Simulate progress on all tables
		mock1.SimulateProgress(0.5)
		mock2.SimulateProgress(0.7) // This one has watermark error
		mock3.SimulateProgress(0.3)

		// GetLowWatermark should succeed and include table1 and table3, skip table2
		watermark, err := chunker.GetLowWatermark()
		require.NoError(t, err, "Should not fail when one table has watermark error")

		// Parse the watermark to verify contents
		var watermarks map[string]string
		err = json.Unmarshal([]byte(watermark), &watermarks)
		require.NoError(t, err)

		// Should contain table1 and table3, but not table2 (error)
		require.Contains(t, watermarks, "test.table1", "Should include table1 watermark")
		require.Contains(t, watermarks, "test.table3", "Should include table3 watermark")
		require.NotContains(t, watermarks, "test.table2", "Should skip table2 due to watermark error")
	})

	t.Run("RegressionOpenAtWatermarkMissingTable", func(t *testing.T) {
		// Regression test for: OpenAtWatermark should handle missing table watermarks gracefully

		mock1 := NewMockChunker("table1", 1000)
		mock2 := NewMockChunker("table2", 2000)
		chunker := NewMultiChunker(mock1, mock2).(*multiChunker)

		// Create watermark with only table1 (table2 missing - simulates watermark not ready scenario)
		watermarks := map[string]string{
			"test.table1": "500", // "test.table2" intentionally missing
		}
		watermarkJSON, err := json.Marshal(watermarks)
		require.NoError(t, err)

		// Should succeed - table1 resumes from watermark, table2 starts from scratch
		err = chunker.OpenAtWatermark(string(watermarkJSON))
		require.NoError(t, err, "Should handle missing table watermarks gracefully")

		// Verify table1 was opened at watermark position
		progress1, _, _ := mock1.Progress()
		require.Equal(t, uint64(500), progress1, "table1 should resume from watermark position")

		// Verify table2 was opened from scratch (position 0)
		progress2, _, _ := mock2.Progress()
		require.Equal(t, uint64(0), progress2, "table2 should start from scratch when no watermark")
	})
}
