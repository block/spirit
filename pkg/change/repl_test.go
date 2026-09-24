package change

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// Concurrent allocation must be unique, not merely unlikely to collide.
func TestNewServerIDConcurrent(t *testing.T) {
	const workers, perWorker = 100, 100
	ids := make(chan uint32, workers*perWorker)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for range perWorker {
				ids <- NewServerID()
			}
		})
	}
	wg.Wait()
	close(ids)
	seen := make(map[uint32]struct{}, workers*perWorker)
	for id := range ids {
		require.GreaterOrEqual(t, id, uint32(1001))
		_, duplicate := seen[id]
		require.False(t, duplicate, "duplicate server ID: %d", id)
		seen[id] = struct{}{}
	}
	require.Len(t, seen, workers*perWorker)
	// A single sequence produces one contiguous block in the circular ID
	// range. Count its missing successors rather than comparing min/max,
	// which would flake when the random starting point crosses the wrap.
	boundaries := 0
	for id := range seen {
		successor := id + 1
		if id == ^uint32(0) {
			successor = 1001
		}
		if _, ok := seen[successor]; !ok {
			boundaries++
		}
	}
	require.Equal(t, 1, boundaries, "NewServerID must use one process-wide sequence")
}

func TestServerIDSequenceWrap(t *testing.T) {
	sequence := newServerIDSequence(uint32(serverIDRange - 3))
	require.Equal(t, ^uint32(0)-1, sequence.next())
	require.Equal(t, ^uint32(0), sequence.next())
	require.Equal(t, uint32(1001), sequence.next())
	require.Equal(t, uint32(1002), sequence.next())
}

// TestNewServerIDRange tests that NewServerID always returns values in the expected range.
func TestNewServerIDRange(t *testing.T) {
	for range 1000 {
		id := NewServerID()
		require.GreaterOrEqual(t, id, uint32(1001), "ServerID should be >= 1001")
	}
}
