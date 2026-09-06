package move

import (
	"database/sql"
	"reflect"
	"testing"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/require"
)

func TestMoveConnectionBudget(t *testing.T) {
	r, err := NewRunner(&Move{})
	require.NoError(t, err)
	require.Equal(t, dbconn.DefaultMaxConnections, r.move.MaxConnections)
	field, ok := reflect.TypeFor[Move]().FieldByName("MaxConnections")
	require.True(t, ok)
	require.Equal(t, "128", field.Tag.Get("default"))
	for _, budget := range []int{-1, 4, 7} {
		require.Error(t, (&Move{Threads: 2, MaxConnections: budget}).Validate())
	}
	require.NoError(t, (&Move{Threads: 2, MaxConnections: 8, WriteThreads: 100}).Validate())
	r.move.MaxConnections = 8
	r.move.Threads = 16
	r.move.WriteThreads = 100
	require.NoError(t, r.fitReadThreadsToPools())
	require.Equal(t, 2, r.move.Threads)
	require.Equal(t, 100, r.move.WriteThreads)
	require.Equal(t, 8, r.move.MaxConnections)
}

func TestMoveSharedSnapshotPoolBudget(t *testing.T) {
	r, err := NewRunner(&Move{Threads: 2, MaxConnections: 8})
	require.NoError(t, err)
	db := new(sql.DB) // Identity only; this test performs no database operations.
	r.targets = []applier.Target{{DB: db}, {DB: db}}
	require.NoError(t, r.fitReadThreadsToPools())
	require.Equal(t, 1, r.move.Threads)
	r.targets = append(r.targets, applier.Target{DB: db}, applier.Target{DB: db})
	require.ErrorContains(t, r.fitReadThreadsToPools(), "checksum snapshot pools")
}

func TestMoveSharedSnapshotLockReserve(t *testing.T) {
	r, err := NewRunner(&Move{Threads: 2, MaxConnections: 16})
	require.NoError(t, err)
	db := new(sql.DB)
	for range 5 {
		r.targets = append(r.targets, applier.Target{DB: db})
	}
	// Five locks plus two spare connections reserve seven slots, leaving
	// nine for five snapshot pools: one reader each, not two.
	require.NoError(t, r.fitReadThreadsToPools())
	require.Equal(t, 1, r.move.Threads)
}

func TestMoveTableStatisticsReserve(t *testing.T) {
	r, err := NewRunner(&Move{Threads: 4, MaxConnections: 12})
	require.NoError(t, err) // Parse-time headroom fits, before table discovery.
	r.sourceTables = make([]*table.TableInfo, 20)
	require.NoError(t, r.fitReadThreadsToPools())
	require.Equal(t, 1, r.move.Threads)
}
