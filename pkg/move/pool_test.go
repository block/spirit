package move

import (
	"database/sql"
	"reflect"
	"testing"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/flags"
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
		require.Error(t, (&Move{Common: flags.Common{Threads: 2, MaxConnections: budget}}).Validate())
	}
	require.NoError(t, (&Move{Common: flags.Common{Threads: 2, MaxConnections: 8, WriteThreads: 100}}).Validate())
	r.move.MaxConnections = 8
	r.move.Threads = 16
	r.move.WriteThreads = 100
	require.NoError(t, r.fitReadThreadsToPools())
	require.Equal(t, 2, r.move.Threads)
	require.Equal(t, 100, r.move.WriteThreads)
	require.Equal(t, 8, r.move.MaxConnections)
}

// The checker reads each distinct target handle once per chunk, so targets
// reusing one handle cost one connection per worker. A handle that is both a
// source and a target is read on both sides at once, so it costs two.
func TestMoveSharedHandleBudget(t *testing.T) {
	r, err := NewRunner(&Move{Common: flags.Common{Threads: 4, MaxConnections: 16}})
	require.NoError(t, err)
	db := new(sql.DB) // Identity only; this test performs no database operations.
	r.targets = []applier.Target{{DB: db}, {DB: db}, {DB: db}}
	// 16 less the reserve of 6 leaves 10, and each worker holds one.
	require.NoError(t, r.fitReadThreadsToPools())
	require.Equal(t, 4, r.move.Threads, "a target handle reused three times counts once")

	r.move.Threads = 8
	r.sources = []sourceInfo{{db: db}}
	// Each worker now holds two connections on db: 10/2 leaves room for five.
	require.NoError(t, r.fitReadThreadsToPools())
	require.Equal(t, 5, r.move.Threads, "a handle shared by a source and a target counts twice")
}

func TestMoveTableStatisticsReserve(t *testing.T) {
	r, err := NewRunner(&Move{Common: flags.Common{Threads: 4, MaxConnections: 12}})
	require.NoError(t, err) // Parse-time headroom fits, before table discovery.
	r.sourceTables = make([]*table.TableInfo, 20)
	require.NoError(t, r.fitReadThreadsToPools())
	require.Equal(t, 1, r.move.Threads)
}
