package table

import (
	"database/sql"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

func TestCompositeChunker(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS composite`)
	table := `CREATE TABLE composite (
		id bigint NOT NULL AUTO_INCREMENT,
		age int(11) NOT NULL,
		PRIMARY KEY (id, age)
	)`
	testutils.RunSQL(t, table)

	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	}()

	t1 := NewTableInfo(db, "test", "composite")
	require.NoError(t, t1.SetInfo(t.Context()))

	chunker, err := NewChunker(t1, ChunkerConfig{})
	require.NoError(t, err)
	require.IsType(t, &chunkerComposite{}, chunker)
}

func TestOptimisticChunker(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS optimistic`)
	table := `CREATE TABLE optimistic (
		id bigint NOT NULL AUTO_INCREMENT,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)

	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	}()

	t1 := NewTableInfo(db, "test", "optimistic")
	require.NoError(t, t1.SetInfo(t.Context()))

	chunker, err := NewChunker(t1, ChunkerConfig{})
	require.NoError(t, err)
	require.IsType(t, &chunkerOptimistic{}, chunker)
}

func TestNewCompositeChunkerWithKeyAndWhere(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS composite`)
	table := `CREATE TABLE composite (
		id bigint NOT NULL AUTO_INCREMENT,
		age int(11) NOT NULL,
		PRIMARY KEY (id),
        KEY age_idx (age)
	)`
	testutils.RunSQL(t, table)

	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	}()

	t1 := NewTableInfo(db, "test", "composite")
	require.NoError(t, t1.SetInfo(t.Context()))

	// When Key and Where are specified, NewChunker should always return a
	// composite chunker even though this table has a single-column auto-inc PK.
	chunker, err := NewChunker(t1, ChunkerConfig{
		Key:   "age_idx",
		Where: "age > 50",
	})
	require.NoError(t, err)
	require.IsType(t, &chunkerComposite{}, chunker)
	require.Equal(t, "age_idx", chunker.(*chunkerComposite).keyName)
	require.Equal(t, "age > 50", chunker.(*chunkerComposite).where)
}
