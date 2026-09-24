package table

import (
	"database/sql"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// TestUniqueSecondaryIndexes pins the set the change feed's flush partitioning
// treats as its conflict surface. Getting the *membership* right matters more
// than the mechanics: a non-unique index wrongly included would send the drain
// sorting by a key that cannot collide, and PRIMARY wrongly included would send
// it sorting by one whose neighbours provably do not collide either.
func TestUniqueSecondaryIndexes(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS unique_idx_probe`)
	testutils.RunSQL(t, `CREATE TABLE unique_idx_probe (
		id bigint NOT NULL AUTO_INCREMENT,
		token varchar(255) NOT NULL,
		key_a varchar(255) NOT NULL,
		key_b varchar(255) NOT NULL,
		key_c varchar(50) NOT NULL,
		created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		payload bigint NOT NULL DEFAULT 0,
		PRIMARY KEY (id),
		UNIQUE KEY unq_token (token),
		UNIQUE KEY unq_composite (key_a, key_b, key_c),
		KEY idx_created_at (created_at),
		KEY idx_payload (payload)
	)`)

	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ti := NewTableInfo(db, "test", "unique_idx_probe")
	require.NoError(t, ti.SetInfo(t.Context()))

	got, err := ti.UniqueSecondaryIndexes(t.Context())
	require.NoError(t, err)
	require.Equal(t, []UniqueIndex{
		{Name: "unq_composite", Columns: []string{"key_a", "key_b", "key_c"}},
		{Name: "unq_token", Columns: []string{"token"}},
	}, got, "only UNIQUE secondary indexes, composite columns in key order")

	// The non-unique indexes and PRIMARY are all absent, which is the whole
	// point — assert it by name so a query change that widened the filter is
	// unmistakable rather than showing up as a length mismatch.
	for _, idx := range got {
		require.NotContains(t, []string{"PRIMARY", "idx_created_at", "idx_payload"}, idx.Name)
	}
}

// TestUniqueSecondaryIndexesSkipsFunctionalIndexes covers the index a caller
// holding a row image cannot use: a functional index's key is an expression, so
// information_schema reports a NULL COLUMN_NAME and there is no stored column to
// read a value from. Returning it with its columns silently dropped would be the
// bad outcome — a caller would sort by a partial key believing it had the whole
// one.
func TestUniqueSecondaryIndexesSkipsFunctionalIndexes(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS unique_idx_functional`)
	testutils.RunSQL(t, `CREATE TABLE unique_idx_functional (
		id bigint NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		other varchar(255) NOT NULL,
		PRIMARY KEY (id),
		UNIQUE KEY unq_lower ((lower(name))),
		UNIQUE KEY unq_mixed (other, (lower(name))),
		UNIQUE KEY unq_plain (other)
	)`)

	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ti := NewTableInfo(db, "test", "unique_idx_functional")
	require.NoError(t, ti.SetInfo(t.Context()))

	got, err := ti.UniqueSecondaryIndexes(t.Context())
	require.NoError(t, err)
	// unq_mixed is the case that needs the end-of-loop filter rather than a
	// `continue`: its stored column is collected *before* the functional one is
	// seen, so it has to be removed after the fact.
	require.Equal(t, []UniqueIndex{
		{Name: "unq_plain", Columns: []string{"other"}},
	}, got)
}

// TestUniqueSecondaryIndexesWithoutDB pins that a metadata-only TableInfo — the
// shape NewTableInfoFromMeta returns, used by callers holding DDL and no server
// — gets an error rather than a nil-pointer panic. The change feed calls this on
// a table it did not construct, so the nil case is reachable from outside this
// package.
func TestUniqueSecondaryIndexesWithoutDB(t *testing.T) {
	ti, err := NewTableInfoFromMeta("test", "meta_only",
		[]ColumnMeta{{Name: "id", MySQLType: "bigint"}}, []string{"id"})
	require.NoError(t, err)

	got, err := ti.UniqueSecondaryIndexes(t.Context())
	require.Nil(t, got)
	require.ErrorContains(t, err, "no database handle")
}
