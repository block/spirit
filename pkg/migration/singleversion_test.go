//go:build singleversion

// This file (together with resume_test.go) makes up the
// "single-version" test suite: tests that exercise Spirit's own logic and do
// not depend on the MySQL server version, so there is no value in re-running
// them against every MySQL version in the CI matrix. They are gated behind the
// `singleversion` build tag and run in one dedicated CI workflow against MySQL
// 8.0.45, twice — once with GTIDs enabled and once without, because the
// change-source coordinate scheme is auto-detected from the server
// (change.NewAutoClient) and the resume/checkpoint tests should cover both
// formats — see .github/workflows/mysql8.0.45-singleversion-docker.yml and the
// `singleversion-test` service in compose/compose.yml. Every other version job
// runs without the tag and therefore excludes these files.
//
// The dedicated job selects the suite with
// `-run 'Resume|Checkpoint|UniqueOnNonUniqueData|ChunkerPrefetching|Unparsable'`,
// so a new single-version test must either match that pattern by name or be
// added to it.
//
// To run the suite locally:
//
//	go test -tags singleversion -run 'Resume|Checkpoint|UniqueOnNonUniqueData|ChunkerPrefetching|Unparsable' ./pkg/migration/...
//
// A plain `go test ./...` (no tag) skips these files.
package migration

import (
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// TestUniqueOnNonUniqueData tests that we:
// 1. Fail trying to add a unique index on non-unique data.
// 2. The error does not blame spirit, but is instead suggestive of user-data error.
func TestUniqueOnNonUniqueData(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "uniquet1", `CREATE TABLE uniquet1 (id int not null primary key auto_increment, b int not null, pad1 varbinary(1024))`)
	tt.SeedRows(t, "INSERT INTO uniquet1 (b, pad1) SELECT 1, RANDOM_BYTES(1024)", 100000)
	testutils.RunSQL(t, `UPDATE uniquet1 SET b = id`)
	testutils.RunSQL(t, `UPDATE uniquet1 SET b = 12345 ORDER BY RAND() LIMIT 2`)

	m := NewTestMigration(t, WithStatement("ALTER TABLE uniquet1 ADD UNIQUE (b)"))
	err := m.Run()
	require.Error(t, err)
	require.ErrorContains(t, err, "checksum failed after several attempts. This is likely related to your statement adding a UNIQUE index on non-unique data")
}

// TestUnparsableStatements tests that the behavior is expected in cases
// where we know the parser does not support the statement. We document
// that we require the parser to parse the statement for it to execute,
// which feels like a reasonable limitation based on its capabilities.
// The parser fork in pkg/parser removes limitations the TiDB parser had
// (e.g. https://github.com/pingcap/tidb/issues/57768, fixed here as
// https://github.com/block/spirit/issues/542), so this also covers cases
// that used to be unparsable and now work.
func TestUnparsableStatements(t *testing.T) {
	t.Parallel()
	// CREATE TABLE with an expression default on a BLOB column.
	m := NewTestMigration(t, WithStatement(`CREATE TABLE t1parse (id int not null primary key auto_increment, b BLOB DEFAULT ('abc'))`))
	require.NoError(t, m.Run())

	// ALTER TABLE with BLOB DEFAULT via --statement. This used to fail with
	// Error 1101 because the TiDB parser restored DEFAULT ('abc') as the
	// bare literal DEFAULT 'abc' (issue #542); the parser fork preserves
	// the parentheses, so it now works.
	m = NewTestMigration(t, WithStatement("ALTER TABLE t1parse ADD COLUMN c BLOB DEFAULT ('abc')"))
	require.NoError(t, m.Run())

	// The same thing via the legacy --table/--alter path (this path always
	// worked: the alter clause is executed verbatim, without a parser round
	// trip). Adds c2 since the --statement run above already added c.
	// Dies with the legacy path.
	m = NewTestMigration(t)
	m.Table = "t1parse"
	m.Alter = "ADD COLUMN c2 BLOB DEFAULT ('abc')"
	require.NoError(t, m.Run())

	// CREATE TRIGGER — not supported.
	m = NewTestMigration(t, WithStatement("CREATE TRIGGER ins_sum BEFORE INSERT ON t1parse FOR EACH ROW SET @sum = @sum + NEW.b;"))
	err := m.Run()
	require.Error(t, err)
	require.ErrorContains(t, err, "line 1 column 14 near \"TRIGGER")

	// https://github.com/pingcap/tidb/pull/61498
	// Legacy --table/--alter path for the same reason as above.
	m = NewTestMigration(t)
	m.Table = "t1parse"
	m.Alter = `ADD COLUMN src_col timestamp NULL DEFAULT NULL, add column new_col timestamp NULL DEFAULT(src_col)`
	require.NoError(t, m.Run())

	// Cleanup
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1parse`)
}

// TestChunkerPrefetching tests that the chunker handles large ID gaps correctly.
func TestChunkerPrefetching(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "prefetchtest", `CREATE TABLE prefetchtest (
		id BIGINT NOT NULL AUTO_INCREMENT,
		created_at DATETIME(3) NULL,
		PRIMARY KEY (id)
	)`)
	// Insert about 11K rows, then add large ID gaps to test prefetching.
	testutils.RunSQL(t, `INSERT INTO prefetchtest (created_at) VALUES (NULL)`)
	testutils.RunSQL(t, `INSERT INTO prefetchtest (created_at) SELECT NULL FROM prefetchtest a JOIN prefetchtest b JOIN prefetchtest c`)
	testutils.RunSQL(t, `INSERT INTO prefetchtest (created_at) SELECT NULL FROM prefetchtest a JOIN prefetchtest b JOIN prefetchtest c`)
	testutils.RunSQL(t, `INSERT INTO prefetchtest (created_at) SELECT NULL FROM prefetchtest a JOIN prefetchtest b JOIN prefetchtest c`)
	testutils.RunSQL(t, `INSERT INTO prefetchtest (created_at) SELECT NULL FROM prefetchtest a JOIN prefetchtest b LIMIT 10000`)

	// Insert far-off IDs to create large gaps that test the prefetcher.
	testutils.RunSQL(t, `INSERT INTO prefetchtest (id, created_at) VALUES (300000000000, NULL)`)
	testutils.RunSQL(t, `INSERT INTO prefetchtest (created_at) SELECT NULL FROM prefetchtest a JOIN prefetchtest b LIMIT 300000`)
	testutils.RunSQL(t, `INSERT INTO prefetchtest (id, created_at) VALUES (600000000000, NULL)`)
	testutils.RunSQL(t, `INSERT INTO prefetchtest (created_at) SELECT NULL FROM prefetchtest a JOIN prefetchtest b LIMIT 300000`)
	testutils.RunSQL(t, `INSERT INTO prefetchtest (id, created_at) VALUES (900000000000, NULL)`)

	m := NewTestRunner(t, "prefetchtest", "engine=innodb")
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())
}
