package change

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	mysql2 "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// This file holds the datatype-replication suite for the buffered
// subscription path: binlog row event -> bufferedMap -> applier
// REPLACE INTO. That path re-serialises every column value through
// table.Datum, so a type-specific encoding bug shows up here as a
// destination row that no longer matches the source.
//
// The motivating regression is BIT(N): a BIT value read from the binlog
// arrives as an int64 (go-mysql's decodeBit). Before commit fd73e807 the
// applier emitted it as a *quoted* SQL string literal, which MySQL
// interprets as a byte-for-byte bit pattern rather than a numeric
// coercion — producing either the wrong value or, when the textual form
// is wider than the column, the production error:
//
//	failed to upsert rows: failed to execute upsert: unsafe warning:
//	Out of range value for column '...' at row 1
//
// TestBufferedDatatypeReplication guards that path for every common
// column type; TestBufferedMapGeometry and TestBufferedMapJSONNumberRoundTrip
// cover the two types whose equality cannot be expressed with a plain
// NULL-safe comparison and so need bespoke assertions.

// datatypeCase describes one column type and a set of SQL value
// expressions to push through the replication path. Each expression
// becomes its own row, so a case must supply at least two values for the
// DELETE phase to remain meaningful.
type datatypeCase struct {
	name   string
	colDef string   // definition for the `val` column, e.g. "BIT(1) NOT NULL DEFAULT b'0'"
	values []string // SQL value expressions, one row each, e.g. "b'0'", "b'1'", "NULL"
}

// requiresVector reports whether the case needs the VECTOR type (MySQL 9.7+),
// so it can be skipped on the older servers in the CI matrix. Derived from the
// column definition rather than a flag, so any vector case added later is
// gated automatically.
func (c datatypeCase) requiresVector() bool {
	return strings.Contains(strings.ToUpper(c.colDef), "VECTOR")
}

func datatypeCases() []datatypeCase {
	return []datatypeCase{
		// ---- signed integers ----
		{"tinyint_signed", "TINYINT NOT NULL", []string{"-128", "0", "127"}},
		{"smallint_signed", "SMALLINT NOT NULL", []string{"-32768", "0", "32767"}},
		{"mediumint_signed", "MEDIUMINT NOT NULL", []string{"-8388608", "0", "8388607"}},
		{"int_signed", "INT NOT NULL", []string{"-2147483648", "0", "2147483647"}},
		{"bigint_signed", "BIGINT NOT NULL", []string{"-9223372036854775808", "0", "9223372036854775807"}},

		// ---- unsigned integers (the top of each range exercises the
		// signed->unsigned bit-pattern reinterpret in NewDatum) ----
		{"tinyint_unsigned", "TINYINT UNSIGNED NOT NULL", []string{"0", "255"}},
		{"smallint_unsigned", "SMALLINT UNSIGNED NOT NULL", []string{"0", "65535"}},
		{"mediumint_unsigned", "MEDIUMINT UNSIGNED NOT NULL", []string{"0", "16777215"}},
		{"int_unsigned", "INT UNSIGNED NOT NULL", []string{"0", "4294967295"}},
		{"bigint_unsigned", "BIGINT UNSIGNED NOT NULL", []string{"0", "1", "18446744073709551615"}},

		// ---- BIT: the reported production failure ----
		// summary_backfill bit(1) NOT NULL DEFAULT b'0' is exactly the
		// column from the bug report.
		{"bit1_production", "BIT(1) NOT NULL DEFAULT b'0'", []string{"b'0'", "b'1'"}},
		{"bit8", "BIT(8) NOT NULL", []string{"b'0'", "b'1'", "127", "200", "b'11111111'"}},
		{"bit16", "BIT(16) NOT NULL", []string{"0", "1", "b'1010101010101010'", "65535"}},
		// BIT(64) with the high bit set decodes to a negative int64, the
		// case that broke the old quoted-string serialisation hardest.
		{"bit64", "BIT(64) NOT NULL", []string{"0", "1", "9223372036854775808", "18446744073709551615"}},

		// ---- boolean (a TINYINT(1) alias) ----
		{"bool", "BOOL NOT NULL", []string{"0", "1", "true", "false"}},

		// ---- fixed / floating point ----
		{"decimal_small", "DECIMAL(10,2) NOT NULL", []string{"0.00", "-12345678.90", "99999999.99"}},
		{"decimal_big", "DECIMAL(30,10) NOT NULL", []string{"0.0000000000", "-12345678901234567890.1234567890", "99999999999999999999.9999999999"}},
		{"float", "FLOAT NOT NULL", []string{"-3.5", "0", "3.25", "3.5"}},
		{"double", "DOUBLE NOT NULL", []string{"-2.5", "0", "1234.5", "3.141592653589793"}},

		// ---- character strings (escaping + unicode + empty) ----
		{"char", "CHAR(16) NOT NULL", []string{"'abc'", "''", "'1234567890123456'"}},
		{"varchar", "VARCHAR(255) NOT NULL", []string{"'hello'", "'quote'' and \\\\ backslash'", "'unicode: café 😀'", "''"}},
		{"text", "TEXT NOT NULL", []string{"'a longer text value'", "'with \"double\" quotes'", "''"}},

		// ---- binary strings (high bytes + interior/trailing zeros) ----
		{"binary", "BINARY(8) NOT NULL", []string{"X'0001020304ff0000'", "X'0000000000000000'", "X'ffffffffffffffff'"}},
		{"varbinary", "VARBINARY(16) NOT NULL", []string{"X'deadbeef'", "X'00'", "X'aabbccdd00000000'", "X'ff'"}},
		{"blob", "BLOB NOT NULL", []string{"X'00112233ff'", "X'ff00'", "X'00'"}},

		// ---- vector (MySQL 9.7+) ----
		// A VECTOR is a packed array of 4-byte little-endian floats that
		// the binlog delivers as raw bytes. The all-zeros vector is the
		// case that matters: its byte image is valid UTF-8, so the
		// applier's "hex-encode anything that isn't valid UTF-8"
		// heuristic would emit it as a character-set string literal —
		// which the server rejects outright with
		// ER_INVALID_CAST_TO_VECTOR ("Value of type 'string, size: 12'
		// cannot be converted to 'vector' type"), aborting the flush.
		// The value expressions below span the same hazard for other
		// widths: exponents/mantissas whose bytes land in the ASCII
		// range are equally "valid UTF-8" by accident.
		{"vector", "VECTOR(3) NOT NULL", []string{"STRING_TO_VECTOR('[0,0,0]')", "STRING_TO_VECTOR('[1.5,-2,0.0003]')", "STRING_TO_VECTOR('[1e30,-1e-5,123456789.5]')"}},
		{"vector_nullable", "VECTOR(4) NULL", []string{"NULL", "STRING_TO_VECTOR('[0,0,0,0]')", "STRING_TO_VECTOR('[1,2,3,4]')"}},
		// The default (unspecified) dimension is stored as vector(2048), but
		// the stored image is only as wide as the value written into it.
		{"vector_default_dim", "VECTOR NULL", []string{"STRING_TO_VECTOR('[0]')", "STRING_TO_VECTOR('[0,0]')", "NULL"}},

		// ---- temporal ----
		{"date", "DATE NOT NULL", []string{"'2026-06-10'", "'1000-01-01'", "'9999-12-31'"}},
		{"datetime", "DATETIME NOT NULL", []string{"'2026-06-10 07:40:12'", "'1000-01-01 00:00:00'", "'9999-12-31 23:59:59'"}},
		{"datetime6", "DATETIME(6) NOT NULL", []string{"'2026-06-10 07:40:12.123456'", "'2000-01-01 00:00:00.000001'"}},
		{"timestamp", "TIMESTAMP NOT NULL", []string{"'2026-06-10 07:40:12'", "'2000-01-01 12:00:00'"}},
		{"timestamp6", "TIMESTAMP(6) NOT NULL", []string{"'2026-06-10 07:40:12.654321'", "'2000-01-01 12:00:00.000010'"}},
		{"time", "TIME NOT NULL", []string{"'12:34:56'", "'-12:34:56'", "'838:59:59'", "'00:00:00'"}},
		{"time6", "TIME(6) NOT NULL", []string{"'12:34:56.789012'", "'-12:34:56.789012'"}},
		{"year", "YEAR NOT NULL", []string{"2026", "1901", "2155"}},

		// ---- enum / set (decoded from ordinal/bitmask by DecodeBinlogRow) ----
		{"enum", "ENUM('a','b','c') NOT NULL", []string{"'a'", "'b'", "'c'"}},
		{"set", "SET('x','y','z') NOT NULL", []string{"'x'", "'x,y'", "'x,y,z'", "''"}},

		// ---- nullable variants: NULL must round-trip alongside a value ----
		{"nullable_int", "INT NULL", []string{"NULL", "42", "-7"}},
		{"nullable_varchar", "VARCHAR(50) NULL", []string{"NULL", "'present'", "''"}},
		{"nullable_bit", "BIT(8) NULL", []string{"NULL", "b'10101010'", "0"}},
		{"nullable_blob", "BLOB NULL", []string{"NULL", "X'00ff'"}},
		{"nullable_datetime", "DATETIME(6) NULL", []string{"NULL", "'2026-06-10 07:40:12.123456'"}},
	}
}

// TestBufferedDatatypeReplication pushes representative values for every
// common MySQL column type through the buffered replication path and
// asserts the destination matches the source after INSERT, after an
// UPDATE that carries a full row image, and after a DELETE.
//
// The UPDATE phase bumps an unrelated `touched` column rather than `val`
// itself: with binlog_row_image=FULL that still emits a full AFTER image
// containing `val`, so the applier re-serialises the value — the exact
// shape of the production bug, where a write to a neighbouring column
// dragged a BIT value through the applier and failed the upsert.
func TestBufferedDatatypeReplication(t *testing.T) {
	for _, tc := range datatypeCases() {
		t.Run(tc.name, func(t *testing.T) {
			if tc.requiresVector() {
				testutils.SkipUnlessVectorSupported(t)
			}
			require.GreaterOrEqual(t, len(tc.values), 2,
				"each case needs >=2 values so the DELETE phase is non-vacuous")

			srcDDL := fmt.Sprintf(`CREATE TABLE subscription_test (
				id INT NOT NULL AUTO_INCREMENT,
				touched INT NOT NULL DEFAULT 0,
				val %s,
				PRIMARY KEY (id)
			)`, tc.colDef)
			dstDDL := fmt.Sprintf(`CREATE TABLE _subscription_test_new (
				id INT NOT NULL AUTO_INCREMENT,
				touched INT NOT NULL DEFAULT 0,
				val %s,
				PRIMARY KEY (id)
			)`, tc.colDef)
			srcTable, dstTable := setupTestTables(t, srcDDL, dstDDL)
			db, client := startBufferedSubscriptionFor(t, srcTable, dstTable)

			// Phase 1: INSERT one row per value through the binlog INSERT path.
			tuples := make([]string, len(tc.values))
			for i, v := range tc.values {
				tuples[i] = "(" + v + ")"
			}
			testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s (val) VALUES %s",
				srcTable.QuotedTableName, strings.Join(tuples, ", ")))
			flushAndSync(t, db, client, srcTable, dstTable)

			var srcCount int
			require.NoError(t, db.QueryRowContext(t.Context(),
				fmt.Sprintf("SELECT COUNT(*) FROM %s", srcTable.QuotedTableName)).Scan(&srcCount))
			require.Equal(t, len(tc.values), srcCount, "all inserted rows should be present on the source")

			// Phase 2: UPDATE an unrelated column. The full AFTER image
			// carries `val` back through the applier's REPLACE INTO.
			testutils.RunSQL(t, fmt.Sprintf("UPDATE %s SET touched = touched + 1",
				srcTable.QuotedTableName))
			flushAndSync(t, db, client, srcTable, dstTable)

			// Phase 3: DELETE the lowest-id row.
			testutils.RunSQL(t, fmt.Sprintf("DELETE FROM %s ORDER BY id LIMIT 1",
				srcTable.QuotedTableName))
			flushAndSync(t, db, client, srcTable, dstTable)
		})
	}
}

// flushAndSync waits for the binlog reader to catch up, flushes the
// buffered changes to the destination, and asserts the two tables match.
func flushAndSync(t *testing.T, db *sql.DB, client *binlogClient, src, dst *table.TableInfo) {
	t.Helper()
	require.NoError(t, client.BlockWait(t.Context()))
	require.NoError(t, client.Flush(t.Context()))
	requireBufferedReplicaInSync(t, db, src, dst)
}

// requireBufferedReplicaInSync asserts the destination table is a faithful
// copy of the source: identical row counts, and every source row present
// in the destination with matching `touched` and `val` columns. The
// NULL-safe <=> operator handles NULLs and is valid for every scalar type
// in datatypeCases (GEOMETRY and JSON are tested separately because they
// need bespoke comparisons).
func requireBufferedReplicaInSync(t *testing.T, db *sql.DB, src, dst *table.TableInfo) {
	t.Helper()
	var srcCount, dstCount int
	require.NoError(t, db.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT COUNT(*) FROM %s", src.QuotedTableName)).Scan(&srcCount))
	require.NoError(t, db.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT COUNT(*) FROM %s", dst.QuotedTableName)).Scan(&dstCount))
	require.Equalf(t, srcCount, dstCount, "row count: source=%d destination=%d", srcCount, dstCount)

	mismatch := fmt.Sprintf(
		"SELECT COUNT(*) FROM %s s LEFT JOIN %s d ON s.id = d.id "+
			"WHERE d.id IS NULL OR NOT (s.touched <=> d.touched) OR NOT (s.val <=> d.val)",
		src.QuotedTableName, dst.QuotedTableName)
	var diff int
	require.NoError(t, db.QueryRowContext(t.Context(), mismatch).Scan(&diff))
	if diff > 0 {
		dumpDatatypeMismatch(t, db, src, dst)
	}
	require.Zerof(t, diff, "%d destination row(s) differ from source after replication", diff)
}

// dumpDatatypeMismatch logs the hex of the first few diverging rows so a
// failure shows the actual stored bytes on each side rather than just a
// count. CAST(... AS BINARY) renders every scalar type as bytes.
func dumpDatatypeMismatch(t *testing.T, db *sql.DB, src, dst *table.TableInfo) {
	t.Helper()
	q := fmt.Sprintf(
		"SELECT s.id, HEX(CAST(s.val AS BINARY)), HEX(CAST(d.val AS BINARY)) "+
			"FROM %s s LEFT JOIN %s d ON s.id = d.id "+
			"WHERE d.id IS NULL OR NOT (s.touched <=> d.touched) OR NOT (s.val <=> d.val) "+
			"ORDER BY s.id LIMIT 10",
		src.QuotedTableName, dst.QuotedTableName)
	rows, err := db.QueryContext(t.Context(), q)
	if err != nil {
		t.Logf("could not dump mismatch detail: %v", err)
		return
	}
	defer utils.CloseAndLog(rows)
	for rows.Next() {
		var id int
		var srcHex, dstHex sql.NullString
		if err := rows.Scan(&id, &srcHex, &dstHex); err != nil {
			t.Logf("scanning mismatch detail: %v", err)
			return
		}
		t.Logf("  mismatch id=%d  src=0x%s  dst=0x%s", id, srcHex.String, dstHex.String)
	}
	if err := rows.Err(); err != nil {
		t.Logf("iterating mismatch detail: %v", err)
	}
}

// startBufferedSubscriptionFor wires a binlog client with a single-target
// applier and a buffered subscription for the given source/destination
// tables, starts it, and registers teardown. It returns a fresh DB handle
// for assertions plus the started client.
func startBufferedSubscriptionFor(t *testing.T, srcTable, dstTable *table.TableInfo) (*sql.DB, *binlogClient) {
	t.Helper()
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	target := applier.Target{
		DB:       db,
		KeyRange: "0",
		Config:   cfg,
	}
	appl, err := applier.NewSingleTargetApplier(target, applier.NewApplierDefaultConfig())
	require.NoError(t, err)
	client := NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, appl, NewClientDefaultConfig()).(*binlogClient)
	chunker, err := table.NewChunker(srcTable, table.ChunkerConfig{NewTable: dstTable})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(srcTable, dstTable, chunker))
	require.NoError(t, client.Start(t.Context()))
	t.Cleanup(func() { client.Close() })
	return db, client
}

// TestBufferedMapGeometry exercises a GEOMETRY column through the buffered
// replication path. Geometry values cannot be compared with =/<=>, so this
// test asserts equality via ST_AsText and a CRC32 over the WKT.
func TestBufferedMapGeometry(t *testing.T) {
	t1 := `CREATE TABLE subscription_test (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		location GEOMETRY NOT NULL SRID 4326,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		location GEOMETRY NOT NULL SRID 4326,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)
	db, client := startBufferedSubscriptionFor(t, srcTable, dstTable)

	// INSERT geometry data.
	testutils.RunSQL(t, fmt.Sprintf(`INSERT INTO %s (id, name, location) VALUES
		(1, 'Statue of Liberty', ST_GeomFromText('POINT(-74.0445 40.6892)', 4326, 'axis-order=long-lat')),
		(2, 'Eiffel Tower', ST_GeomFromText('POINT(2.2945 48.8584)', 4326, 'axis-order=long-lat')),
		(3, 'Big Ben', ST_GeomFromText('POINT(-0.1246 51.5007)', 4326, 'axis-order=long-lat'))
	`, srcTable.QuotedTableName))
	require.NoError(t, client.BlockWait(t.Context()))
	require.Equal(t, 3, client.GetDeltaLen())

	// Inspect the buffered subscription directly to verify row images
	// contain geometry data.
	sub := getBufferedMap(t, client, srcTable.SchemaName+"."+srcTable.TableName)
	require.False(t, sub.changes["1"].logicalRow.IsDeleted)
	// The row image should have 3 columns: id, name, location (binary geometry).
	require.Len(t, sub.changes["1"].logicalRow.RowImage, 3)

	// Flush and verify the geometry data was applied correctly.
	allFlushed, err := sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)
	require.True(t, allFlushed)

	var count int
	require.NoError(t, db.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT COUNT(*) FROM %s", dstTable.QuotedTableName)).Scan(&count))
	require.Equal(t, 3, count)

	// Verify the geometry data round-trips correctly via ST_AsText.
	var wkt string
	require.NoError(t, db.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT ST_AsText(location) FROM %s WHERE id = 1", dstTable.QuotedTableName)).Scan(&wkt))
	require.Contains(t, wkt, "POINT")

	// UPDATE geometry data — move the Eiffel Tower.
	testutils.RunSQL(t, fmt.Sprintf(`UPDATE %s SET location = ST_GeomFromText('POINT(2.3 48.9)', 4326, 'axis-order=long-lat') WHERE id = 2`,
		srcTable.QuotedTableName))
	require.NoError(t, client.BlockWait(t.Context()))
	allFlushed, err = sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)
	require.True(t, allFlushed)

	require.NoError(t, db.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT ST_AsText(location) FROM %s WHERE id = 2", dstTable.QuotedTableName)).Scan(&wkt))
	require.Contains(t, wkt, "2.3")

	// DELETE a row.
	testutils.RunSQL(t, fmt.Sprintf("DELETE FROM %s WHERE id = 3", srcTable.QuotedTableName))
	require.NoError(t, client.BlockWait(t.Context()))
	allFlushed, err = sub.Flush(t.Context(), false, nil)
	require.NoError(t, err)
	require.True(t, allFlushed)

	require.NoError(t, db.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT COUNT(*) FROM %s", dstTable.QuotedTableName)).Scan(&count))
	require.Equal(t, 2, count)

	// Final checksum: verify all geometry data matches.
	var checksumSrc, checksumDst string
	require.NoError(t, db.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT BIT_XOR(CRC32(CONCAT(id, name, ST_AsText(location)))) FROM %s", srcTable.QuotedTableName)).Scan(&checksumSrc))
	require.NoError(t, db.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT BIT_XOR(CRC32(CONCAT(id, name, ST_AsText(location)))) FROM %s", dstTable.QuotedTableName)).Scan(&checksumDst))
	require.Equal(t, checksumSrc, checksumDst)
}

// TestBufferedMapJSONNumberRoundTrip is a regression test for a checksum
// mismatch on JSON columns that surfaces whenever a binlog event for a row
// with a numeric JSON value flows through the applier.
//
// Scenario: the row already exists in both the source and the _new table
// (the bulk copier preserves the JSON binary server-side via
// INSERT … SELECT), but a concurrent UPDATE on an unrelated non-JSON
// column emits a full row image through binlog, and the JSON column is
// re-serialised by the applier on the way to _new.
//
// The default go-mysql JSON decoder builds a Go value tree and then
// json.Marshal's it, which silently drops type tags: JSONB_DOUBLE 1.0
// becomes "1" (re-parsed as JSON INTEGER), and JSONB_OPAQUE/NEWDECIMAL
// 1.0 becomes "\"1.0\"" (re-parsed as JSON STRING). The CRC32 over
// CAST(j AS json) then disagrees, and every retry of pkg/checksum finds
// fresh mismatches as concurrent traffic continues — manifesting as
// "checksum found differences on every attempt (N/N)" on JSON-bearing
// tables.
//
// All subtests pass with RenderJSONAsMySQLText=true on the
// BinlogSyncerConfig, which routes JSON values through a renderer that
// emits MySQL-text-compatible JSON directly from the JSONB byte stream,
// preserving each value's original type tag.
func TestBufferedMapJSONNumberRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		// initial JSON value seeded into both src and _new before the
		// subscription begins — i.e. the state the bulk copier has
		// already produced server-side.
		initialJSON string
	}{
		// CAST(... AS DOUBLE) forces a JSONB_DOUBLE in MySQL.
		{
			name:        "json_double_with_double_cast",
			initialJSON: `JSON_OBJECT('amount', CAST(1.0 AS DOUBLE), 'rate', CAST(2.5 AS DOUBLE))`,
		},
		// Bare 1.0 inside JSON_OBJECT is a SQL decimal literal that
		// MySQL stores as JSONB_OPAQUE/NEWDECIMAL. Without the
		// MySQL-text renderer this used to round-trip as a JSON STRING.
		{
			name:        "json_decimal_in_object",
			initialJSON: `JSON_OBJECT('amount', 1.0, 'rate', 2.5)`,
		},
		// A JSON array containing bare 1.0 literals among integer
		// zeros — the production failure shape. In array context MySQL
		// stores 1.0 as JSONB_DOUBLE.
		{
			name:        "json_double_array",
			initialJSON: `'[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1.0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1.0, 1.0]'`,
		},
		// Mixed-type object: string, int, bool, null, nested array of
		// decimals. Exercises object key handling alongside the
		// numeric subcases.
		{
			name:        "json_mixed_object",
			initialJSON: `JSON_OBJECT('s', 'hi', 'n', 42, 'b', CAST(1 AS JSON), 'z', CAST(NULL AS JSON), 'a', JSON_ARRAY(1.0, 2.5, 3.0))`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Schema: a non-JSON column that the workload bumps, plus
			// the JSON column we care about preserving.
			t1 := `CREATE TABLE subscription_test (
				id INT NOT NULL,
				updated_at DATETIME(3) NOT NULL,
				j JSON NOT NULL,
				PRIMARY KEY (id)
			)`
			t2 := `CREATE TABLE _subscription_test_new (
				id INT NOT NULL,
				updated_at DATETIME(3) NOT NULL,
				j JSON NOT NULL,
				PRIMARY KEY (id)
			)`
			srcTable, dstTable := setupTestTables(t, t1, t2)

			// Seed both tables BEFORE starting the binlog subscription
			// so the JSON binary is identical on both sides (server-side
			// INSERT … SELECT, no Go round-trip yet). This simulates the
			// state Spirit's bulk copier produces before the checksum
			// phase begins.
			testutils.RunSQL(t, fmt.Sprintf(
				"INSERT INTO %s (id, updated_at, j) VALUES (1, '2026-05-13 10:58:35.612', %s), (2, '2026-05-13 10:58:35.612', %s)",
				srcTable.QuotedTableName, tc.initialJSON, tc.initialJSON))
			testutils.RunSQL(t, fmt.Sprintf(
				"INSERT INTO %s (id, updated_at, j) SELECT id, updated_at, j FROM %s",
				dstTable.QuotedTableName, srcTable.QuotedTableName))

			// Sanity check: the seed produced matching CRC32 before any
			// binlog activity. If this fails the test is invalid.
			require.Equal(t, fetchJSONColumnCRC(t, "j", srcTable), fetchJSONColumnCRC(t, "j", dstTable),
				"pre-condition: server-side seed must produce identical JSON binary on both tables")

			db, client := startBufferedSubscriptionFor(t, srcTable, dstTable)

			// Bump a non-JSON column on the source. With
			// binlog_row_image=FULL the UPDATE event carries the full
			// AFTER image — including the unchanged JSON column — and
			// the applier writes the row back to _new through the JSON
			// re-encode path that this test guards against.
			const newUpdatedAt = "2026-05-13 11:00:00.000"
			testutils.RunSQL(t, fmt.Sprintf(
				"UPDATE %s SET updated_at = '%s' WHERE id = 1",
				srcTable.QuotedTableName, newUpdatedAt))
			require.NoError(t, client.BlockWait(t.Context()))
			require.NoError(t, client.Flush(t.Context()))

			// Sanity check: the UPDATE event must have been applied to
			// _new. Without this, a regression that silently dropped the
			// event would let the JSON CRCs stay equal (they were equal
			// from the seed) and the test would pass vacuously.
			var dstUpdatedAt string
			require.NoError(t, db.QueryRowContext(t.Context(),
				fmt.Sprintf("SELECT updated_at FROM %s WHERE id = 1", dstTable.QuotedTableName),
			).Scan(&dstUpdatedAt))
			require.Equal(t, newUpdatedAt, dstUpdatedAt,
				"binlog UPDATE event did not propagate to _new; JSON CRC equality below is vacuous")

			// Compare via the CRC32 of a strict CAST(j AS json). This is
			// deliberately STRICTER than pkg/checksum, which normalizes
			// JSON through a text round-trip (see table.castExpr): this
			// test pins the renderer's text fidelity for values that ARE
			// text-expressible, independent of the checksum's relaxation.
			srcCk := fetchJSONColumnCRC(t, "j", srcTable)
			dstCk := fetchJSONColumnCRC(t, "j", dstTable)
			if srcCk != dstCk {
				// Pull both rendered values for a useful failure message.
				var srcJSON, dstJSON string
				require.NoError(t, db.QueryRowContext(t.Context(),
					fmt.Sprintf("SELECT CAST(j AS char CHARACTER SET utf8mb4) FROM %s WHERE id = 1", srcTable.QuotedTableName),
				).Scan(&srcJSON))
				require.NoError(t, db.QueryRowContext(t.Context(),
					fmt.Sprintf("SELECT CAST(j AS char CHARACTER SET utf8mb4) FROM %s WHERE id = 1", dstTable.QuotedTableName),
				).Scan(&dstJSON))
				t.Fatalf("checksum mismatch after binlog UPDATE on a non-JSON column rewrote the JSON value:\n  src(id=1) = %s\n  dst(id=1) = %s", srcJSON, dstJSON)
			}
		})
	}
}

// fetchJSONColumnCRC returns the table-aggregate CRC32 of a JSON column
// (BIT_XOR of CRC32(CAST(col AS json)) across all rows), using the same
// IFNULL / ISNULL structure as pkg/checksum but a strict CAST(col AS json)
// — stricter than the text-round-trip cast pkg/checksum uses (see
// table.castExpr). Used by TestBufferedMapJSONNumberRoundTrip.
func fetchJSONColumnCRC(t *testing.T, col string, tbl *table.TableInfo) int64 {
	t.Helper()
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	var ck int64
	require.NoError(t, db.QueryRowContext(t.Context(), fmt.Sprintf(
		"SELECT BIT_XOR(CRC32(CONCAT(IFNULL(CAST(`id` AS signed),''), ISNULL(`id`), IFNULL(CAST(`%s` AS json),''), ISNULL(`%s`)))) FROM %s",
		col, col, tbl.QuotedTableName)).Scan(&ck))
	return ck
}

// TestBufferedBinaryTrailingZerosToVarbinary is the regression test for
// block/spirit#945 (the gh-ost #909 analog): MySQL strips trailing 0x00
// pad bytes from BINARY(N) values in the binlog row image
// (Field_string::pack) and expects the reader to re-pad to the declared
// width. go-mysql returns the stripped value as-is, so without
// DecodeBinlogRow's re-padding the buffered replay writes short values
// into any target column that does not itself re-pad server-side.
//
// The destination column here is VARBINARY — the case where the
// truncation is visible. (With a BINARY(N) destination MySQL re-pads on
// INSERT, masking the bug; that shape is covered by datatypeCases.)
func TestBufferedBinaryTrailingZerosToVarbinary(t *testing.T) {
	srcDDL := `CREATE TABLE subscription_test (
		id INT NOT NULL AUTO_INCREMENT,
		touched INT NOT NULL DEFAULT 0,
		val BINARY(8) NOT NULL,
		PRIMARY KEY (id)
	)`
	dstDDL := `CREATE TABLE _subscription_test_new (
		id INT NOT NULL AUTO_INCREMENT,
		touched INT NOT NULL DEFAULT 0,
		val VARBINARY(16) NOT NULL,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, srcDDL, dstDDL)
	db, client := startBufferedSubscriptionFor(t, srcTable, dstTable)

	// Phase 1: INSERT values whose row images are stripped to different
	// lengths: 4 bytes + 4 trailing zeros, all zeros (stripped to
	// nothing), interior zeros with a non-zero tail (not stripped), and
	// no zeros at all.
	testutils.RunSQL(t, fmt.Sprintf(`INSERT INTO %s (val) VALUES
		(X'aabbccdd00000000'),
		(X'0000000000000000'),
		(X'1122003300440055'),
		(X'ffffffffffffffff')`, srcTable.QuotedTableName))
	flushAndSync(t, db, client, srcTable, dstTable)

	// Every destination row must hold the full 8-byte width. The <=>
	// comparison in flushAndSync already proves equality; this assertion
	// states the original failure mode explicitly.
	var shortRows int
	require.NoError(t, db.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE OCTET_LENGTH(val) <> 8", dstTable.QuotedTableName)).Scan(&shortRows))
	require.Zero(t, shortRows, "BINARY(8) values must replay into VARBINARY at their full 8-byte width")

	// Phase 2: UPDATE an unrelated column so the full AFTER image drags
	// the trailing-zero values back through the applier.
	testutils.RunSQL(t, fmt.Sprintf("UPDATE %s SET touched = touched + 1", srcTable.QuotedTableName))
	flushAndSync(t, db, client, srcTable, dstTable)

	// Phase 3: DELETE a row.
	testutils.RunSQL(t, fmt.Sprintf("DELETE FROM %s ORDER BY id LIMIT 1", srcTable.QuotedTableName))
	flushAndSync(t, db, client, srcTable, dstTable)
}

// TestBufferedBinaryPKTrailingZeros covers the primary-key side of
// block/spirit#945: when a BINARY(N) column is part of the PRIMARY KEY,
// a stripped row image also strips the replication key. A DELETE built
// from the stripped key (e.g. WHERE pk IN ('a')) never matches the
// full-width row in the destination — binary comparison does not pad —
// so deletes silently miss and the destination diverges.
func TestBufferedBinaryPKTrailingZeros(t *testing.T) {
	srcDDL := `CREATE TABLE subscription_test (
		pk BINARY(8) NOT NULL,
		val INT NOT NULL,
		PRIMARY KEY (pk)
	)`
	dstDDL := `CREATE TABLE _subscription_test_new (
		pk BINARY(8) NOT NULL,
		val INT NOT NULL,
		PRIMARY KEY (pk)
	)`
	srcTable, dstTable := setupTestTables(t, srcDDL, dstDDL)
	db, client := startBufferedSubscriptionFor(t, srcTable, dstTable)

	// Key bytes are ASCII + trailing NULs: NUL is valid UTF-8, so the
	// quoted key literal that DeleteKeys builds stays inside what the
	// unsafe-warning check allows. Keys containing non-UTF8 bytes like
	// 0xBB are covered separately by TestBufferedBinaryPKNonUTF8 (the
	// block/spirit#948 fix), which is independent of the row-image
	// padding under test here.
	testutils.RunSQL(t, fmt.Sprintf(`INSERT INTO %s (pk, val) VALUES
		(X'6100000000000000', 1),
		(X'6200000000000000', 2),
		(X'6364656600000000', 3)`, srcTable.QuotedTableName))
	require.NoError(t, client.BlockWait(t.Context()))
	require.NoError(t, client.Flush(t.Context()))

	// UPDATE one row, DELETE another — both replicate by key.
	testutils.RunSQL(t, fmt.Sprintf("UPDATE %s SET val = 10 WHERE pk = X'6100000000000000'", srcTable.QuotedTableName))
	testutils.RunSQL(t, fmt.Sprintf("DELETE FROM %s WHERE pk = X'6200000000000000'", srcTable.QuotedTableName))
	require.NoError(t, client.BlockWait(t.Context()))
	require.NoError(t, client.Flush(t.Context()))

	var srcCount, dstCount int
	require.NoError(t, db.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT COUNT(*) FROM %s", srcTable.QuotedTableName)).Scan(&srcCount))
	require.NoError(t, db.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT COUNT(*) FROM %s", dstTable.QuotedTableName)).Scan(&dstCount))
	require.Equal(t, 2, srcCount)
	require.Equal(t, srcCount, dstCount, "DELETE by a trailing-zero BINARY PK must replicate (stripped keys never match)")

	var diff int
	require.NoError(t, db.QueryRowContext(t.Context(), fmt.Sprintf(
		"SELECT COUNT(*) FROM %s s LEFT JOIN %s d ON s.pk = d.pk WHERE d.pk IS NULL OR NOT (s.val <=> d.val)",
		srcTable.QuotedTableName, dstTable.QuotedTableName)).Scan(&diff))
	require.Zero(t, diff, "destination rows must match source by full-width binary PK")
}

// TestBufferedBinaryPKNonUTF8 pins block/spirit#948: a BINARY(N) primary
// key whose bytes are not valid UTF-8 (e.g. 0xBB) must replicate through
// the buffered DELETE / PK-changing UPDATE paths. The fix renders DELETE
// key tuples through table.Datum (hex-encoding binary), the same way the
// upsert path does; the previous reverse-the-hash path quoted every
// component as a '...' string literal, so a non-UTF8 key tripped MySQL's
// "Invalid utf8mb4 character string" warning, which dbconn escalates to
// an error and aborts the flush. This is independent of the #945
// trailing-zero stripping covered by TestBufferedBinaryPKTrailingZeros.
func TestBufferedBinaryPKNonUTF8(t *testing.T) {
	srcDDL := `CREATE TABLE subscription_test (
		pk BINARY(8) NOT NULL,
		val INT NOT NULL,
		PRIMARY KEY (pk)
	)`
	dstDDL := `CREATE TABLE _subscription_test_new (
		pk BINARY(8) NOT NULL,
		val INT NOT NULL,
		PRIMARY KEY (pk)
	)`
	srcTable, dstTable := setupTestTables(t, srcDDL, dstDDL)
	db, client := startBufferedSubscriptionFor(t, srcTable, dstTable)

	// 0xBB is not valid UTF-8. The third row is full-width (no trailing
	// 0x00 to strip) so the failure is purely about quoting, not padding.
	testutils.RunSQL(t, fmt.Sprintf(`INSERT INTO %s (pk, val) VALUES
		(X'bb00000000000000', 1),
		(X'cc00000000000000', 2),
		(X'bbccddeeff112233', 3)`, srcTable.QuotedTableName))
	require.NoError(t, client.BlockWait(t.Context()))
	require.NoError(t, client.Flush(t.Context()))

	// DELETE one row by its non-UTF8 key.
	testutils.RunSQL(t, fmt.Sprintf("DELETE FROM %s WHERE pk = X'cc00000000000000'", srcTable.QuotedTableName))
	// PK-changing UPDATE: emits a DELETE for the old (non-UTF8) PK plus an
	// INSERT for the new one — exercises the delete-of-old-PK path too.
	testutils.RunSQL(t, fmt.Sprintf("UPDATE %s SET pk = X'aa00000000000000' WHERE pk = X'bb00000000000000'", srcTable.QuotedTableName))
	require.NoError(t, client.BlockWait(t.Context()))
	require.NoError(t, client.Flush(t.Context()))

	var srcCount, dstCount int
	require.NoError(t, db.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT COUNT(*) FROM %s", srcTable.QuotedTableName)).Scan(&srcCount))
	require.NoError(t, db.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT COUNT(*) FROM %s", dstTable.QuotedTableName)).Scan(&dstCount))
	require.Equal(t, 2, srcCount)
	require.Equal(t, srcCount, dstCount, "DELETE/UPDATE by a non-UTF8 BINARY PK must replicate")

	var diff int
	require.NoError(t, db.QueryRowContext(t.Context(), fmt.Sprintf(
		"SELECT COUNT(*) FROM %s s LEFT JOIN %s d ON s.pk = d.pk WHERE d.pk IS NULL OR NOT (s.val <=> d.val)",
		srcTable.QuotedTableName, dstTable.QuotedTableName)).Scan(&diff))
	require.Zero(t, diff, "destination rows must match source by non-UTF8 binary PK")
}
