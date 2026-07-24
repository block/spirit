package change

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file holds the JSON value-fidelity suite for the buffered
// replication write path: binlog row event -> go-mysql
// RenderJSONAsMySQLText -> applier REPLACE INTO (JSON travels as a
// quoted text literal) -> MySQL JSON parse on the destination.
//
// TestBufferedMapJSONNumberRoundTrip (subscription_buffered_datatypes_test.go)
// pins the renderer's TYPE fidelity for common values. This suite pins
// VALUE fidelity for the corner of the double space where the renderer's
// textual spelling interacts badly with MySQL's JSON parser, plus the
// zero-value temporal opaques whose spelling go-mysql gets wrong.
//
// STATUS: the go.mod pin (github.com/morgo/go-mysql
// v1.16.1-0.20260723231236-3aced1dddcf4, the TEMP replace in go.mod)
// carries the formatMySQLDouble my_gcvt-parity fix
// (replication/json_mysql_text.go) and the zero-temporal opaque fix
// (replication/json_binary.go), so the huge-double and zero-temporal
// classes assert green and gate the pin: dropping the replace for an
// upstream go-mysql without those fixes turns every CI job red here.
// seventeen_digit_mantissa stays skipped BY DESIGN — MySQL's own JSON
// text parser misrounds that class (bugs #116160/#112904), which no
// client-side renderer can compensate; do not unskip it, and do not
// weaken any assertions to make them pass.
//
// Every case asserts two per-row fingerprints between source and
// destination:
//
//   - STRICT:  CRC32(CAST(j AS json)) — the canonical render of the
//     stored JSONB binary. A mismatch means the destination persisted a
//     different document than the source.
//   - RELAXED: CRC32(CAST(CAST(j AS char CHARACTER SET utf8mb4) AS json))
//     — the text-round-trip cast pkg/checksum actually uses (see
//     table.castExpr). A mismatch here means Spirit's production
//     checksum itself would flag (or, for corruption the round-trip
//     collapses, MISS) the difference.
//
// Strict-equal implies relaxed-equal (both fingerprints are functions of
// the canonical render), so per case the possible outcomes are:
// both pass, strict-only fails (corruption the production checksum is
// blind to), or both fail (corruption the checksum sees).

// jsonFidelityCase is one row: a JSON document expression plus which
// fingerprints to assert on it.
type jsonFidelityCase struct {
	name string
	doc  string // SQL expression producing the row's JSON document
	// assertStrict additionally pins the STRICT fingerprint. Disabled
	// only where text mediation degrades the type by design (JSONB
	// decimal -> JSON double: MySQL's JSON text grammar has no decimal
	// literal), which makes the strict form unequal for reasons that are
	// not a renderer bug. The RELAXED fingerprint is asserted for every
	// case.
	assertStrict bool
	// zeroDateMode runs the source INSERT on a session with sql_mode=''
	// so zero temporals survive NO_ZERO_DATE / strict mode.
	zeroDateMode bool
	// skip marks a case whose correct behavior is not achievable by any
	// renderer fix (needs a parse-aware formatter); the assertion code
	// still runs for unskipped cases, so unskipping is a one-line change.
	skip string
}

func jsonFidelityCases() []jsonFidelityCase {
	return []jsonFidelityCase{
		// ---- PIN-GATED: huge integral doubles -------------------------
		// Green under the current fork pin; red if the pin is dropped for
		// an unfixed upstream go-mysql. Upstream's formatMySQLDouble
		// (replication/json_mysql_text.go:102-111 at
		// v1.15.1-0.20260526024741-088eb1fbf0ea) rendered whole-number
		// doubles with strconv.FormatFloat(f, 'f', 1, 64): the FULL
		// decimal expansion, not MySQL's shortest round-trip spelling.
		// MySQL reparses the applier's text with rapidjson's
		// normal-precision path, which is NOT correctly rounded (MySQL
		// bugs #116160 / #112904), so long expansions can land on a
		// different double than the source stored. The source values here
		// are built with SQL DOUBLE constructors, which go through
		// my_strtod and ARE correctly rounded. The pinned fork renders
		// my_gcvt-parity shortest forms instead, which reparse exactly.
		//
		// Verified against MySQL 8.0.45; per-key expectations under the
		// UNFIXED upstream renderer (what this case guards against):
		//
		//   'a' 1e16:    applier writes "10000000000000000.0" where MySQL
		//        renders "1e16" — but the expansion reparses exactly, so
		//        the damage is confined to the SQL statement bytes.
		//        Regression canary: equal today, must stay equal.
		//   'b' 2^53:    "9007199254740992.0" reparses exactly => canary.
		//   'c' 1e25:    "10000000000000000905969664.0" reparses one ulp
		//        LOW: dst renders 9.999999999999999e24, src 1e25. STRICT
		//        fails. RELAXED passes — but only because re-parsing the
		//        source's own render ("1e25") misrounds onto the same
		//        corrupted double: the production checksum is blind to
		//        this corruption, which is exactly why the strict
		//        fingerprint exists.
		//   'd' DBL_MAX: the 311-byte expansion of
		//        1.7976931348623157e308 reparses ~6 ulp low to
		//        1.7976931348623145e308. STRICT and RELAXED both fail
		//        (value corruption the production checksum sees).
		//   'e' 2^100:   "1267650600228229401496703205376.0" reparses to
		//        1.2676506002282291e30, src renders 1.2676506002282294e30.
		//        STRICT and RELAXED both fail.
		{
			name: "huge_integral_doubles",
			doc: `JSON_OBJECT(
				'a', 1e16,
				'b', CAST('9007199254740992' AS DOUBLE),
				'c', 1e25,
				'd', 1.7976931348623157e308,
				'e', CAST('1267650600228229401496703205376' AS DOUBLE))`,
			assertStrict: true,
		},

		// ---- PIN-GATED: zero-value temporal opaques -------------------
		// Green under the current fork pin; red if the pin is dropped for
		// an unfixed upstream go-mysql. Upstream renders the zero TIME as
		// "00:00:00" (replication/json_binary.go:539) and the zero
		// DATETIME as "0000-00-00 00:00:00" (json_binary.go:563), but
		// MySQL's render of the temporal opaques always carries the
		// 6-digit fraction: "00:00:00.000000" /
		// "0000-00-00 00:00:00.000000". An applier text write with the
		// shorter spelling persists a differing JSON STRING on the
		// destination — a difference no reparse can heal (STRICT and
		// RELAXED would both fail). The pinned fork renders the fractions
		// MySQL-identically.
		{
			// Midnight TIME is a valid value under strict sql_mode.
			name:         "zero_time_opaque",
			doc:          `JSON_OBJECT('t', CAST('00:00:00' AS TIME(6)))`,
			assertStrict: true,
		},
		{
			// The zero DATETIME needs sql_mode='' at INSERT time
			// (NO_ZERO_DATE turns the CAST into NULL); the destination
			// write is unaffected since the applier ships a JSON string.
			name:         "zero_datetime_opaque",
			doc:          `JSON_OBJECT('dt', CAST('0000-00-00 00:00:00' AS DATETIME(6)))`,
			assertStrict: true,
			zeroDateMode: true,
		},

		// ---- SKIPPED: 17-significant-digit mantissas ------------------
		// Unfixable by ANY fixed rendering: MySQL's JSON parser misrounds
		// the byte-perfect shortest spelling "1.2345678901234567e34" to
		// 1.234567890123457e34 (MySQL bugs #116160, #112904). Today's
		// full-expansion render happens to reparse exactly for THIS value,
		// so an unskipped assertion would pass on the current pin and then
		// regress the moment go-mysql switches to MySQL's own shortest
		// form. Landing such values safely needs a parse-aware formatter
		// that picks a spelling MySQL's parser maps back onto the source
		// double; skip until that follow-up.
		{
			name:         "seventeen_digit_mantissa",
			doc:          `JSON_OBJECT('x', CAST('1.2345678901234567e34' AS DOUBLE))`,
			assertStrict: true,
			skip: "17-digit mantissas misround through MySQL's JSON text parse under any fixed rendering " +
				"(MySQL bugs #116160, #112904); blocked on the parse-aware formatter follow-up in go-mysql",
		},

		// ---- PASS TODAY: regression guards ----------------------------
		// Scale-rendered DECIMAL: the source JSONB opaque renders with its
		// full scale ({"m": 169.090000}) and the text write degrades it to
		// a JSON DOUBLE on the destination ({"m": 169.09}), because
		// MySQL's JSON text grammar has no decimal literal. The STRICT
		// fingerprint is therefore unequal BY DESIGN and is not asserted;
		// the RELAXED production cast degrades both sides identically and
		// must stay equal.
		{
			name:         "decimal_scale_render",
			doc:          `JSON_OBJECT('m', CAST(169.09 AS DECIMAL(12,6)))`,
			assertStrict: false,
		},
		// Small doubles: the non-integral shortest-form branch ('g') and
		// the integral trailing-".0" branch at magnitudes where the
		// expansion reparses exactly.
		{
			name:         "small_doubles",
			doc:          `JSON_OBJECT('a', CAST(0.1 AS DOUBLE), 'b', CAST(169.09 AS DOUBLE), 'c', CAST(1.0 AS DOUBLE))`,
			assertStrict: true,
		},
		// Non-zero temporal opaques: go-mysql's fraction-bearing renders
		// match MySQL's spelling exactly, so the destination's JSON STRING
		// renders byte-identically to the source's opaque.
		{
			name: "nonzero_temporal_opaques",
			doc: `JSON_OBJECT(
				'dt', CAST('2026-06-10 07:40:12.123456' AS DATETIME(6)),
				't', CAST('12:34:56.789012' AS TIME(6)))`,
			assertStrict: true,
		},
		// Generic (non-temporal, non-decimal) opaque: both sides render
		// the "base64:typeNN:..." spelling.
		{
			name:         "binary_opaque",
			doc:          `JSON_OBJECT('bin', CAST(X'deadbeef' AS BINARY(4)))`,
			assertStrict: true,
		},
		// Plain strings (incl. multi-byte UTF-8) and integer extremes.
		{
			name: "strings_and_ints",
			doc: `JSON_OBJECT(
				's', 'hello',
				'u', 'café 😀',
				'i', CAST(-9223372036854775808 AS SIGNED),
				'm', CAST(18446744073709551615 AS UNSIGNED),
				'z', 0)`,
			assertStrict: true,
		},
	}
}

// jsonRowFingerprint carries both per-row fingerprints plus their
// human-readable renders for failure output.
type jsonRowFingerprint struct {
	strictCRC     int64
	relaxedCRC    int64
	strictRender  string // canonical render of the stored document
	relaxedRender string // render after the production checksum's text round-trip
}

// fetchJSONRowFingerprint computes the strict and relaxed fingerprints of
// column j for a single row. The relaxed expression is the JSON branch of
// table.castExpr, i.e. what pkg/checksum really compares.
func fetchJSONRowFingerprint(t *testing.T, db *sql.DB, tbl *table.TableInfo, id int) jsonRowFingerprint {
	t.Helper()
	const strictExpr = "CAST(`j` AS json)"
	const relaxedExpr = "CAST(CAST(`j` AS char CHARACTER SET utf8mb4) AS json)"
	var fp jsonRowFingerprint
	require.NoError(t, db.QueryRowContext(t.Context(), fmt.Sprintf(
		"SELECT CRC32(%s), CRC32(%s), CAST(%s AS char CHARACTER SET utf8mb4), CAST(%s AS char CHARACTER SET utf8mb4) FROM %s WHERE id = %d",
		strictExpr, relaxedExpr, strictExpr, relaxedExpr, tbl.QuotedTableName, id)).
		Scan(&fp.strictCRC, &fp.relaxedCRC, &fp.strictRender, &fp.relaxedRender))
	return fp
}

// TestBufferedJSONValueFidelity pushes one row per jsonFidelityCase
// through the buffered binlog applier (INSERTs happen after the
// subscription starts, so every value is re-serialised by the renderer
// on its way to the destination), flushes once, and asserts the
// destination's stored JSON matches the source per row.
func TestBufferedJSONValueFidelity(t *testing.T) {
	cases := jsonFidelityCases()

	t1 := `CREATE TABLE subscription_test (
		id INT NOT NULL,
		j JSON NOT NULL,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id INT NOT NULL,
		j JSON NOT NULL,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)
	db, client := startBufferedSubscriptionFor(t, srcTable, dstTable)

	// Insert every case's row AFTER the subscription started: the
	// destination receives each document exclusively through binlog row
	// event -> RenderJSONAsMySQLText -> applier text write.
	for i, tc := range cases {
		stmt := fmt.Sprintf("INSERT INTO %s (id, j) VALUES (%d, %s)",
			srcTable.QuotedTableName, i+1, tc.doc)
		if tc.zeroDateMode {
			// Dedicated connection so sql_mode='' cannot leak into the
			// pool used by the applier or the assertions.
			conn, err := db.Conn(t.Context())
			require.NoError(t, err)
			_, err = conn.ExecContext(t.Context(), "SET SESSION sql_mode = ''")
			require.NoError(t, err)
			_, err = conn.ExecContext(t.Context(), stmt)
			require.NoError(t, err)
			require.NoError(t, conn.Close())
		} else {
			testutils.RunSQL(t, stmt)
		}
	}

	// Single flush for all rows.
	require.NoError(t, client.BlockWait(t.Context()))
	require.NoError(t, client.Flush(t.Context()))

	// Vacuity guard: the destination only ever receives rows through the
	// applier, so a dropped event would make the per-row queries below
	// pass trivially or error confusingly. Check the count first.
	var dstCount int
	require.NoError(t, db.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT COUNT(*) FROM %s", dstTable.QuotedTableName)).Scan(&dstCount))
	require.Equal(t, len(cases), dstCount, "every inserted row must reach the destination via the applier")

	for i, tc := range cases {
		id := i + 1
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip != "" {
				t.Skip(tc.skip)
			}
			src := fetchJSONRowFingerprint(t, db, srcTable, id)
			dst := fetchJSONRowFingerprint(t, db, dstTable, id)

			// Dump both sides' renders whenever an asserted fingerprint
			// diverges so the failure output shows the actual documents,
			// not just CRCs. (An unasserted strict divergence — the
			// by-design decimal degradation — is not worth log noise.)
			if src.relaxedCRC != dst.relaxedCRC || (tc.assertStrict && src.strictCRC != dst.strictCRC) {
				t.Logf("JSON render divergence for row id=%d:\n"+
					"  src strict  = %s\n  dst strict  = %s\n"+
					"  src relaxed = %s\n  dst relaxed = %s",
					id, src.strictRender, dst.strictRender, src.relaxedRender, dst.relaxedRender)
			}

			// assert (not require) so a case that fails both fingerprints
			// reports both in a single run.
			assert.Equalf(t, src.relaxedCRC, dst.relaxedCRC,
				"RELAXED fingerprint (production checksum cast, table.castExpr) diverged: "+
					"the corruption is visible to pkg/checksum")
			if tc.assertStrict {
				assert.Equalf(t, src.strictCRC, dst.strictCRC,
					"STRICT fingerprint (CAST(j AS json)) diverged: "+
						"the destination persisted a different document than the source")
			}
		})
	}
}
