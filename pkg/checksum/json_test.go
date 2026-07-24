package checksum

import (
	"database/sql"
	"testing"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	mysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// TestJSONChecksumDecimalTypeDegradation pins the production bug directly:
// a JSON document holding a DECIMAL-typed scalar (e.g. from
// JSON_OBJECT('a', CAST(169.09 AS DECIMAL(12,6)))) renders at its declared
// scale ("169.090000"). Text numbers only ever parse back as
// INTEGER/UNSIGNED/DOUBLE, so a binlog-applied row round-trips through text
// and re-renders shorter ("169.09") — same JSON value, different bytes. The
// target row here is built with exactly that round-trip (CAST(CAST(j AS
// CHAR) AS JSON)) to stand in for what the applier would have written,
// without needing a live migration or concurrent DML to provoke it.
func TestJSONChecksumDecimalTypeDegradation(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS chkjsondec1, _chkjsondec1_new, _chkjsondec1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE chkjsondec1 (a INT NOT NULL, j JSON NOT NULL, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _chkjsondec1_new (a INT NOT NULL, j JSON NOT NULL, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _chkjsondec1_chkpnt (a INT)") // for binlog advancement
	testutils.RunSQL(t, `INSERT INTO chkjsondec1 VALUES (1, JSON_OBJECT('a', CAST(169.09 AS DECIMAL(12,6))))`)
	testutils.RunSQL(t, `INSERT INTO _chkjsondec1_new SELECT a, CAST(CAST(j AS CHAR) AS JSON) FROM chkjsondec1`)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "chkjsondec1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_chkjsondec1_new")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	feed := change.NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), change.NewClientDefaultConfig())
	defer feed.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t2, chunker))
	require.NoError(t, feed.Start(t.Context()))
	require.NoError(t, chunker.Open())

	checker, err := NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, NewCheckerDefaultConfig())
	require.NoError(t, err)
	singleChecker, ok := checker.(*SingleChecker)
	require.True(t, ok, "checker is not of type *SingleChecker")
	require.NoError(t, singleChecker.runChecksum(t.Context()))
}

// TestJSONChecksumFullMantissaTextImage pins the asymmetric-cast contract on
// the class of values that motivated it: doubles whose shortest decimal form
// needs 17 significant digits. MySQL's JSON text parser misrounds these by
// ±1 ulp (bugs #116160/#112904), and for some values repeated parse/render
// cycles never converge — each cycle drifts a further ulp (verified on
// 8.0.45: 4.3319172962174079e299 renders → re-parses → re-renders as
// ...408, ...409, ...4094, never stabilizing). The target here is built with
// exactly one text round-trip (CAST(CAST(j AS CHAR CHARACTER SET utf8mb4)
// AS JSON)), matching the production contract expression and the image the
// buffered copier and binlog applier produce, so the asymmetric checksum
// (source round-trips, target renders strictly) must pass on the first
// attempt. Under the previous symmetric casts this population fails:
// the target side's extra re-parse lands a drifting value on yet another
// neighbor, a self-minted mismatch that no number of retries or recopies
// could clear (verified on 8.0.45 by reverting to symmetric casts: the
// misparse-affected rows 1, 2 and 4 flag on every attempt).
func TestJSONChecksumFullMantissaTextImage(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS chkjsonasym1, _chkjsonasym1_new, _chkjsonasym1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE chkjsonasym1 (a INT NOT NULL, j JSON, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _chkjsonasym1_new (a INT NOT NULL, j JSON, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _chkjsonasym1_chkpnt (a INT)") // for binlog advancement
	// CAST(CAST(s AS DOUBLE) AS JSON) builds the JSON double via strtod
	// (correct parsing), bypassing the buggy JSON text parser — the same
	// binary value an application would have stored.
	testutils.RunSQL(t, `INSERT INTO chkjsonasym1 VALUES
		(1, CAST(CAST('4.3319172962174079e299' AS DOUBLE) AS JSON)),
		(2, CAST(CAST('1.5709949153046066e-55' AS DOUBLE) AS JSON)),
		(3, CAST(CAST('1.2345678901234567e34' AS DOUBLE) AS JSON)),
		(4, JSON_OBJECT('nested', CAST(CAST('4.3319172962174079e299' AS DOUBLE) AS JSON), 'ok', 42)),
		(5, JSON_ARRAY(1, 'two', true, NULL)),
		(6, NULL),
		(7, CAST('9.007199254740992e15' AS JSON))`)
	// One text round-trip: the image every text-mediated write path stores.
	testutils.RunSQL(t, `INSERT INTO _chkjsonasym1_new SELECT a, CAST(CAST(j AS CHAR CHARACTER SET utf8mb4) AS JSON) FROM chkjsonasym1`)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "chkjsonasym1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_chkjsonasym1_new")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	feed := change.NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), change.NewClientDefaultConfig())
	defer feed.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t2, chunker))
	require.NoError(t, feed.Start(t.Context()))
	require.NoError(t, chunker.Open())

	// FixDifferences is left false (the default): a single pass must find
	// zero differences, i.e. this passes on the first attempt with no repair.
	checker, err := NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, NewCheckerDefaultConfig())
	require.NoError(t, err)
	singleChecker, ok := checker.(*SingleChecker)
	require.True(t, ok, "checker is not of type *SingleChecker")
	require.NoError(t, singleChecker.runChecksum(t.Context()))
	require.Equal(t, uint64(0), singleChecker.differencesFound.Load())
}

// TestDistributedJSONChecksumTextImage is the distributed (move/sync) twin
// of TestJSONChecksumFullMantissaTextImage. The DistributedChecker builds
// its checksum SQL independently of the single-server checker (see
// ChecksumChunk), so the side-dependent JSON casts need their own
// end-to-end pin: without it, reverting distributed.go's target side to the
// source expression leaves every JSON test green. The target table here
// lives in a separate database and holds exactly the one-text-round-trip
// image that the move applier's text-mediated writes produce, over the same
// misparse-affected population (17-significant-digit doubles that never
// converge under repeated parse/render). The asymmetric checksum — source
// round-trips, target renders strictly — must pass on the first attempt
// with zero differences. With symmetric casts the target side's extra
// re-parse lands the drifting values on yet another neighbor, self-minting
// mismatches (verified on 8.0.45 by reverting targetChecksumCols to
// sourceChecksumCols in ChecksumChunk: this test fails on rows 1, 2 and 4).
func TestDistributedJSONChecksumTextImage(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	newDBName, _ := testutils.CreateUniqueTestDatabase(t)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS chkjsonasym_dist1")
	testutils.RunSQL(t, "CREATE TABLE chkjsonasym_dist1 (a INT NOT NULL, j JSON, PRIMARY KEY (a))")
	// Same population as TestJSONChecksumFullMantissaTextImage: the
	// misparse-affected doubles built via strtod (correct parsing, the same
	// binary values an application would have stored), plus nested, array,
	// NULL and integer-boundary shapes.
	testutils.RunSQL(t, `INSERT INTO chkjsonasym_dist1 VALUES
		(1, CAST(CAST('4.3319172962174079e299' AS DOUBLE) AS JSON)),
		(2, CAST(CAST('1.5709949153046066e-55' AS DOUBLE) AS JSON)),
		(3, CAST(CAST('1.2345678901234567e34' AS DOUBLE) AS JSON)),
		(4, JSON_OBJECT('nested', CAST(CAST('4.3319172962174079e299' AS DOUBLE) AS JSON), 'ok', 42)),
		(5, JSON_ARRAY(1, 'two', true, NULL)),
		(6, NULL),
		(7, CAST('9.007199254740992e15' AS JSON))`)
	// The target holds the one-text-round-trip image: what the move
	// applier's writes leave on the target server, since they transfer each
	// document as rendered text that the target re-parses.
	testutils.RunSQL(t, "CREATE TABLE "+newDBName+".chkjsonasym_dist1 LIKE chkjsonasym_dist1")
	testutils.RunSQL(t, "INSERT INTO "+newDBName+".chkjsonasym_dist1 SELECT a, CAST(CAST(j AS CHAR CHARACTER SET utf8mb4) AS JSON) FROM chkjsonasym_dist1")

	destCfg := cfg.Clone()
	destCfg.DBName = newDBName

	src, err := dbconn.New(cfg.FormatDSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(src)
	dest, err := dbconn.New(destCfg.FormatDSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(dest)

	t1 := table.NewTableInfo(src, "test", "chkjsonasym_dist1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(dest, newDBName, "chkjsonasym_dist1")
	require.NoError(t, t2.SetInfo(t.Context()))

	target := applier.Target{DB: dest, KeyRange: "0", Config: destCfg}
	app, err := applier.NewSingleTargetApplier(target, applier.NewApplierDefaultConfig())
	require.NoError(t, err)

	feed := change.NewBinlogClient(src, cfg.Addr, cfg.User, cfg.Passwd, app, change.NewClientDefaultConfig())
	defer feed.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t2, chunker))
	require.NoError(t, feed.Start(t.Context()))
	require.NoError(t, chunker.Open())

	// Setting an Applier selects the DistributedChecker. FixDifferences is
	// left false (the default): a single pass must find zero differences,
	// i.e. this passes on the first attempt with no repair.
	config := NewCheckerDefaultConfig()
	config.Applier = app

	checker, err := NewChecker([]*sql.DB{src}, chunker, []change.Source{feed}, config)
	require.NoError(t, err)
	distChecker, ok := checker.(*DistributedChecker)
	require.True(t, ok, "checker is not of type *DistributedChecker")
	require.NoError(t, checker.Run(t.Context()))
	require.Equal(t, uint64(0), distChecker.differencesFound.Load())
}

// TestJSONChecksumMisparsedDoubleRepairConverges proves the repair side of
// the text-image contract. The target rows here hold the source documents
// byte-for-byte (server-side INSERT..SELECT — the unbuffered copy path, or
// tampering), NOT the one-text-round-trip image the checksum's source side
// predicts, plus one genuinely different document. The asymmetric checksum
// must flag this: for a misparsed double, byte-equal is the WRONG target
// state (the deployed text write paths could never have produced it), and
// notably the previous symmetric casts were blind to it — both sides
// round-tripped onto the same value. The repair then recopies the chunk
// through RepairExprs' text round-trip, so the recopied rows store exactly
// the predicted image and the next attempt converges. A byte-faithful repair
// would NOT converge (Run would exhaust MaxRetries=2 re-flagging the same
// rows): that this test passes is the convergence proof.
func TestJSONChecksumMisparsedDoubleRepairConverges(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS chkjsonasym2, _chkjsonasym2_new, _chkjsonasym2_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE chkjsonasym2 (a INT NOT NULL, j JSON, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _chkjsonasym2_new (a INT NOT NULL, j JSON, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _chkjsonasym2_chkpnt (a INT)") // for binlog advancement
	testutils.RunSQL(t, `INSERT INTO chkjsonasym2 VALUES
		(1, CAST(CAST('4.3319172962174079e299' AS DOUBLE) AS JSON)),
		(2, CAST(CAST('1.2345678901234567e34' AS DOUBLE) AS JSON)),
		(3, JSON_ARRAY(1, 'two', true, NULL))`)
	// Byte-faithful target: matches source bytes, violates the text-image
	// contract for rows 1 and 2 (their text image is a different double).
	testutils.RunSQL(t, `INSERT INTO _chkjsonasym2_new SELECT a, j FROM chkjsonasym2`)
	// Plus one unambiguous, parser-independent corruption.
	testutils.RunSQL(t, `UPDATE _chkjsonasym2_new SET j = JSON_ARRAY(99) WHERE a = 3`)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "chkjsonasym2")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_chkjsonasym2_new")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	feed := change.NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), change.NewClientDefaultConfig())
	defer feed.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t2, chunker))
	require.NoError(t, feed.Start(t.Context()))
	require.NoError(t, chunker.Open())

	// Phase 1: with FixDifferences off, the divergence must be flagged.
	detectChecker, err := NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, NewCheckerDefaultConfig())
	require.NoError(t, err)
	singleChecker, ok := detectChecker.(*SingleChecker)
	require.True(t, ok, "checker is not of type *SingleChecker")
	err = singleChecker.runChecksum(t.Context())
	require.ErrorContains(t, err, "checksum mismatch")

	// Phase 2: with FixDifferences on, attempt 1 repairs and attempt 2 must
	// come back clean — MaxRetries=2 leaves no room for a repair that fails
	// to converge.
	require.NoError(t, chunker.Reset())
	config := NewCheckerDefaultConfig()
	config.FixDifferences = true
	config.MaxRetries = 2
	fixChecker, err := NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, config)
	require.NoError(t, err)
	require.NoError(t, fixChecker.Run(t.Context()))

	// The repair must have rewritten the target as the text image, not the
	// source bytes: every target document's stored rendering equals the
	// render→parse→render of its source document. On 8.0.45 the byte image
	// fails this for rows 1 and 2, so this also proves rows were rewritten.
	var notTextImage int
	err = db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM chkjsonasym2 s JOIN _chkjsonasym2_new t USING (a)
		WHERE CAST(t.j AS CHAR CHARACTER SET utf8mb4) <>
		      CAST(CAST(CAST(s.j AS CHAR CHARACTER SET utf8mb4) AS JSON) AS CHAR CHARACTER SET utf8mb4)`).Scan(&notTextImage)
	require.NoError(t, err)
	require.Equal(t, 0, notTextImage)
}

// TestJSONChecksumDecimalGenuineMismatchStillCaught is the flip side of
// TestJSONChecksumDecimalTypeDegradation: the text round-trip must collapse
// type-tag degradation, but it must not become blind to real value
// differences. The target here is built from a DECIMAL value different from
// the source (42.10 vs 42.00), round-tripped through text exactly like the
// passing case above — so if the checksum were over-relaxed to ignore all
// DECIMAL-sourced JSON differences, this would wrongly pass too.
func TestJSONChecksumDecimalGenuineMismatchStillCaught(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS chkjsondec2, _chkjsondec2_new, _chkjsondec2_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE chkjsondec2 (a INT NOT NULL, j JSON NOT NULL, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _chkjsondec2_new (a INT NOT NULL, j JSON NOT NULL, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _chkjsondec2_chkpnt (a INT)") // for binlog advancement
	testutils.RunSQL(t, `INSERT INTO chkjsondec2 VALUES (1, JSON_OBJECT('a', CAST(42.00 AS DECIMAL(12,6))))`)
	testutils.RunSQL(t, `INSERT INTO _chkjsondec2_new VALUES (1, CAST(CAST(JSON_OBJECT('a', CAST(42.10 AS DECIMAL(12,6))) AS CHAR) AS JSON))`)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "chkjsondec2")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_chkjsondec2_new")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	feed := change.NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), change.NewClientDefaultConfig())
	defer feed.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t2, chunker))
	require.NoError(t, feed.Start(t.Context()))
	require.NoError(t, chunker.Open())

	checker, err := NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, NewCheckerDefaultConfig())
	require.NoError(t, err)
	singleChecker, ok := checker.(*SingleChecker)
	require.True(t, ok, "checker is not of type *SingleChecker")
	err = singleChecker.runChecksum(t.Context())
	require.ErrorContains(t, err, "checksum mismatch")
}

// TestJSONChecksumTextToJSONConversion pins a LONGTEXT->JSON conversion
// migration: the source column keeps the raw application text, which can be
// pretty-printed (extra whitespace) while the target's JSON column always
// holds the canonical rendering. castExpr's outer CAST(... AS json) parses
// the source text (the inner char cast is an identity for a text column),
// landing both sides on the same canonical form, so whitespace-only
// differences must not fail the checksum.
func TestJSONChecksumTextToJSONConversion(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS chkjsontxt1, _chkjsontxt1_new, _chkjsontxt1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE chkjsontxt1 (a INT NOT NULL, j LONGTEXT NOT NULL, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _chkjsontxt1_new (a INT NOT NULL, j JSON NOT NULL, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _chkjsontxt1_chkpnt (a INT)") // for binlog advancement
	testutils.RunSQL(t, `INSERT INTO chkjsontxt1 VALUES (1, JSON_PRETTY(JSON_OBJECT('a', 1, 'b', 'x')))`)
	testutils.RunSQL(t, `INSERT INTO _chkjsontxt1_new VALUES (1, JSON_OBJECT('a', 1, 'b', 'x'))`)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "chkjsontxt1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_chkjsontxt1_new")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	feed := change.NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), change.NewClientDefaultConfig())
	defer feed.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t2, chunker))
	require.NoError(t, feed.Start(t.Context()))
	require.NoError(t, chunker.Open())

	checker, err := NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, NewCheckerDefaultConfig())
	require.NoError(t, err)
	singleChecker, ok := checker.(*SingleChecker)
	require.True(t, ok, "checker is not of type *SingleChecker")
	require.NoError(t, singleChecker.runChecksum(t.Context()))
}
