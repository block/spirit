package check

import (
	"context"
	"database/sql"
	"log/slog"
	"slices"
	"testing"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

func TestTableCompatibilityCheckPass(t *testing.T) {
	dbName, db := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE compat_pass (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255))")

	tblInfo := table.NewTableInfo(db, dbName, "compat_pass")
	require.NoError(t, tblInfo.SetInfo(context.Background()))

	r := Resources{
		SourceTables: []*table.TableInfo{tblInfo},
	}
	err := tableCompatibilityCheck(context.Background(), r, slog.Default())
	require.NoError(t, err)
}

// TestTableCompatibilityCheckNonMemoryComparablePK pins that a VARCHAR PK
// (non-memory-comparable) is now accepted. The subscription routes those
// tables through bufferedMap's FIFO queue mode — see issue #607.
func TestTableCompatibilityCheckNonMemoryComparablePK(t *testing.T) {
	dbName, db := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE compat_varchar_pk (id VARCHAR(255) NOT NULL PRIMARY KEY, val INT)")

	tblInfo := table.NewTableInfo(db, dbName, "compat_varchar_pk")
	require.NoError(t, tblInfo.SetInfo(context.Background()))

	r := Resources{
		SourceTables: []*table.TableInfo{tblInfo},
	}
	require.NoError(t, tableCompatibilityCheck(context.Background(), r, slog.Default()))
}

func TestTableCompatibilityCheckMultipleTables(t *testing.T) {
	dbName, db := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE int_pk_table (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, val INT)")
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE varchar_pk_table (id VARCHAR(255) NOT NULL PRIMARY KEY, val INT)")

	intPKTable := table.NewTableInfo(db, dbName, "int_pk_table")
	require.NoError(t, intPKTable.SetInfo(context.Background()))

	varcharPKTable := table.NewTableInfo(db, dbName, "varchar_pk_table")
	require.NoError(t, varcharPKTable.SetInfo(context.Background()))

	r := Resources{
		SourceTables: []*table.TableInfo{intPKTable, varcharPKTable},
	}
	require.NoError(t, tableCompatibilityCheck(context.Background(), r, slog.Default()))
}

func TestTableCompatibilityCheckNoTables(t *testing.T) {
	// Empty table list should pass (nothing to check)
	r := Resources{
		SourceTables: []*table.TableInfo{},
	}
	err := tableCompatibilityCheck(context.Background(), r, slog.Default())
	require.NoError(t, err)
}

// TestTableCompatibilityCheckFloatAndBitPK checks that a source table whose
// primary key includes a FLOAT or a BIT column is refused, and that the same
// types outside the primary key are not.
func TestTableCompatibilityCheckFloatAndBitPK(t *testing.T) {
	dbName, db := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE float_pk (id INT NOT NULL, f FLOAT NOT NULL, PRIMARY KEY (id, f))")
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE bit_pk (b BIT(16) NOT NULL PRIMARY KEY, v INT)")
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE float_bit_cols (id INT NOT NULL PRIMARY KEY, f FLOAT, b BIT(8))")

	info := func(name string) *table.TableInfo {
		ti := table.NewTableInfo(db, dbName, name)
		require.NoError(t, ti.SetInfo(t.Context()))
		return ti
	}
	floatPK, bitPK, cols := info("float_pk"), info("bit_pk"), info("float_bit_cols")

	err := tableCompatibilityCheck(t.Context(), Resources{SourceTables: []*table.TableInfo{cols, floatPK}}, slog.Default())
	require.ErrorContains(t, err, `table 'float_pk' cannot be moved: primary key column "f" of table "float_pk" is a FLOAT, which is not supported`)

	err = tableCompatibilityCheck(t.Context(), Resources{SourceTables: []*table.TableInfo{cols, bitPK}}, slog.Default())
	require.ErrorContains(t, err, `table 'bit_pk' cannot be moved: primary key column "b" of table "bit_pk" is a BIT, which is not supported`)

	require.NoError(t, tableCompatibilityCheck(t.Context(), Resources{SourceTables: []*table.TableInfo{cols}}, slog.Default()))
}

// TestTableCompatibilityCheckMisreportedEnumSet checks that a table with an
// ENUM or SET member that SHOW CREATE TABLE reports as '?' (a character
// outside utf8mb3) is refused: the move creates the target from that
// definition, so the target would not have the member. A member that is a '?'
// is not refused.
func TestTableCompatibilityCheckMisreportedEnumSet(t *testing.T) {
	dbName, db := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE enum_4byte (id INT NOT NULL PRIMARY KEY, e ENUM('😀','a')) DEFAULT CHARSET=utf8mb4")
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE set_4byte (id INT NOT NULL PRIMARY KEY, s SET('x','🎉')) DEFAULT CHARSET=utf8mb4")
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE enum_qmark (id INT NOT NULL PRIMARY KEY, e ENUM('?','a'), s SET('?','x')) DEFAULT CHARSET=utf8mb4")

	info := func(name string) *table.TableInfo {
		ti := table.NewTableInfo(db, dbName, name)
		require.NoError(t, ti.SetInfo(t.Context()))
		return ti
	}
	enum4, set4, qmark := info("enum_4byte"), info("set_4byte"), info("enum_qmark")

	err := tableCompatibilityCheck(t.Context(), Resources{SourceTables: []*table.TableInfo{qmark, enum4}}, slog.Default())
	require.ErrorContains(t, err, `table 'enum_4byte' cannot be moved: column "e" of table "enum_4byte" is enum('?','a'), but MySQL stores a member with a character outside utf8mb3 there`)
	err = tableCompatibilityCheck(t.Context(), Resources{SourceTables: []*table.TableInfo{set4}}, slog.Default())
	require.ErrorContains(t, err, `table 'set_4byte' cannot be moved: column "s" of table "set_4byte" is set('x','?')`)

	require.NoError(t, tableCompatibilityCheck(t.Context(), Resources{SourceTables: []*table.TableInfo{qmark}}, slog.Default()))
}

// TestTableCompatibilityCheckMisreportedEnumSetOnLaterSource checks that a
// misreported member is refused on every source, not only the first. Each
// source is compared with the first by SHOW CREATE TABLE, where
// ENUM('?','a') and ENUM('😀','a') are both reported as enum('?','a').
func TestTableCompatibilityCheckMisreportedEnumSetOnLaterSource(t *testing.T) {
	name0, db0 := createW3DDatabase(t)
	name1, db1 := createW3DDatabase(t)
	testutils.RunSQLInDatabase(t, name0, "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, e ENUM('?','a')) DEFAULT CHARSET=utf8mb4")
	testutils.RunSQLInDatabase(t, name1, "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, e ENUM('😀','a')) DEFAULT CHARSET=utf8mb4")
	info := func(db *sql.DB, schema string) *table.TableInfo {
		ti := table.NewTableInfo(db, schema, "t")
		require.NoError(t, ti.SetInfo(t.Context()))
		return ti
	}
	t0, t1 := info(db0, name0), info(db1, name1)

	r := Resources{
		Sources:      []SourceResource{{DB: db0, Tables: []*table.TableInfo{t0}}, {DB: db1, Tables: []*table.TableInfo{t1}}},
		SourceTables: []*table.TableInfo{t0},
	}
	err := tableCompatibilityCheck(t.Context(), r, slog.Default())
	require.ErrorContains(t, err, `table 't' on source 1 cannot be moved: column "e" of table "t" is enum('?','a'), but MySQL stores a member with a character outside utf8mb3 there`)

	r.Sources = r.Sources[:1]
	require.NoError(t, tableCompatibilityCheck(t.Context(), r, slog.Default()))
}

// TestTableCompatibilityCheckRegisteredForResume pins that a resume from
// checkpoint applies the table requirements too: that path runs the resume
// checks instead of re-running the post-setup ones.
func TestTableCompatibilityCheckRegisteredForResume(t *testing.T) {
	lock.Lock()
	defer lock.Unlock()
	require.Equal(t, ScopePostSetup, checks["table_compatibility"].scope)
	require.Equal(t, ScopeResume, checks["table_compatibility_resume"].scope)
}

// TestTableCompatibilityCheckUnsupportedNames checks that a '.' or a backtick
// in a moved table's name, its schema, or a source or target schema is
// refused, and that ordinary names pass. The check reads only the names, so
// the table metadata is built without a database.
func TestTableCompatibilityCheckUnsupportedNames(t *testing.T) {
	keyed := func(schema, name string) *table.TableInfo {
		return &table.TableInfo{SchemaName: schema, TableName: name, KeyColumns: []string{"id"}}
	}
	withDB := func(name string) *mysql.Config {
		cfg := mysql.NewConfig()
		cfg.DBName = name
		return cfg
	}
	tests := []struct {
		name    string
		r       Resources
		wantErr string
	}{
		{
			name: "plain names pass",
			r: Resources{
				Sources:      []SourceResource{{Config: withDB("src1")}},
				Targets:      []applier.Target{{Config: withDB("dst1")}},
				SourceTables: []*table.TableInfo{keyed("src1", "t1"), keyed("src1", "t2")},
			},
		},
		{
			name:    "dot in table name",
			r:       Resources{SourceTables: []*table.TableInfo{keyed("src1", "t1"), keyed("src1", "t.2")}},
			wantErr: `table 't.2' cannot be moved: table name "t.2" contains a '.', which Spirit does not support`,
		},
		{
			name:    "backtick in table name",
			r:       Resources{SourceTables: []*table.TableInfo{keyed("src1", "t`2")}},
			wantErr: "table 't`2' cannot be moved: table name \"t`2\" contains a backtick, which Spirit does not support",
		},
		{
			name:    "dot in table schema name",
			r:       Resources{SourceTables: []*table.TableInfo{keyed("src.1", "t1")}},
			wantErr: `table 't1' cannot be moved: schema name "src.1" contains a '.', which Spirit does not support`,
		},
		{
			name:    "backtick in table schema name",
			r:       Resources{SourceTables: []*table.TableInfo{keyed("src`1", "t1")}},
			wantErr: "table 't1' cannot be moved: schema name \"src`1\" contains a backtick",
		},
		{
			name: "dot in a second source schema",
			r: Resources{
				Sources:      []SourceResource{{Config: withDB("src1")}, {Config: withDB("src.2")}},
				SourceTables: []*table.TableInfo{keyed("src1", "t1")},
			},
			wantErr: `cannot move: source schema name "src.2" contains a '.', which Spirit does not support`,
		},
		{
			name: "backtick in source schema",
			r: Resources{
				Sources: []SourceResource{{Config: withDB("src`1")}},
			},
			wantErr: "cannot move: source schema name \"src`1\" contains a backtick",
		},
		{
			name: "dot in target schema",
			r: Resources{
				Targets:      []applier.Target{{Config: withDB("dst.1")}},
				SourceTables: []*table.TableInfo{keyed("src1", "t1")},
			},
			wantErr: `cannot move: target schema name "dst.1" contains a '.', which Spirit does not support`,
		},
		{
			name: "backtick in target schema",
			r: Resources{
				Targets: []applier.Target{{Config: withDB("dst1")}, {Config: withDB("dst`2")}},
			},
			wantErr: "cannot move: target schema name \"dst`2\" contains a backtick",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Both registrations share the callback; run through RunChecks at
			// each scope so the resume path is covered as well.
			for _, scope := range []ScopeFlag{ScopePostSetup, ScopeResume} {
				err := RunChecks(t.Context(), tt.r, slog.Default(), scope, otherChecks("table_compatibility", "table_compatibility_resume")...)
				if tt.wantErr == "" {
					require.NoError(t, err)
					continue
				}
				require.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}

// otherChecks returns the names of every registered check except keep, for
// excluding them from RunChecks.
func otherChecks(keep ...string) []string {
	lock.Lock()
	defer lock.Unlock()
	var names []string
	for name := range checks {
		if !slices.Contains(keep, name) {
			names = append(names, name)
		}
	}
	return names
}
