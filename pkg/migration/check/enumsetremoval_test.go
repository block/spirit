package check

import (
	"database/sql"
	"log/slog"
	"testing"

	_ "github.com/block/mysql"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestEnumSetRemovalCheckEnumToVarchar(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS enumremoval`)
	testutils.RunSQL(t, `CREATE TABLE enumremoval (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		status ENUM('active', 'inactive', 'pending') NOT NULL
	)`)

	tbl := table.NewTableInfo(db, "test", "enumremoval")
	require.NoError(t, tbl.SetInfo(t.Context()))

	// ENUM→VARCHAR is supported: processRowsEvent decodes the binlog
	// ordinal into the element string before the applier sees it.
	r := Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE enumremoval MODIFY COLUMN status VARCHAR(255) NOT NULL")[0],
	}
	require.NoError(t, enumSetRemovalCheck(t.Context(), r, slog.Default()))
}

func TestEnumSetRemovalCheckEnumToText(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS enumremoval2`)
	testutils.RunSQL(t, `CREATE TABLE enumremoval2 (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		status ENUM('active', 'inactive') NOT NULL
	)`)

	tbl := table.NewTableInfo(db, "test", "enumremoval2")
	require.NoError(t, tbl.SetInfo(t.Context()))

	// ENUM→TEXT is supported for the same reason as ENUM→VARCHAR.
	r := Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE enumremoval2 MODIFY COLUMN status TEXT NOT NULL")[0],
	}
	require.NoError(t, enumSetRemovalCheck(t.Context(), r, slog.Default()))
}

func TestEnumSetRemovalCheckEnumToInt(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS enumremoval3`)
	testutils.RunSQL(t, `CREATE TABLE enumremoval3 (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		status ENUM('active', 'inactive') NOT NULL
	)`)

	tbl := table.NewTableInfo(db, "test", "enumremoval3")
	require.NoError(t, tbl.SetInfo(t.Context()))

	// ENUM→INT is still rejected: the decoded string would be coerced
	// to 0, silently losing data.
	r := Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE enumremoval3 MODIFY COLUMN status INT NOT NULL")[0],
	}
	err = enumSetRemovalCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
	require.ErrorContains(t, err, "ENUM")
	require.ErrorContains(t, err, "string-typed target")
}

func TestEnumSetRemovalCheckSetToVarchar(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS setremoval`)
	testutils.RunSQL(t, `CREATE TABLE setremoval (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		flags SET('read', 'write', 'execute') NOT NULL
	)`)

	tbl := table.NewTableInfo(db, "test", "setremoval")
	require.NoError(t, tbl.SetInfo(t.Context()))

	// SET→VARCHAR is supported: bitmask is decoded to a comma-joined
	// string of element names before the applier sees it.
	r := Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE setremoval MODIFY COLUMN flags VARCHAR(255) NOT NULL")[0],
	}
	require.NoError(t, enumSetRemovalCheck(t.Context(), r, slog.Default()))
}

func TestEnumSetRemovalCheckChangeColumn(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS enumremoval_change`)
	testutils.RunSQL(t, `CREATE TABLE enumremoval_change (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		status ENUM('active', 'inactive') NOT NULL
	)`)

	tbl := table.NewTableInfo(db, "test", "enumremoval_change")
	require.NoError(t, tbl.SetInfo(t.Context()))

	// CHANGE COLUMN ENUM→VARCHAR is supported.
	r := Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE enumremoval_change CHANGE COLUMN status status VARCHAR(255) NOT NULL")[0],
	}
	require.NoError(t, enumSetRemovalCheck(t.Context(), r, slog.Default()))
}

func TestEnumSetRemovalCheckEnumToEnum(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS enumremoval_safe`)
	testutils.RunSQL(t, `CREATE TABLE enumremoval_safe (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		status ENUM('active', 'inactive') NOT NULL
	)`)

	tbl := table.NewTableInfo(db, "test", "enumremoval_safe")
	require.NoError(t, tbl.SetInfo(t.Context()))

	// ENUM→ENUM (append): handled by enumReorderCheck, not this check.
	r := Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE enumremoval_safe MODIFY COLUMN status ENUM('active', 'inactive', 'pending') NOT NULL")[0],
	}
	err = enumSetRemovalCheck(t.Context(), r, slog.Default())
	require.NoError(t, err)
}

func TestEnumSetRemovalCheckNonEnumColumn(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS enumremoval_nochange`)
	testutils.RunSQL(t, `CREATE TABLE enumremoval_nochange (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255) NOT NULL
	)`)

	tbl := table.NewTableInfo(db, "test", "enumremoval_nochange")
	require.NoError(t, tbl.SetInfo(t.Context()))

	// VARCHAR→TEXT: not an ENUM/SET removal.
	r := Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE enumremoval_nochange MODIFY COLUMN name TEXT NOT NULL")[0],
	}
	err = enumSetRemovalCheck(t.Context(), r, slog.Default())
	require.NoError(t, err)
}

func TestEnumSetRemovalCheckEnumToSet(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS enumtoset`)
	testutils.RunSQL(t, `CREATE TABLE enumtoset (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		status ENUM('active', 'inactive') NOT NULL
	)`)

	tbl := table.NewTableInfo(db, "test", "enumtoset")
	require.NoError(t, tbl.SetInfo(t.Context()))

	// ENUM→SET with the same elements is supported: each decoded ENUM
	// string is a single-element value the SET accepts unchanged.
	r := Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE enumtoset MODIFY COLUMN status SET('active', 'inactive') NOT NULL")[0],
	}
	require.NoError(t, enumSetRemovalCheck(t.Context(), r, slog.Default()))

	// Element reordering or extra new elements is also fine — the
	// destination SET parses the decoded string against its own element
	// list, so any superset of the ENUM elements works.
	r.Statement = statement.MustNew("ALTER TABLE enumtoset MODIFY COLUMN status SET('inactive', 'active', 'pending') NOT NULL")[0]
	require.NoError(t, enumSetRemovalCheck(t.Context(), r, slog.Default()))
}

func TestEnumSetRemovalCheckEnumToSetMissingElement(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS enumtoset_missing`)
	testutils.RunSQL(t, `CREATE TABLE enumtoset_missing (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		status ENUM('active', 'inactive', 'pending') NOT NULL
	)`)

	tbl := table.NewTableInfo(db, "test", "enumtoset_missing")
	require.NoError(t, tbl.SetInfo(t.Context()))

	// The new SET drops 'pending'. We allow the check to pass — if
	// 'pending' rows actually exist they land as the empty set and the
	// post-cutover checksum will reject; if no row used 'pending', the
	// migration succeeds. Rejecting at preflight would block legitimate
	// schema cleanups that drop defined-but-unused ENUM values.
	r := Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE enumtoset_missing MODIFY COLUMN status SET('active', 'inactive') NOT NULL")[0],
	}
	require.NoError(t, enumSetRemovalCheck(t.Context(), r, slog.Default()))
}

func TestEnumSetRemovalCheckSetToEnum(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS settoenum`)
	testutils.RunSQL(t, `CREATE TABLE settoenum (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		flags SET('read', 'write') NOT NULL
	)`)

	tbl := table.NewTableInfo(db, "test", "settoenum")
	require.NoError(t, tbl.SetInfo(t.Context()))

	// SET→ENUM is rejected: SET cells can hold multiple elements that
	// ENUM cannot represent.
	r := Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE settoenum MODIFY COLUMN flags ENUM('read', 'write') NOT NULL")[0],
	}
	err = enumSetRemovalCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
	require.ErrorContains(t, err, "unsafe SET to ENUM")
}
