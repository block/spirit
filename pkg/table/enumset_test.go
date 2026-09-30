package table

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeEnumOrdinal(t *testing.T) {
	elements := []string{"active", "inactive", "pending"}

	got, err := decodeEnumOrdinal(1, elements)
	require.NoError(t, err)
	require.Equal(t, "active", got)

	got, err = decodeEnumOrdinal(2, elements)
	require.NoError(t, err)
	require.Equal(t, "inactive", got)

	got, err = decodeEnumOrdinal(3, elements)
	require.NoError(t, err)
	require.Equal(t, "pending", got)

	// 0 is MySQL's "invalid value" sentinel; preserved as empty string.
	got, err = decodeEnumOrdinal(0, elements)
	require.NoError(t, err)
	require.Empty(t, got)

	_, err = decodeEnumOrdinal(4, elements)
	require.Error(t, err)
	_, err = decodeEnumOrdinal(-1, elements)
	require.Error(t, err)
}

func TestDecodeSetBitmask(t *testing.T) {
	elements := []string{"read", "write", "execute"}

	got, err := decodeSetBitmask(0, elements)
	require.NoError(t, err)
	require.Empty(t, got)

	got, err = decodeSetBitmask(1, elements) // read
	require.NoError(t, err)
	require.Equal(t, "read", got)

	got, err = decodeSetBitmask(3, elements) // read | write
	require.NoError(t, err)
	require.Equal(t, "read,write", got)

	got, err = decodeSetBitmask(5, elements) // read | execute
	require.NoError(t, err)
	require.Equal(t, "read,execute", got)

	got, err = decodeSetBitmask(7, elements) // all
	require.NoError(t, err)
	require.Equal(t, "read,write,execute", got)

	_, err = decodeSetBitmask(8, elements) // bit 3, no element
	require.Error(t, err)
	// -1 (all 64 bits) is rejected here because elements only defines 3.
	// The 64-element case is covered separately below.
	_, err = decodeSetBitmask(-1, elements)
	require.Error(t, err)
}

// TestDecodeSetBitmask64Elements covers the upper edge of MySQL SET:
// 64 members means valid bitmasks can use bit 63, which surfaces as a
// negative int64 from the go-mysql binlog reader. Regression test for
// the earlier check that rejected any negative input outright.
func TestDecodeSetBitmask64Elements(t *testing.T) {
	elements := make([]string, 64)
	for i := range elements {
		elements[i] = fmt.Sprintf("e%d", i)
	}

	// Bit 63 alone — int64 representation is math.MinInt64 (negative).
	// Build via math.MinInt64 to avoid constant-overflow rules around
	// 1 << 63 in either signed or unsigned form.
	bit63 := int64(math.MinInt64)
	got, err := decodeSetBitmask(bit63, elements)
	require.NoError(t, err)
	require.Equal(t, "e63", got)

	// All 64 bits set — int64 representation is -1.
	got, err = decodeSetBitmask(-1, elements)
	require.NoError(t, err)
	expectedParts := make([]string, 64)
	for i := range expectedParts {
		expectedParts[i] = fmt.Sprintf("e%d", i)
	}
	require.Equal(t, strings.Join(expectedParts, ","), got)

	// Mixed: bit 0 + bit 63.
	got, err = decodeSetBitmask(int64(1)|bit63, elements)
	require.NoError(t, err)
	require.Equal(t, "e0,e63", got)
}

// TestSetInfoReadsStoredEnumSetMembers covers ENUM and SET members with a
// character outside utf8mb3. information_schema reports each such character
// as '?', so the binlog decoder, which maps an ordinal or bitmask to member
// text, wrote '?' in place of the member: the column rejected it, or took
// the '?' member when there is one. SetInfo reads the members MySQL stores.
func TestSetInfoReadsStoredEnumSetMembers(t *testing.T) {
	tt := testutils.NewTestTable(t, "enumset_stored", `CREATE TABLE enumset_stored (
		id INT NOT NULL PRIMARY KEY,
		e ENUM('😀','a','?') NOT NULL,
		s SET('🎉','x','y🎉') NOT NULL,
		q ENUM('?','b') NOT NULL,
		p ENUM('c','d') NOT NULL
	) DEFAULT CHARSET=utf8mb4`)

	ti := NewTableInfo(tt.DB, "test", "enumset_stored")
	require.NoError(t, ti.SetInfo(t.Context()))

	// The reported members, which SetInfo must not keep.
	tp, ok := ti.GetColumnMySQLType("e")
	require.True(t, ok)
	require.Equal(t, "enum('?','a','?')", tp)

	members, ok := ti.EnumSetMembers("e")
	require.True(t, ok)
	assert.Equal(t, []string{"😀", "a", "?"}, members)
	members, ok = ti.EnumSetMembers("s")
	require.True(t, ok)
	assert.Equal(t, []string{"🎉", "x", "y🎉"}, members)
	// A '?' that is the member itself is read back unchanged.
	members, ok = ti.EnumSetMembers("q")
	require.True(t, ok)
	assert.Equal(t, []string{"?", "b"}, members)
	members, ok = ti.EnumSetMembers("p")
	require.True(t, ok)
	assert.Equal(t, []string{"c", "d"}, members)
	_, ok = ti.EnumSetMembers("id")
	assert.False(t, ok)

	row := []any{int32(1), int64(1), int64(5), int64(1), int64(2)}
	require.NoError(t, ti.DecodeBinlogRow(row))
	assert.Equal(t, []any{int32(1), "😀", "🎉,y🎉", "?", "d"}, row)
	row = []any{int32(2), int64(3), int64(2), int64(2), int64(1)}
	require.NoError(t, ti.DecodeBinlogRow(row))
	assert.Equal(t, []any{int32(2), "?", "x", "b", "c"}, row)

	err := ti.MisreportedEnumSetError()
	require.Error(t, err)
	assert.Contains(t, err.Error(), `column "e" of table "enumset_stored"`)
}

// TestMisreportedEnumSetErrorIgnoresQuestionMarkMembers checks that a member
// that really is a '?' is not mistaken for one MySQL reports as '?'.
func TestMisreportedEnumSetErrorIgnoresQuestionMarkMembers(t *testing.T) {
	tt := testutils.NewTestTable(t, "enumset_qmark", `CREATE TABLE enumset_qmark (
		id INT NOT NULL PRIMARY KEY,
		q ENUM('?','b?') NOT NULL,
		s SET('?','x') NOT NULL
	) DEFAULT CHARSET=utf8mb4`)

	ti := NewTableInfo(tt.DB, "test", "enumset_qmark")
	require.NoError(t, ti.SetInfo(t.Context()))
	require.NoError(t, ti.MisreportedEnumSetError())
	members, ok := ti.EnumSetMembers("q")
	require.True(t, ok)
	assert.Equal(t, []string{"?", "b?"}, members)
}

// TestMisreportedEnumSetErrorIgnoresEscapedMembers checks that a member
// information_schema reports escaped (a backslash as \\, a newline as \n) is
// not mistaken for a misreported one, and is decoded as MySQL stores it.
func TestMisreportedEnumSetErrorIgnoresEscapedMembers(t *testing.T) {
	tt := testutils.NewTestTable(t, "enumset_escaped", `CREATE TABLE enumset_escaped (
		id INT NOT NULL PRIMARY KEY,
		e ENUM('a\\b','?','nl\nx') NOT NULL
	) DEFAULT CHARSET=utf8mb4`)

	ti := NewTableInfo(tt.DB, "test", "enumset_escaped")
	require.NoError(t, ti.SetInfo(t.Context()))
	require.NoError(t, ti.MisreportedEnumSetError())
	members, ok := ti.EnumSetMembers("e")
	require.True(t, ok)
	assert.Equal(t, []string{`a\b`, "?", "nl\nx"}, members)
}

// TestSetInfoStoredEnumSetMembersRequirePrimaryKey checks the probe on a
// server with sql_require_primary_key=ON, which refuses a table without a
// primary key (error 3750), temporary tables included.
func TestSetInfoStoredEnumSetMembersRequirePrimaryKey(t *testing.T) {
	testutils.NewTestTable(t, "enumset_reqpk", `CREATE TABLE enumset_reqpk (
		id INT NOT NULL PRIMARY KEY,
		e ENUM('😀','?') NOT NULL
	) DEFAULT CHARSET=utf8mb4`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	cfg.Params["sql_require_primary_key"] = "ON"
	db, err := sql.Open("block-mysql", cfg.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	var on int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT @@SESSION.sql_require_primary_key").Scan(&on))
	require.Equal(t, 1, on)

	ti := NewTableInfo(db, "test", "enumset_reqpk")
	require.NoError(t, ti.SetInfo(t.Context()))
	members, ok := ti.EnumSetMembers("e")
	require.True(t, ok)
	assert.Equal(t, []string{"😀", "?"}, members)
}

// TestSetInfoStoredMembersOf64MemberSet: a SET can have 64 members, and the
// top one is bit 63. The members read back must stay in declaration order,
// so that DecodeBinlogRow maps each bit to its own member. Sorting the probe's
// rows by the value (col+0) put bit 63 first, since it is compared as a
// double and 1<<63 overflows to a negative number.
func TestSetInfoStoredMembersOf64MemberSet(t *testing.T) {
	want := make([]string, 64)
	quoted := make([]string, 64)
	for i := range want {
		want[i] = fmt.Sprintf("m%d", i)
	}
	want[1] = "?" // a real '?' member: reported and stored alike
	for i, m := range want {
		quoted[i] = "'" + m + "'"
	}
	tt := testutils.NewTestTable(t, "enumset_set64", fmt.Sprintf(`CREATE TABLE enumset_set64 (
		id INT NOT NULL PRIMARY KEY,
		s SET(%s) NOT NULL
	) DEFAULT CHARSET=utf8mb4`, strings.Join(quoted, ",")))

	ti := NewTableInfo(tt.DB, "test", "enumset_set64")
	require.NoError(t, ti.SetInfo(t.Context()))
	members, ok := ti.EnumSetMembers("s")
	require.True(t, ok)
	assert.Equal(t, want, members)
	require.NoError(t, ti.MisreportedEnumSetError())

	row := []any{int32(1), int64(1)}
	require.NoError(t, ti.DecodeBinlogRow(row))
	assert.Equal(t, []any{int32(1), "m0"}, row)
}

// TestSetInfoStoredEnumSetMembersProbeKeyClash checks the probe on a column
// that has the name of the probe table's key column.
func TestSetInfoStoredEnumSetMembersProbeKeyClash(t *testing.T) {
	tt := testutils.NewTestTable(t, "enumset_keyclash", "CREATE TABLE enumset_keyclash (\n"+
		"		id INT NOT NULL PRIMARY KEY,\n"+
		"		`"+enumSetProbeIDColumn+"` ENUM('😀','?') NOT NULL\n"+
		"	) DEFAULT CHARSET=utf8mb4")

	ti := NewTableInfo(tt.DB, "test", "enumset_keyclash")
	require.NoError(t, ti.SetInfo(t.Context()))
	members, ok := ti.EnumSetMembers(enumSetProbeIDColumn)
	require.True(t, ok)
	assert.Equal(t, []string{"😀", "?"}, members)
}

// TestSetInfoStoredEnumSetMembersNeedTemporaryTables checks that SetInfo
// fails, naming the privilege, when it cannot read back the members of a
// column reported with a '?', instead of decoding binlog rows to '?'.
func TestSetInfoStoredEnumSetMembersNeedTemporaryTables(t *testing.T) {
	testutils.NewTestTable(t, "enumset_noprivs", `CREATE TABLE enumset_noprivs (
		id INT NOT NULL PRIMARY KEY,
		e ENUM('😀','a') NOT NULL,
		p ENUM('c','d') NOT NULL
	) DEFAULT CHARSET=utf8mb4`)

	// Managing users needs privileges the test DSN's user may not have.
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	cfg.User = "root"
	rootDB, err := sql.Open("block-mysql", cfg.FormatDSN())
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(rootDB) }) // runs after the DROP USER below
	runAsRoot := func(stmt string) {
		_, err := rootDB.ExecContext(t.Context(), stmt)
		require.NoError(t, err)
	}
	runAsRoot("DROP USER IF EXISTS enumsetnoprivs")
	runAsRoot("CREATE USER enumsetnoprivs")
	t.Cleanup(func() {
		_, err := rootDB.ExecContext(context.Background(), "DROP USER IF EXISTS enumsetnoprivs")
		assert.NoError(t, err)
	})
	runAsRoot("GRANT SELECT ON test.* TO enumsetnoprivs")

	cfg.User = "enumsetnoprivs"
	cfg.Passwd = ""
	db, err := sql.Open("block-mysql", cfg.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	db.SetMaxOpenConns(1)

	ti := NewTableInfo(db, "test", "enumset_noprivs")
	ti.DisableAnalyze = true // ANALYZE TABLE needs INSERT
	err = ti.SetInfo(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CREATE TEMPORARY TABLES")

	runAsRoot("GRANT CREATE TEMPORARY TABLES ON test.* TO enumsetnoprivs")
	require.NoError(t, ti.SetInfo(t.Context()))
	members, ok := ti.EnumSetMembers("e")
	require.True(t, ok)
	assert.Equal(t, []string{"😀", "a"}, members)

	// The probe's temporary table does not outlive SetInfo: its connection is
	// closed rather than returned to the pool. With one connection allowed,
	// a reused probe session would still hold the table (error 1050).
	_, err = db.ExecContext(t.Context(), "CREATE TEMPORARY TABLE "+enumSetProbeTable+" (a INT)")
	require.NoError(t, err)
}

// TestDecodeBinlogRowEscapedMembers decodes ENUM and SET members that
// information_schema reports escaped in column_type (a backslash as \\, a
// newline as \n). The decoder wrote the escaped text: MySQL rejected it as not
// a member (warning 1265), or took a different member that the escaped text
// happens to spell, as ordinal 1 below did with member 2.
func TestDecodeBinlogRowEscapedMembers(t *testing.T) {
	tt := testutils.NewTestTable(t, "enumset_escaped_decode", `CREATE TABLE enumset_escaped_decode (
		id INT NOT NULL PRIMARY KEY,
		e ENUM('a\\b','a\\\\b','nl\nx','cr\rx','nul\0x','q''x') NOT NULL,
		s SET('a\\b','x','nl\nx') NOT NULL
	) DEFAULT CHARSET=utf8mb4`)

	ti := NewTableInfo(tt.DB, "test", "enumset_escaped_decode")
	require.NoError(t, ti.SetInfo(t.Context()))
	tp, ok := ti.GetColumnMySQLType("e")
	require.True(t, ok)
	require.Equal(t, `enum('a\\b','a\\\\b','nl\nx','cr\rx','nul\0x','q''x')`, tp)

	want := []string{`a\b`, `a\\b`, "nl\nx", "cr\rx", "nul\x00x", "q'x"}
	for i, member := range want {
		row := []any{int32(i), int64(i + 1), int64(5)}
		require.NoError(t, ti.DecodeBinlogRow(row))
		assert.Equal(t, []any{int32(i), member, "a\\b,nl\nx"}, row)
	}
}

// TestSetInfoBinaryEnumHexMember reads an ENUM of a binary-charset column
// whose members are not all valid utf8mb3. information_schema reports such a
// member as a hex literal (x'815c'), which the parser refused, so SetInfo
// failed and no schema change could run on the table.
func TestSetInfoBinaryEnumHexMember(t *testing.T) {
	tt := testutils.NewTestTable(t, "enumset_hex_member", `CREATE TABLE enumset_hex_member (
		id INT NOT NULL PRIMARY KEY,
		e ENUM(x'5c', x'815c', 'q''x', x'f09f9880', x'eda080', x'00', x'c0af', x'41FF', x'e9', x'0a', x'', x'ffab') CHARACTER SET binary NOT NULL
	)`)
	ti := NewTableInfo(tt.DB, "test", "enumset_hex_member")
	require.NoError(t, ti.SetInfo(t.Context()))
	tp, ok := ti.GetColumnMySQLType("e")
	require.True(t, ok)
	require.Equal(t, `enum('\\',x'815c','q''x',x'f09f9880',x'eda080','\0',x'c0af',x'41ff',x'e9','\n','',x'ffab')`, tp)

	want := []string{`\`, "\x81\\", "q'x", "\U0001F600", "\xed\xa0\x80", "\x00", "\xc0\xaf", "A\xff", "\xe9", "\n", "", "\xff\xab"}
	members, ok := ti.EnumSetMembers("e")
	require.True(t, ok)
	assert.Equal(t, want, members)

	// QuoteEnumSetMember writes each member as MySQL does.
	quoted := make([]string, len(want))
	for i, member := range want {
		quoted[i] = utils.QuoteEnumSetMember(member)
	}
	assert.Equal(t, tp, "enum("+strings.Join(quoted, ",")+")")

	for i, member := range want {
		row := []any{int32(i), int64(i + 1)}
		require.NoError(t, ti.DecodeBinlogRow(row))
		assert.Equal(t, member, row[1])
	}
}
