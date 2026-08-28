package lint

import (
	"slices"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/require"
)

// --- CREATE TABLE in changes ---

func TestDatetimeIndexPositionLinter_CreateTable_DatetimeNotLast(t *testing.T) {
	sql := `CREATE TABLE jobs (
		id INT PRIMARY KEY,
		updated_at DATETIME NOT NULL,
		attempt INT NOT NULL,
		KEY updated_at_attempt (updated_at, attempt)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &DatetimeIndexPositionLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)
	require.Equal(t, "datetime_index_position", violations[0].Linter.Name())
	require.Equal(t, SeverityWarning, violations[0].Severity)
	require.Equal(t, "jobs", violations[0].Location.Table)
	require.NotNil(t, violations[0].Location.Index)
	require.Equal(t, "updated_at_attempt", *violations[0].Location.Index)
	require.NotNil(t, violations[0].Location.Column)
	require.Equal(t, "updated_at", *violations[0].Location.Column)
	require.Contains(t, violations[0].Message, "DATETIME")
	require.Contains(t, violations[0].Message, "updated_at")
	require.Contains(t, violations[0].Message, "range")
}

func TestDatetimeIndexPositionLinter_CreateTable_TimestampNotLast(t *testing.T) {
	sql := `CREATE TABLE events (
		id INT PRIMARY KEY,
		created_at TIMESTAMP NOT NULL,
		shard_id INT NOT NULL,
		KEY created_shard (created_at, shard_id)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := (&DatetimeIndexPositionLinter{}).Lint(nil, stmts)

	require.Len(t, violations, 1)
	require.Equal(t, SeverityWarning, violations[0].Severity)
	require.Contains(t, violations[0].Message, `"TIMESTAMP" column "created_at"`)
	require.Equal(t, "created_at", *violations[0].Location.Column)
}

func TestDatetimeIndexPositionLinter_CreateTable_DateNotLast(t *testing.T) {
	sql := `CREATE TABLE bookings (
		id INT PRIMARY KEY,
		booking_date DATE NOT NULL,
		user_id INT NOT NULL,
		KEY date_user (booking_date, user_id)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := (&DatetimeIndexPositionLinter{}).Lint(nil, stmts)

	require.Len(t, violations, 1)
	require.Equal(t, SeverityWarning, violations[0].Severity)
	require.Contains(t, violations[0].Message, "DATE")
	require.Equal(t, "booking_date", *violations[0].Location.Column)
}

func TestDatetimeIndexPositionLinter_CreateTable_DatetimeLast(t *testing.T) {
	sql := `CREATE TABLE jobs (
		id INT PRIMARY KEY,
		attempt INT NOT NULL,
		updated_at DATETIME NOT NULL,
		KEY attempt_updated (attempt, updated_at)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := (&DatetimeIndexPositionLinter{}).Lint(nil, stmts)

	// DATETIME is last — no violation
	require.Empty(t, violations)
}

func TestDatetimeIndexPositionLinter_CreateTable_SingleColumnIndex(t *testing.T) {
	sql := `CREATE TABLE jobs (
		id INT PRIMARY KEY,
		updated_at DATETIME NOT NULL,
		KEY updated (updated_at)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := (&DatetimeIndexPositionLinter{}).Lint(nil, stmts)

	// Single-column index — no violation regardless of type
	require.Empty(t, violations)
}

func TestDatetimeIndexPositionLinter_CreateTable_NoDatetimeColumns(t *testing.T) {
	sql := `CREATE TABLE users (
		id INT PRIMARY KEY,
		name VARCHAR(100),
		email VARCHAR(255),
		KEY name_email (name, email)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := (&DatetimeIndexPositionLinter{}).Lint(nil, stmts)

	require.Empty(t, violations)
}

func TestDatetimeIndexPositionLinter_CreateTable_DatetimeInMiddle(t *testing.T) {
	sql := `CREATE TABLE jobs (
		id INT PRIMARY KEY,
		shard_id INT NOT NULL,
		updated_at DATETIME NOT NULL,
		attempt INT NOT NULL,
		KEY shard_updated_attempt (shard_id, updated_at, attempt)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := (&DatetimeIndexPositionLinter{}).Lint(nil, stmts)

	// updated_at is in position 2 of 3 — flagged
	require.Len(t, violations, 1)
	require.Equal(t, "updated_at", *violations[0].Location.Column)
	require.Contains(t, violations[0].Message, "position 2 of 3")
}

func TestDatetimeIndexPositionLinter_CreateTable_MultipleDatetimesNotLast(t *testing.T) {
	sql := `CREATE TABLE audit (
		id INT PRIMARY KEY,
		created_at DATETIME NOT NULL,
		updated_at DATETIME NOT NULL,
		actor VARCHAR(100),
		KEY two_dates_actor (created_at, updated_at, actor)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := (&DatetimeIndexPositionLinter{}).Lint(nil, stmts)

	// Both created_at and updated_at are in non-last positions
	require.Len(t, violations, 2)
	cols := map[string]bool{}
	for _, v := range violations {
		require.Equal(t, SeverityWarning, v.Severity)
		cols[*v.Location.Column] = true
	}
	require.True(t, cols["created_at"])
	require.True(t, cols["updated_at"])
}

func TestDatetimeIndexPositionLinter_CreateTable_PrimaryKeyCompositeDatetimeNotLast(t *testing.T) {
	sql := `CREATE TABLE log_partitions (
		log_day DATE NOT NULL,
		shard_id INT NOT NULL,
		PRIMARY KEY (log_day, shard_id)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := (&DatetimeIndexPositionLinter{}).Lint(nil, stmts)

	// PRIMARY KEY with DATE in non-last position — flagged
	require.Len(t, violations, 1)
	require.Equal(t, "log_day", *violations[0].Location.Column)
	require.Equal(t, "PRIMARY", *violations[0].Location.Index)
}

func TestDatetimeIndexPositionLinter_CreateTable_UniqueIndexDatetimeNotLast(t *testing.T) {
	sql := `CREATE TABLE bookings (
		id INT PRIMARY KEY,
		booking_date DATE NOT NULL,
		ref VARCHAR(64) NOT NULL,
		UNIQUE KEY date_ref (booking_date, ref)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := (&DatetimeIndexPositionLinter{}).Lint(nil, stmts)

	require.Len(t, violations, 1)
	require.Equal(t, "booking_date", *violations[0].Location.Column)
}

// --- Existing tables (legacy schemas) ---

func TestDatetimeIndexPositionLinter_ExistingTable_DatetimeNotLast(t *testing.T) {
	existingSQL := `CREATE TABLE jobs (
		id INT PRIMARY KEY,
		updated_at DATETIME NOT NULL,
		attempt INT NOT NULL,
		KEY updated_at_attempt (updated_at, attempt)
	)`
	ct, err := statement.ParseCreateTable(existingSQL)
	require.NoError(t, err)

	violations := (&DatetimeIndexPositionLinter{}).Lint([]*statement.CreateTable{ct}, nil)

	require.Len(t, violations, 1)
	require.Equal(t, SeverityWarning, violations[0].Severity)
	require.Equal(t, "updated_at", *violations[0].Location.Column)
}

// --- ALTER TABLE paths ---

func TestDatetimeIndexPositionLinter_AlterAddBadIndex(t *testing.T) {
	existingSQL := `CREATE TABLE jobs (
		id INT PRIMARY KEY,
		updated_at DATETIME NOT NULL,
		attempt INT NOT NULL
	)`
	ct, err := statement.ParseCreateTable(existingSQL)
	require.NoError(t, err)

	alterSQL := `ALTER TABLE jobs ADD INDEX updated_at_attempt (updated_at, attempt)`
	stmts, err := statement.New(alterSQL)
	require.NoError(t, err)

	violations := (&DatetimeIndexPositionLinter{}).Lint([]*statement.CreateTable{ct}, stmts)

	require.Len(t, violations, 1)
	require.Equal(t, SeverityWarning, violations[0].Severity)
	require.Equal(t, "updated_at", *violations[0].Location.Column)
	require.Equal(t, "updated_at_attempt", *violations[0].Location.Index)
}

func TestDatetimeIndexPositionLinter_AlterAddUnnamedBadIndex(t *testing.T) {
	// ALTER TABLE ... ADD INDEX without a name leaves idx.Name empty in the
	// post-state. The violation should still produce a useful label (derived
	// from the column list) and should not set Location.Index to an empty
	// string.
	existingSQL := `CREATE TABLE jobs (
		id INT PRIMARY KEY,
		updated_at DATETIME NOT NULL,
		attempt INT NOT NULL
	)`
	ct, err := statement.ParseCreateTable(existingSQL)
	require.NoError(t, err)

	alterSQL := `ALTER TABLE jobs ADD INDEX (updated_at, attempt)`
	stmts, err := statement.New(alterSQL)
	require.NoError(t, err)

	violations := (&DatetimeIndexPositionLinter{}).Lint([]*statement.CreateTable{ct}, stmts)

	require.Len(t, violations, 1)
	require.Equal(t, SeverityWarning, violations[0].Severity)
	require.Contains(t, violations[0].Message, "Unnamed index on (updated_at, attempt)")
	require.NotContains(t, violations[0].Message, `Index ""`)
	require.Nil(t, violations[0].Location.Index, "Location.Index should be nil when the index has no name")
	require.Equal(t, "updated_at", *violations[0].Location.Column)
	require.NotNil(t, violations[0].Suggestion)
	require.Contains(t, *violations[0].Suggestion, "unnamed index on (updated_at, attempt)")
}

func TestDatetimeIndexPositionLinter_AlterAddGoodIndex(t *testing.T) {
	existingSQL := `CREATE TABLE jobs (
		id INT PRIMARY KEY,
		updated_at DATETIME NOT NULL,
		attempt INT NOT NULL
	)`
	ct, err := statement.ParseCreateTable(existingSQL)
	require.NoError(t, err)

	alterSQL := `ALTER TABLE jobs ADD INDEX attempt_updated (attempt, updated_at)`
	stmts, err := statement.New(alterSQL)
	require.NoError(t, err)

	violations := (&DatetimeIndexPositionLinter{}).Lint([]*statement.CreateTable{ct}, stmts)

	require.Empty(t, violations)
}

func TestDatetimeIndexPositionLinter_AlterDropBadIndex(t *testing.T) {
	// Existing table has a bad index; the ALTER drops it. Post-state has no
	// bad index, so no violation should fire.
	existingSQL := `CREATE TABLE jobs (
		id INT PRIMARY KEY,
		updated_at DATETIME NOT NULL,
		attempt INT NOT NULL,
		KEY updated_at_attempt (updated_at, attempt)
	)`
	ct, err := statement.ParseCreateTable(existingSQL)
	require.NoError(t, err)

	alterSQL := `ALTER TABLE jobs DROP INDEX updated_at_attempt`
	stmts, err := statement.New(alterSQL)
	require.NoError(t, err)

	violations := (&DatetimeIndexPositionLinter{}).Lint([]*statement.CreateTable{ct}, stmts)

	require.Empty(t, violations, "DROP INDEX should clear the warning since post-state no longer has the bad index")
}

func TestDatetimeIndexPositionLinter_AlterReplaceBadIndexWithGood(t *testing.T) {
	// Existing has bad ordering; ALTER drops the bad one and adds a reordered one.
	existingSQL := `CREATE TABLE jobs (
		id INT PRIMARY KEY,
		updated_at DATETIME NOT NULL,
		attempt INT NOT NULL,
		KEY bad_idx (updated_at, attempt)
	)`
	ct, err := statement.ParseCreateTable(existingSQL)
	require.NoError(t, err)

	alterSQL := `ALTER TABLE jobs DROP INDEX bad_idx, ADD INDEX good_idx (attempt, updated_at)`
	stmts, err := statement.New(alterSQL)
	require.NoError(t, err)

	violations := (&DatetimeIndexPositionLinter{}).Lint([]*statement.CreateTable{ct}, stmts)

	require.Empty(t, violations)
}

// --- Edge cases ---

func TestDatetimeIndexPositionLinter_FulltextIndexIgnored(t *testing.T) {
	sql := `CREATE TABLE articles (
		id INT PRIMARY KEY,
		title VARCHAR(255),
		body TEXT,
		FULLTEXT KEY ft_title_body (title, body)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := (&DatetimeIndexPositionLinter{}).Lint(nil, stmts)

	// No datetime columns anyway, but the linter should also short-circuit
	// before even consulting fulltext indexes.
	require.Empty(t, violations)
}

func TestDatetimeIndexPositionLinter_NoIndexes(t *testing.T) {
	sql := `CREATE TABLE jobs (
		id INT PRIMARY KEY,
		updated_at DATETIME NOT NULL
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := (&DatetimeIndexPositionLinter{}).Lint(nil, stmts)

	require.Empty(t, violations)
}

func TestDatetimeIndexPositionLinter_CaseInsensitiveColumnLookup(t *testing.T) {
	// Column declared with mixed case, index references it with different case.
	sql := `CREATE TABLE jobs (
		id INT PRIMARY KEY,
		UpdatedAt DATETIME NOT NULL,
		attempt INT NOT NULL,
		KEY mixed (updatedat, attempt)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := (&DatetimeIndexPositionLinter{}).Lint(nil, stmts)

	require.Len(t, violations, 1)
	require.Equal(t, SeverityWarning, violations[0].Severity)
}

func TestDatetimeIndexPositionLinter_NilInputs(t *testing.T) {
	violations := (&DatetimeIndexPositionLinter{}).Lint(nil, nil)
	require.Empty(t, violations)
}

func TestDatetimeIndexPositionLinter_OtherDateLikeTypesNotFlagged(t *testing.T) {
	// TIME and YEAR are not range-heavy in the same way and aren't covered.
	sql := `CREATE TABLE schedule (
		id INT PRIMARY KEY,
		duration TIME NOT NULL,
		shift_year YEAR NOT NULL,
		other INT NOT NULL,
		KEY time_other (duration, other),
		KEY year_other (shift_year, other)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := (&DatetimeIndexPositionLinter{}).Lint(nil, stmts)

	require.Empty(t, violations)
}

// --- Linter metadata ---

func TestDatetimeIndexPositionLinter_Name(t *testing.T) {
	linter := &DatetimeIndexPositionLinter{}
	require.Equal(t, "datetime_index_position", linter.Name())
}

func TestDatetimeIndexPositionLinter_Description(t *testing.T) {
	linter := &DatetimeIndexPositionLinter{}
	require.NotEmpty(t, linter.Description())
	require.Contains(t, linter.Description(), "DATETIME")
}

func TestDatetimeIndexPositionLinter_String(t *testing.T) {
	linter := &DatetimeIndexPositionLinter{}
	str := linter.String()
	require.Contains(t, str, "datetime_index_position")
}

func TestDatetimeIndexPositionLinter_ViolationString(t *testing.T) {
	sql := `CREATE TABLE jobs (
		id INT PRIMARY KEY,
		updated_at DATETIME NOT NULL,
		attempt INT NOT NULL,
		KEY updated_at_attempt (updated_at, attempt)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := (&DatetimeIndexPositionLinter{}).Lint(nil, stmts)
	require.Len(t, violations, 1)
	str := violations[0].String()
	require.Contains(t, str, "[WARNING]")
	require.Contains(t, str, "datetime_index_position")
}

// --- Registration ---

func TestDatetimeIndexPositionLinter_Registered(t *testing.T) {
	resetForTest(t)
	Register(&DatetimeIndexPositionLinter{})

	found := slices.Contains(List(), "datetime_index_position")
	require.True(t, found, "datetime_index_position linter should be registered")
}
