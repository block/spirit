package statement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckConstraintsReferenced(t *testing.T) {
	tests := []struct {
		statement string
		expected  []string
	}{
		{"ALTER TABLE t1 DROP CHECK chk_a", []string{"chk_a"}},
		// DROP CONSTRAINT is a distinct clause type, but the name it carries
		// is still a check constraint candidate.
		{"ALTER TABLE t1 DROP CONSTRAINT chk_a", []string{"chk_a"}},
		{"ALTER TABLE t1 ALTER CHECK chk_a NOT ENFORCED", []string{"chk_a"}},
		{"ALTER TABLE t1 DROP CHECK chk_a, ALTER CHECK chk_b ENFORCED", []string{"chk_a", "chk_b"}},
		// Adding a check constraint names nothing that already exists.
		{"ALTER TABLE t1 ADD CONSTRAINT chk_a CHECK (a > 0)", nil},
		{"ALTER TABLE t1 ADD COLUMN b INT", nil},
		{"ALTER TABLE t1 DROP FOREIGN KEY fk_a", nil},
		{"CREATE TABLE t1 (a INT)", nil},
	}
	for _, test := range tests {
		stmts := MustNew(test.statement)
		assert.Equal(t, test.expected, stmts[0].CheckConstraintsReferenced(), test.statement)
	}
}

func TestGenericConstraintDrops(t *testing.T) {
	tests := []struct {
		statement string
		expected  []string
	}{
		// Only DROP CONSTRAINT leaves the constraint type unsaid.
		{"ALTER TABLE t1 DROP CONSTRAINT dup", []string{"dup"}},
		{"ALTER TABLE t1 DROP CONSTRAINT a, DROP CONSTRAINT b", []string{"a", "b"}},
		{"ALTER TABLE t1 DROP CONSTRAINT dup, ENGINE=InnoDB", []string{"dup"}},
		// These say which kind they mean, so MySQL resolves them in one
		// namespace and they can never be ambiguous.
		{"ALTER TABLE t1 DROP CHECK dup", nil},
		{"ALTER TABLE t1 ALTER CHECK dup NOT ENFORCED", nil},
		{"ALTER TABLE t1 DROP FOREIGN KEY dup", nil},
		{"ALTER TABLE t1 DROP KEY dup", nil},
		{"ALTER TABLE t1 ADD CONSTRAINT dup CHECK (a > 0)", nil},
		{"CREATE TABLE t1 (a INT)", nil},
	}
	for _, test := range tests {
		stmts := MustNew(test.statement)
		assert.Equal(t, test.expected, stmts[0].GenericConstraintDrops(), test.statement)
	}
}

func TestAlterWithRenamedCheckConstraints(t *testing.T) {
	// Names as they are on the table the ALTER will actually be applied to,
	// keyed by the name on the table the user named.
	renames := map[string]string{
		"chk_a": "_t1_new_chk_1",
		"chk_b": "_t1_new_chk_2",
	}
	tests := []struct {
		statement string
		expected  string
		unnamed   []string
	}{
		// A dropped constraint is retargeted at the new table's name for it.
		{
			"ALTER TABLE t1 DROP CHECK chk_a",
			"DROP CHECK `_t1_new_chk_1`",
			nil,
		},
		// Matching is case-insensitive, as it is in MySQL.
		{
			"ALTER TABLE t1 DROP CHECK CHK_A",
			"DROP CHECK `_t1_new_chk_1`",
			nil,
		},
		{
			"ALTER TABLE t1 ALTER CHECK chk_b NOT ENFORCED",
			"ALTER CHECK `_t1_new_chk_2` NOT ENFORCED",
			nil,
		},
		// Switching enforcement on is the clause that has to reach the copy
		// algorithm: MySQL supports only ALGORITHM=COPY for it.
		{
			"ALTER TABLE t1 ALTER CHECK chk_b ENFORCED",
			"ALTER CHECK `_t1_new_chk_2` ENFORCED",
			nil,
		},
		// A re-added constraint keeps its enforcement state when it loses its name.
		{
			"ALTER TABLE t1 DROP CHECK chk_a, ADD CONSTRAINT chk_a CHECK (a > 0) NOT ENFORCED",
			"DROP CHECK `_t1_new_chk_1`, ADD CHECK(`a`>0) NOT ENFORCED",
			[]string{"chk_a"},
		},
		// The drop-and-re-add idiom: the re-added constraint loses its symbol,
		// because the table being replaced still holds that name.
		{
			"ALTER TABLE t1 DROP CHECK chk_a, ADD CONSTRAINT chk_a CHECK (a IN ('x','y'))",
			"DROP CHECK `_t1_new_chk_1`, ADD CHECK(`a` IN (_UTF8MB4'x',_UTF8MB4'y')) ENFORCED",
			[]string{"chk_a"},
		},
		// A name this ALTER does not free up is left alone: if it is in use,
		// MySQL rejecting it is the answer the user would get themselves.
		{
			"ALTER TABLE t1 DROP CHECK chk_a, ADD CONSTRAINT chk_b CHECK (b > 0)",
			"DROP CHECK `_t1_new_chk_1`, ADD CONSTRAINT `chk_b` CHECK(`b`>0) ENFORCED",
			nil,
		},
		// A name that is not on the table is left alone too, so that MySQL
		// still reports it as missing.
		{
			"ALTER TABLE t1 DROP CHECK chk_nonexistent",
			"DROP CHECK `chk_nonexistent`",
			nil,
		},
		// DROP CONSTRAINT is retargeted the same way, but keeps its own
		// keyword: rewriting it to DROP CHECK would narrow which constraints
		// MySQL resolves the name against (block/spirit#1183).
		{
			"ALTER TABLE t1 DROP CONSTRAINT chk_a",
			"DROP CONSTRAINT `_t1_new_chk_1`",
			nil,
		},
		{
			"ALTER TABLE t1 DROP CONSTRAINT chk_a, ADD CONSTRAINT chk_a CHECK (a > 0)",
			"DROP CONSTRAINT `_t1_new_chk_1`, ADD CHECK(`a`>0) ENFORCED",
			[]string{"chk_a"},
		},
		// A DROP CONSTRAINT naming something that is not a check constraint is
		// not in the rename map, so it passes through untouched.
		{
			"ALTER TABLE t1 DROP CONSTRAINT uq_name",
			"DROP CONSTRAINT `uq_name`",
			nil,
		},
		// Clauses that have nothing to do with check constraints are untouched,
		// including an added constraint that was never named.
		{
			"ALTER TABLE t1 ADD COLUMN b INT, ADD CHECK (b > 0), ENGINE=InnoDB",
			"ADD COLUMN `b` INT, ADD CHECK(`b`>0) ENFORCED, ENGINE = InnoDB",
			nil,
		},
	}
	for _, test := range tests {
		stmts := MustNew(test.statement)
		alter, unnamed, err := stmts[0].AlterWithRenamedCheckConstraints(renames)
		require.NoError(t, err, test.statement)
		assert.Equal(t, test.expected, alter, test.statement)
		assert.Equal(t, test.unnamed, unnamed, test.statement)
		// Rewriting must not disturb the statement's own ALTER, which the
		// direct-DDL attempts apply to the user's table.
		assert.Equal(t, MustNew(test.statement)[0].Alter, stmts[0].Alter, test.statement)
	}
}

// TestDropConstraintKeywordPreserved pins the clause text spirit sends to
// MySQL. AbstractStatement.Alter is the restored form of the user's ALTER, not
// their original text, so a parser that folded DROP CONSTRAINT into DROP CHECK
// silently rewrote the statement into one MySQL rejects with error 3821 for a
// UNIQUE or FOREIGN KEY constraint (block/spirit#1183).
func TestDropConstraintKeywordPreserved(t *testing.T) {
	tests := []struct {
		statement string
		expected  string
	}{
		{"ALTER TABLE t1 DROP CONSTRAINT uq_name", "DROP CONSTRAINT `uq_name`"},
		{"ALTER TABLE t1 DROP CONSTRAINT fk_ch_p", "DROP CONSTRAINT `fk_ch_p`"},
		{"ALTER TABLE t1 DROP CONSTRAINT chk_age", "DROP CONSTRAINT `chk_age`"},
		{"ALTER TABLE t1 DROP CHECK chk_age", "DROP CHECK `chk_age`"},
		{
			"ALTER TABLE t1 DROP CONSTRAINT a, DROP CHECK b, ADD COLUMN c INT",
			"DROP CONSTRAINT `a`, DROP CHECK `b`, ADD COLUMN `c` INT",
		},
		// ALTER CONSTRAINT and ALTER CHECK are genuine synonyms in MySQL —
		// both apply only to check constraints — so that fold is retained.
		{"ALTER TABLE t1 ALTER CONSTRAINT chk_age NOT ENFORCED", "ALTER CHECK `chk_age` NOT ENFORCED"},
	}
	for _, test := range tests {
		stmts := MustNew(test.statement)
		assert.Equal(t, test.expected, stmts[0].Alter, test.statement)
	}
}

func TestAlterWithRenamedCheckConstraintsNotAlterTable(t *testing.T) {
	stmts := MustNew("CREATE TABLE t1 (a INT)")
	_, _, err := stmts[0].AlterWithRenamedCheckConstraints(nil)
	require.ErrorIs(t, err, ErrNotAlterTable)
}
