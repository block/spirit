package statement

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/parser/ast"
)

func init() { registerNormalizer(primaryKeyNotNullNormalizer{}) }

// primaryKeyNotNullNormalizer marks every PRIMARY KEY column NOT NULL, which is
// what MySQL stores: a column in the primary key is implicitly NOT NULL, so
// `a INT, PRIMARY KEY (a)` is reported by SHOW CREATE TABLE as
// `a int NOT NULL`. The parser applies this only to the inline spelling
// (`a INT PRIMARY KEY`); without this rule a table-level key column that omits
// NOT NULL parses as nullable, and diffing it against the live table emits a
// MODIFY COLUMN ... NULL that MySQL rejects (error 1171).
//
// The promotion is implicit only. A key column that explicitly declares NULL
// or DEFAULT NULL is one MySQL refuses to create (error 1171), so it is left
// nullable, whichever way the key is spelled, rather than silently accepted.
// Diff and DeclarativeToImperative reject a target schema in that state.
//
// It reads both the table-level PRIMARY KEY index and the inline PrimaryKey
// flag, so it is order-independent with respect to primaryKeyNormalizer.
type primaryKeyNotNullNormalizer struct{}

func (primaryKeyNotNullNormalizer) Name() string { return "primary-key-not-null" }

func (primaryKeyNotNullNormalizer) Normalize(ct *CreateTable) *CreateTable {
	pkColumns := primaryKeyColumnSet(ct)
	for i := range ct.Columns {
		col := &ct.Columns[i]
		if pkColumns[strings.ToLower(col.Name)] {
			col.Nullable = col.declaresNull()
		}
	}
	return ct
}

// primaryKeyColumnSet returns the lowercased names of the columns in ct's
// primary key, from the table-level index or an inline PrimaryKey flag.
func primaryKeyColumnSet(ct *CreateTable) map[string]bool {
	set := make(map[string]bool)
	if pk := ct.getPrimaryKeyIndex(); pk != nil {
		for _, c := range pk.Columns {
			set[strings.ToLower(c)] = true
		}
	}
	for _, col := range ct.Columns {
		if col.PrimaryKey {
			set[strings.ToLower(col.Name)] = true
		}
	}
	return set
}

// declaresNull reports whether the column definition explicitly permits NULL,
// with a NULL attribute or a literal DEFAULT NULL. It reads the AST because
// Nullable cannot tell an explicit NULL apart from an omitted NOT NULL. A
// column built without a Raw definition declares nothing.
//
// Any NULL attribute counts, even one followed by NOT NULL: MySQL rejects
// `a INT NULL NOT NULL` in a primary key rather than letting the last attribute
// win. An expression default, DEFAULT (NULL), does not count: MySQL accepts it
// on a key column and stores the column NOT NULL.
func (c *Column) declaresNull() bool {
	if c.Raw == nil {
		return false
	}
	for _, opt := range c.Raw.Options {
		switch opt.Tp { //nolint:exhaustive
		case ast.ColumnOptionNull:
			return true
		case ast.ColumnOptionDefaultValue:
			if v, ok := opt.Expr.(*ast.ValueExpr); ok && v.Kind() == ast.KindNull {
				return true
			}
		}
	}
	return false
}

// checkPrimaryKeyNullability rejects a table whose primary key column
// explicitly declares NULL or DEFAULT NULL, which MySQL refuses to create.
// Such a column is left nullable by primaryKeyNotNullNormalizer, so a diff
// toward it would emit a MODIFY ... NULL that MySQL rejects too; failing
// early reports it at plan time instead.
func checkPrimaryKeyNullability(ct *CreateTable) error {
	pkColumns := primaryKeyColumnSet(ct)
	for i := range ct.Columns {
		col := &ct.Columns[i]
		if pkColumns[strings.ToLower(col.Name)] && col.declaresNull() {
			return fmt.Errorf("column %q is part of the PRIMARY KEY but declares NULL, which MySQL rejects; remove NULL or DEFAULT NULL from its definition, or use a UNIQUE key if it must allow NULL", col.Name)
		}
	}
	return nil
}
