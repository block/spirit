package statement

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
)

// DiffCreateTables compares two CREATE TABLE statements and returns a runnable
// ALTER TABLE statement describing how they differ, or an empty string if they
// are equivalent under opts.
//
// The comparison is performed by parsing both statements and diffing the
// structured form via CreateTable.Diff, so it is insensitive to the textual
// noise two servers can put in SHOW CREATE TABLE output. What is and isn't
// compared is controlled by opts; with NewDiffOptions the comparison:
//   - ignores AUTO_INCREMENT counter values (instance-specific noise),
//   - ignores ENGINE and ROW_FORMAT cosmetic defaults,
//   - DOES compare column types, nullability, defaults, and per-column /
//     per-table CHARACTER SET and COLLATE,
//   - DOES compare indexes (including the primary key) and constraints.
//
// "want" is the schema treated as the source of truth; "got" is the schema
// being validated against it. The returned statement describes the
// transformation that would turn "got" into "want", which is what makes the
// message actionable. "table" is the real (logical) table name used to build
// the runnable "ALTER TABLE <table>" prefix, escaped so identifiers containing
// backticks remain valid — the two CREATE TABLE statements themselves may name
// tables on different instances, so their names are normalized away before the
// diff and never compared.
//
// CreateTable.Diff deliberately splits some reconciliations across more than
// one ALTER — a partition-type change, or an option-only index change that
// MySQL would no-op if its DROP and ADD shared a statement. Those are emitted
// as separate semicolon-separated ALTERs, in order, rather than merged into one
// (which would produce SQL that silently does the wrong thing).
//
// If opts is nil, NewDiffOptions() defaults are used.
func DiffCreateTables(table, wantCreate, gotCreate string, opts *DiffOptions) (string, error) {
	want, err := ParseCreateTable(wantCreate)
	if err != nil {
		return "", fmt.Errorf("failed to parse reference CREATE TABLE: %w", err)
	}
	got, err := ParseCreateTable(gotCreate)
	if err != nil {
		return "", fmt.Errorf("failed to parse CREATE TABLE under validation: %w", err)
	}
	// Diff requires both tables to have the same name. The two CREATE TABLE
	// statements come from tables with the same logical name on different
	// instances, but rewrite both names to a fixed token so the comparison is
	// purely structural and never trips on the name guard.
	want.TableName = "t"
	got.TableName = "t"

	// Diff(got -> want): the returned clauses are the ALTER that would morph the
	// validated schema ("got") into the reference schema ("want"). If nil, the
	// two schemas are equivalent under the canonicalization rules above.
	stmts, err := got.Diff(want, opts)
	if err != nil {
		return "", fmt.Errorf("failed to diff CREATE TABLE statements: %w", err)
	}
	if len(stmts) == 0 {
		return "", nil
	}
	// Prefix each statement with an escaped "ALTER TABLE <table>" so the output
	// is directly runnable. Diff already grouped the clauses that may share an
	// ALTER, so preserve its statement boundaries rather than flattening them.
	prefix, err := sqlescape.EscapeSQL("ALTER TABLE %n ", table)
	if err != nil {
		return "", err
	}
	alters := make([]string, 0, len(stmts))
	for _, s := range stmts {
		if s.Alter != "" {
			alters = append(alters, prefix+s.Alter)
		}
	}
	if len(alters) == 0 {
		return "", nil
	}
	return strings.Join(alters, "; "), nil
}
