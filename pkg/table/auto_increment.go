package table

import (
	"fmt"
	"slices"
	"strings"

	"github.com/block/spirit/pkg/parser"
	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/parser/format"
)

// StripAutoIncrement removes the table-level AUTO_INCREMENT=N counter from a
// CREATE TABLE statement and returns every other byte exactly as it was given.
// The counter records where one instance's sequence happens to sit, so it does
// not belong in DDL kept as an artifact: a schema file, a stored snapshot,
// anything a tool writes back or a human reads.
//
// Reach for this only when the DDL text is the output. To compare two schemas,
// use statement.Diff, which ignores the counter by default
// (DiffOptions.IgnoreAutoIncrement). To canonicalize a schema file against a
// live server, use pkg/fmt, which resets the counter in MySQL and reads the
// table back rather than editing the text at all.
//
// This is deliberately not a format.RestoreFlag. A restore flag is
// serialization policy: it decides how the parse tree gets written out, and
// everything it touches goes through Restore, which re-renders the whole
// statement. Re-rendering is the bug this function exists to avoid. Keeping the
// bytes around the removed option means editing the source text, which only a
// caller holding that text can do.
//
// Only the table option is removed. The column-level AUTO_INCREMENT attribute is
// preserved: dropping it would produce a table whose ids no longer generate. A
// statement carrying no counter is returned untouched, so the word appearing in
// a column default, a comment or an identifier is never rewritten.
//
// The parser says where the counter is and how far it runs. It records the
// option's span while it reads the statement, so nothing here searches the text
// or reads any SQL: the keyword inside a COMMENT, a column default or a quoted
// identifier is not in that span, and neither is any of the punctuation the
// option is allowed to carry — a FORCE prefix, spacing around the `=`, a comment
// between the `=` and the digits. The removal is a slice at two offsets the
// parser vouched for.
//
// A statement that cannot be parsed, or whose counter cannot be removed cleanly,
// is an error rather than a passthrough: returning the input would leak the
// counter into the artifact.
func StripAutoIncrement(stmt string) (string, error) {
	create, err := parseCreateTable(stmt)
	if err != nil {
		return "", fmt.Errorf("parse CREATE TABLE to strip its auto-increment counter: %w", err)
	}
	i := slices.IndexFunc(create.Options, isAutoIncrementOption)
	if i < 0 {
		return stmt, nil
	}
	start, end, ok := counterSpan(stmt, create.Options[i])
	if !ok {
		return "", fmt.Errorf("locate the auto-increment counter in CREATE TABLE %s: the parser places it at offset %d, where the statement does not hold it", create.Table.Name.O, create.Options[i].OriginTextPosition())
	}
	stripped := stmt[:trimSpacingBefore(stmt, start)] + stmt[end:]
	if err := verifyOnlyCounterRemoved(create, stripped); err != nil {
		return "", fmt.Errorf("strip the auto-increment counter from CREATE TABLE %s: %w", create.Table.Name.O, err)
	}
	return stripped, nil
}

// counterSpan returns the half-open range the option occupies in stmt. Both ends
// come from the parser, which recorded them against this same text, so ok is
// false only if the two have been allowed to drift apart — a parser bug, or a
// caller passing an option parsed from some other statement. Saying so beats
// slicing out of range.
func counterSpan(stmt string, opt *ast.TableOption) (start, end int, ok bool) {
	start, text := opt.OriginTextPosition(), opt.OriginalText()
	end = start + len(text)
	if start < 0 || end > len(stmt) || stmt[start:end] != text {
		return 0, 0, false
	}
	return start, end, true
}

// verifyOnlyCounterRemoved reports whether stripped is the parsed original with
// its counter dropped and nothing else touched. Both sides are compared in the
// parser's canonical form, so the check is on the statement's meaning rather
// than on the bytes the removal happened to leave behind. original is consumed:
// its counter option is deleted in place to build the expected form.
func verifyOnlyCounterRemoved(original *ast.CreateTableStmt, stripped string) error {
	strippedCreate, err := parseCreateTable(stripped)
	if err != nil {
		return fmt.Errorf("re-parse the stripped statement: %w", err)
	}
	original.Options = slices.DeleteFunc(original.Options, isAutoIncrementOption)
	want, err := restoreCreateTable(original)
	if err != nil {
		return fmt.Errorf("restore the parsed statement without its counter: %w", err)
	}
	got, err := restoreCreateTable(strippedCreate)
	if err != nil {
		return fmt.Errorf("restore the stripped statement: %w", err)
	}
	if want != got {
		return fmt.Errorf("removing the counter changed the rest of the statement")
	}
	return nil
}

func parseCreateTable(stmt string) (*ast.CreateTableStmt, error) {
	node, err := parser.New().ParseOneStmt(stmt, "", "")
	if err != nil {
		return nil, err
	}
	create, ok := node.(*ast.CreateTableStmt)
	if !ok {
		return nil, fmt.Errorf("statement is a %T, not a CREATE TABLE", node)
	}
	return create, nil
}

func isAutoIncrementOption(opt *ast.TableOption) bool {
	return opt != nil && opt.Tp == ast.TableOptionAutoIncrement
}

func restoreCreateTable(stmt *ast.CreateTableStmt) (string, error) {
	var sb strings.Builder
	if err := stmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
		return "", err
	}
	return sb.String(), nil
}

// trimSpacingBefore walks back over the blanks preceding i so the removal takes
// the separator with the option. Line breaks are left in place to preserve the
// statement's layout.
func trimSpacingBefore(s string, i int) int {
	for i > 0 && (s[i-1] == ' ' || s[i-1] == '\t') {
		i--
	}
	return i
}
