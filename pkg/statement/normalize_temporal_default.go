package statement

import (
	"strings"

	"github.com/block/spirit/pkg/utils"
)

func init() { registerNormalizer(temporalDefaultNormalizer{}) }

// temporalDefaultNormalizer rewrites a literal DEFAULT on a DATE, DATETIME,
// TIMESTAMP or TIME column to the value MySQL stores, in the text SHOW CREATE
// TABLE reports: 'YYYY-MM-DD', 'YYYY-MM-DD HH:MM:SS' or '[-]HH:MM:SS', each
// with exactly the column's fractional digits. MySQL accepts a temporal
// literal in many spellings — a date without a time, one-digit fields, a
// two-digit year, any punctuation between fields, a compact digit string, a
// number, a fraction longer than the column keeps — and reports only the
// stored value, so a column declared `datetime DEFAULT '2020-1-1'` reads back
// as `DEFAULT '2020-01-01 00:00:00'`. Without the rule the declared literal
// diffs against the live one forever: every run emits a MODIFY COLUMN that
// stores the same value again. Each reading below was taken from MySQL 8.0.43
// with the default strict sql_mode; the reader itself is [utils.Temporal].
//
//	datetime DEFAULT '2020-1-1'                -> '2020-01-01 00:00:00'
//	datetime DEFAULT '20-01-01T10:00'          -> '2020-01-01 10:00:00'
//	datetime DEFAULT '2020.01.01 10.00.00'     -> '2020-01-01 10:00:00'
//	datetime DEFAULT 20200101                  -> '2020-01-01 00:00:00'
//	datetime DEFAULT 101                       -> '2000-01-01 00:00:00'
//	datetime(3) DEFAULT '2020-01-01 10:00:00'  -> '2020-01-01 10:00:00.000'
//	datetime(3) DEFAULT '... 10:00:00.1234'    -> '2020-01-01 10:00:00.123'
//	datetime DEFAULT '2020-01-01 10:00:00.4'   -> '2020-01-01 10:00:00'
//	date DEFAULT '2020-01-01 23:59:59.4'       -> '2020-01-01'
//	date DEFAULT 20200101                      -> '2020-01-01'
//	time DEFAULT '1:2'                         -> '01:02:00'
//	time DEFAULT '1 2:3:4.4'                   -> '26:03:04'
//	time DEFAULT 100                           -> '00:01:00'
//	time(1) DEFAULT 1.54                       -> '00:00:01.5'
//	time DEFAULT '-0:00:00.4'                  -> '00:00:00'
//
// A fraction past the column's precision is read only where MySQL's two ways
// of shortening it agree. Under the default sql_mode MySQL rounds it half up,
// in two steps: the digit past the microseconds rounds them first (a DATETIME
// string from the seventh digit, a TIME string from the last digit written),
// then the microseconds round to the precision, and the carry runs through
// the seconds, minutes, hours and date. With TIME_TRUNCATE_FRACTIONAL in the
// session's sql_mode MySQL truncates instead: '12:34:56.9' on a time is
// '12:34:57' under one mode and '12:34:56' under the other. The rule cannot
// see the session of the CREATE, so a literal the two readings disagree on
// is left as written; reading it under an assumed mode would make a schema
// compare equal to a live value it may not have. The literal then keeps
// diffing against the live table, and the MODIFY the diff emits carries it as
// written (Column.DefaultAsWritten), so MySQL stores under the session's own
// mode what the CREATE did. A literal the two readings agree on, one whose
// extra digits round down ('10:00:00.4', '.1234' on a datetime(3),
// '10:00:00.1234564999' on a datetime(6), where the seventh digit decides),
// is read.
//
// The result is recorded as a [DefaultKindString], the form SHOW CREATE TABLE
// reports, so a declared number compares equal to the live string. It is the
// value Diff compares, not the one it emits.
//
// Left alone, so that the diff keeps emitting the literal as written:
//
//   - a fraction past the column's precision that MySQL rounds under the
//     default sql_mode and truncates under TIME_TRUNCATE_FRACTIONAL (above):
//     '12:34:56.9' on a time, '1.55' on a time(1), '23:59:59.5' on a
//     datetime or a date. Every carry into year 0000 is one of these, and
//     MySQL does not even carry it under the default mode: it stores the zero
//     date ('0000-12-09 23:59:59.5' is '0000-00-00 00:00:00').
//   - a value MySQL rejects (an invalid date, a zero month or day, a field
//     out of range, a carry past 9999-12-31 or 838:59:59), so the MODIFY
//     fails the way it would have anyway.
//   - a literal with a time-zone suffix ('2020-01-01 10:00:00+00:00'), which
//     MySQL converts to the session time zone: no fixed text reproduces it.
//   - a float literal (`1e2`), which MySQL reads through a double, rounding a
//     long fraction differently from the decimal reading.
//   - the spellings MySQL reads by rules not worth reproducing: a compact
//     digit string of a width other than 6, 8, 12 or 14 ('10101' is
//     2010-10-01), a thirteen-digit number (a year 100-999 datetime), a TIME
//     string that starts with a colon or that MySQL reads as a DATETIME first
//     (twelve or more digits, or a date: '1999-12-29 12:00' stores
//     '12:00:00'), and a number MySQL reads as a DATETIME on a TIME column.
//   - a hex or bit literal, TRUE/FALSE, an expression default and NULL.
//
// TIMESTAMP has one more wrinkle this rule cannot remove: MySQL converts a
// TIMESTAMP default to UTC with the session time zone of the CREATE and
// reports it in the session time zone of the SHOW CREATE TABLE, so the live
// text depends on both sessions. The rule canonicalizes the spelling only.
type temporalDefaultNormalizer struct{}

func (temporalDefaultNormalizer) Name() string { return "temporal-default" }

func (temporalDefaultNormalizer) Normalize(ct *CreateTable) *CreateTable {
	for i := range ct.Columns {
		c := &ct.Columns[i]
		if c.Default == nil || c.DefaultIsExpr {
			continue
		}
		if c.DefaultKind != DefaultKindString && c.DefaultKind != DefaultKindNumber {
			continue
		}
		stored, ok := temporalDefaultText(c, strings.ToLower(c.Type))
		if !ok {
			continue
		}
		c.Default, c.DefaultKind = &stored, DefaultKindString
	}
	return ct
}

// temporalDefaultText returns the text SHOW CREATE TABLE reports for a
// temporal column's literal default, or false where the default is not one
// this rule converts: an unreadable literal, or one whose fraction MySQL
// rounds under one sql_mode and truncates under another. See
// [temporalDefaultNormalizer].
func temporalDefaultText(c *Column, typ string) (string, bool) {
	text := *c.Default
	number := c.DefaultKind == DefaultKindNumber
	if number && strings.ContainsAny(text, "eE") {
		return "", false // a float literal is read through a double
	}
	fsp := 0
	if c.Length != nil {
		fsp = *c.Length
	}
	switch typ {
	case "date", "datetime", "timestamp":
		var value utils.Temporal
		var ok bool
		if number {
			value, ok = utils.ParseDateTimeNumber(text)
		} else {
			value, ok = utils.ParseDateTimeString(text)
		}
		if !ok {
			return "", false
		}
		if typ == "date" {
			// A DATE keeps the date the time part rounds to, or the date
			// as written under truncation; read only where that is one date.
			rounded, ok := value.RoundDateTime(0)
			if !ok || rounded.DateString() != value.Truncate(0).DateString() {
				return "", false
			}
			return rounded.DateString(), true
		}
		rounded, ok := value.RoundDateTime(fsp)
		if !ok || rounded.DateTimeString(fsp) != value.Truncate(fsp).DateTimeString(fsp) {
			return "", false
		}
		return rounded.DateTimeString(fsp), true
	case "time":
		var value utils.Temporal
		var ok bool
		if number {
			value, ok = utils.ParseTimeNumber(text)
		} else {
			value, ok = utils.ParseTimeString(text)
		}
		if !ok {
			return "", false
		}
		rounded, ok := value.RoundTime(fsp)
		if !ok || rounded.TimeString(fsp) != value.Truncate(fsp).TimeString(fsp) {
			return "", false
		}
		return rounded.TimeString(fsp), true
	}
	return "", false
}
