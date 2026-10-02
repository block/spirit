package utils

import (
	"fmt"
	"math"
	"strings"
	"time"
)

// Temporal is a MySQL DATETIME, DATE or TIME value as MySQL reads it from a
// literal: its fields, its microseconds, and the digit MySQL keeps past the
// microseconds to round them with. Build one with [ParseDateTimeString],
// [ParseDateTimeNumber], [ParseTimeString] or [ParseTimeNumber]; round it to
// the column's fractional-seconds precision with [Temporal.RoundDateTime] or
// [Temporal.RoundTime], or truncate it with [Temporal.Truncate]; render it
// with [Temporal.DateTimeString], [Temporal.DateString] or
// [Temporal.TimeString].
//
// The parsers accept the spellings MySQL 8.0 accepts and reads unambiguously,
// and reject (return false for) everything else, including the spellings MySQL
// rejects. A caller canonicalizing a literal can therefore leave any rejected
// text as written: it is either invalid, which MySQL reports the same way in
// any spelling, or a form rare enough not to be worth modelling.
type Temporal struct {
	Negative                    bool // TIME only
	Year, Month, Day            int  // DATETIME and DATE; zero for TIME
	Hour, Minute, Second, Micro int  // Hour runs to 838 for TIME
	// nanos is the nanosecond part MySQL rounds into Micro before it rounds
	// to the column's precision, always a multiple of 100 (one digit).
	nanos int
}

const (
	maxTimeHours    = 838
	maxTimeHHMMSS   = 8385959
	microsPerSecond = 1_000_000
	// yearCutoff is the two-digit year MySQL splits centuries at: 00-69 are
	// 2000-2069 and 70-99 are 1970-1999.
	yearCutoff = 70
	// temporalSpace is what MySQL skips around a temporal literal (isspace in
	// its latin1 character set).
	temporalSpace = " \t\n\r\f\v"
)

// ParseDateTimeString reads a DATETIME, TIMESTAMP or DATE string literal:
// 'YYYY-MM-DD[ HH:MM[:SS[.fraction]]]' with any punctuation between the date
// fields and between the time fields, spaces, a 'T' or punctuation between
// the date and the time, fields of any width, a trailing separator, and a
// two-digit year resolved to 1970-2069; or the compact 'YYMMDD', 'YYYYMMDD',
// 'YYMMDDHHMMSS', 'YYYYMMDDHHMMSS' and 'YYMMDDTHHMMSS' / 'YYYYMMDDTHHMMSS',
// the last four with an optional fraction. Whitespace around the value is
// skipped. The fields are range-checked (month, day for the month and year,
// hour, minute, second), so a date MySQL rejects is false. The time fields
// are zero when absent; the caller decides whether to keep them.
//
// A time-zone suffix ('2020-01-01 10:00:00+00:00') is false: MySQL converts
// the value to the session time zone, so no fixed text reproduces it.
func ParseDateTimeString(text string) (Temporal, bool) {
	text = strings.Trim(text, temporalSpace)
	if text == "" {
		return Temporal{}, false
	}
	body, fraction, hasFraction := strings.Cut(text, ".")
	if hasFraction && !allDigits(fraction) {
		// The dot was a field separator ('2020.01.01'), not a fraction.
		body, fraction, hasFraction = text, "", false
	}
	datePart, timePart, hasT := strings.Cut(body, "T")
	if datePart != "" && allDigits(datePart) && (!hasT || allDigits(timePart)) {
		return parseCompactDateTime(datePart, timePart, hasT, fraction, hasFraction)
	}
	return parseDelimitedDateTime(text)
}

// parseCompactDateTime reads the digit-only spellings. MySQL reads any run of
// digits by a year-length rule that also admits odd widths ('10101' is
// 2010-10-01); only the documented widths are read here.
func parseCompactDateTime(datePart, timePart string, hasT bool, fraction string, hasFraction bool) (Temporal, bool) {
	var fields string
	switch {
	case hasT:
		if (len(datePart) != 6 && len(datePart) != 8) || len(timePart) != 6 {
			return Temporal{}, false
		}
		fields = datePart + timePart
	case len(datePart) == 6 || len(datePart) == 8:
		if hasFraction {
			return Temporal{}, false
		}
		fields = datePart + "000000"
	case len(datePart) == 12 || len(datePart) == 14:
		fields = datePart
	default:
		return Temporal{}, false
	}
	return temporalFromDigits(fields, fraction)
}

// temporalFromDigits reads YYMMDDhhmmss or YYYYMMDDhhmmss digits, with the
// fraction's digits as MySQL keeps them (six, and the seventh to round with).
func temporalFromDigits(fields, fraction string) (Temporal, bool) {
	yearWidth := len(fields) - 10
	year := atoi(fields[:yearWidth])
	if yearWidth == 2 {
		year = expandTwoDigitYear(year)
	}
	rest := fields[yearWidth:]
	t := Temporal{
		Year: year, Month: atoi(rest[0:2]), Day: atoi(rest[2:4]),
		Hour: atoi(rest[4:6]), Minute: atoi(rest[6:8]), Second: atoi(rest[8:10]),
	}
	t.Micro, t.nanos = readFraction(fraction, false)
	if !t.validDateTime() {
		return Temporal{}, false
	}
	return t, true
}

// parseDelimitedDateTime reads the separated spelling: three date fields, up
// to three time fields, and an optional fraction after the seconds.
func parseDelimitedDateTime(text string) (Temporal, bool) {
	runs := splitRuns(text)
	// Digits first, then alternating separator and digits.
	if len(runs) == 0 || !runs[0].digits {
		return Temporal{}, false
	}
	var fields, seps []string
	for i, r := range runs {
		if r.digits != (i%2 == 0) {
			return Temporal{}, false
		}
		if r.digits {
			fields = append(fields, r.text)
		} else {
			seps = append(seps, r.text)
		}
	}
	// MySQL accepts a trailing separator after the day, hour or minute
	// ('2020-01-01-', '2020-01-01 10:'), and after the seconds only the dot of
	// an empty fraction ('10:00:00.'); anything else there is trailing garbage.
	fraction := ""
	if len(seps) == len(fields) {
		last := seps[len(seps)-1]
		switch {
		case len(fields) == 6 && last == ".":
		case len(fields) >= 3 && len(fields) <= 5 && isPunctuation(last):
		default:
			return Temporal{}, false
		}
		seps = seps[:len(seps)-1]
	}
	if len(fields) == 7 {
		if seps[5] != "." {
			return Temporal{}, false
		}
		fraction, fields, seps = fields[6], fields[:6], seps[:5]
	}
	if len(fields) < 3 || len(fields) > 6 {
		return Temporal{}, false
	}
	for i, sep := range seps {
		if i == 2 {
			// Between the date and the time: any mix of spaces and punctuation,
			// or a single 'T' directly after the day.
			if sep == "T" || isPunctuationOrSpace(sep) {
				continue
			}
			return Temporal{}, false
		}
		if !isPunctuation(sep) {
			return Temporal{}, false
		}
	}
	year := atoi(fields[0])
	if len(fields[0]) == 2 {
		year = expandTwoDigitYear(year)
	}
	t := Temporal{Year: year, Month: atoi(fields[1]), Day: atoi(fields[2])}
	if len(fields) > 3 {
		t.Hour = atoi(fields[3])
	}
	if len(fields) > 4 {
		t.Minute = atoi(fields[4])
	}
	if len(fields) > 5 {
		t.Second = atoi(fields[5])
	}
	t.Micro, t.nanos = readFraction(fraction, false)
	if !t.validDateTime() {
		return Temporal{}, false
	}
	return t, true
}

// ParseDateTimeNumber reads a DATETIME, TIMESTAMP or DATE numeric literal the
// way MySQL does (number_to_datetime): the integer part is YYMMDD,
// YYYYMMDD, YYMMDDHHMMSS or YYYYMMDDHHMMSS by its magnitude, so 10101 is
// 2001-01-01 and 9991231 is 0999-12-31; a two-digit year resolves to
// 1970-2069. A fraction is read only after the fourteen-digit form. Zero,
// which MySQL rejects as a default, is false, and so is a thirteen-digit
// number (a year 100-999 datetime), which is left to the caller.
func ParseDateTimeNumber(text string) (Temporal, bool) {
	digits, fraction, hasFraction := strings.Cut(strings.TrimPrefix(text, "+"), ".")
	if digits == "" || !allDigits(digits) || (hasFraction && !allDigits(fraction)) {
		return Temporal{}, false
	}
	digits = strings.TrimLeft(digits, "0")
	var fields string
	switch n := len(digits); {
	case n == 0:
		return Temporal{}, false
	case n <= 6:
		fields = strings.Repeat("0", 6-n) + digits + "000000"
	case n <= 8:
		fields = strings.Repeat("0", 8-n) + digits + "000000"
	case n <= 12:
		fields = strings.Repeat("0", 12-n) + digits
	case n == 14:
		fields = digits
	default:
		return Temporal{}, false
	}
	if hasFraction && len(digits) != 14 {
		return Temporal{}, false
	}
	return temporalFromDigits(fields, fraction)
}

// ParseTimeString reads a TIME string literal: '[-][D ]HH:MM[:SS][.fraction]'
// with fields of any width and the day part separated by spaces or tabs, or
// '[-]HHMMSS[.fraction]' read right to left as seconds, minutes and hours.
// Whitespace around the value is skipped. Hours (days × 24 + hours) run to
// 838 and minutes and seconds to 59; the maximum 838:59:59 admits no
// fraction. A fraction past six digits is rounded from its last digit, which
// is how MySQL's TIME reader (unlike its DATETIME reader) carries the extra
// digits.
//
// MySQL first tries a string of twelve or more characters as a DATETIME and
// keeps its time part ('2020-01-01 10:00:00' is '10:00:00'); such strings are
// false here unless they cannot be a date, so the caller leaves them alone.
func ParseTimeString(text string) (Temporal, bool) {
	text = strings.Trim(text, temporalSpace)
	t := Temporal{Negative: strings.HasPrefix(text, "-")}
	if t.Negative {
		text = text[1:]
	}
	body, fraction, hasFraction := strings.Cut(text, ".")
	if hasFraction && !allDigits(fraction) {
		return Temporal{}, false
	}
	days := 0
	if i := strings.IndexAny(body, " \t"); i >= 0 {
		dayText := body[:i]
		body = strings.TrimLeft(body[i:], " \t")
		if dayText == "" || !allDigits(dayText) || !strings.Contains(body, ":") {
			return Temporal{}, false
		}
		days = atoi(dayText)
	}
	switch parts := strings.Split(body, ":"); {
	case len(parts) == 1:
		if body == "" {
			// '.5' and '.' are zero with a fraction.
			if !hasFraction {
				return Temporal{}, false
			}
		} else if (len(body) >= 12 && strings.Trim(body, "0") != "") || !t.setHHMMSS(body) {
			// Twelve or more digits read as a DATETIME first (see above).
			return Temporal{}, false
		}
	case len(parts) == 2 || len(parts) == 3:
		for _, part := range parts {
			if part == "" || !allDigits(part) {
				return Temporal{}, false
			}
		}
		t.Hour, t.Minute = atoi(parts[0]), atoi(parts[1])
		if len(parts) == 3 {
			t.Second = atoi(parts[2])
		}
	default:
		return Temporal{}, false
	}
	t.Hour += days * 24
	t.Micro, t.nanos = readFraction(fraction, true)
	if !t.validTime() {
		return Temporal{}, false
	}
	return t, true
}

// ParseTimeNumber reads a TIME numeric literal the way MySQL does
// (number_to_time): the integer part is HHMMSS read right to left, up to
// 838:59:59, and the fraction is seconds.
func ParseTimeNumber(text string) (Temporal, bool) {
	t := Temporal{Negative: strings.HasPrefix(text, "-")}
	if t.Negative {
		text = text[1:]
	}
	body, fraction, hasFraction := strings.Cut(strings.TrimPrefix(text, "+"), ".")
	if body == "" || (hasFraction && !allDigits(fraction)) || !t.setHHMMSS(body) {
		return Temporal{}, false
	}
	t.Micro, t.nanos = readFraction(fraction, false)
	if !t.validTime() {
		return Temporal{}, false
	}
	return t, true
}

// setHHMMSS reads a run of digits right to left as seconds, minutes and
// hours. MySQL bounds the number at 8385959 before splitting it.
func (t *Temporal) setHHMMSS(digits string) bool {
	if digits == "" || !allDigits(digits) {
		return false
	}
	v := atoi(digits)
	if v > maxTimeHHMMSS {
		return false
	}
	t.Hour, t.Minute, t.Second = v/10000, v/100%100, v%100
	return true
}

// readFraction returns the microseconds a fraction's digits spell and the
// digit MySQL rounds them with: the seventh digit, or for a TIME string the
// last digit written (lastDigitRounds).
func readFraction(fraction string, lastDigitRounds bool) (micro, nanos int) {
	if fraction == "" {
		return 0, 0
	}
	kept := fraction
	if len(kept) > 6 {
		kept = kept[:6]
	}
	micro = atoi(kept) * pow10(6-len(kept))
	if len(fraction) > 6 {
		roundingDigit := fraction[6]
		if lastDigitRounds {
			roundingDigit = fraction[len(fraction)-1]
		}
		nanos = 100 * int(roundingDigit-'0')
	}
	return micro, nanos
}

func (t Temporal) validDateTime() bool {
	return t.Year >= 0 && t.Year <= 9999 && t.Month >= 1 && t.Month <= 12 &&
		t.Day >= 1 && t.Day <= daysInMonth(t.Year, t.Month) &&
		t.Hour <= 23 && t.Minute <= 59 && t.Second <= 59
}

func (t Temporal) validTime() bool {
	if t.Hour > maxTimeHours || t.Minute > 59 || t.Second > 59 {
		return false
	}
	atMax := t.Hour == maxTimeHours && t.Minute == 59 && t.Second == 59
	return !atMax || (t.Micro == 0 && t.nanos == 0)
}

// RoundDateTime rounds the value to fsp fractional digits the way MySQL
// stores it under its default sql_mode: the digit past the microseconds
// rounds them first, then the microseconds round half up to fsp, each
// carrying into the seconds and on through the date. It returns false when
// the carry runs past 9999-12-31, and when it happens in year 0000, where
// MySQL does not carry the date but stores the zero date ('0000-12-09
// 23:59:59.5' is '0000-00-00 00:00:00', '0000-06-15 10:00:00.5' is
// '0000-00-00 10:00:01'), a value this reader does not produce.
func (t Temporal) RoundDateTime(fsp int) (Temporal, bool) {
	micro, carry := roundMicro(t.Micro, t.nanos, fsp)
	t.Micro, t.nanos = micro, 0
	if carry {
		if t.Year == 0 {
			return Temporal{}, false
		}
		tm := time.Date(t.Year, time.Month(t.Month), t.Day, t.Hour, t.Minute, t.Second+1, 0, time.UTC)
		if tm.Year() > 9999 {
			return Temporal{}, false
		}
		t.Year, t.Month, t.Day = tm.Year(), int(tm.Month()), tm.Day()
		t.Hour, t.Minute, t.Second = tm.Hour(), tm.Minute(), tm.Second()
	}
	return t, true
}

// RoundTime is [Temporal.RoundDateTime] for a TIME value: the carry runs into
// the hours, and the result is false past 838:59:59.
func (t Temporal) RoundTime(fsp int) (Temporal, bool) {
	micro, carry := roundMicro(t.Micro, t.nanos, fsp)
	t.Micro, t.nanos = micro, 0
	if carry {
		seconds := t.Hour*3600 + t.Minute*60 + t.Second + 1
		t.Hour, t.Minute, t.Second = seconds/3600, seconds/60%60, seconds%60
		if !t.validTime() {
			return Temporal{}, false
		}
	}
	return t, true
}

// Truncate drops the fraction past fsp digits the way MySQL stores the value
// with TIME_TRUNCATE_FRACTIONAL in its sql_mode, for a DATETIME, DATE or TIME
// alike: nothing rounds and nothing carries (a negative TIME truncates toward
// zero), so the result is always valid.
func (t Temporal) Truncate(fsp int) Temporal {
	if fsp < 0 {
		fsp = 0
	}
	if fsp < 6 {
		t.Micro -= t.Micro % pow10(6-fsp)
	}
	t.nanos = 0
	return t
}

// roundMicro applies MySQL's two rounding steps and reports a carry into the
// seconds.
func roundMicro(micro, nanos, fsp int) (int, bool) {
	if nanos >= 500 {
		micro++
	}
	if fsp < 0 {
		fsp = 0
	}
	if fsp < 6 {
		unit := pow10(6 - fsp)
		rem := micro % unit
		micro -= rem
		if rem*2 >= unit {
			micro += unit
		}
	}
	if micro >= microsPerSecond {
		return micro - microsPerSecond, true
	}
	return micro, false
}

// DateTimeString renders 'YYYY-MM-DD HH:MM:SS' with fsp fraction digits.
func (t Temporal) DateTimeString(fsp int) string {
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d%s", t.Year, t.Month, t.Day, t.Hour, t.Minute, t.Second, fractionString(t.Micro, fsp))
}

// DateString renders 'YYYY-MM-DD'.
func (t Temporal) DateString() string {
	return fmt.Sprintf("%04d-%02d-%02d", t.Year, t.Month, t.Day)
}

// TimeString renders '[-]HH:MM:SS' with fsp fraction digits; hours take as
// many digits as they need, and a zero value carries no sign.
func (t Temporal) TimeString(fsp int) string {
	sign := ""
	if t.Negative && (t.Hour != 0 || t.Minute != 0 || t.Second != 0 || t.Micro != 0) {
		sign = "-"
	}
	return fmt.Sprintf("%s%02d:%02d:%02d%s", sign, t.Hour, t.Minute, t.Second, fractionString(t.Micro, fsp))
}

func fractionString(micro, fsp int) string {
	if fsp <= 0 {
		return ""
	}
	if fsp > 6 {
		fsp = 6
	}
	return "." + fmt.Sprintf("%06d", micro)[:fsp]
}

func expandTwoDigitYear(year int) int {
	if year < yearCutoff {
		return 2000 + year
	}
	return 1900 + year
}

func daysInMonth(year, month int) int {
	switch month {
	case 2:
		if year%4 == 0 && (year%100 != 0 || year%400 == 0) {
			return 29
		}
		return 28
	case 4, 6, 9, 11:
		return 30
	default:
		return 31
	}
}

// run is a maximal run of digits or of non-digits in a temporal string.
type run struct {
	text   string
	digits bool
}

func splitRuns(text string) []run {
	var runs []run
	start := 0
	for i := 1; i <= len(text); i++ {
		if i == len(text) || isDigit(text[i]) != isDigit(text[start]) {
			runs = append(runs, run{text: text[start:i], digits: isDigit(text[start])})
			start = i
		}
	}
	return runs
}

// isPunctuation reports whether a separator run is ASCII punctuation only;
// isPunctuationOrSpace also admits whitespace. MySQL takes any run of
// punctuation between two fields, and spaces as well between the date and
// the time.
func isPunctuation(sep string) bool {
	return sep != "" && strings.IndexFunc(sep, func(r rune) bool { return !isPunct(r) }) < 0
}

func isPunctuationOrSpace(sep string) bool {
	return sep != "" && strings.IndexFunc(sep, func(r rune) bool {
		return !isPunct(r) && !strings.ContainsRune(temporalSpace, r)
	}) < 0
}

func isPunct(r rune) bool {
	if r <= ' ' || r >= 0x7f || isDigit(byte(r)) {
		return false
	}
	return (r < 'a' || r > 'z') && (r < 'A' || r > 'Z')
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func allDigits(s string) bool {
	for i := range len(s) {
		if !isDigit(s[i]) {
			return false
		}
	}
	return true
}

// atoi converts a run of digits already validated by allDigits. A value past
// nine digits saturates, which every caller's range check rejects.
func atoi(digits string) int {
	digits = strings.TrimLeft(digits, "0")
	if len(digits) > 9 {
		return math.MaxInt32
	}
	v := 0
	for i := range len(digits) {
		v = v*10 + int(digits[i]-'0')
	}
	return v
}

func pow10(n int) int {
	v := 1
	for range n {
		v *= 10
	}
	return v
}
