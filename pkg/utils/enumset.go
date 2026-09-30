package utils

import (
	"encoding/hex"
	"fmt"
	"strings"
	"unicode/utf8"
)

// IsEnumType reports whether a MySQL column type string (as reported by
// information_schema.COLUMNS.column_type) is an ENUM, e.g. "enum('a','b')".
func IsEnumType(mysqlType string) bool {
	return strings.HasPrefix(strings.ToLower(mysqlType), "enum(")
}

// IsSetType reports whether a MySQL column type string is a SET,
// e.g. "set('a','b')".
func IsSetType(mysqlType string) bool {
	return strings.HasPrefix(strings.ToLower(mysqlType), "set(")
}

// IsEnumOrSetType reports whether a MySQL column type string is an ENUM or SET.
func IsEnumOrSetType(mysqlType string) bool {
	return IsEnumType(mysqlType) || IsSetType(mysqlType)
}

// ParseEnumSetElements extracts the element values from a MySQL type string
// like "enum('a','b','c')" or "set('x','y','z')", as information_schema
// reports it in COLUMNS.COLUMN_TYPE.
//
// It uses a small state machine to correctly handle values containing
// commas, the characters MySQL escapes in that text, and the hex literals it
// writes for members that are not valid utf8mb3 (see QuoteEnumSetMember).
//
// It is fail-closed: any unexpected characters or malformed structure returns
// an error so that callers (safety checks, the binlog ENUM/SET decoder) are
// never silently bypassed.
func ParseEnumSetElements(mysqlType string) ([]string, error) {
	if !IsEnumOrSetType(mysqlType) {
		return nil, fmt.Errorf("not an enum/set type: %q", mysqlType)
	}

	start := strings.IndexByte(mysqlType, '(')
	if start < 0 {
		return nil, fmt.Errorf("missing opening parenthesis in type: %q", mysqlType)
	}
	end := strings.LastIndexByte(mysqlType, ')')
	if end <= start {
		return nil, fmt.Errorf("missing closing parenthesis in type: %q", mysqlType)
	}

	inner := mysqlType[start+1 : end]
	if len(inner) == 0 {
		return nil, nil // e.g. enum() — valid but empty
	}

	return parseSQLQuotedList(inner)
}

// QuoteEnumSetMember renders an ENUM or SET member the way MySQL writes it in
// information_schema.COLUMNS.COLUMN_TYPE (and SHOW CREATE TABLE).
//
// A member that is valid utf8mb3 is written in single quotes, with a single
// quote doubled and a backslash, newline, carriage return and NUL
// backslash-escaped. No other character is escaped: a tab, Ctrl-Z, backspace
// or double quote is written as itself. This is MySQL's append_unescaped().
//
// Any other member is written as a hex literal of its bytes, e.g. x'815c':
// invalid UTF-8, and a character outside utf8mb3 (a 4-byte UTF-8 character).
// Only a binary-charset column can hold such bytes, and MySQL writes its
// members this way. (A column of any other charset holds a 4-byte character
// only in utf8mb4, and MySQL reports that character as '?'. Spirit writes the
// member instead, so that it reads back unchanged.)
//
// parseSQLQuotedList decodes exactly these forms.
func QuoteEnumSetMember(member string) string {
	if !isUTF8MB3(member) {
		return "x'" + hex.EncodeToString([]byte(member)) + "'"
	}
	var buf strings.Builder
	buf.Grow(len(member) + 2)
	buf.WriteByte('\'')
	for i := 0; i < len(member); i++ {
		switch c := member[i]; c {
		case 0:
			buf.WriteString(`\0`)
		case '\n':
			buf.WriteString(`\n`)
		case '\r':
			buf.WriteString(`\r`)
		case '\\':
			buf.WriteString(`\\`)
		case '\'':
			buf.WriteString(`''`)
		default:
			buf.WriteByte(c)
		}
	}
	buf.WriteByte('\'')
	return buf.String()
}

// isUTF8MB3 reports whether s is valid UTF-8 with no character outside the
// Basic Multilingual Plane, which is what utf8mb3 can hold.
func isUTF8MB3(s string) bool {
	if !utf8.ValidString(s) {
		return false
	}
	return strings.IndexFunc(s, func(r rune) bool { return r > 0xFFFF }) < 0
}

// parseSQLQuotedList parses a comma-separated list of single-quoted SQL
// strings and x'<hex>' literals, correctly handling embedded commas and the
// escapes QuoteEnumSetMember writes.
//
// It is fail-closed: any unexpected character outside of quotes, and any
// backslash escape MySQL does not write in COLUMN_TYPE, causes the parser to
// return an error rather than silently producing a partial or wrong result.
func parseSQLQuotedList(s string) ([]string, error) {
	var elems []string
	i := 0
	n := len(s)

	// expectValue tracks whether we are currently expecting the start of a
	// quoted value (true) or a delimiter/completion after a value (false).
	expectValue := true

	for {
		// Skip whitespace outside of values.
		for i < n && (s[i] == ' ' || s[i] == '\t') {
			i++
		}

		if expectValue {
			// We are expecting the start of a quoted value.
			if i >= n {
				// Empty input is handled by the caller; reaching EOF here
				// indicates a trailing delimiter or otherwise malformed input.
				if len(elems) == 0 {
					return nil, fmt.Errorf("empty quoted list %q", s)
				}
				return nil, fmt.Errorf("trailing delimiter in quoted list %q", s)
			}
			if s[i] == 'x' && i+1 < n && s[i+1] == '\'' {
				// A hex literal: MySQL writes a member that is not valid
				// utf8mb3 this way (see QuoteEnumSetMember).
				end := strings.IndexByte(s[i+2:], '\'')
				if end < 0 {
					return nil, fmt.Errorf("unterminated hex literal at position %d in quoted list %q", i, s)
				}
				b, err := hex.DecodeString(s[i+2 : i+2+end])
				if err != nil {
					return nil, fmt.Errorf("invalid hex literal at position %d in quoted list %q: %w", i, s, err)
				}
				elems = append(elems, string(b))
				i += 2 + end + 1
				expectValue = false
				continue
			}
			if s[i] != '\'' {
				return nil, fmt.Errorf("unexpected character %q at position %d in quoted list %q", s[i], i, s)
			}

			// Opening quote found; collect the value.
			i++ // skip opening quote
			var buf strings.Builder
			closed := false
			for i < n {
				if s[i] == '\'' {
					// Check for doubled quote (escape sequence).
					if i+1 < n && s[i+1] == '\'' {
						buf.WriteByte('\'')
						i += 2
						continue
					}
					// Closing quote.
					i++
					closed = true
					break
				}
				if s[i] == '\\' {
					if i+1 >= n {
						return nil, fmt.Errorf("unterminated escape sequence at position %d in quoted list %q", i, s)
					}
					switch s[i+1] {
					case '0':
						buf.WriteByte(0)
					case 'n':
						buf.WriteByte('\n')
					case 'r':
						buf.WriteByte('\r')
					case '\\':
						buf.WriteByte('\\')
					default:
						return nil, fmt.Errorf("unexpected escape sequence %q at position %d in quoted list %q", s[i:i+2], i, s)
					}
					i += 2
					continue
				}
				buf.WriteByte(s[i])
				i++
			}
			if !closed {
				return nil, fmt.Errorf("unterminated quoted string in quoted list %q", s)
			}
			elems = append(elems, buf.String())
			// Next we expect either a comma delimiter or the end of the list.
			expectValue = false
		} else {
			// We have just parsed a value; now expect either a comma or EOF.
			if i >= n {
				// No more input; successfully parsed all elements.
				break
			}
			if s[i] != ',' {
				return nil, fmt.Errorf("unexpected character %q at position %d in quoted list %q", s[i], i, s)
			}
			// Consume the comma and loop back to parse the next value.
			i++
			expectValue = true
		}
	}

	return elems, nil
}
