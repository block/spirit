package utils

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseEnumSetElements(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
		wantErr  bool
	}{
		// Basic cases.
		{"enum('a','b','c')", []string{"a", "b", "c"}, false},
		{"set('read','write','execute')", []string{"read", "write", "execute"}, false},
		{"enum('active','inactive','pending')", []string{"active", "inactive", "pending"}, false},

		// Non-enum/set types return an error (fail-closed).
		{"int", nil, true},
		{"varchar(255)", nil, true},

		// Empty element list: valid syntax, no elements, no error.
		{"enum()", nil, false},

		// Values containing commas.
		{"enum('a,b','c')", []string{"a,b", "c"}, false},
		{"enum('one','two,three','four')", []string{"one", "two,three", "four"}, false},

		// Values containing escaped (doubled) single quotes.
		{"enum('it''s','ok')", []string{"it's", "ok"}, false},
		{"enum('he said ''hi''','bye')", []string{"he said 'hi'", "bye"}, false},

		// Commas and escaped quotes combined.
		{"enum('a,b''c','d')", []string{"a,b'c", "d"}, false},

		// Single element.
		{"enum('only')", []string{"only"}, false},

		// Spaces between elements (MySQL SHOW CREATE TABLE may include them).
		{"enum('a', 'b', 'c')", []string{"a", "b", "c"}, false},

		// Case-insensitive prefix.
		{"ENUM('X','Y')", []string{"X", "Y"}, false},
		{"SET('r','w')", []string{"r", "w"}, false},

		// Empty string as a valid ENUM value.
		{"enum('','a','b')", []string{"", "a", "b"}, false},

		// Characters MySQL backslash-escapes in column_type: a backslash,
		// newline, carriage return and NUL (see QuoteEnumSetMember).
		{`enum('a\\b','c')`, []string{`a\b`, "c"}, false},
		{`enum('nl\nx','cr\rx','nul\0x')`, []string{"nl\nx", "cr\rx", "nul\x00x"}, false},
		{`set('a\\b','nl\nx')`, []string{`a\b`, "nl\nx"}, false},
		{`enum('\\','\\\\','\\n')`, []string{`\`, `\\`, `\n`}, false},
		{`enum('a\\''b','c')`, []string{`a\'b`, "c"}, false},
		// Characters MySQL writes as themselves: a tab, Ctrl-Z, backspace and
		// double quote.
		{"enum('tab\tx','cz\x1ax','bk\bx','dq\"x')", []string{"tab\tx", "cz\x1ax", "bk\bx", "dq\"x"}, false},

		// A member that is not valid utf8mb3, which only a binary-charset
		// column can hold, is written as a hex literal. This is what MySQL
		// reports for ENUM(x'5c', x'815c', 'q''x', x'f09f9880', x'eda080',
		// x'00', x'c0af', x'41ff', x'e9', x'0a', x'', x'ffab') CHARACTER SET binary.
		{`enum('\\',x'815c','q''x')`, []string{`\`, "\x81\\", "q'x"}, false},
		{`enum('\\',x'815c','q''x',x'f09f9880',x'eda080','\0',x'c0af',x'41ff',x'e9','\n','',x'ffab')`,
			[]string{`\`, "\x81\\", "q'x", "\U0001F600", "\xed\xa0\x80", "\x00", "\xc0\xaf", "A\xff", "\xe9", "\n", "", "\xff\xab"}, false},
		{`set(x'815c','x')`, []string{"\x81\\", "x"}, false},

		// Malformed inputs: fail-closed (return error, not partial results).
		{`enum(x'8')`, nil, true},           // odd number of hex digits
		{`enum(x'zz')`, nil, true},          // not hex
		{`enum(x'81`, nil, true},            // unterminated hex literal
		{`enum(X'81')`, nil, true},          // MySQL writes a lower-case x
		{`enum(x'81'x'82')`, nil, true},     // no delimiter between elements
		{`enum(x 'ab')`, nil, true},         // x not followed by a quote
		{`enum(x'81','a',)`, nil, true},     // trailing comma after a hex literal
		{`enum('a\tb')`, nil, true},         // escape MySQL does not write in column_type
		{`enum('a\Zb')`, nil, true},         // likewise
		{`enum('a\'b')`, nil, true},         // MySQL doubles a quote, it does not escape it
		{`enum('a\"b')`, nil, true},         // likewise for a double quote
		{`enum('a\%b')`, nil, true},         // \% is written as \\%
		{`enum('ab\')`, nil, true},          // backslash before the closing quote
		{`enum('a\\\b')`, nil, true},        // odd backslash run
		{"enum(a,'b','c')", nil, true},      // unquoted value
		{"enum('a','b',3)", nil, true},      // numeric literal without quotes
		{"enum('a'  x  'b')", nil, true},    // junk between elements
		{"enum('a','b", nil, true},          // unterminated quote (missing closing paren clips it)
		{"enum('unterminated", nil, true},   // unterminated quote, no closing paren
		{"enum('a',,'b')", nil, true},       // empty element between delimiters
		{"enum('a', 'b', 'c',)", nil, true}, // trailing comma is malformed
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result, err := ParseEnumSetElements(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				require.Nil(t, result)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestIsEnumOrSetType(t *testing.T) {
	// ENUM types
	require.True(t, IsEnumOrSetType("enum('a','b','c')"))
	require.True(t, IsEnumOrSetType("ENUM('A','B')"))
	require.True(t, IsEnumOrSetType("Enum('x')"))

	// SET types
	require.True(t, IsEnumOrSetType("set('read','write')"))
	require.True(t, IsEnumOrSetType("SET('r','w')"))
	require.True(t, IsEnumOrSetType("Set('a')"))

	// Non-ENUM/SET types
	require.False(t, IsEnumOrSetType("varchar(191)"))
	require.False(t, IsEnumOrSetType("int"))
	require.False(t, IsEnumOrSetType("bigint"))
	require.False(t, IsEnumOrSetType("text"))
	require.False(t, IsEnumOrSetType("decimal(10,2)"))
	require.False(t, IsEnumOrSetType(""))
}

func TestIsEnumType(t *testing.T) {
	require.True(t, IsEnumType("enum('a','b','c')"))
	require.True(t, IsEnumType("ENUM('A','B')"))
	require.False(t, IsEnumType("set('read','write')"))
	require.False(t, IsEnumType("varchar(191)"))
	require.False(t, IsEnumType(""))
}

func TestIsSetType(t *testing.T) {
	require.True(t, IsSetType("set('read','write')"))
	require.True(t, IsSetType("SET('r','w')"))
	require.False(t, IsSetType("enum('a','b')"))
	require.False(t, IsSetType("varchar(191)"))
	require.False(t, IsSetType(""))
}

func TestParseSQLQuotedListUnterminated(t *testing.T) {
	// A quoted string that is never closed should return an error.
	result, err := parseSQLQuotedList("'abc")
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "unterminated")
}

// TestQuoteEnumSetMember checks that QuoteEnumSetMember writes each member the
// way MySQL writes it in column_type, and that ParseEnumSetElements reads every
// byte value back unchanged.
func TestQuoteEnumSetMember(t *testing.T) {
	for member, want := range map[string]string{
		"a":        "'a'",
		"":         "''",
		`a\b`:      `'a\\b'`,
		"nl\nx":    `'nl\nx'`,
		"cr\rx":    `'cr\rx'`,
		"nul\x00x": `'nul\0x'`,
		"it's":     "'it''s'",
		"tab\tx":   "'tab\tx'",
		"cz\x1ax":  "'cz\x1ax'",
		`dq"x`:     `'dq"x'`,
		`\%`:       `'\\%'`,
		"a,b":      "'a,b'",
		// Not valid utf8mb3: a hex literal.
		"\x81\\":       "x'815c'",
		"\U0001F600":   "x'f09f9880'",
		"\xed\xa0\x80": "x'eda080'", // an encoded surrogate
		"\xc0\xaf":     "x'c0af'",   // an overlong encoding
		"A\xff":        "x'41ff'",
	} {
		assert.Equal(t, want, QuoteEnumSetMember(member), "member %q", member)
	}

	members := make([]string, 0, 256)
	quoted := make([]string, 0, 256)
	for b := range 256 {
		member := "x" + string([]byte{byte(b)}) + "y"
		members = append(members, member)
		quoted = append(quoted, QuoteEnumSetMember(member))
	}
	got, err := ParseEnumSetElements("enum(" + strings.Join(quoted, ",") + ")")
	require.NoError(t, err)
	require.Equal(t, members, got)
}

func TestParseSQLQuotedListTrailingBackslash(t *testing.T) {
	// A backslash with nothing after it is an unterminated escape.
	result, err := parseSQLQuotedList(`'abc\`)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "unterminated escape")
}
