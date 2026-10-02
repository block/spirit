package statement

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLookupCollationProperties(t *testing.T) {
	tests := []struct {
		collation string
		want      CollationProperties
	}{
		{"utf8mb4_0900_ai_ci", CollationProperties{CaseSensitive: false, PadSpace: false}},
		{"utf8mb4_0900_as_ci", CollationProperties{CaseSensitive: false, PadSpace: false}},
		{"utf8mb4_0900_as_cs", CollationProperties{CaseSensitive: true, PadSpace: false}},
		{"utf8mb4_ja_0900_as_cs_ks", CollationProperties{CaseSensitive: true, PadSpace: false}},
		{"utf8mb4_cs_0900_ai_ci", CollationProperties{CaseSensitive: false, PadSpace: false}},
		{"utf8mb4_cs_0900_as_cs", CollationProperties{CaseSensitive: true, PadSpace: false}},
		{"utf8mb4_0900_bin", CollationProperties{CaseSensitive: true, PadSpace: false, Binary: true}},
		{"utf8mb4_general_ci", CollationProperties{CaseSensitive: false, PadSpace: true}},
		{"utf8mb4_bin", CollationProperties{CaseSensitive: true, PadSpace: true, Binary: true}},
		{"latin1_general_cs", CollationProperties{CaseSensitive: true, PadSpace: true}},
		{"utf8mb3_swedish_ci", CollationProperties{CaseSensitive: false, PadSpace: true}},
		{"utf8mb3_general_ci", CollationProperties{CaseSensitive: false, PadSpace: true}},
		{"utf8_unicode_ci", CollationProperties{CaseSensitive: false, PadSpace: true}},
		{"UTF8MB4_0900_AI_CI", CollationProperties{CaseSensitive: false, PadSpace: false}},
		{"binary", CollationProperties{CaseSensitive: true, PadSpace: false, Binary: true}},
	}
	for _, tt := range tests {
		t.Run(tt.collation, func(t *testing.T) {
			got, err := LookupCollationProperties(tt.collation)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// A collation the parser does not know is an error, not a guess: a caller
// deciding whether a collation change alters comparisons has to report it as
// unknown.
func TestLookupCollationPropertiesUnknown(t *testing.T) {
	_, err := LookupCollationProperties("utf8mb4_custom_collation")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "utf8mb4_custom_collation")
}

// Every collation the server reports is known, and its properties match how
// the server actually compares strings: 'a' against 'A' for case, and 'a'
// against 'a ' for trailing spaces. The pad attribute must also match what
// information_schema reports. A binary collation also tells 'a' from 'á', and
// in the Unicode charsets a precomposed 'é' from 'e' and a combining accent,
// which a weight-based collation can call equal even when it is
// accent-sensitive.
func TestLookupCollationPropertiesMatchesServer(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	rows, err := db.QueryContext(t.Context(), "SELECT COLLATION_NAME, CHARACTER_SET_NAME, PAD_ATTRIBUTE FROM information_schema.COLLATIONS ORDER BY COLLATION_NAME")
	require.NoError(t, err)
	type serverCollation struct{ name, charset, pad string }
	var collations []serverCollation
	for rows.Next() {
		var c serverCollation
		require.NoError(t, rows.Scan(&c.name, &c.charset, &c.pad))
		collations = append(collations, c)
	}
	require.NoError(t, rows.Err())
	utils.CloseAndLog(rows)
	require.NotEmpty(t, collations)

	for _, c := range collations {
		t.Run(c.name, func(t *testing.T) {
			props, err := LookupCollationProperties(c.name)
			require.NoError(t, err)
			assert.Equal(t, c.pad == "PAD SPACE", props.PadSpace, "pad attribute")

			text := func(s string) string {
				return fmt.Sprintf("CONVERT(_utf8mb4'%s' USING `%s`) COLLATE `%s`", s, c.charset, c.name)
			}
			var caseEqual, padEqual, accentEqual bool
			err = db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT %s = %s, %s = %s, %s = %s",
				text("a"), text("A"), text("a"), text("a "), text("a"), text("á"),
			)).Scan(&caseEqual, &padEqual, &accentEqual)
			require.NoError(t, err)
			assert.Equal(t, !caseEqual, props.CaseSensitive, "case sensitivity")
			assert.Equal(t, padEqual, props.PadSpace, "trailing-space comparison")
			if !props.Binary {
				return
			}
			assert.False(t, accentEqual, "a binary collation compares accents")
			if c.charset == "utf8mb4" || c.charset == "utf8mb3" {
				var composedEqual bool
				require.NoError(t, db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT %s = %s",
					text("\u00e9"), text("e\u0301"),
				)).Scan(&composedEqual))
				assert.False(t, composedEqual, "a binary collation compares code points, not canonical equivalence")
			}
		})
	}
}
