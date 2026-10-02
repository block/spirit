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
		{"utf8mb4_0900_ai_ci", CollationProperties{Accent: Insensitive, Kana: Insensitive, UCA: UCA900}},
		{"utf8mb4_0900_as_ci", CollationProperties{Accent: Sensitive, Kana: Insensitive, UCA: UCA900}},
		{"utf8mb4_0900_as_cs", CollationProperties{CaseSensitive: true, Accent: Sensitive, Kana: Sensitive, UCA: UCA900}},
		{"utf8mb4_ja_0900_as_cs", CollationProperties{CaseSensitive: true, Accent: Sensitive, Kana: Insensitive, UCA: UCA900}},
		{"utf8mb4_ja_0900_as_cs_ks", CollationProperties{CaseSensitive: true, Accent: Sensitive, Kana: Sensitive, UCA: UCA900}},
		{"utf8mb4_cs_0900_ai_ci", CollationProperties{Accent: Insensitive, Kana: Insensitive, UCA: UCA900}},
		{"utf8mb4_cs_0900_as_cs", CollationProperties{CaseSensitive: true, Accent: Sensitive, Kana: Sensitive, UCA: UCA900}},
		{"utf8mb4_0900_bin", CollationProperties{CaseSensitive: true, Binary: true, Accent: Sensitive, Kana: Sensitive}},
		{"utf8mb4_unicode_520_ci", CollationProperties{PadSpace: true, Accent: Insensitive, Kana: Insensitive, UCA: UCA520}},
		{"utf8mb4_unicode_ci", CollationProperties{PadSpace: true, Accent: Insensitive, Kana: Insensitive, UCA: UCA400}},
		{"utf8mb4_vietnamese_ci", CollationProperties{PadSpace: true, Accent: Insensitive, Kana: Insensitive, UCA: UCA400}},
		{"gb18030_unicode_520_ci", CollationProperties{PadSpace: true, Accent: Insensitive, Kana: Insensitive, UCA: UCA520}},
		{"utf8mb4_general_ci", CollationProperties{PadSpace: true}},
		{"utf8mb4_bin", CollationProperties{CaseSensitive: true, PadSpace: true, Binary: true, Accent: Sensitive, Kana: Sensitive}},
		{"latin1_general_ci", CollationProperties{PadSpace: true}},
		{"latin1_general_cs", CollationProperties{CaseSensitive: true, PadSpace: true}},
		{"utf8mb3_swedish_ci", CollationProperties{PadSpace: true, Accent: Insensitive, Kana: Insensitive, UCA: UCA400}},
		{"utf8mb3_general_ci", CollationProperties{PadSpace: true}},
		{"utf8mb3_tolower_ci", CollationProperties{PadSpace: true}},
		{"utf16le_general_ci", CollationProperties{PadSpace: true}},
		{"utf8_unicode_ci", CollationProperties{PadSpace: true, Accent: Insensitive, Kana: Insensitive, UCA: UCA400}},
		{"UTF8MB4_0900_AI_CI", CollationProperties{Accent: Insensitive, Kana: Insensitive, UCA: UCA900}},
		{"binary", CollationProperties{CaseSensitive: true, Binary: true, Accent: Sensitive, Kana: Sensitive}},
	}
	for _, tt := range tests {
		t.Run(tt.collation, func(t *testing.T) {
			got, err := LookupCollationProperties(tt.collation)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// Each UCA version compares greater than the one before it, so a caller can
// tell a move onto an older algorithm with a comparison.
func TestUCAVersionOrder(t *testing.T) {
	assert.Less(t, UCANone, UCA400)
	assert.Less(t, UCA400, UCA520)
	assert.Less(t, UCA520, UCA900)
	assert.Equal(t, "9.0.0", UCA900.String())
	assert.Equal(t, "none", UCANone.String())
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
// the server actually compares strings:
//
//   - 'a' against 'A' for case, and 'a' against 'a ' for trailing spaces. The
//     pad attribute must also match what information_schema reports.
//   - Accents against a set of accented letters: an accent-sensitive
//     collation tells every pair apart, and an accent-insensitive one folds at
//     least one, since a language tailoring keeps the letters its alphabet
//     counts as its own.
//   - Hiragana 'あ' against katakana 'ア' for kana.
//   - The UCA version, by letters Unicode added after each version: a
//     collation folds the case of a letter its weights assign and compares an
//     unassigned one by code point. Glagolitic arrived in Unicode 4.1 and Osage
//     in 9.0. UCA 4.0.0 collations weigh every supplementary character alike,
//     so Osage only tells 5.2.0 from 9.0.0. Case folding shows only through a
//     _ci collation, and every version has one.
//
// A binary collation also tells, in the Unicode charsets, a precomposed 'é'
// from 'e' and a combining accent, which a weight-based collation can call
// equal even when it is accent-sensitive. A pair the charset cannot represent
// is skipped.
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

	accentPairs := [][2]string{{"a", "á"}, {"a", "à"}, {"a", "â"}, {"e", "é"}, {"e", "è"}, {"o", "ó"}, {"u", "ú"}}
	for _, c := range collations {
		t.Run(c.name, func(t *testing.T) {
			props, err := LookupCollationProperties(c.name)
			require.NoError(t, err)
			assert.Equal(t, c.pad == "PAD SPACE", props.PadSpace, "pad attribute")

			// compare reports whether a and b compare equal under the
			// collation, and whether the charset represents both.
			compare := func(a, b string) (equal, representable bool) {
				text := func(s string) string {
					return fmt.Sprintf("CONVERT(_utf8mb4'%s' USING `%s`) COLLATE `%s`", s, c.charset, c.name)
				}
				roundTrips := func(s string) string {
					return fmt.Sprintf("CONVERT(CONVERT(_utf8mb4'%s' USING `%s`) USING utf8mb4) = _utf8mb4'%s' COLLATE utf8mb4_bin", s, c.charset, s)
				}
				require.NoError(t, db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT %s = %s, %s AND %s",
					text(a), text(b), roundTrips(a), roundTrips(b),
				)).Scan(&equal, &representable))
				return equal, representable
			}

			caseEqual, _ := compare("a", "A")
			assert.Equal(t, !caseEqual, props.CaseSensitive, "case sensitivity")
			padEqual, _ := compare("a", "a ")
			assert.Equal(t, padEqual, props.PadSpace, "trailing-space comparison")

			var folded []string
			for _, pair := range accentPairs {
				if equal, representable := compare(pair[0], pair[1]); representable && equal {
					folded = append(folded, pair[1])
				}
			}
			switch props.Accent {
			case Sensitive:
				assert.Empty(t, folded, "an accent-sensitive collation folds no accent")
			case Insensitive:
				assert.NotEmpty(t, folded, "an accent-insensitive collation folds an accent")
			case SensitivityUnknown:
				// The name does not decide it, so there is no claim to check.
			}

			if equal, representable := compare("あ", "ア"); representable {
				switch props.Kana {
				case Sensitive:
					assert.False(t, equal, "a kana-sensitive collation tells hiragana from katakana")
				case Insensitive:
					assert.True(t, equal, "a kana-insensitive collation folds hiragana and katakana")
				case SensitivityUnknown:
					// The name does not decide it, so there is no claim to check.
				}
			}

			if props.UCA != UCANone && !props.CaseSensitive {
				glagolitic, _ := compare("\u2c00", "\u2c30")
				assert.Equal(t, props.UCA >= UCA520, glagolitic, "UCA %s folds the case of Glagolitic", props.UCA)
				if osage, representable := compare("\U000104b0", "\U000104d8"); representable && props.UCA >= UCA520 {
					assert.Equal(t, props.UCA == UCA900, osage, "UCA %s folds the case of Osage", props.UCA)
				}
			}

			if !props.Binary {
				return
			}
			if c.charset == "utf8mb4" || c.charset == "utf8mb3" {
				composedEqual, _ := compare("\u00e9", "e\u0301")
				assert.False(t, composedEqual, "a binary collation compares code points, not canonical equivalence")
			}
		})
	}
}
