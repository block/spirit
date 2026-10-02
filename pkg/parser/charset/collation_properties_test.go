package charset

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// comparison is the part of a Collation that says how it compares strings.
type comparison struct {
	CaseSensitive   bool
	PadSpace        bool
	Binary          bool
	AccentSensitive Sensitivity
	KanaSensitive   Sensitivity
	UCAVersion      UCAVersion
}

func comparisonOf(c *Collation) comparison {
	return comparison{
		CaseSensitive:   c.CaseSensitive,
		PadSpace:        c.PadAttribute == PadSpace,
		Binary:          c.Binary,
		AccentSensitive: c.AccentSensitive,
		KanaSensitive:   c.KanaSensitive,
		UCAVersion:      c.UCAVersion,
	}
}

func TestCollationComparison(t *testing.T) {
	tests := []struct {
		collation string
		want      comparison
	}{
		{"utf8mb4_0900_ai_ci", comparison{AccentSensitive: Insensitive, KanaSensitive: Insensitive, UCAVersion: UCA900}},
		{"utf8mb4_0900_as_ci", comparison{AccentSensitive: Sensitive, KanaSensitive: Insensitive, UCAVersion: UCA900}},
		{"utf8mb4_0900_as_cs", comparison{CaseSensitive: true, AccentSensitive: Sensitive, KanaSensitive: Sensitive, UCAVersion: UCA900}},
		{"utf8mb4_ja_0900_as_cs", comparison{CaseSensitive: true, AccentSensitive: Sensitive, KanaSensitive: Insensitive, UCAVersion: UCA900}},
		{"utf8mb4_ja_0900_as_cs_ks", comparison{CaseSensitive: true, AccentSensitive: Sensitive, KanaSensitive: Sensitive, UCAVersion: UCA900}},
		{"utf8mb4_cs_0900_ai_ci", comparison{AccentSensitive: Insensitive, KanaSensitive: Insensitive, UCAVersion: UCA900}},
		{"utf8mb4_cs_0900_as_cs", comparison{CaseSensitive: true, AccentSensitive: Sensitive, KanaSensitive: Sensitive, UCAVersion: UCA900}},
		{"utf8mb4_0900_bin", comparison{CaseSensitive: true, Binary: true, AccentSensitive: Sensitive, KanaSensitive: Sensitive}},
		{"utf8mb4_unicode_520_ci", comparison{PadSpace: true, AccentSensitive: Insensitive, KanaSensitive: Insensitive, UCAVersion: UCA520}},
		{"utf8mb4_unicode_ci", comparison{PadSpace: true, AccentSensitive: Insensitive, KanaSensitive: Insensitive, UCAVersion: UCA400}},
		{"utf8mb4_vietnamese_ci", comparison{PadSpace: true, AccentSensitive: Insensitive, KanaSensitive: Insensitive, UCAVersion: UCA400}},
		{"gb18030_unicode_520_ci", comparison{PadSpace: true, AccentSensitive: Insensitive, KanaSensitive: Insensitive, UCAVersion: UCA520}},
		{"utf8mb4_general_ci", comparison{PadSpace: true}},
		{"utf8mb4_bin", comparison{CaseSensitive: true, PadSpace: true, Binary: true, AccentSensitive: Sensitive, KanaSensitive: Sensitive}},
		{"latin1_general_ci", comparison{PadSpace: true}},
		{"latin1_general_cs", comparison{CaseSensitive: true, PadSpace: true}},
		{"utf8mb3_swedish_ci", comparison{PadSpace: true, AccentSensitive: Insensitive, KanaSensitive: Insensitive, UCAVersion: UCA400}},
		{"utf8mb3_general_ci", comparison{PadSpace: true}},
		{"utf8mb3_tolower_ci", comparison{PadSpace: true}},
		{"utf16le_general_ci", comparison{PadSpace: true}},
		{"utf8_unicode_ci", comparison{PadSpace: true, AccentSensitive: Insensitive, KanaSensitive: Insensitive, UCAVersion: UCA400}},
		{"UTF8MB4_0900_AI_CI", comparison{AccentSensitive: Insensitive, KanaSensitive: Insensitive, UCAVersion: UCA900}},
		{"binary", comparison{CaseSensitive: true, Binary: true, AccentSensitive: Sensitive, KanaSensitive: Sensitive}},
	}
	for _, tt := range tests {
		t.Run(tt.collation, func(t *testing.T) {
			c, err := FindCollationByName(tt.collation)
			require.NoError(t, err)
			assert.Equal(t, tt.want, comparisonOf(c))
		})
	}
}

// Each UCA version compares greater than the one before it.
func TestUCAVersionOrder(t *testing.T) {
	assert.Less(t, UCANone, UCA400)
	assert.Less(t, UCA400, UCA520)
	assert.Less(t, UCA520, UCA900)
	assert.Equal(t, "9.0.0", UCA900.String())
	assert.Equal(t, "none", UCANone.String())
}

// successors follows DeprecatedByCollationID from a collation to the current
// one that replaces it.
func successors(t *testing.T, name string) []string {
	t.Helper()
	c, err := FindCollationByName(name)
	require.NoError(t, err)
	var chain []string
	for c.DeprecatedByCollationID != 0 {
		c, err = FindCollationByID(c.DeprecatedByCollationID)
		require.NoError(t, err)
		chain = append(chain, c.Name)
		require.Less(t, len(chain), len(collations), "deprecation cycle from %q", name)
	}
	return chain
}

func TestDeprecatedByCollationID(t *testing.T) {
	tests := []struct {
		collation string
		want      []string
	}{
		{"utf8mb4_general_ci", []string{"utf8mb4_unicode_520_ci", "utf8mb4_0900_ai_ci"}},
		{"utf8mb4_unicode_ci", []string{"utf8mb4_unicode_520_ci", "utf8mb4_0900_ai_ci"}},
		{"utf8mb4_german2_ci", []string{"utf8mb4_de_pb_0900_ai_ci"}},
		{"utf8mb3_general_ci", []string{"utf8mb4_general_ci", "utf8mb4_unicode_520_ci", "utf8mb4_0900_ai_ci"}},
		{"utf8_bin", []string{"utf8mb4_bin"}},
		{"utf8mb3_tolower_ci", nil},
		{"utf8mb4_0900_ai_ci", nil},
		{"utf8mb4_bin", nil},
		{"latin1_swedish_ci", nil},
	}
	for _, tt := range tests {
		t.Run(tt.collation, func(t *testing.T) {
			assert.Equal(t, tt.want, successors(t, tt.collation))
		})
	}
}

// Every collation that names a successor is replaced by one of the same
// charset on a newer UCA version, or by the utf8mb4 collation of the same
// name. Every utf8mb4 collation older than UCA 9.0.0 names one, apart from
// the binary collation and the two languages MySQL has no 0900 collation for.
func TestDeprecatedByCollationIDIsComplete(t *testing.T) {
	var withoutSuccessor []string
	for _, c := range collations {
		if c.DeprecatedByCollationID == 0 {
			if c.CharsetName == CharsetUTF8MB4 && !c.Binary && c.UCAVersion < UCA900 {
				withoutSuccessor = append(withoutSuccessor, c.Name)
			}
			continue
		}
		next, err := FindCollationByID(c.DeprecatedByCollationID)
		require.NoError(t, err, c.Name)
		if c.CharsetName == CharsetUTF8 {
			assert.Equal(t, CharsetUTF8MB4+strings.TrimPrefix(c.Name, CharsetUTF8), next.Name, c.Name)
			continue
		}
		assert.Equal(t, c.CharsetName, next.CharsetName, c.Name)
		assert.Greater(t, next.UCAVersion, c.UCAVersion, c.Name)
	}
	assert.ElementsMatch(t, []string{"utf8mb4_persian_ci", "utf8mb4_sinhala_ci"}, withoutSuccessor)
}

func TestFindCollationByID(t *testing.T) {
	for _, c := range collations {
		got, err := FindCollationByID(c.ID)
		require.NoError(t, err)
		assert.Equal(t, c, got)
	}
	_, err := FindCollationByID(-1)
	require.Error(t, err)
}

// Every collation the server reports is known, and how it compares strings
// matches what the server actually does:
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
func TestCollationComparisonMatchesServer(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	rows, err := db.QueryContext(t.Context(), "SELECT COLLATION_NAME, CHARACTER_SET_NAME, PAD_ATTRIBUTE FROM information_schema.COLLATIONS ORDER BY COLLATION_NAME")
	require.NoError(t, err)
	type serverCollation struct{ name, charset, pad string }
	var serverCollations []serverCollation
	for rows.Next() {
		var c serverCollation
		require.NoError(t, rows.Scan(&c.name, &c.charset, &c.pad))
		serverCollations = append(serverCollations, c)
	}
	require.NoError(t, rows.Err())
	utils.CloseAndLog(rows)
	require.NotEmpty(t, serverCollations)

	accentPairs := [][2]string{{"a", "á"}, {"a", "à"}, {"a", "â"}, {"e", "é"}, {"e", "è"}, {"o", "ó"}, {"u", "ú"}}
	for _, sc := range serverCollations {
		t.Run(sc.name, func(t *testing.T) {
			c, err := FindCollationByName(sc.name)
			require.NoError(t, err)
			assert.Equal(t, sc.pad, c.PadAttribute, "pad attribute")

			// compare reports whether a and b compare equal under the
			// collation, and whether the charset represents both.
			compare := func(a, b string) (equal, representable bool) {
				text := func(s string) string {
					return fmt.Sprintf("CONVERT(_utf8mb4'%s' USING `%s`) COLLATE `%s`", s, sc.charset, sc.name)
				}
				roundTrips := func(s string) string {
					return fmt.Sprintf("CONVERT(CONVERT(_utf8mb4'%s' USING `%s`) USING utf8mb4) = _utf8mb4'%s' COLLATE utf8mb4_bin", s, sc.charset, s)
				}
				require.NoError(t, db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT %s = %s, %s AND %s",
					text(a), text(b), roundTrips(a), roundTrips(b),
				)).Scan(&equal, &representable))
				return equal, representable
			}

			caseEqual, _ := compare("a", "A")
			assert.Equal(t, !caseEqual, c.CaseSensitive, "case sensitivity")
			padEqual, _ := compare("a", "a ")
			assert.Equal(t, padEqual, c.PadAttribute == PadSpace, "trailing-space comparison")

			var folded []string
			for _, pair := range accentPairs {
				if equal, representable := compare(pair[0], pair[1]); representable && equal {
					folded = append(folded, pair[1])
				}
			}
			switch c.AccentSensitive {
			case Sensitive:
				assert.Empty(t, folded, "an accent-sensitive collation folds no accent")
			case Insensitive:
				assert.NotEmpty(t, folded, "an accent-insensitive collation folds an accent")
			case SensitivityUnknown:
				// The name does not decide it, so there is no claim to check.
			}

			if equal, representable := compare("あ", "ア"); representable {
				switch c.KanaSensitive {
				case Sensitive:
					assert.False(t, equal, "a kana-sensitive collation tells hiragana from katakana")
				case Insensitive:
					assert.True(t, equal, "a kana-insensitive collation folds hiragana and katakana")
				case SensitivityUnknown:
					// The name does not decide it, so there is no claim to check.
				}
			}

			if c.UCAVersion != UCANone && !c.CaseSensitive {
				glagolitic, _ := compare("\u2c00", "\u2c30")
				assert.Equal(t, c.UCAVersion >= UCA520, glagolitic, "UCA %s folds the case of Glagolitic", c.UCAVersion)
				if osage, representable := compare("\U000104b0", "\U000104d8"); representable && c.UCAVersion >= UCA520 {
					assert.Equal(t, c.UCAVersion == UCA900, osage, "UCA %s folds the case of Osage", c.UCAVersion)
				}
			}

			if !c.Binary {
				return
			}
			if sc.charset == "utf8mb4" || sc.charset == "utf8mb3" {
				composedEqual, _ := compare("\u00e9", "e\u0301")
				assert.False(t, composedEqual, "a binary collation compares code points, not canonical equivalence")
			}
		})
	}
}
