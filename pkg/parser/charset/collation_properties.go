package charset

import (
	"fmt"
	"slices"
	"strings"
)

// Sensitivity reports whether a collation tells apart strings that differ
// only in one respect, such as an accent or kana form. A collation's name does
// not always decide it, so the zero value is SensitivityUnknown.
type Sensitivity int

const (
	// SensitivityUnknown means the collation's name does not decide it.
	SensitivityUnknown Sensitivity = iota
	// Sensitive means the strings compare unequal.
	Sensitive
	// Insensitive means the strings compare equal.
	Insensitive
)

func (s Sensitivity) String() string {
	switch s {
	case Sensitive:
		return "sensitive"
	case Insensitive:
		return "insensitive"
	case SensitivityUnknown:
		return "unknown"
	}
	return "unknown"
}

// UCAVersion is the version of the Unicode Collation Algorithm a collation
// takes its weights from. Later versions compare greater.
type UCAVersion int

const (
	// UCANone is a collation that does not use UCA weights: a binary
	// collation, or one of the per-charset collations such as general_ci.
	UCANone UCAVersion = iota
	// UCA400 is UCA 4.0.0: unicode_ci, and the language tailorings whose
	// names carry no version.
	UCA400
	// UCA520 is UCA 5.2.0, the _520_ collations.
	UCA520
	// UCA900 is UCA 9.0.0, the _0900_ collations.
	UCA900
)

func (v UCAVersion) String() string {
	switch v {
	case UCA400:
		return "4.0.0"
	case UCA520:
		return "5.2.0"
	case UCA900:
		return "9.0.0"
	case UCANone:
		return "none"
	}
	return "none"
}

// collationRow is one entry of the collation table. init derives the rest of
// a Collation from it.
type collationRow struct {
	ID           int
	CharsetName  string
	Name         string
	IsDefault    bool
	Sortlen      int
	PadAttribute string
}

// unicodeCharsets are the charsets whose collations other than the
// per-charset ones (general_ci, general_mysql500_ci, tolower_ci) are UCA
// collations. utf16le has only general_ci and a binary collation.
var unicodeCharsets = map[string]bool{
	CharsetUTF8:    true,
	CharsetUTF8MB3: true,
	CharsetUTF8MB4: true,
	CharsetUCS2:    true,
	CharsetUTF16:   true,
	CharsetUTF32:   true,
}

// deprecatedBy names the collation that replaces each obsolete utf8mb4 one,
// one UCA version at a time: general_ci and unicode_ci by unicode_520_ci, which
// in turn gives way to 0900_ai_ci, and each UCA 4.0.0 language tailoring by the
// 0900_ai_ci collation for the same language. utf8mb3 collations are not
// listed: init points each at its utf8mb4 counterpart.
var deprecatedBy = map[string]string{
	"utf8mb4_general_ci":     "utf8mb4_unicode_520_ci",
	"utf8mb4_unicode_ci":     "utf8mb4_unicode_520_ci",
	"utf8mb4_unicode_520_ci": "utf8mb4_0900_ai_ci",
	"utf8mb4_croatian_ci":    "utf8mb4_hr_0900_ai_ci",
	"utf8mb4_czech_ci":       "utf8mb4_cs_0900_ai_ci",
	"utf8mb4_danish_ci":      "utf8mb4_da_0900_ai_ci",
	"utf8mb4_esperanto_ci":   "utf8mb4_eo_0900_ai_ci",
	"utf8mb4_estonian_ci":    "utf8mb4_et_0900_ai_ci",
	"utf8mb4_german2_ci":     "utf8mb4_de_pb_0900_ai_ci",
	"utf8mb4_hungarian_ci":   "utf8mb4_hu_0900_ai_ci",
	"utf8mb4_icelandic_ci":   "utf8mb4_is_0900_ai_ci",
	"utf8mb4_latvian_ci":     "utf8mb4_lv_0900_ai_ci",
	"utf8mb4_lithuanian_ci":  "utf8mb4_lt_0900_ai_ci",
	"utf8mb4_polish_ci":      "utf8mb4_pl_0900_ai_ci",
	"utf8mb4_roman_ci":       "utf8mb4_la_0900_ai_ci",
	"utf8mb4_romanian_ci":    "utf8mb4_ro_0900_ai_ci",
	"utf8mb4_slovak_ci":      "utf8mb4_sk_0900_ai_ci",
	"utf8mb4_slovenian_ci":   "utf8mb4_sl_0900_ai_ci",
	"utf8mb4_spanish2_ci":    "utf8mb4_es_trad_0900_ai_ci",
	"utf8mb4_spanish_ci":     "utf8mb4_es_0900_ai_ci",
	"utf8mb4_swedish_ci":     "utf8mb4_sv_0900_ai_ci",
	"utf8mb4_turkish_ci":     "utf8mb4_tr_0900_ai_ci",
	"utf8mb4_vietnamese_ci":  "utf8mb4_vi_0900_ai_ci",
}

// newCollation builds a Collation from its table row, deriving how it compares
// strings from MySQL's collation naming:
//
//   - A _bin suffix, or the binary collation, compares bytes or code points.
//     That makes it Binary and sensitive to case, accents and kana.
//   - _cs and _ci name case sensitivity, _as and _ai accent sensitivity, and
//     _ks kana sensitivity.
//   - _0900_ and _520_ name the UCA version. unicode_ci and the language
//     tailorings of the Unicode charsets without one are UCA 4.0.0.
//   - A UCA _ci collation without an accent suffix ignores accents.
//   - Without _ks, a UCA _ci collation ignores kana, and a UCA 9.0.0 _as_cs
//     collation compares kana unless MySQL offers a _ks variant of it, which
//     exists because it does not. kanaVariants holds the names of those
//     variants.
//
// A legacy collation without a suffix for accents or kana, such as
// latin1_general_ci, leaves them SensitivityUnknown: its name does not decide
// them, and such collations differ.
func newCollation(row collationRow, kanaVariants map[string]bool) (*Collation, error) {
	c := &Collation{
		ID:           row.ID,
		CharsetName:  row.CharsetName,
		Name:         row.Name,
		IsDefault:    row.IsDefault,
		Sortlen:      row.Sortlen,
		PadAttribute: row.PadAttribute,
	}
	parts := strings.Split(strings.ToLower(row.Name), "_")
	suffix := caseSuffix(parts)
	if strings.EqualFold(row.Name, CollationBin) {
		suffix = "bin"
	}
	switch suffix {
	case "bin":
		c.CaseSensitive, c.Binary = true, true
		c.AccentSensitive, c.KanaSensitive = Sensitive, Sensitive
		return c, nil
	case "cs":
		c.CaseSensitive = true
	case "ci":
	default:
		return nil, fmt.Errorf("collation %q names no case sensitivity", row.Name)
	}
	c.UCAVersion = ucaVersion(row.CharsetName, parts)
	if c.UCAVersion == UCANone {
		return c, nil
	}
	c.AccentSensitive = accentSensitivity(parts, c.CaseSensitive)
	c.KanaSensitive = kanaSensitivity(c, parts, kanaVariants)
	return c, nil
}

// caseSuffix returns the last of bin, cs, or ci in a collation name's parts,
// or "" when there is none. It reads from the end: a language code such as
// Czech's "cs" follows the charset name and must not be taken for one.
func caseSuffix(parts []string) string {
	for _, part := range slices.Backward(parts) {
		switch part {
		case "bin", "cs", "ci":
			return part
		}
	}
	return ""
}

// ucaVersion returns the UCA version of a non-binary collation.
func ucaVersion(charsetName string, parts []string) UCAVersion {
	switch {
	case slices.Contains(parts, "0900"):
		return UCA900
	case slices.Contains(parts, "520"):
		return UCA520
	case !unicodeCharsets[charsetName]:
		return UCANone
	case parts[1] == "general" || parts[1] == "tolower":
		return UCANone
	default:
		return UCA400
	}
}

// accentSensitivity returns the accent sensitivity of a UCA collation.
func accentSensitivity(parts []string, caseSensitive bool) Sensitivity {
	switch {
	case suffixAfterVersion(parts, "as"):
		return Sensitive
	case suffixAfterVersion(parts, "ai"):
		return Insensitive
	case !caseSensitive:
		return Insensitive
	default:
		return SensitivityUnknown
	}
}

// kanaSensitivity returns the kana sensitivity of a UCA collation.
func kanaSensitivity(c *Collation, parts []string, kanaVariants map[string]bool) Sensitivity {
	switch {
	case suffixAfterVersion(parts, "ks"):
		return Sensitive
	case !c.CaseSensitive:
		return Insensitive
	case c.UCAVersion != UCA900:
		return SensitivityUnknown
	case kanaVariants[strings.ToLower(c.Name)+"_ks"]:
		return Insensitive
	default:
		return Sensitive
	}
}

// suffixAfterVersion reports whether suffix follows the UCA version in a
// collation name's parts. Only versioned names carry accent and kana
// suffixes, so a language code is never read as one.
func suffixAfterVersion(parts []string, suffix string) bool {
	for i, part := range parts {
		if part == "0900" || part == "520" {
			return slices.Contains(parts[i+1:], suffix)
		}
	}
	return false
}

// buildCollations builds every Collation in the table and points each obsolete
// one at the collation that replaces it.
func buildCollations(rows []collationRow) ([]*Collation, error) {
	names := make(map[string]bool, len(rows))
	for _, row := range rows {
		names[strings.ToLower(row.Name)] = true
	}
	out := make([]*Collation, 0, len(rows))
	byName := make(map[string]*Collation, len(rows))
	for _, row := range rows {
		c, err := newCollation(row, names)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
		byName[c.Name] = c
	}
	for _, c := range out {
		successor := deprecatedBy[c.Name]
		if c.CharsetName == CharsetUTF8 {
			// MySQL deprecates utf8mb3. The registry spells it utf8.
			successor = CharsetUTF8MB4 + strings.TrimPrefix(c.Name, CharsetUTF8)
		}
		if successor == "" {
			continue
		}
		next, ok := byName[successor]
		if !ok {
			if c.CharsetName == CharsetUTF8 {
				// No utf8mb4 counterpart, such as for tolower_ci.
				continue
			}
			return nil, fmt.Errorf("collation %q is deprecated by unknown collation %q", c.Name, successor)
		}
		c.DeprecatedByCollationID = next.ID
	}
	return out, nil
}
