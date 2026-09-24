package lint

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/block/spirit/pkg/statement"
)

func init() {
	Register(&TypePedanticLinter{})
}

// tpDefaultAssumeCharset is the charset assumed for tables whose DDL declares
// none. MySQL 8.0's own compiled-in default is utf8mb4 (collation
// utf8mb4_0900_ai_ci), and it is the only charset spirit supports migrating
// to, so it is the best available stand-in for DDL that leaves the question to
// the server. Schemas whose server or database default is something else
// should set assumeCharset accordingly.
const tpDefaultAssumeCharset = "utf8mb4"

// TypePedanticLinter enforces type consistency across tables in the same schema.
//
// Rule 1 (same_name): Columns sharing a name across tables should share a type.
// Rule 2 (inferred_fk): Columns named like {table}_id are inferred to reference
// {table}.id and should match its type — JOINs across mismatched types force
// implicit casts and prevent index use.
//
// Both rules also compare the effective collation of text columns, since a
// collation difference breaks a join just as thoroughly as a type difference.
//
// Both rules operate on a synthesized post-state view: existing tables with
// pending CREATE TABLE / ALTER TABLE changes applied. This makes the linter
// useful both for whole-schema audits and for ALTER-driven migration flows.
type TypePedanticLinter struct {
	checkSameName   bool
	checkInferredFK bool
	checkCollation  bool
	requireIndexed  bool
	// assumedCharset/assumedCollation stand in for tables that declare no
	// charset at all; empty means "skip such tables" (see assumeCharset).
	assumedCharset    string
	assumedCollation  string
	ignoreColumns     map[string]struct{}
	fkSeverity        Severity
	sameNameSeverity  Severity
	collationSeverity Severity
}

func (l *TypePedanticLinter) Name() string { return "type_pedantic" }
func (l *TypePedanticLinter) Description() string {
	return "Cross-table column consistency: same-name columns and inferred {table}_id foreign keys should match types and collations"
}
func (l *TypePedanticLinter) String() string { return Stringer(l) }

func (l *TypePedanticLinter) DefaultConfig() map[string]string {
	return map[string]string{
		"checkSameName":     "true",
		"checkInferredFK":   "true",
		"checkCollation":    "true",
		"assumeCharset":     "utf8mb4",
		"requireIndexed":    "true",
		"ignoreColumns":     "id",
		"fkSeverity":        "warning",
		"sameNameSeverity":  "warning",
		"collationSeverity": "warning",
	}
}

// setDefaults restores all fields to their default values. Used both as the
// fallback when Lint is called before Configure, and as the prelude inside
// Configure so partial-config calls don't leave stale state from a previous
// configuration.
func (l *TypePedanticLinter) setDefaults() {
	l.checkSameName = true
	l.checkInferredFK = true
	l.checkCollation = true
	l.assumedCharset, l.assumedCollation = tpDefaultAssumedCharset()
	l.requireIndexed = true
	l.ignoreColumns = map[string]struct{}{"id": {}}
	l.fkSeverity = SeverityWarning
	l.sameNameSeverity = SeverityWarning
	l.collationSeverity = SeverityWarning
}

func (l *TypePedanticLinter) Configure(config map[string]string) error {
	// Always start from defaults so a partial-config call produces the same
	// state as a full-config call with only the overridden keys.
	l.setDefaults()
	for k, v := range config {
		switch k {
		case "checkSameName":
			b, err := ConfigBool(v, k)
			if err != nil {
				return err
			}
			l.checkSameName = b
		case "checkInferredFK":
			b, err := ConfigBool(v, k)
			if err != nil {
				return err
			}
			l.checkInferredFK = b
		case "checkCollation":
			b, err := ConfigBool(v, k)
			if err != nil {
				return err
			}
			l.checkCollation = b
		case "assumeCharset":
			cs, collation, err := tpParseAssumeCharset(v, k)
			if err != nil {
				return err
			}
			l.assumedCharset, l.assumedCollation = cs, collation
		case "requireIndexed":
			b, err := ConfigBool(v, k)
			if err != nil {
				return err
			}
			l.requireIndexed = b
		case "ignoreColumns":
			l.ignoreColumns = tpParseIgnoreList(v)
		case "fkSeverity":
			sev, err := tpParseSeverity(v, k)
			if err != nil {
				return err
			}
			l.fkSeverity = sev
		case "sameNameSeverity":
			sev, err := tpParseSeverity(v, k)
			if err != nil {
				return err
			}
			l.sameNameSeverity = sev
		case "collationSeverity":
			sev, err := tpParseSeverity(v, k)
			if err != nil {
				return err
			}
			l.collationSeverity = sev
		default:
			return fmt.Errorf("unknown config key for %s: %s", l.Name(), k)
		}
	}
	return nil
}

func tpParseIgnoreList(value string) map[string]struct{} {
	out := make(map[string]struct{})
	for c := range strings.SplitSeq(value, ",") {
		c = strings.ToLower(strings.TrimSpace(c))
		if c != "" {
			out[c] = struct{}{}
		}
	}
	return out
}

// tpDefaultAssumedCharset resolves the assumeCharset default. The value is a
// constant, so a failure here would be a programming error rather than bad
// user input; fall back to skipping undeclared tables if it ever happens.
func tpDefaultAssumedCharset() (charset, collation string) {
	cs, collation, ok := statement.DefaultCollationForCharset(tpDefaultAssumeCharset)
	if !ok {
		return "", ""
	}
	return cs, collation
}

// tpParseAssumeCharset resolves a configured charset name to the (charset,
// collation) pair to stand in for tables that declare neither. The empty
// string disables the fallback, restoring a strict "only compare what the DDL
// states" mode.
func tpParseAssumeCharset(value, key string) (charset, collation string, err error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return "", "", nil
	}
	cs, collation, ok := statement.DefaultCollationForCharset(value)
	if !ok {
		return "", "", fmt.Errorf("invalid value for %s: %s (not a known character set)", key, value)
	}
	return cs, collation, nil
}

func tpParseSeverity(value, key string) (Severity, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "error":
		return SeverityError, nil
	case "warning":
		return SeverityWarning, nil
	case "info":
		return SeverityInfo, nil
	default:
		return 0, fmt.Errorf("invalid value for %s: %s (expected error, warning, or info)", key, value)
	}
}

// tpCanonicalType returns a comparable string representation of a column's
// full type — type name, length, precision and signedness, as
// information_schema would spell it. Charset and collation are deliberately
// absent: they only reach this string through the binary-vs-text type names
// (varbinary, blob), and are compared separately by tpEffectiveCollation so
// that an undeterminable collation can be skipped rather than mistaken for a
// difference.
func tpCanonicalType(col *statement.Column) string {
	if col.Raw != nil && col.Raw.Tp != nil {
		return col.Raw.Tp.InfoSchemaStr()
	}
	return col.Type
}

// tpEffectiveCollation returns the charset and collation a text column
// compares under, or ("", "") when the column carries no charset or the
// collation cannot be determined. A charset difference always shows up as a
// collation difference too — collation names are charset-prefixed — so a
// single comparison on collation covers both; only the wording of the
// resulting violation needs to tell them apart, because the consequences do
// differ (see tpCollationConsequence).
//
// DDL that declares no charset anywhere falls back to assumeCharset, because
// otherwise deleting a table's DEFAULT CHARSET line would make the linter
// quieter: the table drops out of its comparison group, and a group left with
// one determined column has nothing left to disagree about. A charset that the
// parser doesn't recognize is still skipped — there is no default to look up,
// and assuming one would compare a real declaration against a guess.
func (l *TypePedanticLinter) tpEffectiveCollation(t *statement.CreateTable, col *statement.Column) (charset, collation string) {
	if !col.CarriesCharset() {
		return "", ""
	}
	charset, collation = col.EffectiveCharsetCollation(t)
	if charset == "" && collation == "" {
		return l.assumedCharset, l.assumedCollation
	}
	if collation == "" {
		return "", ""
	}
	return charset, collation
}

// tpCollationConsequence explains what a collation difference costs, which
// depends on whether the charsets differ too.
//
// Differing charsets have two possible outcomes and the wording has to cover
// both. When one charset converts to the other, MySQL converts and the index
// on the wider side survives — it is the narrower side's index that goes
// unused. When neither is a superset of the other (latin1 vs latin2, say)
// there is nothing to convert to, and the comparison fails with ERROR 1267
// exactly like the same-charset case. Deciding which applies would mean
// reproducing MySQL's charset-superset table, so one sentence names both
// rather than claiming the milder one.
func tpCollationConsequence(charsetA, charsetB string) string {
	if charsetA != charsetB {
		return "joining columns with different charsets forces an implicit conversion — it prevents index use on the narrower side, and fails outright with ERROR 1267 (Illegal mix of collations) when neither charset converts to the other"
	}
	return "comparing columns with different collations of the same charset fails with ERROR 1267 (Illegal mix of collations)"
}

// tpTieConsequence is tpCollationConsequence for a tied vote, where more than
// two collations may be in play. Any pair of differing charsets in the set
// gives the group its consequence; if they all share one charset, the
// same-charset wording applies.
func tpTieConsequence(distinct []string, charsetOf map[string]string) string {
	first := charsetOf[distinct[0]]
	for _, collation := range distinct[1:] {
		if charsetOf[collation] != first {
			return tpCollationConsequence(first, charsetOf[collation])
		}
	}
	return tpCollationConsequence(first, first)
}

func (l *TypePedanticLinter) Lint(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) (violations []Violation) {
	if l.ignoreColumns == nil {
		l.setDefaults()
	}

	tables := PostState(existingTables, changes)
	tableByName := make(map[string]*statement.CreateTable, len(tables))
	for _, t := range tables {
		tableByName[strings.ToLower(t.TableName)] = t
	}

	if l.checkSameName {
		violations = append(violations, l.lintSameName(tables)...)
	}
	if l.checkInferredFK {
		violations = append(violations, l.lintInferredFK(tables, tableByName)...)
	}

	return violations
}

// tpCollectIndexedColumns returns the lower-cased set of every column that
// participates in any index on the table. Uses GetIndexes() so that inline
// column-level PRIMARY KEY and UNIQUE declarations are honored — those don't
// appear in the raw Indexes slice.
func tpCollectIndexedColumns(t *statement.CreateTable) map[string]struct{} {
	out := make(map[string]struct{})
	for _, idx := range t.GetIndexes() {
		for _, col := range idx.Columns {
			out[strings.ToLower(col)] = struct{}{}
		}
	}
	return out
}

type tpColRef struct {
	table *statement.CreateTable
	col   *statement.Column
	typ   string
	// charset and collation are the column's effective values, or "" when it
	// carries no charset or the statement leaves them underdetermined.
	charset   string
	collation string
}

func (l *TypePedanticLinter) lintSameName(tables []*statement.CreateTable) []Violation {
	// Precompute per-table indexed-column sets keyed by lower-cased table name.
	indexedByTable := make(map[string]map[string]struct{}, len(tables))
	if l.requireIndexed {
		for _, t := range tables {
			indexedByTable[strings.ToLower(t.TableName)] = tpCollectIndexedColumns(t)
		}
	}

	byName := make(map[string][]tpColRef)
	hasIndexed := make(map[string]bool)
	for _, t := range tables {
		tLower := strings.ToLower(t.TableName)
		for i := range t.Columns {
			c := &t.Columns[i]
			lower := strings.ToLower(c.Name)
			if _, skip := l.ignoreColumns[lower]; skip {
				continue
			}
			ref := tpColRef{
				table: t,
				col:   c,
				typ:   tpCanonicalType(c),
			}
			if l.checkCollation {
				ref.charset, ref.collation = l.tpEffectiveCollation(t, c)
			}
			byName[lower] = append(byName[lower], ref)
			if l.requireIndexed {
				if _, ok := indexedByTable[tLower][lower]; ok {
					hasIndexed[lower] = true
				}
			}
		}
	}

	names := slices.Sorted(maps.Keys(byName))

	var violations []Violation
	for _, name := range names {
		refs := byName[name]
		if len(refs) < 2 {
			continue
		}
		if l.requireIndexed && !hasIndexed[name] {
			continue
		}
		violations = append(violations, l.sameNameTypes(refs)...)
		if l.checkCollation {
			violations = append(violations, l.sameNameCollations(refs)...)
		}
	}
	return violations
}

// sameNameTypes implements Rule 1's type comparison across one group of
// same-named columns.
func (l *TypePedanticLinter) sameNameTypes(refs []tpColRef) []Violation {
	typeCounts := make(map[string]int)
	typeTables := make(map[string][]string)
	for _, r := range refs {
		typeCounts[r.typ]++
		typeTables[r.typ] = append(typeTables[r.typ], r.table.TableName)
	}
	if len(typeCounts) == 1 {
		return nil
	}

	var violations []Violation
	majority, clear := tpPickMajority(typeCounts)
	if clear {
		majorityTables := tpDedupeStrings(typeTables[majority])
		for _, r := range refs {
			if r.typ == majority {
				continue
			}
			colName := r.col.Name
			example := strings.Join(tpFirstN(majorityTables, 3), ", ")
			violations = append(violations, Violation{
				Linter:   l,
				Severity: l.sameNameSeverity,
				Message: fmt.Sprintf(
					"Column %q in table %q has type %q but %d other table(s) use type %q (e.g. %s)",
					r.col.Name, r.table.TableName, r.typ, len(majorityTables), majority, example,
				),
				Location:   &Location{Table: r.table.TableName, Column: &colName},
				Suggestion: new(fmt.Sprintf("Align %s.%s to type %q for consistency", r.table.TableName, r.col.Name, majority)),
				Context: map[string]any{
					"current_type":  r.typ,
					"expected_type": majority,
					"rule":          "same_name",
				},
			})
		}
		return violations
	}

	// Tied top counts — no canonical "right" type. Report every occurrence
	// as inconsistent, listing the conflicting types so the user can decide.
	distinct := slices.Sorted(maps.Keys(typeCounts))
	for _, r := range refs {
		colName := r.col.Name
		violations = append(violations, Violation{
			Linter:   l,
			Severity: l.sameNameSeverity,
			Message: fmt.Sprintf(
				"Column %q in table %q has type %q; inconsistent across schema (types in use: %s)",
				r.col.Name, r.table.TableName, r.typ, quoteJoin(distinct),
			),
			Location:   &Location{Table: r.table.TableName, Column: &colName},
			Suggestion: new(fmt.Sprintf("Pick one canonical type for column %q across all tables; the larger/safer type is usually right", r.col.Name)),
			Context: map[string]any{
				"current_type":      r.typ,
				"conflicting_types": distinct,
				"rule":              "same_name",
			},
		})
	}
	return violations
}

// sameNameCollations implements Rule 1's collation comparison across one group
// of same-named columns. Refs whose effective collation is undeterminable —
// non-text columns, and text columns on a table with no DEFAULT CHARSET — drop
// out of the vote entirely rather than forming a bucket of their own, so an
// unwritten charset is never reported as a difference.
func (l *TypePedanticLinter) sameNameCollations(refs []tpColRef) []Violation {
	determined := make([]tpColRef, 0, len(refs))
	counts := make(map[string]int)
	tablesByCollation := make(map[string][]string)
	charsetOf := make(map[string]string)
	for _, r := range refs {
		if r.collation == "" {
			continue
		}
		determined = append(determined, r)
		counts[r.collation]++
		tablesByCollation[r.collation] = append(tablesByCollation[r.collation], r.table.TableName)
		charsetOf[r.collation] = r.charset
	}
	if len(determined) < 2 || len(counts) == 1 {
		return nil
	}

	var violations []Violation
	majority, clear := tpPickMajority(counts)
	if clear {
		majorityTables := tpDedupeStrings(tablesByCollation[majority])
		for _, r := range determined {
			if r.collation == majority {
				continue
			}
			colName := r.col.Name
			example := strings.Join(tpFirstN(majorityTables, 3), ", ")
			violations = append(violations, Violation{
				Linter:   l,
				Severity: l.collationSeverity,
				Message: fmt.Sprintf(
					"Column %q in table %q uses collation %q but %d other table(s) use %q (e.g. %s) — %s",
					r.col.Name, r.table.TableName, r.collation, len(majorityTables), majority, example,
					tpCollationConsequence(r.charset, charsetOf[majority]),
				),
				Location: &Location{Table: r.table.TableName, Column: &colName},
				Suggestion: new(fmt.Sprintf(
					"Convert %s.%s to CHARACTER SET %s COLLATE %s for consistency",
					r.table.TableName, r.col.Name, charsetOf[majority], majority,
				)),
				Context: map[string]any{
					"current_collation":  r.collation,
					"expected_collation": majority,
					"current_charset":    r.charset,
					"expected_charset":   charsetOf[majority],
					"charset_differs":    r.charset != charsetOf[majority],
					"rule":               "same_name_collation",
				},
			})
		}
		return violations
	}

	// Tied top counts — same reasoning as the type rule: report every
	// occurrence rather than picking a winner alphabetically.
	distinct := slices.Sorted(maps.Keys(counts))
	// A tie is what the most ordinary mixed-charset legacy schema produces —
	// one latin1 table, one utf8mb4 table, no majority — so it needs the
	// consequence clause just as much as the majority branch.
	consequence := tpTieConsequence(distinct, charsetOf)
	for _, r := range determined {
		colName := r.col.Name
		violations = append(violations, Violation{
			Linter:   l,
			Severity: l.collationSeverity,
			Message: fmt.Sprintf(
				"Column %q in table %q uses collation %q; inconsistent across schema (collations in use: %s) — %s",
				r.col.Name, r.table.TableName, r.collation, quoteJoin(distinct), consequence,
			),
			Location:   &Location{Table: r.table.TableName, Column: &colName},
			Suggestion: new(fmt.Sprintf("Pick one canonical collation for column %q across all tables", r.col.Name)),
			Context: map[string]any{
				"current_collation":      r.collation,
				"current_charset":        r.charset,
				"conflicting_collations": distinct,
				"rule":                   "same_name_collation",
			},
		})
	}
	return violations
}

func (l *TypePedanticLinter) lintInferredFK(tables []*statement.CreateTable, tableByName map[string]*statement.CreateTable) []Violation {
	var violations []Violation
	for _, t := range tables {
		for i := range t.Columns {
			c := &t.Columns[i]
			lower := strings.ToLower(c.Name)
			if _, skip := l.ignoreColumns[lower]; skip {
				continue
			}
			if !strings.HasSuffix(lower, "_id") {
				continue
			}
			base := lower[:len(lower)-len("_id")]
			if base == "" {
				continue
			}
			target := tpFindFKTarget(tableByName, base, t.TableName)
			if target == nil {
				continue
			}
			idCol := tpFindIDColumn(target)
			if idCol == nil {
				continue
			}
			colName := c.Name
			colType := tpCanonicalType(c)
			idType := tpCanonicalType(idCol)
			if colType != idType {
				violations = append(violations, Violation{
					Linter:   l,
					Severity: l.fkSeverity,
					Message: fmt.Sprintf(
						"Column %q in table %q has type %q but inferred FK target %q.id has type %q",
						c.Name, t.TableName, colType, target.TableName, idType,
					),
					Location: &Location{Table: t.TableName, Column: &colName},
					Suggestion: new(fmt.Sprintf(
						"Align types: %s.%s (%q) and %s.id (%q) should match — grow the smaller side rather than shrink the larger",
						t.TableName, c.Name, colType, target.TableName, idType,
					)),
					Context: map[string]any{
						"current_type":     colType,
						"expected_type":    idType,
						"referenced_table": target.TableName,
						"rule":             "inferred_fk",
					},
				})
			}
			// The collation check is independent of the type check: a
			// varchar FK can match its target's width exactly and still be
			// unjoinable because the two sides collate differently.
			if !l.checkCollation {
				continue
			}
			colCharset, colCollation := l.tpEffectiveCollation(t, c)
			idCharset, idCollation := l.tpEffectiveCollation(target, idCol)
			if colCollation == "" || idCollation == "" || colCollation == idCollation {
				continue
			}
			violations = append(violations, Violation{
				Linter:   l,
				Severity: l.collationSeverity,
				Message: fmt.Sprintf(
					"Column %q in table %q uses collation %q but inferred FK target %q.id uses %q — %s",
					c.Name, t.TableName, colCollation, target.TableName, idCollation,
					tpCollationConsequence(colCharset, idCharset),
				),
				Location: &Location{Table: t.TableName, Column: &colName},
				Suggestion: new(fmt.Sprintf(
					"Convert %s.%s to CHARACTER SET %s COLLATE %s to match %s.id",
					t.TableName, c.Name, idCharset, idCollation, target.TableName,
				)),
				Context: map[string]any{
					"current_collation":  colCollation,
					"expected_collation": idCollation,
					"current_charset":    colCharset,
					"expected_charset":   idCharset,
					"charset_differs":    colCharset != idCharset,
					"referenced_table":   target.TableName,
					"rule":               "inferred_fk_collation",
				},
			})
		}
	}
	return violations
}

// tpFindFKTarget tries common pluralization variants of base to locate a
// candidate referenced table. Skips self-references.
func tpFindFKTarget(tables map[string]*statement.CreateTable, base, selfName string) *statement.CreateTable {
	selfLower := strings.ToLower(selfName)
	for _, name := range tpPluralCandidates(base) {
		if name == selfLower {
			continue
		}
		if t, ok := tables[name]; ok {
			return t
		}
	}
	return nil
}

// tpPluralCandidates returns plausible table-name forms for an FK base.
// Order matters: the literal base comes first, then +s, then +es, then y→ies.
// This covers:
//
//	customer    → [customer, customers]
//	address     → [address, addresses]            (s-stem: +es)
//	process     → [process, processes]            (s-stem: +es)
//	bus         → [bus, buses]                    (s-stem: +es)
//	box         → [box, boxs, boxes]              (x-stem: +es; boxs is harmless noise)
//	tomato      → [tomato, tomatos, tomatoes]     (o-stem: +es)
//	category    → [category, categorys, categories] (y-stem: +ies)
//	city        → [city, citys, cities]
func tpPluralCandidates(base string) []string {
	if base == "" {
		return nil
	}
	out := []string{base}
	if !strings.HasSuffix(base, "s") {
		out = append(out, base+"s")
	}
	switch {
	case strings.HasSuffix(base, "s"),
		strings.HasSuffix(base, "x"),
		strings.HasSuffix(base, "z"),
		strings.HasSuffix(base, "ch"),
		strings.HasSuffix(base, "sh"),
		strings.HasSuffix(base, "o"):
		out = append(out, base+"es")
	}
	if strings.HasSuffix(base, "y") && len(base) > 1 {
		out = append(out, base[:len(base)-1]+"ies")
	}
	return out
}

func tpFindIDColumn(t *statement.CreateTable) *statement.Column {
	for i := range t.Columns {
		if strings.EqualFold(t.Columns[i].Name, "id") {
			return &t.Columns[i]
		}
	}
	return nil
}

// tpPickMajority returns (winningType, true) when one type strictly dominates,
// or ("", false) when the top count is tied between two or more types.
func tpPickMajority(counts map[string]int) (string, bool) {
	if len(counts) == 0 {
		return "", false
	}
	keys := slices.Sorted(maps.Keys(counts))

	var first string
	firstCount, secondCount := -1, -1
	for _, k := range keys {
		c := counts[k]
		switch {
		case c > firstCount:
			secondCount = firstCount
			firstCount = c
			first = k
		case c > secondCount:
			secondCount = c
		}
	}
	if firstCount > secondCount {
		return first, true
	}
	return "", false
}

func tpDedupeStrings(ss []string) []string {
	return slices.Compact(slices.Sorted(slices.Values(ss)))
}

func tpFirstN(s []string, n int) []string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
