package statement

import (
	"fmt"
)

func init() { registerNormalizer(columnCheckNormalizer{}) }

// columnCheckNormalizer hoists column-level CHECK constraints into table-level
// constraints, mirroring what MySQL does in SHOW CREATE TABLE. Without this,
// a column-level CHECK lives only in Column.Checks while the live table (after
// the ALTER is applied) reports the same constraint in CreateTable.Constraints.
// A re-diff would then (a) keep emitting MODIFY COLUMN because Column.Checks
// differs and (b) try to DROP the live CHECK because it is absent from the
// target's Constraints. Hoisting at parse time keeps the parsed form canonical
// so the re-diff converges and no constraint is ever dropped.
//
// Every CHECK a column carries is hoisted, with its name and its enforcement:
// `c INT CHECK (c > 0) CHECK (c < 10)` is two constraints, and
// `CHECK (c > 0) NOT ENFORCED` stays NOT ENFORCED. Keeping only the last one,
// or dropping the enforcement, would make a diff silently remove a constraint
// the live table has, or start enforcing one the user declared unenforced.
//
// MySQL auto-names unnamed CHECK constraints `<table>_chk_<n>`. We replicate
// that here: user-named CHECKs keep their name; unnamed ones are numbered in
// the order they are encountered (existing table-level CHECKs first, then the
// hoisted column-level CHECKs in column order).
//
// Naming caveat: MySQL numbers CHECKs in true declaration order, interleaving
// column-level and table-level CHECKs by their position in the statement. The
// TiDB parser does not expose reliable per-node source offsets, so when a table
// mixes column-level and table-level CHECKs our generated `_chk_<n>` numbers can
// differ from MySQL's. This is purely cosmetic and never causes a constraint to
// be dropped or a re-diff to diverge: diffConstraints pairs CHECK constraints by
// expression when the names differ (see matchedByExpression in diffConstraints),
// so the round-trip still converges. The common case — a table whose CHECKs are
// all column-level — numbers identically to MySQL.
type columnCheckNormalizer struct{}

func (columnCheckNormalizer) Name() string { return "column-checks" }

func (columnCheckNormalizer) Normalize(ct *CreateTable) *CreateTable {
	// Track names already in use so generated names never collide with a
	// user-supplied constraint name.
	usedNames := make(map[string]bool)
	for i := range ct.Constraints {
		if ct.Constraints[i].Name != "" {
			usedNames[ct.Constraints[i].Name] = true
		}
	}

	// Append a hoisted table-level CHECK for each column-level CHECK, then
	// clear the column-level field so it no longer participates in column
	// equality or column-definition emission.
	for i := range ct.Columns {
		col := &ct.Columns[i]
		for _, check := range col.Checks {
			expr := check.Expression
			c := Constraint{
				Name:        check.Name,
				Type:        "CHECK",
				Expression:  &expr,
				NotEnforced: check.NotEnforced,
			}
			definition := checkConstraintDefinition(&c)
			c.Definition = &definition
			ct.Constraints = append(ct.Constraints, c)
			if check.Name != "" {
				usedNames[check.Name] = true
			}
		}
		col.Checks = nil
	}

	// Number the unnamed CHECK constraints `<table>_chk_<n>`. Resolve indices
	// (not pointers) after all appends are complete so a slice reallocation
	// during the append loop above cannot leave us with dangling references.
	counter := 0
	for i := range ct.Constraints {
		c := &ct.Constraints[i]
		if c.Type != "CHECK" || c.Name != "" {
			continue
		}
		var name string
		for {
			counter++
			name = fmt.Sprintf("%s_chk_%d", ct.TableName, counter)
			if !usedNames[name] {
				break
			}
		}
		usedNames[name] = true
		c.Name = name
	}
	return ct
}
