package statement

import (
	"fmt"
	"strings"
)

func init() { registerNormalizer(indexNormalizer{}) }

// indexNormalizer canonicalizes a table's secondary indexes the way MySQL does
// when it stores the definition, so diffIndexes can compare two tables by
// walking ct.Indexes directly:
//
//  1. Inline column-level UNIQUE (`c INT UNIQUE`) is materialized into a
//     table-level UNIQUE index. MySQL never stores a column-level UNIQUE — it
//     canonicalizes it into a `UNIQUE KEY` named after the column (suffixed
//     _2, _3, ... on collision), which is what SHOW CREATE TABLE reports. The
//     synthesized index is tagged InlineDerived because its name is only a
//     guess at the server-assigned one.
//  2. Unnamed table-level indexes are given MySQL's default name (the first
//     column, suffixed on collision).
//
// The two steps share one name-reservation pass: the server names indexes in
// declaration order and column definitions precede table-level keys, so inline
// uniques claim their names before unnamed keys are numbered (e.g. in
// `c INT UNIQUE, KEY (c, d)` the unique gets `c` and the key is pushed to
// `c_2`). Keeping both steps in one rule is what makes that ordering reliable.
// PRIMARY KEY indexes are left untouched (MySQL always names them PRIMARY).
type indexNormalizer struct{}

func (indexNormalizer) Name() string { return "index-normalization" }

func (indexNormalizer) Normalize(ct *CreateTable) *CreateTable {
	// Track all used index names (including already-named indexes), keyed
	// lowercased because MySQL compares index names case-insensitively: in
	// `KEY X (x), KEY (x)` the unnamed key is x_2. The value is the highest _N
	// suffix seen for that base name (0 = base name only).
	usedNames := make(map[string]int)
	for i := range ct.Indexes {
		if ct.Indexes[i].Name != "" {
			usedNames[strings.ToLower(ct.Indexes[i].Name)] = 0
		}
	}

	// Step 1: materialize inline column-level UNIQUEs as table-level indexes.
	// This happens before unnamed table-level indexes are named so the unique
	// claims the column name first, matching the server's declaration order.
	for i := range ct.Columns {
		col := &ct.Columns[i]
		if !col.Unique {
			continue
		}
		name := col.Name
		for suffix := 2; ; suffix++ {
			if _, taken := usedNames[strings.ToLower(name)]; !taken {
				break
			}
			name = fmt.Sprintf("%s_%d", col.Name, suffix)
		}
		usedNames[strings.ToLower(name)] = 0
		ct.Indexes = append(ct.Indexes, Index{
			Name:          name,
			Type:          "UNIQUE",
			Columns:       []string{col.Name},
			ColumnList:    []IndexColumn{{Name: col.Name}},
			InlineDerived: true,
		})
		col.Unique = false
	}

	// Step 2: assign MySQL's default name to any unnamed non-PRIMARY index.
	for i := range ct.Indexes {
		idx := &ct.Indexes[i]
		if idx.Name != "" || idx.Type == "PRIMARY KEY" {
			continue
		}

		// Determine the base name from the first column.
		// For expression indexes the first column name may be empty;
		// fall back to a functional_index convention similar to MySQL 8.0.
		var baseName string
		switch {
		case len(idx.Columns) > 0 && idx.Columns[0] != "":
			baseName = idx.Columns[0]
		case len(idx.ColumnList) > 0 && idx.ColumnList[0].Name != "":
			baseName = idx.ColumnList[0].Name
		case len(idx.ColumnList) > 0:
			baseName = "functional_index"
		default:
			baseName = "idx"
		}

		// Find a unique name following MySQL's convention:
		// first try baseName, then baseName_2, baseName_3, ...
		candidate := baseName
		if _, taken := usedNames[strings.ToLower(candidate)]; taken {
			// Start from _2 and increment
			suffix := 2
			if prev, ok := usedNames[strings.ToLower(baseName)]; ok && prev >= 2 {
				suffix = prev + 1
			}
			for {
				candidate = fmt.Sprintf("%s_%d", baseName, suffix)
				if _, taken := usedNames[strings.ToLower(candidate)]; !taken {
					break
				}
				suffix++
			}
		}

		idx.Name = candidate
		usedNames[strings.ToLower(candidate)] = 0
		// Track the highest suffix used for this base name
		if candidate != baseName {
			// Extract suffix number
			var suffixNum int
			if _, err := fmt.Sscanf(candidate, baseName+"_%d", &suffixNum); err == nil {
				if current, ok := usedNames[strings.ToLower(baseName)]; !ok || suffixNum > current {
					usedNames[strings.ToLower(baseName)] = suffixNum
				}
			}
		}
	}
	return ct
}
