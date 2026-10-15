package statement

import (
	"fmt"
	"strings"
)

// checkUniqueNames rejects a table that declares a column, index, or
// constraint name more than once, which MySQL refuses to create. MySQL
// compares these names case-insensitively. Diff matches elements by name, so
// a repeated one has no single definition to diff: it would emit an ALTER
// MySQL rejects, drop nothing for a repeated source column, or fail while
// positioning a repeated target column. Unnamed indexes and constraints are
// skipped, since MySQL names them itself.
func checkUniqueNames(ct *CreateTable) error {
	kinds := []struct {
		kind  string
		names []string
	}{
		{"column", make([]string, 0, len(ct.Columns))},
		{"index", make([]string, 0, len(ct.Indexes))},
		{"constraint", make([]string, 0, len(ct.Constraints))},
	}
	for _, col := range ct.Columns {
		kinds[0].names = append(kinds[0].names, col.Name)
	}
	for _, idx := range ct.Indexes {
		kinds[1].names = append(kinds[1].names, idx.Name)
	}
	for _, c := range ct.Constraints {
		kinds[2].names = append(kinds[2].names, c.Name)
	}
	for _, k := range kinds {
		seen := make(map[string]bool, len(k.names))
		for _, name := range k.names {
			if name == "" {
				continue
			}
			key := strings.ToLower(name)
			if seen[key] {
				return fmt.Errorf("%s %q is declared more than once, which MySQL rejects", k.kind, name)
			}
			seen[key] = true
		}
	}
	return nil
}
