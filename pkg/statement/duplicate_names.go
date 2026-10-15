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
// positioning a repeated target column. Each kind of constraint is its own
// namespace, so a CHECK and a foreign key may share a name. Unnamed indexes
// and constraints are skipped, since MySQL names them itself.
func checkUniqueNames(ct *CreateTable) error {
	columns := make([]string, 0, len(ct.Columns))
	for _, col := range ct.Columns {
		columns = append(columns, col.Name)
	}
	if err := checkUniqueKind("column", columns); err != nil {
		return err
	}
	indexes := make([]string, 0, len(ct.Indexes))
	for _, idx := range ct.Indexes {
		indexes = append(indexes, idx.Name)
	}
	if err := checkUniqueKind("index", indexes); err != nil {
		return err
	}
	var types []string
	constraints := make(map[string][]string)
	for _, c := range ct.Constraints {
		if _, seen := constraints[c.Type]; !seen {
			types = append(types, c.Type)
		}
		constraints[c.Type] = append(constraints[c.Type], c.Name)
	}
	for _, typ := range types {
		if err := checkUniqueKind(typ+" constraint", constraints[typ]); err != nil {
			return err
		}
	}
	return nil
}

// checkUniqueKind rejects a name that appears in names more than once,
// compared case-insensitively. Empty names are skipped.
func checkUniqueKind(kind string, names []string) error {
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		if name == "" {
			continue
		}
		key := strings.ToLower(name)
		if seen[key] {
			return fmt.Errorf("%s %q is declared more than once, which MySQL rejects", kind, name)
		}
		seen[key] = true
	}
	return nil
}
