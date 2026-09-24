package check

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/block/spirit/pkg/parser/mysql"
)

func init() {
	registerCheck("setReorder", setReorderCheck, ScopePreflight|ScopeStatement)
}

// setReorderCheck prevents SET value reordering in all modes.
//
// MySQL outputs SET member strings in the order they are defined in the column
// definition. When SET members are reordered, the string representation of
// multi-member values changes (e.g. "read,execute" becomes "execute,read"),
// even though the actual data is correct. This causes Spirit's CRC32 checksum
// to fail because it compares CAST(col AS char) representations, which differ
// between source and target tables. The migration would always fail at the
// checksum phase.
//
// Since checksums are compulsory: it's better to fast-fail than get to the end
// and find out that the data is "corrupted".
//
// Adding new SET values at the end of the list is always safe. The new list must
// start with the existing values as a prefix.
func setReorderCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	haveTypes, err := requireCurrentColumnTypes(r, logger, "setReorder")
	if err != nil {
		return err
	}
	if !haveTypes {
		return nil
	}
	for _, col := range findModifiedEnumSetColumns(*r.Statement.StmtNode) {
		if col.ColDef.Tp.GetType() != mysql.TypeSet {
			continue // handled by enumReorderCheck
		}

		newElems := col.ColDef.Tp.GetElems()
		if len(newElems) == 0 {
			continue
		}

		existingType, ok := r.Table.GetColumnMySQLType(col.LookupName)
		if !ok {
			return cannotClassify("unable to validate SET reorder for column %q: existing column type not found in table metadata", col.LookupName)
		}

		// The SET reorder check only applies when the existing column is
		// also a SET. If the existing column is a different type (e.g.,
		// VARCHAR → SET or ENUM → SET), the reorder check is not applicable.
		// Cross-type conversions like ENUM → SET are caught by enumSetRemovalCheck.
		if !isSetType(existingType) {
			continue
		}

		existingElems, err := parseEnumSetValues(existingType)
		if err != nil {
			return cannotClassify("unable to validate SET reorder for column %q: %w", col.LookupName, err)
		}
		if len(existingElems) == 0 {
			continue
		}

		if !isPrefix(existingElems, newElems) {
			return fmt.Errorf("unsafe SET value reorder on column %q. "+
				"Reordering SET members changes the string representation of multi-member values "+
				"(e.g. 'read,execute' becomes 'execute,read'), which causes the checksum to fail. "+
				"Only add new values at the end of the list",
				col.LookupName)
		}
	}
	return nil
}
