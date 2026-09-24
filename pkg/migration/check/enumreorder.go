package check

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/block/spirit/pkg/parser/mysql"
)

func init() {
	registerCheck("enumReorder", enumReorderCheck, ScopePreflight|ScopeStatement)
}

// enumReorderCheck prevents ENUM value reordering and middle-insertion.
//
// The binlog replay path (bufferedMap) receives ENUM values as integer
// ordinals from the go-mysql library. Spirit decodes those ordinals to
// their string form against the SOURCE table's element list before
// applying them to the target, so two kinds of change are safe:
//
//   - Dropping existing values from anywhere in the list. Retained values
//     keep their string form; rows that held a dropped value land in the
//     target as ” (the empty string — MySQL's invalid-ENUM sentinel) and
//     the post-cutover checksum catches the mismatch.
//   - Appending new values at the end of the list.
//
// The unsafe shapes — reordering (a kept value changes its relative
// position) and middle-insertion (a brand-new value appears before a
// retained existing value) — are refused because they break the
// "kept values keep their relative order" invariant that the decode path
// relies on.
func enumReorderCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	haveTypes, err := requireCurrentColumnTypes(r, logger, "enumReorder")
	if err != nil {
		return err
	}
	if !haveTypes {
		return nil
	}
	for _, col := range findModifiedEnumSetColumns(*r.Statement.StmtNode) {
		if col.ColDef.Tp.GetType() == mysql.TypeSet {
			continue // handled by setReorderCheck
		}

		newElems := col.ColDef.Tp.GetElems()
		if len(newElems) == 0 {
			continue
		}

		existingType, ok := r.Table.GetColumnMySQLType(col.LookupName)
		if !ok {
			return cannotClassify("unable to validate ENUM change for column %q: existing column type not found in table metadata", col.LookupName)
		}

		// The ENUM reorder check only applies when the existing column is
		// also an ENUM. If the existing column is a different type (e.g.,
		// VARCHAR → ENUM or SET → ENUM), the reorder check is not applicable.
		// Cross-type conversions like SET → ENUM are caught by enumSetRemovalCheck.
		if !isEnumType(existingType) {
			continue
		}

		existingElems, err := parseEnumSetValues(existingType)
		if err != nil {
			return cannotClassify("unable to validate ENUM change for column %q: %w", col.LookupName, err)
		}
		if len(existingElems) == 0 {
			continue
		}

		if !isCompatibleEnumChange(existingElems, newElems) {
			return fmt.Errorf("unsafe ENUM value reorder on column %q is not supported. "+
				"The binlog replay path uses integer ordinals for ENUM values, so retained values "+
				"must keep their relative order and new values must be appended at the end. "+
				"Dropping existing values from anywhere in the list is allowed",
				col.LookupName)
		}
	}
	return nil
}
