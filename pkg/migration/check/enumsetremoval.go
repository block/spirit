package check

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/block/spirit/pkg/parser/mysql"
)

func init() {
	registerCheck("enumSetRemoval", enumSetRemovalCheck, ScopePreflight|ScopeStatement)
}

// enumSetRemovalCheck prevents unsafe ENUM/SET type conversions.
//
// The binlog replay path receives ENUM/SET values as integer ordinals
// or bitmasks from the go-mysql library. processRowsEvent decodes those
// back to their string form before handing the row image to the
// applier, so ENUM/SET → string-like target columns (VARCHAR, CHAR,
// TEXT, BLOB, etc.) are now safe.
//
// ENUM → SET is also supported: each ENUM ordinal decodes to a
// single-element string which the SET column accepts unchanged. We do
// not require every existing ENUM element to appear in the new SET —
// a defined-but-unused ENUM value is harmless, and the post-cutover
// checksum catches any row whose ENUM value really is missing from
// the new SET (it would land as the empty set and mismatch).
//
// What this check still blocks:
//
// 1. ENUM/SET → numeric (or any non-string type): even after string
// decoding, the destination would coerce "red" → 0, which is almost
// certainly not what the operator intended.
//
// 2. SET → ENUM: SET values can hold multiple elements (comma-joined)
// which an ENUM column cannot represent. Supporting this would require
// scanning the table to confirm every row has at most one element set;
// not done here — block instead.
//
// 3. SET reorder is enforced separately by setReorderCheck, which
// disallows it because the checksum compares CAST(col AS char) and the
// string order changes when bits are reordered.
func enumSetRemovalCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	haveTypes, err := requireCurrentColumnTypes(r, logger, "enumSetRemoval")
	if err != nil {
		return err
	}
	if !haveTypes {
		return nil
	}
	for _, col := range findModifiedColumns(*r.Statement.StmtNode) {
		newTp := col.ColDef.Tp.GetType()

		existingType, ok := r.Table.GetColumnMySQLType(col.LookupName)
		if !ok {
			// For columns that don't exist yet (e.g., ADD COLUMN parsed as
			// MODIFY), there's no existing type to validate against.
			continue
		}

		if !isEnumOrSetType(existingType) {
			continue // existing column is not ENUM/SET; nothing to protect
		}

		existingIsSet := isSetType(existingType)

		// SET → ENUM is rejected: a SET row can hold N elements but an
		// ENUM cell holds at most one. We don't scan to prove single-value
		// usage, so we refuse the conversion outright.
		if existingIsSet && newTp == mysql.TypeEnum {
			return fmt.Errorf("unsafe SET to ENUM type conversion on column %q is not supported. "+
				"SET columns can hold multiple elements per row, which ENUM cannot represent. "+
				"Convert via an intermediate VARCHAR if you need to migrate the data manually",
				col.LookupName)
		}

		// ENUM → SET: allowed unconditionally. Each ENUM ordinal decodes
		// to a single-element string that the SET parses against its own
		// element list; if a row's value isn't in the new SET it lands
		// as the empty set, and the post-cutover checksum catches the
		// mismatch. Pre-checking element membership would reject
		// migrations that drop defined-but-unused ENUM values, which is
		// a legitimate use case.
		if !existingIsSet && newTp == mysql.TypeSet {
			continue
		}

		// Decoding only helps when the destination accepts a string. For
		// numeric (or other non-string) destinations the decoded value
		// would be coerced — typically to 0 — silently corrupting data.
		if newTp != mysql.TypeEnum && newTp != mysql.TypeSet && !isStringTargetType(newTp) {
			typeName := "ENUM"
			if existingIsSet {
				typeName = "SET"
			}
			return fmt.Errorf("unsafe %s to %s type conversion on column %q is not supported. "+
				"%s values would be coerced from their string form to %s, "+
				"which loses the original value (typically becomes 0). "+
				"Use a string-typed target (VARCHAR, CHAR, TEXT, BLOB, etc.) instead, "+
				"or keep the column as %s",
				typeName, col.ColDef.Tp.String(), col.LookupName, typeName, col.ColDef.Tp.String(), typeName)
		}
	}
	return nil
}

// isStringTargetType reports whether a MySQL type accepts a decoded
// ENUM/SET string value without lossy coercion. CHAR/VARCHAR/TEXT and
// the BLOB family all qualify (BLOB columns store bytes verbatim).
func isStringTargetType(tp byte) bool {
	switch tp {
	case mysql.TypeVarchar,
		mysql.TypeString,     // CHAR
		mysql.TypeVarString,  // VAR_STRING (parser internal)
		mysql.TypeTinyBlob,   // TINYTEXT/TINYBLOB
		mysql.TypeMediumBlob, // MEDIUMTEXT/MEDIUMBLOB
		mysql.TypeLongBlob,   // LONGTEXT/LONGBLOB
		mysql.TypeBlob:       // TEXT/BLOB
		return true
	}
	return false
}
