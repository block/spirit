package statement

import "strings"

func init() { registerNormalizer(autoIncrementNotNullNormalizer{}) }

// autoIncrementNotNullNormalizer marks an AUTO_INCREMENT column NOT NULL and
// drops its DEFAULT NULL, which is what MySQL stores: the AUTO_INCREMENT
// attribute implies NOT NULL, so `id INT AUTO_INCREMENT, UNIQUE KEY (id)` is
// reported by SHOW CREATE TABLE as `id int NOT NULL AUTO_INCREMENT`, and so
// are `id INT NULL AUTO_INCREMENT` and `id INT AUTO_INCREMENT DEFAULT NULL`.
// Without this rule the authored column parses as nullable, and every diff
// against the live table emits MODIFY COLUMN id int NULL AUTO_INCREMENT: a
// full table copy, after which MySQL stores the column NOT NULL again and the
// next diff emits the same MODIFY.
//
// The implication is positional, because MySQL applies the attributes in the
// order written: AUTO_INCREMENT sets NOT NULL, and a NULL attribute written
// after it clears it again, so `id INT AUTO_INCREMENT NULL` is a nullable
// AUTO_INCREMENT column. The rule therefore forces NOT NULL only when no NULL
// follows the AUTO_INCREMENT; otherwise the parsed nullability (the last of
// NULL / NOT NULL) stands. formatColumn writes a nullable AUTO_INCREMENT
// column as `AUTO_INCREMENT NULL` for the same reason. Such a column cannot
// converge, though: SHOW CREATE TABLE reports it as `int AUTO_INCREMENT`,
// which as CREATE TABLE input means NOT NULL, so a live nullable
// AUTO_INCREMENT column is read back as NOT NULL.
//
// A primary key column is left to primaryKeyNotNullNormalizer: MySQL rejects
// an explicit NULL or DEFAULT NULL there whatever its position (error 1171),
// which that rule leaves visible for checkPrimaryKeyNullability to report, so
// the two rules never disagree on a column. The DEFAULT NULL is dropped
// whether or not the column ends up nullable: an AUTO_INCREMENT column takes
// no other default (MySQL rejects one, error 1067), and SHOW CREATE TABLE never
// reports DEFAULT NULL on one.
type autoIncrementNotNullNormalizer struct{}

func (autoIncrementNotNullNormalizer) Name() string { return "auto-increment-not-null" }

func (autoIncrementNotNullNormalizer) Normalize(ct *CreateTable) *CreateTable {
	pkColumns := primaryKeyColumnSet(ct)
	for i := range ct.Columns {
		col := &ct.Columns[i]
		if !col.AutoInc || pkColumns[strings.ToLower(col.Name)] {
			continue
		}
		if !col.declaresNullAfterAutoIncrement() {
			col.Nullable = false
		}
		if col.Default != nil && !col.DefaultIsExpr && *col.Default == "NULL" && col.DefaultKind != DefaultKindString {
			col.Default, col.DefaultAsWritten = nil, nil
		}
	}
	return ct
}
