package statement

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSchemaDiffIgnoresColumnAutoIncrement locks in the behavior that the
// pre-flight schema comparison treats a column-level AUTO_INCREMENT
// difference as equivalent. This is the unsharded-source → sharded-target
// case: the source carries AUTO_INCREMENT on its primary key, while the
// sharded target intentionally drops it in favor of a Vitess sequence. That
// difference does not affect copy correctness and must not block the copy
// (move's target_state / resume_state checks), which previously reported a
// spurious "schema does not match source" mismatch.
func TestSchemaDiffIgnoresColumnAutoIncrement(t *testing.T) {
	source := "CREATE TABLE `corder` (\n" +
		"  `order_id` bigint NOT NULL AUTO_INCREMENT,\n" +
		"  `customer_id` bigint DEFAULT NULL,\n" +
		"  PRIMARY KEY (`order_id`)\n" +
		") ENGINE=InnoDB"
	target := "CREATE TABLE `corder` (\n" +
		"  `order_id` bigint NOT NULL,\n" +
		"  `customer_id` bigint DEFAULT NULL,\n" +
		"  PRIMARY KEY (`order_id`)\n" +
		") ENGINE=InnoDB"

	diff, err := SchemaDiff("corder", source, target)
	require.NoError(t, err)
	require.Empty(t, diff, "a column-level AUTO_INCREMENT difference must not be reported as a schema mismatch")
}

// TestSchemaDiffDetectsRealMismatch ensures the AUTO_INCREMENT relaxation is
// narrow: a genuine column difference (here a type change, the dangerous case
// the check exists to catch) is still reported even when AUTO_INCREMENT also
// differs.
func TestSchemaDiffDetectsRealMismatch(t *testing.T) {
	source := "CREATE TABLE `corder` (\n" +
		"  `order_id` bigint NOT NULL AUTO_INCREMENT,\n" +
		"  `sku` varbinary(128) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`order_id`)\n" +
		") ENGINE=InnoDB"
	target := "CREATE TABLE `corder` (\n" +
		"  `order_id` bigint NOT NULL,\n" +
		"  `sku` varchar(128) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`order_id`)\n" +
		") ENGINE=InnoDB"

	diff, err := SchemaDiff("corder", source, target)
	require.NoError(t, err)
	require.NotEmpty(t, diff, "a genuine column type mismatch must still be reported")
	require.Contains(t, diff, "sku", "the reported diff should name the mismatched column")
}

// TestSchemaDiffDetectsCollationMismatch pins the most dangerous pre-created
// target case: a column whose collation differs (e.g. source utf8mb4_bin vs
// target's case-insensitive default). On a primary-key column this makes
// REPLACE/INSERT IGNORE silently collapse rows that differ only by case, so
// the comparison must flag it.
func TestSchemaDiffDetectsCollationMismatch(t *testing.T) {
	source := "CREATE TABLE `t1` (\n" +
		"  `id` varchar(36) COLLATE utf8mb4_bin NOT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	target := "CREATE TABLE `t1` (\n" +
		"  `id` varchar(36) NOT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"

	diff, err := SchemaDiff("t1", source, target)
	require.NoError(t, err)
	require.NotEmpty(t, diff, "a collation mismatch must be reported")
}
