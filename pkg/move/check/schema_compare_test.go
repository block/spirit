package check

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSchemaDiffIgnoresColumnAutoIncrement locks in the behavior that the
// move-tables schema comparisons treat a column-level AUTO_INCREMENT
// difference as equivalent. This is the unsharded-source → sharded-target
// case: the source carries AUTO_INCREMENT on its primary key, while the
// sharded target intentionally drops it in favor of a Vitess sequence. That
// difference does not affect copy correctness and must not block the move
// (target_state / resume_state checks), which previously reported a spurious
// "schema does not match source" mismatch.
//
// The relaxation lives in the shared base options, so it holds for both the
// source↔source and the source→target comparison.
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

	diff, err := schemaDiff("corder", source, target)
	require.NoError(t, err)
	require.Empty(t, diff, "a column-level AUTO_INCREMENT difference must not be reported as a schema mismatch")

	diff, err = targetSchemaDiff("corder", source, target)
	require.NoError(t, err)
	require.Empty(t, diff, "the source→target comparison must ignore column AUTO_INCREMENT too")
}

// TestTargetSchemaDiffIgnoresNotNullOnTarget covers the other way a sharded
// target is deliberately stricter than its unsharded source: the shard key is
// NOT NULL on the target because a Vitess primary vindex cannot map NULL to a
// keyspace id, while the source still permits NULL because the ALTER to
// tighten it was never affordable on a multi-terabyte table. The move must be
// allowed to proceed into that target.
func TestTargetSchemaDiffIgnoresNotNullOnTarget(t *testing.T) {
	source := "CREATE TABLE `corder` (\n" +
		"  `order_id` bigint NOT NULL AUTO_INCREMENT,\n" +
		"  `customer_id` bigint DEFAULT NULL,\n" +
		"  PRIMARY KEY (`order_id`)\n" +
		") ENGINE=InnoDB"
	target := "CREATE TABLE `corder` (\n" +
		"  `order_id` bigint NOT NULL,\n" +
		"  `customer_id` bigint NOT NULL,\n" +
		"  PRIMARY KEY (`order_id`)\n" +
		") ENGINE=InnoDB"

	diff, err := targetSchemaDiff("corder", source, target)
	require.NoError(t, err)
	require.Empty(t, diff, "a target that tightens a nullable source column must not be reported as a mismatch")
}

// TestTargetSchemaDiffRejectsNullableTarget pins the direction. Forgiving a
// stricter target must not become forgiving a weaker one: a target that lost a
// NOT NULL the source had can accept rows the source never could, and that is
// a real mismatch the check has to keep reporting.
func TestTargetSchemaDiffRejectsNullableTarget(t *testing.T) {
	source := "CREATE TABLE `corder` (\n" +
		"  `order_id` bigint NOT NULL,\n" +
		"  `customer_id` bigint NOT NULL,\n" +
		"  PRIMARY KEY (`order_id`)\n" +
		") ENGINE=InnoDB"
	target := "CREATE TABLE `corder` (\n" +
		"  `order_id` bigint NOT NULL,\n" +
		"  `customer_id` bigint DEFAULT NULL,\n" +
		"  PRIMARY KEY (`order_id`)\n" +
		") ENGINE=InnoDB"

	diff, err := targetSchemaDiff("corder", source, target)
	require.NoError(t, err)
	require.NotEmpty(t, diff, "a target that drops a NOT NULL the source had must still be reported")
	require.Contains(t, diff, "customer_id", "the reported diff should name the mismatched column")
}

// TestSchemaDiffRejectsNullabilityBetweenSources keeps the nullability
// relaxation out of the source↔source comparison. Sources of one move must be
// identical — tolerating a tightening there would make the verdict depend on
// which source happened to sort first, and would let genuine drift between
// shards pass as expected divergence.
func TestSchemaDiffRejectsNullabilityBetweenSources(t *testing.T) {
	nullable := "CREATE TABLE `corder` (\n" +
		"  `order_id` bigint NOT NULL,\n" +
		"  `customer_id` bigint DEFAULT NULL,\n" +
		"  PRIMARY KEY (`order_id`)\n" +
		") ENGINE=InnoDB"
	notNull := "CREATE TABLE `corder` (\n" +
		"  `order_id` bigint NOT NULL,\n" +
		"  `customer_id` bigint NOT NULL,\n" +
		"  PRIMARY KEY (`order_id`)\n" +
		") ENGINE=InnoDB"

	diff, err := schemaDiff("corder", nullable, notNull)
	require.NoError(t, err)
	require.NotEmpty(t, diff, "nullability drift between sources must be reported")

	diff, err = schemaDiff("corder", notNull, nullable)
	require.NoError(t, err)
	require.NotEmpty(t, diff, "nullability drift between sources must be reported in either order")
}

// TestTargetSchemaDiffNarrowNotNullRelaxation ensures the nullability
// relaxation cannot smuggle anything else past the check: a tightened column
// that also changes type is still a mismatch.
func TestTargetSchemaDiffNarrowNotNullRelaxation(t *testing.T) {
	source := "CREATE TABLE `corder` (\n" +
		"  `order_id` bigint NOT NULL,\n" +
		"  `customer_id` bigint DEFAULT NULL,\n" +
		"  PRIMARY KEY (`order_id`)\n" +
		") ENGINE=InnoDB"
	target := "CREATE TABLE `corder` (\n" +
		"  `order_id` bigint NOT NULL,\n" +
		"  `customer_id` int NOT NULL,\n" +
		"  PRIMARY KEY (`order_id`)\n" +
		") ENGINE=InnoDB"

	diff, err := targetSchemaDiff("corder", source, target)
	require.NoError(t, err)
	require.NotEmpty(t, diff, "a type change on a tightened column must still be reported")
	require.Contains(t, diff, "customer_id", "the reported diff should name the mismatched column")
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

	diff, err := schemaDiff("corder", source, target)
	require.NoError(t, err)
	require.NotEmpty(t, diff, "a genuine column type mismatch must still be reported")
	require.Contains(t, diff, "sku", "the reported diff should name the mismatched column")
}
