package statement

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDiff uses a table-driven approach to test various diff scenarios
func TestDiff(t *testing.T) {
	tests := []struct {
		name     string
		source   string
		target   string
		expected string // empty string means no diff expected
		// expectedStatements, when set, asserts the full ordered list of
		// emitted statements. Use it for diffs that intentionally produce more
		// than one statement (e.g. option-only index changes split into a
		// separate DROP and ADD). When set, expected is ignored.
		expectedStatements []string
	}{
		{
			name:     "PrimaryKeyCommentChange",
			source:   "CREATE TABLE t1 (id INT, PRIMARY KEY (id) COMMENT 'before')",
			target:   "CREATE TABLE t1 (id INT, PRIMARY KEY (id) COMMENT 'after')",
			expected: "ALTER TABLE `t1` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`) COMMENT 'after'",
		},
		{
			name:     "NoChanges",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			expected: "",
		},
		{
			name:     "NoChanges_DatetimeWithPrecision",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3))",
			expected: "",
		},
		{
			name:     "AddColumn",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT)",
			expected: "ALTER TABLE `t1` ADD COLUMN `b` int NULL",
		},
		{
			name:     "AddColumnInMiddle",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT, c INT)",
			expected: "ALTER TABLE `t1` ADD COLUMN `b` int NULL AFTER `id`",
		},
		{
			name:     "AddColumnAtEnd",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT, c INT)",
			expected: "ALTER TABLE `t1` ADD COLUMN `c` int NULL",
		},
		{
			name:     "DropColumn",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			expected: "ALTER TABLE `t1` DROP COLUMN `b`",
		},
		{
			name:     "ModifyColumn",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b VARCHAR(100))",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `b` varchar(100) NULL",
		},
		{
			name:     "ReorderColumn",
			source:   "CREATE TABLE t1 (a INT, b INT, c INT)",
			target:   "CREATE TABLE t1 (c INT, a INT, b INT)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `c` int NULL FIRST",
		},
		{
			name:     "AddIndex",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT, INDEX idx_b (b))",
			expected: "ALTER TABLE `t1` ADD INDEX `idx_b` (`b`)",
		},
		{
			name:     "DropIndex",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT, INDEX idx_b (b))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT)",
			expected: "ALTER TABLE `t1` DROP INDEX `idx_b`",
		},
		{
			name:     "AddUniqueIndex",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, email VARCHAR(100))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, email VARCHAR(100), UNIQUE KEY idx_email (email))",
			expected: "ALTER TABLE `t1` ADD UNIQUE INDEX `idx_email` (`email`)",
		},
		// Inline column-level UNIQUE: MySQL canonicalizes `c INT UNIQUE` into
		// a table-level `UNIQUE KEY c (c)` (that is what SHOW CREATE TABLE
		// reports), so the two representations must diff as equal. Regression:
		// the inline form used to be invisible to diffIndexes, so diffing the
		// live canonical form against a desired inline form emitted
		// `MODIFY COLUMN c ..., DROP INDEX c` — silently dropping the
		// uniqueness constraint — and never converged.
		{
			name:     "InlineUniqueVsCanonicalNoChange",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, UNIQUE KEY c (c))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT UNIQUE)",
			expected: "",
		},
		{
			name:     "CanonicalVsInlineUniqueNoChange",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT UNIQUE)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, UNIQUE KEY c (c))",
			expected: "",
		},
		{
			name:     "InlineUniqueBothSidesNoChange",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT UNIQUE)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT UNIQUE)",
			expected: "",
		},
		{
			// Uniqueness added via the inline form: emitted exactly once, as
			// an index-level ADD (a MODIFY COLUMN cannot express UNIQUE).
			name:     "AddInlineUnique",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT UNIQUE)",
			expected: "ALTER TABLE `t1` ADD UNIQUE INDEX `c` (`c`)",
		},
		{
			// A NEW column declared inline-unique: the ADD COLUMN does not
			// carry UNIQUE, so the folded index set must add the index —
			// exactly once.
			name:     "AddColumnWithInlineUnique",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT UNIQUE)",
			expected: "ALTER TABLE `t1` ADD COLUMN `c` int NULL, ADD UNIQUE INDEX `c` (`c`)",
		},
		{
			// Uniqueness removed relative to an inline declaration.
			name:     "DropInlineUnique",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT UNIQUE)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT)",
			expected: "ALTER TABLE `t1` DROP INDEX `c`",
		},
		{
			// Safety net: an inline-derived name is only a guess at the
			// server-assigned one. A live unique index on the same column set
			// under a different explicit name satisfies the inline
			// declaration — it must never be dropped.
			name:     "InlineUniqueMatchesDifferentlyNamedLiveUnique",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, UNIQUE KEY uniq_c (c))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT UNIQUE)",
			expected: "",
		},
		{
			// The pairing compares the pair under the live name, so an option
			// the declaration does not carry is cleared from the live index
			// instead of being hidden by the pairing.
			name:     "InlineUniqueAdoptsLiveNameAndClearsComment",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, UNIQUE KEY c_2 (c) COMMENT 'x')",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT UNIQUE)",
			expected: "ALTER TABLE `t1` DROP INDEX `c_2`, ADD UNIQUE INDEX `c_2` (`c`)",
		},
		{
			name:     "InlineUniqueAdoptsLiveNameAndRestoresVisibility",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, UNIQUE KEY c_2 (c) INVISIBLE)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT UNIQUE)",
			expected: "ALTER TABLE `t1` ALTER INDEX `c_2` VISIBLE",
		},
		{
			// The other direction: a source written inline against a target
			// with an explicit name and a comment takes the target's name.
			name:     "InlineSourceUniqueAdoptsTargetName",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT UNIQUE)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, UNIQUE KEY uniq_c (c) COMMENT 'x')",
			expected: "ALTER TABLE `t1` DROP INDEX `uniq_c`, ADD UNIQUE INDEX `uniq_c` (`c`) COMMENT 'x'",
		},
		{
			// Name collision with an unnamed table-level key: the server
			// names indexes in declaration order, so the inline unique claims
			// `c` and the unnamed KEY (c, d) is pushed to `c_2`. The parsed
			// inline form must synthesize the same names as the live
			// canonical form.
			name:     "InlineUniqueNameCollision",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, d INT, UNIQUE KEY c (c), KEY c_2 (c, d))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT UNIQUE, d INT, KEY (c, d))",
			expected: "",
		},
		{
			// Interleaved declaration: an explicit KEY named after the unique
			// column forces the server to suffix the inline unique to c_2
			// (verified against MySQL 8.0: `d int, key c (d), c int unique`).
			// Explicit names are reserved before inline uniques claim theirs,
			// so the synthesized name matches.
			name:     "InlineUniqueSuffixedPastExplicitKey",
			source:   "CREATE TABLE t1 (d INT, c INT, UNIQUE KEY c_2 (c), KEY c (d))",
			target:   "CREATE TABLE t1 (d INT, KEY c (d), c INT UNIQUE)",
			expected: "",
		},
		// Type canonicalization: the live table (source) is in MySQL's canonical
		// form, while the user's saved schema (target) uses a convenience spelling
		// the parser folds to the same type. These must diff as equal so a schema
		// file written with BOOL / BOOLEAN / SERIAL does not churn against the
		// live table. See docs/fmt.md for the transformations MySQL applies.
		{
			// BOOL is an alias for tinyint(1); the parser folds it, so the user's
			// `active BOOL` matches the live `active tinyint(1)`.
			name:     "CanonicalTinyintVsBool",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, active tinyint(1))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, active BOOL)",
			expected: "",
		},
		{
			// BOOLEAN is likewise an alias for tinyint(1).
			name:     "CanonicalTinyintVsBoolean",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, active tinyint(1))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, active BOOLEAN)",
			expected: "",
		},
		{
			// A numeric boolean default matches the canonical quoted form: MySQL
			// renders numeric column defaults quoted (tinyint(1) DEFAULT '0'), and
			// diff treats the bare and quoted numeric default as the same value.
			name:     "CanonicalTinyintVsBooleanDefaultZero",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, active tinyint(1) DEFAULT '0')",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, active BOOLEAN DEFAULT 0)",
			expected: "",
		},
		{
			// SERIAL expands to BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE. The
			// canonical live form is the expanded column plus a UNIQUE KEY named
			// after the column; normalization materializes the inline UNIQUE the
			// parser derives from SERIAL, so the two forms diff as equal.
			name:     "CanonicalVsSerial",
			source:   "CREATE TABLE t1 (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, UNIQUE KEY id (id))",
			target:   "CREATE TABLE t1 (id SERIAL)",
			expected: "",
		},
		{
			// AUTO_INCREMENT implies NOT NULL: MySQL stores every one of these
			// as `int NOT NULL AUTO_INCREMENT`, so none of them is a change.
			name:     "AutoIncrementImpliesNotNull",
			source:   "CREATE TABLE t1 (id INT NOT NULL AUTO_INCREMENT, x INT PRIMARY KEY, UNIQUE KEY k (id))",
			target:   "CREATE TABLE t1 (id INT AUTO_INCREMENT, x INT PRIMARY KEY, UNIQUE KEY k (id))",
			expected: "",
		},
		{
			name:     "AutoIncrementNullBeforeIsNotNull",
			source:   "CREATE TABLE t1 (id INT NOT NULL AUTO_INCREMENT, x INT PRIMARY KEY, UNIQUE KEY k (id))",
			target:   "CREATE TABLE t1 (id INT NULL AUTO_INCREMENT, x INT PRIMARY KEY, UNIQUE KEY k (id))",
			expected: "",
		},
		{
			name:     "AutoIncrementDefaultNullDropped",
			source:   "CREATE TABLE t1 (id INT NOT NULL AUTO_INCREMENT, x INT PRIMARY KEY, UNIQUE KEY k (id))",
			target:   "CREATE TABLE t1 (id INT AUTO_INCREMENT DEFAULT NULL, x INT PRIMARY KEY, UNIQUE KEY k (id))",
			expected: "",
		},
		{
			// A NULL after the AUTO_INCREMENT is the one spelling of a nullable
			// AUTO_INCREMENT column, and the MODIFY has to keep that order or
			// MySQL stores it NOT NULL.
			name:     "AutoIncrementNullAfterIsNullable",
			source:   "CREATE TABLE t1 (id INT NOT NULL AUTO_INCREMENT, x INT PRIMARY KEY, UNIQUE KEY k (id))",
			target:   "CREATE TABLE t1 (id INT AUTO_INCREMENT NULL, x INT PRIMARY KEY, UNIQUE KEY k (id))",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `id` int AUTO_INCREMENT NULL",
		},
		{
			// MySQL stores a literal default converted to the column's type and
			// reports the result, so the declared spelling has to be read the
			// same way or every run re-emits the same MODIFY.
			name:     "NumericDefaultDecimalPadded",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c DECIMAL(6,2) DEFAULT '1.20')",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c DECIMAL(6,2) DEFAULT 1.2)",
			expected: "",
		},
		{
			name:     "NumericDefaultIntegerFromString",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT DEFAULT '1')",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT DEFAULT '001')",
			expected: "",
		},
		{
			name:     "NumericDefaultDoubleExponent",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c DOUBLE DEFAULT '1e16', d DOUBLE DEFAULT '100')",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c DOUBLE DEFAULT 10000000000000000, d DOUBLE DEFAULT 1e2)",
			expected: "",
		},
		{
			// A float is compared by its exact value, not by the six
			// significant digits SHOW CREATE TABLE prints: the live '1.23457'
			// is a different float from 1.23456789.
			name:     "NumericDefaultFloatPastSixDigitsDiffers",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c FLOAT DEFAULT '1.23457')",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c FLOAT DEFAULT 1.23456789)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `c` float NULL DEFAULT 1.23456789",
		},
		{
			name:     "NumericDefaultFloatSixDigitsNoDiff",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c FLOAT DEFAULT '1.23457')",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c FLOAT DEFAULT 1.23457e0)",
			expected: "",
		},
		{
			name:     "NumericDefaultVarcharFromDecimal",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c VARCHAR(10) DEFAULT '1.50')",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c VARCHAR(10) DEFAULT 1.50)",
			expected: "",
		},
		{
			name:     "NumericDefaultAdded",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c DECIMAL(6,2))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c DECIMAL(6,2) DEFAULT 1.2)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `c` decimal(6,2) NULL DEFAULT 1.2",
		},
		{
			name:     "NumericDefaultChanged",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c DECIMAL(6,2) DEFAULT '1.20')",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c DECIMAL(6,2) DEFAULT 1.21)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `c` decimal(6,2) NULL DEFAULT 1.21",
		},
		{
			// MySQL stores a temporal literal in any of its accepted spellings
			// and reports the stored value, so the declared spelling has to be
			// read the same way or every run re-emits the same MODIFY.
			name:     "TemporalDefaultDateWithoutTime",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c DATETIME DEFAULT '2020-01-01 00:00:00')",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c DATETIME DEFAULT '2020-1-1')",
			expected: "",
		},
		{
			name:     "TemporalDefaultFractionPadded",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c DATETIME(3) DEFAULT '2020-01-01 10:00:00.000')",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c DATETIME(3) DEFAULT '2020-01-01 10:00:00')",
			expected: "",
		},
		{
			name:     "TemporalDefaultDateFromNumber",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c DATE DEFAULT '2020-01-01')",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c DATE DEFAULT 20200101)",
			expected: "",
		},
		{
			name:     "TemporalDefaultTimeWithDays",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c TIME DEFAULT '26:03:04')",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c TIME DEFAULT '1 2:3:4.4')",
			expected: "",
		},
		{
			// MySQL rounds the fraction under its default sql_mode and
			// truncates it under TIME_TRUNCATE_FRACTIONAL; the rule reads
			// neither, so the literal diffs against both stored values.
			name:     "TemporalDefaultFractionModeDependentDiffs",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c TIME DEFAULT '26:03:05')",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c TIME DEFAULT '1 2:3:4.5')",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `c` time NULL DEFAULT '1 2:3:4.5'",
		},
		{
			name:     "TemporalDefaultAdded",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c DATETIME)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c DATETIME DEFAULT '2020-1-1')",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `c` datetime NULL DEFAULT '2020-1-1'",
		},
		{
			name:     "TemporalDefaultChanged",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c TIME DEFAULT '01:02:00')",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c TIME DEFAULT '1:3')",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `c` time NULL DEFAULT '1:3'",
		},
		{
			// MySQL stores an expression default in its own parenthesization
			// and without unary pluses, like every other stored expression.
			name:     "ExpressionDefaultNegatedLiteral",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT DEFAULT (-(1)))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT DEFAULT (-1))",
			expected: "",
		},
		{
			name:     "ExpressionDefaultUnaryPlus",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT DEFAULT (1))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT DEFAULT (+1))",
			expected: "",
		},
		{
			name:     "ExpressionDefaultNegatedArgument",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT DEFAULT (abs(-(1))))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT DEFAULT (abs(-1)))",
			expected: "",
		},
		{
			name:     "ExpressionDefaultChanged",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT DEFAULT (-(1)))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT DEFAULT (-2))",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `c` int NULL DEFAULT (-2)",
		},
		{
			name:     "CheckUnaryPlus",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, CONSTRAINT chk CHECK ((`c` > 1)))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, CONSTRAINT chk CHECK (c > +1))",
			expected: "",
		},
		{
			name:     "GeneratedUnaryPlus",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, g INT GENERATED ALWAYS AS ((`c` + 1)) VIRTUAL)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, g INT GENERATED ALWAYS AS (+c + +1) VIRTUAL)",
			expected: "",
		},
		// MySQL refuses to MODIFY a column to or from a VIRTUAL generated
		// column (error 3106, "Changing the STORED status"); such a column is
		// dropped and added back, at its position. Regular <-> STORED stays a
		// MODIFY. See rebuiltColumns.
		{
			name:     "GeneratedVirtualToStoredIsRebuilt",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, g INT AS (c + 1) VIRTUAL)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, g INT AS (c + 1) STORED)",
			expected: "ALTER TABLE `t1` DROP COLUMN `g`, ADD COLUMN `g` int GENERATED ALWAYS AS (`c`+1) STORED NULL",
		},
		{
			name:     "GeneratedStoredToVirtualIsRebuilt",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, g INT AS (c + 1) STORED)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, g INT AS (c + 1) VIRTUAL)",
			expected: "ALTER TABLE `t1` DROP COLUMN `g`, ADD COLUMN `g` int GENERATED ALWAYS AS (`c`+1) VIRTUAL NULL",
		},
		{
			// A VIRTUAL column holds no data, so a DROP+ADD of the regular
			// column would leave it NULL. It is rebuilt STORED first, which
			// MySQL fills from the expression, then MODIFYed into a regular
			// column, which keeps the values. See virtualToRegularIntermediate.
			name:   "GeneratedVirtualToRegularIsStagedThroughStored",
			source: "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, g INT AS (c + 1) VIRTUAL)",
			target: "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, g INT)",
			expectedStatements: []string{
				"ALTER TABLE `t1` DROP COLUMN `g`, ADD COLUMN `g` int GENERATED ALWAYS AS (`c`+1) STORED NULL",
				"ALTER TABLE `t1` MODIFY COLUMN `g` int NULL",
			},
		},
		{
			// The second statement carries the type and attribute changes,
			// and everything else the diff emits.
			name:   "GeneratedVirtualToRegularStagesOtherChangesSecond",
			source: "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, g INT AS (c + 1) VIRTUAL)",
			target: "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, g BIGINT NOT NULL DEFAULT 0, d INT)",
			expectedStatements: []string{
				"ALTER TABLE `t1` DROP COLUMN `g`, ADD COLUMN `g` int GENERATED ALWAYS AS (`c`+1) STORED NULL",
				"ALTER TABLE `t1` MODIFY COLUMN `g` bigint NOT NULL DEFAULT 0, ADD COLUMN `d` int NULL",
			},
		},
		{
			// Dropping the column the expression read is fine in the second
			// statement: the regular column no longer reads it.
			name:   "GeneratedVirtualToRegularDropsReadColumnSecond",
			source: "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, g INT AS (c + 1) VIRTUAL)",
			target: "CREATE TABLE t1 (id INT PRIMARY KEY, g INT)",
			expectedStatements: []string{
				"ALTER TABLE `t1` DROP COLUMN `g`, ADD COLUMN `g` int GENERATED ALWAYS AS (`c`+1) STORED NULL",
				"ALTER TABLE `t1` DROP COLUMN `c`, MODIFY COLUMN `g` int NULL",
			},
		},
		{
			// A STORED column reading a rebuilt column is not rebuilt with it
			// when the target makes it regular: the MODIFY keeps its values,
			// and MySQL accepts it in the same ALTER as the DROP.
			name:     "GeneratedDependentToRegularIsModifiedNotRebuilt",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, g INT AS (c + 1) VIRTUAL, s INT AS (g + 1) STORED)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, g INT AS (c + 1) STORED, s INT)",
			expected: "ALTER TABLE `t1` DROP COLUMN `g`, ADD COLUMN `g` int GENERATED ALWAYS AS (`c`+1) STORED NULL AFTER `c`, MODIFY COLUMN `s` int NULL",
		},
		{
			name:     "RegularToGeneratedVirtualIsRebuilt",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, g INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, g INT AS (c + 1) VIRTUAL)",
			expected: "ALTER TABLE `t1` DROP COLUMN `g`, ADD COLUMN `g` int GENERATED ALWAYS AS (`c`+1) VIRTUAL NULL",
		},
		{
			name:     "RegularToGeneratedStoredIsModified",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, g INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, g INT AS (c + 1) STORED)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `g` int GENERATED ALWAYS AS (`c`+1) STORED NULL",
		},
		{
			name:     "GeneratedStoredToRegularIsModified",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, g INT AS (c + 1) STORED)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, g INT)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `g` int NULL",
		},
		{
			name:     "GeneratedExpressionChangeIsModified",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, g INT AS (c + 1) VIRTUAL)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, g INT AS (c + 2) VIRTUAL)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `g` int GENERATED ALWAYS AS (`c`+2) VIRTUAL NULL",
		},
		{
			name:     "GeneratedRebuildKeepsPosition",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, g INT AS (id + 1) VIRTUAL, c INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, g INT AS (id + 1) STORED, c INT)",
			expected: "ALTER TABLE `t1` DROP COLUMN `g`, ADD COLUMN `g` int GENERATED ALWAYS AS (`id`+1) STORED NULL AFTER `id`",
		},
		{
			// A plain key part on the column survives a DROP+ADD in one ALTER.
			name:     "GeneratedRebuildKeepsPlainIndex",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, g INT AS (c + 1) VIRTUAL, KEY kg (g), KEY kcg (c, g))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, g INT AS (c + 1) STORED, KEY kg (g), KEY kcg (c, g))",
			expected: "ALTER TABLE `t1` DROP COLUMN `g`, ADD COLUMN `g` int GENERATED ALWAYS AS (`c`+1) STORED NULL",
		},
		{
			// A functional index reading the column blocks its DROP (error
			// 3837) unless the same ALTER drops it; it is added back.
			name:     "GeneratedRebuildReaddsFunctionalIndex",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, g INT AS (c + 1) VIRTUAL, KEY kf ((g + 1)))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, g INT AS (c + 1) STORED, KEY kf ((g + 1)))",
			expected: "ALTER TABLE `t1` DROP COLUMN `g`, ADD COLUMN `g` int GENERATED ALWAYS AS (`c`+1) STORED NULL, DROP INDEX `kf`, ADD INDEX `kf` ((`g`+1))",
		},
		{
			// A CHECK reading the column blocks its DROP (error 3959) unless
			// the same ALTER drops it; the target's is added back.
			name:     "GeneratedRebuildReaddsCheck",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, g INT AS (c + 1) VIRTUAL, CONSTRAINT ck CHECK (g > c))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, g INT AS (c + 1) STORED, CONSTRAINT ck CHECK (g > c))",
			expected: "ALTER TABLE `t1` DROP COLUMN `g`, ADD COLUMN `g` int GENERATED ALWAYS AS (`c`+1) STORED NULL, DROP CHECK `ck`, ADD CONSTRAINT `ck` CHECK (`g`>`c`)",
		},
		{
			// The re-add takes the target's text, even when the source's was
			// an enforcement-only difference away from it.
			name:     "GeneratedRebuildReaddsChangedCheck",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, g INT AS (c + 1) VIRTUAL, CONSTRAINT ck CHECK (g > c))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, g INT AS (c + 1) STORED, CONSTRAINT ck CHECK (g > c) NOT ENFORCED)",
			expected: "ALTER TABLE `t1` DROP COLUMN `g`, ADD COLUMN `g` int GENERATED ALWAYS AS (`c`+1) STORED NULL, DROP CHECK `ck`, ADD CONSTRAINT `ck` CHECK (`g`>`c`) NOT ENFORCED",
		},
		{
			// A generated column reading a rebuilt column blocks its DROP
			// (error 3108) and is rebuilt with it.
			name:     "GeneratedRebuildCascadesToDependents",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, g INT AS (id + 1) VIRTUAL, h INT AS (g + 1) VIRTUAL, c INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, g INT AS (id + 1) STORED, h INT AS (g + 1) VIRTUAL, c INT)",
			expected: "ALTER TABLE `t1` DROP COLUMN `g`, DROP COLUMN `h`, ADD COLUMN `g` int GENERATED ALWAYS AS (`id`+1) STORED NULL AFTER `id`, ADD COLUMN `h` int GENERATED ALWAYS AS (`g`+1) VIRTUAL NULL AFTER `g`",
		},
		{
			// MySQL reports a functional index key part wrapped in its own
			// parentheses, KEY k (((`c` + 1))); the authored KEY k ((c+1)) is
			// the same index.
			name:     "FunctionalIndexParensConverge",
			source:   "CREATE TABLE `t1` (`id` int NOT NULL, `c` int DEFAULT NULL, PRIMARY KEY (`id`), KEY `k` (((`c` + 1))))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, KEY k ((c+1)))",
			expected: "",
		},
		{
			name:     "FunctionalIndexAdded",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, KEY k ((c+1)))",
			expected: "ALTER TABLE `t1` ADD INDEX `k` ((`c`+1))",
		},
		{
			name:     "FunctionalIndexExpressionChanged",
			source:   "CREATE TABLE `t1` (`id` int NOT NULL, `c` int DEFAULT NULL, PRIMARY KEY (`id`), KEY `k` (((`c` + 1))))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, KEY k ((c+2)))",
			expected: "ALTER TABLE `t1` DROP INDEX `k`, ADD INDEX `k` ((`c`+2))",
		},
		{
			// MySQL reports column references in a stored expression in the
			// column's declared case; the authored spelling is the same
			// expression (columnReferenceCaseNormalizer).
			name:     "GeneratedColumnReferenceCaseConverges",
			source:   "CREATE TABLE `t1` (`id` int NOT NULL, `c` int DEFAULT NULL, `g` int GENERATED ALWAYS AS ((`c` + 1)) STORED, PRIMARY KEY (`id`))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, g INT AS (C + 1) STORED)",
			expected: "",
		},
		{
			name:     "FunctionalIndexReferenceCaseConverges",
			source:   "CREATE TABLE `t1` (`id` int NOT NULL, `c` int DEFAULT NULL, PRIMARY KEY (`id`), KEY `k` (((`c` + 1))))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, KEY k ((C+1)))",
			expected: "",
		},
		{
			name:     "CheckReferenceCaseConverges",
			source:   "CREATE TABLE `t1` (`id` int NOT NULL, `c` int DEFAULT NULL, PRIMARY KEY (`id`), CONSTRAINT `t1_chk_1` CHECK ((`c` > 0)))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, CONSTRAINT t1_chk_1 CHECK (C > 0))",
			expected: "",
		},
		{
			name:     "PartitionReferenceCaseConverges",
			source:   "CREATE TABLE `t1` (`id` int NOT NULL, PRIMARY KEY (`id`)) PARTITION BY HASH (`id`) PARTITIONS 2",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) PARTITION BY HASH (ID) PARTITIONS 2",
			expected: "",
		},
		{
			// The respelling follows the declared column, so an emitted
			// expression names the column as the table declares it.
			name:     "GeneratedColumnEmittedInDeclaredCase",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, Col INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, Col INT, g INT AS (COL + 1) STORED)",
			expected: "ALTER TABLE `t1` ADD COLUMN `g` int GENERATED ALWAYS AS (`Col`+1) STORED NULL",
		},
		{
			// Reverse direction: the user's BOOLEAN schema as source, canonical
			// tinyint(1) as target. Still equal — canonicalization is symmetric.
			name:     "BooleanVsCanonicalTinyint",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, active BOOLEAN)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, active tinyint(1))",
			expected: "",
		},
		{
			// Adding WITH PARSER to an index with an unchanged column list is
			// an option-only change. A combined DROP+ADD in a single ALTER is a
			// MySQL no-op, so the diff swaps the index for a replacement under
			// a temporary name and renames it back in a final statement.
			name:   "AddFulltextParser",
			source: "CREATE TABLE t1 (id INT PRIMARY KEY, b TEXT, FULLTEXT KEY ft_b (b))",
			target: "CREATE TABLE t1 (id INT PRIMARY KEY, b TEXT, FULLTEXT KEY ft_b (b) WITH PARSER ngram)",
			expectedStatements: []string{
				"ALTER TABLE `t1` ADD FULLTEXT INDEX `_ft_b_new` (`b`) WITH PARSER ngram, DROP INDEX `ft_b`",
				"ALTER TABLE `t1` RENAME INDEX `_ft_b_new` TO `ft_b`",
			},
		},
		{
			name:   "RemoveFulltextParser",
			source: "CREATE TABLE t1 (id INT PRIMARY KEY, b TEXT, FULLTEXT KEY ft_b (b) WITH PARSER ngram)",
			target: "CREATE TABLE t1 (id INT PRIMARY KEY, b TEXT, FULLTEXT KEY ft_b (b))",
			expectedStatements: []string{
				"ALTER TABLE `t1` ADD FULLTEXT INDEX `_ft_b_new` (`b`), DROP INDEX `ft_b`",
				"ALTER TABLE `t1` RENAME INDEX `_ft_b_new` TO `ft_b`",
			},
		},
		{
			name:     "FulltextParserNoChange",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b TEXT, FULLTEXT KEY ft_b (b) WITH PARSER ngram)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b TEXT, FULLTEXT KEY ft_b (b) WITH PARSER ngram)",
			expected: "",
		},
		{
			// An index rebuilt for an unrelated reason (here: a column list
			// change) must preserve WITH PARSER in the re-add.
			name:     "FulltextRebuildPreservesParser",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b TEXT, c TEXT, FULLTEXT KEY ft_b (b) WITH PARSER ngram)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b TEXT, c TEXT, FULLTEXT KEY ft_b (b, c) WITH PARSER ngram)",
			expected: "ALTER TABLE `t1` DROP INDEX `ft_b`, ADD FULLTEXT INDEX `ft_b` (`b`, `c`) WITH PARSER ngram",
		},
		{
			// KEY_BLOCK_SIZE on an unchanged column list is an option-only
			// change; it is swapped like AddFulltextParser.
			name:   "AddIndexKeyBlockSize",
			source: "CREATE TABLE t1 (id INT PRIMARY KEY, b VARCHAR(100), KEY idx_b (b)) ROW_FORMAT=COMPRESSED",
			target: "CREATE TABLE t1 (id INT PRIMARY KEY, b VARCHAR(100), KEY idx_b (b) KEY_BLOCK_SIZE=8) ROW_FORMAT=COMPRESSED",
			expectedStatements: []string{
				"ALTER TABLE `t1` ADD INDEX `_idx_b_new` (`b`) KEY_BLOCK_SIZE=8, DROP INDEX `idx_b`",
				"ALTER TABLE `t1` RENAME INDEX `_idx_b_new` TO `idx_b`",
			},
		},
		{
			name:   "RemoveIndexKeyBlockSize",
			source: "CREATE TABLE t1 (id INT PRIMARY KEY, b VARCHAR(100), KEY idx_b (b) KEY_BLOCK_SIZE=8) ROW_FORMAT=COMPRESSED",
			target: "CREATE TABLE t1 (id INT PRIMARY KEY, b VARCHAR(100), KEY idx_b (b)) ROW_FORMAT=COMPRESSED",
			expectedStatements: []string{
				"ALTER TABLE `t1` ADD INDEX `_idx_b_new` (`b`), DROP INDEX `idx_b`",
				"ALTER TABLE `t1` RENAME INDEX `_idx_b_new` TO `idx_b`",
			},
		},
		{
			// The replacement's temporary name avoids every index name on
			// either side, compared case-insensitively as MySQL does (error
			// 1061 otherwise).
			name:   "OptionOnlyChangeNextToTheTemporaryName",
			source: "CREATE TABLE t1 (id INT PRIMARY KEY, b VARCHAR(100), KEY idx_b (b), KEY _IDX_B_new (id)) ROW_FORMAT=COMPRESSED",
			target: "CREATE TABLE t1 (id INT PRIMARY KEY, b VARCHAR(100), KEY idx_b (b) KEY_BLOCK_SIZE=8, KEY _IDX_B_new (id)) ROW_FORMAT=COMPRESSED",
			expectedStatements: []string{
				"ALTER TABLE `t1` ADD INDEX `_idx_b_new2` (`b`) KEY_BLOCK_SIZE=8, DROP INDEX `idx_b`",
				"ALTER TABLE `t1` RENAME INDEX `_idx_b_new2` TO `idx_b`",
			},
		},
		{
			// Every swap shares one statement, and every replacement is
			// renamed back in one final statement.
			name:   "TwoOptionOnlyChangesShareTheSwapAndTheRename",
			source: "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT, KEY ka (a), KEY kb (b)) ROW_FORMAT=COMPRESSED",
			target: "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT, KEY ka (a) KEY_BLOCK_SIZE=4, KEY kb (b) KEY_BLOCK_SIZE=4) ROW_FORMAT=COMPRESSED",
			expectedStatements: []string{
				"ALTER TABLE `t1` ADD INDEX `_ka_new` (`a`) KEY_BLOCK_SIZE=4, DROP INDEX `ka`, ADD INDEX `_kb_new` (`b`) KEY_BLOCK_SIZE=4, DROP INDEX `kb`",
				"ALTER TABLE `t1` RENAME INDEX `_ka_new` TO `ka`, RENAME INDEX `_kb_new` TO `kb`",
			},
		},
		{
			// InnoDB builds one FULLTEXT index per ALTER (error 1795), so a
			// FULLTEXT swap takes a statement of its own, after the shared
			// one; the renames still share a statement.
			name:   "FulltextSwapTakesItsOwnStatement",
			source: "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b TEXT, c TEXT, KEY ka (a), FULLTEXT KEY fb (b), FULLTEXT KEY fc (c)) ROW_FORMAT=COMPRESSED",
			target: "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b TEXT, c TEXT, KEY ka (a) KEY_BLOCK_SIZE=4, FULLTEXT KEY fb (b) WITH PARSER ngram, FULLTEXT KEY fc (c) WITH PARSER ngram) ROW_FORMAT=COMPRESSED",
			expectedStatements: []string{
				"ALTER TABLE `t1` ADD INDEX `_ka_new` (`a`) KEY_BLOCK_SIZE=4, DROP INDEX `ka`",
				"ALTER TABLE `t1` ADD FULLTEXT INDEX `_fb_new` (`b`) WITH PARSER ngram, DROP INDEX `fb`",
				"ALTER TABLE `t1` ADD FULLTEXT INDEX `_fc_new` (`c`) WITH PARSER ngram, DROP INDEX `fc`",
				"ALTER TABLE `t1` RENAME INDEX `_ka_new` TO `ka`, RENAME INDEX `_fb_new` TO `fb`, RENAME INDEX `_fc_new` TO `fc`",
			},
		},
		{
			// An index declaring the table's own KEY_BLOCK_SIZE is reported
			// without it (indexDefaultsNormalizer), so it is no change.
			name:     "IndexKeyBlockSizeEqualToTheTableNoChange",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b VARCHAR(100), KEY idx_b (b)) ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=4",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b VARCHAR(100), KEY idx_b (b) KEY_BLOCK_SIZE=4) ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=4",
			expected: "",
		},
		{
			name:     "IndexKeyBlockSizeNoChange",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b VARCHAR(100), KEY idx_b (b) KEY_BLOCK_SIZE=8) ROW_FORMAT=COMPRESSED",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b VARCHAR(100), KEY idx_b (b) KEY_BLOCK_SIZE=8) ROW_FORMAT=COMPRESSED",
			expected: "",
		},
		{
			// InnoDB drops an index KEY_BLOCK_SIZE on an uncompressed table,
			// so it is not a change there (indexDefaultsNormalizer). The diff
			// used to rebuild the index on every run.
			name:     "IndexKeyBlockSizeIgnoredOnUncompressedTable",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b VARCHAR(100), KEY idx_b (b))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b VARCHAR(100), KEY idx_b (b) KEY_BLOCK_SIZE=8)",
			expected: "",
		},
		{
			// An index rebuilt for an unrelated reason (here: a column list
			// change) must preserve KEY_BLOCK_SIZE in the re-add.
			name:     "IndexRebuildPreservesKeyBlockSize",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT, KEY idx_ab (a) KEY_BLOCK_SIZE=8) ROW_FORMAT=COMPRESSED",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT, KEY idx_ab (a, b) KEY_BLOCK_SIZE=8) ROW_FORMAT=COMPRESSED",
			expected: "ALTER TABLE `t1` DROP INDEX `idx_ab`, ADD INDEX `idx_ab` (`a`, `b`) KEY_BLOCK_SIZE=8",
		},
		{
			name:     "ColumnWithDefault",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, status VARCHAR(20))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, status VARCHAR(20) DEFAULT 'active')",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `status` varchar(20) NULL DEFAULT 'active'",
		},
		{
			name:     "ColumnNullability",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `name` varchar(100) NOT NULL",
		},
		{
			// NULL and DEFAULT NULL are semantically equivalent for nullable columns.
			// User schema might say `NULL` but MySQL outputs `DEFAULT NULL`.
			name:     "NullableColumnDefaultNormalization",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100) NULL)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100) DEFAULT NULL)",
			expected: "", // No changes expected - they're equivalent
		},
		{
			// Implicit nullable (no NULL/NOT NULL specified) vs explicit DEFAULT NULL
			name:     "ImplicitNullableColumnDefaultNormalization",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100) DEFAULT NULL)",
			expected: "", // No changes expected - implicit null equals DEFAULT NULL
		},
		{
			name:     "TableOptions",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) ENGINE=InnoDB",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) ENGINE=InnoDB COMMENT='test table'",
			expected: "ALTER TABLE `t1` COMMENT='test table'",
		},
		{
			name:     "ChangeTableComment",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) COMMENT='old comment'",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) COMMENT='new comment'",
			expected: "ALTER TABLE `t1` COMMENT='new comment'",
		},
		{
			// Removing a table comment must emit an explicit COMMENT='' to
			// clear it. Previously the difference was detected but no clause
			// was emitted, so two different schemas diffed as equal.
			name:     "RemoveTableComment",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) COMMENT='old comment'",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			expected: "ALTER TABLE `t1` COMMENT=''",
		},
		{
			name:     "EnumColumn",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, status ENUM('active', 'inactive'))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, status ENUM('active', 'inactive', 'pending'))",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `status` enum('active','inactive','pending') NULL",
		},
		{
			name:     "DecimalColumn",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, price DECIMAL(10,2))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, price DECIMAL(12,4))",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `price` decimal(12,4) NULL",
		},
		{
			name:     "UnsignedColumn",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, count INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, count INT UNSIGNED)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `count` int unsigned NULL",
		},
		{
			name:     "ColumnComment",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100) COMMENT 'User name')",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `name` varchar(100) NULL COMMENT 'User name'",
		},
		{
			name:     "ColumnCommentRogueValues1",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY COMMENT 'Line1\\nLine2\\rLine3')",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `id` int NOT NULL COMMENT 'Line1\\nLine2\\rLine3'",
		},
		{
			name:   "ColumnCommentRogueValues2",
			source: "CREATE TABLE t1 (id INT PRIMARY KEY)",
			target: `CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(50) DEFAULT "O'Brien")`,
			// The true default is O'Brien (one apostrophe); it is escaped
			// exactly once on emission. The previous expectation
			// 'O\'\'Brien' was the double-escaping bug (parse left the
			// value escaped, then emission escaped it again).
			expected: "ALTER TABLE `t1` ADD COLUMN `name` varchar(50) NULL DEFAULT 'O\\'Brien'",
		},

		{
			name:     "AutoIncrement",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY AUTO_INCREMENT)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `id` int NOT NULL AUTO_INCREMENT",
		},
		{
			name:     "SetColumn",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, permissions SET('read', 'write'))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, permissions SET('read', 'write', 'execute'))",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `permissions` set('read','write','execute') NULL",
		},
		{
			name:     "CheckConstraint",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, age INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, age INT, CONSTRAINT chk_age CHECK (age >= 0))",
			expected: "ALTER TABLE `t1` ADD CONSTRAINT `chk_age` CHECK (`age`>=0)",
		},
		{
			name:     "DropCheckConstraint",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, age INT, CONSTRAINT chk_age CHECK (age >= 0))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, age INT)",
			expected: "ALTER TABLE `t1` DROP CHECK `chk_age`",
		},
		{
			name:     "TimestampDefaultCurrentTimestamp",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, created_at TIMESTAMP)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `created_at` timestamp NULL DEFAULT current_timestamp",
		},
		{
			name:     "TimestampExplicitDefaults",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, ts TIMESTAMP NOT NULL)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, ts TIMESTAMP NOT NULL DEFAULT '2023-01-01 00:00:00')",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `ts` timestamp NOT NULL DEFAULT '2023-01-01 00:00:00'",
		},
		{
			name:     "MultipleChanges",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT, c VARCHAR(50))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b VARCHAR(100), d INT, INDEX idx_d (d))",
			expected: "ALTER TABLE `t1` DROP COLUMN `c`, MODIFY COLUMN `b` varchar(100) NULL, ADD COLUMN `d` int NULL, ADD INDEX `idx_d` (`d`)",
		},
		{
			name:     "DefaultValueFunction_NOW",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, created_at DATETIME)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, created_at DATETIME DEFAULT NOW())",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `created_at` datetime NULL DEFAULT current_timestamp",
		},
		{
			name:     "DefaultValueFunction_CurrentTimestampWithPrecision",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, created_at DATETIME(3))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3))",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `created_at` datetime(3) NOT NULL DEFAULT current_timestamp(3)",
		},
		{
			name:     "DefaultValueFunction_UUID",
			source:   "CREATE TABLE t1 (id VARCHAR(36) PRIMARY KEY, name VARCHAR(100))",
			target:   "CREATE TABLE t1 (id VARCHAR(36) PRIMARY KEY DEFAULT (UUID()), name VARCHAR(100))",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `id` varchar(36) NOT NULL DEFAULT (uuid())",
		},
		{
			name: "MultipleChangesComplex",
			source: `CREATE TABLE products (
			id INT PRIMARY KEY,
			name VARCHAR(100),
			price DECIMAL(10,2),
			old_column INT
		)`,
			target: `CREATE TABLE products (
			id INT PRIMARY KEY,
			name VARCHAR(200) NOT NULL,
			price DECIMAL(12,4),
			description TEXT,
			INDEX idx_name (name)
		)`,
			expected: "ALTER TABLE `products` DROP COLUMN `old_column`, MODIFY COLUMN `name` varchar(200) NOT NULL, MODIFY COLUMN `price` decimal(12,4) NULL, ADD COLUMN `description` text NULL, ADD INDEX `idx_name` (`name`)",
		},
		{
			name:     "VirtualColumns",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, first_name VARCHAR(50), last_name VARCHAR(50))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, first_name VARCHAR(50), last_name VARCHAR(50), full_name VARCHAR(101) AS (CONCAT(first_name, ' ', last_name)) VIRTUAL)",
			expected: "ALTER TABLE `t1` ADD COLUMN `full_name` varchar(101) GENERATED ALWAYS AS (CONCAT(`first_name`, ' ', `last_name`)) VIRTUAL NULL",
		},
		{
			name:     "StoredColumns",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, price DECIMAL(10,2), tax_rate DECIMAL(5,4))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, price DECIMAL(10,2), tax_rate DECIMAL(5,4), total DECIMAL(10,2) AS (price * (1 + tax_rate)) STORED)",
			expected: "ALTER TABLE `t1` ADD COLUMN `total` decimal(10,2) GENERATED ALWAYS AS (`price`*(1+`tax_rate`)) STORED NULL",
		},
		{
			name:     "Timestamps1",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, updated_at TIMESTAMP)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `updated_at` timestamp NULL DEFAULT current_timestamp ON UPDATE current_timestamp",
		},
		{
			name:     "Timestamps2",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, ts TIMESTAMP)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, ts TIMESTAMP NULL DEFAULT NULL)",
			expected: "", // Nullable column with no default and DEFAULT NULL are semantically equivalent
		},
		// Index Modifications
		{
			name:     "ModifyIndexVisibility_MakeInvisible",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name) INVISIBLE)",
			expected: "ALTER TABLE `t1` ALTER INDEX `idx_name` INVISIBLE",
		},
		{
			name:     "ModifyIndexVisibility_MakeVisible",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name) INVISIBLE)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name))",
			expected: "ALTER TABLE `t1` ALTER INDEX `idx_name` VISIBLE",
		},
		{
			// InnoDB has no hash indexes: USING HASH builds a B-tree and is
			// not reported, so it is not a change (indexDefaultsNormalizer).
			name:     "UsingHashIsNoChangeOnInnoDB",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name) USING HASH)",
			expected: "",
		},
		{
			name:     "ModifyIndexTypeBtree",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name) USING BTREE)",
			expected: "ALTER TABLE `t1` DROP INDEX `idx_name`, ADD INDEX `idx_name` (`name`) USING BTREE",
		},
		{
			name:     "ModifyIndexTypeHashOnMemoryEngine",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name)) ENGINE=MEMORY",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name) USING HASH) ENGINE=MEMORY",
			expected: "ALTER TABLE `t1` DROP INDEX `idx_name`, ADD INDEX `idx_name` (`name`) USING HASH",
		},
		{
			// VISIBLE is the default and is never reported, on a secondary
			// index or on the primary key (where the ALTER INDEX used to be
			// emitted with an empty name).
			name:     "ExplicitVisibleIndexNoChange",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name))",
			target:   "CREATE TABLE t1 (id INT, name VARCHAR(100), PRIMARY KEY (id) VISIBLE, INDEX idx_name (name) VISIBLE)",
			expected: "",
		},
		{
			name:     "ExplicitVisibleTargetRestoresVisibility",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name) INVISIBLE)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name) VISIBLE)",
			expected: "ALTER TABLE `t1` ALTER INDEX `idx_name` VISIBLE",
		},
		{
			name:     "AddIndexWithComment",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name) COMMENT 'name index')",
			expected: "ALTER TABLE `t1` ADD INDEX `idx_name` (`name`) COMMENT 'name index'",
		},

		// Fulltext indexes. (Spatial indexes are covered next to the SRID
		// cases in diff_column_options_test.go.)
		{
			name:     "AddFulltextIndex",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, content TEXT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, content TEXT, FULLTEXT INDEX idx_content (content))",
			expected: "ALTER TABLE `t1` ADD FULLTEXT INDEX `idx_content` (`content`)",
		},
		{
			name:     "DropFulltextIndex",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, content TEXT, FULLTEXT INDEX idx_content (content))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, content TEXT)",
			expected: "ALTER TABLE `t1` DROP INDEX `idx_content`",
		},
		// InnoDB builds one FULLTEXT index per ALTER (error 1795): the first
		// add stays in the combined ALTER, each further one is a statement
		// of its own after it.
		{
			name:   "TwoFulltextAdditionsAreSplit",
			source: "CREATE TABLE t1 (id INT PRIMARY KEY, a TEXT, b TEXT)",
			target: "CREATE TABLE t1 (id INT PRIMARY KEY, a TEXT, b TEXT, FULLTEXT KEY k1 (a), FULLTEXT KEY k2 (b))",
			expectedStatements: []string{
				"ALTER TABLE `t1` ADD FULLTEXT INDEX `k1` (`a`)",
				"ALTER TABLE `t1` ADD FULLTEXT INDEX `k2` (`b`)",
			},
		},
		{
			name:   "ThreeFulltextAdditionsAlongsideOtherChanges",
			source: "CREATE TABLE t1 (id INT PRIMARY KEY, a TEXT, b TEXT)",
			target: "CREATE TABLE t1 (id INT PRIMARY KEY, a TEXT, b TEXT, c TEXT, FULLTEXT KEY k1 (a), FULLTEXT KEY k2 (b), FULLTEXT KEY k3 (c), KEY kc (c(10)))",
			expectedStatements: []string{
				"ALTER TABLE `t1` ADD COLUMN `c` text NULL, ADD FULLTEXT INDEX `k1` (`a`), ADD INDEX `kc` (`c`(10))",
				"ALTER TABLE `t1` ADD FULLTEXT INDEX `k2` (`b`)",
				"ALTER TABLE `t1` ADD FULLTEXT INDEX `k3` (`c`)",
			},
		},
		{
			// A rebuilt FULLTEXT index counts as the one creation.
			name:   "FulltextRebuiltAndAddedAreSplit",
			source: "CREATE TABLE t1 (id INT PRIMARY KEY, a TEXT, b TEXT, FULLTEXT KEY k1 (a))",
			target: "CREATE TABLE t1 (id INT PRIMARY KEY, a TEXT, b TEXT, FULLTEXT KEY k1 (a, b), FULLTEXT KEY k2 (b))",
			expectedStatements: []string{
				"ALTER TABLE `t1` DROP INDEX `k1`, ADD FULLTEXT INDEX `k1` (`a`, `b`)",
				"ALTER TABLE `t1` ADD FULLTEXT INDEX `k2` (`b`)",
			},
		},
		// Constraint Modifications
		{
			name:     "ModifyCheckConstraint",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, age INT, CONSTRAINT chk_age CHECK (age >= 0))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, age INT, CONSTRAINT chk_age CHECK (age >= 18))",
			expected: "ALTER TABLE `t1` DROP CHECK `chk_age`, ADD CONSTRAINT `chk_age` CHECK (`age`>=18)",
		},
		{
			// The _utf8mb3 MySQL reports on a literal stored from an older
			// client is kept: the diff cannot tell an inert introducer from
			// one that decides the value. The schema's bare spelling (and
			// here its other name) diffs once; the re-added constraint,
			// stored from a utf8mb4 session, then agrees.
			name:     "CheckConstraintUTF8MB3IntroducerDiffers",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, type enum('A','B'), tok varchar(15), CONSTRAINT chk_tok_abc123 CHECK (type = _utf8mb3'A' AND tok IS NOT NULL OR type = _utf8mb3'B' AND tok IS NULL))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, type enum('A','B'), tok varchar(15), CONSTRAINT chk_tok_def456 CHECK (type = 'A' AND tok IS NOT NULL OR type = 'B' AND tok IS NULL))",
			expected: "ALTER TABLE `t1` DROP CHECK `chk_tok_abc123`, ADD CONSTRAINT `chk_tok_def456` CHECK (`type`='A' AND `tok` IS NOT NULL OR `type`='B' AND `tok` IS NULL)",
		},
		// Charset introducers. Every introducer a literal carries is kept
		// except _utf8mb4, the one a bare literal parses to and the one MySQL
		// writes on a bare literal stored from a utf8mb4 session: an
		// introducer can change the value even of an ASCII literal
		// (UPPER(_latin5'i') is 'İ', STRCMP(_latin1'a', _latin1'a ') is 0),
		// and Spirit does not evaluate expressions. See restoreExprText.
		{
			name:     "GeneratedColumnBinaryIntroducerDiffers",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, g INT AS (CHAR_LENGTH('€')) STORED)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, g INT AS (CHAR_LENGTH(_binary'€')) STORED)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `g` int GENERATED ALWAYS AS (CHAR_LENGTH(_BINARY'€')) STORED NULL",
		},
		{
			name:     "ExpressionDefaultKeepsIntroducerUnderCollate",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c VARCHAR(10) DEFAULT ('a' COLLATE utf8mb4_bin))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c VARCHAR(10) DEFAULT (_latin1'a' COLLATE latin1_bin))",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `c` varchar(10) NULL DEFAULT (_LATIN1'a' COLLATE latin1_bin)",
		},
		{
			// An introducer anywhere under COLLATE decides the charset the
			// collation must belong to; folding it is error 1253.
			name:     "ExpressionDefaultKeepsIntroducerUnderCollateFunction",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c VARCHAR(10) DEFAULT (CONCAT('a') COLLATE utf8mb4_bin))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c VARCHAR(10) DEFAULT (CONCAT(_latin1'a') COLLATE latin1_bin))",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `c` varchar(10) NULL DEFAULT (CONCAT(_LATIN1'a') COLLATE latin1_bin)",
		},
		{
			// CHARSET() returns the charset the introducer names: 'latin1'
			// against 'utf8mb4'. A different value, kept as written.
			name:     "ExpressionDefaultKeepsIntroducerInsideCharset",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c VARCHAR(10) DEFAULT (CHARSET('a')))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c VARCHAR(10) DEFAULT (CHARSET(_latin1'a')))",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `c` varchar(10) NULL DEFAULT (charset(_latin1'a'))",
		},
		{
			// The parser spells utf8mb3 as utf8, an alias MySQL accepts.
			name:     "ExpressionDefaultKeepsIntroducerInsideCollation",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c VARCHAR(32) DEFAULT (COLLATION('a')))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c VARCHAR(32) DEFAULT (COLLATION(_utf8mb3'a')))",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `c` varchar(32) NULL DEFAULT (collation(_utf8'a'))",
		},
		{
			// WEIGHT_STRING returns the collation weights, which differ per
			// charset even for ASCII; the introducer is kept however deep.
			name:     "GeneratedColumnKeepsIntroducerInsideWeightString",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, g VARCHAR(32) AS (HEX(WEIGHT_STRING('a'))) STORED)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, g VARCHAR(32) AS (HEX(WEIGHT_STRING(IF(id, _latin1'a', _latin1'b')))) STORED)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `g` varchar(32) GENERATED ALWAYS AS (HEX(WEIGHT_STRING(IF(`id`, _LATIN1'a', _LATIN1'b')))) STORED NULL",
		},
		{
			// An ASCII literal's introducer can still decide the value
			// (UPPER(_latin5'i') is 'İ'), so it is kept and the expressions
			// differ.
			name:     "GeneratedColumnLatin1ASCIIIntroducerInsideUpperDiffers",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, g VARCHAR(10) AS (UPPER(_latin1'a')) STORED)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, g VARCHAR(10) AS (UPPER('a')) STORED)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `g` varchar(10) GENERATED ALWAYS AS (UPPER('a')) STORED NULL",
		},
		{
			name:     "GeneratedColumnLatin5IntroducerInsideUpperDiffers",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, g VARCHAR(10) AS (UPPER('i')) STORED)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, g VARCHAR(10) AS (UPPER(_latin5'i')) STORED)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `g` varchar(10) GENERATED ALWAYS AS (UPPER(_LATIN5'i')) STORED NULL",
		},
		{
			name:     "FunctionalIndexBinaryIntroducerDiffers",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, z VARCHAR(10), KEY fk ((CONCAT(z, 'x'))))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, z VARCHAR(10), KEY fk ((CONCAT(z, _binary'x'))))",
			expected: "ALTER TABLE `t1` DROP INDEX `fk`, ADD INDEX `fk` ((CONCAT(`z`, _BINARY'x')))",
		},
		{
			name:     "CheckLatin1ASCIIIntroducerDiffers",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c VARCHAR(10), CHECK (c <> _latin1'abc'))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c VARCHAR(10), CHECK (c <> 'abc'))",
			expected: "ALTER TABLE `t1` DROP CHECK `t1_chk_1`, ADD CONSTRAINT `t1_chk_1` CHECK (`c`!='abc')",
		},
		{
			name:     "CheckLatin1NonASCIIIntroducerDiffers",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c VARCHAR(10), CHECK (c <> _latin1'é'))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c VARCHAR(10), CHECK (c <> 'é'))",
			expected: "ALTER TABLE `t1` DROP CHECK `t1_chk_1`, ADD CONSTRAINT `t1_chk_1` CHECK (`c`!='é')",
		},
		{
			name:     "CheckUTF16IntroducerDiffers",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c VARCHAR(10), CHECK (c <> _utf16'x'))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c VARCHAR(10), CHECK (c <> 'x'))",
			expected: "ALTER TABLE `t1` DROP CHECK `t1_chk_1`, ADD CONSTRAINT `t1_chk_1` CHECK (`c`!='x')",
		},
		{
			// N'x' is _utf8mb3'x', kept like any other introducer.
			name:     "CheckNationalLiteralDiffers",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c VARCHAR(10), CHECK (c <> N'x'))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c VARCHAR(10), CHECK (c <> 'x'))",
			expected: "ALTER TABLE `t1` DROP CHECK `t1_chk_1`, ADD CONSTRAINT `t1_chk_1` CHECK (`c`!='x')",
		},
		{
			// A literal-style default is a value: MySQL converts it to the
			// column's charset and reports it with no introducer.
			name:     "LiteralDefaultIntroducerNoDiff",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c VARCHAR(10) DEFAULT _latin1'x')",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c VARCHAR(10) DEFAULT 'x')",
			expected: "",
		},
		// Column-level CHECKs. A column can carry several, each with its own
		// name and enforcement; all of them are hoisted (columnCheckNormalizer).
		// Keeping only the last one made a diff drop the others from the live
		// table; dropping NOT ENFORCED made it start enforcing the constraint.
		{
			name:     "ColumnMultipleChecksAdded",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT CHECK (c > 0) CHECK (c < 10))",
			expected: "ALTER TABLE `t1` ADD CONSTRAINT `t1_chk_1` CHECK (`c`>0), ADD CONSTRAINT `t1_chk_2` CHECK (`c`<10)",
		},
		{
			name:     "ColumnMultipleChecksNoDiff",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, CONSTRAINT t1_chk_1 CHECK ((c > 0)), CONSTRAINT t1_chk_2 CHECK ((c < 10)))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT CHECK (c > 0) CHECK (c < 10))",
			expected: "",
		},
		{
			name:     "ColumnCheckNotEnforcedAdded",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT CONSTRAINT ck CHECK (c > 0) NOT ENFORCED)",
			expected: "ALTER TABLE `t1` ADD CONSTRAINT `ck` CHECK (`c`>0) NOT ENFORCED",
		},
		{
			name:     "ColumnCheckEnforcementToggled",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, CONSTRAINT ck CHECK ((c > 0)))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT CONSTRAINT ck CHECK (c > 0) NOT ENFORCED)",
			expected: "ALTER TABLE `t1` ALTER CHECK `ck` NOT ENFORCED",
		},
		{
			name:     "ColumnCheckNotEnforcedNoDiff",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, CONSTRAINT ck CHECK ((c > 0)) /*!80016 NOT ENFORCED */)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT CONSTRAINT ck CHECK (c > 0) NOT ENFORCED)",
			expected: "",
		},
		// Invisible columns and the other per-column attributes MySQL
		// reports: NOT SECONDARY, COLUMN_FORMAT, STORAGE and
		// SECONDARY_ENGINE_ATTRIBUTE. They used to land in the unmodeled
		// Options map, which Diff ignores: no diff when only they changed,
		// and, because MODIFY COLUMN replaces the whole definition, silently
		// cleared by any other change to the column.
		{
			name:     "InvisibleColumnAdded",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT INVISIBLE)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `c` int NULL INVISIBLE",
		},
		{
			name:     "InvisibleColumnRemoved",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT INVISIBLE)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `c` int NULL",
		},
		{
			name:     "InvisibleColumnNoDiff",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, `c` int DEFAULT NULL /*!80023 INVISIBLE */)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT INVISIBLE)",
			expected: "",
		},
		{
			name:     "ExplicitVisibleColumnNoDiff",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT VISIBLE)",
			expected: "",
		},
		{
			name:     "CommentChangePreservesInvisible",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT INVISIBLE)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT INVISIBLE COMMENT 'x')",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `c` int NULL INVISIBLE COMMENT 'x'",
		},
		{
			name:     "NotSecondaryAdded",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT NOT SECONDARY)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `c` int NULL NOT SECONDARY",
		},
		{
			name:     "NotSecondaryNoDiff",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, `c` int NOT SECONDARY DEFAULT NULL)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT NOT SECONDARY)",
			expected: "",
		},
		{
			// MySQL stores a JSON integer exactly and reports the change;
			// comparing through float64 folded everything past 2^53 together.
			name:     "SecondaryEngineAttributeLargeIntegerDiffers",
			source:   `CREATE TABLE t1 (id INT PRIMARY KEY, c INT SECONDARY_ENGINE_ATTRIBUTE='{"x":9007199254740992}')`,
			target:   `CREATE TABLE t1 (id INT PRIMARY KEY, c INT SECONDARY_ENGINE_ATTRIBUTE='{"x":9007199254740993}')`,
			expected: "ALTER TABLE `t1` MODIFY COLUMN `c` int NULL SECONDARY_ENGINE_ATTRIBUTE='{\\\"x\\\":9007199254740993}'",
		},
		{
			name:   "IndexSecondaryEngineAttributeLargeIntegerDiffers",
			source: `CREATE TABLE t1 (id INT PRIMARY KEY, c INT, KEY k (c) SECONDARY_ENGINE_ATTRIBUTE='{"x":9007199254740992}')`,
			target: `CREATE TABLE t1 (id INT PRIMARY KEY, c INT, KEY k (c) SECONDARY_ENGINE_ATTRIBUTE='{"x":9007199254740993}')`,
			expectedStatements: []string{
				"ALTER TABLE `t1` ADD INDEX `_k_new` (`c`) SECONDARY_ENGINE_ATTRIBUTE='{\\\"x\\\":9007199254740993}', DROP INDEX `k`",
				"ALTER TABLE `t1` RENAME INDEX `_k_new` TO `k`",
			},
		},
		{
			// MySQL reports 1e2 as 100.0 and an integer past uint64 as a double.
			name:     "SecondaryEngineAttributeNumberFormsNoDiff",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, `c` int DEFAULT NULL /*!80021 SECONDARY_ENGINE_ATTRIBUTE '{\"w\": 1.2345678901234568e22, \"y\": 1.0, \"z\": 100.0}' */)",
			target:   `CREATE TABLE t1 (id INT PRIMARY KEY, c INT SECONDARY_ENGINE_ATTRIBUTE='{"z":1e2,"y":1,"w":12345678901234567890123}')`,
			expected: "",
		},
		{
			name:     "SecondaryEngineAttributeAdded",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT)",
			target:   `CREATE TABLE t1 (id INT PRIMARY KEY, c INT SECONDARY_ENGINE_ATTRIBUTE='{"x":1}')`,
			expected: "ALTER TABLE `t1` MODIFY COLUMN `c` int NULL SECONDARY_ENGINE_ATTRIBUTE='{\\\"x\\\":1}'",
		},
		{
			// MySQL re-serializes the JSON (here with a space after the
			// colon); the attribute is compared as a JSON document.
			name:     "SecondaryEngineAttributeReserializedNoDiff",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, `c` int DEFAULT NULL /*!80021 SECONDARY_ENGINE_ATTRIBUTE '{\"x\": 1}' */)",
			target:   `CREATE TABLE t1 (id INT PRIMARY KEY, c INT SECONDARY_ENGINE_ATTRIBUTE='{"x":1}')`,
			expected: "",
		},
		{
			name:     "CommentChangePreservesSecondaryEngineAttribute",
			source:   `CREATE TABLE t1 (id INT PRIMARY KEY, c INT SECONDARY_ENGINE_ATTRIBUTE='{"x":1}')`,
			target:   `CREATE TABLE t1 (id INT PRIMARY KEY, c INT SECONDARY_ENGINE_ATTRIBUTE='{"x":1}' COMMENT 'x')`,
			expected: "ALTER TABLE `t1` MODIFY COLUMN `c` int NULL COMMENT 'x' SECONDARY_ENGINE_ATTRIBUTE='{\\\"x\\\":1}'",
		},
		{
			name:     "ColumnFormatAndStorageAdded",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT COLUMN_FORMAT FIXED STORAGE DISK)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `c` int NULL STORAGE DISK COLUMN_FORMAT FIXED",
		},
		{
			name:     "ColumnFormatAndStorageNoDiff",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, `c` int /*!50606 STORAGE DISK */ /*!50606 COLUMN_FORMAT FIXED */ DEFAULT NULL)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT COLUMN_FORMAT fixed STORAGE disk)",
			expected: "",
		},
		{
			name:     "ColumnFormatDefaultNoDiff",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT COLUMN_FORMAT DEFAULT STORAGE DEFAULT SECONDARY_ENGINE_ATTRIBUTE='')",
			expected: "",
		},
		{
			// When constraint names differ AND expressions actually differ, it should still produce a diff.
			name:     "CheckConstraintDifferentNameDifferentExpression",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, age INT, CONSTRAINT chk_v1 CHECK (age >= 0))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, age INT, CONSTRAINT chk_v2 CHECK (age >= 18))",
			expected: "ALTER TABLE `t1` DROP CHECK `chk_v1`, ADD CONSTRAINT `chk_v2` CHECK (`age`>=18)",
		},
		{
			// MySQL rewrites stored CHECK expressions into a fully
			// parenthesized canonical form, so SHOW CREATE TABLE (the source
			// here) never returns the expression as the user wrote it (the
			// target). The two must converge with no diff.
			name:     "CheckConstraintMySQLCanonicalParensNoDiff",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, kind enum('x','y') NOT NULL, ref_x INT, ref_y INT, CONSTRAINT chk_kind_ref CHECK ((((`kind` = _utf8mb4'x') and (`ref_x` is not null) and (`ref_y` is null)) or ((`kind` = _utf8mb4'y') and (`ref_y` is not null) and (`ref_x` is null)))))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, kind enum('x','y') NOT NULL, ref_x INT, ref_y INT, CONSTRAINT chk_kind_ref CHECK ((kind = 'x' AND ref_x IS NOT NULL AND ref_y IS NULL) OR (kind = 'y' AND ref_y IS NOT NULL AND ref_x IS NULL)))",
			expected: "",
		},
		{
			// CHECK constraint names are schema-scoped, so creating a shadow
			// table renames them; after cutover the live table carries a
			// different name AND MySQL's canonical parenthesization. The
			// cross-name expression matching must still converge with no diff.
			name:     "CheckConstraintRenamedAndCanonicalParensNoDiff",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, kind enum('x','y') NOT NULL, ref_x INT, ref_y INT, CONSTRAINT chk_kind_ref_renamed CHECK ((((`kind` = _utf8mb4'x') and (`ref_x` is not null)) or ((`kind` = _utf8mb4'y') and (`ref_y` is not null)))))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, kind enum('x','y') NOT NULL, ref_x INT, ref_y INT, CONSTRAINT chk_kind_ref CHECK ((kind = 'x' AND ref_x IS NOT NULL) OR (kind = 'y' AND ref_y IS NOT NULL)))",
			expected: "",
		},
		{
			// Parentheses that change operator binding are semantic, not
			// cosmetic: (a OR b) AND c is a different constraint from
			// a OR b AND c, and normalization must keep them distinct.
			name:     "CheckConstraintMovedParensStillDiffs",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT, c INT, CONSTRAINT chk_expr CHECK ((a = 1 OR b = 2) AND c = 3))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT, c INT, CONSTRAINT chk_expr CHECK (a = 1 OR b = 2 AND c = 3))",
			expected: "ALTER TABLE `t1` DROP CHECK `chk_expr`, ADD CONSTRAINT `chk_expr` CHECK (`a`=1 OR `b`=2 AND `c`=3)",
		},
		{
			// Same distinctness for non-binary operators: NOT and IS NULL
			// render without connecting parentheses, so only explicit
			// parenthesization of each operator keeps (NOT a) IS NULL apart
			// from NOT (a IS NULL).
			name:     "CheckConstraintUnaryIsNullParensStillDiffs",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, CONSTRAINT chk_expr CHECK (NOT (a IS NULL)))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, CONSTRAINT chk_expr CHECK ((NOT a) IS NULL))",
			expected: "ALTER TABLE `t1` DROP CHECK `chk_expr`, ADD CONSTRAINT `chk_expr` CHECK ((NOT `a`) IS NULL)",
		},
		// CHECK constraint enforcement ([NOT] ENFORCED)
		{
			// MySQL's SHOW CREATE TABLE renders NOT ENFORCED inside a
			// versioned comment; it must converge with the plain spelling.
			name:     "CheckNotEnforcedBothSides",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, age INT, CONSTRAINT chk_age CHECK ((age >= 0)) /*!80016 NOT ENFORCED */)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, age INT, CONSTRAINT chk_age CHECK (age >= 0) NOT ENFORCED)",
			expected: "",
		},
		{
			// Explicit ENFORCED is the default; it converges with the absent
			// keyword (MySQL omits ENFORCED from SHOW CREATE TABLE).
			name:     "CheckExplicitEnforcedEqualsAbsent",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, age INT, CONSTRAINT chk_age CHECK (age >= 0))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, age INT, CONSTRAINT chk_age CHECK (age >= 0) ENFORCED)",
			expected: "",
		},
		{
			// An enforcement-only change is applied in place with ALTER CHECK
			// rather than DROP+ADD.
			name:     "CheckFlipToNotEnforced",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, age INT, CONSTRAINT chk_age CHECK (age >= 0))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, age INT, CONSTRAINT chk_age CHECK (age >= 0) NOT ENFORCED)",
			expected: "ALTER TABLE `t1` ALTER CHECK `chk_age` NOT ENFORCED",
		},
		{
			name:     "CheckFlipToEnforced",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, age INT, CONSTRAINT chk_age CHECK ((age >= 0)) /*!80016 NOT ENFORCED */)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, age INT, CONSTRAINT chk_age CHECK (age >= 0))",
			expected: "ALTER TABLE `t1` ALTER CHECK `chk_age` ENFORCED",
		},
		{
			// When a NOT ENFORCED check is re-added for another reason (here
			// an expression change), the ADD must preserve NOT ENFORCED —
			// previously it silently re-enabled enforcement.
			name:     "CheckNotEnforcedReAddKeepsNotEnforced",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, age INT, CONSTRAINT chk_age CHECK ((age >= 0)) /*!80016 NOT ENFORCED */)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, age INT, CONSTRAINT chk_age CHECK (age >= 18) NOT ENFORCED)",
			expected: "ALTER TABLE `t1` DROP CHECK `chk_age`, ADD CONSTRAINT `chk_age` CHECK (`age`>=18) NOT ENFORCED",
		},
		{
			name:     "AddNewCheckNotEnforced",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, age INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, age INT, CONSTRAINT chk_age CHECK (age >= 0) NOT ENFORCED)",
			expected: "ALTER TABLE `t1` ADD CONSTRAINT `chk_age` CHECK (`age`>=0) NOT ENFORCED",
		},
		{
			// Same expression under different names but different enforcement
			// must NOT be treated as a rename-equivalent pair.
			name:     "CheckEnforcementDiffersAcrossNames",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, age INT, CONSTRAINT chk_v1 CHECK (age >= 0))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, age INT, CONSTRAINT chk_v2 CHECK (age >= 0) NOT ENFORCED)",
			expected: "ALTER TABLE `t1` DROP CHECK `chk_v1`, ADD CONSTRAINT `chk_v2` CHECK (`age`>=0) NOT ENFORCED",
		},
		{
			name:     "AddForeignKey",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id))",
			expected: "ALTER TABLE `t1` ADD CONSTRAINT `fk_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)",
		},
		{
			name:     "DropForeignKey",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT)",
			expected: "ALTER TABLE `t1` DROP FOREIGN KEY `fk_user`",
		},
		// Table Options
		{
			name:     "ChangeEngine",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) ENGINE=InnoDB",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) ENGINE=MyISAM",
			expected: "", // ENGINE is ignored by default (NewDiffOptions)
		},
		{
			name:     "ChangeCharset",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) CHARSET=utf8mb4",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) CHARSET=latin1",
			expected: "ALTER TABLE `t1` DEFAULT CHARSET=latin1, COLLATE=latin1_swedish_ci",
		},
		{
			name:     "ChangeCollation",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) COLLATE=utf8mb4_general_ci",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) COLLATE=utf8mb4_unicode_ci",
			expected: "ALTER TABLE `t1` COLLATE=utf8mb4_unicode_ci",
		},
		{
			name:     "ChangeRowFormatIgnoredByDefault",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) ROW_FORMAT=COMPACT",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) ROW_FORMAT=DYNAMIC",
			expected: "",
		},

		{
			name:     "ChangeAutoIncrement",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY AUTO_INCREMENT) AUTO_INCREMENT=1",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY AUTO_INCREMENT) AUTO_INCREMENT=100",
			expected: "", // note: this is intentional; we don't propagate AUTO_INCREMENT to the diff.
		},
		// Composite Primary Key
		{
			// Primary key columns are implicitly NOT NULL, so the target's
			// `a` and `b` normalize to NOT NULL and the diff states that
			// explicitly.
			name:     "CompositePrimaryKey",
			source:   "CREATE TABLE t1 (a INT, b INT)",
			target:   "CREATE TABLE t1 (a INT, b INT, PRIMARY KEY (a, b))",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `a` int NOT NULL, MODIFY COLUMN `b` int NOT NULL, ADD PRIMARY KEY (`a`, `b`)",
		},
		{
			// The inline PRIMARY KEY made `id` NOT NULL, and DROP PRIMARY KEY
			// does not revert that, so the nullable target needs the MODIFY.
			name:     "DropPrimaryKey",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			target:   "CREATE TABLE t1 (id INT)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `id` int NULL, DROP PRIMARY KEY",
		},
		{
			name:     "DropPrimaryKeyCanonicalForm",
			source:   "CREATE TABLE `t1` (`id` int NOT NULL,  PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
			target:   "CREATE TABLE t1 (id INT NOT NULL)",
			expected: "ALTER TABLE `t1` DROP PRIMARY KEY",
		},
		// A column leaving the primary key keeps its NOT NULL (DROP PRIMARY
		// KEY never relaxes it), so a target that declares it nullable gets a
		// MODIFY like any other column.
		{
			name:     "ColumnLeavesPrimaryKeyAndRelaxesWhenPrimaryKeyMoves",
			source:   "CREATE TABLE t1 (a VARCHAR(10) NOT NULL, b VARCHAR(10), PRIMARY KEY (a))",
			target:   "CREATE TABLE t1 (a VARCHAR(10), b VARCHAR(10) NOT NULL, PRIMARY KEY (b))",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `a` varchar(10) NULL, MODIFY COLUMN `b` varchar(10) NOT NULL, DROP PRIMARY KEY, ADD PRIMARY KEY (`b`)",
		},
		{
			name:     "ColumnLeavesPrimaryKeyAndRelaxesWhenPrimaryKeyDropped",
			source:   "CREATE TABLE t1 (a VARCHAR(10) NOT NULL, b VARCHAR(10), PRIMARY KEY (a))",
			target:   "CREATE TABLE t1 (a VARCHAR(10), b VARCHAR(10))",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `a` varchar(10) NULL, DROP PRIMARY KEY",
		},
		{
			// The new key column omits NOT NULL, which the key implies, so
			// the diff makes it NOT NULL alongside relaxing the old one.
			name:     "ColumnLeavesPrimaryKeyAndRelaxesWhenNewKeyColumnOmitsNotNull",
			source:   "CREATE TABLE t1 (a VARCHAR(10) NOT NULL, b VARCHAR(10), PRIMARY KEY (a))",
			target:   "CREATE TABLE t1 (a VARCHAR(10), b VARCHAR(10), PRIMARY KEY (b))",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `a` varchar(10) NULL, MODIFY COLUMN `b` varchar(10) NOT NULL, DROP PRIMARY KEY, ADD PRIMARY KEY (`b`)",
		},
		{
			name:     "AutoIncrementColumnLeavesPrimaryKeyAndRelaxes",
			source:   "CREATE TABLE t1 (id INT NOT NULL AUTO_INCREMENT, b VARCHAR(10) NOT NULL, PRIMARY KEY (id))",
			target:   "CREATE TABLE t1 (id INT, b VARCHAR(10) NOT NULL, PRIMARY KEY (b))",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `id` int NULL, DROP PRIMARY KEY, ADD PRIMARY KEY (`b`)",
		},
		{
			// Every attribute change on the former PK column is emitted, not
			// just the nullability.
			name:     "ColumnLeavesPrimaryKeyAndChangesTypeCommentAndNullability",
			source:   "CREATE TABLE t1 (a VARCHAR(10) NOT NULL, b VARCHAR(10), PRIMARY KEY (a))",
			target:   "CREATE TABLE t1 (a BIGINT COMMENT 'reshaped', b VARCHAR(10) NOT NULL, PRIMARY KEY (b))",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `a` bigint NULL COMMENT 'reshaped', MODIFY COLUMN `b` varchar(10) NOT NULL, DROP PRIMARY KEY, ADD PRIMARY KEY (`b`)",
		},

		// Multi-column Indexes
		{
			name:     "AddMultiColumnIndex",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, first_name VARCHAR(50), last_name VARCHAR(50))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, first_name VARCHAR(50), last_name VARCHAR(50), INDEX idx_name (first_name, last_name))",
			expected: "ALTER TABLE `t1` ADD INDEX `idx_name` (`first_name`, `last_name`)",
		},
		// Column Charset/Collation
		{
			name:     "ColumnCharset",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100)) CHARSET=utf8mb4",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100) CHARSET utf8mb4) CHARSET utf8mb4",
			expected: "", // redundant; simplifies.
		},
		{
			name:     "ColumnCharsetDiffers",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100)) CHARSET=utf8mb4",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100) CHARSET latin1) CHARSET utf8mb4",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `name` varchar(100) CHARACTER SET latin1 COLLATE latin1_swedish_ci NULL",
		},
		{
			name:     "ColumnCollation",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100)) CHARSET utf8mb4",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100) COLLATE utf8mb4_bin) CHARSET utf8mb4",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `name` varchar(100) COLLATE utf8mb4_bin NULL",
		},
		// NCHAR/NVARCHAR and their NATIONAL aliases always use the national
		// character set, utf8mb3. Sources are the live SHOW CREATE TABLE
		// forms from MySQL 8.0.
		{
			name:     "NationalCharset",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name varchar(100) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL, code char(3) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name NVARCHAR(100), code NCHAR(3)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			expected: "",
		},
		{
			name:     "NationalCharsetAliases",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a varchar(4) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL, b varchar(5) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL, c char(6) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL, d char(7) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL) DEFAULT CHARSET=utf8mb4",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a NCHAR VARYING(4), b NATIONAL VARCHAR(5), c NATIONAL CHAR(6), d NATIONAL CHARACTER(7)) DEFAULT CHARSET=utf8mb4",
			expected: "",
		},
		{
			name:     "NationalCharsetBinary",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name varchar(100) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin DEFAULT NULL, code char(3) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin DEFAULT NULL) DEFAULT CHARSET=utf8mb4",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name NVARCHAR(100) BINARY, code NCHAR(3) BINARY) DEFAULT CHARSET=utf8mb4",
			expected: "",
		},
		{
			name:     "NationalCharsetCollate",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name varchar(100) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin DEFAULT NULL) DEFAULT CHARSET=utf8mb4",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name NVARCHAR(100) COLLATE utf8mb3_bin) DEFAULT CHARSET=utf8mb4",
			expected: "",
		},
		// BINARY and COLLATE together: with an explicit column charset (which
		// every national type has) MySQL keeps the COLLATE; without one, BINARY
		// wins. Sources are the live forms from MySQL 8.0.43.
		{
			name:     "NationalCharsetBinaryCollateNoTableDefault",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c char(5) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci DEFAULT NULL)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c NCHAR(5) BINARY COLLATE utf8mb3_unicode_ci)",
			expected: "",
		},
		{
			name:     "NationalCharsetBinaryCollateUtf8mb4",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c char(5) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci DEFAULT NULL) DEFAULT CHARSET=utf8mb4",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c NCHAR(5) BINARY COLLATE utf8mb3_unicode_ci) DEFAULT CHARSET=utf8mb4",
			expected: "",
		},
		{
			name:     "NationalCharsetBinaryCollateLatin1",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c char(5) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci DEFAULT NULL) DEFAULT CHARSET=latin1",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c NCHAR(5) BINARY COLLATE utf8mb3_unicode_ci) DEFAULT CHARSET=latin1",
			expected: "",
		},
		{
			name:     "ExplicitCharsetBinaryCollate",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c varchar(10) CHARACTER SET latin1 COLLATE latin1_general_ci DEFAULT NULL) DEFAULT CHARSET=utf8mb4",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c varchar(10) CHARACTER SET latin1 BINARY COLLATE latin1_general_ci) DEFAULT CHARSET=utf8mb4",
			expected: "",
		},
		{
			name:     "BinaryWinsOverCollateWithoutCharset",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL) DEFAULT CHARSET=utf8mb4",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c varchar(10) BINARY COLLATE utf8mb4_general_ci) DEFAULT CHARSET=utf8mb4",
			expected: "",
		},
		{
			// A utf8mb3 table default written without COLLATE means
			// utf8mb3_general_ci, so it no longer matches a live table with a
			// different utf8mb3 collation: the table and its inheriting
			// columns are re-collated, as creating the file on MySQL would.
			name:     "Utf8mb3TableDefaultCollation",
			source:   "CREATE TABLE s2 (id int NOT NULL, c varchar(10) DEFAULT NULL, u varchar(10) DEFAULT NULL, UNIQUE KEY u (u), PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_unicode_ci",
			target:   "CREATE TABLE s2 (id int NOT NULL, c varchar(10), u varchar(10), UNIQUE KEY u (u), PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb3",
			expected: "ALTER TABLE `s2` MODIFY COLUMN `c` varchar(10) COLLATE utf8_general_ci NULL, MODIFY COLUMN `u` varchar(10) COLLATE utf8_general_ci NULL, COLLATE=utf8_general_ci",
		},
		{
			// Declaring utf8mb3 explicitly, directly or through NVARCHAR,
			// matches a column that inherits a utf8mb3 table default.
			name:     "NationalCharsetInheritedFromTable",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a varchar(3) DEFAULT NULL, b varchar(3) DEFAULT NULL) DEFAULT CHARSET=utf8mb3",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a varchar(3) CHARACTER SET utf8mb3, b NVARCHAR(3)) DEFAULT CHARSET=utf8mb3",
			expected: "",
		},
		{
			name:     "NationalCharsetDiffersFromTableCharset",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name varchar(100) DEFAULT NULL) DEFAULT CHARSET=utf8mb4",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name NVARCHAR(100)) DEFAULT CHARSET=utf8mb4",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `name` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NULL",
		},
		// Every charset other than utf8mb4 has a fixed default collation,
		// which SHOW CREATE TABLE writes out on a column that declares the
		// charset. Sources are the live forms from MySQL 8.0.
		{
			name:     "ExplicitCharsetDefaultCollation",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a varchar(3) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL, b varchar(3) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT NULL, c varchar(3) CHARACTER SET koi8r COLLATE koi8r_general_ci DEFAULT NULL, d text CHARACTER SET utf16 COLLATE utf16_general_ci) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a varchar(3) CHARACTER SET latin1, b varchar(3) CHARACTER SET ascii, c varchar(3) CHARACTER SET koi8r, d text CHARACTER SET utf16) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			expected: "",
		},
		{
			// MySQL 8.0.28 spells utf8mb3 as utf8 on the column and as
			// utf8mb3 on the table.
			name:     "ExplicitCharsetDefaultCollation_8028Utf8Spelling",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a varchar(3) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL, b varchar(3) DEFAULT NULL) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a varchar(3) CHARACTER SET utf8mb3, b varchar(3)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			expected: "",
		},
		{
			name:     "ExplicitCharsetNonDefaultCollationDiffers",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a varchar(3) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL) DEFAULT CHARSET=utf8mb4",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a varchar(3) CHARACTER SET latin1) DEFAULT CHARSET=utf8mb4",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `a` varchar(3) CHARACTER SET latin1 COLLATE latin1_swedish_ci NULL",
		},
		{
			// A column declaring the table's charset explicitly matches one
			// that inherits it, whichever side each is on. MySQL writes the
			// explicit one out as CHARACTER SET latin1 COLLATE
			// latin1_swedish_ci and the inherited one bare.
			name:     "ExplicitCharsetInheritedFromTable",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a varchar(3) DEFAULT NULL, b varchar(3) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL) DEFAULT CHARSET=latin1",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a varchar(3) CHARACTER SET latin1, b varchar(3)) DEFAULT CHARSET=latin1",
			expected: "",
		},
		{
			// DEFAULT CHARSET=latin1 means latin1_swedish_ci, so a table on
			// another latin1 collation is converged onto it. Before the
			// default was filled in, this emitted nothing and left the table
			// on latin1_bin.
			name:     "TableCharsetWithoutCollationSelectsDefault",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a varchar(3) COLLATE latin1_bin DEFAULT NULL) DEFAULT CHARSET=latin1 COLLATE=latin1_bin",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a varchar(3)) DEFAULT CHARSET=latin1",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `a` varchar(3) COLLATE latin1_swedish_ci NULL, COLLATE=latin1_swedish_ci",
		},
		{
			// DEFAULT CHARSET=utf8mb4 without a COLLATE gives the table the
			// server's default_collation_for_utf8mb4, which can only be
			// utf8mb4_0900_ai_ci or utf8mb4_general_ci, so a utf8mb4_bin
			// table is converged. The table option cannot name the server's
			// choice, so it restates the charset alone, and the inheriting
			// column is MODIFYed in the same ALTER with CHARACTER SET
			// utf8mb4, which MySQL resolves the same way.
			name:     "Utf8mb4WithoutCollationIsNotUtf8mb4Bin",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a varchar(3) COLLATE utf8mb4_bin DEFAULT NULL) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a varchar(3)) DEFAULT CHARSET=utf8mb4",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `a` varchar(3) CHARACTER SET utf8mb4 NULL, DEFAULT CHARSET=utf8mb4",
		},
		{
			// The live form: an inheriting column writes no COLLATE.
			name:     "Utf8mb4WithoutCollationIsNotUtf8mb4Bin_LiveForm",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a varchar(3) DEFAULT NULL) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a varchar(3)) DEFAULT CHARSET=utf8mb4",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `a` varchar(3) CHARACTER SET utf8mb4 NULL, DEFAULT CHARSET=utf8mb4",
		},
		{
			// A column the live table spells out on a server default still
			// needs the MODIFY: on the other server default it would not
			// follow the table.
			name:     "Utf8mb4WithoutCollationIsNotUtf8mb4Bin_ColumnOnServerDefault",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a varchar(3) COLLATE utf8mb4_0900_ai_ci DEFAULT NULL) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a varchar(3)) DEFAULT CHARSET=utf8mb4",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `a` varchar(3) CHARACTER SET utf8mb4 NULL, DEFAULT CHARSET=utf8mb4",
		},
		{
			// Converging the other way sets COLLATE=utf8mb4_bin, which
			// leaves existing columns on the old collation, so the
			// inheriting column is MODIFYed onto it.
			name:     "Utf8mb4WithoutCollationIsNotUtf8mb4Bin_Reverse",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a varchar(3)) DEFAULT CHARSET=utf8mb4",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a varchar(3) DEFAULT NULL) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `a` varchar(3) COLLATE utf8mb4_bin NULL DEFAULT NULL, COLLATE=utf8mb4_bin",
		},
		{
			// Either server default is one the declared table can have, so
			// it stays underdetermined against them.
			name:     "Utf8mb4WithoutCollationMatchesServerDefault",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a varchar(3) DEFAULT NULL) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a varchar(3)) DEFAULT CHARSET=utf8mb4",
			expected: "",
		},
		{
			name:     "Utf8mb4WithoutCollationMatchesServerDefault_0900",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a varchar(3) DEFAULT NULL) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a varchar(3)) DEFAULT CHARSET=utf8mb4",
			expected: "",
		},
		{
			// A column the declaration says inherits the table, but which the
			// live table spells out on the other server default, is still
			// MODIFYed onto the table's collation.
			name:     "Utf8mb4WithoutCollationColumnOffTableServerDefault",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a varchar(3) COLLATE utf8mb4_general_ci DEFAULT NULL) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a varchar(3)) DEFAULT CHARSET=utf8mb4",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `a` varchar(3) NULL",
		},
		{
			// A table with no charset clause inherits the schema default,
			// which can be any collation, so it stays underdetermined.
			name:     "NoTableCharsetStaysUnderdetermined",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a varchar(3) DEFAULT NULL) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a varchar(3))",
			expected: "",
		},
		{
			// Converting another charset to a bare utf8mb4 spells the
			// charset on the inheriting column for the same reason.
			name:     "Latin1ToUtf8mb4WithoutCollation",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a varchar(3) DEFAULT NULL) DEFAULT CHARSET=latin1",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a varchar(3)) DEFAULT CHARSET=utf8mb4",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `a` varchar(3) CHARACTER SET utf8mb4 NULL, DEFAULT CHARSET=utf8mb4",
		},
		{
			// A column that names utf8mb4 without a COLLATE is not
			// underdetermined in the same way: it takes the server's
			// default_collation_for_utf8mb4, which cannot be utf8mb4_bin,
			// whatever the table's collation is.
			name:     "Utf8mb4ColumnWithoutCollationIsNotTheTableCollation",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b varchar(3) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b varchar(3) CHARACTER SET utf8mb4) DEFAULT CHARSET=utf8mb4",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `b` varchar(3) CHARACTER SET utf8mb4 NULL, DEFAULT CHARSET=utf8mb4",
		},
		{
			// The live form of that column in a utf8mb4_bin table inherits
			// the table collation and writes no COLLATE, so the written
			// values alone would compare equal.
			name:     "Utf8mb4ColumnWithoutCollationAgainstInheritedNonDefault",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b varchar(3) DEFAULT NULL) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b varchar(3) CHARACTER SET utf8mb4) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `b` varchar(3) CHARACTER SET utf8mb4 NULL",
		},
		{
			name:     "Utf8mb4ColumnWithoutCollationAgainstInheritedNonDefault_Reverse",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b varchar(3) CHARACTER SET utf8mb4) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b varchar(3) DEFAULT NULL) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `b` varchar(3) COLLATE utf8mb4_bin NULL DEFAULT NULL",
		},
		{
			// A table on a server-default collation is one the bare column
			// can take, so an inheriting live column matches.
			name:     "Utf8mb4ColumnWithoutCollationAgainstInheritedServerDefault",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b varchar(3) DEFAULT NULL) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b varchar(3) CHARACTER SET utf8mb4) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
			expected: "",
		},
		{
			// A column naming utf8mb4 without a COLLATE takes the server's
			// utf8mb4 default, not the table's collation, and SHOW CREATE
			// TABLE writes that default out when the table uses another
			// charset. The source is the live form from MySQL 8.0.43.
			// Before the fix this emitted a MODIFY restating the bare
			// charset on every plan, which could never converge.
			name:     "Utf8mb4ColumnWithoutCollationInOtherCharsetTable",
			source:   "CREATE TABLE t1 (b char(4) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT 'a') DEFAULT CHARSET=latin1",
			target:   "CREATE TABLE t1 (b char(4) CHARACTER SET utf8mb4 DEFAULT 'a') DEFAULT CHARSET=latin1",
			expected: "",
		},
		{
			name:     "Utf8mb4ColumnWithoutCollationInOtherCharsetTable_Reverse",
			source:   "CREATE TABLE t1 (b char(4) CHARACTER SET utf8mb4 DEFAULT 'a') DEFAULT CHARSET=latin1",
			target:   "CREATE TABLE t1 (b char(4) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT 'a') DEFAULT CHARSET=latin1",
			expected: "",
		},
		{
			// The same holds in a utf8mb4 table on a collation other than
			// the server's default: the column does not take the table's.
			name:     "Utf8mb4ColumnWithoutCollationInOtherCollationTable",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b varchar(3) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b varchar(3) CHARACTER SET utf8mb4) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
			expected: "",
		},
		{
			// default_collation_for_utf8mb4 can also be utf8mb4_general_ci,
			// which the bare column then takes.
			name:     "Utf8mb4ColumnWithoutCollationMatchesGeneralCI",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b varchar(3) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL) DEFAULT CHARSET=latin1",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b varchar(3) CHARACTER SET utf8mb4) DEFAULT CHARSET=latin1",
			expected: "",
		},
		{
			// default_collation_for_utf8mb4 accepts only utf8mb4_0900_ai_ci
			// and utf8mb4_general_ci, so a live column on utf8mb4_bin is
			// never what the bare column creates: it is MODIFYed.
			name:     "Utf8mb4ColumnWithoutCollationDetectsNonDefaultCollation",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b varchar(3) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL) DEFAULT CHARSET=latin1",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b varchar(3) CHARACTER SET utf8mb4) DEFAULT CHARSET=latin1",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `b` varchar(3) CHARACTER SET utf8mb4 NULL",
		},
		{
			name:     "Utf8mb4ColumnWithoutCollationDetectsNonDefaultCollation_Utf8mb4Table",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b varchar(3) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b varchar(3) CHARACTER SET utf8mb4) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `b` varchar(3) CHARACTER SET utf8mb4 NULL",
		},
		{
			// The bare column on the source side of a diff between two
			// declared schemas: a target that names a non-default collation
			// is still applied.
			name:     "Utf8mb4ColumnWithoutCollationToExplicitNonDefaultCollation",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b varchar(3) CHARACTER SET utf8mb4) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b varchar(3) COLLATE utf8mb4_bin) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `b` varchar(3) COLLATE utf8mb4_bin NULL",
		},
		{
			// The charset is still compared: a utf8mb4 column does not match
			// one inheriting a latin1 table default.
			name:     "Utf8mb4ColumnWithoutCollationStillComparesCharset",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b varchar(3) DEFAULT NULL) DEFAULT CHARSET=latin1",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b varchar(3) CHARACTER SET utf8mb4) DEFAULT CHARSET=latin1",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `b` varchar(3) CHARACTER SET utf8mb4 NULL",
		},
		{
			// A column that inherits a utf8mb4 table default is not covered
			// by the exception: it takes the table's collation, so a live
			// column on another collation is MODIFYed back onto it, and the
			// MODIFY converges. The source is the live form, which spells
			// the charset out.
			name:     "InheritingColumnStillDetectsCollationDrift",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b varchar(3) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b varchar(3)) DEFAULT CHARSET=utf8mb4",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `b` varchar(3) NULL",
		},
		// A table-level DEFAULT CHARSET/COLLATE change only affects columns
		// added later, so when the table defaults differ, a column that
		// inherits its table default must still be MODIFYed to converge in a
		// single ALTER — even against a target column that (explicitly or by
		// inheritance) matches the target table's different default.
		{
			name:     "TableCollationChangeModifiesInheritingColumn_ExplicitTarget",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100)) CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100) COLLATE utf8mb4_general_ci) CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `name` varchar(100) COLLATE utf8mb4_general_ci NULL, COLLATE=utf8mb4_general_ci",
		},
		{
			name:     "TableCollationChangeModifiesInheritingColumn_InheritedTarget",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100)) CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100)) CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `name` varchar(100) COLLATE utf8mb4_general_ci NULL, COLLATE=utf8mb4_general_ci",
		},
		{
			name:     "TableCharsetChangeModifiesInheritingColumn",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100)) CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100)) CHARSET=latin1 COLLATE=latin1_swedish_ci",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `name` varchar(100) CHARACTER SET latin1 COLLATE latin1_swedish_ci NULL, DEFAULT CHARSET=latin1, COLLATE=latin1_swedish_ci",
		},
		// When both tables share the same defaults, a column that inherits
		// them and a column that explicitly restates them are the same
		// column — no MODIFY in either direction.
		{
			name:     "SameTableDefaults_InheritedVsExplicitColumn",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100)) CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci) CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
			expected: "",
		},
		{
			name:     "SameTableDefaults_ExplicitVsInheritedColumn",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci) CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100)) CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
			expected: "",
		},
		// A column already carrying the target's collation explicitly does
		// not need a MODIFY when only the table default changes: the
		// table-option clause alone converges.
		{
			name:     "TableCollationChangeSkipsAlreadyMatchingColumn",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100) COLLATE utf8mb4_general_ci) CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100) COLLATE utf8mb4_general_ci) CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
			expected: "ALTER TABLE `t1` COLLATE=utf8mb4_general_ci",
		},
		// Nil TableOptions tests - verifies no panic when TableOptions is nil
		// This can happen when CREATE TABLE has no explicit ENGINE/CHARSET clause
		{
			name:     "NilTableOptions_SourceHasNoOptions",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100) CHARSET utf8mb4) CHARSET utf8mb4",
			expected: "ALTER TABLE `t1` DEFAULT CHARSET=utf8mb4",
		},
		{
			name:     "NilTableOptions_TargetHasNoOptions",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100) CHARSET utf8mb4) CHARSET utf8mb4",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100))",
			expected: "", // Column charset matches table default, normalized to nil on both sides
		},
		{
			name:     "NilTableOptions_BothHaveNoOptions",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100))",
			expected: "",
		},
		{
			name:     "NilTableOptions_ColumnCharsetExplicit",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100) CHARSET latin1)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100) CHARSET utf8mb4)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `name` varchar(100) CHARACTER SET utf8mb4 NULL",
		},
		// Edge Cases
		{
			name:     "RemoveDefault",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, status VARCHAR(20) DEFAULT 'active')",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, status VARCHAR(20))",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `status` varchar(20) NULL",
		},
		// Boolean Defaults - TRUE/FALSE should not be quoted
		{
			name:     "BooleanDefaultFalse",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, is_active BOOL)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, is_active BOOL DEFAULT FALSE)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `is_active` tinyint(1) NULL DEFAULT FALSE",
		},
		{
			name:     "BooleanDefaultTrue",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, is_active BOOL)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, is_active BOOL DEFAULT TRUE)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `is_active` tinyint(1) NULL DEFAULT TRUE",
		},
		{
			name:     "AddBooleanColumnWithDefault",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, is_instant BOOL DEFAULT FALSE)",
			expected: "ALTER TABLE `t1` ADD COLUMN `is_instant` tinyint(1) NULL DEFAULT FALSE",
		},
		{
			name:     "AddColumnFirst",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			target:   "CREATE TABLE t1 (new_col INT, id INT PRIMARY KEY)",
			expected: "ALTER TABLE `t1` ADD COLUMN `new_col` int NULL FIRST",
		},
		{
			name:     "ChangeColumnOrder",
			source:   "CREATE TABLE t1 (a INT, b INT, c INT)",
			target:   "CREATE TABLE t1 (b INT, c INT, a INT)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `b` int NULL FIRST, MODIFY COLUMN `c` int NULL AFTER `b`",
		},
		// Binary/Blob Types
		{
			name:     "BinaryColumn",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, data VARBINARY(100))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, data VARBINARY(200))",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `data` varbinary(200) NULL",
		},
		{
			name:     "BlobColumn",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, data BLOB)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, data LONGBLOB)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `data` longblob NULL",
		},
		// JSON and other modern types
		{
			name:     "JsonColumn",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, data TEXT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, data JSON)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `data` json NULL",
		},

		// Partitioned Tables
		{
			name:     "AddRangePartition",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, created_at DATE)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, created_at DATE) PARTITION BY RANGE (YEAR(created_at)) (PARTITION p0 VALUES LESS THAN (2020), PARTITION p1 VALUES LESS THAN (2021))",
			expected: "ALTER TABLE `t1` PARTITION BY RANGE (YEAR(`created_at`)) (PARTITION `p0` VALUES LESS THAN (2020), PARTITION `p1` VALUES LESS THAN (2021))",
		},
		{
			name:     "AddHashPartition",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT) PARTITION BY HASH(user_id) PARTITIONS 4",
			expected: "ALTER TABLE `t1` PARTITION BY HASH (`user_id`) PARTITIONS 4",
		},
		{
			name:     "AddKeyPartition",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100)) PARTITION BY KEY(id) PARTITIONS 4",
			expected: "ALTER TABLE `t1` PARTITION BY KEY (`id`) PARTITIONS 4",
		},
		{
			name:     "AddListPartition",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, region VARCHAR(50))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, region VARCHAR(50)) PARTITION BY LIST COLUMNS(region) (PARTITION pNorth VALUES IN('US', 'CA'), PARTITION pSouth VALUES IN('MX', 'BR'))",
			expected: "ALTER TABLE `t1` PARTITION BY LIST COLUMNS (`region`) (PARTITION `pNorth` VALUES IN ('CA', 'US'), PARTITION `pSouth` VALUES IN ('BR', 'MX'))",
		},
		{
			name:     "RemovePartition",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT) PARTITION BY HASH(user_id) PARTITIONS 4",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT)",
			expected: "ALTER TABLE `t1` REMOVE PARTITIONING",
		},
		{
			name:     "ChangePartitionCount",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT) PARTITION BY HASH(id) PARTITIONS 4",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT) PARTITION BY HASH(id) PARTITIONS 8",
			expected: "ALTER TABLE `t1` ADD PARTITION PARTITIONS 4",
		},

		// Index Column Order Changes
		{
			name:     "ChangeIndexColumnOrder",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT, c INT, INDEX idx_abc (a, b, c))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT, c INT, INDEX idx_abc (b, a, c))",
			expected: "ALTER TABLE `t1` DROP INDEX `idx_abc`, ADD INDEX `idx_abc` (`b`, `a`, `c`)",
		},
		{
			name:     "ChangeIndexColumnOrderUnique",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT, UNIQUE INDEX idx_ab (a, b))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT, UNIQUE INDEX idx_ab (b, a))",
			expected: "ALTER TABLE `t1` DROP INDEX `idx_ab`, ADD UNIQUE INDEX `idx_ab` (`b`, `a`)",
		},
		{
			name:     "ChangeIndexAddColumn",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT, c INT, INDEX idx_ab (a, b))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT, c INT, INDEX idx_ab (a, b, c))",
			expected: "ALTER TABLE `t1` DROP INDEX `idx_ab`, ADD INDEX `idx_ab` (`a`, `b`, `c`)",
		},
		{
			name:     "ChangeIndexRemoveColumn",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT, c INT, INDEX idx_abc (a, b, c))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT, c INT, INDEX idx_abc (a, b))",
			expected: "ALTER TABLE `t1` DROP INDEX `idx_abc`, ADD INDEX `idx_abc` (`a`, `b`)",
		},

		// Index Key Part Direction Changes (MySQL 8.0+ descending indexes).
		// KEY (a) and KEY (a DESC) are physically different indexes, so a
		// direction change must produce a DROP+ADD rather than a nil diff.
		{
			name:     "ChangeIndexAscToDesc",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, INDEX idx_a (a))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, INDEX idx_a (a DESC))",
			expected: "ALTER TABLE `t1` DROP INDEX `idx_a`, ADD INDEX `idx_a` (`a` DESC)",
		},
		{
			name:     "ChangeIndexDescToAsc",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, INDEX idx_a (a DESC))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, INDEX idx_a (a))",
			expected: "ALTER TABLE `t1` DROP INDEX `idx_a`, ADD INDEX `idx_a` (`a`)",
		},
		{
			name:     "NoChangesDescIndex",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT, INDEX idx_ab (a DESC, b))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT, INDEX idx_ab (a DESC, b))",
			expected: "",
		},
		{
			name:     "AddIndexWithDescParts",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, c VARCHAR(100))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, c VARCHAR(100), INDEX idx_mixed (a DESC, c(10) DESC, (lower(c)) DESC))",
			expected: "ALTER TABLE `t1` ADD INDEX `idx_mixed` (`a` DESC, `c`(10) DESC, (LOWER(`c`)) DESC)",
		},

		// Foreign Key with ON DELETE / ON UPDATE
		{
			name:     "AddForeignKeyWithOnDelete",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)",
			expected: "ALTER TABLE `t1` ADD CONSTRAINT `fk_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE",
		},
		{
			name:     "AddForeignKeyWithOnUpdate",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON UPDATE CASCADE)",
			expected: "ALTER TABLE `t1` ADD CONSTRAINT `fk_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON UPDATE CASCADE",
		},
		{
			name:     "AddForeignKeyWithBothActions",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE ON UPDATE RESTRICT)",
			expected: "ALTER TABLE `t1` ADD CONSTRAINT `fk_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE ON UPDATE RESTRICT",
		},
		{
			name:     "AddForeignKeyWithSetNull",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL)",
			expected: "ALTER TABLE `t1` ADD CONSTRAINT `fk_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE SET NULL",
		},
		{
			// MySQL rejects a DROP FOREIGN KEY and an ADD CONSTRAINT under the
			// same name in one ALTER (error 1826), so the new definition is
			// added under a replacement name next to the DROP, and keeps it:
			// the table is never without the constraint, and a foreign key is
			// paired by definition whatever its name (below).
			name:     "ChangeForeignKeyAction",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)",
			expected: "ALTER TABLE `t1` DROP FOREIGN KEY `fk_user`, ADD CONSTRAINT `_fk_user_new` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE",
		},
		{
			name:     "AddOnDeleteToExistingForeignKey",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)",
			expected: "ALTER TABLE `t1` DROP FOREIGN KEY `fk_user`, ADD CONSTRAINT `_fk_user_new` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE",
		},
		{
			// The replacement name avoids every index and constraint name on
			// either side: MySQL names the index it creates for a foreign
			// key after the constraint.
			name:     "ChangeForeignKeyActionReplacementNameTaken",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, KEY _fk_user_new (user_id), CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, KEY _fk_user_new (user_id), CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)",
			expected: "ALTER TABLE `t1` DROP FOREIGN KEY `fk_user`, ADD CONSTRAINT `_fk_user_new2` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE",
		},
		{
			// An index the target adds under the name is avoided: MySQL
			// renames the index it created for the foreign key after the new
			// constraint, so the ALTER would fail with error 1061.
			name:     "ReplacementNameAvoidsATargetOnlyIndex",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, c INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, c INT, KEY _fk_user_new (c), CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)",
			expected: "ALTER TABLE `t1` ADD INDEX `_fk_user_new` (`c`), DROP FOREIGN KEY `fk_user`, ADD CONSTRAINT `_fk_user_new2` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE",
		},
		{
			// A foreign key the target adds under the name is avoided for
			// the same reason (error 1061).
			name:     "ReplacementNameAvoidsATargetOnlyForeignKey",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, c INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, c INT, CONSTRAINT _fk_user_new FOREIGN KEY (c) REFERENCES users(id), CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)",
			expected: "ALTER TABLE `t1` DROP FOREIGN KEY `fk_user`, ADD CONSTRAINT `_fk_user_new2` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE, ADD CONSTRAINT `_fk_user_new` FOREIGN KEY (`c`) REFERENCES `users` (`id`)",
		},
		{
			// A foreign key the same ALTER drops is avoided: MySQL keeps the
			// index it created for that foreign key, under the same name, so
			// the ALTER would fail with error 1061.
			name:     "ReplacementNameAvoidsASourceOnlyForeignKey",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, c INT, CONSTRAINT _fk_user_new FOREIGN KEY (c) REFERENCES users(id), CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, c INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)",
			expected: "ALTER TABLE `t1` DROP FOREIGN KEY `_fk_user_new`, DROP FOREIGN KEY `fk_user`, ADD CONSTRAINT `_fk_user_new2` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE",
		},
		{
			// An index the same ALTER drops is avoided too. MySQL would
			// accept its name, but the rule is every name on either side.
			name:     "ReplacementNameAvoidsASourceOnlyIndex",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, c INT, KEY _fk_user_new (c), CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, c INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)",
			expected: "ALTER TABLE `t1` DROP INDEX `_fk_user_new`, DROP FOREIGN KEY `fk_user`, ADD CONSTRAINT `_fk_user_new2` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE",
		},
		{
			// The foreign key keeps the replacement name, and is paired with
			// the target's by definition, as any renamed foreign key is.
			name:     "ForeignKeyUnderReplacementNameNoDiff",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT _fk_user_new FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)",
			expected: "",
		},
		{
			// A later change under the desired name fits one ALTER under
			// the two names, and brings the name back.
			name:     "ForeignKeyUnderReplacementNameChangedAgain",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT _fk_user_new FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL)",
			expected: "ALTER TABLE `t1` DROP FOREIGN KEY `_fk_user_new`, ADD CONSTRAINT `fk_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE SET NULL",
		},
		{
			name:     "ForeignKeyRenamedWithSameDefinitionNoDiff",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_old FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)",
			expected: "",
		},
		{
			// A foreign key changed under a new name fits one ALTER.
			name:     "ChangeForeignKeyActionUnderNewName",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user2 FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)",
			expected: "ALTER TABLE `t1` DROP FOREIGN KEY `fk_user`, ADD CONSTRAINT `fk_user2` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE",
		},
		{
			// Foreign key names are case-insensitive in MySQL, so a name that
			// differs only in case collides the same way (error 1826).
			name:     "ChangeForeignKeyActionUnderCaseChangedName",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT FK_USER FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)",
			expected: "ALTER TABLE `t1` DROP FOREIGN KEY `fk_user`, ADD CONSTRAINT `_FK_USER_new` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE",
		},
		{
			// The new definitions join the primary ALTER under replacement
			// names, next to every other change; a foreign key under a new
			// name joins it under its own.
			name:     "ForeignKeyReplacementsJoinOtherChanges",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT, CONSTRAINT fk_a FOREIGN KEY (a) REFERENCES pa(id), CONSTRAINT fk_b FOREIGN KEY (b) REFERENCES pb(id))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT, c INT, CONSTRAINT fk_a FOREIGN KEY (a) REFERENCES pa(id) ON DELETE CASCADE, CONSTRAINT fk_b FOREIGN KEY (b) REFERENCES pb(id) ON DELETE SET NULL, CONSTRAINT fk_c FOREIGN KEY (c) REFERENCES pc(id))",
			expected: "ALTER TABLE `t1` ADD COLUMN `c` int NULL, DROP FOREIGN KEY `fk_a`, DROP FOREIGN KEY `fk_b`, ADD CONSTRAINT `_fk_a_new` FOREIGN KEY (`a`) REFERENCES `pa` (`id`) ON DELETE CASCADE, ADD CONSTRAINT `_fk_b_new` FOREIGN KEY (`b`) REFERENCES `pb` (`id`) ON DELETE SET NULL, ADD CONSTRAINT `fk_c` FOREIGN KEY (`c`) REFERENCES `pc` (`id`)",
		},
		// Schema-qualified references. SHOW CREATE TABLE qualifies a
		// reference only when the parent is in another schema, and a parsed
		// CREATE TABLE does not know its own schema, so two references differ
		// only when both are qualified with different schemas.
		{
			name:     "AddForeignKeyToOtherSchema",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES db2.users(id))",
			expected: "ALTER TABLE `t1` ADD CONSTRAINT `fk_user` FOREIGN KEY (`user_id`) REFERENCES `db2`.`users` (`id`)",
		},
		{
			name:     "ForeignKeyReferencedSchemaChanged",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES db1.users(id))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES db2.users(id))",
			expected: "ALTER TABLE `t1` DROP FOREIGN KEY `fk_user`, ADD CONSTRAINT `_fk_user_new` FOREIGN KEY (`user_id`) REFERENCES `db2`.`users` (`id`)",
		},
		{
			name:     "ForeignKeyQualifiedMatchesUnqualified",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES db1.users(id))",
			expected: "",
		},
		{
			name:     "ForeignKeyUnqualifiedMatchesQualified",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES db1.users(id))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id))",
			expected: "",
		},
		{
			name:     "ForeignKeySameSchemaSpelledEqual",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES db1.users(id))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES `db1`.`users`(id))",
			expected: "",
		},
		{
			// NO ACTION is MySQL's default referential action, and SHOW CREATE
			// TABLE omits it. An explicitly spelled NO ACTION in the desired
			// schema must compare equal to the live table's absent clause, or
			// the same DROP+ADD FOREIGN KEY re-emits on every declarative run.
			name:     "ForeignKeyNoActionEqualsAbsent",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE NO ACTION ON UPDATE NO ACTION)",
			expected: "",
		},
		{
			name:     "ForeignKeyAbsentEqualsNoAction",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON UPDATE NO ACTION)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id))",
			expected: "",
		},
		{
			// A genuinely new FK spelled with NO ACTION is emitted without the
			// clause, so the ADD round-trips through SHOW CREATE TABLE.
			name:     "AddForeignKeyWithNoActionOmitsClause",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE NO ACTION)",
			expected: "ALTER TABLE `t1` ADD CONSTRAINT `fk_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)",
		},
		{
			// RESTRICT is semantically identical to NO ACTION in InnoDB, but
			// SHOW CREATE TABLE prints it, so it round-trips verbatim and must
			// NOT be normalized away.
			name:     "ForeignKeyRestrictRoundTrips",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT ON UPDATE RESTRICT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT ON UPDATE RESTRICT)",
			expected: "",
		},
		{
			name:     "ForeignKeyAddRestrictStillDiffs",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT)",
			expected: "ALTER TABLE `t1` DROP FOREIGN KEY `fk_user`, ADD CONSTRAINT `_fk_user_new` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE RESTRICT",
		},
		{
			name:     "ForeignKeyCascadeToNoAction",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE NO ACTION)",
			expected: "ALTER TABLE `t1` DROP FOREIGN KEY `fk_user`, ADD CONSTRAINT `_fk_user_new` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)",
		},

		// Composite Primary Key Changes
		{
			name:     "AddCompositePrimaryKey",
			source:   "CREATE TABLE t1 (a INT NOT NULL, b INT NOT NULL)",
			target:   "CREATE TABLE t1 (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (a, b))",
			expected: "ALTER TABLE `t1` ADD PRIMARY KEY (`a`, `b`)",
		},
		{
			name:     "DropCompositePrimaryKey",
			source:   "CREATE TABLE t1 (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (a, b))",
			target:   "CREATE TABLE t1 (a INT NOT NULL, b INT NOT NULL)",
			expected: "ALTER TABLE `t1` DROP PRIMARY KEY",
		},
		{
			name:     "ChangeCompositePrimaryKeyColumns",
			source:   "CREATE TABLE t1 (a INT NOT NULL, b INT NOT NULL, c INT NOT NULL, PRIMARY KEY (a, b))",
			target:   "CREATE TABLE t1 (a INT NOT NULL, b INT NOT NULL, c INT NOT NULL, PRIMARY KEY (a, c))",
			expected: "ALTER TABLE `t1` DROP PRIMARY KEY, ADD PRIMARY KEY (`a`, `c`)",
		},
		{
			name:     "ChangeCompositePrimaryKeyOrder",
			source:   "CREATE TABLE t1 (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (a, b))",
			target:   "CREATE TABLE t1 (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (b, a))",
			expected: "ALTER TABLE `t1` DROP PRIMARY KEY, ADD PRIMARY KEY (`b`, `a`)",
		},
		{
			name:     "ChangeSingleToCompositePrimaryKey",
			source:   "CREATE TABLE t1 (a INT NOT NULL PRIMARY KEY, b INT NOT NULL)",
			target:   "CREATE TABLE t1 (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (a, b))",
			expected: "ALTER TABLE `t1` DROP PRIMARY KEY, ADD PRIMARY KEY (`a`, `b`)",
		},
		{
			name:     "ChangeCompositeToSinglePrimaryKey",
			source:   "CREATE TABLE t1 (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (a, b))",
			target:   "CREATE TABLE t1 (a INT NOT NULL PRIMARY KEY, b INT NOT NULL)",
			expected: "ALTER TABLE `t1` DROP PRIMARY KEY, ADD PRIMARY KEY (`a`)",
		},
		{
			name:     "CompositePrimaryKeyWithAutoIncrement",
			source:   "CREATE TABLE t1 (a INT NOT NULL, b INT NOT NULL)",
			target:   "CREATE TABLE t1 (a INT NOT NULL AUTO_INCREMENT, b INT NOT NULL, PRIMARY KEY (a, b))",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `a` int NOT NULL AUTO_INCREMENT, ADD PRIMARY KEY (`a`, `b`)",
		},

		// Column Rename Tests - Should show as DROP + ADD (not safe to rename)
		{
			name:     "RenameColumn",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, old_name VARCHAR(100))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, new_name VARCHAR(100))",
			expected: "ALTER TABLE `t1` DROP COLUMN `old_name`, ADD COLUMN `new_name` varchar(100) NULL",
		},
		{
			name:     "RenameColumnWithData",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_name VARCHAR(100) NOT NULL DEFAULT 'unknown')",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, username VARCHAR(100) NOT NULL DEFAULT 'unknown')",
			expected: "ALTER TABLE `t1` DROP COLUMN `user_name`, ADD COLUMN `username` varchar(100) NOT NULL DEFAULT 'unknown'",
		},
		{
			name:     "RenameColumnWithIndex",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, old_col VARCHAR(100), INDEX idx_old (old_col))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, new_col VARCHAR(100), INDEX idx_new (new_col))",
			expected: "ALTER TABLE `t1` DROP COLUMN `old_col`, ADD COLUMN `new_col` varchar(100) NULL, DROP INDEX `idx_old`, ADD INDEX `idx_new` (`new_col`)",
		},

		// Index Rename Tests - Should show as DROP + ADD (not optimized)
		{
			name:     "RenameIndex",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX old_idx (name))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX new_idx (name))",
			expected: "ALTER TABLE `t1` DROP INDEX `old_idx`, ADD INDEX `new_idx` (`name`)",
		},
		{
			name:     "RenameUniqueIndex",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, email VARCHAR(100), UNIQUE INDEX old_uniq (email))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, email VARCHAR(100), UNIQUE INDEX new_uniq (email))",
			expected: "ALTER TABLE `t1` DROP INDEX `old_uniq`, ADD UNIQUE INDEX `new_uniq` (`email`)",
		},
		{
			name:     "RenameMultiColumnIndex",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT, INDEX idx_old (a, b))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT, INDEX idx_new (a, b))",
			expected: "ALTER TABLE `t1` DROP INDEX `idx_old`, ADD INDEX `idx_new` (`a`, `b`)",
		},

		// Inline PK vs Table-level PK equivalence tests
		// MySQL normalizes inline PK to table-level PK in SHOW CREATE TABLE output,
		// so these should be considered equivalent (no DROP/ADD PRIMARY KEY)
		{
			name:     "InlinePKVsTableLevelPK_NoChange",
			source:   "CREATE TABLE t1 (id INT NOT NULL, PRIMARY KEY (id))",
			target:   "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY)",
			expected: "",
		},
		{
			name:     "TableLevelPKVsInlinePK_NoChange",
			source:   "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY)",
			target:   "CREATE TABLE t1 (id INT NOT NULL, PRIMARY KEY (id))",
			expected: "",
		},
		{
			name:     "TableLevelPKWithInlinePKAddIndex",
			source:   "CREATE TABLE t1 (id INT NOT NULL, name VARCHAR(100), PRIMARY KEY (id))",
			target:   "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name))",
			expected: "ALTER TABLE `t1` ADD INDEX `idx_name` (`name`)",
		},
		{
			name:     "InlinePKWithTableLevelPKAddIndex",
			source:   "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, name VARCHAR(100))",
			target:   "CREATE TABLE t1 (id INT NOT NULL, name VARCHAR(100), PRIMARY KEY (id), INDEX idx_name (name))",
			expected: "ALTER TABLE `t1` ADD INDEX `idx_name` (`name`)",
		},
		{
			name:     "InlinePKWithAutoIncrementAddIndex",
			source:   "CREATE TABLE t1 (id INT NOT NULL AUTO_INCREMENT, name VARCHAR(100), PRIMARY KEY (id))",
			target:   "CREATE TABLE t1 (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name))",
			expected: "ALTER TABLE `t1` ADD INDEX `idx_name` (`name`)",
		},

		// Prefix Index Tests (index with length specification)
		{
			name:     "AddPrefixIndex",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, content TEXT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, content TEXT, INDEX idx_content (content(100)))",
			expected: "ALTER TABLE `t1` ADD INDEX `idx_content` (`content`(100))",
		},
		{
			name:     "ChangePrefixIndexLength",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, content TEXT, INDEX idx_content (content(50)))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, content TEXT, INDEX idx_content (content(100)))",
			expected: "ALTER TABLE `t1` DROP INDEX `idx_content`, ADD INDEX `idx_content` (`content`(100))",
		},
		{
			name:     "AddPrefixToExistingIndex",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name(50)))",
			expected: "ALTER TABLE `t1` DROP INDEX `idx_name`, ADD INDEX `idx_name` (`name`(50))",
		},
		{
			name:     "RemovePrefixFromIndex",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name(50)))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name))",
			expected: "ALTER TABLE `t1` DROP INDEX `idx_name`, ADD INDEX `idx_name` (`name`)",
		},
		{
			name:     "MultiColumnPrefixIndex",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a VARCHAR(100), b TEXT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a VARCHAR(100), b TEXT, INDEX idx_ab (a(20), b(50)))",
			expected: "ALTER TABLE `t1` ADD INDEX `idx_ab` (`a`(20), `b`(50))",
		},

		// Expression/Functional Index Tests
		// Note: Expression indexes are not fully supported by TiDB parser for ALTER statements
		// The parser can read them from CREATE TABLE but cannot generate valid ALTER statements
		// These tests document the current behavior - they generate empty column lists
		{
			name:     "AddExpressionIndex",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, email VARCHAR(100))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, email VARCHAR(100), INDEX idx_lower_email ((LOWER(email))))",
			expected: "ALTER TABLE `t1` ADD INDEX `idx_lower_email` ((LOWER(`email`)))",
		},
		{
			name:     "DropExpressionIndex",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, email VARCHAR(100), INDEX idx_lower_email ((LOWER(email))))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, email VARCHAR(100))",
			expected: "ALTER TABLE `t1` DROP INDEX `idx_lower_email`",
		},
		{
			name:     "ChangeExpressionIndex",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, email VARCHAR(100), INDEX idx_email ((LOWER(email))))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, email VARCHAR(100), INDEX idx_email ((UPPER(email))))",
			expected: "ALTER TABLE `t1` DROP INDEX `idx_email`, ADD INDEX `idx_email` ((UPPER(`email`)))",
		},
		{
			name:     "ExpressionIndexWithMultipleColumns",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, first_name VARCHAR(50), last_name VARCHAR(50))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, first_name VARCHAR(50), last_name VARCHAR(50), INDEX idx_full ((CONCAT(first_name, ' ', last_name))))",
			expected: "ALTER TABLE `t1` ADD INDEX `idx_full` ((CONCAT(`first_name`, ' ', `last_name`)))",
		},

		// Mixed Index Type Changes
		{
			name:     "ChangeIndexTypeAndAddPrefix",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), UNIQUE INDEX idx_name (name(50)))",
			expected: "ALTER TABLE `t1` DROP INDEX `idx_name`, ADD UNIQUE INDEX `idx_name` (`name`(50))",
		},
		{
			name:     "RenameAndChangePrefixLength",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, content TEXT, INDEX old_idx (content(50)))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, content TEXT, INDEX new_idx (content(100)))",
			expected: "ALTER TABLE `t1` DROP INDEX `old_idx`, ADD INDEX `new_idx` (`content`(100))",
		},
		// Unnamed index auto-naming tests
		{
			name:     "UnnamedIndexMatchesNormalized",
			source:   "CREATE TABLE t1 (id INT NOT NULL AUTO_INCREMENT, branch_name VARCHAR(100) DEFAULT NULL, PRIMARY KEY (id), INDEX (branch_name))",
			target:   "CREATE TABLE t1 (id INT NOT NULL AUTO_INCREMENT, branch_name VARCHAR(100) DEFAULT NULL, PRIMARY KEY (id), KEY `branch_name` (`branch_name`))",
			expected: "",
		},
		{
			name:     "UnnamedIndexAutoNamed",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT, INDEX (b))",
			expected: "ALTER TABLE `t1` ADD INDEX `b` (`b`)",
		},
		{
			name:     "UnnamedIndexDuplicateColumn",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT, INDEX b (b))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT, INDEX b (b), INDEX (b))",
			expected: "ALTER TABLE `t1` ADD INDEX `b_2` (`b`)",
		},
		{
			name:     "MultipleUnnamedIndexesSameColumn",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT, c INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT, c INT, INDEX (b), INDEX (b))",
			expected: "ALTER TABLE `t1` ADD INDEX `b_2` (`b`), ADD INDEX `b` (`b`)",
		},
		{
			name:     "UnnamedUniqueIndex",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, email VARCHAR(100))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, email VARCHAR(100), UNIQUE INDEX (email))",
			expected: "ALTER TABLE `t1` ADD UNIQUE INDEX `email` (`email`)",
		},
		{
			name:     "UnnamedIndexNoDiffBothUnnamed",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT, INDEX (b))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT, INDEX (b))",
			expected: "",
		},
		{
			name:     "NamedPKMatchesUnnamedPK",
			source:   "CREATE TABLE `t1` (\n  `version` varchar(50) NOT NULL,\n  `installed_by` varchar(30) DEFAULT NULL,\n  PRIMARY KEY `version` (`version`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			target:   "CREATE TABLE `t1` (\n  `version` varchar(50) NOT NULL,\n  `installed_by` varchar(30) DEFAULT NULL,\n  PRIMARY KEY (`version`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			expected: "",
		},
		{
			name:     "NamedPKNoDiffIdentical",
			source:   "CREATE TABLE `t1` (\n  `version` varchar(50) NOT NULL,\n  PRIMARY KEY `version` (`version`)\n)",
			target:   "CREATE TABLE `t1` (\n  `version` varchar(50) NOT NULL,\n  PRIMARY KEY `version` (`version`)\n)",
			expected: "",
		},
		{
			name:     "NamedPKReversed",
			source:   "CREATE TABLE `t1` (\n  `version` varchar(50) NOT NULL,\n  PRIMARY KEY (`version`)\n)",
			target:   "CREATE TABLE `t1` (\n  `version` varchar(50) NOT NULL,\n  PRIMARY KEY `version` (`version`)\n)",
			expected: "",
		},
		{
			name:     "AddJSONColumnWithExpressionDefault",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, metadata JSON NOT NULL DEFAULT (json_object()))",
			expected: "ALTER TABLE `t1` ADD COLUMN `metadata` json NOT NULL DEFAULT (json_object())",
		},
		{
			name:     "AddJSONColumnWithJsonArrayDefault",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, tags JSON NOT NULL DEFAULT (json_array()))",
			expected: "ALTER TABLE `t1` ADD COLUMN `tags` json NOT NULL DEFAULT (json_array())",
		},
		{
			name:     "NoChanges_JSONColumnWithExpressionDefault",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, metadata JSON NOT NULL DEFAULT (json_object()))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, metadata JSON NOT NULL DEFAULT (json_object()))",
			expected: "",
		},
		{
			name:     "ModifyColumnToAddJSONExpressionDefault",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, metadata JSON NULL)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, metadata JSON NOT NULL DEFAULT (json_object()))",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `metadata` json NOT NULL DEFAULT (json_object())",
		},
		{
			name: "AddJSONColumnWithExpressionDefaultToExistingTable",
			source: `CREATE TABLE t1 (
				id bigint PRIMARY KEY AUTO_INCREMENT,
				name varchar(255) NOT NULL,
				details json,
				customer_id bigint NOT NULL,
				UNIQUE KEY unq_name (name)
			) ENGINE InnoDB DEFAULT CHARSET utf8mb4`,
			target: `CREATE TABLE t1 (
				id bigint PRIMARY KEY AUTO_INCREMENT,
				name varchar(255) NOT NULL,
				details json,
				customer_id bigint NOT NULL,
				extra json NOT NULL DEFAULT (json_object()),
				UNIQUE KEY unq_name (name)
			) ENGINE InnoDB DEFAULT CHARSET utf8mb4`,
			expected: "ALTER TABLE `t1` ADD COLUMN `extra` json NOT NULL DEFAULT (json_object())",
		},
		// MySQL column identifiers are case-insensitive — `id` and `ID`
		// refer to the same column. A diff between two CREATE TABLE
		// statements that differ only in column-name case should produce
		// no ALTER. The PK case is the one gh-ost's `modify-change-case-pk`
		// localtest exercises.
		{
			name:     "PrimaryKeyColumnCaseOnly",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c1 INT NOT NULL DEFAULT 0)",
			target:   "CREATE TABLE t1 (ID INT PRIMARY KEY, c1 INT NOT NULL DEFAULT 0)",
			expected: "",
		},
		{
			name:     "NonPrimaryKeyColumnCaseOnly",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c2 INT NOT NULL DEFAULT 0)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, C2 INT NOT NULL DEFAULT 0)",
			expected: "",
		},
		{
			name:     "CompoundPrimaryKeyColumnCaseOnly",
			source:   "CREATE TABLE t1 (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (a, b))",
			target:   "CREATE TABLE t1 (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (A, B))",
			expected: "",
		},
		// Reordering with uppercase column names must still emit MODIFY
		// AFTER clauses. Regression for a read against
		// needsExplicitPosition that used the original (uppercase) name
		// even though the map is keyed by lowercased identifier.
		{
			name:     "ReorderUppercaseColumns",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, A INT NOT NULL, B INT NOT NULL)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, B INT NOT NULL, A INT NOT NULL)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `B` int NOT NULL AFTER `id`",
		},
		// Positioning follows the clauses through the way MySQL applies them
		// (see calculateColumnPositioning). Dropping a column used to count
		// as an implicit move for its successor, so the reorder below was
		// never emitted and the live table ended up as (id, b, d).
		{
			name:     "ReorderAfterDrop",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT, c INT, d INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, d INT, b INT)",
			expected: "ALTER TABLE `t1` DROP COLUMN `a`, DROP COLUMN `c`, MODIFY COLUMN `d` int NULL AFTER `id`",
		},
		{
			name:     "DropWithoutReorderNoPosition",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT, c INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT, c INT)",
			expected: "ALTER TABLE `t1` DROP COLUMN `a`",
		},
		{
			name:     "AddInMiddleThenReorder",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, x INT, b INT, a INT)",
			expected: "ALTER TABLE `t1` ADD COLUMN `x` int NULL AFTER `id`, MODIFY COLUMN `b` int NULL AFTER `x`",
		},
		{
			name:     "MoveLastColumnFirst",
			source:   "CREATE TABLE t1 (a INT, b INT, c INT, d INT)",
			target:   "CREATE TABLE t1 (d INT, a INT, b INT, c INT)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `d` int NULL FIRST",
		},
		// Table options SHOW CREATE TABLE reports that Diff used to discard.
		// Each is cleared by the value MySQL reads back as unset.
		{
			name:     "TableStatsOptionsAdded",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) STATS_PERSISTENT=0 STATS_AUTO_RECALC=1 STATS_SAMPLE_PAGES=42",
			expected: "ALTER TABLE `t1` STATS_PERSISTENT=0, STATS_AUTO_RECALC=1, STATS_SAMPLE_PAGES=42",
		},
		{
			name:     "TableStatsOptionsReset",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) STATS_PERSISTENT=0 STATS_AUTO_RECALC=1 STATS_SAMPLE_PAGES=42",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			expected: "ALTER TABLE `t1` STATS_PERSISTENT=DEFAULT, STATS_AUTO_RECALC=DEFAULT, STATS_SAMPLE_PAGES=DEFAULT",
		},
		{
			name:     "TableStatsOptionsDefaultNoDiff",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) STATS_PERSISTENT=DEFAULT STATS_AUTO_RECALC=DEFAULT STATS_SAMPLE_PAGES=DEFAULT",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			expected: "",
		},
		{
			name:     "TableStatsPersistentToggled",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) STATS_PERSISTENT=1",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) STATS_PERSISTENT=0",
			expected: "ALTER TABLE `t1` STATS_PERSISTENT=0",
		},
		{
			name:     "AutoextendSizeSuffixNoDiff",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) /*!80023 AUTOEXTEND_SIZE=4194304 */ ENGINE=InnoDB",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) AUTOEXTEND_SIZE=4M",
			expected: "",
		},
		{
			name:     "AutoextendSizeAdded",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) AUTOEXTEND_SIZE=4M",
			expected: "ALTER TABLE `t1` AUTOEXTEND_SIZE=4194304",
		},
		{
			name:     "AutoextendSizeReset",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) AUTOEXTEND_SIZE=4M",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			expected: "ALTER TABLE `t1` AUTOEXTEND_SIZE=0",
		},
		{
			name:     "TableSecondaryEngineAttributeAdded",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) SECONDARY_ENGINE_ATTRIBUTE='{\"t\":1}'",
			expected: "ALTER TABLE `t1` SECONDARY_ENGINE_ATTRIBUTE='{\\\"t\\\":1}'",
		},
		{
			name:     "TableSecondaryEngineAttributeReset",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) SECONDARY_ENGINE_ATTRIBUTE='{\"t\":1}'",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			expected: "ALTER TABLE `t1` SECONDARY_ENGINE_ATTRIBUTE=''",
		},
		{
			name:     "TableSecondaryEngineAttributeReserializedNoDiff",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) /*!80021 SECONDARY_ENGINE_ATTRIBUTE='{\"t\": 1}' */",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) SECONDARY_ENGINE_ATTRIBUTE='{\"t\":1}'",
			expected: "",
		},
		{
			name:     "TableStorageHintsAdded",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) MIN_ROWS=10 MAX_ROWS=1000 AVG_ROW_LENGTH=100 PACK_KEYS=1 CHECKSUM=1 DELAY_KEY_WRITE=1",
			expected: "ALTER TABLE `t1` MIN_ROWS=10, MAX_ROWS=1000, AVG_ROW_LENGTH=100, PACK_KEYS=1, CHECKSUM=1, DELAY_KEY_WRITE=1",
		},
		{
			name:     "TableStorageHintsReset",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) MIN_ROWS=10 MAX_ROWS=1000 AVG_ROW_LENGTH=100 PACK_KEYS=1 CHECKSUM=1 DELAY_KEY_WRITE=1",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			expected: "ALTER TABLE `t1` MIN_ROWS=0, MAX_ROWS=0, AVG_ROW_LENGTH=0, PACK_KEYS=DEFAULT, CHECKSUM=0, DELAY_KEY_WRITE=0",
		},
		{
			// The table-level KEY_BLOCK_SIZE goes with ROW_FORMAT, which the
			// default options ignore (see TestDiffWithOptions for the rest).
			name:     "TableKeyBlockSizeIgnoredByDefault",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) ROW_FORMAT=COMPRESSED",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=4",
			expected: "",
		},
		// An index's SECONDARY_ENGINE_ATTRIBUTE. Adding or removing it alone
		// is an option-only change: a combined DROP+ADD is a MySQL no-op that
		// leaves the attribute as it was, so the index is swapped for a
		// replacement under a temporary name.
		{
			name:   "IndexSecondaryEngineAttributeAdded",
			source: "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, KEY k (c))",
			target: "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, KEY k (c) SECONDARY_ENGINE_ATTRIBUTE='{\"k\":1}')",
			expectedStatements: []string{
				"ALTER TABLE `t1` ADD INDEX `_k_new` (`c`) SECONDARY_ENGINE_ATTRIBUTE='{\\\"k\\\":1}', DROP INDEX `k`",
				"ALTER TABLE `t1` RENAME INDEX `_k_new` TO `k`",
			},
		},
		{
			name:   "IndexSecondaryEngineAttributeRemoved",
			source: "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, KEY k (c) SECONDARY_ENGINE_ATTRIBUTE='{\"k\":1}')",
			target: "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, KEY k (c))",
			expectedStatements: []string{
				"ALTER TABLE `t1` ADD INDEX `_k_new` (`c`), DROP INDEX `k`",
				"ALTER TABLE `t1` RENAME INDEX `_k_new` TO `k`",
			},
		},
		{
			name:     "IndexSecondaryEngineAttributeReserializedNoDiff",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, KEY k (c) /*!80021 SECONDARY_ENGINE_ATTRIBUTE '{\"k\": 1}' */)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, KEY k (c) SECONDARY_ENGINE_ATTRIBUTE='{\"k\":1}')",
			expected: "",
		},
		{
			name:     "IndexRebuildPreservesSecondaryEngineAttribute",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, KEY k (c) COMMENT 'a' SECONDARY_ENGINE_ATTRIBUTE='{\"k\":1}')",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, KEY k (c) COMMENT 'b' SECONDARY_ENGINE_ATTRIBUTE='{\"k\":1}')",
			expected: "ALTER TABLE `t1` DROP INDEX `k`, ADD INDEX `k` (`c`) COMMENT 'b' SECONDARY_ENGINE_ATTRIBUTE='{\\\"k\\\":1}'",
		},
		{
			name:     "IndexVisibilityChangeKeepsSecondaryEngineAttribute",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, KEY k (c) SECONDARY_ENGINE_ATTRIBUTE='{\"k\":1}')",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, KEY k (c) SECONDARY_ENGINE_ATTRIBUTE='{\"k\":1}' INVISIBLE)",
			expected: "ALTER TABLE `t1` ALTER INDEX `k` INVISIBLE",
		},
		// Partitioning. The sources below are shaped like SHOW CREATE TABLE
		// output, which always prints a per-partition `ENGINE = InnoDB` that
		// human-authored SQL omits; that clause must not register as a change
		// (it cannot differ from the table engine) or every partitioned table
		// would repartition itself on every run.
		{
			name:     "NoChanges_PartitionedEngineClauseOnly",
			source:   "CREATE TABLE t1 (dt DATE NOT NULL, PRIMARY KEY (dt)) PARTITION BY RANGE (YEAR(dt)) (PARTITION p0 VALUES LESS THAN (2020) ENGINE = InnoDB, PARTITION p1 VALUES LESS THAN MAXVALUE ENGINE = InnoDB)",
			target:   "CREATE TABLE t1 (dt DATE NOT NULL, PRIMARY KEY (dt)) PARTITION BY RANGE (YEAR(dt)) (PARTITION p0 VALUES LESS THAN (2020), PARTITION p1 VALUES LESS THAN MAXVALUE)",
			expected: "",
		},
		{
			name:     "NoChanges_Subpartitioned",
			source:   "CREATE TABLE t1 (dt DATE NOT NULL, PRIMARY KEY (dt)) PARTITION BY RANGE (year(`dt`)) SUBPARTITION BY HASH (dayofmonth(`dt`)) SUBPARTITIONS 2 (PARTITION p0 VALUES LESS THAN (2020) ENGINE = InnoDB, PARTITION p1 VALUES LESS THAN MAXVALUE ENGINE = InnoDB)",
			target:   "CREATE TABLE t1 (dt DATE NOT NULL, PRIMARY KEY (dt)) PARTITION BY RANGE (YEAR(dt)) SUBPARTITION BY HASH (dayofmonth(dt)) SUBPARTITIONS 2 (PARTITION p0 VALUES LESS THAN (2020), PARTITION p1 VALUES LESS THAN MAXVALUE)",
			expected: "",
		},
		{
			// SUBPARTITION BY with neither a count nor explicit names is legal
			// (MySQL defaults to one subpartition per partition and reports no
			// SUBPARTITIONS line), so no count must be invented on emission.
			name:     "NoChanges_SubpartitionedWithoutCount",
			source:   "CREATE TABLE t1 (dt DATE NOT NULL, PRIMARY KEY (dt)) PARTITION BY RANGE (year(`dt`)) SUBPARTITION BY HASH (dayofmonth(`dt`)) (PARTITION p0 VALUES LESS THAN (2020) ENGINE = InnoDB, PARTITION p1 VALUES LESS THAN MAXVALUE ENGINE = InnoDB)",
			target:   "CREATE TABLE t1 (dt DATE NOT NULL, PRIMARY KEY (dt)) PARTITION BY RANGE (YEAR(dt)) SUBPARTITION BY HASH (dayofmonth(dt)) (PARTITION p0 VALUES LESS THAN (2020), PARTITION p1 VALUES LESS THAN MAXVALUE)",
			expected: "",
		},
		{
			name:     "NoChanges_SubpartitionsNamedExplicitly",
			source:   "CREATE TABLE t1 (dt DATE NOT NULL, PRIMARY KEY (dt)) PARTITION BY RANGE (year(`dt`)) SUBPARTITION BY KEY (dt) (PARTITION p0 VALUES LESS THAN (2020) (SUBPARTITION s0 COMMENT = 'sc0' ENGINE = InnoDB, SUBPARTITION s1 ENGINE = InnoDB), PARTITION p1 VALUES LESS THAN MAXVALUE (SUBPARTITION s2 ENGINE = InnoDB, SUBPARTITION s3 ENGINE = InnoDB))",
			target:   "CREATE TABLE t1 (dt DATE NOT NULL, PRIMARY KEY (dt)) PARTITION BY RANGE (YEAR(dt)) SUBPARTITION BY KEY (dt) (PARTITION p0 VALUES LESS THAN (2020) (SUBPARTITION s0 COMMENT 'sc0', SUBPARTITION s1), PARTITION p1 VALUES LESS THAN MAXVALUE (SUBPARTITION s2, SUBPARTITION s3))",
			expected: "",
		},
		// A real subpartitioning change must round-trip the whole clause,
		// including SUBPARTITION BY: the PARTITION BY replaces the
		// partitioning wholesale, so anything it omits is dropped.
		{
			name:   "ChangeSubpartitionCount",
			source: "CREATE TABLE t1 (dt DATE NOT NULL, PRIMARY KEY (dt)) PARTITION BY RANGE (year(`dt`)) SUBPARTITION BY HASH (dayofmonth(`dt`)) SUBPARTITIONS 2 (PARTITION p0 VALUES LESS THAN (2020) ENGINE = InnoDB, PARTITION p1 VALUES LESS THAN MAXVALUE ENGINE = InnoDB)",
			target: "CREATE TABLE t1 (dt DATE NOT NULL, PRIMARY KEY (dt)) PARTITION BY RANGE (YEAR(dt)) SUBPARTITION BY HASH (dayofmonth(dt)) SUBPARTITIONS 4 (PARTITION p0 VALUES LESS THAN (2020), PARTITION p1 VALUES LESS THAN MAXVALUE)",
			expectedStatements: []string{
				"ALTER TABLE `t1` PARTITION BY RANGE (YEAR(`dt`)) SUBPARTITION BY HASH (DAYOFMONTH(`dt`)) SUBPARTITIONS 4 (PARTITION `p0` VALUES LESS THAN (2020), PARTITION `p1` VALUES LESS THAN MAXVALUE)",
			},
		},
		{
			name:   "AddSubpartitioning",
			source: "CREATE TABLE t1 (dt DATE NOT NULL, PRIMARY KEY (dt)) PARTITION BY RANGE (year(`dt`)) (PARTITION p0 VALUES LESS THAN (2020) ENGINE = InnoDB, PARTITION p1 VALUES LESS THAN MAXVALUE ENGINE = InnoDB)",
			target: "CREATE TABLE t1 (dt DATE NOT NULL, PRIMARY KEY (dt)) PARTITION BY RANGE (YEAR(dt)) SUBPARTITION BY LINEAR KEY (dt) SUBPARTITIONS 2 (PARTITION p0 VALUES LESS THAN (2020), PARTITION p1 VALUES LESS THAN MAXVALUE)",
			expectedStatements: []string{
				"ALTER TABLE `t1` PARTITION BY RANGE (YEAR(`dt`)) SUBPARTITION BY LINEAR KEY (`dt`) SUBPARTITIONS 2 (PARTITION `p0` VALUES LESS THAN (2020), PARTITION `p1` VALUES LESS THAN MAXVALUE)",
			},
		},
		{
			name:   "RemoveSubpartitioning",
			source: "CREATE TABLE t1 (dt DATE NOT NULL, PRIMARY KEY (dt)) PARTITION BY RANGE (year(`dt`)) SUBPARTITION BY HASH (dayofmonth(`dt`)) SUBPARTITIONS 2 (PARTITION p0 VALUES LESS THAN (2020) ENGINE = InnoDB, PARTITION p1 VALUES LESS THAN MAXVALUE ENGINE = InnoDB)",
			target: "CREATE TABLE t1 (dt DATE NOT NULL, PRIMARY KEY (dt)) PARTITION BY RANGE (YEAR(dt)) (PARTITION p0 VALUES LESS THAN (2020), PARTITION p1 VALUES LESS THAN MAXVALUE)",
			expectedStatements: []string{
				"ALTER TABLE `t1` PARTITION BY RANGE (YEAR(`dt`)) (PARTITION `p0` VALUES LESS THAN (2020), PARTITION `p1` VALUES LESS THAN MAXVALUE)",
			},
		},
		{
			name:   "RepartitionCarriesSubpartitionNamesAndComments",
			source: "CREATE TABLE t1 (dt DATE NOT NULL, PRIMARY KEY (dt)) PARTITION BY RANGE (year(`dt`)) SUBPARTITION BY KEY (dt) (PARTITION p0 VALUES LESS THAN (2020) (SUBPARTITION s0 COMMENT = 'sc0' ENGINE = InnoDB, SUBPARTITION s1 ENGINE = InnoDB))",
			target: "CREATE TABLE t1 (dt DATE NOT NULL, PRIMARY KEY (dt)) PARTITION BY RANGE (YEAR(dt)) SUBPARTITION BY KEY (dt) (PARTITION p0 VALUES LESS THAN (2030) (SUBPARTITION s0 COMMENT 'sc0', SUBPARTITION s1))",
			expectedStatements: []string{
				"ALTER TABLE `t1` PARTITION BY RANGE (YEAR(`dt`)) SUBPARTITION BY KEY (`dt`) SUBPARTITIONS 2 (PARTITION `p0` VALUES LESS THAN (2030) (SUBPARTITION `s0` COMMENT = 'sc0', SUBPARTITION `s1`))",
			},
		},
		// A partition comment is part of the definition and has to survive a
		// repartition too — the emitted PARTITION BY is the only definition
		// MySQL will see.
		{
			name:   "RepartitionCarriesPartitionComment",
			source: "CREATE TABLE t1 (dt DATE NOT NULL, PRIMARY KEY (dt)) PARTITION BY RANGE (year(`dt`)) (PARTITION p0 VALUES LESS THAN (2020) COMMENT = 'keep me' ENGINE = InnoDB)",
			target: "CREATE TABLE t1 (dt DATE NOT NULL, PRIMARY KEY (dt)) PARTITION BY RANGE (YEAR(dt)) SUBPARTITION BY HASH (dayofmonth(dt)) SUBPARTITIONS 2 (PARTITION p0 VALUES LESS THAN (2020) COMMENT 'keep me')",
			expectedStatements: []string{
				"ALTER TABLE `t1` PARTITION BY RANGE (YEAR(`dt`)) SUBPARTITION BY HASH (DAYOFMONTH(`dt`)) SUBPARTITIONS 2 (PARTITION `p0` VALUES LESS THAN (2020) COMMENT = 'keep me')",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ct1, err := ParseCreateTable(tt.source)
			require.NoError(t, err)

			ct2, err := ParseCreateTable(tt.target)
			require.NoError(t, err)

			stmts, err := ct1.Diff(ct2, nil)
			require.NoError(t, err)

			switch {
			case len(tt.expectedStatements) > 0:
				require.Len(t, stmts, len(tt.expectedStatements))
				for i, want := range tt.expectedStatements {
					require.Equal(t, want, stmts[i].Statement)
				}
			case tt.expected == "":
				require.Nil(t, stmts, "expected nil for identical tables")
			default:
				require.Len(t, stmts, 1)
				require.Equal(t, tt.expected, stmts[0].Statement)
			}
		})
	}
}

// TestDiff_NumericDefaultQuoting verifies that on a numeric column a bare
// default (DEFAULT 0, as a user typically writes it) and a quoted default
// (DEFAULT '0', the form MySQL's SHOW CREATE TABLE always renders) are treated
// as the same default, so a diff between the two converges to no change. The
// quotedness still matters on string columns, where 'NULL' is a real default
// distinct from no default — that case must still produce a change.
func TestDiff_NumericDefaultQuoting(t *testing.T) {
	tests := []struct {
		name        string
		source      string
		target      string
		expectEmpty bool
	}{
		{
			name:        "bigint_quoted_source_bare_target",
			source:      "CREATE TABLE t1 (id BIGINT UNSIGNED NOT NULL, amount BIGINT NOT NULL DEFAULT '0', PRIMARY KEY (id))",
			target:      "CREATE TABLE t1 (id BIGINT UNSIGNED NOT NULL, amount BIGINT NOT NULL DEFAULT 0, PRIMARY KEY (id))",
			expectEmpty: true,
		},
		{
			name:        "bigint_bare_source_quoted_target",
			source:      "CREATE TABLE t1 (id BIGINT UNSIGNED NOT NULL, amount BIGINT NOT NULL DEFAULT 0, PRIMARY KEY (id))",
			target:      "CREATE TABLE t1 (id BIGINT UNSIGNED NOT NULL, amount BIGINT NOT NULL DEFAULT '0', PRIMARY KEY (id))",
			expectEmpty: true,
		},
		{
			name:        "int_quoted_vs_bare",
			source:      "CREATE TABLE t1 (id INT PRIMARY KEY, n INT NOT NULL DEFAULT '42')",
			target:      "CREATE TABLE t1 (id INT PRIMARY KEY, n INT NOT NULL DEFAULT 42)",
			expectEmpty: true,
		},
		{
			name:        "decimal_quoted_vs_bare_same_value",
			source:      "CREATE TABLE t1 (id INT PRIMARY KEY, price DECIMAL(10,2) NOT NULL DEFAULT '0.00')",
			target:      "CREATE TABLE t1 (id INT PRIMARY KEY, price DECIMAL(10,2) NOT NULL DEFAULT 0.00)",
			expectEmpty: true,
		},
		{
			name:        "numeric_value_change_still_diffs",
			source:      "CREATE TABLE t1 (id INT PRIMARY KEY, n INT NOT NULL DEFAULT '0')",
			target:      "CREATE TABLE t1 (id INT PRIMARY KEY, n INT NOT NULL DEFAULT 5)",
			expectEmpty: false,
		},
		{
			name:        "string_literal_null_default_still_diffs",
			source:      "CREATE TABLE t1 (id INT PRIMARY KEY, c VARCHAR(20))",
			target:      "CREATE TABLE t1 (id INT PRIMARY KEY, c VARCHAR(20) DEFAULT 'NULL')",
			expectEmpty: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source, err := ParseCreateTable(tt.source)
			require.NoError(t, err)
			target, err := ParseCreateTable(tt.target)
			require.NoError(t, err)

			stmts, err := source.Diff(target, nil)
			require.NoError(t, err)

			if tt.expectEmpty {
				require.Nil(t, stmts, "numeric default quoting must not produce a diff")
			} else {
				require.Len(t, stmts, 1)
			}
		})
	}
}

func TestDiff_DifferentTableNames(t *testing.T) {
	ct1, err := ParseCreateTable("CREATE TABLE t1 (id INT PRIMARY KEY)")
	require.NoError(t, err)

	ct2, err := ParseCreateTable("CREATE TABLE t2 (id INT PRIMARY KEY)")
	require.NoError(t, err)

	_, err = ct1.Diff(ct2, nil)
	require.Error(t, err, "expected error when diffing tables with different names")
}

func TestDiff_DiffOptions(t *testing.T) {
	tests := []struct {
		name     string
		source   string
		target   string
		opts     *DiffOptions
		expected string
	}{
		// NewDiffOptions defaults: IgnoreAutoIncrement=true, IgnoreEngine=true
		{
			name:     "DefaultIgnoresEngine",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) ENGINE=InnoDB",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) ENGINE=MyISAM",
			opts:     nil, // nil uses NewDiffOptions()
			expected: "",
		},
		{
			name:     "DefaultIgnoresAutoIncrement",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY AUTO_INCREMENT) AUTO_INCREMENT=1",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY AUTO_INCREMENT) AUTO_INCREMENT=100",
			opts:     nil,
			expected: "",
		},
		{
			name:     "DefaultDetectsCharset",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) CHARSET=utf8mb4",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) CHARSET=latin1",
			opts:     nil,
			expected: "ALTER TABLE `t1` DEFAULT CHARSET=latin1, COLLATE=latin1_swedish_ci",
		},
		{
			name:     "DefaultDetectsCollation",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) COLLATE=utf8mb4_general_ci",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) COLLATE=utf8mb4_unicode_ci",
			opts:     nil,
			expected: "ALTER TABLE `t1` COLLATE=utf8mb4_unicode_ci",
		},
		{
			name:     "DefaultDetectsPartitioning",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT) PARTITION BY HASH(user_id) PARTITIONS 4",
			opts:     nil,
			expected: "ALTER TABLE `t1` PARTITION BY HASH (`user_id`) PARTITIONS 4",
		},

		// Explicit IgnoreEngine=false detects engine changes
		{
			name:     "ExplicitDetectEngine",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) ENGINE=InnoDB",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) ENGINE=MyISAM",
			opts:     &DiffOptions{IgnoreEngine: false},
			expected: "ALTER TABLE `t1` ENGINE=MyISAM",
		},

		// Explicit IgnoreAutoIncrement=false detects auto_increment changes
		{
			name:     "ExplicitDetectAutoIncrement",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY AUTO_INCREMENT) AUTO_INCREMENT=1",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY AUTO_INCREMENT) AUTO_INCREMENT=100",
			opts:     &DiffOptions{IgnoreAutoIncrement: false},
			expected: "ALTER TABLE `t1` AUTO_INCREMENT=100",
		},

		// IgnoreColumnAutoIncrement controls the column-level AUTO_INCREMENT
		// flag, which is distinct from the AUTO_INCREMENT=N table-option
		// counter (IgnoreAutoIncrement). By default a column gaining or losing
		// AUTO_INCREMENT is a real change; the move-tables target-state check
		// sets IgnoreColumnAutoIncrement so an unsharded source can move into a
		// sharded target that drops AUTO_INCREMENT in favor of a Vitess sequence.
		{
			name:     "DefaultDetectsColumnAutoIncrement",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY AUTO_INCREMENT)",
			opts:     nil, // nil uses NewDiffOptions(): IgnoreColumnAutoIncrement=false
			expected: "ALTER TABLE `t1` MODIFY COLUMN `id` int NOT NULL AUTO_INCREMENT",
		},
		{
			name:     "IgnoreColumnAutoIncrement_TargetAdds",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY AUTO_INCREMENT)",
			opts:     &DiffOptions{IgnoreColumnAutoIncrement: true},
			expected: "",
		},
		{
			// The move-tables scenario: the source carries AUTO_INCREMENT, the
			// sharded target dropped it because IDs come from a Vitess sequence.
			name:     "IgnoreColumnAutoIncrement_SourceDrops",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY AUTO_INCREMENT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			opts:     &DiffOptions{IgnoreColumnAutoIncrement: true},
			expected: "",
		},
		{
			name:     "IgnoreColumnAutoIncrement_StillDetectsColumnChanges",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY AUTO_INCREMENT, b INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b VARCHAR(100))",
			opts:     &DiffOptions{IgnoreColumnAutoIncrement: true},
			expected: "ALTER TABLE `t1` MODIFY COLUMN `b` varchar(100) NULL",
		},

		// IgnoreNotNullRelaxation is covered by
		// TestDiff_IgnoreNotNullRelaxation instead of here. It is the one
		// directional option, and this table's "source"/"target" fields are
		// Diff's receiver and parameter — the opposite order to the reference
		// and validated schemas every consumer passes to DiffCreateTables — so
		// stating its direction in these names would read backwards. The
		// dedicated test asserts it in the orientation consumers see.
		{
			name:     "DefaultDetectsNullability",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, customer_id BIGINT NOT NULL)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, customer_id BIGINT NULL)",
			opts:     nil, // nil uses NewDiffOptions(): IgnoreNotNullRelaxation=false
			expected: "ALTER TABLE `t1` MODIFY COLUMN `customer_id` bigint NULL",
		},

		// IgnoreCharsetCollation
		{
			name:     "IgnoreCharsetCollation_Charset",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) CHARSET=utf8mb4",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) CHARSET=latin1",
			opts:     &DiffOptions{IgnoreCharsetCollation: true, IgnoreAutoIncrement: true, IgnoreEngine: true},
			expected: "",
		},
		{
			// The bare utf8mb4 table rule is a table-option difference, so it
			// is suppressed with the rest of them.
			name:     "IgnoreCharsetCollation_Utf8mb4WithoutCollation",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a varchar(3) DEFAULT NULL) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a varchar(3)) DEFAULT CHARSET=utf8mb4",
			opts:     &DiffOptions{IgnoreCharsetCollation: true, IgnoreAutoIncrement: true, IgnoreEngine: true},
			expected: "",
		},
		{
			name:     "IgnoreCharsetCollation_Collation",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) COLLATE=utf8mb4_general_ci",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) COLLATE=utf8mb4_unicode_ci",
			opts:     &DiffOptions{IgnoreCharsetCollation: true, IgnoreAutoIncrement: true, IgnoreEngine: true},
			expected: "",
		},
		{
			name:     "IgnoreCharsetCollation_Both",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) CHARSET=latin1 COLLATE=latin1_swedish_ci",
			opts:     &DiffOptions{IgnoreCharsetCollation: true, IgnoreAutoIncrement: true, IgnoreEngine: true},
			expected: "",
		},
		{
			name:     "IgnoreCharsetCollation_StillDetectsColumnChanges",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT) CHARSET=utf8mb4",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b VARCHAR(100)) CHARSET=latin1",
			opts:     &DiffOptions{IgnoreCharsetCollation: true, IgnoreAutoIncrement: true, IgnoreEngine: true},
			expected: "ALTER TABLE `t1` MODIFY COLUMN `b` varchar(100) NULL",
		},
		{
			// A column naming utf8mb4 without a COLLATE still matches its
			// live form when resolution against the table defaults is
			// skipped, in both directions.
			name:     "IgnoreCharsetCollation_Utf8mb4ColumnWithoutCollation",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b char(4) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT 'a') DEFAULT CHARSET=latin1",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b char(4) CHARACTER SET utf8mb4 DEFAULT 'a') DEFAULT CHARSET=latin1",
			opts:     &DiffOptions{IgnoreCharsetCollation: true, IgnoreAutoIncrement: true, IgnoreEngine: true},
			expected: "",
		},
		{
			name:     "IgnoreCharsetCollation_Utf8mb4ColumnWithoutCollation_Reverse",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b char(4) CHARACTER SET utf8mb4 DEFAULT 'a') DEFAULT CHARSET=latin1",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b char(4) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT 'a') DEFAULT CHARSET=latin1",
			opts:     &DiffOptions{IgnoreCharsetCollation: true, IgnoreAutoIncrement: true, IgnoreEngine: true},
			expected: "",
		},
		{
			name:     "IgnoreCharsetCollation_Utf8mb4ColumnWithoutCollationDetectsNonDefault",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b char(4) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT 'a') DEFAULT CHARSET=latin1",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b char(4) CHARACTER SET utf8mb4 DEFAULT 'a') DEFAULT CHARSET=latin1",
			opts:     &DiffOptions{IgnoreCharsetCollation: true, IgnoreAutoIncrement: true, IgnoreEngine: true},
			expected: "ALTER TABLE `t1` MODIFY COLUMN `b` char(4) CHARACTER SET utf8mb4 NULL DEFAULT 'a'",
		},
		{
			// A column that names utf8mb4 *with* a COLLATE is not the bare
			// case: its written collation is compared, even against a
			// server-default collation.
			name:     "IgnoreCharsetCollation_Utf8mb4ColumnWithCollationStillCompared",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b char(4) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT 'a') DEFAULT CHARSET=latin1",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b char(4) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT 'a') DEFAULT CHARSET=latin1",
			opts:     &DiffOptions{IgnoreCharsetCollation: true, IgnoreAutoIncrement: true, IgnoreEngine: true},
			expected: "ALTER TABLE `t1` MODIFY COLUMN `b` char(4) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT 'a'",
		},

		// IgnorePartitioning
		{
			name:     "IgnorePartitioning_Add",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT) PARTITION BY HASH(user_id) PARTITIONS 4",
			opts:     &DiffOptions{IgnorePartitioning: true, IgnoreAutoIncrement: true, IgnoreEngine: true},
			expected: "",
		},
		{
			name:     "IgnorePartitioning_Remove",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT) PARTITION BY HASH(user_id) PARTITIONS 4",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT)",
			opts:     &DiffOptions{IgnorePartitioning: true, IgnoreAutoIncrement: true, IgnoreEngine: true},
			expected: "",
		},
		{
			name:     "IgnorePartitioning_StillDetectsColumnChanges",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT) PARTITION BY HASH(id) PARTITIONS 4",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b VARCHAR(100)) PARTITION BY HASH(id) PARTITIONS 8",
			opts:     &DiffOptions{IgnorePartitioning: true, IgnoreAutoIncrement: true, IgnoreEngine: true},
			expected: "ALTER TABLE `t1` MODIFY COLUMN `b` varchar(100) NULL",
		},

		// Comment is always compared (no ignore option)
		{
			name:     "CommentAlwaysDetected",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) COMMENT='test table'",
			opts:     &DiffOptions{IgnoreAutoIncrement: true, IgnoreEngine: true, IgnoreCharsetCollation: true, IgnorePartitioning: true, IgnoreRowFormat: true},
			expected: "ALTER TABLE `t1` COMMENT='test table'",
		},

		// ROW_FORMAT is ignored by default but can be detected with IgnoreRowFormat=false
		{
			name:     "RowFormatDetectedWhenNotIgnored",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) ROW_FORMAT=COMPACT",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) ROW_FORMAT=DYNAMIC",
			opts:     &DiffOptions{IgnoreAutoIncrement: true, IgnoreEngine: true, IgnoreCharsetCollation: true, IgnorePartitioning: true, IgnoreRowFormat: false},
			expected: "ALTER TABLE `t1` ROW_FORMAT=DYNAMIC",
		},
		{
			name:     "RowFormatIgnoredByDefault",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) ROW_FORMAT=COMPACT",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) ROW_FORMAT=DYNAMIC",
			opts:     nil, // nil uses NewDiffOptions() which sets IgnoreRowFormat=true
			expected: "",
		},
		{
			name:     "RowFormatDefaultToCompressed",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) ROW_FORMAT=COMPRESSED",
			opts:     &DiffOptions{IgnoreAutoIncrement: true, IgnoreEngine: true, IgnoreRowFormat: false},
			expected: "ALTER TABLE `t1` ROW_FORMAT=COMPRESSED",
		},
		// The table-level KEY_BLOCK_SIZE is the compressed page size and goes
		// with ROW_FORMAT: ignored with it, and cleared in the same statement
		// as a row format change, which InnoDB otherwise rejects.
		{
			name:     "KeyBlockSizeDetectedWithRowFormat",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) ROW_FORMAT=COMPRESSED",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=4",
			opts:     &DiffOptions{IgnoreAutoIncrement: true, IgnoreEngine: true, IgnoreRowFormat: false},
			expected: "ALTER TABLE `t1` KEY_BLOCK_SIZE=4",
		},
		{
			name:     "KeyBlockSizeClearedWithRowFormatChange",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=4",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) ROW_FORMAT=DYNAMIC",
			opts:     &DiffOptions{IgnoreAutoIncrement: true, IgnoreEngine: true, IgnoreRowFormat: false},
			expected: "ALTER TABLE `t1` ROW_FORMAT=DYNAMIC, KEY_BLOCK_SIZE=0",
		},
		{
			name:     "KeyBlockSizeIgnoredWithRowFormat",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=4",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) ROW_FORMAT=DYNAMIC",
			opts:     nil,
			expected: "",
		},
		// A target without a row format clears the source's with
		// ROW_FORMAT=DEFAULT, the option MySQL stores as none; a target that
		// writes ROW_FORMAT=DEFAULT out is the same target.
		{
			name:     "RowFormatClearedWhenTargetOmitsIt",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) ROW_FORMAT=COMPACT",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			opts:     &DiffOptions{IgnoreAutoIncrement: true, IgnoreEngine: true, IgnoreRowFormat: false},
			expected: "ALTER TABLE `t1` ROW_FORMAT=DEFAULT",
		},
		{
			name:     "RowFormatDefaultParsesAsNone",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) ROW_FORMAT=COMPACT",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) ROW_FORMAT=DEFAULT",
			opts:     &DiffOptions{IgnoreAutoIncrement: true, IgnoreEngine: true, IgnoreRowFormat: false},
			expected: "ALTER TABLE `t1` ROW_FORMAT=DEFAULT",
		},
		{
			name:     "RowFormatDefaultAgainstNoneIsNoop",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) ROW_FORMAT=DEFAULT",
			opts:     &DiffOptions{IgnoreAutoIncrement: true, IgnoreEngine: true, IgnoreRowFormat: false},
			expected: "",
		},
		{
			name:     "RowFormatAndKeyBlockSizeClearedTogether",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			opts:     &DiffOptions{IgnoreAutoIncrement: true, IgnoreEngine: true, IgnoreRowFormat: false},
			expected: "ALTER TABLE `t1` ROW_FORMAT=DEFAULT, KEY_BLOCK_SIZE=0",
		},
		{
			name:     "RowFormatClearIgnoredByDefault",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) ROW_FORMAT=COMPACT",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			opts:     nil,
			expected: "",
		},

		// Combined: ignore everything possible, still detect column + index changes
		{
			name:     "IgnoreAllOptionsButDetectSchemaChanges",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100)) ENGINE=InnoDB CHARSET=utf8mb4 ROW_FORMAT=COMPACT",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name)) ENGINE=MyISAM CHARSET=latin1 ROW_FORMAT=DYNAMIC",
			opts:     &DiffOptions{IgnoreAutoIncrement: true, IgnoreEngine: true, IgnoreCharsetCollation: true, IgnorePartitioning: true, IgnoreRowFormat: true},
			expected: "ALTER TABLE `t1` ADD INDEX `idx_name` (`name`)",
		},

		// Zero-value DiffOptions (all false) detects everything
		{
			name:     "ZeroValueOptsDetectsEverything",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY AUTO_INCREMENT) ENGINE=InnoDB AUTO_INCREMENT=1",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY AUTO_INCREMENT) ENGINE=MyISAM AUTO_INCREMENT=100",
			opts:     &DiffOptions{},
			expected: "ALTER TABLE `t1` ENGINE=MyISAM, AUTO_INCREMENT=100",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ct1, err := ParseCreateTable(tt.source)
			require.NoError(t, err)

			ct2, err := ParseCreateTable(tt.target)
			require.NoError(t, err)

			stmts, err := ct1.Diff(ct2, tt.opts)
			require.NoError(t, err)

			if tt.expected == "" {
				require.Nil(t, stmts, "expected nil diff")
			} else {
				require.Len(t, stmts, 1)
				require.Equal(t, tt.expected, stmts[0].Statement)
			}
		})
	}
}

// TestDiff_IgnoreNotNullRelaxation covers the one directional DiffOption, and
// deliberately drives it through DiffCreateTables rather than Diff, because
// only that orientation reads the way consumers use it.
//
// Diff compares got->want (DiffCreateTables calls got.Diff(want, opts)), so the
// receiver is the schema being validated and the parameter is the reference it
// is validated against. Naming a direction in terms of Diff's own arguments
// therefore inverts it. Here "reference" and "validated" are named for their
// roles, and move-tables' use of them is spelled out per case: the reference is
// the move SOURCE, the validated schema is the physical TARGET, and the rule
// being asserted is that a target may be *stricter* than its source (NOT NULL
// where the source permits NULL) but never looser. See
// move/check.TargetSchemaDiff.
func TestDiff_IgnoreNotNullRelaxation(t *testing.T) {
	tests := []struct {
		name string
		// reference is the source of truth — the move's SOURCE table.
		reference string
		// validated is the schema checked against it — the move's TARGET.
		validated string
		relax     bool
		// expected is the ALTER that would turn validated into reference,
		// i.e. what a consumer reports as a mismatch. Empty means the two are
		// equivalent and the check passes.
		expected string
	}{
		{
			// Without the option a stricter target is a mismatch, which is why
			// the option has to exist for a sharded move at all.
			name:      "DefaultRejectsStricterTarget",
			reference: "CREATE TABLE t1 (id INT PRIMARY KEY, customer_id BIGINT NULL)",
			validated: "CREATE TABLE t1 (id INT PRIMARY KEY, customer_id BIGINT NOT NULL)",
			relax:     false,
			expected:  "ALTER TABLE `t1` MODIFY COLUMN `customer_id` bigint NULL",
		},
		{
			// The move-tables case: the source column still permits NULL, the
			// sharded target declares NOT NULL because a primary vindex cannot
			// map NULL to a keyspace id. Accepted.
			name:      "StricterTargetAccepted",
			reference: "CREATE TABLE t1 (id INT PRIMARY KEY, customer_id BIGINT NULL)",
			validated: "CREATE TABLE t1 (id INT PRIMARY KEY, customer_id BIGINT NOT NULL)",
			relax:     true,
			expected:  "",
		},
		{
			// Same case in the form SHOW CREATE TABLE actually reports a
			// nullable column, which is what the move checks feed in: the
			// rendered `DEFAULT NULL` must not read as a default difference
			// against the NOT NULL side's absent default.
			name:      "StricterTargetAccepted_SourceRendersDefaultNull",
			reference: "CREATE TABLE t1 (id INT PRIMARY KEY, customer_id BIGINT DEFAULT NULL)",
			validated: "CREATE TABLE t1 (id INT PRIMARY KEY, customer_id BIGINT NOT NULL)",
			relax:     true,
			expected:  "",
		},
		{
			// The direction that matters for safety: a target that LOST a
			// NOT NULL its source had is still a mismatch, so the option can
			// never quietly accept a looser target.
			name:      "LooserTargetStillRejected",
			reference: "CREATE TABLE t1 (id INT PRIMARY KEY, customer_id BIGINT NOT NULL)",
			validated: "CREATE TABLE t1 (id INT PRIMARY KEY, customer_id BIGINT NULL)",
			relax:     true,
			expected:  "ALTER TABLE `t1` MODIFY COLUMN `customer_id` bigint NOT NULL",
		},
		{
			// Forgiving one column's nullability does not stop the diff
			// reporting a different column's change.
			name:      "OtherColumnChangesStillDetected",
			reference: "CREATE TABLE t1 (id INT PRIMARY KEY, customer_id BIGINT NULL, b INT)",
			validated: "CREATE TABLE t1 (id INT PRIMARY KEY, customer_id BIGINT NOT NULL, b VARCHAR(100))",
			relax:     true,
			expected:  "ALTER TABLE `t1` MODIFY COLUMN `b` int NULL",
		},
		{
			// The relaxation is scoped to nullability alone: a column that also
			// changes type is still reported, so the option cannot smuggle a
			// type change past a consumer's check.
			name:      "TypeChangeStillDetected",
			reference: "CREATE TABLE t1 (id INT PRIMARY KEY, customer_id BIGINT NULL)",
			validated: "CREATE TABLE t1 (id INT PRIMARY KEY, customer_id INT NOT NULL)",
			relax:     true,
			expected:  "ALTER TABLE `t1` MODIFY COLUMN `customer_id` bigint NULL",
		},
		{
			// Only the bare NULL keyword collapses to "no default", so a real
			// default the target lacks is still reported.
			name:      "ExplicitDefaultStillDetected",
			reference: "CREATE TABLE t1 (id INT PRIMARY KEY, customer_id BIGINT NULL DEFAULT '0')",
			validated: "CREATE TABLE t1 (id INT PRIMARY KEY, customer_id BIGINT NOT NULL)",
			relax:     true,
			expected:  "ALTER TABLE `t1` MODIFY COLUMN `customer_id` bigint NULL DEFAULT '0'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := NewDiffOptions()
			opts.IgnoreNotNullRelaxation = tt.relax

			diff, err := DiffCreateTables("t1", tt.reference, tt.validated, opts)
			require.NoError(t, err)
			require.Equal(t, tt.expected, diff)
		})
	}
}

// TestDiffPartitionChanges covers how a partition change is emitted. MySQL
// only accepts PARTITION BY / REMOVE PARTITIONING after other alter clauses
// when separated by a space, and ADD/COALESCE PARTITION not alongside other
// alter clauses at all.
func TestDiffPartitionChanges(t *testing.T) {
	const rangeBase = "CREATE TABLE t1 (id INT NOT NULL, b INT, PRIMARY KEY (id)) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))"
	tests := []struct {
		name     string
		source   string
		target   string
		expected []string
	}{
		{
			// A repartition replaces the partitioning, type included, in one
			// statement: no REMOVE PARTITIONING first.
			name:     "ChangeType",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT) PARTITION BY HASH(user_id) PARTITIONS 4",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT) PARTITION BY KEY(id) PARTITIONS 4",
			expected: []string{"ALTER TABLE `t1` PARTITION BY KEY (`id`) PARTITIONS 4"},
		},
		{
			name:     "ChangeTypeWithColumn",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT) PARTITION BY HASH(user_id) PARTITIONS 4",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, c INT) PARTITION BY KEY(id) PARTITIONS 4",
			expected: []string{"ALTER TABLE `t1` ADD COLUMN `c` int NULL PARTITION BY KEY (`id`) PARTITIONS 4"},
		},
		{
			name:     "AddPartitioningWithColumn",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT, c INT) PARTITION BY HASH(id) PARTITIONS 2",
			expected: []string{"ALTER TABLE `t1` ADD COLUMN `c` int NULL PARTITION BY HASH (`id`) PARTITIONS 2"},
		},
		{
			name:     "RemovePartitioningWithColumn",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT) PARTITION BY HASH(id) PARTITIONS 2",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT, c INT)",
			expected: []string{"ALTER TABLE `t1` ADD COLUMN `c` int NULL REMOVE PARTITIONING"},
		},
		{
			name:     "CoalesceAlone",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT) PARTITION BY HASH(id) PARTITIONS 4",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT) PARTITION BY HASH(id) PARTITIONS 2",
			expected: []string{"ALTER TABLE `t1` COALESCE PARTITION 2"},
		},
		{
			// COALESCE can't share an ALTER, and rehashes every row anyway, so
			// it is folded into the column change as a repartition.
			name:     "CoalesceWithColumn",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT) PARTITION BY HASH(id) PARTITIONS 4",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT, c INT) PARTITION BY HASH(id) PARTITIONS 2",
			expected: []string{"ALTER TABLE `t1` ADD COLUMN `c` int NULL PARTITION BY HASH (`id`) PARTITIONS 2"},
		},
		{
			name:     "AddHashPartitionsWithColumn",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT) PARTITION BY HASH(id) PARTITIONS 2",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT, c INT) PARTITION BY HASH(id) PARTITIONS 4",
			expected: []string{"ALTER TABLE `t1` ADD COLUMN `c` int NULL PARTITION BY HASH (`id`) PARTITIONS 4"},
		},
		{
			// Appending RANGE partitions is metadata-only ADD PARTITION.
			name:     "AppendRangePartitions",
			source:   rangeBase,
			target:   "CREATE TABLE t1 (id INT NOT NULL, b INT, PRIMARY KEY (id)) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20), PARTITION p2 VALUES LESS THAN (30), PARTITION pmax VALUES LESS THAN MAXVALUE)",
			expected: []string{"ALTER TABLE `t1` ADD PARTITION (PARTITION `p2` VALUES LESS THAN (30), PARTITION `pmax` VALUES LESS THAN MAXVALUE)"},
		},
		{
			// ADD PARTITION can't share an ALTER, but is cheaper as its own
			// statement than a repartition folded into the column change.
			name:   "AppendRangePartitionWithColumn",
			source: rangeBase,
			target: "CREATE TABLE t1 (id INT NOT NULL, b INT, c INT, PRIMARY KEY (id)) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20), PARTITION p2 VALUES LESS THAN (30))",
			expected: []string{
				"ALTER TABLE `t1` ADD COLUMN `c` int NULL",
				"ALTER TABLE `t1` ADD PARTITION (PARTITION `p2` VALUES LESS THAN (30))",
			},
		},
		{
			// The separate ADD PARTITION would run after the MODIFY, which can
			// move a stored partition-key value past the last existing
			// partition (9.996 rounds to 10.00). Folded into one PARTITION BY,
			// MySQL places rows against the target partitions.
			name:     "AppendRangePartitionWithPartitionKeyChange",
			source:   "CREATE TABLE t1 (id INT NOT NULL, d DECIMAL(10,3) NOT NULL, PRIMARY KEY (id, d)) PARTITION BY RANGE (FLOOR(d)) (PARTITION p0 VALUES LESS THAN (10))",
			target:   "CREATE TABLE t1 (id INT NOT NULL, d DECIMAL(10,2) NOT NULL, PRIMARY KEY (id, d)) PARTITION BY RANGE (FLOOR(d)) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))",
			expected: []string{"ALTER TABLE `t1` MODIFY COLUMN `d` decimal(10,2) NOT NULL PARTITION BY RANGE (FLOOR(`d`)) (PARTITION `p0` VALUES LESS THAN (10), PARTITION `p1` VALUES LESS THAN (20))"},
		},
		{
			name:     "AppendRangeColumnsPartitionWithPartitionKeyChange",
			source:   "CREATE TABLE t1 (id INT NOT NULL, d DATETIME NOT NULL, PRIMARY KEY (id, d)) PARTITION BY RANGE COLUMNS (d) (PARTITION p0 VALUES LESS THAN ('2026-11-01'))",
			target:   "CREATE TABLE t1 (id INT NOT NULL, d DATE NOT NULL, PRIMARY KEY (id, d)) PARTITION BY RANGE COLUMNS (d) (PARTITION p0 VALUES LESS THAN ('2026-11-01'), PARTITION p1 VALUES LESS THAN ('2026-12-01'))",
			expected: []string{"ALTER TABLE `t1` MODIFY COLUMN `d` date NOT NULL PARTITION BY RANGE COLUMNS (`d`) (PARTITION `p0` VALUES LESS THAN ('2026-11-01'), PARTITION `p1` VALUES LESS THAN ('2026-12-01'))"},
		},
		{
			// The partitioning reads d through the generated column g.
			name:     "AppendRangePartitionWithGeneratedPartitionKeyChange",
			source:   "CREATE TABLE t1 (d DECIMAL(10,3), g INT AS (FLOOR(d)) STORED) PARTITION BY RANGE (g) (PARTITION p0 VALUES LESS THAN (10))",
			target:   "CREATE TABLE t1 (d DECIMAL(10,2), g INT AS (FLOOR(d)) STORED) PARTITION BY RANGE (g) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))",
			expected: []string{"ALTER TABLE `t1` MODIFY COLUMN `d` decimal(10,2) NULL PARTITION BY RANGE (`g`) (PARTITION `p0` VALUES LESS THAN (10), PARTITION `p1` VALUES LESS THAN (20))"},
		},
		{
			// A column the partitioning does not read can still change separately.
			name:     "AppendRangePartitionWithUnrelatedGeneratedColumnChange",
			source:   "CREATE TABLE t1 (d DECIMAL(10,3), e INT, g INT AS (FLOOR(d)) STORED) PARTITION BY RANGE (g) (PARTITION p0 VALUES LESS THAN (10))",
			target:   "CREATE TABLE t1 (d DECIMAL(10,3), e BIGINT, g INT AS (FLOOR(d)) STORED) PARTITION BY RANGE (g) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))",
			expected: []string{"ALTER TABLE `t1` MODIFY COLUMN `e` bigint NULL", "ALTER TABLE `t1` ADD PARTITION (PARTITION `p1` VALUES LESS THAN (20))"},
		},
		{
			name:     "KeyAlgorithmChange",
			source:   "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY) PARTITION BY KEY ALGORITHM=1 (id) PARTITIONS 2",
			target:   "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY) PARTITION BY KEY ALGORITHM=2 (id) PARTITIONS 2",
			expected: []string{"ALTER TABLE `t1` PARTITION BY KEY (`id`) PARTITIONS 2"},
		},
		{
			name:     "KeyAlgorithmWithCountChange",
			source:   "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY) PARTITION BY KEY (id) PARTITIONS 2",
			target:   "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY) PARTITION BY KEY ALGORITHM=1 (id) PARTITIONS 3",
			expected: []string{"ALTER TABLE `t1` PARTITION BY KEY ALGORITHM=1 (`id`) PARTITIONS 3"},
		},
		{
			name:     "AppendWithSubpartitionKeyAlgorithmChange",
			source:   "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY) PARTITION BY RANGE (id) SUBPARTITION BY KEY ALGORITHM=1 (id) SUBPARTITIONS 2 (PARTITION p0 VALUES LESS THAN (10))",
			target:   "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY) PARTITION BY RANGE (id) SUBPARTITION BY KEY (id) SUBPARTITIONS 2 (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))",
			expected: []string{"ALTER TABLE `t1` PARTITION BY RANGE (`id`) SUBPARTITION BY KEY (`id`) SUBPARTITIONS 2 (PARTITION `p0` VALUES LESS THAN (10), PARTITION `p1` VALUES LESS THAN (20))"},
		},
		{
			name:     "AppendExpressionBound",
			source:   "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10))",
			target:   "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (UNIX_TIMESTAMP('2031-01-01 00:00:00')))",
			expected: []string{"ALTER TABLE `t1` ADD PARTITION (PARTITION `p1` VALUES LESS THAN (UNIX_TIMESTAMP('2031-01-01 00:00:00')))"},
		},
		{
			// A LIST value left as an expression may evaluate differently
			// when the ALTER runs (UNIX_TIMESTAMP reads the session time
			// zone), and a LIST REORGANIZE deletes the rows of a value it
			// loses. PARTITION BY fails with 1526 instead.
			name:     "ListExpressionValueCommentChange",
			source:   "CREATE TABLE t1 (id BIGINT NOT NULL PRIMARY KEY) PARTITION BY LIST (id) (PARTITION p0 VALUES IN (1), PARTITION p1 VALUES IN (UNIX_TIMESTAMP('2030-01-01 00:00:00')) COMMENT 'old')",
			target:   "CREATE TABLE t1 (id BIGINT NOT NULL PRIMARY KEY) PARTITION BY LIST (id) (PARTITION p0 VALUES IN (1), PARTITION p1 VALUES IN (UNIX_TIMESTAMP('2030-01-01 00:00:00')) COMMENT 'new')",
			expected: []string{"ALTER TABLE `t1` PARTITION BY LIST (`id`) (PARTITION `p0` VALUES IN (1), PARTITION `p1` VALUES IN (UNIX_TIMESTAMP('2030-01-01 00:00:00')) COMMENT = 'new')"},
		},
		{
			name:     "ListColumnsExpressionInTupleCommentChange",
			source:   "CREATE TABLE t1 (a BIGINT NOT NULL, b INT NOT NULL, PRIMARY KEY (a, b)) PARTITION BY LIST COLUMNS (a, b) (PARTITION p0 VALUES IN ((UNIX_TIMESTAMP('2030-01-01 00:00:00'), 1)) COMMENT 'old')",
			target:   "CREATE TABLE t1 (a BIGINT NOT NULL, b INT NOT NULL, PRIMARY KEY (a, b)) PARTITION BY LIST COLUMNS (a, b) (PARTITION p0 VALUES IN ((UNIX_TIMESTAMP('2030-01-01 00:00:00'), 1)) COMMENT 'new')",
			expected: []string{"ALTER TABLE `t1` PARTITION BY LIST COLUMNS (`a`, `b`) (PARTITION `p0` VALUES IN ((UNIX_TIMESTAMP('2030-01-01 00:00:00'), 1)) COMMENT = 'new')"},
		},
		{
			// A folded constant is a value, so it still qualifies.
			name:     "ListFoldedValueCommentChange",
			source:   "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY) PARTITION BY LIST (id) (PARTITION p0 VALUES IN (1), PARTITION p1 VALUES IN (10 + 10) COMMENT 'old')",
			target:   "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY) PARTITION BY LIST (id) (PARTITION p0 VALUES IN (1), PARTITION p1 VALUES IN (20) COMMENT 'new')",
			expected: []string{"ALTER TABLE `t1` REORGANIZE PARTITION `p1` INTO (PARTITION `p1` VALUES IN (20) COMMENT = 'new')"},
		},
		{
			name:     "PartitionMaxRowsChange",
			source:   "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20) MAX_ROWS = 100)",
			target:   "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20) MAX_ROWS = 200)",
			expected: []string{"ALTER TABLE `t1` REORGANIZE PARTITION `p1` INTO (PARTITION `p1` VALUES LESS THAN (20) MAX_ROWS = 200)"},
		},
		{
			name:     "PartitionStorageOptionsEmitted",
			source:   "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10))",
			target:   "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10) COMMENT 'c' DATA DIRECTORY '/data/' INDEX DIRECTORY '/idx' MAX_ROWS 9 MIN_ROWS 1 TABLESPACE ts1 NODEGROUP 0)",
			expected: []string{"ALTER TABLE `t1` REORGANIZE PARTITION `p0` INTO (PARTITION `p0` VALUES LESS THAN (10) COMMENT = 'c' DATA DIRECTORY = '/data' INDEX DIRECTORY = '/idx' MAX_ROWS = 9 MIN_ROWS = 1 TABLESPACE = `ts1` NODEGROUP = 0)"},
		},
		{
			name:     "AppendWithStorageOptions",
			source:   "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10))",
			target:   "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20) MAX_ROWS = 5)",
			expected: []string{"ALTER TABLE `t1` ADD PARTITION (PARTITION `p1` VALUES LESS THAN (20) MAX_ROWS = 5)"},
		},
		{
			name:     "SubpartitionStorageOptionChange",
			source:   "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY) PARTITION BY RANGE (id) SUBPARTITION BY HASH (id) (PARTITION p0 VALUES LESS THAN (10) (SUBPARTITION s0, SUBPARTITION s1))",
			target:   "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY) PARTITION BY RANGE (id) SUBPARTITION BY HASH (id) (PARTITION p0 VALUES LESS THAN (10) MAX_ROWS = 9 (SUBPARTITION s0 MAX_ROWS = 5, SUBPARTITION s1))",
			expected: []string{"ALTER TABLE `t1` REORGANIZE PARTITION `p0` INTO (PARTITION `p0` VALUES LESS THAN (10) (SUBPARTITION `s0` MAX_ROWS = 5, SUBPARTITION `s1` MAX_ROWS = 9))"},
		},
		{
			// MySQL prints a partition's options on each named subpartition.
			name:     "PartitionOptionsOnNamedSubpartitionsNoDiff",
			source:   "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY) PARTITION BY RANGE (id) SUBPARTITION BY HASH (id) (PARTITION p0 VALUES LESS THAN (10) (SUBPARTITION s0 COMMENT = 'c' MAX_ROWS = 9, SUBPARTITION s1 COMMENT = 'c' MAX_ROWS = 9))",
			target:   "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY) PARTITION BY RANGE (id) SUBPARTITION BY HASH (id) (PARTITION p0 VALUES LESS THAN (10) COMMENT 'c' MAX_ROWS = 9 (SUBPARTITION s0, SUBPARTITION s1))",
			expected: []string{},
		},
		{
			name:     "FilePerTableTablespaceNoDiff",
			source:   "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10) TABLESPACE = `innodb_file_per_table`, PARTITION p1 VALUES LESS THAN (20) TABLESPACE = `innodb_file_per_table`)",
			target:   "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))",
			expected: []string{},
		},
		{
			name:     "AppendListPartition",
			source:   "CREATE TABLE t1 (id INT NOT NULL, PRIMARY KEY (id)) PARTITION BY LIST (id) (PARTITION p0 VALUES IN (1, 2))",
			target:   "CREATE TABLE t1 (id INT NOT NULL, PRIMARY KEY (id)) PARTITION BY LIST (id) (PARTITION p0 VALUES IN (1, 2), PARTITION p1 VALUES IN (3))",
			expected: []string{"ALTER TABLE `t1` ADD PARTITION (PARTITION `p1` VALUES IN (3))"},
		},
		{
			name:     "AppendRangeColumnsPartition",
			source:   "CREATE TABLE t1 (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (a, b)) PARTITION BY RANGE COLUMNS (a, b) (PARTITION p0 VALUES LESS THAN (10, 10))",
			target:   "CREATE TABLE t1 (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (a, b)) PARTITION BY RANGE COLUMNS (a, b) (PARTITION p0 VALUES LESS THAN (10, 10), PARTITION p1 VALUES LESS THAN (20, MAXVALUE))",
			expected: []string{"ALTER TABLE `t1` ADD PARTITION (PARTITION `p1` VALUES LESS THAN (20, MAXVALUE))"},
		},
		{
			name:     "AppendSubpartitionedRangePartition",
			source:   "CREATE TABLE t1 (dt DATE NOT NULL, PRIMARY KEY (dt)) PARTITION BY RANGE (YEAR(dt)) SUBPARTITION BY HASH (dayofmonth(dt)) SUBPARTITIONS 2 (PARTITION p0 VALUES LESS THAN (2020))",
			target:   "CREATE TABLE t1 (dt DATE NOT NULL, PRIMARY KEY (dt)) PARTITION BY RANGE (YEAR(dt)) SUBPARTITION BY HASH (dayofmonth(dt)) SUBPARTITIONS 2 (PARTITION p0 VALUES LESS THAN (2020), PARTITION p1 VALUES LESS THAN (2030))",
			expected: []string{"ALTER TABLE `t1` ADD PARTITION (PARTITION `p1` VALUES LESS THAN (2030))"},
		},
		{
			// Dropping a partition is a repartition, never DROP PARTITION:
			// DROP PARTITION deletes the partition's rows.
			name:     "DropRangePartition",
			source:   rangeBase,
			target:   "CREATE TABLE t1 (id INT NOT NULL, b INT, PRIMARY KEY (id)) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10))",
			expected: []string{"ALTER TABLE `t1` PARTITION BY RANGE (`id`) (PARTITION `p0` VALUES LESS THAN (10))"},
		},
		{
			// Inserting a partition between two others splits the next one.
			name:     "SplitRangePartition",
			source:   rangeBase,
			target:   "CREATE TABLE t1 (id INT NOT NULL, b INT, PRIMARY KEY (id)) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1a VALUES LESS THAN (15), PARTITION p1 VALUES LESS THAN (20))",
			expected: []string{"ALTER TABLE `t1` REORGANIZE PARTITION `p1` INTO (PARTITION `p1a` VALUES LESS THAN (15), PARTITION `p1` VALUES LESS THAN (20))"},
		},
		{
			// The usual rolling-window change: split next month out of the
			// MAXVALUE partition.
			name:     "SplitMaxvaluePartition",
			source:   "CREATE TABLE t1 (d DATE NOT NULL, PRIMARY KEY (d)) PARTITION BY RANGE COLUMNS (d) (PARTITION p202610 VALUES LESS THAN ('2026-11-01'), PARTITION pmax VALUES LESS THAN (MAXVALUE))",
			target:   "CREATE TABLE t1 (d DATE NOT NULL, PRIMARY KEY (d)) PARTITION BY RANGE COLUMNS (d) (PARTITION p202610 VALUES LESS THAN ('2026-11-01'), PARTITION p202611 VALUES LESS THAN ('2026-12-01'), PARTITION pmax VALUES LESS THAN (MAXVALUE))",
			expected: []string{"ALTER TABLE `t1` REORGANIZE PARTITION `pmax` INTO (PARTITION `p202611` VALUES LESS THAN ('2026-12-01'), PARTITION `pmax` VALUES LESS THAN MAXVALUE)"},
		},
		{
			name:     "MergeRangePartitions",
			source:   "CREATE TABLE t1 (id INT NOT NULL, PRIMARY KEY (id)) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20), PARTITION p2 VALUES LESS THAN (30))",
			target:   "CREATE TABLE t1 (id INT NOT NULL, PRIMARY KEY (id)) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10), PARTITION p2 VALUES LESS THAN (30))",
			expected: []string{"ALTER TABLE `t1` REORGANIZE PARTITION `p1`, `p2` INTO (PARTITION `p2` VALUES LESS THAN (30))"},
		},
		{
			name:     "RenameRangePartition",
			source:   rangeBase,
			target:   "CREATE TABLE t1 (id INT NOT NULL, b INT, PRIMARY KEY (id)) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10), PARTITION q1 VALUES LESS THAN (20))",
			expected: []string{"ALTER TABLE `t1` REORGANIZE PARTITION `p1` INTO (PARTITION `q1` VALUES LESS THAN (20))"},
		},
		{
			// Moving a range boundary inside the run is fine; the run still
			// ends at 20.
			name:     "MoveRangeBoundary",
			source:   rangeBase,
			target:   "CREATE TABLE t1 (id INT NOT NULL, b INT, PRIMARY KEY (id)) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (5), PARTITION p1 VALUES LESS THAN (20))",
			expected: []string{"ALTER TABLE `t1` REORGANIZE PARTITION `p0`, `p1` INTO (PARTITION `p0` VALUES LESS THAN (5), PARTITION `p1` VALUES LESS THAN (20))"},
		},
		{
			// Shrinking the table's range is not a REORGANIZE (MySQL error
			// 1520): a repartition fails if rows fall outside the new range.
			name:     "ShrinkLastRangePartition",
			source:   rangeBase,
			target:   "CREATE TABLE t1 (id INT NOT NULL, b INT, PRIMARY KEY (id)) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (15))",
			expected: []string{"ALTER TABLE `t1` PARTITION BY RANGE (`id`) (PARTITION `p0` VALUES LESS THAN (10), PARTITION `p1` VALUES LESS THAN (15))"},
		},
		{
			name:     "MoveListValue",
			source:   "CREATE TABLE t1 (id INT NOT NULL, PRIMARY KEY (id)) PARTITION BY LIST (id) (PARTITION p0 VALUES IN (1, 2), PARTITION p1 VALUES IN (3), PARTITION p2 VALUES IN (4))",
			target:   "CREATE TABLE t1 (id INT NOT NULL, PRIMARY KEY (id)) PARTITION BY LIST (id) (PARTITION p0 VALUES IN (1), PARTITION p1 VALUES IN (2, 3), PARTITION p2 VALUES IN (4))",
			expected: []string{"ALTER TABLE `t1` REORGANIZE PARTITION `p0`, `p1` INTO (PARTITION `p0` VALUES IN (1), PARTITION `p1` VALUES IN (2, 3))"},
		},
		{
			// A LIST REORGANIZE that leaves a value out silently deletes the
			// rows holding it, so dropping a value is a repartition, which
			// fails (error 1526) instead.
			name:     "DropListValue",
			source:   "CREATE TABLE t1 (id INT NOT NULL, PRIMARY KEY (id)) PARTITION BY LIST (id) (PARTITION p0 VALUES IN (1, 2), PARTITION p1 VALUES IN (3))",
			target:   "CREATE TABLE t1 (id INT NOT NULL, PRIMARY KEY (id)) PARTITION BY LIST (id) (PARTITION p0 VALUES IN (1), PARTITION p1 VALUES IN (3))",
			expected: []string{"ALTER TABLE `t1` PARTITION BY LIST (`id`) (PARTITION `p0` VALUES IN (1), PARTITION `p1` VALUES IN (3))"},
		},
		{
			name:     "DropListPartition",
			source:   "CREATE TABLE t1 (id INT NOT NULL, PRIMARY KEY (id)) PARTITION BY LIST (id) (PARTITION p0 VALUES IN (1, 2), PARTITION p1 VALUES IN (3), PARTITION p2 VALUES IN (4))",
			target:   "CREATE TABLE t1 (id INT NOT NULL, PRIMARY KEY (id)) PARTITION BY LIST (id) (PARTITION p0 VALUES IN (1, 2), PARTITION p2 VALUES IN (4))",
			expected: []string{"ALTER TABLE `t1` PARTITION BY LIST (`id`) (PARTITION `p0` VALUES IN (1, 2), PARTITION `p2` VALUES IN (4))"},
		},
		{
			// Multi-column LIST COLUMNS values are tuples, and are emitted as
			// tuples.
			name:     "AddMultiColumnListPartitioning",
			source:   "CREATE TABLE t1 (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (a, b))",
			target:   "CREATE TABLE t1 (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (a, b)) PARTITION BY LIST COLUMNS (a, b) (PARTITION p0 VALUES IN ((1, 2), (3, 4)), PARTITION p1 VALUES IN ((5, 6)))",
			expected: []string{"ALTER TABLE `t1` PARTITION BY LIST COLUMNS (`a`, `b`) (PARTITION `p0` VALUES IN ((1, 2), (3, 4)), PARTITION `p1` VALUES IN ((5, 6)))"},
		},
		{
			// The same values grouped into different tuples are different
			// partitioning. Moving a tuple between partitions keeps the set
			// of tuples, so it is a REORGANIZE.
			name:     "MoveMultiColumnListTuple",
			source:   "CREATE TABLE t1 (a INT NOT NULL, b VARCHAR(10) NOT NULL, PRIMARY KEY (a, b)) PARTITION BY LIST COLUMNS (a, b) (PARTITION p0 VALUES IN ((1, 'x'), (3, 'y')), PARTITION p1 VALUES IN ((5, 'z')))",
			target:   "CREATE TABLE t1 (a INT NOT NULL, b VARCHAR(10) NOT NULL, PRIMARY KEY (a, b)) PARTITION BY LIST COLUMNS (a, b) (PARTITION p0 VALUES IN ((1, 'x')), PARTITION p1 VALUES IN ((3, 'y'), (5, 'z')))",
			expected: []string{"ALTER TABLE `t1` REORGANIZE PARTITION `p0`, `p1` INTO (PARTITION `p0` VALUES IN ((1, 'x')), PARTITION `p1` VALUES IN ((3, 'y'), (5, 'z')))"},
		},
		{
			// Regrouping the same scalar values into different tuples changes
			// the set of tuples: a repartition, never a REORGANIZE.
			name:     "RegroupMultiColumnListTuples",
			source:   "CREATE TABLE t1 (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (a, b)) PARTITION BY LIST COLUMNS (a, b) (PARTITION p0 VALUES IN ((1, 2), (3, 4)))",
			target:   "CREATE TABLE t1 (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (a, b)) PARTITION BY LIST COLUMNS (a, b) (PARTITION p0 VALUES IN ((1, 3), (2, 4)))",
			expected: []string{"ALTER TABLE `t1` PARTITION BY LIST COLUMNS (`a`, `b`) (PARTITION `p0` VALUES IN ((1, 3), (2, 4)))"},
		},
		{
			// NULL is a value, not the string 'NULL': emitted quoted, the
			// REORGANIZE would move the NULL rows into no partition and MySQL
			// would delete them.
			name:     "ListNullValueCommentChange",
			source:   "CREATE TABLE t1 (id INT NOT NULL, s VARCHAR(10)) PARTITION BY LIST COLUMNS (s) (PARTITION p0 VALUES IN (NULL, 'a'), PARTITION p1 VALUES IN ('b'))",
			target:   "CREATE TABLE t1 (id INT NOT NULL, s VARCHAR(10)) PARTITION BY LIST COLUMNS (s) (PARTITION p0 VALUES IN (NULL, 'a') COMMENT 'x', PARTITION p1 VALUES IN ('b'))",
			expected: []string{"ALTER TABLE `t1` REORGANIZE PARTITION `p0` INTO (PARTITION `p0` VALUES IN (NULL, 'a') COMMENT = 'x')"},
		},
		{
			// NULL and the string 'NULL' are different values, so swapping one
			// for the other changes the value set: a repartition.
			name:     "ListNullValueToStringNull",
			source:   "CREATE TABLE t1 (id INT NOT NULL, s VARCHAR(10)) PARTITION BY LIST COLUMNS (s) (PARTITION p0 VALUES IN (NULL, 'a'))",
			target:   "CREATE TABLE t1 (id INT NOT NULL, s VARCHAR(10)) PARTITION BY LIST COLUMNS (s) (PARTITION p0 VALUES IN ('NULL', 'a'))",
			expected: []string{"ALTER TABLE `t1` PARTITION BY LIST COLUMNS (`s`) (PARTITION `p0` VALUES IN ('NULL', 'a'))"},
		},
		{
			// Appending a partition while the subpartitioning changes is a
			// repartition: ADD PARTITION would leave the subpartitioning as
			// it was.
			name:     "AppendWithSubpartitionChange",
			source:   "CREATE TABLE t1 (id INT NOT NULL, PRIMARY KEY (id)) PARTITION BY RANGE (id) SUBPARTITION BY HASH (id) SUBPARTITIONS 2 (PARTITION p0 VALUES LESS THAN (10))",
			target:   "CREATE TABLE t1 (id INT NOT NULL, PRIMARY KEY (id)) PARTITION BY RANGE (id) SUBPARTITION BY HASH (id) SUBPARTITIONS 4 (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))",
			expected: []string{"ALTER TABLE `t1` PARTITION BY RANGE (`id`) SUBPARTITION BY HASH (`id`) SUBPARTITIONS 4 (PARTITION `p0` VALUES LESS THAN (10), PARTITION `p1` VALUES LESS THAN (20))"},
		},
		{
			// REORGANIZE can't share an ALTER and copies the table in spirit
			// anyway, so alongside a column change it is a repartition.
			name:     "SplitRangePartitionWithColumn",
			source:   rangeBase,
			target:   "CREATE TABLE t1 (id INT NOT NULL, b INT, c INT, PRIMARY KEY (id)) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1a VALUES LESS THAN (15), PARTITION p1 VALUES LESS THAN (20))",
			expected: []string{"ALTER TABLE `t1` ADD COLUMN `c` int NULL PARTITION BY RANGE (`id`) (PARTITION `p0` VALUES LESS THAN (10), PARTITION `p1a` VALUES LESS THAN (15), PARTITION `p1` VALUES LESS THAN (20))"},
		},
		{
			name:     "AppendWithChangedExpression",
			source:   rangeBase,
			target:   "CREATE TABLE t1 (id INT NOT NULL, b INT NOT NULL, PRIMARY KEY (id, b)) PARTITION BY RANGE (b) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20), PARTITION p2 VALUES LESS THAN (30))",
			expected: []string{"ALTER TABLE `t1` MODIFY COLUMN `b` int NOT NULL, DROP PRIMARY KEY, ADD PRIMARY KEY (`id`, `b`) PARTITION BY RANGE (`b`) (PARTITION `p0` VALUES LESS THAN (10), PARTITION `p1` VALUES LESS THAN (20), PARTITION `p2` VALUES LESS THAN (30))"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source, err := ParseCreateTable(tt.source)
			require.NoError(t, err)
			target, err := ParseCreateTable(tt.target)
			require.NoError(t, err)
			stmts, err := source.Diff(target, nil)
			require.NoError(t, err)
			got := make([]string, 0, len(stmts))
			for _, s := range stmts {
				got = append(got, s.Statement)
			}
			require.Equal(t, tt.expected, got)
		})
	}
}

func TestNewDiffOptions(t *testing.T) {
	opts := NewDiffOptions()
	require.True(t, opts.IgnoreAutoIncrement, "IgnoreAutoIncrement should default to true")
	require.False(t, opts.IgnoreColumnAutoIncrement, "IgnoreColumnAutoIncrement should default to false")
	require.True(t, opts.IgnoreEngine, "IgnoreEngine should default to true")
	require.False(t, opts.IgnoreCharsetCollation, "IgnoreCharsetCollation should default to false")
	require.False(t, opts.IgnorePartitioning, "IgnorePartitioning should default to false")
	require.True(t, opts.IgnoreRowFormat, "IgnoreRowFormat should default to true")
}

// TestDiffPartitionStorageOptionChange checks that a change to any one
// storage option of a partition, or of a named subpartition, is a diff.
func TestDiffPartitionStorageOptionChange(t *testing.T) {
	options := []struct{ from, to, emitted string }{
		{"DATA DIRECTORY = '/a'", "DATA DIRECTORY = '/b'", "DATA DIRECTORY = '/b'"},
		{"INDEX DIRECTORY = '/a'", "INDEX DIRECTORY = '/b'", "INDEX DIRECTORY = '/b'"},
		{"MAX_ROWS = 1", "MAX_ROWS = 2", "MAX_ROWS = 2"},
		{"MIN_ROWS = 1", "MIN_ROWS = 2", "MIN_ROWS = 2"},
		{"TABLESPACE = ts1", "TABLESPACE = ts2", "TABLESPACE = `ts2`"},
		{"NODEGROUP = 1", "NODEGROUP = 2", "NODEGROUP = 2"},
	}
	for _, opt := range options {
		t.Run(opt.to, func(t *testing.T) {
			for _, layout := range []string{
				"PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10) %s)",
				"PARTITION BY RANGE (id) SUBPARTITION BY HASH (id) (PARTITION p0 VALUES LESS THAN (10) (SUBPARTITION s0 %s, SUBPARTITION s1))",
			} {
				source, err := ParseCreateTable("CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY) " + strings.Replace(layout, "%s", opt.from, 1))
				require.NoError(t, err)
				target, err := ParseCreateTable("CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY) " + strings.Replace(layout, "%s", opt.to, 1))
				require.NoError(t, err)
				stmts, err := source.Diff(target, nil)
				require.NoError(t, err)
				require.Len(t, stmts, 1, layout)
				require.Contains(t, stmts[0].Statement, "REORGANIZE PARTITION `p0` INTO")
				require.Contains(t, stmts[0].Statement, opt.emitted)
			}
		})
	}
}

// A table-level KEY_BLOCK_SIZE change (compared under IgnoreRowFormat: false)
// emits only the new size. On a table that stays compressed MySQL keeps the old
// size on the existing indexes, so that change does not converge; compressed
// tables are out of scope, and the diff does not re-create the indexes (the
// primary key would need a DROP PRIMARY KEY, which Spirit refuses).
func TestDiffTableKeyBlockSizeChange(t *testing.T) {
	compared := NewDiffOptions()
	compared.IgnoreRowFormat = false
	tests := []struct {
		name     string
		source   string
		target   string
		opts     *DiffOptions
		expected []string
	}{
		{
			name:     "Resized",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, KEY k (c), UNIQUE KEY u (c, id)) ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, KEY k (c), UNIQUE KEY u (c, id)) ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=4",
			opts:     compared,
			expected: []string{"ALTER TABLE `t1` KEY_BLOCK_SIZE=4"},
		},
		{
			name:     "ToTheImplicitSize",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, KEY k (c)) ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=4",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, KEY k (c)) ROW_FORMAT=COMPRESSED",
			opts:     compared,
			expected: []string{"ALTER TABLE `t1` KEY_BLOCK_SIZE=0"},
		},
		{
			// A table without an explicit size stores none on its indexes, so
			// they take the new one on their own: this one converges.
			name:     "FromTheImplicitSize",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, KEY k (c)) ROW_FORMAT=COMPRESSED",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, KEY k (c)) ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=4",
			opts:     compared,
			expected: []string{"ALTER TABLE `t1` KEY_BLOCK_SIZE=4"},
		},
		{
			// InnoDB drops every index KEY_BLOCK_SIZE when the table stops
			// being compressed.
			name:     "ToUncompressed",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, KEY k (c)) ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, KEY k (c)) ROW_FORMAT=DYNAMIC",
			opts:     compared,
			expected: []string{"ALTER TABLE `t1` ROW_FORMAT=DYNAMIC, KEY_BLOCK_SIZE=0"},
		},
		{
			name:     "IgnoredByDefault",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, KEY k (c)) ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, KEY k (c)) ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=4",
			opts:     nil,
			expected: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source, err := ParseCreateTable(tt.source)
			require.NoError(t, err)
			target, err := ParseCreateTable(tt.target)
			require.NoError(t, err)
			stmts, err := source.Diff(target, tt.opts)
			require.NoError(t, err)
			var got []string
			for _, s := range stmts {
				got = append(got, s.Statement)
			}
			assert.Equal(t, tt.expected, got)
		})
	}
}

// replacementName stays inside MySQL's 64-character identifier limit and
// clear of every name in use, whatever its case.
func TestReplacementName(t *testing.T) {
	taken := map[string]bool{"k": true, "_k_new": true, "_k_new2": true}
	assert.Equal(t, "_k_new3", replacementName("k", taken))
	assert.True(t, taken["_k_new3"], "the chosen name is taken from then on")
	assert.Equal(t, "_K_new4", replacementName("K", taken), "names are compared case-insensitively, and the given case kept")

	long := strings.Repeat("é", 64)
	got := replacementName(long, map[string]bool{})
	assert.Len(t, []rune(got), 64)
	assert.Equal(t, "_"+strings.Repeat("é", 59)+"_new", got)
	got = replacementName(long, map[string]bool{strings.ToLower(got): true})
	assert.Len(t, []rune(got), 64)
	assert.Equal(t, "_"+strings.Repeat("é", 58)+"_new2", got)
}
