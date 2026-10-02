package statement

import (
	"testing"

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
			expected: "ALTER TABLE `t1` MODIFY COLUMN `c` int NULL FIRST, MODIFY COLUMN `a` int NULL AFTER `c`, MODIFY COLUMN `b` int NULL AFTER `a`",
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
			// MySQL no-op, so the diff must emit two separate statements.
			name:   "AddFulltextParser",
			source: "CREATE TABLE t1 (id INT PRIMARY KEY, b TEXT, FULLTEXT KEY ft_b (b))",
			target: "CREATE TABLE t1 (id INT PRIMARY KEY, b TEXT, FULLTEXT KEY ft_b (b) WITH PARSER ngram)",
			expectedStatements: []string{
				"ALTER TABLE `t1` DROP INDEX `ft_b`",
				"ALTER TABLE `t1` ADD FULLTEXT INDEX `ft_b` (`b`) WITH PARSER ngram",
			},
		},
		{
			name:   "RemoveFulltextParser",
			source: "CREATE TABLE t1 (id INT PRIMARY KEY, b TEXT, FULLTEXT KEY ft_b (b) WITH PARSER ngram)",
			target: "CREATE TABLE t1 (id INT PRIMARY KEY, b TEXT, FULLTEXT KEY ft_b (b))",
			expectedStatements: []string{
				"ALTER TABLE `t1` DROP INDEX `ft_b`",
				"ALTER TABLE `t1` ADD FULLTEXT INDEX `ft_b` (`b`)",
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
			// change; emit it as two separate statements (see AddFulltextParser).
			name:   "AddIndexKeyBlockSize",
			source: "CREATE TABLE t1 (id INT PRIMARY KEY, b VARCHAR(100), KEY idx_b (b))",
			target: "CREATE TABLE t1 (id INT PRIMARY KEY, b VARCHAR(100), KEY idx_b (b) KEY_BLOCK_SIZE=8)",
			expectedStatements: []string{
				"ALTER TABLE `t1` DROP INDEX `idx_b`",
				"ALTER TABLE `t1` ADD INDEX `idx_b` (`b`) KEY_BLOCK_SIZE=8",
			},
		},
		{
			name:   "RemoveIndexKeyBlockSize",
			source: "CREATE TABLE t1 (id INT PRIMARY KEY, b VARCHAR(100), KEY idx_b (b) KEY_BLOCK_SIZE=8)",
			target: "CREATE TABLE t1 (id INT PRIMARY KEY, b VARCHAR(100), KEY idx_b (b))",
			expectedStatements: []string{
				"ALTER TABLE `t1` DROP INDEX `idx_b`",
				"ALTER TABLE `t1` ADD INDEX `idx_b` (`b`)",
			},
		},
		{
			name:     "IndexKeyBlockSizeNoChange",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, b VARCHAR(100), KEY idx_b (b) KEY_BLOCK_SIZE=8)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b VARCHAR(100), KEY idx_b (b) KEY_BLOCK_SIZE=8)",
			expected: "",
		},
		{
			// An index rebuilt for an unrelated reason (here: a column list
			// change) must preserve KEY_BLOCK_SIZE in the re-add.
			name:     "IndexRebuildPreservesKeyBlockSize",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT, KEY idx_ab (a) KEY_BLOCK_SIZE=8)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT, KEY idx_ab (a, b) KEY_BLOCK_SIZE=8)",
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
			name:     "ModifyIndexType",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name) USING HASH)",
			expected: "ALTER TABLE `t1` DROP INDEX `idx_name`, ADD INDEX `idx_name` (`name`) USING HASH",
		},
		{
			name:     "AddIndexWithComment",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name) COMMENT 'name index')",
			expected: "ALTER TABLE `t1` ADD INDEX `idx_name` (`name`) COMMENT 'name index'",
		},

		// Fulltext indexes
		// Note: Spatial indexes can not be supported, because the TiDB parser does not support them.
		// i.e. GEOMETRY, POINT, LINESTRING, and other spatial column types.
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
		// Constraint Modifications
		{
			name:     "ModifyCheckConstraint",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, age INT, CONSTRAINT chk_age CHECK (age >= 0))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, age INT, CONSTRAINT chk_age CHECK (age >= 18))",
			expected: "ALTER TABLE `t1` DROP CHECK `chk_age`, ADD CONSTRAINT `chk_age` CHECK (`age`>=18)",
		},
		{
			// CHECK constraints with charset introducers like _utf8mb3 are normalized
			// during parsing. MySQL generates different auto-names based on the original
			// expression text, so the same logical constraint can have different names.
			// The diff should recognize these as equivalent and produce no diff.
			name:     "CheckConstraintCharsetIntroducerNoDiff",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, type enum('A','B'), tok varchar(15), CONSTRAINT chk_tok_abc123 CHECK (type = _utf8mb3'A' AND tok IS NOT NULL OR type = _utf8mb3'B' AND tok IS NULL))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, type enum('A','B'), tok varchar(15), CONSTRAINT chk_tok_def456 CHECK (type = 'A' AND tok IS NOT NULL OR type = 'B' AND tok IS NULL))",
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
			expected: "ALTER TABLE `s2` MODIFY COLUMN `c` varchar(10) NULL, MODIFY COLUMN `u` varchar(10) NULL, COLLATE=utf8_general_ci",
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
			expected: "ALTER TABLE `t1` MODIFY COLUMN `a` varchar(3) NULL, COLLATE=latin1_swedish_ci",
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
			expected: "ALTER TABLE `t1` MODIFY COLUMN `a` varchar(3) NULL DEFAULT NULL, COLLATE=utf8mb4_bin",
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
			expected: "ALTER TABLE `t1` MODIFY COLUMN `b` varchar(3) NULL DEFAULT NULL",
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
			expected: "ALTER TABLE `t1` MODIFY COLUMN `name` varchar(100) NULL, COLLATE=utf8mb4_general_ci",
		},
		{
			name:     "TableCharsetChangeModifiesInheritingColumn",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100)) CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100)) CHARSET=latin1 COLLATE=latin1_swedish_ci",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `name` varchar(100) NULL, DEFAULT CHARSET=latin1, COLLATE=latin1_swedish_ci",
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
			expected: "ALTER TABLE `t1` MODIFY COLUMN `is_active` tinyint(1) NULL DEFAULT 0",
		},
		{
			name:     "BooleanDefaultTrue",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, is_active BOOL)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, is_active BOOL DEFAULT TRUE)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `is_active` tinyint(1) NULL DEFAULT 1",
		},
		{
			name:     "AddBooleanColumnWithDefault",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, is_instant BOOL DEFAULT FALSE)",
			expected: "ALTER TABLE `t1` ADD COLUMN `is_instant` tinyint(1) NULL DEFAULT 0",
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
			expected: "ALTER TABLE `t1` MODIFY COLUMN `b` int NULL FIRST, MODIFY COLUMN `c` int NULL AFTER `b`, MODIFY COLUMN `a` int NULL AFTER `c`",
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
			expected: "ALTER TABLE `t1` PARTITION BY LIST COLUMNS (`region`) (PARTITION `pNorth` VALUES IN ('US', 'CA'), PARTITION `pSouth` VALUES IN ('MX', 'BR'))",
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
			name:     "ChangeForeignKeyAction",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)",
			expected: "ALTER TABLE `t1` DROP FOREIGN KEY `fk_user`, ADD CONSTRAINT `fk_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE",
		},
		{
			name:     "AddOnDeleteToExistingForeignKey",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)",
			expected: "ALTER TABLE `t1` DROP FOREIGN KEY `fk_user`, ADD CONSTRAINT `fk_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE",
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
			expected: "ALTER TABLE `t1` DROP FOREIGN KEY `fk_user`, ADD CONSTRAINT `fk_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE RESTRICT",
		},
		{
			name:     "ForeignKeyCascadeToNoAction",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE NO ACTION)",
			expected: "ALTER TABLE `t1` DROP FOREIGN KEY `fk_user`, ADD CONSTRAINT `fk_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)",
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
			expected: "ALTER TABLE `t1` MODIFY COLUMN `B` int NOT NULL AFTER `id`, MODIFY COLUMN `A` int NOT NULL AFTER `B`",
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
