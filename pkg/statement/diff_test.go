package statement

import (
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
			expected: "ALTER TABLE `t1` ADD COLUMN `b` int(11) NULL",
		},
		{
			name:     "AddColumnInMiddle",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, c INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, b INT, c INT)",
			expected: "ALTER TABLE `t1` ADD COLUMN `b` int(11) NULL AFTER `id`",
		},
		{
			name:     "AddColumnAtEnd",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT, c INT)",
			expected: "ALTER TABLE `t1` ADD COLUMN `c` int(11) NULL",
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
			expected: "ALTER TABLE `t1` MODIFY COLUMN `c` int(11) NULL FIRST, MODIFY COLUMN `a` int(11) NULL AFTER `c`, MODIFY COLUMN `b` int(11) NULL AFTER `a`",
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
			expected: "ALTER TABLE `t1` MODIFY COLUMN `count` int(11) unsigned NULL",
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
			expected: "ALTER TABLE `t1` MODIFY COLUMN `id` int(11) NOT NULL COMMENT 'Line1\\nLine2\\rLine3'",
		},
		{
			name:     "ColumnCommentRogueValues2",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			target:   `CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(50) DEFAULT "O'Brien")`,
			expected: "ALTER TABLE `t1` ADD COLUMN `name` varchar(50) NULL DEFAULT 'O\\'\\'Brien'",
		},

		{
			name:     "AutoIncrement",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY AUTO_INCREMENT)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `id` int(11) NOT NULL AUTO_INCREMENT",
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
			expected: "ALTER TABLE `t1` DROP COLUMN `c`, MODIFY COLUMN `b` varchar(100) NULL, ADD COLUMN `d` int(11) NULL, ADD INDEX `idx_d` (`d`)",
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
			expected: "ALTER TABLE `t1` ADD COLUMN `full_name` varchar(101) NULL",
		},
		{
			name:     "StoredColumns",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, price DECIMAL(10,2), tax_rate DECIMAL(5,4))",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, price DECIMAL(10,2), tax_rate DECIMAL(5,4), total DECIMAL(10,2) AS (price * (1 + tax_rate)) STORED)",
			expected: "ALTER TABLE `t1` ADD COLUMN `total` decimal(10,2) NULL",
		},
		{
			name:     "Timestamps1",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, updated_at TIMESTAMP)",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `updated_at` timestamp NULL DEFAULT current_timestamp",
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
			expected: "ALTER TABLE `t1` DEFAULT CHARSET=latin1",
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
			name:     "CompositePrimaryKey",
			source:   "CREATE TABLE t1 (a INT, b INT)",
			target:   "CREATE TABLE t1 (a INT, b INT, PRIMARY KEY (a, b))",
			expected: "ALTER TABLE `t1` ADD PRIMARY KEY (`a`, `b`)",
		},
		{
			name:     "DropPrimaryKey",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY)",
			target:   "CREATE TABLE t1 (id INT)",
			expected: "ALTER TABLE `t1` DROP PRIMARY KEY",
		},
		{
			name:     "DropPrimaryKeyCanonicalForm",
			source:   "CREATE TABLE `t1` (`id` int NOT NULL,  PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
			target:   "CREATE TABLE t1 (id INT NOT NULL)",
			expected: "ALTER TABLE `t1` DROP PRIMARY KEY",
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
			expected: "ALTER TABLE `t1` MODIFY COLUMN `name` varchar(100) CHARACTER SET latin1 NULL",
		},
		{
			name:     "ColumnCollation",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100)) CHARSET utf8mb4",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100) COLLATE utf8mb4_bin) CHARSET utf8mb4",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `name` varchar(100) COLLATE utf8mb4_bin NULL",
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
			expected: "ALTER TABLE `t1` ADD COLUMN `new_col` int(11) NULL FIRST",
		},
		{
			name:     "ChangeColumnOrder",
			source:   "CREATE TABLE t1 (a INT, b INT, c INT)",
			target:   "CREATE TABLE t1 (b INT, c INT, a INT)",
			expected: "ALTER TABLE `t1` MODIFY COLUMN `b` int(11) NULL FIRST, MODIFY COLUMN `c` int(11) NULL AFTER `b`, MODIFY COLUMN `a` int(11) NULL AFTER `c`",
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
			expected: "ALTER TABLE `t1` MODIFY COLUMN `a` int(11) NOT NULL AUTO_INCREMENT, ADD PRIMARY KEY (`a`, `b`)",
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ct1, err := ParseCreateTable(tt.source)
			require.NoError(t, err)

			ct2, err := ParseCreateTable(tt.target)
			require.NoError(t, err)

			stmts, err := ct1.Diff(ct2, nil)
			require.NoError(t, err)

			if tt.expected == "" {
				assert.Nil(t, stmts, "expected nil for identical tables")
			} else {
				require.Len(t, stmts, 1)
				assert.Equal(t, tt.expected, stmts[0].Statement)
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
	assert.Error(t, err, "expected error when diffing tables with different names")
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
			expected: "ALTER TABLE `t1` DEFAULT CHARSET=latin1",
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

		// IgnoreCharsetCollation
		{
			name:     "IgnoreCharsetCollation_Charset",
			source:   "CREATE TABLE t1 (id INT PRIMARY KEY) CHARSET=utf8mb4",
			target:   "CREATE TABLE t1 (id INT PRIMARY KEY) CHARSET=latin1",
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
				assert.Nil(t, stmts, "expected nil diff")
			} else {
				require.Len(t, stmts, 1)
				assert.Equal(t, tt.expected, stmts[0].Statement)
			}
		})
	}
}

// TestDiff_ChangePartitionType tests the multi-statement case where changing
// partition type requires REMOVE PARTITIONING followed by a separate PARTITION BY.
func TestDiff_ChangePartitionType(t *testing.T) {
	ct1, err := ParseCreateTable("CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT) PARTITION BY HASH(user_id) PARTITIONS 4")
	require.NoError(t, err)

	ct2, err := ParseCreateTable("CREATE TABLE t1 (id INT PRIMARY KEY, user_id INT) PARTITION BY KEY(id) PARTITIONS 4")
	require.NoError(t, err)

	stmts, err := ct1.Diff(ct2, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 2, "changing partition type should produce two statements")
	assert.Equal(t, "ALTER TABLE `t1` REMOVE PARTITIONING", stmts[0].Statement)
	assert.Equal(t, "ALTER TABLE `t1` PARTITION BY KEY (`id`) PARTITIONS 4", stmts[1].Statement)
}

func TestNewDiffOptions(t *testing.T) {
	opts := NewDiffOptions()
	assert.True(t, opts.IgnoreAutoIncrement, "IgnoreAutoIncrement should default to true")
	assert.True(t, opts.IgnoreEngine, "IgnoreEngine should default to true")
	assert.False(t, opts.IgnoreCharsetCollation, "IgnoreCharsetCollation should default to false")
	assert.False(t, opts.IgnorePartitioning, "IgnorePartitioning should default to false")
	assert.True(t, opts.IgnoreRowFormat, "IgnoreRowFormat should default to true")
}

// TestHelperFunctions tests the helper functions used in diff.go
func TestHelperFunctions(t *testing.T) {
	t.Run("stringPtrEqual", func(t *testing.T) {
		str1 := "test"
		str2 := "test"
		str3 := "different"

		assert.True(t, stringPtrEqual(nil, nil))
		assert.True(t, stringPtrEqual(&str1, &str2))
		assert.False(t, stringPtrEqual(&str1, nil))
		assert.False(t, stringPtrEqual(nil, &str1))
		assert.False(t, stringPtrEqual(&str1, &str3))
	})

	t.Run("intPtrEqual", func(t *testing.T) {
		int1 := 10
		int2 := 10
		int3 := 20

		assert.True(t, intPtrEqual(nil, nil))
		assert.True(t, intPtrEqual(&int1, &int2))
		assert.False(t, intPtrEqual(&int1, nil))
		assert.False(t, intPtrEqual(nil, &int1))
		assert.False(t, intPtrEqual(&int1, &int3))
	})

	t.Run("boolPtrEqual", func(t *testing.T) {
		bool1 := true
		bool2 := true
		bool3 := false

		assert.True(t, boolPtrEqual(nil, nil))
		assert.True(t, boolPtrEqual(&bool1, &bool2))
		assert.False(t, boolPtrEqual(&bool1, nil))
		assert.False(t, boolPtrEqual(nil, &bool1))
		assert.False(t, boolPtrEqual(&bool1, &bool3))
	})

	t.Run("needsQuotes", func(t *testing.T) {
		// Functions and keywords should not be quoted
		assert.False(t, needsQuotes("NULL"))
		assert.False(t, needsQuotes("null"))
		assert.False(t, needsQuotes("CURRENT_TIMESTAMP"))
		assert.False(t, needsQuotes("current_timestamp"))
		assert.False(t, needsQuotes("NOW()"))
		assert.False(t, needsQuotes("now()"))
		assert.False(t, needsQuotes("CURRENT_TIMESTAMP(6)"))

		// Numeric values should not be quoted
		assert.False(t, needsQuotes("0"))
		assert.False(t, needsQuotes("123"))
		assert.False(t, needsQuotes("-456"))
		assert.False(t, needsQuotes("3.14"))
		assert.False(t, needsQuotes("-2.5"))

		// String values should be quoted
		assert.True(t, needsQuotes("active"))
		assert.True(t, needsQuotes("hello world"))
		assert.True(t, needsQuotes("2023-01-01 00:00:00"))
		assert.True(t, needsQuotes(""))
		assert.True(t, needsQuotes("O'Brien"))
	})

	t.Run("getPreviousColumn", func(t *testing.T) {
		columns := []Column{
			{Name: "id"},
			{Name: "name"},
			{Name: "email"},
			{Name: "created_at"},
		}

		assert.Equal(t, "", getPreviousColumn(columns, "id"))
		assert.Equal(t, "id", getPreviousColumn(columns, "name"))
		assert.Equal(t, "name", getPreviousColumn(columns, "email"))
		assert.Equal(t, "email", getPreviousColumn(columns, "created_at"))
		assert.Equal(t, "", getPreviousColumn(columns, "nonexistent"))
	})

	t.Run("getPrimaryKeyIndex", func(t *testing.T) {
		// Table with no primary key
		ct1, err := ParseCreateTable("CREATE TABLE t1 (id INT, name VARCHAR(100))")
		require.NoError(t, err)
		assert.Nil(t, ct1.getPrimaryKeyIndex())

		// Table with inline primary key (column-level)
		ct2, err := ParseCreateTable("CREATE TABLE t2 (id INT PRIMARY KEY)")
		require.NoError(t, err)
		assert.Nil(t, ct2.getPrimaryKeyIndex()) // inline PK is not in Indexes

		// Table with table-level primary key
		ct3, err := ParseCreateTable("CREATE TABLE t3 (id INT, PRIMARY KEY (id))")
		require.NoError(t, err)
		pk := ct3.getPrimaryKeyIndex()
		require.NotNil(t, pk)
		assert.Equal(t, "PRIMARY KEY", pk.Type)
		assert.Equal(t, []string{"id"}, pk.Columns)

		// Table with composite primary key
		ct4, err := ParseCreateTable("CREATE TABLE t4 (a INT, b INT, PRIMARY KEY (a, b))")
		require.NoError(t, err)
		pk = ct4.getPrimaryKeyIndex()
		require.NotNil(t, pk)
		assert.Equal(t, "PRIMARY KEY", pk.Type)
		assert.Equal(t, []string{"a", "b"}, pk.Columns)
	})
}
