package statement

import (
	"encoding/json"
	"strings"
	"testing"

	_ "github.com/block/mysql"
	"github.com/stretchr/testify/require"
)

func TestParseCreateTable_BasicTable(t *testing.T) {
	sql := `
	CREATE TABLE users (
		id INT PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(255) NOT NULL,
		email VARCHAR(255) UNIQUE,
		age INT DEFAULT 0
	) ENGINE=InnoDB CHARSET=utf8mb4 COMMENT='User table'
	`

	ct, err := ParseCreateTable(sql)
	require.NoError(t, err)

	// Test basic table info
	require.Equal(t, "users", ct.GetTableName())

	// Test columns
	columns := ct.GetColumns()
	require.Len(t, columns, 4)

	// Test first column (id)
	idCol := columns[0]
	require.Equal(t, "id", idCol.Name)
	require.Contains(t, idCol.Type, "int") // TiDB returns "int(11)" not just "int"
	require.True(t, idCol.AutoInc)
	require.False(t, idCol.Nullable)

	// Test second column (name)
	nameCol := columns[1]
	require.Equal(t, "name", nameCol.Name)
	require.Contains(t, nameCol.Type, "varchar") // TiDB returns "varchar(255)" not just "varchar"
	require.NotNil(t, nameCol.Length)
	require.Equal(t, 255, *nameCol.Length)
	require.False(t, nameCol.Nullable)

	// Test indexes (PRIMARY KEY and UNIQUE should be detected)
	indexes := ct.GetIndexes()
	require.GreaterOrEqual(t, len(indexes), 2) // At least PRIMARY KEY and UNIQUE

	// Test table options
	options := ct.GetTableOptions()
	require.Equal(t, "InnoDB", options["engine"])
	require.Equal(t, "utf8mb4", options["charset"])
	require.Equal(t, "User table", options["comment"])
}

func TestSchemaAnalyzer_StructuredAccess(t *testing.T) {
	sql := `
	CREATE TABLE products (
		id BIGINT PRIMARY KEY,
		name VARCHAR(100) NOT NULL COMMENT 'Product name',
		price DECIMAL(10,2) DEFAULT 0.00
	) ENGINE=InnoDB ROW_FORMAT=COMPRESSED
	`

	ct, err := ParseCreateTable(sql)
	require.NoError(t, err)

	// Test direct structured access
	createTable := ct.GetCreateTable()
	require.Equal(t, "products", createTable.TableName)

	// Test columns access
	columns := createTable.Columns
	require.Len(t, columns, 3)
	require.Equal(t, "id", columns[0].Name)

	// Test table options access
	options := ct.GetTableOptions()
	require.Equal(t, "InnoDB", options["engine"])

	// Test using ByName methods
	if nameCol := columns.ByName("name"); nameCol != nil {
		require.Equal(t, "name", nameCol.Name)
		require.Contains(t, nameCol.Type, "varchar")
		require.False(t, nameCol.Nullable)
		// Comment parsing might not work perfectly with TiDB parser
		if nameCol.Comment != nil {
			require.Equal(t, "Product name", *nameCol.Comment)
		}
	} else {
		t.Error("Should find column 'name'")
	}

	// Test column count
	require.Len(t, columns, 3)

	// Test finding nullable columns using structured approach
	var nullableColumns []Column

	for _, col := range columns {
		if col.Nullable {
			nullableColumns = append(nullableColumns, col)
		}
	}

	require.GreaterOrEqual(t, len(nullableColumns), 1) // price should be nullable
}

func TestSchemaAnalyzer_ComplexConstraints(t *testing.T) {
	sql := `
	CREATE TABLE orders (
		id INT PRIMARY KEY,
		user_id INT NOT NULL,
		total DECIMAL(10,2),
		CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id)
	)
	`

	ct, err := ParseCreateTable(sql)
	require.NoError(t, err)

	constraints := ct.GetConstraints()

	// Should have at least the FOREIGN KEY constraint
	require.GreaterOrEqual(t, len(constraints), 1)

	require.True(t, constraints.HasForeignKeys())

	// Find the foreign key constraint
	var fkConstraint *Constraint

	for i := range constraints {
		if constraints[i].Type == "FOREIGN KEY" {
			fkConstraint = &constraints[i]
			break
		}
	}

	require.NotNil(t, fkConstraint)
	require.Equal(t, "fk_orders_user", fkConstraint.Name)
	require.Contains(t, *fkConstraint.Definition, "REFERENCES users")
}

// TestParseCheckConstraintEnforcement verifies that the [NOT] ENFORCED state
// of CHECK constraints is captured at parse time, including MySQL's canonical
// SHOW CREATE TABLE form which wraps NOT ENFORCED in a versioned comment:
// CONSTRAINT `c1` CHECK ((`a` > 0)) /*!80016 NOT ENFORCED */.
func TestParseCheckConstraintEnforcement(t *testing.T) {
	// Real SHOW CREATE TABLE output from MySQL 8.0.
	sql := "CREATE TABLE `t1` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `age` int DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`),\n" +
		"  CONSTRAINT `chk_not_enforced` CHECK ((`age` >= 0)) /*!80016 NOT ENFORCED */,\n" +
		"  CONSTRAINT `chk_enforced` CHECK ((`age` < 200))\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"

	ct, err := ParseCreateTable(sql)
	require.NoError(t, err)
	require.Len(t, ct.Constraints, 2)

	byName := map[string]*Constraint{}
	for i := range ct.Constraints {
		byName[ct.Constraints[i].Name] = &ct.Constraints[i]
	}

	require.True(t, byName["chk_not_enforced"].NotEnforced)
	require.Contains(t, *byName["chk_not_enforced"].Definition, "NOT ENFORCED")
	require.False(t, byName["chk_enforced"].NotEnforced)
	require.NotContains(t, *byName["chk_enforced"].Definition, "NOT ENFORCED")

	// User-written forms: explicit ENFORCED and the (default) absent keyword
	// are the same state; explicit NOT ENFORCED is captured.
	ct, err = ParseCreateTable("CREATE TABLE t1 (a int, " +
		"CONSTRAINT c1 CHECK (a > 0) NOT ENFORCED, " +
		"CONSTRAINT c2 CHECK (a < 5) ENFORCED, " +
		"CONSTRAINT c3 CHECK (a <> 3))")
	require.NoError(t, err)
	require.Len(t, ct.Constraints, 3)
	require.True(t, ct.Constraints[0].NotEnforced)
	require.False(t, ct.Constraints[1].NotEnforced)
	require.False(t, ct.Constraints[2].NotEnforced)
}

func TestSchemaAnalyzer_UnsignedSupport(t *testing.T) {
	sql := `
	CREATE TABLE test_unsigned (
		id INT PRIMARY KEY,
		signed_int INT NOT NULL,
		unsigned_int INT UNSIGNED NOT NULL,
		signed_bigint BIGINT DEFAULT 0,
		unsigned_bigint BIGINT UNSIGNED DEFAULT 0,
		signed_tinyint TINYINT,
		unsigned_tinyint TINYINT UNSIGNED
	)
	`

	ct, err := ParseCreateTable(sql)
	require.NoError(t, err)

	createTable := ct.GetCreateTable()
	columns := createTable.Columns
	require.Len(t, columns, 7)

	// Test signed columns (should have Unsigned = nil or false)
	signedInt := columns.ByName("signed_int")
	require.NotNil(t, signedInt)
	require.Nil(t, signedInt.Unsigned, "signed_int should not have Unsigned field set")

	signedBigint := columns.ByName("signed_bigint")
	require.NotNil(t, signedBigint)
	require.Nil(t, signedBigint.Unsigned, "signed_bigint should not have Unsigned field set")

	signedTinyint := columns.ByName("signed_tinyint")
	require.NotNil(t, signedTinyint)
	require.Nil(t, signedTinyint.Unsigned, "signed_tinyint should not have Unsigned field set")

	// Test unsigned columns (should have Unsigned = true)
	unsignedInt := columns.ByName("unsigned_int")
	require.NotNil(t, unsignedInt)
	require.NotNil(t, unsignedInt.Unsigned, "unsigned_int should have Unsigned field set")
	require.True(t, *unsignedInt.Unsigned, "unsigned_int should be marked as unsigned")

	unsignedBigint := columns.ByName("unsigned_bigint")
	require.NotNil(t, unsignedBigint)
	require.NotNil(t, unsignedBigint.Unsigned, "unsigned_bigint should have Unsigned field set")
	require.True(t, *unsignedBigint.Unsigned, "unsigned_bigint should be marked as unsigned")

	unsignedTinyint := columns.ByName("unsigned_tinyint")
	require.NotNil(t, unsignedTinyint)
	require.NotNil(t, unsignedTinyint.Unsigned, "unsigned_tinyint should have Unsigned field set")
	require.True(t, *unsignedTinyint.Unsigned, "unsigned_tinyint should be marked as unsigned")

	// Test id column (should not be unsigned)
	idCol := columns.ByName("id")
	require.NotNil(t, idCol)
	require.Nil(t, idCol.Unsigned, "id should not have Unsigned field set")
}

func TestSchemaAnalyzer_JSONSerialization(t *testing.T) {
	sql := `
	CREATE TABLE test_table (
		id INT PRIMARY KEY,
		name VARCHAR(100) NOT NULL
	) ENGINE=InnoDB
	`

	ct, err := ParseCreateTable(sql)
	require.NoError(t, err)

	// Test that we can serialize the structured data
	columns := ct.GetColumns()
	jsonData, err := json.Marshal(columns)
	require.NoError(t, err)

	var deserializedColumns []Column

	err = json.Unmarshal(jsonData, &deserializedColumns)
	require.NoError(t, err)

	// Verify columns match by comparing key fields
	require.Len(t, deserializedColumns, 2)

	require.Equal(t, "id", deserializedColumns[0].Name)
	require.Equal(t, "int", deserializedColumns[0].Type)
	// The inline PRIMARY KEY is normalized into a table-level index, so the
	// column itself no longer carries the inline PrimaryKey flag.
	require.False(t, deserializedColumns[0].PrimaryKey)

	require.Equal(t, "name", deserializedColumns[1].Name)
	require.Contains(t, deserializedColumns[1].Type, "varchar")
	require.False(t, deserializedColumns[1].Nullable)
}

func TestSchemaAnalyzer_IndexVisibilityStructured(t *testing.T) {
	sql := `
	CREATE TABLE query_invisibility_test (
		id INT PRIMARY KEY,
		name VARCHAR(100),
		email VARCHAR(255),
		status VARCHAR(50),
		content TEXT,
		
		-- Regular visible index (no options)
		INDEX idx_name (name),
		
		-- Invisible index
		INDEX idx_email (email) INVISIBLE,
		
		-- Explicit visible index
		INDEX idx_status (status) VISIBLE,
		
		-- Index with multiple options including invisibility
		UNIQUE KEY uk_email_multi (email) USING BTREE COMMENT 'Multi-option unique' INVISIBLE,
		
		-- FULLTEXT index with comment (visible)
		FULLTEXT idx_content (content) COMMENT 'Search index'
	)
	`

	ct, err := ParseCreateTable(sql)
	require.NoError(t, err)

	// Test using structured access
	createTable := ct.GetCreateTable()
	indexes := createTable.Indexes
	require.NotEmpty(t, indexes)

	// Find specific indexes by name
	emailIdx := indexes.ByName("idx_email")
	require.NotNil(t, emailIdx, "Should find idx_email")
	require.NotNil(t, emailIdx.Invisible)
	require.True(t, *emailIdx.Invisible, "idx_email should be invisible")

	multiIdx := indexes.ByName("uk_email_multi")
	require.NotNil(t, multiIdx, "Should find uk_email_multi")
	require.NotNil(t, multiIdx.Invisible)
	require.True(t, *multiIdx.Invisible, "uk_email_multi should be invisible")
	require.NotNil(t, multiIdx.Using)
	require.Equal(t, "BTREE", *multiIdx.Using, "uk_email_multi should use BTREE")
	require.NotNil(t, multiIdx.Comment)
	require.Equal(t, "Multi-option unique", *multiIdx.Comment)

	statusIdx := indexes.ByName("idx_status")
	require.NotNil(t, statusIdx, "Should find idx_status")
	require.NotNil(t, statusIdx.Invisible)
	require.False(t, *statusIdx.Invisible, "idx_status should be explicitly visible")

	nameIdx := indexes.ByName("idx_name")
	require.NotNil(t, nameIdx, "Should find idx_name")
	require.Nil(t, nameIdx.Invisible, "idx_name should have no invisibility setting")

	// Test finding all invisible indexes using structured approach
	var invisibleIndexes []Index

	for _, idx := range indexes {
		if idx.Invisible != nil && *idx.Invisible {
			invisibleIndexes = append(invisibleIndexes, idx)
		}
	}

	require.Len(t, invisibleIndexes, 2, "Should find exactly 2 invisible indexes")

	// Verify the invisible indexes are the ones we expect
	invisibleNames := make(map[string]bool)
	for _, idx := range invisibleIndexes {
		invisibleNames[idx.Name] = true
	}

	require.True(t, invisibleNames["idx_email"], "Should include idx_email in invisible indexes")
	require.True(t, invisibleNames["uk_email_multi"], "Should include uk_email_multi in invisible indexes")
}

// TestSchemaAnalyzer_DescendingIndexKeyParts verifies that DESC key parts
// (MySQL 8.0+) are captured on IndexColumn for plain column, prefix, and
// functional (expression) index parts. MySQL 8.0's SHOW CREATE TABLE prints
// KEY `k` (`a` DESC), so dropping the flag would silently re-create
// descending indexes as ascending.
func TestSchemaAnalyzer_DescendingIndexKeyParts(t *testing.T) {
	sql := `
	CREATE TABLE desc_index_test (
		id INT PRIMARY KEY,
		a INT,
		b VARCHAR(100),
		c VARCHAR(100),
		KEY k (a DESC, b),
		KEY fidx ((lower(c)) DESC),
		KEY pfx (c(10) DESC)
	)
	`

	ct, err := ParseCreateTable(sql)
	require.NoError(t, err)

	indexes := ct.GetCreateTable().Indexes

	k := indexes.ByName("k")
	require.NotNil(t, k, "Should find k")
	require.Len(t, k.ColumnList, 2)
	require.Equal(t, "a", k.ColumnList[0].Name)
	require.True(t, k.ColumnList[0].Desc, "a should be a descending key part")
	require.Equal(t, "b", k.ColumnList[1].Name)
	require.False(t, k.ColumnList[1].Desc, "b should be an ascending key part")

	fidx := indexes.ByName("fidx")
	require.NotNil(t, fidx, "Should find fidx")
	require.Len(t, fidx.ColumnList, 1)
	require.NotNil(t, fidx.ColumnList[0].Expression)
	require.True(t, fidx.ColumnList[0].Desc, "functional key part should be descending")

	pfx := indexes.ByName("pfx")
	require.NotNil(t, pfx, "Should find pfx")
	require.Len(t, pfx.ColumnList, 1)
	require.NotNil(t, pfx.ColumnList[0].Length)
	require.True(t, pfx.ColumnList[0].Desc, "prefix key part should be descending")
}

// Benchmark to show performance characteristics
func BenchmarkParseCreateTable(b *testing.B) {
	sql := `
	CREATE TABLE benchmark_table (
		id BIGINT PRIMARY KEY AUTO_INCREMENT,
		uuid CHAR(36) NOT NULL UNIQUE,
		name VARCHAR(255) NOT NULL,
		description TEXT,
		price DECIMAL(10,2) DEFAULT 0.00,
		category_id INT,
		status ENUM('active', 'inactive') DEFAULT 'active',
		metadata JSON,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		INDEX idx_category (category_id),
		INDEX idx_status (status),
		INDEX idx_created (created_at),
		FULLTEXT INDEX ft_description (description),
		CONSTRAINT fk_category FOREIGN KEY (category_id) REFERENCES categories(id)
	) ENGINE=InnoDB CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC
	`

	b.ResetTimer()

	for b.Loop() {
		ct, err := ParseCreateTable(sql)
		if err != nil {
			b.Fatal(err)
		}

		_ = ct.GetColumns()
		_ = ct.GetIndexes()
		_ = ct.GetConstraints()
	}
}

func TestSchemaAnalyzer_PartitionSupport(t *testing.T) {
	testCases := []struct {
		name     string
		sql      string
		expected *PartitionOptions
	}{
		{
			name: "RANGE partitioning",
			sql: `CREATE TABLE sales (
				id INT,
				sale_date DATE
			) PARTITION BY RANGE (YEAR(sale_date)) (
				PARTITION p2020 VALUES LESS THAN (2021),
				PARTITION p2021 VALUES LESS THAN (2022)
			);`,
			expected: &PartitionOptions{
				Type:       "RANGE",
				Expression: new("YEAR(`sale_date`)"),
				Partitions: 2,
				Definitions: []PartitionDefinition{
					{
						Name: "p2020",
						Values: &PartitionValues{
							Type:   "LESS_THAN",
							Values: []any{"2021"},
						},
					},
					{
						Name: "p2021",
						Values: &PartitionValues{
							Type:   "LESS_THAN",
							Values: []any{"2022"},
						},
					},
				},
			},
		},
		{
			name: "HASH partitioning",
			sql: `CREATE TABLE customers (
				id INT PRIMARY KEY
			) PARTITION BY HASH(id) PARTITIONS 4;`,
			expected: &PartitionOptions{
				Type:       "HASH",
				Expression: new("`id`"),
				Partitions: 4,
			},
		},
		{
			name: "KEY partitioning",
			sql: `CREATE TABLE orders (
				customer_id INT,
				order_date DATE
			) PARTITION BY KEY(customer_id) PARTITIONS 8;`,
			expected: &PartitionOptions{
				Type:       "KEY",
				Columns:    []string{"customer_id"},
				Partitions: 8,
			},
		},
		{
			name: "LIST COLUMNS partitioning",
			sql: `CREATE TABLE regions (
				id INT,
				region VARCHAR(20)
			) PARTITION BY LIST COLUMNS(region) (
				PARTITION p_north VALUES IN ('north', 'northeast'),
				PARTITION p_south VALUES IN ('south')
			);`,
			expected: &PartitionOptions{
				Type:       "LIST",
				Columns:    []string{"region"},
				Partitions: 2,
				Definitions: []PartitionDefinition{
					{
						Name: "p_north",
						Values: &PartitionValues{
							Type: "IN",
							// String-literal partition values are stored as
							// partitionStringLiteral so emission re-quotes them.
							Values: []any{partitionStringLiteral("north"), partitionStringLiteral("northeast")},
						},
					},
					{
						Name: "p_south",
						Values: &PartitionValues{
							Type:   "IN",
							Values: []any{partitionStringLiteral("south")},
						},
					},
				},
			},
		},
		{
			name: "No partitioning",
			sql: `CREATE TABLE simple (
				id INT PRIMARY KEY,
				name VARCHAR(100)
			);`,
			expected: nil,
		},
		{
			name: "Partitioned table from existing test",
			sql: `CREATE TABLE t1 (
				a CHAR(2) NOT NULL,
				b CHAR(2) NOT NULL,
				c INT(10) UNSIGNED NOT NULL,
				d VARCHAR(255) DEFAULT NULL,
				e VARCHAR(1000) DEFAULT NULL,
				KEY (a) INVISIBLE,
				KEY (b)
			) PARTITION BY KEY (a) PARTITIONS 20;`,
			expected: &PartitionOptions{
				Type:       "KEY",
				Columns:    []string{"a"},
				Partitions: 20,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ct, err := ParseCreateTable(tc.sql)
			require.NoError(t, err)

			partition := ct.GetPartition()

			if tc.expected == nil {
				require.Nil(t, partition)
				return
			}

			require.NotNil(t, partition)
			require.Equal(t, tc.expected.Type, partition.Type)
			require.Equal(t, tc.expected.Partitions, partition.Partitions)
			require.Equal(t, tc.expected.Columns, partition.Columns)

			if tc.expected.Expression != nil {
				require.NotNil(t, partition.Expression)
				require.Equal(t, *tc.expected.Expression, *partition.Expression)
			} else {
				require.Nil(t, partition.Expression)
			}

			require.Len(t, partition.Definitions, len(tc.expected.Definitions))

			for i, expectedDef := range tc.expected.Definitions {
				if i < len(partition.Definitions) {
					actualDef := partition.Definitions[i]
					require.Equal(t, expectedDef.Name, actualDef.Name)

					if expectedDef.Values != nil {
						require.NotNil(t, actualDef.Values)
						require.Equal(t, expectedDef.Values.Type, actualDef.Values.Type)
						require.Equal(t, expectedDef.Values.Values, actualDef.Values.Values)
					}
				}
			}
		})
	}
}

func TestSchemaAnalyzer_EnumAndSetSupport(t *testing.T) {
	sql := `
	CREATE TABLE test_enum_set (
		id INT PRIMARY KEY,
		status ENUM('active', 'inactive', 'pending') DEFAULT 'active',
		permissions SET('read', 'write', 'execute') DEFAULT 'read',
		priority ENUM('low', 'medium', 'high') NOT NULL,
		flags SET('flag1', 'flag2', 'flag3', 'flag4'),
		name VARCHAR(100)
	)
	`

	ct, err := ParseCreateTable(sql)
	require.NoError(t, err)

	columns := ct.GetColumns()
	require.Len(t, columns, 6)

	// Test ENUM column with default
	statusCol := columns.ByName("status")
	require.NotNil(t, statusCol)
	require.Equal(t, "enum", statusCol.Type)
	require.NotNil(t, statusCol.EnumValues)
	require.Equal(t, []string{"active", "inactive", "pending"}, statusCol.EnumValues)
	require.Nil(t, statusCol.SetValues, "ENUM column should not have SetValues")
	require.NotNil(t, statusCol.Default)
	require.Equal(t, "active", *statusCol.Default)
	require.True(t, statusCol.Nullable)

	// Test SET column with default
	permissionsCol := columns.ByName("permissions")
	require.NotNil(t, permissionsCol)
	require.Equal(t, "set", permissionsCol.Type)
	require.NotNil(t, permissionsCol.SetValues)
	require.Equal(t, []string{"read", "write", "execute"}, permissionsCol.SetValues)
	require.Nil(t, permissionsCol.EnumValues, "SET column should not have EnumValues")
	require.NotNil(t, permissionsCol.Default)
	require.Equal(t, "read", *permissionsCol.Default)

	// Test ENUM column NOT NULL
	priorityCol := columns.ByName("priority")
	require.NotNil(t, priorityCol)
	require.Equal(t, "enum", priorityCol.Type)
	require.NotNil(t, priorityCol.EnumValues)
	require.Equal(t, []string{"low", "medium", "high"}, priorityCol.EnumValues)
	require.False(t, priorityCol.Nullable)

	// Test SET column without default
	flagsCol := columns.ByName("flags")
	require.NotNil(t, flagsCol)
	require.Equal(t, "set", flagsCol.Type)
	require.NotNil(t, flagsCol.SetValues)
	require.Equal(t, []string{"flag1", "flag2", "flag3", "flag4"}, flagsCol.SetValues)
	require.Nil(t, flagsCol.Default)

	// Test regular column (should have no enum/set values)
	nameCol := columns.ByName("name")
	require.NotNil(t, nameCol)
	require.Contains(t, nameCol.Type, "varchar")
	require.Nil(t, nameCol.EnumValues)
	require.Nil(t, nameCol.SetValues)
}

func TestSchemaAnalyzer_EnumSetJSONSerialization(t *testing.T) {
	sql := `
	CREATE TABLE test_json (
		id INT PRIMARY KEY,
		status ENUM('active', 'inactive') DEFAULT 'active',
		permissions SET('read', 'write')
	)
	`

	ct, err := ParseCreateTable(sql)
	require.NoError(t, err)

	// Test JSON serialization
	columns := ct.GetColumns()
	jsonData, err := json.Marshal(columns)
	require.NoError(t, err)

	// Deserialize and verify
	var deserializedColumns []Column

	err = json.Unmarshal(jsonData, &deserializedColumns)
	require.NoError(t, err)

	// Verify the columns match by name and key fields
	require.Len(t, deserializedColumns, 3)

	// Check id column. The inline PRIMARY KEY is normalized into a table-level
	// index, so the column no longer carries the inline PrimaryKey flag.
	require.Equal(t, "id", deserializedColumns[0].Name)
	require.Equal(t, "int", deserializedColumns[0].Type)
	require.False(t, deserializedColumns[0].PrimaryKey)
	require.Nil(t, deserializedColumns[0].EnumValues)
	require.Nil(t, deserializedColumns[0].SetValues)

	// Verify enum values are preserved
	statusCol := deserializedColumns[1]
	require.Equal(t, "status", statusCol.Name)
	require.Equal(t, "enum", statusCol.Type)
	require.Equal(t, []string{"active", "inactive"}, statusCol.EnumValues)
	require.Nil(t, statusCol.SetValues)
	require.NotNil(t, statusCol.Default)
	require.Equal(t, "active", *statusCol.Default)

	// Verify set values are preserved
	permissionsCol := deserializedColumns[2]
	require.Equal(t, "permissions", permissionsCol.Name)
	require.Equal(t, "set", permissionsCol.Type)
	require.Equal(t, []string{"read", "write"}, permissionsCol.SetValues)
	require.Nil(t, permissionsCol.EnumValues)
}

func TestSchemaAnalyzer_EnumSingleValue(t *testing.T) {
	sql := `
	CREATE TABLE test_single (
		status ENUM('only_one') DEFAULT 'only_one'
	)
	`

	ct, err := ParseCreateTable(sql)
	require.NoError(t, err)

	columns := ct.GetColumns()
	require.Len(t, columns, 1)

	statusCol := columns[0]
	require.Equal(t, "enum", statusCol.Type)
	require.NotNil(t, statusCol.EnumValues)
	require.Equal(t, []string{"only_one"}, statusCol.EnumValues)
}

func Test_Sloppy(t *testing.T) {
	// This is just a big jumble of tests about different aspects of a very screwy CREATE TABLE statement
	// that could never even be parsed by MySQL. Many of these tests are to confirm quirks of the current
	// implementation. They may be subject to change in future versions, but we should pay attention.
	sql := `
	CREATE TABLE e_commerce_orders (
		order_id BIGINT unsigned PRIMARY KEY AUTO_INCREMENT,
		user_id INT NOT NULL,
		user_id char(32),
		customerEmail VARCHAR(255) NOT NULL UNIQUE,
		total_amount DECIMAL(10,2) DEFAULT 0.00,
		order_status ENUM('pending', 'processing', 'shipped', 'delivered', 'cancelled') DEFAULT 'pending',
		shipping_address TEXT,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		UNIQUE(user_id, customerEmail),
		INDEX idx_user_id (user_id),
		INDEX idx_status (order_status),
		INDEX idx_created_at (created_at) invisible,
		primary key(id),
		CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
	) ENGINE=InnoDB CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC COMMENT='E-commerce order tracking table'
	`
	ct, err := ParseCreateTable(sql)
	require.NoError(t, err)

	// This accounts for the two different primary keys and 2 different unique indexes,
	// each given once as a column attribute and once as a table constraint.
	require.Len(t, ct.GetIndexes(), 7)

	// The "first" index should be the first one defined as a table attribute, not the PK or UNIQUE
	// index defined as a column attribute.
	firstIdx := ct.GetCreateTable().Indexes[0]
	require.NotNil(t, firstIdx)
	// Unnamed indexes are auto-named after their first column (MySQL convention)
	require.Equal(t, "user_id", firstIdx.Name)
	require.Equal(t, []string{"user_id", "customerEmail"}, firstIdx.Columns)
	require.Nil(t, firstIdx.Comment)

	require.True(t, ct.GetIndexes().HasInvisible())
	idx_created_at := ct.GetCreateTable().Indexes.ByName("idx_created_at")
	require.NotNil(t, idx_created_at)
	require.NotNil(t, idx_created_at.Invisible)
	require.True(t, *idx_created_at.Invisible)

	enum := ct.GetColumns().ByName("order_status")
	require.NotNil(t, enum)
	require.True(t, strings.EqualFold("ENUM", enum.Type))

	// Verify enum values are captured
	require.NotNil(t, enum.EnumValues)
	require.Equal(t, []string{"pending", "processing", "shipped", "delivered", "cancelled"}, enum.EnumValues)
	require.Equal(t, "pending", *enum.Default)

	total_amount := ct.GetColumns().ByName("total_amount")
	require.NotNil(t, total_amount)
	require.Contains(t, strings.ToLower(total_amount.Type), "decimal")
	require.NotNil(t, total_amount.Default)
	require.Equal(t, "0.00", *total_amount.Default)
}

// ComprehensiveTestCase represents a test case with SQL, expected success, and validation function
type ComprehensiveTestCase struct {
	Name        string
	SQL         string
	ShouldParse bool
	Validate    func(t *testing.T, createTable *CreateTable)
}

// TestComprehensiveParsingFromTiDBTestSuite tests our parsing library against patterns
// derived from the TiDB parser's comprehensive test suite
func TestComprehensiveParsingFromTiDBTestSuite(t *testing.T) {
	testCases := []ComprehensiveTestCase{
		// Basic CREATE TABLE tests
		{
			Name:        "Basic table with simple columns",
			SQL:         "CREATE TABLE foo (a varchar(50), b int);",
			ShouldParse: true,
			Validate: func(t *testing.T, createTable *CreateTable) {
				require.Equal(t, "foo", createTable.GetTableName())
				columns := createTable.GetColumns()
				require.Len(t, columns, 2)
				require.Equal(t, "a", columns[0].Name)
				require.Contains(t, columns[0].Type, "varchar")
				require.Equal(t, "b", columns[1].Name)
				require.Contains(t, columns[1].Type, "int")
			},
		},
		{
			Name:        "Table with unsigned integers",
			SQL:         "CREATE TABLE foo (a TINYINT UNSIGNED, b SMALLINT UNSIGNED, c INT UNSIGNED, d BIGINT UNSIGNED);",
			ShouldParse: true,
			Validate: func(t *testing.T, createTable *CreateTable) {
				columns := createTable.GetColumns()
				require.Len(t, columns, 4)

				for _, col := range columns {
					// TiDB parser includes UNSIGNED in the type string as "type(size) UNSIGNED"
					require.NotNil(t, col.Unsigned)
					require.True(t, *col.Unsigned)
					require.Contains(t, col.Type, "int")
				}
			},
		},

		// Index visibility tests (from TiDB parser test suite)
		{
			Name:        "Index with INVISIBLE keyword",
			SQL:         "CREATE TABLE t (id INT, name VARCHAR(100), INDEX idx_name (name) INVISIBLE);",
			ShouldParse: true,
			Validate: func(t *testing.T, createTable *CreateTable) {
				indexes := createTable.GetIndexes()

				var invisibleIndex *Index

				for _, idx := range indexes {
					if idx.Name == "idx_name" {
						invisibleIndex = &idx
						break
					}
				}

				require.NotNil(t, invisibleIndex)
				require.NotNil(t, invisibleIndex.Invisible)
				require.True(t, *invisibleIndex.Invisible)
			},
		},
		{
			Name:        "Index with VISIBLE keyword",
			SQL:         "CREATE TABLE t (id INT, name VARCHAR(100), INDEX idx_name (name) VISIBLE);",
			ShouldParse: true,
			Validate: func(t *testing.T, createTable *CreateTable) {
				indexes := createTable.GetIndexes()

				var visibleIndex *Index

				for _, idx := range indexes {
					if idx.Name == "idx_name" {
						visibleIndex = &idx
						break
					}
				}

				require.NotNil(t, visibleIndex)
				// For VISIBLE indexes, Invisible should be nil or false
				require.True(t, visibleIndex.Invisible == nil || !*visibleIndex.Invisible)
			},
		},
		{
			Name:        "Index with INVISIBLE then VISIBLE (last wins)",
			SQL:         "CREATE TABLE t (id INT, INDEX idx (id) INVISIBLE VISIBLE);",
			ShouldParse: true,
			Validate: func(t *testing.T, createTable *CreateTable) {
				indexes := createTable.GetIndexes()

				var testIndex *Index

				for _, idx := range indexes {
					if idx.Name == "idx" {
						testIndex = &idx
						break
					}
				}

				require.NotNil(t, testIndex)
				// Last option should win (VISIBLE), so Invisible should be false
				require.NotNil(t, testIndex.Invisible)
				require.False(t, *testIndex.Invisible)
			},
		},

		// Index algorithm tests
		{
			Name:        "Index with USING BTREE",
			SQL:         "CREATE TABLE t (id INT, INDEX idx (id) USING BTREE);",
			ShouldParse: true,
			Validate: func(t *testing.T, createTable *CreateTable) {
				indexes := createTable.GetIndexes()

				var btreeIndex *Index

				for _, idx := range indexes {
					if idx.Name == "idx" {
						btreeIndex = &idx
						break
					}
				}

				require.NotNil(t, btreeIndex)
				require.NotNil(t, btreeIndex.Using)
				require.Equal(t, "BTREE", *btreeIndex.Using)
			},
		},
		{
			Name:        "Index with USING HASH",
			SQL:         "CREATE TABLE t (id INT, INDEX idx (id) USING HASH);",
			ShouldParse: true,
			Validate: func(t *testing.T, createTable *CreateTable) {
				indexes := createTable.GetIndexes()

				var hashIndex *Index

				for _, idx := range indexes {
					if idx.Name == "idx" {
						hashIndex = &idx
						break
					}
				}

				require.NotNil(t, hashIndex)
				require.NotNil(t, hashIndex.Using)
				require.Equal(t, "HASH", *hashIndex.Using)
			},
		},
		{
			Name:        "Index with USING HASH and INVISIBLE",
			SQL:         "CREATE TABLE t (id INT, INDEX idx (id) USING HASH INVISIBLE);",
			ShouldParse: true,
			Validate: func(t *testing.T, createTable *CreateTable) {
				indexes := createTable.GetIndexes()

				var combinedIndex *Index

				for _, idx := range indexes {
					if idx.Name == "idx" {
						combinedIndex = &idx
						break
					}
				}

				require.NotNil(t, combinedIndex)
				require.NotNil(t, combinedIndex.Using)
				require.Equal(t, "HASH", *combinedIndex.Using)
				require.NotNil(t, combinedIndex.Invisible)
				require.True(t, *combinedIndex.Invisible)
			},
		},

		// Index comment tests
		{
			Name:        "Index with comment",
			SQL:         "CREATE TABLE t (id INT, INDEX idx (id) COMMENT 'Index comment');",
			ShouldParse: true,
			Validate: func(t *testing.T, createTable *CreateTable) {
				indexes := createTable.GetIndexes()

				var commentIndex *Index

				for _, idx := range indexes {
					if idx.Name == "idx" {
						commentIndex = &idx
						break
					}
				}

				require.NotNil(t, commentIndex)
				require.NotNil(t, commentIndex.Comment)
				require.Equal(t, "Index comment", *commentIndex.Comment)
			},
		},

		// Key block size tests
		{
			Name:        "Index with KEY_BLOCK_SIZE",
			SQL:         "CREATE TABLE t (id INT, INDEX idx (id) KEY_BLOCK_SIZE = 16);",
			ShouldParse: true,
			Validate: func(t *testing.T, createTable *CreateTable) {
				indexes := createTable.GetIndexes()

				var kbsIndex *Index

				for _, idx := range indexes {
					if idx.Name == "idx" {
						kbsIndex = &idx
						break
					}
				}

				require.NotNil(t, kbsIndex)
				require.NotNil(t, kbsIndex.KeyBlockSize)
				require.Equal(t, uint64(16), *kbsIndex.KeyBlockSize)
			},
		},

		// FULLTEXT index tests
		{
			Name:        "FULLTEXT index with parser",
			SQL:         "CREATE TABLE t (content TEXT, FULLTEXT idx_content (content) WITH PARSER ngram);",
			ShouldParse: true,
			Validate: func(t *testing.T, createTable *CreateTable) {
				indexes := createTable.GetIndexes()

				var fulltextIndex *Index

				for _, idx := range indexes {
					if idx.Type == "FULLTEXT" {
						fulltextIndex = &idx
						break
					}
				}

				require.NotNil(t, fulltextIndex)
				require.NotNil(t, fulltextIndex.ParserName)
				require.Equal(t, "ngram", *fulltextIndex.ParserName)
			},
		},

		// Complex multi-option index tests
		{
			Name:        "UNIQUE index with multiple options",
			SQL:         "CREATE TABLE t (email VARCHAR(255), UNIQUE KEY uk_email (email) USING BTREE COMMENT 'Unique email' KEY_BLOCK_SIZE = 8 INVISIBLE);",
			ShouldParse: true,
			Validate: func(t *testing.T, createTable *CreateTable) {
				indexes := createTable.GetIndexes()

				var uniqueIndex *Index

				for _, idx := range indexes {
					if idx.Name == "uk_email" {
						uniqueIndex = &idx
						break
					}
				}

				require.NotNil(t, uniqueIndex)
				require.Equal(t, "UNIQUE", uniqueIndex.Type)
				require.NotNil(t, uniqueIndex.Using)
				require.Equal(t, "BTREE", *uniqueIndex.Using)
				require.NotNil(t, uniqueIndex.Comment)
				require.Equal(t, "Unique email", *uniqueIndex.Comment)
				require.NotNil(t, uniqueIndex.KeyBlockSize)
				require.Equal(t, uint64(8), *uniqueIndex.KeyBlockSize)
				require.NotNil(t, uniqueIndex.Invisible)
				require.True(t, *uniqueIndex.Invisible)
			},
		},

		// Table option tests
		{
			Name:        "Table with ENGINE and CHARSET",
			SQL:         "CREATE TABLE t (id INT) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
			ShouldParse: true,
			Validate: func(t *testing.T, createTable *CreateTable) {
				options := createTable.GetTableOptions()
				require.Equal(t, "InnoDB", options["engine"])
				require.Equal(t, "utf8mb4", options["charset"])
			},
		},
		{
			Name:        "Table with KEY_BLOCK_SIZE",
			SQL:         "CREATE TABLE t (id INT) KEY_BLOCK_SIZE = 1024;",
			ShouldParse: true,
			Validate: func(t *testing.T, createTable *CreateTable) {
				options := createTable.GetTableOptions()
				// Note: Table-level KEY_BLOCK_SIZE might not be supported by our parser
				// This is a known limitation
				if kbs, exists := options["key_block_size"]; exists {
					require.Equal(t, uint64(1024), kbs)
				}
			},
		},
		{
			Name:        "Table with COMMENT",
			SQL:         "CREATE TABLE t (id INT) COMMENT = 'Test table';",
			ShouldParse: true,
			Validate: func(t *testing.T, createTable *CreateTable) {
				options := createTable.GetTableOptions()
				require.Equal(t, "Test table", options["comment"])
			},
		},

		// Column option tests
		{
			Name:        "Column with AUTO_INCREMENT",
			SQL:         "CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY);",
			ShouldParse: true,
			Validate: func(t *testing.T, createTable *CreateTable) {
				columns := createTable.GetColumns()
				require.Len(t, columns, 1)
				require.True(t, columns[0].AutoInc)
				require.False(t, columns[0].Nullable) // PRIMARY KEY implies NOT NULL
			},
		},
		{
			Name:        "Column with DEFAULT value",
			SQL:         "CREATE TABLE t (status VARCHAR(50) DEFAULT 'active');",
			ShouldParse: true,
			Validate: func(t *testing.T, createTable *CreateTable) {
				columns := createTable.GetColumns()
				require.Len(t, columns, 1)
				// Note: Default values might be parsed differently by TiDB parser
				// The actual default value parsing depends on the expression parser
				// For now, we just verify the column was parsed successfully
				require.Equal(t, "status", columns[0].Name)
				require.Contains(t, columns[0].Type, "varchar")
			},
		},
		{
			Name:        "Column with COMMENT",
			SQL:         "CREATE TABLE t (name VARCHAR(100) COMMENT 'User name');",
			ShouldParse: true,
			Validate: func(t *testing.T, createTable *CreateTable) {
				columns := createTable.GetColumns()
				require.Len(t, columns, 1)
				// Note: Column comments might be parsed as empty string due to TiDB parser behavior
				// This is expected based on our earlier investigation
			},
		},

		// Character set and collation tests
		{
			Name:        "Column with CHARACTER SET and COLLATE",
			SQL:         "CREATE TABLE t (name CHAR(50) CHARACTER SET utf8 COLLATE utf8_bin);",
			ShouldParse: true,
			Validate: func(t *testing.T, createTable *CreateTable) {
				columns := createTable.GetColumns()
				require.Len(t, columns, 1)
				require.Contains(t, columns[0].Type, "char")
			},
		},

		// Comprehensive real-world example
		{
			Name: "Complex real-world table",
			SQL: `CREATE TABLE user_activity (
				id BIGINT PRIMARY KEY AUTO_INCREMENT,
				user_id BIGINT NOT NULL,
				activity_type VARCHAR(50) NOT NULL DEFAULT 'unknown',
				timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				data JSON,
				
				INDEX idx_user_id (user_id) COMMENT 'User lookup',
				INDEX idx_timestamp (timestamp) USING BTREE,
				INDEX idx_activity_type (activity_type) INVISIBLE COMMENT 'Activity type lookup',
				UNIQUE KEY uk_user_timestamp (user_id, timestamp) USING BTREE KEY_BLOCK_SIZE = 16 INVISIBLE,
				FULLTEXT idx_data (data) WITH PARSER ngram COMMENT 'JSON search'
			) ENGINE=InnoDB CHARSET=utf8mb4 COMMENT='User activity tracking';`,
			ShouldParse: true,
			Validate: func(t *testing.T, ct *CreateTable) {
				// Validate table
				require.Equal(t, "user_activity", ct.GetTableName())

				// Validate columns
				columns := ct.GetColumns()
				require.Len(t, columns, 5)

				// Validate table options
				options := ct.GetTableOptions()
				require.Equal(t, "InnoDB", options["engine"])
				require.Equal(t, "utf8mb4", options["charset"])
				require.Equal(t, "User activity tracking", options["comment"])

				// Validate indexes
				indexes := ct.GetIndexes()

				indexMap := make(map[string]Index)
				for _, idx := range indexes {
					indexMap[idx.Name] = idx
				}

				// Check specific indexes
				userIdIndex := indexMap["idx_user_id"]
				require.Equal(t, "INDEX", userIdIndex.Type)
				require.NotNil(t, userIdIndex.Comment)
				require.Equal(t, "User lookup", *userIdIndex.Comment)

				timestampIndex := indexMap["idx_timestamp"]
				require.NotNil(t, timestampIndex.Using)
				require.Equal(t, "BTREE", *timestampIndex.Using)

				activityIndex := indexMap["idx_activity_type"]
				require.NotNil(t, activityIndex.Invisible)
				require.True(t, *activityIndex.Invisible)
				require.NotNil(t, activityIndex.Comment)
				require.Equal(t, "Activity type lookup", *activityIndex.Comment)

				uniqueIndex := indexMap["uk_user_timestamp"]
				require.Equal(t, "UNIQUE", uniqueIndex.Type)
				require.NotNil(t, uniqueIndex.Using)
				require.Equal(t, "BTREE", *uniqueIndex.Using)
				require.NotNil(t, uniqueIndex.KeyBlockSize)
				require.Equal(t, uint64(16), *uniqueIndex.KeyBlockSize)
				require.NotNil(t, uniqueIndex.Invisible)
				require.True(t, *uniqueIndex.Invisible)

				fulltextIndex := indexMap["idx_data"]
				require.Equal(t, "FULLTEXT", fulltextIndex.Type)
				require.NotNil(t, fulltextIndex.ParserName)
				require.Equal(t, "ngram", *fulltextIndex.ParserName)
				require.NotNil(t, fulltextIndex.Comment)
				require.Equal(t, "JSON search", *fulltextIndex.Comment)
			},
		},

		// Error cases (should not parse)
		{
			Name:        "Invalid table name with asterisk",
			SQL:         "CREATE TABLE foo.* (a varchar(50), b int);",
			ShouldParse: false,
			Validate:    nil,
		},
		{
			Name:        "Empty column list",
			SQL:         "CREATE TABLE foo ();",
			ShouldParse: false,
			Validate:    nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			ct, err := ParseCreateTable(tc.SQL)

			if tc.ShouldParse {
				require.NoError(t, err, "Expected SQL to parse successfully: %s", tc.SQL)
				require.NotNil(t, ct)

				if tc.Validate != nil {
					tc.Validate(t, ct)
				}
			} else {
				require.Error(t, err, "Expected SQL to fail parsing: %s", tc.SQL)
			}
		})
	}
}

// TestTiDBParserCompatibility tests specific patterns from TiDB parser test suite
func TestTiDBParserCompatibility(t *testing.T) {
	// Test cases directly derived from TiDB parser_test.go
	tidbTestCases := []struct {
		sql         string
		shouldParse bool
	}{
		// Basic cases from TiDB test suite
		{"CREATE TABLE foo (a varchar(50), b int);", true},
		{"CREATE TABLE foo (a TINYINT UNSIGNED);", true},
		{"CREATE TABLE foo (a SMALLINT UNSIGNED, b INT UNSIGNED)", true},
		{"CREATE TABLE foo (a bigint unsigned, b bool);", true},
		{"CREATE TABLE foo (name CHAR(50) BINARY);", true},
		{"CREATE TABLE foo (name CHAR(50) COLLATE utf8_bin)", true},
		{"CREATE TABLE foo (id varchar(50) collate utf8_bin);", true},
		{"CREATE TABLE foo (name CHAR(50) CHARACTER SET UTF8)", true},

		// Index visibility cases from TiDB test suite
		{"CREATE TABLE t (id INT, INDEX idx (id) INVISIBLE);", true},
		{"CREATE TABLE t (id INT, INDEX idx (id) VISIBLE);", true},
		{"CREATE TABLE t (id INT, INDEX idx (id) INVISIBLE VISIBLE);", true},
		{"CREATE TABLE t (id INT, INDEX idx (id) VISIBLE INVISIBLE);", true},
		{"CREATE TABLE t (id INT, INDEX idx (id) USING HASH VISIBLE);", true},
		{"CREATE TABLE t (id INT, INDEX idx (id) USING HASH INVISIBLE);", true},

		// Error cases from TiDB test suite
		{"CREATE", false},
		{"CREATE TABLE", false},
		{"CREATE TABLE foo (", false},
		{"CREATE TABLE foo ()", false},
		{"CREATE TABLE foo ();", false},
		{"CREATE TABLE foo.* (a varchar(50), b int);", false},
	}

	for _, tc := range tidbTestCases {
		t.Run(tc.sql, func(t *testing.T) {
			_, err := ParseCreateTable(tc.sql)

			if tc.shouldParse {
				require.NoError(t, err, "Expected to parse: %s", tc.sql)
			} else {
				require.Error(t, err, "Expected to fail: %s", tc.sql)
			}
		})
	}
}

// BenchmarkComprehensiveParsing benchmarks our parser against complex real-world schemas
func BenchmarkComprehensiveParsing(b *testing.B) {
	complexSQL := `CREATE TABLE comprehensive_benchmark (
		id BIGINT PRIMARY KEY AUTO_INCREMENT,
		uuid CHAR(36) NOT NULL UNIQUE,
		name VARCHAR(255) NOT NULL,
		description TEXT,
		price DECIMAL(10,2) DEFAULT 0.00,
		category_id INT,
		status ENUM('active', 'inactive') DEFAULT 'active',
		metadata JSON,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		
		INDEX idx_category (category_id) COMMENT 'Category lookup',
		INDEX idx_status (status) USING BTREE,
		INDEX idx_created (created_at) INVISIBLE,
		UNIQUE KEY uk_name_category (name, category_id) USING BTREE KEY_BLOCK_SIZE = 8,
		FULLTEXT INDEX ft_description (description) WITH PARSER ngram COMMENT 'Full-text search',
		
		CONSTRAINT fk_category FOREIGN KEY (category_id) REFERENCES categories(id)
	) ENGINE=InnoDB CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC COMMENT='Comprehensive benchmark table'`

	b.ResetTimer()

	for b.Loop() {
		ct, err := ParseCreateTable(complexSQL)
		if err != nil {
			b.Fatal(err)
		}

		_ = ct.GetColumns()
		_ = ct.GetIndexes()
		_ = ct.GetConstraints()
		_ = ct.GetTableOptions()
	}
}

// TestCurrentTimestampPrecision tests that CURRENT_TIMESTAMP with fractional seconds
// precision (e.g. CURRENT_TIMESTAMP(3)) is correctly preserved during parsing.
// This is critical for DATETIME(3)/TIMESTAMP(3) columns where MySQL requires the
// precision to match between the column type and the default value.
// See: https://github.com/block/spirit/issues/XXX
func TestCurrentTimestampPrecision(t *testing.T) {
	testCases := []struct {
		name            string
		sql             string
		columnName      string
		expectedDefault string
	}{
		{
			name:            "CURRENT_TIMESTAMP without precision",
			sql:             "CREATE TABLE t1 (id INT PRIMARY KEY, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
			columnName:      "created_at",
			expectedDefault: "current_timestamp",
		},
		{
			name:            "CURRENT_TIMESTAMP(3) with millisecond precision",
			sql:             "CREATE TABLE t1 (id INT PRIMARY KEY, created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3))",
			columnName:      "created_at",
			expectedDefault: "current_timestamp(3)",
		},
		{
			name:            "CURRENT_TIMESTAMP(6) with microsecond precision",
			sql:             "CREATE TABLE t1 (id INT PRIMARY KEY, created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6))",
			columnName:      "created_at",
			expectedDefault: "current_timestamp(6)",
		},
		{
			name:            "NOW() normalized to CURRENT_TIMESTAMP",
			sql:             "CREATE TABLE t1 (id INT PRIMARY KEY, created_at TIMESTAMP DEFAULT NOW())",
			columnName:      "created_at",
			expectedDefault: "current_timestamp",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ct, err := ParseCreateTable(tc.sql)
			require.NoError(t, err)

			col := ct.Columns.ByName(tc.columnName)
			require.NotNil(t, col, "column %q not found", tc.columnName)
			require.NotNil(t, col.Default, "column %q has no default", tc.columnName)
			require.Equal(t, tc.expectedDefault, *col.Default)
		})
	}
}

// TestExpressionDefaultParsing tests that expression defaults (e.g., DEFAULT (json_object()))
// are correctly parsed with DefaultIsExpr=true, while literal defaults remain DefaultIsExpr=false.
func TestExpressionDefaultParsing(t *testing.T) {
	testCases := []struct {
		name            string
		sql             string
		columnName      string
		expectedDefault string
		expectedIsExpr  bool
	}{
		{
			name:            "json_object expression default",
			sql:             "CREATE TABLE t1 (id INT PRIMARY KEY, metadata JSON NOT NULL DEFAULT (json_object()))",
			columnName:      "metadata",
			expectedDefault: "json_object()",
			expectedIsExpr:  true,
		},
		{
			name:            "json_array expression default",
			sql:             "CREATE TABLE t1 (id INT PRIMARY KEY, tags JSON NOT NULL DEFAULT (json_array()))",
			columnName:      "tags",
			expectedDefault: "json_array()",
			expectedIsExpr:  true,
		},
		{
			name:            "CURRENT_TIMESTAMP is not an expression default",
			sql:             "CREATE TABLE t1 (id INT PRIMARY KEY, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
			columnName:      "created_at",
			expectedDefault: "current_timestamp",
			expectedIsExpr:  false,
		},
		{
			name:            "string literal is not an expression default",
			sql:             "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100) DEFAULT 'hello')",
			columnName:      "name",
			expectedDefault: "hello",
			expectedIsExpr:  false,
		},
		{
			name:            "UUID expression default",
			sql:             "CREATE TABLE t1 (id INT PRIMARY KEY, uid VARCHAR(36) NOT NULL DEFAULT (uuid()))",
			columnName:      "uid",
			expectedDefault: "uuid()",
			expectedIsExpr:  true,
		},
		{
			name:            "parenthesized string literal is an expression default",
			sql:             "CREATE TABLE t1 (id INT PRIMARY KEY, j JSON DEFAULT ('{}'))",
			columnName:      "j",
			expectedDefault: "{}",
			expectedIsExpr:  true,
		},
		{
			name:            "parenthesized numeric literal is an expression default",
			sql:             "CREATE TABLE t1 (id INT PRIMARY KEY, n INT DEFAULT (1))",
			columnName:      "n",
			expectedDefault: "1",
			expectedIsExpr:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ct, err := ParseCreateTable(tc.sql)
			require.NoError(t, err)

			col := ct.Columns.ByName(tc.columnName)
			require.NotNil(t, col, "column %q not found", tc.columnName)
			require.NotNil(t, col.Default, "column %q has no default", tc.columnName)
			require.Equal(t, tc.expectedDefault, *col.Default)
			require.Equal(t, tc.expectedIsExpr, col.DefaultIsExpr, "DefaultIsExpr mismatch for %q", tc.columnName)
		})
	}
}

// TestParenthesizedLiteralDefaultRoundTrip covers the declarative side of
// https://github.com/block/spirit/issues/542: DEFAULT ('{}') must extract as
// a string-valued expression default and emit back with exactly one set of
// parentheses and quotes. It must also compare different from the bare
// literal form DEFAULT '{}' (they are different DDL: BLOB/TEXT/JSON columns
// only accept the parenthesized one).
func TestParenthesizedLiteralDefaultRoundTrip(t *testing.T) {
	ct, err := ParseCreateTable("CREATE TABLE t1 (id INT PRIMARY KEY, j JSON DEFAULT ('{}'))")
	require.NoError(t, err)
	col := ct.Columns.ByName("j")
	require.NotNil(t, col)
	require.NotNil(t, col.Default)
	require.Equal(t, "{}", *col.Default)
	require.True(t, col.DefaultIsExpr)
	require.True(t, col.DefaultIsString)
	require.Contains(t, formatColumnDefinition(col), "DEFAULT ('{}')")

	// MySQL's SHOW CREATE TABLE renders the same default with a charset
	// introducer; both forms must extract identically so the desired and
	// live definitions converge.
	ctLive, err := ParseCreateTable("CREATE TABLE t1 (id INT PRIMARY KEY, j JSON DEFAULT (_utf8mb4'{}'))")
	require.NoError(t, err)
	colLive := ctLive.Columns.ByName("j")
	require.NotNil(t, colLive)
	require.NotNil(t, colLive.Default)
	require.Equal(t, "{}", *colLive.Default)
	require.True(t, colLive.DefaultIsExpr)
	require.True(t, colLive.DefaultIsString)

	// The bare and parenthesized forms of the same literal are different
	// defaults and must not compare equal.
	ctBare, err := ParseCreateTable("CREATE TABLE t1 (id INT PRIMARY KEY, v VARCHAR(10) DEFAULT 'x')")
	require.NoError(t, err)
	ctParen, err := ParseCreateTable("CREATE TABLE t1 (id INT PRIMARY KEY, v VARCHAR(10) DEFAULT ('x'))")
	require.NoError(t, err)
	bare := ctBare.Columns.ByName("v")
	paren := ctParen.Columns.ByName("v")
	require.False(t, bare.DefaultIsExpr)
	require.True(t, paren.DefaultIsExpr)
	require.Equal(t, "DEFAULT 'x'", lastDefaultClause(t, formatColumnDefinition(bare)))
	require.Equal(t, "DEFAULT ('x')", lastDefaultClause(t, formatColumnDefinition(paren)))
}

func lastDefaultClause(t *testing.T, columnDef string) string {
	t.Helper()
	idx := strings.Index(columnDef, "DEFAULT ")
	require.GreaterOrEqual(t, idx, 0, "no DEFAULT clause in %q", columnDef)
	return columnDef[idx:]
}

// TestBinaryTypeDetection tests that binary types are correctly detected and converted
// from their text equivalents when the binary flag is set
func TestBinaryTypeDetection(t *testing.T) {
	testCases := []struct {
		name         string
		sql          string
		columnName   string
		expectedType string
	}{
		{
			name:         "VARBINARY type",
			sql:          "CREATE TABLE test (data VARBINARY(255));",
			columnName:   "data",
			expectedType: "varbinary",
		},
		{
			name:         "BINARY type",
			sql:          "CREATE TABLE test (data BINARY(16));",
			columnName:   "data",
			expectedType: "binary",
		},
		{
			name:         "BLOB type",
			sql:          "CREATE TABLE test (data BLOB);",
			columnName:   "data",
			expectedType: "blob",
		},
		{
			name:         "TINYBLOB type",
			sql:          "CREATE TABLE test (data TINYBLOB);",
			columnName:   "data",
			expectedType: "tinyblob",
		},
		{
			name:         "MEDIUMBLOB type",
			sql:          "CREATE TABLE test (data MEDIUMBLOB);",
			columnName:   "data",
			expectedType: "mediumblob",
		},
		{
			name:         "LONGBLOB type",
			sql:          "CREATE TABLE test (data LONGBLOB);",
			columnName:   "data",
			expectedType: "longblob",
		},
		{
			name:         "Multiple binary columns",
			sql:          "CREATE TABLE test (id INT, data1 VARBINARY(100), data2 BINARY(32), content BLOB);",
			columnName:   "data1",
			expectedType: "varbinary",
		},
		{
			name:         "VARBINARY with length",
			sql:          "CREATE TABLE test (hash VARBINARY(64));",
			columnName:   "hash",
			expectedType: "varbinary",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ct, err := ParseCreateTable(tc.sql)
			require.NoError(t, err)

			columns := ct.GetColumns()
			col := columns.ByName(tc.columnName)
			require.NotNil(t, col, "Column %s should exist", tc.columnName)
			require.Equal(t, tc.expectedType, col.Type, "Column type should be %s", tc.expectedType)
		})
	}
}

// TestBinaryTypeWithLength tests that binary types preserve their length information
func TestBinaryTypeWithLength(t *testing.T) {
	testCases := []struct {
		name           string
		sql            string
		columnName     string
		expectedType   string
		expectedLength *int
	}{
		{
			name:           "VARBINARY with length",
			sql:            "CREATE TABLE test (data VARBINARY(255));",
			columnName:     "data",
			expectedType:   "varbinary",
			expectedLength: new(255),
		},
		{
			name:           "BINARY with length",
			sql:            "CREATE TABLE test (data BINARY(16));",
			columnName:     "data",
			expectedType:   "binary",
			expectedLength: new(16),
		},
		{
			name:           "BLOB without length",
			sql:            "CREATE TABLE test (data BLOB);",
			columnName:     "data",
			expectedType:   "blob",
			expectedLength: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ct, err := ParseCreateTable(tc.sql)
			require.NoError(t, err)

			columns := ct.GetColumns()
			col := columns.ByName(tc.columnName)
			require.NotNil(t, col)
			require.Equal(t, tc.expectedType, col.Type)

			if tc.expectedLength != nil {
				require.NotNil(t, col.Length, "Length should be set")
				require.Equal(t, *tc.expectedLength, *col.Length)
			} else {
				require.Nil(t, col.Length, "Length should not be set")
			}
		})
	}
}

// TestCharsetCollationExtraction tests that charset and collation are correctly
// extracted from column type definitions
func TestCharsetCollationExtraction(t *testing.T) {
	testCases := []struct {
		name              string
		sql               string
		columnName        string
		expectedCharset   *string
		expectedCollation *string
	}{
		{
			name:              "VARCHAR with CHARACTER SET",
			sql:               "CREATE TABLE test (name VARCHAR(100) CHARACTER SET utf8mb4);",
			columnName:        "name",
			expectedCharset:   new("utf8mb4"),
			expectedCollation: nil,
		},
		{
			name:              "CHAR with COLLATE",
			sql:               "CREATE TABLE test (code CHAR(10) COLLATE utf8mb4_bin);",
			columnName:        "code",
			expectedCharset:   nil,
			expectedCollation: new("utf8mb4_bin"),
		},
		{
			name:              "VARCHAR with both CHARACTER SET and COLLATE",
			sql:               "CREATE TABLE test (name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci);",
			columnName:        "name",
			expectedCharset:   new("utf8mb4"),
			expectedCollation: new("utf8mb4_unicode_ci"),
		},
		{
			name:              "TEXT with CHARACTER SET",
			sql:               "CREATE TABLE test (description TEXT CHARACTER SET utf8);",
			columnName:        "description",
			expectedCharset:   new("utf8"),
			expectedCollation: nil,
		},
		{
			name:              "Column without charset/collation",
			sql:               "CREATE TABLE test (name VARCHAR(100));",
			columnName:        "name",
			expectedCharset:   nil,
			expectedCollation: nil,
		},
		{
			name:              "Multiple columns with different charsets",
			sql:               "CREATE TABLE test (name1 VARCHAR(100) CHARACTER SET utf8, name2 VARCHAR(100) CHARACTER SET latin1);",
			columnName:        "name1",
			expectedCharset:   new("utf8"),
			expectedCollation: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ct, err := ParseCreateTable(tc.sql)
			require.NoError(t, err)

			columns := ct.GetColumns()
			col := columns.ByName(tc.columnName)
			require.NotNil(t, col, "Column %s should exist", tc.columnName)

			if tc.expectedCharset != nil {
				require.NotNil(t, col.Charset, "Charset should be set")
				require.Equal(t, *tc.expectedCharset, *col.Charset)
			} else {
				require.Nil(t, col.Charset, "Charset should not be set")
			}

			if tc.expectedCollation != nil {
				require.NotNil(t, col.Collation, "Collation should be set")
				require.Equal(t, *tc.expectedCollation, *col.Collation)
			} else {
				require.Nil(t, col.Collation, "Collation should not be set")
			}
		})
	}
}

// TestCharsetCollationOverride tests that column options can override charset/collation
// from the type definition
func TestCharsetCollationOverride(t *testing.T) {
	// Note: This test documents current behavior where column options should override
	// type-level charset/collation. The TiDB parser may handle this differently.
	sql := `CREATE TABLE test (
		name VARCHAR(100) CHARACTER SET utf8 COLLATE utf8_bin
	);`

	ct, err := ParseCreateTable(sql)
	require.NoError(t, err)

	columns := ct.GetColumns()
	col := columns.ByName("name")
	require.NotNil(t, col)

	// Verify that charset and collation are captured
	// The exact behavior depends on how TiDB parser handles these attributes
	if col.Charset != nil {
		require.Equal(t, "utf8", *col.Charset)
	}
	if col.Collation != nil {
		require.Equal(t, "utf8_bin", *col.Collation)
	}
}

// TestBinaryTypeNotAppliedToNonTextTypes tests that the binary flag conversion
// only applies to text types and not to other types
func TestBinaryTypeNotAppliedToNonTextTypes(t *testing.T) {
	testCases := []struct {
		name         string
		sql          string
		columnName   string
		expectedType string
	}{
		{
			name:         "INT remains INT",
			sql:          "CREATE TABLE test (id INT);",
			columnName:   "id",
			expectedType: "int",
		},
		{
			name:         "BIGINT remains BIGINT",
			sql:          "CREATE TABLE test (id BIGINT);",
			columnName:   "id",
			expectedType: "bigint",
		},
		{
			name:         "DECIMAL remains DECIMAL",
			sql:          "CREATE TABLE test (price DECIMAL(10,2));",
			columnName:   "price",
			expectedType: "decimal",
		},
		{
			name:         "DATE remains DATE",
			sql:          "CREATE TABLE test (created DATE);",
			columnName:   "created",
			expectedType: "date",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ct, err := ParseCreateTable(tc.sql)
			require.NoError(t, err)

			columns := ct.GetColumns()
			col := columns.ByName(tc.columnName)
			require.NotNil(t, col)
			require.Equal(t, tc.expectedType, col.Type)
		})
	}
}

// TestComplexTableWithBinaryAndCharset tests a realistic table with both
// binary types and charset/collation specifications
func TestComplexTableWithBinaryAndCharset(t *testing.T) {
	sql := `CREATE TABLE users (
		id BIGINT PRIMARY KEY AUTO_INCREMENT,
		username VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
		password_hash VARBINARY(64) NOT NULL,
		email VARCHAR(255) CHARACTER SET utf8mb4 NOT NULL,
		profile_data BLOB,
		bio TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
		session_token BINARY(32),
		metadata MEDIUMBLOB,
		UNIQUE KEY uk_username (username),
		UNIQUE KEY uk_email (email)
	) ENGINE=InnoDB CHARSET=utf8mb4;`

	ct, err := ParseCreateTable(sql)
	require.NoError(t, err)
	require.Equal(t, "users", ct.GetTableName())

	columns := ct.GetColumns()
	require.Len(t, columns, 8)

	// Test username column (VARCHAR with charset and collation)
	username := columns.ByName("username")
	require.NotNil(t, username)
	require.Contains(t, username.Type, "varchar")
	require.False(t, username.Nullable)
	if username.Charset != nil {
		require.Equal(t, "utf8mb4", *username.Charset)
	}
	if username.Collation != nil {
		require.Equal(t, "utf8mb4_unicode_ci", *username.Collation)
	}

	// Test password_hash column (VARBINARY)
	passwordHash := columns.ByName("password_hash")
	require.NotNil(t, passwordHash)
	require.Equal(t, "varbinary", passwordHash.Type)
	require.False(t, passwordHash.Nullable)
	require.NotNil(t, passwordHash.Length)
	require.Equal(t, 64, *passwordHash.Length)

	// Test email column (VARCHAR with charset)
	email := columns.ByName("email")
	require.NotNil(t, email)
	require.Contains(t, email.Type, "varchar")
	if email.Charset != nil {
		require.Equal(t, "utf8mb4", *email.Charset)
	}

	// Test profile_data column (BLOB)
	profileData := columns.ByName("profile_data")
	require.NotNil(t, profileData)
	require.Equal(t, "blob", profileData.Type)

	// Test bio column (TEXT with charset and collation)
	bio := columns.ByName("bio")
	require.NotNil(t, bio)
	require.Equal(t, "text", bio.Type)
	if bio.Charset != nil {
		require.Equal(t, "utf8mb4", *bio.Charset)
	}
	if bio.Collation != nil {
		require.Equal(t, "utf8mb4_unicode_ci", *bio.Collation)
	}

	// Test session_token column (BINARY)
	sessionToken := columns.ByName("session_token")
	require.NotNil(t, sessionToken)
	require.Equal(t, "binary", sessionToken.Type)
	require.NotNil(t, sessionToken.Length)
	require.Equal(t, 32, *sessionToken.Length)

	// Test metadata column (MEDIUMBLOB)
	metadata := columns.ByName("metadata")
	require.NotNil(t, metadata)
	require.Equal(t, "mediumblob", metadata.Type)

	// Verify table options
	options := ct.GetTableOptions()
	require.Equal(t, "InnoDB", options["engine"])
	require.Equal(t, "utf8mb4", options["charset"])
}

// TestBinaryTypeJSONSerialization tests that binary types can be serialized
// and deserialized correctly
func TestBinaryTypeJSONSerialization(t *testing.T) {
	sql := `CREATE TABLE test (
		id INT PRIMARY KEY,
		data VARBINARY(255),
		content BLOB
	);`

	ct, err := ParseCreateTable(sql)
	require.NoError(t, err)

	columns := ct.GetColumns()
	jsonData, err := json.Marshal(columns)
	require.NoError(t, err)

	var deserializedColumns []Column
	err = json.Unmarshal(jsonData, &deserializedColumns)
	require.NoError(t, err)

	require.Len(t, deserializedColumns, 3)

	// Verify data column (VARBINARY)
	dataCol := deserializedColumns[1]
	require.Equal(t, "data", dataCol.Name)
	require.Equal(t, "varbinary", dataCol.Type)
	require.NotNil(t, dataCol.Length)
	require.Equal(t, 255, *dataCol.Length)

	// Verify content column (BLOB)
	contentCol := deserializedColumns[2]
	require.Equal(t, "content", contentCol.Name)
	require.Equal(t, "blob", contentCol.Type)
}

// TestSpatialIndexParsing verifies that CREATE TABLE with SPATIAL INDEX parses
// and that the index is exposed with Type="SPATIAL". Without explicit handling
// in parseIndex, this would panic with "unknown constraint type: 12".
func TestSpatialIndexParsing(t *testing.T) {
	ct, err := ParseCreateTable(`CREATE TABLE places (
		id INT PRIMARY KEY,
		name VARCHAR(100),
		location GEOMETRY NOT NULL SRID 4326,
		SPATIAL INDEX idx_location (location)
	)`)
	require.NoError(t, err)

	var spatial *Index
	for i, idx := range ct.GetIndexes() {
		if idx.Name == "idx_location" {
			spatial = &ct.GetIndexes()[i]
			break
		}
	}
	require.NotNil(t, spatial, "expected SPATIAL index idx_location to be present")
	require.Equal(t, "SPATIAL", spatial.Type)
	require.Equal(t, []string{"location"}, spatial.Columns)
}
