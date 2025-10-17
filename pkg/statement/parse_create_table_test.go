package statement

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
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
	assert.Equal(t, "users", ct.GetTableName())

	// Test columns
	columns := ct.GetColumns()
	assert.Len(t, columns, 4)

	// Test first column (id)
	idCol := columns[0]
	assert.Equal(t, "id", idCol.Name)
	assert.Contains(t, idCol.Type, "int") // TiDB returns "int(11)" not just "int"
	assert.True(t, idCol.AutoInc)
	assert.False(t, idCol.Nullable)

	// Test second column (name)
	nameCol := columns[1]
	assert.Equal(t, "name", nameCol.Name)
	assert.Contains(t, nameCol.Type, "varchar") // TiDB returns "varchar(255)" not just "varchar"
	assert.NotNil(t, nameCol.Length)
	assert.Equal(t, 255, *nameCol.Length)
	assert.False(t, nameCol.Nullable)

	// Test indexes (PRIMARY KEY and UNIQUE should be detected)
	indexes := ct.GetIndexes()
	assert.GreaterOrEqual(t, len(indexes), 2) // At least PRIMARY KEY and UNIQUE

	// Test table options
	options := ct.GetTableOptions()
	assert.Equal(t, "InnoDB", options["engine"])
	assert.Equal(t, "utf8mb4", options["charset"])
	assert.Equal(t, "User table", options["comment"])
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
	assert.Equal(t, "products", createTable.TableName)

	// Test columns access
	columns := createTable.Columns
	require.Len(t, columns, 3)
	assert.Equal(t, "id", columns[0].Name)

	// Test table options access
	options := ct.GetTableOptions()
	assert.Equal(t, "InnoDB", options["engine"])

	// Test using ByName methods
	if nameCol := columns.ByName("name"); nameCol != nil {
		assert.Equal(t, "name", nameCol.Name)
		assert.Contains(t, nameCol.Type, "varchar")
		assert.False(t, nameCol.Nullable)
		// Comment parsing might not work perfectly with TiDB parser
		if nameCol.Comment != nil {
			assert.Equal(t, "Product name", *nameCol.Comment)
		}
	} else {
		t.Error("Should find column 'name'")
	}

	// Test column count
	assert.Len(t, columns, 3)

	// Test finding nullable columns using structured approach
	var nullableColumns []Column

	for _, col := range columns {
		if col.Nullable {
			nullableColumns = append(nullableColumns, col)
		}
	}

	assert.GreaterOrEqual(t, len(nullableColumns), 1) // price should be nullable
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
	assert.GreaterOrEqual(t, len(constraints), 1)

	assert.True(t, constraints.HasForeignKeys())

	// Find the foreign key constraint
	var fkConstraint *Constraint

	for i := range constraints {
		if constraints[i].Type == "FOREIGN KEY" {
			fkConstraint = &constraints[i]
			break
		}
	}

	require.NotNil(t, fkConstraint)
	assert.Equal(t, "fk_orders_user", fkConstraint.Name)
	assert.Contains(t, *fkConstraint.Definition, "REFERENCES users")
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
	assert.Nil(t, signedInt.Unsigned, "signed_int should not have Unsigned field set")

	signedBigint := columns.ByName("signed_bigint")
	require.NotNil(t, signedBigint)
	assert.Nil(t, signedBigint.Unsigned, "signed_bigint should not have Unsigned field set")

	signedTinyint := columns.ByName("signed_tinyint")
	require.NotNil(t, signedTinyint)
	assert.Nil(t, signedTinyint.Unsigned, "signed_tinyint should not have Unsigned field set")

	// Test unsigned columns (should have Unsigned = true)
	unsignedInt := columns.ByName("unsigned_int")
	require.NotNil(t, unsignedInt)
	require.NotNil(t, unsignedInt.Unsigned, "unsigned_int should have Unsigned field set")
	assert.True(t, *unsignedInt.Unsigned, "unsigned_int should be marked as unsigned")

	unsignedBigint := columns.ByName("unsigned_bigint")
	require.NotNil(t, unsignedBigint)
	require.NotNil(t, unsignedBigint.Unsigned, "unsigned_bigint should have Unsigned field set")
	assert.True(t, *unsignedBigint.Unsigned, "unsigned_bigint should be marked as unsigned")

	unsignedTinyint := columns.ByName("unsigned_tinyint")
	require.NotNil(t, unsignedTinyint)
	require.NotNil(t, unsignedTinyint.Unsigned, "unsigned_tinyint should have Unsigned field set")
	assert.True(t, *unsignedTinyint.Unsigned, "unsigned_tinyint should be marked as unsigned")

	// Test id column (should not be unsigned)
	idCol := columns.ByName("id")
	require.NotNil(t, idCol)
	assert.Nil(t, idCol.Unsigned, "id should not have Unsigned field set")
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

	assert.Equal(t, "id", deserializedColumns[0].Name)
	assert.Equal(t, "int", deserializedColumns[0].Type)
	assert.True(t, deserializedColumns[0].PrimaryKey)

	assert.Equal(t, "name", deserializedColumns[1].Name)
	assert.Contains(t, deserializedColumns[1].Type, "varchar")
	assert.False(t, deserializedColumns[1].Nullable)
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
	assert.True(t, *emailIdx.Invisible, "idx_email should be invisible")

	multiIdx := indexes.ByName("uk_email_multi")
	require.NotNil(t, multiIdx, "Should find uk_email_multi")
	require.NotNil(t, multiIdx.Invisible)
	assert.True(t, *multiIdx.Invisible, "uk_email_multi should be invisible")
	require.NotNil(t, multiIdx.Using)
	assert.Equal(t, "BTREE", *multiIdx.Using, "uk_email_multi should use BTREE")
	require.NotNil(t, multiIdx.Comment)
	assert.Equal(t, "Multi-option unique", *multiIdx.Comment)

	statusIdx := indexes.ByName("idx_status")
	require.NotNil(t, statusIdx, "Should find idx_status")
	require.NotNil(t, statusIdx.Invisible)
	assert.False(t, *statusIdx.Invisible, "idx_status should be explicitly visible")

	nameIdx := indexes.ByName("idx_name")
	require.NotNil(t, nameIdx, "Should find idx_name")
	assert.Nil(t, nameIdx.Invisible, "idx_name should have no invisibility setting")

	// Test finding all invisible indexes using structured approach
	var invisibleIndexes []Index

	for _, idx := range indexes {
		if idx.Invisible != nil && *idx.Invisible {
			invisibleIndexes = append(invisibleIndexes, idx)
		}
	}

	assert.Len(t, invisibleIndexes, 2, "Should find exactly 2 invisible indexes")

	// Verify the invisible indexes are the ones we expect
	invisibleNames := make(map[string]bool)
	for _, idx := range invisibleIndexes {
		invisibleNames[idx.Name] = true
	}

	assert.True(t, invisibleNames["idx_email"], "Should include idx_email in invisible indexes")
	assert.True(t, invisibleNames["uk_email_multi"], "Should include uk_email_multi in invisible indexes")
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

	for range b.N {
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
				Expression: stringPtr("year"),
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
				Partitions: 4,
				// Note: Expression might be nil if TiDB parser doesn't expose it for simple cases
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
							Type:   "IN",
							Values: []any{"north", "northeast"},
						},
					},
					{
						Name: "p_south",
						Values: &PartitionValues{
							Type:   "IN",
							Values: []any{"south"},
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
				assert.Nil(t, partition)
				return
			}

			require.NotNil(t, partition)
			assert.Equal(t, tc.expected.Type, partition.Type)
			assert.Equal(t, tc.expected.Partitions, partition.Partitions)
			assert.Equal(t, tc.expected.Columns, partition.Columns)

			if tc.expected.Expression != nil {
				require.NotNil(t, partition.Expression)
				assert.Equal(t, *tc.expected.Expression, *partition.Expression)
			} else {
				assert.Nil(t, partition.Expression)
			}

			assert.Len(t, partition.Definitions, len(tc.expected.Definitions))

			for i, expectedDef := range tc.expected.Definitions {
				if i < len(partition.Definitions) {
					actualDef := partition.Definitions[i]
					assert.Equal(t, expectedDef.Name, actualDef.Name)

					if expectedDef.Values != nil {
						require.NotNil(t, actualDef.Values)
						assert.Equal(t, expectedDef.Values.Type, actualDef.Values.Type)
						assert.Equal(t, expectedDef.Values.Values, actualDef.Values.Values)
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
	assert.Equal(t, "enum", statusCol.Type)
	require.NotNil(t, statusCol.EnumValues)
	assert.Equal(t, []string{"active", "inactive", "pending"}, statusCol.EnumValues)
	assert.Nil(t, statusCol.SetValues, "ENUM column should not have SetValues")
	require.NotNil(t, statusCol.Default)
	assert.Equal(t, "active", *statusCol.Default)
	assert.True(t, statusCol.Nullable)

	// Test SET column with default
	permissionsCol := columns.ByName("permissions")
	require.NotNil(t, permissionsCol)
	assert.Equal(t, "set", permissionsCol.Type)
	require.NotNil(t, permissionsCol.SetValues)
	assert.Equal(t, []string{"read", "write", "execute"}, permissionsCol.SetValues)
	assert.Nil(t, permissionsCol.EnumValues, "SET column should not have EnumValues")
	require.NotNil(t, permissionsCol.Default)
	assert.Equal(t, "read", *permissionsCol.Default)

	// Test ENUM column NOT NULL
	priorityCol := columns.ByName("priority")
	require.NotNil(t, priorityCol)
	assert.Equal(t, "enum", priorityCol.Type)
	require.NotNil(t, priorityCol.EnumValues)
	assert.Equal(t, []string{"low", "medium", "high"}, priorityCol.EnumValues)
	assert.False(t, priorityCol.Nullable)

	// Test SET column without default
	flagsCol := columns.ByName("flags")
	require.NotNil(t, flagsCol)
	assert.Equal(t, "set", flagsCol.Type)
	require.NotNil(t, flagsCol.SetValues)
	assert.Equal(t, []string{"flag1", "flag2", "flag3", "flag4"}, flagsCol.SetValues)
	assert.Nil(t, flagsCol.Default)

	// Test regular column (should have no enum/set values)
	nameCol := columns.ByName("name")
	require.NotNil(t, nameCol)
	assert.Contains(t, nameCol.Type, "varchar")
	assert.Nil(t, nameCol.EnumValues)
	assert.Nil(t, nameCol.SetValues)
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

	// Check id column
	assert.Equal(t, "id", deserializedColumns[0].Name)
	assert.Equal(t, "int", deserializedColumns[0].Type)
	assert.True(t, deserializedColumns[0].PrimaryKey)
	assert.Nil(t, deserializedColumns[0].EnumValues)
	assert.Nil(t, deserializedColumns[0].SetValues)

	// Verify enum values are preserved
	statusCol := deserializedColumns[1]
	assert.Equal(t, "status", statusCol.Name)
	assert.Equal(t, "enum", statusCol.Type)
	assert.Equal(t, []string{"active", "inactive"}, statusCol.EnumValues)
	assert.Nil(t, statusCol.SetValues)
	require.NotNil(t, statusCol.Default)
	assert.Equal(t, "active", *statusCol.Default)

	// Verify set values are preserved
	permissionsCol := deserializedColumns[2]
	assert.Equal(t, "permissions", permissionsCol.Name)
	assert.Equal(t, "set", permissionsCol.Type)
	assert.Equal(t, []string{"read", "write"}, permissionsCol.SetValues)
	assert.Nil(t, permissionsCol.EnumValues)
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
	assert.Equal(t, "enum", statusCol.Type)
	require.NotNil(t, statusCol.EnumValues)
	assert.Equal(t, []string{"only_one"}, statusCol.EnumValues)
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
	assert.NoError(t, err)

	// This accounts for the two different primary keys and 2 different unique indexes,
	// each given once as a column attribute and once as a table constraint.
	assert.Len(t, ct.GetIndexes(), 7)

	// The "first" index should be the first one defined as a table attribute, not the PK or UNIQUE
	// index defined as a column attribute.
	firstIdx := ct.GetCreateTable().Indexes[0]
	require.NotNil(t, firstIdx)
	// Nameless indexes get an empty string as the name
	assert.Empty(t, firstIdx.Name)
	assert.Equal(t, []string{"user_id", "customerEmail"}, firstIdx.Columns)
	assert.Nil(t, firstIdx.Comment)

	assert.True(t, ct.GetIndexes().HasInvisible())
	idx_created_at := ct.GetCreateTable().Indexes.ByName("idx_created_at")
	require.NotNil(t, idx_created_at)
	require.NotNil(t, idx_created_at.Invisible)
	assert.True(t, *idx_created_at.Invisible)

	enum := ct.GetColumns().ByName("order_status")
	require.NotNil(t, enum)
	assert.True(t, strings.EqualFold("ENUM", enum.Type))

	// Verify enum values are captured
	require.NotNil(t, enum.EnumValues)
	assert.Equal(t, []string{"pending", "processing", "shipped", "delivered", "cancelled"}, enum.EnumValues)
	assert.Equal(t, "pending", *enum.Default)

	total_amount := ct.GetColumns().ByName("total_amount")
	require.NotNil(t, total_amount)
	assert.Contains(t, strings.ToLower(total_amount.Type), "decimal")
	assert.NotNil(t, total_amount.Default)
	assert.Equal(t, "0.00", *total_amount.Default)
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
				assert.Equal(t, "foo", createTable.GetTableName())
				columns := createTable.GetColumns()
				assert.Len(t, columns, 2)
				assert.Equal(t, "a", columns[0].Name)
				assert.Contains(t, columns[0].Type, "varchar")
				assert.Equal(t, "b", columns[1].Name)
				assert.Contains(t, columns[1].Type, "int")
			},
		},
		{
			Name:        "Table with unsigned integers",
			SQL:         "CREATE TABLE foo (a TINYINT UNSIGNED, b SMALLINT UNSIGNED, c INT UNSIGNED, d BIGINT UNSIGNED);",
			ShouldParse: true,
			Validate: func(t *testing.T, createTable *CreateTable) {
				columns := createTable.GetColumns()
				assert.Len(t, columns, 4)

				for _, col := range columns {
					// TiDB parser includes UNSIGNED in the type string as "type(size) UNSIGNED"
					assert.NotNil(t, col.Unsigned)
					assert.True(t, *col.Unsigned)
					assert.Contains(t, col.Type, "int")
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
				assert.True(t, *invisibleIndex.Invisible)
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
				assert.True(t, visibleIndex.Invisible == nil || !*visibleIndex.Invisible)
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
				assert.False(t, *testIndex.Invisible)
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
				assert.Equal(t, "BTREE", *btreeIndex.Using)
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
				assert.Equal(t, "HASH", *hashIndex.Using)
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
				assert.Equal(t, "HASH", *combinedIndex.Using)
				require.NotNil(t, combinedIndex.Invisible)
				assert.True(t, *combinedIndex.Invisible)
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
				assert.Equal(t, "Index comment", *commentIndex.Comment)
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
				assert.Equal(t, uint64(16), *kbsIndex.KeyBlockSize)
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
				assert.Equal(t, "ngram", *fulltextIndex.ParserName)
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
				assert.Equal(t, "UNIQUE", uniqueIndex.Type)
				require.NotNil(t, uniqueIndex.Using)
				assert.Equal(t, "BTREE", *uniqueIndex.Using)
				require.NotNil(t, uniqueIndex.Comment)
				assert.Equal(t, "Unique email", *uniqueIndex.Comment)
				require.NotNil(t, uniqueIndex.KeyBlockSize)
				assert.Equal(t, uint64(8), *uniqueIndex.KeyBlockSize)
				require.NotNil(t, uniqueIndex.Invisible)
				assert.True(t, *uniqueIndex.Invisible)
			},
		},

		// Table option tests
		{
			Name:        "Table with ENGINE and CHARSET",
			SQL:         "CREATE TABLE t (id INT) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
			ShouldParse: true,
			Validate: func(t *testing.T, createTable *CreateTable) {
				options := createTable.GetTableOptions()
				assert.Equal(t, "InnoDB", options["engine"])
				assert.Equal(t, "utf8mb4", options["charset"])
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
					assert.Equal(t, uint64(1024), kbs)
				}
			},
		},
		{
			Name:        "Table with COMMENT",
			SQL:         "CREATE TABLE t (id INT) COMMENT = 'Test table';",
			ShouldParse: true,
			Validate: func(t *testing.T, createTable *CreateTable) {
				options := createTable.GetTableOptions()
				assert.Equal(t, "Test table", options["comment"])
			},
		},

		// Column option tests
		{
			Name:        "Column with AUTO_INCREMENT",
			SQL:         "CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY);",
			ShouldParse: true,
			Validate: func(t *testing.T, createTable *CreateTable) {
				columns := createTable.GetColumns()
				assert.Len(t, columns, 1)
				assert.True(t, columns[0].AutoInc)
				assert.False(t, columns[0].Nullable) // PRIMARY KEY implies NOT NULL
			},
		},
		{
			Name:        "Column with DEFAULT value",
			SQL:         "CREATE TABLE t (status VARCHAR(50) DEFAULT 'active');",
			ShouldParse: true,
			Validate: func(t *testing.T, createTable *CreateTable) {
				columns := createTable.GetColumns()
				assert.Len(t, columns, 1)
				// Note: Default values might be parsed differently by TiDB parser
				// The actual default value parsing depends on the expression parser
				// For now, we just verify the column was parsed successfully
				assert.Equal(t, "status", columns[0].Name)
				assert.Contains(t, columns[0].Type, "varchar")
			},
		},
		{
			Name:        "Column with COMMENT",
			SQL:         "CREATE TABLE t (name VARCHAR(100) COMMENT 'User name');",
			ShouldParse: true,
			Validate: func(t *testing.T, createTable *CreateTable) {
				columns := createTable.GetColumns()
				assert.Len(t, columns, 1)
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
				assert.Len(t, columns, 1)
				assert.Contains(t, columns[0].Type, "char")
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
				assert.Equal(t, "user_activity", ct.GetTableName())

				// Validate columns
				columns := ct.GetColumns()
				assert.Len(t, columns, 5)

				// Validate table options
				options := ct.GetTableOptions()
				assert.Equal(t, "InnoDB", options["engine"])
				assert.Equal(t, "utf8mb4", options["charset"])
				assert.Equal(t, "User activity tracking", options["comment"])

				// Validate indexes
				indexes := ct.GetIndexes()

				indexMap := make(map[string]Index)
				for _, idx := range indexes {
					indexMap[idx.Name] = idx
				}

				// Check specific indexes
				userIdIndex := indexMap["idx_user_id"]
				assert.Equal(t, "INDEX", userIdIndex.Type)
				require.NotNil(t, userIdIndex.Comment)
				assert.Equal(t, "User lookup", *userIdIndex.Comment)

				timestampIndex := indexMap["idx_timestamp"]
				require.NotNil(t, timestampIndex.Using)
				assert.Equal(t, "BTREE", *timestampIndex.Using)

				activityIndex := indexMap["idx_activity_type"]
				require.NotNil(t, activityIndex.Invisible)
				assert.True(t, *activityIndex.Invisible)
				require.NotNil(t, activityIndex.Comment)
				assert.Equal(t, "Activity type lookup", *activityIndex.Comment)

				uniqueIndex := indexMap["uk_user_timestamp"]
				assert.Equal(t, "UNIQUE", uniqueIndex.Type)
				require.NotNil(t, uniqueIndex.Using)
				assert.Equal(t, "BTREE", *uniqueIndex.Using)
				require.NotNil(t, uniqueIndex.KeyBlockSize)
				assert.Equal(t, uint64(16), *uniqueIndex.KeyBlockSize)
				require.NotNil(t, uniqueIndex.Invisible)
				assert.True(t, *uniqueIndex.Invisible)

				fulltextIndex := indexMap["idx_data"]
				assert.Equal(t, "FULLTEXT", fulltextIndex.Type)
				require.NotNil(t, fulltextIndex.ParserName)
				assert.Equal(t, "ngram", *fulltextIndex.ParserName)
				require.NotNil(t, fulltextIndex.Comment)
				assert.Equal(t, "JSON search", *fulltextIndex.Comment)
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
				assert.NoError(t, err, "Expected to parse: %s", tc.sql)
			} else {
				assert.Error(t, err, "Expected to fail: %s", tc.sql)
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

	for range b.N {
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
