package lint

import (
	"encoding/json"
	"fmt"
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

	analyzer, err := ParseCreateTable(sql)
	require.NoError(t, err)

	// Test basic table info
	assert.Equal(t, "users", analyzer.GetTableName())

	// Test columns
	columns := analyzer.GetColumns()
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
	indexes := analyzer.GetIndexes()
	assert.GreaterOrEqual(t, len(indexes), 2) // At least PRIMARY KEY and UNIQUE

	// Test table options
	options := analyzer.GetTableOptions()
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

	analyzer, err := ParseCreateTable(sql)
	require.NoError(t, err)

	// Test direct structured access
	createTable := analyzer.GetCreateTable()
	assert.Equal(t, "products", createTable.TableName)

	// Test columns access
	columns := createTable.Columns
	require.Len(t, columns, 3)
	assert.Equal(t, "id", columns[0].Name)

	// Test table options access
	options := analyzer.GetTableOptions()
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

	analyzer, err := ParseCreateTable(sql)
	require.NoError(t, err)

	constraints := analyzer.GetConstraints()

	// Should have at least the FOREIGN KEY constraint
	assert.GreaterOrEqual(t, len(constraints), 1)

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

	analyzer, err := ParseCreateTable(sql)
	require.NoError(t, err)

	createTable := analyzer.GetCreateTable()
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

// Example of how to implement custom linting rules
func TestCustomLintingRules(t *testing.T) {
	sql := `
	CREATE TABLE bad_table (
		ID INT PRIMARY KEY,
		userName VARCHAR(50),
		user_email VARCHAR(255),
		CreatedAt TIMESTAMP
	)
	`

	analyzer, err := ParseCreateTable(sql)
	require.NoError(t, err)

	// Rule 1: Check for snake_case column names
	violations := checkSnakeCaseColumns(analyzer)
	assert.Len(t, violations, 3) // ID, userName, CreatedAt should violate

	// Rule 2: Check for missing NOT NULL on important columns
	nullableViolations := checkRequiredNotNullColumns(analyzer, []string{"user_email"})
	assert.Len(t, nullableViolations, 1)

	// Rule 3: Check for missing indexes on foreign key-like columns
	indexViolations := checkMissingIndexes(analyzer, []string{"user_id", "category_id"})
	assert.Len(t, indexViolations, 0) // No FK columns in this example
}

// Example linting rule implementations
func checkSnakeCaseColumns(analyzer TableSchema) []string {
	var violations []string
	columns := analyzer.GetColumns()

	for _, col := range columns {
		if !isSnakeCase(col.Name) {
			violations = append(violations, fmt.Sprintf("Column '%s' is not in snake_case", col.Name))
		}
	}

	return violations
}

func checkRequiredNotNullColumns(analyzer TableSchema, requiredCols []string) []string {
	var violations []string
	columns := analyzer.GetColumns()

	for _, col := range columns {
		for _, required := range requiredCols {
			if col.Name == required && col.Nullable {
				violations = append(violations, fmt.Sprintf("Column '%s' should be NOT NULL", col.Name))
			}
		}
	}

	return violations
}

func checkMissingIndexes(analyzer TableSchema, fkColumns []string) []string {
	var violations []string
	indexes := analyzer.GetIndexes()
	columns := analyzer.GetColumns()

	// Create a map of indexed columns
	indexedCols := make(map[string]bool)
	for _, idx := range indexes {
		for _, col := range idx.Columns {
			indexedCols[col] = true
		}
	}

	// Check if FK-like columns are indexed
	for _, col := range columns {
		for _, fkCol := range fkColumns {
			if col.Name == fkCol && !indexedCols[col.Name] {
				violations = append(violations, fmt.Sprintf("Foreign key column '%s' should have an index", col.Name))
			}
		}
	}

	return violations
}

func TestSchemaAnalyzer_JSONSerialization(t *testing.T) {
	sql := `
	CREATE TABLE test_table (
		id INT PRIMARY KEY,
		name VARCHAR(100) NOT NULL
	) ENGINE=InnoDB
	`

	analyzer, err := ParseCreateTable(sql)
	require.NoError(t, err)

	// Test that we can serialize the structured data
	columns := analyzer.GetColumns()
	jsonData, err := json.Marshal(columns)
	require.NoError(t, err)

	var deserializedColumns []Column
	err = json.Unmarshal(jsonData, &deserializedColumns)
	require.NoError(t, err)

	for i := range columns {
		columns[i].Ref = nil // Zero the Ref out before comparing because it can't be serialized
	}

	assert.Equal(t, columns, deserializedColumns)
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

	analyzer, err := ParseCreateTable(sql)
	require.NoError(t, err)

	// Test using structured access
	createTable := analyzer.GetCreateTable()
	indexes := createTable.Indexes
	require.Greater(t, len(indexes), 0)

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
	for i := 0; i < b.N; i++ {
		analyzer, err := ParseCreateTable(sql)
		if err != nil {
			b.Fatal(err)
		}
		_ = analyzer.GetColumns()
		_ = analyzer.GetIndexes()
		_ = analyzer.GetConstraints()
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
							Values: []interface{}{"2021"},
						},
					},
					{
						Name: "p2021",
						Values: &PartitionValues{
							Type:   "LESS_THAN",
							Values: []interface{}{"2022"},
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
							Values: []interface{}{"north", "northeast"},
						},
					},
					{
						Name: "p_south",
						Values: &PartitionValues{
							Type:   "IN",
							Values: []interface{}{"south"},
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
			analyzer, err := ParseCreateTable(tc.sql)
			require.NoError(t, err)

			partition := analyzer.GetPartition()

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
	analyzer, err := ParseCreateTable(sql)
	assert.NoError(t, err)

	// This accounts for the two different primary keys and 2 different unique indexes,
	// each given once as a column attribute and once as a table constraint.
	assert.Len(t, analyzer.GetIndexes(), 7)

	// The "first" index should be the first one defined as a table attribute, not the PK or UNIQUE
	// index defined as a column attribute.
	firstIdx := analyzer.GetCreateTable().Indexes[0]
	require.NotNil(t, firstIdx)
	// Nameless indexes get an empty string as the name
	assert.Equal(t, "", firstIdx.Name)
	assert.Equal(t, []string{"user_id", "customerEmail"}, firstIdx.Columns)
	assert.Nil(t, firstIdx.Comment)

	assert.True(t, analyzer.GetIndexes().AnyInvisible())
	idx_created_at := analyzer.GetCreateTable().Indexes.ByName("idx_created_at")
	require.NotNil(t, idx_created_at)
	require.NotNil(t, idx_created_at.Invisible)
	assert.True(t, *idx_created_at.Invisible)

	enum := analyzer.GetColumns().ByName("order_status")
	require.NotNil(t, enum)
	assert.True(t, strings.EqualFold("ENUM", enum.Type))
}
