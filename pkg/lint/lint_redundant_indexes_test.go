package lint

import (
	"strings"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/require"
)

func TestRedundantIndexLinter_Name(t *testing.T) {
	linter := &RedundantIndexLinter{}
	require.Equal(t, "redundant_indexes", linter.Name())
}

func TestRedundantIndexLinter_Description(t *testing.T) {
	linter := &RedundantIndexLinter{}
	require.NotEmpty(t, linter.Description())
}

func TestRedundantIndexLinter_PrefixRedundancy(t *testing.T) {
	tests := []struct {
		name           string
		createTable    string
		expectViolated bool
		violatedIndex  string
		coveringIndex  string
	}{
		{
			name: "index (a) redundant to index (a, b)",
			createTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				a INT,
				b INT,
				INDEX idx_a (a),
				INDEX idx_ab (a, b)
			)`,
			expectViolated: true,
			violatedIndex:  "idx_a",
			coveringIndex:  "idx_ab",
		},
		{
			name: "index (a, b) redundant to index (a, b, c)",
			createTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				a INT,
				b INT,
				c INT,
				INDEX idx_ab (a, b),
				INDEX idx_abc (a, b, c)
			)`,
			expectViolated: true,
			violatedIndex:  "idx_ab",
			coveringIndex:  "idx_abc",
		},
		{
			name: "index (a, b) NOT redundant to index (a, c)",
			createTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				a INT,
				b INT,
				c INT,
				INDEX idx_ab (a, b),
				INDEX idx_ac (a, c)
			)`,
			expectViolated: false,
		},
		{
			name: "index (a, b) NOT redundant to index (b, a)",
			createTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				a INT,
				b INT,
				INDEX idx_ab (a, b),
				INDEX idx_ba (b, a)
			)`,
			expectViolated: false,
		},
		{
			name: "index (a DESC) redundant to index (a DESC, b)",
			createTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				a INT,
				b INT,
				INDEX idx_a (a DESC),
				INDEX idx_ab (a DESC, b)
			)`,
			expectViolated: true,
			violatedIndex:  "idx_a",
			coveringIndex:  "idx_ab",
		},
		{
			name: "index (a) NOT redundant to index (a DESC, b)",
			createTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				a INT,
				b INT,
				INDEX idx_a (a),
				INDEX idx_ab (a DESC, b)
			)`,
			expectViolated: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			linter := &RedundantIndexLinter{}
			ct, err := statement.ParseCreateTable(tt.createTable)
			require.NoError(t, err)

			violations := linter.Lint([]*statement.CreateTable{ct}, nil)

			if tt.expectViolated {
				require.NotEmpty(t, violations, "Expected violations but got none")
				found := false
				for _, v := range violations {
					if v.Location != nil && v.Location.Index != nil && *v.Location.Index == tt.violatedIndex {
						found = true
						require.Contains(t, v.Message, tt.coveringIndex)
						require.Equal(t, SeverityWarning, v.Severity)
						break
					}
				}
				require.True(t, found, "Expected violation for index %s", tt.violatedIndex)
			} else {
				require.Empty(t, violations, "Expected no violations but got: %v", violations)
			}
		})
	}
}

// TestRedundantIndexLinter_RedundantToUniqueIndex tests a simplified integration
// case where an index is redundant to a UNIQUE index.
func TestRedundantIndexLinter_RedundantToUniqueIndex(t *testing.T) {
	createTable := `CREATE TABLE t1 (
    id bigint unsigned NOT NULL AUTO_INCREMENT,
    token varchar(191) NOT NULL,
    XYZ_id bigint unsigned NOT NULL,
    ZYX_id bigint unsigned NOT NULL,
    type varchar(50) NOT NULL,
    status varchar(50) NOT NULL,
    last_used_at timestamp NULL DEFAULT NULL,
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY (XYZ_id, ZYX_id, type),
    UNIQUE KEY (token),
    KEY idx_XYZ_id (XYZ_id),
    KEY idx_ZYX_id (ZYX_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;`
	linter := &RedundantIndexLinter{}
	ct, err := statement.ParseCreateTable(createTable)
	require.NoError(t, err)

	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	require.Len(t, violations, 1, "Expected one violation")
	require.Contains(t, violations[0].Message, `Index "idx_XYZ_id" on column "XYZ_id" is redundant`)
}

func TestRedundantIndexLinter_DuplicateIndexes(t *testing.T) {
	tests := []struct {
		name           string
		createTable    string
		expectViolated bool
		violatedIndex  string
	}{
		{
			name: "duplicate indexes (a, b)",
			createTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				a INT,
				b INT,
				INDEX idx1 (a, b),
				INDEX idx2 (a, b)
			)`,
			expectViolated: true,
			violatedIndex:  "idx2",
		},
		{
			name: "duplicate single column indexes",
			createTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				a INT,
				INDEX idx1 (a),
				INDEX idx2 (a)
			)`,
			expectViolated: true,
			violatedIndex:  "idx2",
		},
		{
			name: "duplicate UNIQUE indexes",
			createTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				a INT,
				b INT,
				UNIQUE idx1 (a, b),
				UNIQUE idx2 (a, b)
			)`,
			expectViolated: true,
			violatedIndex:  "idx2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			linter := &RedundantIndexLinter{}
			ct, err := statement.ParseCreateTable(tt.createTable)
			require.NoError(t, err)

			violations := linter.Lint([]*statement.CreateTable{ct}, nil)

			if tt.expectViolated {
				require.NotEmpty(t, violations, "Expected violations but got none")
				found := false
				for _, v := range violations {
					if v.Location != nil && v.Location.Index != nil && *v.Location.Index == tt.violatedIndex {
						found = true
						require.Contains(t, v.Message, "duplicate")
						require.Equal(t, SeverityWarning, v.Severity)
						break
					}
				}
				require.True(t, found, "Expected violation for index %s", tt.violatedIndex)
			} else {
				require.Empty(t, violations, "Expected no violations but got: %v", violations)
			}
		})
	}
}

func TestRedundantIndexLinter_RedundantToPrimaryKey(t *testing.T) {
	tests := []struct {
		name           string
		createTable    string
		expectViolated bool
		violatedIndex  string
		isDuplicate    bool
	}{
		{
			name: "index (a) redundant to PK (a)",
			createTable: `CREATE TABLE t1 (
				a INT PRIMARY KEY,
				INDEX idx_a (a)
			)`,
			expectViolated: true,
			violatedIndex:  "idx_a",
			isDuplicate:    true,
		},
		{
			name: "index (a) redundant to PK (a, b)",
			createTable: `CREATE TABLE t1 (
				a INT,
				b INT,
				PRIMARY KEY (a, b),
				INDEX idx_a (a)
			)`,
			expectViolated: true,
			violatedIndex:  "idx_a",
			isDuplicate:    false,
		},
		{
			name: "index (a, b) redundant to PK (a, b, c)",
			createTable: `CREATE TABLE t1 (
				a INT,
				b INT,
				c INT,
				PRIMARY KEY (a, b, c),
				INDEX idx_ab (a, b)
			)`,
			expectViolated: true,
			violatedIndex:  "idx_ab",
			isDuplicate:    false,
		},
		{
			name: "index (a, c) NOT redundant to PK (a, b)",
			createTable: `CREATE TABLE t1 (
				a INT,
				b INT,
				c INT,
				PRIMARY KEY (a, b),
				INDEX idx_ac (a, c)
			)`,
			expectViolated: false,
		},
		{
			name: "index (b) NOT redundant to PK (a, b)",
			createTable: `CREATE TABLE t1 (
				a INT,
				b INT,
				PRIMARY KEY (a, b),
				INDEX idx_b (b)
			)`,
			expectViolated: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			linter := &RedundantIndexLinter{}
			ct, err := statement.ParseCreateTable(tt.createTable)
			require.NoError(t, err)

			violations := linter.Lint([]*statement.CreateTable{ct}, nil)

			if tt.expectViolated {
				require.NotEmpty(t, violations, "Expected violations but got none")
				found := false
				for _, v := range violations {
					if v.Location != nil && v.Location.Index != nil && *v.Location.Index == tt.violatedIndex {
						found = true
						require.Contains(t, v.Message, "PRIMARY KEY")
						require.Equal(t, SeverityWarning, v.Severity)
						if tt.isDuplicate {
							require.Contains(t, v.Message, "duplicate")
						}
						break
					}
				}
				require.True(t, found, "Expected violation for index %s", tt.violatedIndex)
			} else {
				require.Empty(t, violations, "Expected no violations but got: %v", violations)
			}
		})
	}
}

func TestRedundantIndexLinter_PKSuffixRedundancy(t *testing.T) {
	tests := []struct {
		name              string
		createTable       string
		expectViolated    bool
		violatedIndex     string
		redundantColCount int
	}{
		{
			name: "index (b, c, a) has redundant PK suffix (a)",
			createTable: `CREATE TABLE t1 (
				a INT PRIMARY KEY,
				b INT,
				c INT,
				INDEX idx (b, c, a)
			)`,
			expectViolated:    true,
			violatedIndex:     "idx",
			redundantColCount: 1,
		},
		{
			name: "index (c, a, b) has redundant PK suffix (a, b)",
			createTable: `CREATE TABLE t1 (
				a INT,
				b INT,
				c INT,
				PRIMARY KEY (a, b),
				INDEX idx (c, a, b)
			)`,
			expectViolated:    true,
			violatedIndex:     "idx",
			redundantColCount: 2,
		},
		{
			name: "index (c, d, a) has redundant PK prefix suffix (a) when PK is (a, b)",
			createTable: `CREATE TABLE t1 (
				a INT,
				b INT,
				c INT,
				d INT,
				PRIMARY KEY (a, b),
				INDEX idx (c, d, a)
			)`,
			expectViolated:    true,
			violatedIndex:     "idx",
			redundantColCount: 1,
		},
		{
			name: "index (x, y, a, b, c) has redundant PK suffix (a, b, c)",
			createTable: `CREATE TABLE t1 (
				a INT,
				b INT,
				c INT,
				x INT,
				y INT,
				PRIMARY KEY (a, b, c),
				INDEX idx (x, y, a, b, c)
			)`,
			expectViolated:    true,
			violatedIndex:     "idx",
			redundantColCount: 3,
		},
		{
			name: "index (c, b) does NOT have redundant PK suffix when PK is (a, b)",
			createTable: `CREATE TABLE t1 (
				a INT,
				b INT,
				c INT,
				PRIMARY KEY (a, b),
				INDEX idx (c, b)
			)`,
			expectViolated: false,
		},
		{
			name: "index (c, b, a) does NOT have redundant PK suffix when PK is (a, b)",
			createTable: `CREATE TABLE t1 (
				a INT,
				b INT,
				c INT,
				PRIMARY KEY (a, b),
				INDEX idx (c, b, a)
			)`,
			expectViolated: false,
		},
		{
			name: "index (a, b) does NOT have suffix when it's same length as PK",
			createTable: `CREATE TABLE t1 (
				a INT,
				b INT,
				PRIMARY KEY (a, b),
				INDEX idx (a, b)
			)`,
			expectViolated: false, // This is a duplicate, not a suffix issue
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			linter := &RedundantIndexLinter{}
			ct, err := statement.ParseCreateTable(tt.createTable)
			require.NoError(t, err)

			violations := linter.Lint([]*statement.CreateTable{ct}, nil)

			if tt.expectViolated {
				found := false
				for _, v := range violations {
					if v.Location != nil && v.Location.Index != nil && *v.Location.Index == tt.violatedIndex {
						if v.Context != nil {
							if colCount, ok := v.Context["redundant_col_count"]; ok && colCount == tt.redundantColCount {
								found = true
								require.Contains(t, v.Message, "redundant PRIMARY KEY")
								require.Contains(t, v.Message, "suffix")
								require.Equal(t, SeverityWarning, v.Severity)
								break
							}
						}
					}
				}
				require.True(t, found, "Expected PK suffix violation for index %s with %d redundant columns", tt.violatedIndex, tt.redundantColCount)
			} else {
				// Check that there's no PK suffix violation for this index
				for _, v := range violations {
					if v.Location != nil && v.Location.Index != nil && *v.Location.Index == tt.violatedIndex {
						require.NotContains(t, v.Message, "suffix", "Should not have suffix violation")
					}
				}
			}
		})
	}
}

func TestRedundantIndexLinter_TypeCompatibility(t *testing.T) {
	tests := []struct {
		name           string
		createTable    string
		expectViolated bool
		violatedIndex  string
	}{
		{
			name: "INDEX (a) redundant to UNIQUE (a, b)",
			createTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				a INT,
				b INT,
				INDEX idx_a (a),
				UNIQUE idx_ab (a, b)
			)`,
			expectViolated: true,
			violatedIndex:  "idx_a",
		},
		{
			name: "UNIQUE (a) NOT redundant to INDEX (a, b)",
			createTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				a INT,
				b INT,
				UNIQUE idx_a (a),
				INDEX idx_ab (a, b)
			)`,
			expectViolated: false,
		},
		{
			// UNIQUE (a) and UNIQUE (a, b) enforce different uniqueness scopes:
			// the former forbids duplicate values of `a`, the latter only forbids
			// duplicate (a, b) pairs. Dropping UNIQUE (a) would weaken the schema.
			name: "UNIQUE (a) NOT redundant to UNIQUE (a, b)",
			createTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				a INT,
				b INT,
				UNIQUE idx_a (a),
				UNIQUE idx_ab (a, b)
			)`,
			expectViolated: false,
			violatedIndex:  "idx_a",
		},
		{
			// PRIMARY KEY (a, b) only forbids duplicate (a, b) pairs; it does
			// not subsume the stricter UNIQUE (a) constraint.
			name: "UNIQUE (a) NOT redundant to PK (a, b)",
			createTable: `CREATE TABLE t1 (
				a INT,
				b INT,
				PRIMARY KEY (a, b),
				UNIQUE idx_a (a)
			)`,
			expectViolated: false,
			violatedIndex:  "idx_a",
		},
		{
			name: "FULLTEXT index NOT considered redundant",
			createTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				title TEXT,
				INDEX idx_title (title(100)),
				FULLTEXT idx_ft (title)
			)`,
			expectViolated: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			linter := &RedundantIndexLinter{}
			ct, err := statement.ParseCreateTable(tt.createTable)
			require.NoError(t, err)

			violations := linter.Lint([]*statement.CreateTable{ct}, nil)

			if tt.expectViolated {
				found := false
				for _, v := range violations {
					if v.Location != nil && v.Location.Index != nil && *v.Location.Index == tt.violatedIndex {
						found = true
						break
					}
				}
				require.True(t, found, "Expected violation for index %s", tt.violatedIndex)
			} else {
				for _, v := range violations {
					if v.Location != nil && v.Location.Index != nil {
						require.NotEqual(t, tt.violatedIndex, *v.Location.Index, "Should not have violation for %s", tt.violatedIndex)
					}
				}
			}
		})
	}
}

func TestRedundantIndexLinter_MultipleViolations(t *testing.T) {
	createTable := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		a INT,
		b INT,
		c INT,
		INDEX idx_a (a),
		INDEX idx_ab (a, b),
		INDEX idx_abc (a, b, c),
		INDEX idx_dup (a, b),
		INDEX idx_suffix (c, id)
	)`

	linter := &RedundantIndexLinter{}
	ct, err := statement.ParseCreateTable(createTable)
	require.NoError(t, err)

	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// Should detect:
	// 1. idx_a redundant to idx_ab
	// 2. idx_ab redundant to idx_abc
	// 3. idx_dup redundant to idx_abc (or idx_ab)
	// 4. idx_suffix has redundant PK suffix
	require.GreaterOrEqual(t, len(violations), 4, "Expected at least 4 violations")

	violatedIndexes := make(map[string]bool)
	for _, v := range violations {
		if v.Location != nil && v.Location.Index != nil {
			violatedIndexes[*v.Location.Index] = true
		}
	}

	require.True(t, violatedIndexes["idx_a"], "idx_a should be flagged")
	require.True(t, violatedIndexes["idx_ab"], "idx_ab should be flagged")
	require.True(t, violatedIndexes["idx_dup"], "idx_dup should be flagged")
	require.True(t, violatedIndexes["idx_suffix"], "idx_suffix should be flagged")
}

func TestRedundantIndexLinter_NoViolations(t *testing.T) {
	tests := []struct {
		name        string
		createTable string
	}{
		{
			name: "no redundant indexes",
			createTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				a INT,
				b INT,
				c INT,
				INDEX idx_a (a),
				INDEX idx_b (b),
				INDEX idx_c (c)
			)`,
		},
		{
			name: "different column orders",
			createTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				a INT,
				b INT,
				INDEX idx_ab (a, b),
				INDEX idx_ba (b, a)
			)`,
		},
		{
			name: "no indexes",
			createTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				a INT,
				b INT
			)`,
		},
		{
			name: "only primary key",
			createTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY
			)`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			linter := &RedundantIndexLinter{}
			ct, err := statement.ParseCreateTable(tt.createTable)
			require.NoError(t, err)

			violations := linter.Lint([]*statement.CreateTable{ct}, nil)
			require.Empty(t, violations, "Expected no violations but got: %v", violations)
		})
	}
}

func TestRedundantIndexLinter_CompositePrimaryKey(t *testing.T) {
	createTable := `CREATE TABLE t1 (
		tenant_id INT,
		user_id INT,
		email VARCHAR(255),
		created_at TIMESTAMP,
		PRIMARY KEY (tenant_id, user_id),
		INDEX idx_tenant (tenant_id),
		INDEX idx_email (email),
		INDEX idx_email_tenant_user (email, tenant_id, user_id),
		INDEX idx_created (created_at, tenant_id, user_id)
	)`

	linter := &RedundantIndexLinter{}
	ct, err := statement.ParseCreateTable(createTable)
	require.NoError(t, err)

	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	violatedIndexes := make(map[string]string)
	for _, v := range violations {
		if v.Location != nil && v.Location.Index != nil {
			violatedIndexes[*v.Location.Index] = v.Message
		}
	}

	// idx_tenant should be redundant to PK (tenant_id, user_id)
	require.Contains(t, violatedIndexes, "idx_tenant", "idx_tenant should be redundant to PK")

	// idx_email_tenant_user should have redundant PK suffix
	require.Contains(t, violatedIndexes, "idx_email_tenant_user", "idx_email_tenant_user should have redundant suffix")
	require.Contains(t, violatedIndexes["idx_email_tenant_user"], "suffix")

	// idx_email should be redundant to idx_email_tenant_user (prefix match)
	require.Contains(t, violatedIndexes, "idx_email", "idx_email should be redundant to idx_email_tenant_user")

	// idx_created should have redundant PK suffix (tenant_id, user_id)
	require.Contains(t, violatedIndexes, "idx_created", "idx_created should have redundant suffix")
	require.Contains(t, violatedIndexes["idx_created"], "suffix")
}

// TestRedundantIndexLinter_CreateTableInChanges tests that the linter
// correctly detects redundant indexes in CREATE TABLE statements passed as changes.
// This is a regression test for the bug where CREATE TABLE statements in changes
// were not being linted.
func TestRedundantIndexLinter_CreateTableInChanges(t *testing.T) {
	createTableSQL := `CREATE TABLE bankaccount_capability (
		id bigint unsigned NOT NULL AUTO_INCREMENT,
		token varchar(191) NOT NULL,
		bank_account_id bigint unsigned NOT NULL,
		transfer_instruction_route_id bigint unsigned NOT NULL,
		type varchar(50) NOT NULL,
		status varchar(50) NOT NULL,
		last_used_at timestamp NULL DEFAULT NULL,
		created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		PRIMARY KEY (id),
		UNIQUE KEY (bank_account_id, transfer_instruction_route_id, type),
		UNIQUE KEY (token),
		KEY idx_bank_account_id (bank_account_id),
		KEY idx_transfer_instruction_route_id (transfer_instruction_route_id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;`

	linter := &RedundantIndexLinter{}

	// Parse as a change (AbstractStatement)
	abstractStmt, err := statement.New(createTableSQL)
	require.NoError(t, err)
	require.Len(t, abstractStmt, 1)

	// Lint with the CREATE TABLE as a change (not an existing table)
	violations := linter.Lint(nil, abstractStmt)

	// Should detect idx_bank_account_id as redundant to the UNIQUE KEY
	require.Len(t, violations, 1, "Expected one violation for redundant index")
	require.Contains(t, violations[0].Message, "idx_bank_account_id")
	require.Contains(t, violations[0].Message, "redundant")
}

// TestRedundantIndexLinter_AlterTableAddRedundantIndex tests that the linter
// detects redundant indexes being added via ALTER TABLE statements.
func TestRedundantIndexLinter_AlterTableAddRedundantIndex(t *testing.T) {
	tests := []struct {
		name            string
		existingTable   string
		alterSQL        string
		expectViolated  bool
		violatedIndex   string
		messageContains string
	}{
		{
			name: "ADD INDEX redundant to existing index",
			existingTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				a INT,
				b INT,
				INDEX idx_ab (a, b)
			)`,
			alterSQL:        "ALTER TABLE t1 ADD INDEX idx_a (a)",
			expectViolated:  true,
			violatedIndex:   "idx_a",
			messageContains: "redundant",
		},
		{
			name: "ADD INDEX redundant to PRIMARY KEY",
			existingTable: `CREATE TABLE t1 (
				a INT,
				b INT,
				PRIMARY KEY (a, b)
			)`,
			alterSQL:        "ALTER TABLE t1 ADD INDEX idx_a (a)",
			expectViolated:  true,
			violatedIndex:   "idx_a",
			messageContains: "PRIMARY KEY",
		},
		{
			// Adding UNIQUE (a) alongside UNIQUE (a, b) is *not* redundant —
			// the two enforce different uniqueness scopes.
			name: "ADD UNIQUE NOT redundant to wider existing UNIQUE",
			existingTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				a INT,
				b INT,
				UNIQUE KEY idx_ab (a, b)
			)`,
			alterSQL:       "ALTER TABLE t1 ADD UNIQUE KEY idx_a (a)",
			expectViolated: false,
		},
		{
			name: "ADD UNIQUE duplicate of existing UNIQUE",
			existingTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				a INT,
				b INT,
				UNIQUE KEY idx_ab (a, b)
			)`,
			alterSQL:        "ALTER TABLE t1 ADD UNIQUE KEY idx_ab_dup (a, b)",
			expectViolated:  true,
			violatedIndex:   "idx_ab_dup",
			messageContains: "duplicate",
		},
		{
			name: "ADD INDEX with PK suffix redundancy",
			existingTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				a INT,
				b INT
			)`,
			alterSQL:        "ALTER TABLE t1 ADD INDEX idx_ab_id (a, b, id)",
			expectViolated:  true,
			violatedIndex:   "idx_ab_id",
			messageContains: "redundant PRIMARY KEY suffix",
		},
		{
			name: "ADD INDEX not redundant",
			existingTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				a INT,
				b INT,
				INDEX idx_a (a)
			)`,
			alterSQL:       "ALTER TABLE t1 ADD INDEX idx_b (b)",
			expectViolated: false,
		},
		{
			name: "ADD INDEX duplicate of existing",
			existingTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				a INT,
				b INT,
				INDEX idx_ab (a, b)
			)`,
			alterSQL:        "ALTER TABLE t1 ADD INDEX idx_ab_dup (a, b)",
			expectViolated:  true,
			violatedIndex:   "idx_ab_dup",
			messageContains: "duplicate",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			linter := &RedundantIndexLinter{}

			// Parse existing table
			existingCT, err := statement.ParseCreateTable(tt.existingTable)
			require.NoError(t, err)

			// Parse ALTER TABLE statement
			alterStmt, err := statement.New(tt.alterSQL)
			require.NoError(t, err)
			require.Len(t, alterStmt, 1)

			// Run linter
			violations := linter.Lint([]*statement.CreateTable{existingCT}, alterStmt)

			if tt.expectViolated {
				require.NotEmpty(t, violations, "Expected violations but got none")
				found := false
				for _, v := range violations {
					if v.Location != nil && v.Location.Index != nil && *v.Location.Index == tt.violatedIndex {
						found = true
						if tt.messageContains != "" {
							require.Contains(t, v.Message, tt.messageContains)
						}
						require.Equal(t, SeverityWarning, v.Severity)
						break
					}
				}
				require.True(t, found, "Expected violation for index %s", tt.violatedIndex)
			} else {
				require.Empty(t, violations, "Expected no violations but got: %v", violations)
			}
		})
	}
}

// TestRedundantIndexLinter_AlterTableMultipleIndexes tests ALTER TABLE
// statements that add multiple indexes.
func TestRedundantIndexLinter_AlterTableMultipleIndexes(t *testing.T) {
	existingTable := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		a INT,
		b INT,
		c INT,
		INDEX idx_abc (a, b, c)
	)`

	// Add two indexes: one redundant, one not
	alterSQL := `ALTER TABLE t1 
		ADD INDEX idx_a (a),
		ADD INDEX idx_d (c)`

	linter := &RedundantIndexLinter{}

	existingCT, err := statement.ParseCreateTable(existingTable)
	require.NoError(t, err)

	// Parse ALTER TABLE - note this creates separate statements for each ADD INDEX
	alterStmts, err := statement.New(alterSQL)
	require.NoError(t, err)

	violations := linter.Lint([]*statement.CreateTable{existingCT}, alterStmts)

	// Should detect idx_a as redundant but not idx_d
	violatedIndexes := make(map[string]bool)
	for _, v := range violations {
		if v.Location != nil && v.Location.Index != nil {
			violatedIndexes[*v.Location.Index] = true
		}
	}

	require.True(t, violatedIndexes["idx_a"], "idx_a should be flagged as redundant")
	require.False(t, violatedIndexes["idx_d"], "idx_d should not be flagged")
}

// TestRedundantIndexLinter_AlterTableOnNonExistentTable tests that the linter
// gracefully handles ALTER TABLE on tables that don't exist in the schema.
func TestRedundantIndexLinter_AlterTableOnNonExistentTable(t *testing.T) {
	linter := &RedundantIndexLinter{}

	alterSQL := "ALTER TABLE nonexistent ADD INDEX idx_a (a)"
	alterStmt, err := statement.New(alterSQL)
	require.NoError(t, err)

	// Run linter with no existing tables
	violations := linter.Lint(nil, alterStmt)

	// Should not crash and should return no violations
	require.Empty(t, violations)
}

// TestRedundantIndexLinter_PKNotFlaggedAsRedundant is a regression test for
// two issues reported together:
//
//  1. A secondary index whose leading columns match the PRIMARY KEY caused
//     the PRIMARY KEY itself to be reported as redundant. The PRIMARY KEY is
//     a constraint (clustered key + uniqueness + NOT NULL) and cannot be
//     dropped in favor of another index, so it must never be flagged.
//  2. The leading-PK secondary index itself should be flagged as redundant —
//     in InnoDB the PK is the clustered key and secondary indexes auto-append
//     PK columns, so a leading PK column contributes no lookup capability
//     that the PK or a non-PK-leading variant of the index wouldn't already
//     provide.
//
// Scenario: a table is left with both PRIMARY KEY (id) and a secondary index
// (id, parent_id, created_at). The linter should fire on the secondary index
// (not on the PRIMARY KEY), and only when the redundant index is present in
// the post-state.
func TestRedundantIndexLinter_PKNotFlaggedAsRedundant(t *testing.T) {
	createTable := `CREATE TABLE t1 (
		id INT NOT NULL AUTO_INCREMENT,
		parent_id INT NOT NULL,
		payload LONGTEXT,
		note TEXT,
		created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
		PRIMARY KEY (id),
		KEY id_parent_created (id, parent_id, created_at),
		KEY parent_id (parent_id),
		KEY created_at (created_at)
	)`

	linter := &RedundantIndexLinter{}
	ct, err := statement.ParseCreateTable(createTable)
	require.NoError(t, err)

	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	flagged := make(map[string]string)
	for _, v := range violations {
		if v.Location == nil || v.Location.Index == nil {
			continue
		}
		require.NotEqual(t, "PRIMARY", *v.Location.Index,
			"PRIMARY KEY must never be flagged as redundant; got: %s", v.Message)
		flagged[*v.Location.Index] = v.Message
	}

	require.Contains(t, flagged, "id_parent_created",
		"leading-PK secondary index should be flagged as redundant")
	require.Contains(t, flagged["id_parent_created"], "leads with PRIMARY KEY")
}

// TestRedundantIndexLinter_PKPrefixRedundancy covers the leading-PK rule.
func TestRedundantIndexLinter_PKPrefixRedundancy(t *testing.T) {
	tests := []struct {
		name           string
		createTable    string
		expectViolated bool
		violatedIndex  string
	}{
		{
			name: "INDEX (id, a, b) leading single-column PK is redundant",
			createTable: `CREATE TABLE t1 (
				id INT PRIMARY KEY,
				a INT,
				b INT,
				INDEX idx (id, a, b)
			)`,
			expectViolated: true,
			violatedIndex:  "idx",
		},
		{
			name: "INDEX (a, b, c) leading composite PK is redundant",
			createTable: `CREATE TABLE t1 (
				a INT,
				b INT,
				c INT,
				PRIMARY KEY (a, b),
				INDEX idx (a, b, c)
			)`,
			expectViolated: true,
			violatedIndex:  "idx",
		},
		{
			name: "INDEX (a, c) with PK (a, b) — only partial PK at start, not flagged",
			createTable: `CREATE TABLE t1 (
				a INT,
				b INT,
				c INT,
				PRIMARY KEY (a, b),
				INDEX idx (a, c)
			)`,
			expectViolated: false,
		},
		{
			name: "INDEX (a, b) with PK (a, b) is a duplicate, not a PK-prefix violation",
			createTable: `CREATE TABLE t1 (
				a INT,
				b INT,
				PRIMARY KEY (a, b),
				INDEX idx (a, b)
			)`,
			expectViolated: false, // duplicate-of-PK rule fires instead
		},
		{
			name: "INDEX (b, a, c) with PK (a, b) — different order, not flagged",
			createTable: `CREATE TABLE t1 (
				a INT,
				b INT,
				c INT,
				PRIMARY KEY (a, b),
				INDEX idx (b, a, c)
			)`,
			expectViolated: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			linter := &RedundantIndexLinter{}
			ct, err := statement.ParseCreateTable(tt.createTable)
			require.NoError(t, err)

			violations := linter.Lint([]*statement.CreateTable{ct}, nil)

			found := false
			for _, v := range violations {
				if v.Location != nil && v.Location.Index != nil && *v.Location.Index == tt.violatedIndex {
					if strings.Contains(v.Message, "leads with PRIMARY KEY") {
						found = true
						break
					}
				}
			}
			if tt.expectViolated {
				require.True(t, found, "Expected PK-prefix violation for index %s", tt.violatedIndex)
			} else {
				require.False(t, found, "Did not expect PK-prefix violation, got violations: %v", violations)
			}
		})
	}
}

// TestRedundantIndexLinter_PostStateEvaluation verifies that the linter
// evaluates the post-state of the schema (existing tables with pending
// changes applied), matching the convention from #840. ADD INDEX should
// surface redundancies as they are introduced, and DROP INDEX should
// silence redundancies that are being removed.
func TestRedundantIndexLinter_PostStateEvaluation(t *testing.T) {
	existing := `CREATE TABLE t1 (
		id INT NOT NULL AUTO_INCREMENT,
		parent_id INT NOT NULL,
		created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
		PRIMARY KEY (id),
		KEY id_parent_created (id, parent_id, created_at),
		KEY parent_id (parent_id),
		KEY created_at (created_at)
	)`

	existingCT, err := statement.ParseCreateTable(existing)
	require.NoError(t, err)
	linter := &RedundantIndexLinter{}

	// With no pending changes, the redundant index in the table should be
	// flagged.
	violations := linter.Lint([]*statement.CreateTable{existingCT}, nil)
	flagged := false
	for _, v := range violations {
		if v.Location != nil && v.Location.Index != nil && *v.Location.Index == "id_parent_created" {
			flagged = true
		}
	}
	require.True(t, flagged, "should flag the redundant index in pre-state when no DROP is pending")

	// Pending ALTER DROP INDEX should remove the redundant index from the
	// post-state, silencing the warning.
	dropStmts, err := statement.New("ALTER TABLE t1 DROP INDEX id_parent_created")
	require.NoError(t, err)

	violations = linter.Lint([]*statement.CreateTable{existingCT}, dropStmts)
	for _, v := range violations {
		if v.Location != nil && v.Location.Index != nil {
			require.NotEqual(t, "id_parent_created", *v.Location.Index,
				"DROP INDEX should silence the warning, got: %s", v.Message)
		}
	}

	// Inverse: starting from a clean table, ALTER ADD INDEX that introduces a
	// leading-PK index should surface the warning at ADD time.
	cleanExisting := `CREATE TABLE t1 (
		id INT NOT NULL AUTO_INCREMENT,
		parent_id INT NOT NULL,
		created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
		PRIMARY KEY (id),
		KEY parent_id (parent_id),
		KEY created_at (created_at)
	)`
	cleanCT, err := statement.ParseCreateTable(cleanExisting)
	require.NoError(t, err)

	addStmts, err := statement.New("ALTER TABLE t1 ADD INDEX id_parent_created (id, parent_id, created_at)")
	require.NoError(t, err)

	violations = linter.Lint([]*statement.CreateTable{cleanCT}, addStmts)
	flaggedAtAdd := false
	for _, v := range violations {
		if v.Location != nil && v.Location.Index != nil && *v.Location.Index == "id_parent_created" {
			flaggedAtAdd = true
		}
	}
	require.True(t, flaggedAtAdd, "ADD INDEX should surface the leading-PK warning at the time the index is introduced")
}

// indexViolated is a small helper that reports whether the linter flagged
// indexName, and returns the matching message when it did.
func indexViolated(violations []Violation, indexName string) (bool, string) {
	for _, v := range violations {
		if v.Location != nil && v.Location.Index != nil && *v.Location.Index == indexName {
			return true, v.Message
		}
	}
	return false, ""
}

// TestRedundantIndexLinter_UniquePartialPKSuffix is a regression test for the
// bug where a UNIQUE index whose trailing columns happened to be a (partial)
// prefix of the PRIMARY KEY was flagged with a redundant-PK-suffix warning. The
// suggested rewrite would have dropped real uniqueness columns from the index.
//
// A FULL-PK suffix on a UNIQUE index remains flagged (uniqueness is already
// guaranteed by the PK, so the spelled-out suffix is vacuous), and non-unique
// indexes with a partial-PK suffix are still flagged.
func TestRedundantIndexLinter_UniquePartialPKSuffix(t *testing.T) {
	tests := []struct {
		name           string
		createTable    string
		index          string
		expectViolated bool
	}{
		{
			// UNIQUE (x, y, a) is NOT implied by PK (a, b). The trailing `a` is
			// part of the uniqueness scope, not redundant PK bookkeeping.
			name: "UNIQUE with partial-PK suffix NOT flagged",
			createTable: `CREATE TABLE t1 (
				a INT, b INT, x INT, y INT,
				PRIMARY KEY (a, b),
				UNIQUE KEY ux (x, y, a)
			)`,
			index:          "ux",
			expectViolated: false,
		},
		{
			// UNIQUE (x, a, b) ends with the FULL PK (a, b). Uniqueness over
			// (x, a, b) is already guaranteed because (a, b) alone is unique, so
			// spelling out the PK suffix is vacuous and is still flagged.
			name: "UNIQUE with full-PK suffix still flagged",
			createTable: `CREATE TABLE t1 (
				a INT, b INT, x INT,
				PRIMARY KEY (a, b),
				UNIQUE KEY ux (x, a, b)
			)`,
			index:          "ux",
			expectViolated: true,
		},
		{
			// Non-unique INDEX (x, y, a) with PK (a, b): partial-PK suffix `a`
			// is genuinely redundant (InnoDB auto-appends the PK), so still flag.
			name: "non-unique with partial-PK suffix still flagged",
			createTable: `CREATE TABLE t1 (
				a INT, b INT, x INT, y INT,
				PRIMARY KEY (a, b),
				INDEX idx (x, y, a)
			)`,
			index:          "idx",
			expectViolated: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			linter := &RedundantIndexLinter{}
			ct, err := statement.ParseCreateTable(tt.createTable)
			require.NoError(t, err)

			violations := linter.Lint([]*statement.CreateTable{ct}, nil)

			violated, msg := indexViolated(violations, tt.index)
			if tt.expectViolated {
				require.True(t, violated, "expected %s to be flagged, violations: %v", tt.index, violations)
				require.Contains(t, msg, "suffix")
			} else {
				require.False(t, violated, "expected %s NOT to be flagged, got: %s", tt.index, msg)
			}
		})
	}
}

// TestRedundantIndexLinter_FunctionalIndexes is a regression test for the bug
// where functional (expression) indexes were treated as zero-column indexes
// (because the deprecated Index.Columns slice drops expression key parts) and
// were therefore always reported as redundant to any ordinary index.
func TestRedundantIndexLinter_FunctionalIndexes(t *testing.T) {
	t.Run("functional index NOT redundant to ordinary index", func(t *testing.T) {
		createTable := `CREATE TABLE t1 (
			id INT PRIMARY KEY,
			name VARCHAR(255),
			INDEX idx_name (name),
			INDEX functional_index ((LOWER(name)))
		)`
		linter := &RedundantIndexLinter{}
		ct, err := statement.ParseCreateTable(createTable)
		require.NoError(t, err)

		violations := linter.Lint([]*statement.CreateTable{ct}, nil)
		require.Empty(t, violations, "expression index must not be redundant to an ordinary column index: %v", violations)
	})

	t.Run("two identical functional indexes flagged as duplicates", func(t *testing.T) {
		createTable := `CREATE TABLE t1 (
			id INT PRIMARY KEY,
			name VARCHAR(255),
			INDEX f1 ((LOWER(name))),
			INDEX f2 ((LOWER(name)))
		)`
		linter := &RedundantIndexLinter{}
		ct, err := statement.ParseCreateTable(createTable)
		require.NoError(t, err)

		violations := linter.Lint([]*statement.CreateTable{ct}, nil)
		violated, msg := indexViolated(violations, "f2")
		require.True(t, violated, "identical functional indexes should be flagged as duplicates, violations: %v", violations)
		require.Contains(t, msg, "duplicate")
	})

	t.Run("functional index redundant to same-expression-plus-more", func(t *testing.T) {
		// INDEX ((LOWER(name))) is a coverable prefix of
		// INDEX ((LOWER(name)), other): the leading expression key part is
		// identical, so the shorter index is redundant to the longer one. The
		// trailing column is deliberately NOT a PRIMARY KEY column so the only
		// possible finding is the prefix-redundancy we are testing.
		createTable := `CREATE TABLE t1 (
			id INT PRIMARY KEY,
			name VARCHAR(255),
			other INT,
			INDEX f_short ((LOWER(name))),
			INDEX f_long ((LOWER(name)), other)
		)`
		linter := &RedundantIndexLinter{}
		ct, err := statement.ParseCreateTable(createTable)
		require.NoError(t, err)

		violations := linter.Lint([]*statement.CreateTable{ct}, nil)
		shortViolated, shortMsg := indexViolated(violations, "f_short")
		longViolated, _ := indexViolated(violations, "f_long")
		require.True(t, shortViolated, "f_short should be redundant to f_long, violations: %v", violations)
		require.Contains(t, shortMsg, "f_long")
		require.False(t, longViolated, "f_long must NOT be flagged")
	})

	t.Run("different expressions NOT redundant", func(t *testing.T) {
		createTable := `CREATE TABLE t1 (
			id INT PRIMARY KEY,
			name VARCHAR(255),
			INDEX f_lower ((LOWER(name))),
			INDEX f_upper ((UPPER(name)))
		)`
		linter := &RedundantIndexLinter{}
		ct, err := statement.ParseCreateTable(createTable)
		require.NoError(t, err)

		violations := linter.Lint([]*statement.CreateTable{ct}, nil)
		require.Empty(t, violations, "distinct expression indexes must not be redundant: %v", violations)
	})
}

// TestRedundantIndexLinter_PrefixLengths is a regression test for the bug where
// prefix lengths were ignored (Index.Columns drops the (n) in col(n)), so a
// prefix index col(10) and the full-column index col were reported as exact
// duplicates of each other, including an (unsafe) suggestion to drop the full
// index in favor of the prefix index.
func TestRedundantIndexLinter_PrefixLengths(t *testing.T) {
	t.Run("prefix index redundant to full-column index, not vice versa", func(t *testing.T) {
		createTable := `CREATE TABLE t1 (
			id INT PRIMARY KEY,
			name VARCHAR(255),
			INDEX i1 (name(10)),
			INDEX i2 (name)
		)`
		linter := &RedundantIndexLinter{}
		ct, err := statement.ParseCreateTable(createTable)
		require.NoError(t, err)

		violations := linter.Lint([]*statement.CreateTable{ct}, nil)
		i1Violated, i1Msg := indexViolated(violations, "i1")
		i2Violated, _ := indexViolated(violations, "i2")
		require.True(t, i1Violated, "i1 (name(10)) should be redundant to i2 (name), violations: %v", violations)
		require.Contains(t, i1Msg, "redundant")
		require.False(t, i2Violated, "i2 (full column) must NOT be flagged as redundant to a prefix index")
	})

	t.Run("equal prefix lengths are duplicates", func(t *testing.T) {
		createTable := `CREATE TABLE t1 (
			id INT PRIMARY KEY,
			name VARCHAR(255),
			INDEX i1 (name(10)),
			INDEX i2 (name(10))
		)`
		linter := &RedundantIndexLinter{}
		ct, err := statement.ParseCreateTable(createTable)
		require.NoError(t, err)

		violations := linter.Lint([]*statement.CreateTable{ct}, nil)
		violated, msg := indexViolated(violations, "i2")
		require.True(t, violated, "equal-prefix indexes should be duplicates, violations: %v", violations)
		require.Contains(t, msg, "duplicate")
	})

	t.Run("longer prefix covers shorter prefix but not the reverse", func(t *testing.T) {
		// name(20) covers name(10); name(10) does NOT cover name(20).
		createTable := `CREATE TABLE t1 (
			id INT PRIMARY KEY,
			name VARCHAR(255),
			INDEX i_short (name(10)),
			INDEX i_long (name(20))
		)`
		linter := &RedundantIndexLinter{}
		ct, err := statement.ParseCreateTable(createTable)
		require.NoError(t, err)

		violations := linter.Lint([]*statement.CreateTable{ct}, nil)
		shortViolated, shortMsg := indexViolated(violations, "i_short")
		longViolated, _ := indexViolated(violations, "i_long")
		require.True(t, shortViolated, "name(10) should be covered by name(20), violations: %v", violations)
		require.NotContains(t, shortMsg, "duplicate", "differing prefix lengths are not duplicates")
		require.False(t, longViolated, "name(20) must NOT be covered by name(10)")
	})

	t.Run("UNIQUE prefix index NOT redundant to UNIQUE full-column index", func(t *testing.T) {
		// UNIQUE(name(10)) enforces uniqueness over the first 10 characters of
		// name; UNIQUE(name) enforces uniqueness over the WHOLE column. The two
		// constraints are different: dropping UNIQUE(name(10)) would stop
		// rejecting rows that collide on their first 10 chars but differ
		// overall. Redundancy for a UNIQUE candidate-for-removal therefore
		// requires EXACT key-part equality (including prefix length), not the
		// read-side prefix coverage that name(10) has under name. Neither index
		// may be flagged.
		createTable := `CREATE TABLE t1 (
			id INT PRIMARY KEY,
			name VARCHAR(255),
			UNIQUE KEY u_prefix (name(10)),
			UNIQUE KEY u_full (name)
		)`
		linter := &RedundantIndexLinter{}
		ct, err := statement.ParseCreateTable(createTable)
		require.NoError(t, err)

		violations := linter.Lint([]*statement.CreateTable{ct}, nil)
		prefixViolated, prefixMsg := indexViolated(violations, "u_prefix")
		fullViolated, fullMsg := indexViolated(violations, "u_full")
		require.False(t, prefixViolated,
			"UNIQUE(name(10)) must NOT be redundant to UNIQUE(name) — it enforces prefix uniqueness the full-column index does not; got: %s", prefixMsg)
		require.False(t, fullViolated,
			"UNIQUE(name) must NOT be redundant to UNIQUE(name(10)); got: %s", fullMsg)
	})

	t.Run("exact-duplicate UNIQUE prefix indexes ARE still flagged", func(t *testing.T) {
		// Two UNIQUE indexes with the IDENTICAL prefix length enforce the exact
		// same uniqueness guarantee, so one is a genuine duplicate of the other
		// and must still be reported.
		createTable := `CREATE TABLE t1 (
			id INT PRIMARY KEY,
			name VARCHAR(255),
			UNIQUE KEY u1 (name(10)),
			UNIQUE KEY u2 (name(10))
		)`
		linter := &RedundantIndexLinter{}
		ct, err := statement.ParseCreateTable(createTable)
		require.NoError(t, err)

		violations := linter.Lint([]*statement.CreateTable{ct}, nil)
		violated, msg := indexViolated(violations, "u2")
		require.True(t, violated, "exact-duplicate UNIQUE prefix indexes should be flagged, violations: %v", violations)
		require.Contains(t, msg, "duplicate")
	})
}

// TestRedundantIndexLinter_CaseInsensitiveColumns verifies that column-name
// comparison is case-insensitive (MySQL identifiers are case-insensitive), so
// INDEX (A) is recognised as redundant to INDEX (a, b).
func TestRedundantIndexLinter_CaseInsensitiveColumns(t *testing.T) {
	createTable := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		a INT,
		b INT,
		INDEX i1 (A),
		INDEX i2 (a, b)
	)`
	linter := &RedundantIndexLinter{}
	ct, err := statement.ParseCreateTable(createTable)
	require.NoError(t, err)

	violations := linter.Lint([]*statement.CreateTable{ct}, nil)
	violated, msg := indexViolated(violations, "i1")
	require.True(t, violated, "INDEX (A) should be redundant to INDEX (a, b) (case-insensitive), violations: %v", violations)
	require.Contains(t, msg, "redundant")
}

// TestRedundantIndexLinter_AlterTableComplex tests a complex scenario
// with multiple tables and ALTER statements.
func TestRedundantIndexLinter_AlterTableComplex(t *testing.T) {
	// Create two tables
	table1SQL := `CREATE TABLE users (
		id INT PRIMARY KEY,
		email VARCHAR(255),
		name VARCHAR(255),
		UNIQUE KEY idx_email (email)
	)`

	table2SQL := `CREATE TABLE orders (
		id INT PRIMARY KEY,
		user_id INT,
		product_id INT,
		INDEX idx_user_product (user_id, product_id)
	)`

	linter := &RedundantIndexLinter{}

	table1, err := statement.ParseCreateTable(table1SQL)
	require.NoError(t, err)

	table2, err := statement.ParseCreateTable(table2SQL)
	require.NoError(t, err)

	// ALTER statements: one adds redundant index, one doesn't
	alterSQL1 := "ALTER TABLE users ADD INDEX idx_email_dup (email)"
	alterSQL2 := "ALTER TABLE orders ADD INDEX idx_product (product_id)"

	alter1, err := statement.New(alterSQL1)
	require.NoError(t, err)

	alter2, err := statement.New(alterSQL2)
	require.NoError(t, err)

	alter1 = append(alter1, alter2...)

	violations := linter.Lint([]*statement.CreateTable{table1, table2}, alter1)

	// Should detect idx_email_dup as redundant but not idx_product
	violatedIndexes := make(map[string]bool)
	for _, v := range violations {
		if v.Location != nil && v.Location.Index != nil {
			violatedIndexes[*v.Location.Index] = true
		}
	}

	require.True(t, violatedIndexes["idx_email_dup"], "idx_email_dup should be flagged")
	require.False(t, violatedIndexes["idx_product"], "idx_product should not be flagged")
}
