package lint

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/require"
)

// infoLinter is a test linter that produces SeverityInfo violations for any ALTER TABLE change.
type infoLinter struct{}

func (l infoLinter) Name() string        { return "test_info" }
func (l infoLinter) Description() string { return "test linter that produces info violations" }
func (l infoLinter) String() string      { return Stringer(l) }
func (l infoLinter) Lint(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) []Violation {
	var violations []Violation
	for _, ch := range changes {
		if ch.IsAlterTable() {
			violations = append(violations, Violation{
				Linter:   l,
				Severity: SeverityInfo,
				Message:  "this is an informational suggestion",
				Location: &Location{Table: ch.Table},
			})
		}
	}
	return violations
}

func TestPlanChanges_NoChanges(t *testing.T) {
	current := []table.TableSchema{
		{Name: "t1", Schema: "CREATE TABLE t1 (id BIGINT PRIMARY KEY)"},
	}
	plan, err := PlanChanges(current, current, nil, nil)
	require.NoError(t, err)
	require.False(t, plan.HasChanges())
	require.False(t, plan.HasErrors())
	require.False(t, plan.HasWarnings())
}

func TestPlanChanges_AddColumn(t *testing.T) {
	current := []table.TableSchema{
		{Name: "t1", Schema: "CREATE TABLE t1 (id BIGINT PRIMARY KEY)"},
	}
	desired := []table.TableSchema{
		{Name: "t1", Schema: "CREATE TABLE t1 (id BIGINT PRIMARY KEY, name VARCHAR(100))"},
	}
	plan, err := PlanChanges(current, desired, nil, nil)
	require.NoError(t, err)
	require.True(t, plan.HasChanges())
	require.Len(t, plan.Changes, 1)
	require.Contains(t, plan.Changes[0].Statement, "ALTER TABLE")
	require.Equal(t, "t1", plan.Changes[0].TableName)
	// Statement should be semicolon-terminated
	require.Equal(t, byte(';'), plan.Changes[0].Statement[len(plan.Changes[0].Statement)-1])
}

func TestPlanChanges_NewTable(t *testing.T) {
	current := []table.TableSchema{
		{Name: "t1", Schema: "CREATE TABLE t1 (id BIGINT PRIMARY KEY)"},
	}
	desired := []table.TableSchema{
		{Name: "t1", Schema: "CREATE TABLE t1 (id BIGINT PRIMARY KEY)"},
		{Name: "t2", Schema: "CREATE TABLE t2 (id BIGINT PRIMARY KEY, val INT)"},
	}
	plan, err := PlanChanges(current, desired, nil, nil)
	require.NoError(t, err)
	require.True(t, plan.HasChanges())
	require.Len(t, plan.Changes, 1)
	require.Contains(t, plan.Changes[0].Statement, "CREATE TABLE")
	require.Equal(t, "t2", plan.Changes[0].TableName)
}

func TestPlanChanges_DropTable(t *testing.T) {
	current := []table.TableSchema{
		{Name: "t1", Schema: "CREATE TABLE t1 (id BIGINT PRIMARY KEY)"},
		{Name: "t2", Schema: "CREATE TABLE t2 (id BIGINT PRIMARY KEY)"},
	}
	desired := []table.TableSchema{
		{Name: "t1", Schema: "CREATE TABLE t1 (id BIGINT PRIMARY KEY)"},
	}
	plan, err := PlanChanges(current, desired, nil, nil)
	require.NoError(t, err)
	require.True(t, plan.HasChanges())
	require.Len(t, plan.Changes, 1)
	require.Contains(t, plan.Changes[0].Statement, "DROP TABLE")
}

func TestPlanChanges_Statements(t *testing.T) {
	current := []table.TableSchema{
		{Name: "t1", Schema: "CREATE TABLE t1 (id BIGINT PRIMARY KEY)"},
	}
	desired := []table.TableSchema{
		{Name: "t1", Schema: "CREATE TABLE t1 (id BIGINT PRIMARY KEY, name VARCHAR(100))"},
	}
	plan, err := PlanChanges(current, desired, nil, nil)
	require.NoError(t, err)
	stmts := plan.Statements()
	require.Len(t, stmts, 1)
	require.Contains(t, stmts[0], "ALTER TABLE")
}

func TestPlanChanges_WithLintViolations(t *testing.T) {
	// Create a table with a foreign key — the has_fk linter should flag it
	current := []table.TableSchema{
		{Name: "parent", Schema: "CREATE TABLE parent (id BIGINT PRIMARY KEY)"},
		{Name: "child", Schema: `CREATE TABLE child (
			id BIGINT PRIMARY KEY,
			parent_id BIGINT,
			CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES parent(id)
		)`},
	}
	desired := []table.TableSchema{
		{Name: "parent", Schema: "CREATE TABLE parent (id BIGINT PRIMARY KEY)"},
		{Name: "child", Schema: `CREATE TABLE child (
			id BIGINT PRIMARY KEY,
			parent_id BIGINT,
			name VARCHAR(100),
			CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES parent(id)
		)`},
	}
	plan, err := PlanChanges(current, desired, nil, nil)
	require.NoError(t, err)
	require.True(t, plan.HasChanges())
	require.Len(t, plan.Changes, 1)
	require.Equal(t, "child", plan.Changes[0].TableName)
	// The has_fk linter should produce a warning for the child table
	require.True(t, plan.HasWarnings(), "expected lint warnings for table with FK")
	require.NotEmpty(t, plan.Changes[0].Warnings())
}

func TestPlanChanges_StructuredViolations(t *testing.T) {
	current := []table.TableSchema{
		{Name: "parent", Schema: "CREATE TABLE parent (id BIGINT PRIMARY KEY)"},
		{Name: "child", Schema: `CREATE TABLE child (
			id BIGINT PRIMARY KEY,
			parent_id BIGINT,
			CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES parent(id)
		)`},
	}
	desired := []table.TableSchema{
		{Name: "parent", Schema: "CREATE TABLE parent (id BIGINT PRIMARY KEY)"},
		{Name: "child", Schema: `CREATE TABLE child (
			id BIGINT PRIMARY KEY,
			parent_id BIGINT,
			name VARCHAR(100),
			CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES parent(id)
		)`},
	}
	plan, err := PlanChanges(current, desired, nil, nil)
	require.NoError(t, err)
	require.Len(t, plan.Changes, 1)

	change := plan.Changes[0]
	require.NotEmpty(t, change.Violations, "expected structured Violations to be populated")

	// Verify structured fields are accessible
	v := change.Violations[0]
	require.Equal(t, "child", v.Location.Table)
	require.NotEmpty(t, v.Message)
	require.Equal(t, SeverityWarning, v.Severity)
	require.Equal(t, "has_foreign_key", v.Linter.Name())

	// Severity methods filter correctly
	require.Len(t, change.Warnings(), 1)
	require.Empty(t, change.Errors())
}

func TestPlanChanges_DropColumnsHaveColumnDetails(t *testing.T) {
	current := []table.TableSchema{
		{Name: "users", Schema: `CREATE TABLE users (
			id BIGINT PRIMARY KEY,
			email VARCHAR(255),
			phone VARCHAR(255)
		)`},
	}
	desired := []table.TableSchema{
		{Name: "users", Schema: `CREATE TABLE users (
			id BIGINT PRIMARY KEY
		)`},
	}

	plan, err := PlanChanges(current, desired, nil, nil)
	require.NoError(t, err)
	require.Len(t, plan.Changes, 1)
	require.True(t, plan.HasErrors())

	change := plan.Changes[0]
	require.Equal(t, "users", change.TableName)
	require.Len(t, change.Errors(), 2)

	columnMessages := map[string]string{}
	for _, violation := range change.Errors() {
		require.Equal(t, "unsafe", violation.Linter.Name())
		require.NotNil(t, violation.Location)
		require.Equal(t, "users", violation.Location.Table)
		require.NotNil(t, violation.Location.Column)
		columnMessages[*violation.Location.Column] = violation.Message
	}
	require.Equal(t, map[string]string{
		"email": "Unsafe operation detected: \"DROP COLUMN `email`\"",
		"phone": "Unsafe operation detected: \"DROP COLUMN `phone`\"",
	}, columnMessages)
}

func TestPlanChanges_WithLintConfig(t *testing.T) {
	// Disable the has_foreign_key linter — FK warnings should not appear.
	current := []table.TableSchema{
		{Name: "t1", Schema: `CREATE TABLE t1 (
			id BIGINT PRIMARY KEY,
			parent_id BIGINT,
			CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES parent(id)
		)`},
	}
	desired := []table.TableSchema{
		{Name: "t1", Schema: `CREATE TABLE t1 (
			id BIGINT PRIMARY KEY,
			parent_id BIGINT,
			name VARCHAR(100),
			CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES parent(id)
		)`},
	}
	cfg := &Config{
		Enabled: map[string]bool{
			"has_foreign_key": false,
		},
	}
	plan, err := PlanChanges(current, desired, nil, cfg)
	require.NoError(t, err)
	require.True(t, plan.HasChanges())
	// has_foreign_key is disabled and is the only linter that would fire
	// on this schema, so there should be no warnings at all.
	require.False(t, plan.HasWarnings(), "expected no warnings when has_foreign_key is disabled")
	for _, ch := range plan.Changes {
		require.Empty(t, ch.Warnings(), "expected no warnings on any change")
	}
}

func TestPlanChanges_EmptySchemas(t *testing.T) {
	plan, err := PlanChanges(nil, nil, nil, nil)
	require.NoError(t, err)
	require.False(t, plan.HasChanges())
	require.Empty(t, plan.Statements())
}

func TestPlanChanges_WithInfos(t *testing.T) {
	// Register a test linter that produces SeverityInfo violations.
	Register(infoLinter{})
	t.Cleanup(func() {
		lock.Lock()
		delete(linters, "test_info")
		lock.Unlock()
	})

	current := []table.TableSchema{
		{Name: "t1", Schema: "CREATE TABLE t1 (id BIGINT PRIMARY KEY)"},
	}
	desired := []table.TableSchema{
		{Name: "t1", Schema: "CREATE TABLE t1 (id BIGINT PRIMARY KEY, name VARCHAR(100))"},
	}
	plan, err := PlanChanges(current, desired, nil, nil)
	require.NoError(t, err)
	require.True(t, plan.HasChanges())
	require.Len(t, plan.Changes, 1)
	require.Equal(t, "t1", plan.Changes[0].TableName)

	// The test_info linter should produce an info for the ALTER TABLE change.
	require.True(t, plan.HasInfos(), "expected lint infos from test_info linter")
	require.NotEmpty(t, plan.Changes[0].Infos())
	require.Equal(t, "test_info", plan.Changes[0].Infos()[0].Linter.Name())
	require.Contains(t, plan.Changes[0].Infos()[0].Message, "informational suggestion")

	// Infos should not appear as warnings or errors.
	require.Empty(t, plan.Changes[0].Errors())
}

func TestPlanChanges_HasInfosFalseWhenNone(t *testing.T) {
	current := []table.TableSchema{
		{Name: "t1", Schema: "CREATE TABLE t1 (id BIGINT PRIMARY KEY)"},
	}
	desired := []table.TableSchema{
		{Name: "t1", Schema: "CREATE TABLE t1 (id BIGINT PRIMARY KEY, name VARCHAR(100))"},
	}
	plan, err := PlanChanges(current, desired, nil, nil)
	require.NoError(t, err)
	require.True(t, plan.HasChanges())
	// No info-producing linter is registered, so HasInfos should be false.
	require.False(t, plan.HasInfos())
	require.Empty(t, plan.Changes[0].Infos())
}

func TestPlanChanges_InfosNotOnNonAlter(t *testing.T) {
	// Register a test linter that only produces infos for ALTER TABLE.
	Register(infoLinter{})
	t.Cleanup(func() {
		lock.Lock()
		delete(linters, "test_info")
		lock.Unlock()
	})

	// A CREATE TABLE (new table) should not trigger the info linter.
	current := []table.TableSchema{
		{Name: "t1", Schema: "CREATE TABLE t1 (id BIGINT PRIMARY KEY)"},
	}
	desired := []table.TableSchema{
		{Name: "t1", Schema: "CREATE TABLE t1 (id BIGINT PRIMARY KEY)"},
		{Name: "t2", Schema: "CREATE TABLE t2 (id BIGINT PRIMARY KEY, val INT)"},
	}
	plan, err := PlanChanges(current, desired, nil, nil)
	require.NoError(t, err)
	require.True(t, plan.HasChanges())
	require.Len(t, plan.Changes, 1)
	require.Contains(t, plan.Changes[0].Statement, "CREATE TABLE")
	// The info linter only fires on ALTER, so no infos here.
	require.False(t, plan.HasInfos())
	require.Empty(t, plan.Changes[0].Infos())
}

func TestPlanChanges_MultiStatementSameTable(t *testing.T) {
	// Changing partition type produces two ALTER TABLE statements for the same
	// table (REMOVE PARTITIONING, then PARTITION BY ...). Adding a FK to the
	// table ensures the has_foreign_key linter fires. Violations should only
	// be attached to the last statement for the table.
	current := []table.TableSchema{
		{Name: "parent", Schema: "CREATE TABLE parent (id INT NOT NULL PRIMARY KEY)"},
		{Name: "t1", Schema: `CREATE TABLE t1 (
			id INT NOT NULL PRIMARY KEY,
			parent_id INT,
			CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES parent(id)
		) PARTITION BY HASH(id) PARTITIONS 4`},
	}
	desired := []table.TableSchema{
		{Name: "parent", Schema: "CREATE TABLE parent (id INT NOT NULL PRIMARY KEY)"},
		{Name: "t1", Schema: `CREATE TABLE t1 (
			id INT NOT NULL PRIMARY KEY,
			parent_id INT,
			CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES parent(id)
		) PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN (100), PARTITION p1 VALUES LESS THAN MAXVALUE)`},
	}

	plan, err := PlanChanges(current, desired, nil, nil)
	require.NoError(t, err)
	require.True(t, plan.HasChanges())

	// Should have exactly 2 changes for t1: REMOVE PARTITIONING, then PARTITION BY.
	var t1Changes []PlannedChange
	for _, ch := range plan.Changes {
		if ch.TableName == "t1" {
			t1Changes = append(t1Changes, ch)
		}
	}
	require.Len(t, t1Changes, 2, "expected 2 statements for t1 (partition type change)")
	require.Contains(t, t1Changes[0].Statement, "REMOVE PARTITIONING")
	require.Contains(t, t1Changes[1].Statement, "PARTITION BY")

	// Violations should only be on the last statement.
	require.Empty(t, t1Changes[0].Warnings(), "first statement should have no violations")
	require.Empty(t, t1Changes[0].Errors(), "first statement should have no violations")
	require.Empty(t, t1Changes[0].Infos(), "first statement should have no violations")
	require.NotEmpty(t, t1Changes[1].Warnings(), "last statement should carry the FK warning")
	require.True(t, plan.HasWarnings())
}

func TestTerminatedStmt(t *testing.T) {
	require.Equal(t, "SELECT 1;", terminatedStmt("SELECT 1"))
	require.Equal(t, "SELECT 1;", terminatedStmt("SELECT 1;"))
	require.Equal(t, "SELECT 1;", terminatedStmt("  SELECT 1  "))
	require.Empty(t, terminatedStmt(""))
	require.Empty(t, terminatedStmt("   "))
}
