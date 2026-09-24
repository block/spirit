package migration

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
)

type tableChange struct {
	stmt     *statement.AbstractStatement
	table    *table.TableInfo
	newTable *table.TableInfo

	// chunker is the MappedChunker specific to this change.
	// It is set during initChunkers() and used by setupCopierCheckerAndReplClient()
	// to pass to AddSubscription, which requires a single-table MappedChunker
	// (not the multi-chunker wrapper stored on the Runner).
	chunker table.MappedChunker

	// Store a pointer back to the migration runner
	// (for compatibility, we want to eventually remove this)
	runner *Runner
}

func (c *tableChange) createNewTable(ctx context.Context) error {
	newName := utils.NewTableName(c.table.TableName)
	// drop the newName if we've decided to call this func.
	if err := dbconn.Exec(ctx, c.runner.db, "DROP TABLE IF EXISTS %n", newName); err != nil {
		return err
	}
	if err := dbconn.Exec(ctx, c.runner.db, "CREATE TABLE %n LIKE %n",
		newName, c.table.TableName); err != nil {
		return err
	}
	c.newTable = table.NewTableInfo(c.runner.db, c.stmt.Schema, newName)
	if err := c.newTable.SetInfo(ctx); err != nil {
		return err
	}
	return nil
}

// alterNewTable applies the ALTER to the new table.
// It has been pre-checked it is not a rename, or modifying the PRIMARY KEY.
// We first attempt to do this using ALGORITHM=COPY so we don't burn
// an INSTANT version. But surprisingly this is not supported for all DDLs (issue #277)
func (c *tableChange) alterNewTable(ctx context.Context) error {
	// Not necessarily the user's ALTER verbatim: check constraint names have to
	// be rewritten for the new table. See newTableAlter.
	alter, err := c.newTableAlter(ctx)
	if err != nil {
		return err
	}
	// The ALTER clause is spliced in with %r: it is raw SQL that may
	// legitimately contain % characters (e.g. COMMENT '100%new'), which must
	// not be interpreted as format specifiers.
	if err := dbconn.Exec(ctx, c.runner.db, "ALTER TABLE %n %r, ALGORITHM=COPY",
		c.newTable.TableName, sqlescape.RawSQL(alter)); err != nil {
		// Retry without the ALGORITHM=COPY. If there is a second error, then the DDL itself
		// is not supported. It could be a syntax error, in which case we return the second
		// error, which will probably be easier to read because spirit's own ALGORITHM=COPY
		// is not on the end of it. The clauses can still differ from what the user wrote:
		// newTableAlter rewrites check constraint names for the new table, so either error
		// may quote a server-generated name in place of the user's symbol.
		if err := dbconn.Exec(ctx, c.runner.db, "ALTER TABLE %n %r", c.newTable.TableName, sqlescape.RawSQL(alter)); err != nil {
			if alter != c.stmt.TrimAlter() {
				// Say which statement failed, since it is not the one the user
				// wrote and the error can name a constraint they have never seen.
				return fmt.Errorf("%w (applied to the new table as: ALTER TABLE %s %s)",
					err, c.newTable.TableName, alter)
			}
			return err
		}
	}
	// Call GetInfo on the table again, since the columns
	// might have changed and this will affect the row copiers intersect func.
	if err := c.newTable.SetInfo(ctx); err != nil {
		return err
	}

	// Preserve AUTO_INCREMENT value from the original table AFTER the ALTER.
	// CREATE TABLE LIKE doesn't copy AUTO_INCREMENT, and ALTER with ALGORITHM=COPY
	// can reset it. For empty tables, INSERT SELECT won't trigger MySQL's automatic
	// adjustment, so we explicitly set it to prevent new inserts from restarting at 1.
	return c.preserveAutoIncrement(ctx)
}

// newTableAlter returns the ALTER clauses to apply to the new table.
//
// This is the user's ALTER, except for the names of check constraints. Those
// names are unique per schema and not per table (MySQL: "The CONSTRAINT symbol
// value, if defined, must be unique in the database"), so the _new table cannot
// hold them while the user's table still exists: CREATE TABLE .. LIKE gives its
// copies server-generated names instead. Replaying the ALTER verbatim would
// then fail on a name that is not there ("Check constraint 'x' is not found in
// the table", error 3821), or - for an ALTER that drops a constraint and re-adds
// it under the same name - on one that is still owned by the user's table
// ("Duplicate check constraint name 'x'", error 3822).
//
// So DROP CHECK / ALTER CHECK are retargeted at the _new table's names for the
// same constraints, and a re-added name is dropped so MySQL generates one. See
// AlterWithRenamedCheckConstraints for the rewriting rules.
func (c *tableChange) newTableAlter(ctx context.Context) (string, error) {
	// Only an ALTER that names a check constraint needs any of this, which
	// keeps the two SHOW CREATE TABLE round trips off the common path.
	if len(c.stmt.CheckConstraintsReferenced()) == 0 {
		return c.stmt.TrimAlter(), nil
	}
	source, err := tableDefinition(ctx, c.runner.db, c.table.TableName)
	if err != nil {
		return "", err
	}
	if err := rejectAmbiguousConstraintDrop(c.stmt, source, c.table.TableName); err != nil {
		return "", err
	}
	newTable, err := tableDefinition(ctx, c.runner.db, c.newTable.TableName)
	if err != nil {
		return "", err
	}
	renames, err := c.checkConstraintRenames(source, newTable)
	if err != nil {
		return "", err
	}
	alter, unnamed, err := c.stmt.AlterWithRenamedCheckConstraints(renames)
	if err != nil {
		return "", err
	}
	for _, name := range unnamed {
		c.runner.logger.Warn("a CHECK constraint cannot be re-added under the same name by a copy migration, because the name is still held by the table being replaced: MySQL will generate a name for it instead",
			"table", c.table.TableName,
			"constraint", name)
	}
	if alter != c.stmt.TrimAlter() {
		c.runner.logger.Info("rewrote CHECK constraint names for the new table",
			"table", c.newTable.TableName,
			"alter", alter)
	}
	return alter, nil
}

// rejectAmbiguousConstraintDrop refuses an ALTER whose DROP CONSTRAINT names a
// constraint MySQL itself would not resolve, before newTableAlter retargets it
// at the check constraint of that name and quietly makes the migration mean
// something the statement does not.
//
// A table may hold the same symbol in more than one constraint namespace -
// UNIQUE KEY `dup` alongside CONSTRAINT `dup` CHECK (..) is legal - and MySQL
// rejects a DROP CONSTRAINT that cannot tell them apart: "Table has multiple
// constraints with the name 'dup'" (error 3939).
//
// Spirit does not inherit that rejection. The direct-DDL attempt does get
// error 3939 back from the user's table, but a direct-DDL failure is the
// ordinary signal to use the copy algorithm instead, so it is discarded. The
// copy then applies the ALTER to the _new table, where CREATE TABLE .. LIKE
// has renamed the check constraint (see newTableAlter) and left only one `dup`
// behind: the clause resolves there and drops the check constraint while
// keeping the unique key - a table the user never asked for, reported as
// success.
//
// The foreign key namespace is not consulted: spirit refuses a table with
// foreign keys long before this point.
func rejectAmbiguousConstraintDrop(stmt *statement.AbstractStatement, def *statement.CreateTable, tableName string) error {
	for _, name := range stmt.GenericConstraintDrops() {
		var owners []string
		for _, constraint := range def.GetConstraints() {
			if constraint.Type == "CHECK" && strings.EqualFold(constraint.Name, name) {
				owners = append(owners, "CHECK constraint")
			}
		}
		for _, index := range def.GetIndexes() {
			// MySQL resolves DROP CONSTRAINT against unique and primary keys
			// only, so a non-unique index of the same name is not a clash.
			// GetIndexes names the primary key "PRIMARY", which is the symbol
			// MySQL matches it under.
			if (index.Type == "UNIQUE" || index.Type == "PRIMARY KEY") && strings.EqualFold(index.Name, name) {
				owners = append(owners, index.Type)
			}
		}
		if len(owners) > 1 {
			return fmt.Errorf("table %s has multiple constraints named %s (%s), so DROP CONSTRAINT %s is ambiguous and MySQL rejects it: name the one you mean with a constraint specific clause such as DROP CHECK or DROP KEY",
				tableName, name, strings.Join(owners, ", "), name)
		}
	}
	return nil
}

// checkConstraintRenames maps each check constraint name on the table being
// altered (lower-cased, because MySQL matches these names case-insensitively)
// to the name the same constraint has on the _new table.
//
// SHOW CREATE TABLE lists check constraints sorted by name, and CREATE TABLE ..
// LIKE numbers its copies _<table>_chk_1 .. _<table>_chk_N in the order it reads
// the source, which is that same name-sorted order. The copies are then listed
// name-sorted too, and those names sort as strings, so _chk_10 comes back between
// _chk_1 and _chk_2: the two listings only correspond once the copies are put
// back in numeric order. Their expressions and enforcement are compared
// afterwards to confirm the pairing, rather than trusting the ordering and
// retargeting a DROP CHECK at some other constraint.
func (c *tableChange) checkConstraintRenames(sourceDef, newTableDef *statement.CreateTable) (map[string]string, error) {
	source := checkConstraints(sourceDef)
	newTable := checkConstraints(newTableDef)
	if len(source) != len(newTable) {
		return nil, fmt.Errorf("table %s has %d CHECK constraint(s) but its copy %s has %d",
			c.table.TableName, len(source), c.newTable.TableName, len(newTable))
	}
	if err := sortByGeneratedNumber(newTable, c.newTable.TableName); err != nil {
		return nil, err
	}
	renames := make(map[string]string, len(source))
	for i := range source {
		if !checkConstraintsMatch(source[i], newTable[i]) {
			return nil, fmt.Errorf("CHECK constraint %s on %s does not match %s on its copy %s",
				source[i].Name, c.table.TableName, newTable[i].Name, c.newTable.TableName)
		}
		renames[strings.ToLower(source[i].Name)] = newTable[i].Name
	}
	return renames, nil
}

// sortByGeneratedNumber puts a table's check constraints in the order MySQL
// generated their names in, i.e. by the number in <table>_chk_<n> rather than by
// the string that number appears in. It is only for the names on a table created
// by CREATE TABLE .. LIKE, which are always server-generated: a name that does
// not follow the pattern means the assumption the pairing rests on is wrong, so
// it is an error rather than something to sort around.
func sortByGeneratedNumber(constraints statement.Constraints, tableName string) error {
	numbers := make(map[string]int, len(constraints))
	prefix := strings.ToLower(tableName) + "_chk_"
	for _, constraint := range constraints {
		suffix, ok := strings.CutPrefix(strings.ToLower(constraint.Name), prefix)
		if !ok {
			return fmt.Errorf("CHECK constraint %s on %s is not named the way CREATE TABLE .. LIKE names the constraints it copies (%s<n>), so it cannot be matched to a constraint on the table being altered",
				constraint.Name, tableName, prefix)
		}
		number, err := strconv.Atoi(suffix)
		if err != nil {
			return fmt.Errorf("CHECK constraint %s on %s does not end in the number CREATE TABLE .. LIKE appends to the constraints it copies, so it cannot be matched to a constraint on the table being altered",
				constraint.Name, tableName)
		}
		numbers[constraint.Name] = number
	}
	slices.SortFunc(constraints, func(a, b statement.Constraint) int {
		return cmp.Compare(numbers[a.Name], numbers[b.Name])
	})
	return nil
}

// tableDefinition returns the table's SHOW CREATE TABLE, parsed.
func tableDefinition(ctx context.Context, db *sql.DB, tableName string) (*statement.CreateTable, error) {
	var name, createTable string
	if err := db.QueryRowContext(ctx,
		sqlescape.MustEscapeSQL("SHOW CREATE TABLE %n", tableName)).Scan(&name, &createTable); err != nil {
		return nil, fmt.Errorf("could not read the definition of table %s: %w", tableName, err)
	}
	stmts, err := statement.New(createTable)
	if err != nil {
		return nil, fmt.Errorf("could not parse the definition of table %s: %w", tableName, err)
	}
	createStmt, err := stmts[0].ParseCreateTable()
	if err != nil {
		return nil, fmt.Errorf("could not parse the definition of table %s: %w", tableName, err)
	}
	return createStmt, nil
}

// checkConstraints returns the table's CHECK constraints as SHOW CREATE TABLE
// lists them, which is sorted by name.
func checkConstraints(def *statement.CreateTable) statement.Constraints {
	var constraints statement.Constraints
	for _, constraint := range def.GetConstraints() {
		if constraint.Type == "CHECK" {
			constraints = append(constraints, constraint)
		}
	}
	return constraints
}

// checkConstraintsMatch reports whether two CHECK constraints are the same
// constraint: same expression, same enforcement. Both sides come from SHOW
// CREATE TABLE and are parsed the same way, so MySQL's own canonical rendering
// makes the expression comparison textual. Enforcement is part of the
// comparison because constraints can share an expression and differ only in
// whether it is enforced, and dropping the wrong one of those pair would leave
// the table enforcing a rule the user dropped - with nothing to report.
func checkConstraintsMatch(a, b statement.Constraint) bool {
	if a.NotEnforced != b.NotEnforced {
		return false
	}
	if a.Expression == nil || b.Expression == nil {
		return a.Expression == b.Expression
	}
	return *a.Expression == *b.Expression
}

func (c *tableChange) preserveAutoIncrement(ctx context.Context) error {
	// Get AUTO_INCREMENT from the original table.
	var originalAutoInc sql.NullInt64
	err := c.runner.db.QueryRowContext(ctx,
		"SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?",
		c.table.TableName).Scan(&originalAutoInc)
	if err != nil {
		return fmt.Errorf("failed to get AUTO_INCREMENT value from original table: %w", err)
	}

	// If the original table doesn't have a meaningful AUTO_INCREMENT, nothing to preserve.
	if !originalAutoInc.Valid || originalAutoInc.Int64 <= 1 {
		return nil
	}

	// Get AUTO_INCREMENT from the new table to detect if it was explicitly set by the ALTER.
	var newTableAutoInc sql.NullInt64
	err = c.runner.db.QueryRowContext(ctx,
		"SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?",
		c.newTable.TableName).Scan(&newTableAutoInc)
	if err != nil {
		return fmt.Errorf("failed to get AUTO_INCREMENT value from new table: %w", err)
	}

	// Only override AUTO_INCREMENT on the new table if it doesn't appear to have been explicitly set.
	// If the new table's AUTO_INCREMENT is different from the original, the user explicitly changed it.
	if newTableAutoInc.Valid && newTableAutoInc.Int64 > 1 && newTableAutoInc.Int64 != originalAutoInc.Int64 {
		// Respect the explicitly configured AUTO_INCREMENT on the new table.
		return nil
	}

	if err := dbconn.Exec(ctx, c.runner.db, "ALTER TABLE %n AUTO_INCREMENT = %?",
		c.newTable.TableName, originalAutoInc.Int64); err != nil {
		return fmt.Errorf("failed to set AUTO_INCREMENT on new table: %w", err)
	}
	c.runner.logger.Info("preserved AUTO_INCREMENT value",
		"table", c.table.TableName,
		"auto_increment", originalAutoInc.Int64)
	return nil
}

func (c *tableChange) dropOldTable(ctx context.Context) error {
	return dbconn.Exec(ctx, c.runner.db, "DROP TABLE IF EXISTS %n", c.oldTableName())
}

func (c *tableChange) oldTableName() string {
	if !c.runner.migration.SkipDropAfterCutover {
		return utils.OldTableName(c.table.TableName)
	}
	timestamp := c.runner.status.StartTime().UTC().Format(utils.NameFormatTimestamp)
	return utils.OldTableNameWithTimestamp(c.table.TableName, timestamp)
}

func (c *tableChange) attemptInstantDDL(ctx context.Context) error {
	// The user's ALTER clause is spliced in with %r so that % characters in
	// its literals are not interpreted as format specifiers.
	return dbconn.ForceExec(
		ctx,
		c.runner.db,
		[]*table.TableInfo{c.table},
		c.runner.dbConfig,
		c.runner.logger,
		"ALTER TABLE %n ALGORITHM=INSTANT, %r",
		c.table.TableName,
		sqlescape.RawSQL(c.stmt.Alter),
	)
}

func (c *tableChange) attemptInplaceDDL(ctx context.Context) error {
	// As in attemptInstantDDL, the user's ALTER clause is spliced in with %r.
	return dbconn.ForceExec(
		ctx,
		c.runner.db,
		[]*table.TableInfo{c.table},
		c.runner.dbConfig,
		c.runner.logger,
		"ALTER TABLE %n ALGORITHM=INPLACE, LOCK=NONE, %r",
		c.table.TableName,
		sqlescape.RawSQL(c.stmt.Alter),
	)
}

func (c *tableChange) cleanup(ctx context.Context) error {
	if c.newTable != nil {
		if err := dbconn.Exec(ctx, c.runner.db, "DROP TABLE IF EXISTS %n", c.newTable.TableName); err != nil {
			return err
		}
	}
	return nil
}

// ambiguousDDLError converts a direct-DDL failure whose outcome is unknown
// into an ownership-ambiguous error. A deterministic server error ("this
// ALTER cannot be INSTANT") means the DDL definitely did not apply, which is
// the expected case the caller falls through on. A connection loss means the
// server may have applied it and the client never saw the OK packet — falling
// through to the copy algorithm would then build a _new table from a table
// that has *already* been altered. It returns nil when err is unambiguous.
func ambiguousDDLError(err error) error {
	if !dbconn.IsConnectionLossError(err) {
		return nil
	}
	return fmt.Errorf("%w: direct DDL may have committed: %w", status.ErrOwnershipAmbiguous, err)
}

// attemptMySQLDDL "attempts" to use DDL directly on MySQL with an assertion
// such as ALGORITHM=INSTANT. If MySQL is able to use the INSTANT algorithm,
// it will perform the operation without error. If it can't, it will return
// an error. It is important to let MySQL decide if it can handle the DDL
// operation, because keeping track of which operations are "INSTANT"
// is incredibly difficult. It will depend on MySQL minor version,
// and could possibly be specific to the table.
//
// Most failures here are expected and are ignored by the caller, which then
// proceeds with the copy algorithm. The exception is an ownership-ambiguous
// failure (see ambiguousDDLError): the caller must abort on those instead.
func (c *tableChange) attemptMySQLDDL(ctx context.Context) error {
	err := c.attemptInstantDDL(ctx)
	if err == nil {
		c.runner.usedInstantDDL = true // success
		return nil
	}
	if ambiguous := ambiguousDDLError(err); ambiguous != nil {
		return ambiguous
	}

	// Many "inplace" operations (such as adding an index)
	// are only online-safe to do in Aurora GLOBAL
	// because replicas do not use the binlog. Some, however,
	// only modify the table metadata and are safe.
	//
	// Spirit automatically detects safe operations that can use
	// the INPLACE algorithm without blocking read replicas.
	err = c.stmt.AlgorithmInplaceConsideredSafe()
	if err == nil {
		err = c.attemptInplaceDDL(ctx)
		if err == nil {
			c.runner.usedInplaceDDL = true // success
			return nil
		}
		if ambiguous := ambiguousDDLError(err); ambiguous != nil {
			return ambiguous
		}
	}
	c.runner.logger.Info("unable to use INPLACE", "error", err)

	// Failure is expected, since MySQL DDL only applies in limited scenarios
	// Return the error, which will be ignored by the caller.
	// Proceed with regular copy algorithm.
	return err
}

func (c *tableChange) Close() error {
	if c.table != nil {
		return c.table.Close()
	}
	return nil
}
