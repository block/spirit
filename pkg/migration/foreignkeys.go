package migration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"strings"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/migration/check"
	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/parser/format"
	parsermysql "github.com/block/spirit/pkg/parser/mysql"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
)

// Experimental support for the foreign keys of the table being altered
// (--enable-experimental-foreign-keys, MySQL 9.7 and later; see
// check.MinForeignKeyVersion). A table that other tables' foreign keys
// reference is still refused, by the hasforeignkeys check.
//
// The _new table has no foreign keys while the rows are copied. MySQL checks a
// change to a parent table against every child table, so a foreign key on the
// _new table would hold the application to the copy's rows as well as the
// table's: with ON DELETE RESTRICT (the default) it could not delete a parent
// row that a copied child row still references, even after deleting the child
// row, until the change feed applied that delete to the copy.
//
// Setup does add the foreign keys to the empty _new table, so that MySQL
// checks the ALTER against them as a native ALTER would be checked: a column
// type change a foreign key cannot take, a dropped index one needs, a renamed
// column. It then drops them again, keeping their indexes.
//
// The cutover adds them back with the table lock held, after the final flush,
// without checking the rows (foreignKeyCutover.addToNewTables): the rows are a
// copy of the table's, which hold the same foreign keys. That is a metadata
// change only while the new table has an index for each foreign key; without
// one MySQL builds it, with the lock held. Before it locks, the cutover asks
// MySQL which it will be (foreignKeyCutover.probe). Foreign key names are
// unique per schema, so the copies are named by utils.NewForeignKeyName. After
// the RENAME, still under the lock, the foreign keys of the _old table are
// dropped and the copies take their names back (foreignKeyCutover.settle).

// foreignKeyCutover puts the foreign keys of the tables being altered on their
// new tables during the cutover, and settles their names after it.
type foreignKeyCutover struct {
	db       *sql.DB
	dbConfig *dbconn.DBConfig
	logger   *slog.Logger
	tables   []*foreignKeyTable
	// settled is set once the cutover has given the copies their names back.
	settled bool
}

// foreignKeyTable is a table being altered, as foreignKeyCutover needs it.
type foreignKeyTable struct {
	stmt         *statement.AbstractStatement
	table        *table.TableInfo
	newTable     *table.TableInfo
	oldTableName string
	// probed is what the last probe found adds no index, if it found that.
	probed *foreignKeyProbe
}

// foreignKeyProbe is a set of foreign keys that a probe found MySQL adds to a
// table without building an index.
type foreignKeyProbe struct {
	clauses    []string               // the ADD clauses, as addToNewTables runs them
	definition *statement.CreateTable // the table the probe added them to
}

// execFunc runs one DDL statement.
type execFunc func(ctx context.Context, stmt string) error

// underLock runs statements on the locking session, with foreign key checks
// off.
func underLock(lock *dbconn.TableLock) execFunc {
	return func(ctx context.Context, stmt string) error {
		return lock.ExecUnderLockWithoutForeignKeyChecks(ctx, stmt)
	}
}

// referenced returns the tables the foreign keys of the tables being altered
// reference. The cutover lock waits for the sessions that hold them, and the
// force-kill covers them (see dbconn.NewTableLockReferencing).
func (f *foreignKeyCutover) referenced(ctx context.Context) ([]*table.TableInfo, error) {
	var tables []*table.TableInfo
	for _, t := range f.tables {
		parents, err := referencedTables(ctx, f.db, t.table)
		if err != nil {
			return nil, err
		}
		for _, p := range parents {
			if !slices.ContainsFunc(tables, func(o *table.TableInfo) bool {
				return o.SchemaName == p.SchemaName && o.TableName == p.TableName
			}) {
				tables = append(tables, p)
			}
		}
	}
	return tables, nil
}

// probe finds out, before the cutover locks, whether MySQL would build an
// index to add the foreign keys of a table to its new table. It adds the same
// foreign keys, in the same way, to an empty table created LIKE the new table,
// which references empty copies of the parent tables, and refuses the cutover
// if that changed the probe table's indexes. MySQL picks the index a foreign
// key uses from the table's definition, not its rows, so what it does to the
// probe table is what it would do to the new table; asking it avoids having
// to agree with its rule. The tables are copies nothing else uses, so the
// probe waits on no lock the application holds.
//
// addToNewTables only adds the foreign keys the probe added, to a new table
// whose definition is the probe table's.
func (f *foreignKeyCutover) probe(ctx context.Context) error {
	for _, t := range f.tables {
		t.probed = nil
		if err := f.probeTable(ctx, t); err != nil {
			return err
		}
	}
	return nil
}

func (f *foreignKeyCutover) probeTable(ctx context.Context, t *foreignKeyTable) (err error) {
	source, err := tableDefinition(ctx, f.db, t.table.TableName)
	if err != nil {
		return err
	}
	copies, err := foreignKeyCopies(source, t.table.TableName, t.oldTableName, t.stmt)
	if err != nil {
		return fmt.Errorf("%w: %w", check.ErrRefused, err)
	}
	if len(copies) == 0 {
		t.probed = &foreignKeyProbe{}
		return nil
	}
	probeName := utils.ForeignKeyProbeTableName(t.table.TableName)
	parentNames := make(map[string]string) // a parent, quoted, to its copy
	var parents []string                   // the copies, in order
	parentKeys := make([]string, len(copies))
	for i, c := range copies {
		schema, name := c.parent(t.table.SchemaName)
		key := sqlescape.MustEscapeSQL("%n.%n", schema, name)
		parentKeys[i] = key
		if _, ok := parentNames[key]; !ok {
			parentNames[key] = utils.ForeignKeyProbeParentName(t.table.TableName, len(parents))
			parents = append(parents, parentNames[key])
		}
	}
	// A probe that did not finish can leave its tables behind.
	if err := dropProbeTables(ctx, f.db, probeName, parents); err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, dropProbeTables(context.WithoutCancel(ctx), f.db, probeName, parents))
	}()
	for i, c := range copies {
		schema, name := c.parent(t.table.SchemaName)
		if err := dbconn.Exec(ctx, f.db, "CREATE TABLE IF NOT EXISTS %n LIKE %n.%n", parentNames[parentKeys[i]], schema, name); err != nil {
			return fmt.Errorf("could not create a copy of %s to probe the foreign keys of %s: %w", parentKeys[i], t.table.TableName, err)
		}
	}
	if err := dbconn.Exec(ctx, f.db, "CREATE TABLE %n LIKE %n", probeName, t.newTable.TableName); err != nil {
		return fmt.Errorf("could not create a copy of %s to probe the foreign keys of %s: %w", t.newTable.TableName, t.table.TableName, err)
	}
	before, err := tableDefinition(ctx, f.db, probeName)
	if err != nil {
		return err
	}
	var clauses, probeClauses []string
	for i, c := range copies {
		clause, err := restoreForeignKey(c.fk, utils.ForeignKeyProbeName(t.table.TableName, i), c.renames, parentNames[parentKeys[i]])
		if err != nil {
			return err
		}
		clauses = append(clauses, c.clause)
		probeClauses = append(probeClauses, "ADD "+clause)
	}
	if err := dbconn.ExecWithoutForeignKeyChecks(ctx, f.db, "ALTER TABLE %n %r, ALGORITHM=INPLACE, LOCK=NONE",
		probeName, sqlescape.RawSQL(strings.Join(probeClauses, ", "))); err != nil {
		return fmt.Errorf("could not probe the foreign keys of %s: %w", t.table.TableName, err)
	}
	after, err := tableDefinition(ctx, f.db, probeName)
	if err != nil {
		return err
	}
	if !sameIndexes(before, after) {
		var built []string
		for _, idx := range after.GetIndexes() {
			if !slices.ContainsFunc(before.GetIndexes(), func(b statement.Index) bool { return strings.EqualFold(b.Name, idx.Name) }) {
				built = append(built, idx.Name)
			}
		}
		return fmt.Errorf("%w: MySQL would build an index (%s) to add the foreign keys of %s to %s, with the cutover lock held: add an index to %s that leads with the foreign key's columns",
			check.ErrRefused, strings.Join(built, ", "), t.table.TableName, t.newTable.TableName, t.table.TableName)
	}
	t.probed = &foreignKeyProbe{clauses: clauses, definition: before}
	return nil
}

// dropProbeTables drops the tables of a probe: the probe table first, as its
// foreign keys reference the others.
func dropProbeTables(ctx context.Context, db *sql.DB, probeName string, parents []string) error {
	if err := dbconn.Exec(ctx, db, "DROP TABLE IF EXISTS %n", probeName); err != nil {
		return err
	}
	for _, p := range parents {
		if err := dbconn.Exec(ctx, db, "DROP TABLE IF EXISTS %n", p); err != nil {
			return err
		}
	}
	return nil
}

// addToNewTables adds the foreign keys of each table being altered to its new
// table, as utils.NewForeignKeyName names them, except those the ALTER drops.
// It must run with the table lock held and after the final flush: the foreign
// keys are read from the table as it is then, and added without checking the
// rows. It refuses unless they are the foreign keys the probe added and the
// new table's definition is the one the probe added them to: MySQL then adds
// them as a metadata change, as it did to the probe table. A changed index
// list afterwards is refused too, as a backstop.
func (f *foreignKeyCutover) addToNewTables(ctx context.Context, lock *dbconn.TableLock) error {
	exec := underLock(lock)
	for _, t := range f.tables {
		// A failed attempt can leave its copies behind.
		if _, err := dropForeignKeys(ctx, f.db, t.newTable.TableName, exec); err != nil {
			return err
		}
		source, err := tableDefinition(ctx, f.db, t.table.TableName)
		if err != nil {
			return err
		}
		copies, err := foreignKeyCopies(source, t.table.TableName, t.oldTableName, t.stmt)
		if err != nil {
			return fmt.Errorf("%w: %w", check.ErrRefused, err)
		}
		if len(copies) == 0 {
			continue
		}
		before, err := tableDefinition(ctx, f.db, t.newTable.TableName)
		if err != nil {
			return err
		}
		var clauses []string
		for _, c := range copies {
			clauses = append(clauses, c.clause)
		}
		if t.probed == nil || !slices.Equal(clauses, t.probed.clauses) {
			return fmt.Errorf("%w: the foreign keys of %s changed after they were probed", check.ErrRefused, t.table.TableName)
		}
		if !sameDefinition(before, t.probed.definition) {
			return fmt.Errorf("%w: the definition of %s changed after its foreign keys were probed", check.ErrRefused, t.newTable.TableName)
		}
		if err := exec(ctx, sqlescape.MustEscapeSQL("ALTER TABLE %n %r, ALGORITHM=INPLACE, LOCK=NONE",
			t.newTable.TableName, sqlescape.RawSQL(strings.Join(clauses, ", ")))); err != nil {
			return fmt.Errorf("could not add the foreign keys of %s to %s: %w", t.table.TableName, t.newTable.TableName, err)
		}
		after, err := tableDefinition(ctx, f.db, t.newTable.TableName)
		if err != nil {
			return err
		}
		if !sameIndexes(before, after) {
			return fmt.Errorf("%w: adding the foreign keys of %s to %s changed the indexes of %s", check.ErrRefused, t.table.TableName, t.newTable.TableName, t.newTable.TableName)
		}
		f.logger.Info("added foreign keys to the new table", "table", t.newTable.TableName, "foreign-keys", len(clauses))
	}
	return nil
}

// removeFromNewTables drops every foreign key of the new tables. A new table
// that no longer exists, because the cutover renamed it, has none to drop.
func (f *foreignKeyCutover) removeFromNewTables(ctx context.Context, exec execFunc) error {
	var errs []error
	for _, t := range f.tables {
		if _, err := dropForeignKeys(ctx, f.db, t.newTable.TableName, exec); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// settle runs after the cutover's RENAME TABLE: it drops the foreign keys of
// each old table, which hold the original names and would keep checking the
// parent tables' changes against rows that are no longer written to, and then
// gives each table's foreign keys those names back. dropExec runs the DROPs,
// restoreExec the renames, which must run with foreign key checks off.
func (f *foreignKeyCutover) settle(ctx context.Context, dropExec, restoreExec execFunc) error {
	for _, t := range f.tables {
		dropped, err := dropForeignKeys(ctx, f.db, t.oldTableName, dropExec)
		if err != nil {
			return err
		}
		names, err := restoreForeignKeyNames(ctx, f.db, t.table.TableName, restoreExec)
		if err != nil {
			return err
		}
		if dropped > 0 || len(names) > 0 {
			f.logger.Info("moved the foreign keys' names from the old table to the new one",
				"table", t.table.TableName,
				"old-table", t.oldTableName,
				"renamed-from", names,
			)
		}
	}
	f.settled = true
	return nil
}

// settleAfterUnlock settles the names when the cutover could not do it under
// the lock, with connections from the pool. The migration has succeeded by
// then, so a failure is logged rather than returned.
func (f *foreignKeyCutover) settleAfterUnlock(ctx context.Context) {
	dropExec := func(ctx context.Context, stmt string) error {
		return f.forceExec(ctx, stmt)
	}
	restoreExec := func(ctx context.Context, stmt string) error {
		return dbconn.ExecWithoutForeignKeyChecks(ctx, f.db, "%r", sqlescape.RawSQL(stmt))
	}
	if err := f.settle(ctx, dropExec, restoreExec); err != nil {
		f.logger.Error("migration successful but the foreign keys of the old table could not be dropped, or the new table's foreign keys could not be given their names back: drop the old table's foreign keys and rename the new table's by hand",
			"error", err,
		)
	}
}

// removeAfterFailure drops the foreign keys a failed cutover may have left on
// the new tables, with connections from the pool. A failure is logged: the
// next run's setup drops them too.
func (f *foreignKeyCutover) removeAfterFailure(ctx context.Context) {
	if err := f.removeFromNewTables(ctx, f.forceExec); err != nil {
		f.logger.Error("could not drop the foreign keys of the new table after a failed cutover: until they are dropped, changes to the parent tables are checked against the new table too",
			"error", err,
		)
	}
}

// forceExec runs stmt on a connection from the pool, killing the sessions
// that block it on the new and old tables or a referenced table.
func (f *foreignKeyCutover) forceExec(ctx context.Context, stmt string) error {
	tables, err := f.referenced(ctx)
	if err != nil {
		return err
	}
	for _, t := range f.tables {
		tables = append(tables, t.newTable, table.NewTableInfo(f.db, t.table.SchemaName, t.oldTableName))
	}
	return dbconn.ForceExec(ctx, f.db, tables, f.dbConfig, f.logger, "%r", sqlescape.RawSQL(stmt))
}

// referencedTables returns the tables the foreign keys of tbl reference.
func referencedTables(ctx context.Context, db *sql.DB, tbl *table.TableInfo) ([]*table.TableInfo, error) {
	def, err := tableDefinition(ctx, db, tbl.TableName)
	if err != nil {
		return nil, err
	}
	var tables []*table.TableInfo
	for _, fk := range def.GetConstraints() {
		if fk.Type != "FOREIGN KEY" || fk.References == nil {
			continue
		}
		schema := fk.References.Schema
		if schema == "" {
			schema = tbl.SchemaName
		}
		tables = append(tables, table.NewTableInfo(db, schema, fk.References.Table))
	}
	return tables, nil
}

// copyForeignKeys adds the foreign keys of table sourceName to its new table
// newName, under the names utils.NewForeignKeyName gives them, so the ALTER
// that setup applies next is checked against them. The new table is empty, so
// the checks cost nothing: leaving them on keeps MySQL validating the
// definitions. oldName is the name the cutover renames the source table to.
//
// Adding a named foreign key replaces the index MySQL created for the original
// one with an index named after the constraint. The indexes are renamed back
// afterwards, so the new table's indexes are the source table's.
func copyForeignKeys(ctx context.Context, db *sql.DB, logger *slog.Logger, exec execFunc, sourceName, newName, oldName string) error {
	source, err := tableDefinition(ctx, db, sourceName)
	if err != nil {
		return err
	}
	copies, err := foreignKeyCopies(source, sourceName, oldName, nil)
	if err != nil {
		return err
	}
	if len(copies) == 0 {
		return nil
	}
	var clauses []string
	for _, c := range copies {
		clauses = append(clauses, c.clause)
	}
	if err := exec(ctx, sqlescape.MustEscapeSQL("ALTER TABLE %n %r", newName, sqlescape.RawSQL(strings.Join(clauses, ", ")))); err != nil {
		return fmt.Errorf("could not copy the foreign keys of %s to %s: %w", sourceName, newName, err)
	}
	logger.Info("copied foreign keys to the new table to check the ALTER against them", "table", newName, "foreign-keys", len(clauses))
	return restoreIndexNames(ctx, db, exec, source, newName)
}

// foreignKeyCopy is the copy of a foreign key, to add to a new table.
type foreignKeyCopy struct {
	fk      statement.Constraint // the foreign key it copies
	renames map[string]string    // the columns the ALTER renames
	clause  string               // the ADD clause
}

// parent returns the schema and name of the table the foreign key
// references; schema is the schema of the table it belongs to.
func (c foreignKeyCopy) parent(schema string) (string, string) {
	if c.fk.References.Schema != "" {
		schema = c.fk.References.Schema
	}
	return schema, c.fk.References.Table
}

// foreignKeyCopies returns the copy of each foreign key of source, the
// definition of table tableName, named by utils.NewForeignKeyName. With stmt,
// the foreign keys it drops are left out and the columns it renames are
// renamed.
func foreignKeyCopies(source *statement.CreateTable, tableName, oldName string, stmt *statement.AbstractStatement) ([]foreignKeyCopy, error) {
	var dropped []string
	renames := make(map[string]string)
	if stmt != nil {
		dropped = stmt.ForeignKeysDropped()
		for from, to := range stmt.ColumnRenameMap() {
			renames[strings.ToLower(from)] = to
		}
	}
	var copies []foreignKeyCopy
	for _, fk := range source.GetConstraints() {
		if fk.Type != "FOREIGN KEY" {
			continue
		}
		if slices.ContainsFunc(dropped, func(name string) bool { return strings.EqualFold(name, fk.Name) }) {
			continue
		}
		name := utils.NewForeignKeyName(tableName, fk.Name)
		for _, n := range []string{name, utils.RenamedForeignKeyName(fk.Name, tableName, oldName)} {
			if len(n) > utils.MaxTableNameLength {
				return nil, fmt.Errorf("foreign key %s of table %s cannot be copied: the migration would have to name it %s, which is longer than MySQL's %d character limit",
					fk.Name, tableName, n, utils.MaxTableNameLength)
			}
		}
		clause, err := restoreForeignKey(fk, name, renames, "")
		if err != nil {
			return nil, err
		}
		if fk.References == nil {
			return nil, fmt.Errorf("foreign key %s of table %s has no parsed reference", fk.Name, tableName)
		}
		copies = append(copies, foreignKeyCopy{fk: fk, renames: renames, clause: "ADD " + clause})
	}
	return copies, nil
}

// dropForeignKeys drops every foreign key of tableName and returns how many it
// dropped. A table that does not exist has none.
func dropForeignKeys(ctx context.Context, db *sql.DB, tableName string, exec execFunc) (int, error) {
	def, err := tableDefinition(ctx, db, tableName)
	if errors.Is(err, &mysql.MySQLError{Number: parsermysql.ErrNoSuchTable}) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var clauses []string
	for _, fk := range def.GetConstraints() {
		if fk.Type == "FOREIGN KEY" {
			clauses = append(clauses, sqlescape.MustEscapeSQL("DROP FOREIGN KEY %n", fk.Name))
		}
	}
	if len(clauses) == 0 {
		return 0, nil
	}
	if err := exec(ctx, sqlescape.MustEscapeSQL("ALTER TABLE %n %r, ALGORITHM=INPLACE, LOCK=NONE",
		tableName, sqlescape.RawSQL(strings.Join(clauses, ", ")))); err != nil {
		return 0, fmt.Errorf("could not drop the foreign keys of %s: %w", tableName, err)
	}
	return len(clauses), nil
}

// restoreIndexNames renames each index of newName that copyForeignKeys
// replaced back to the name it has on source: an index source does not have,
// on the same columns as an index of source that newName no longer has.
func restoreIndexNames(ctx context.Context, db *sql.DB, exec execFunc, source *statement.CreateTable, newName string) error {
	newTable, err := tableDefinition(ctx, db, newName)
	if err != nil {
		return err
	}
	has := func(def *statement.CreateTable, name string) bool {
		return slices.ContainsFunc(def.GetIndexes(), func(i statement.Index) bool { return strings.EqualFold(i.Name, name) })
	}
	var clauses []string
	for _, idx := range source.GetIndexes() {
		if has(newTable, idx.Name) {
			continue
		}
		i := slices.IndexFunc(newTable.GetIndexes(), func(c statement.Index) bool {
			return !has(source, c.Name) && c.Type == idx.Type && reflect.DeepEqual(c.ColumnList, idx.ColumnList)
		})
		if i < 0 {
			return fmt.Errorf("index %s of %s is missing from %s after its foreign keys were copied", idx.Name, source.GetTableName(), newName)
		}
		clauses = append(clauses, sqlescape.MustEscapeSQL("RENAME INDEX %n TO %n", newTable.GetIndexes()[i].Name, idx.Name))
	}
	if len(clauses) == 0 {
		return nil
	}
	return exec(ctx, sqlescape.MustEscapeSQL("ALTER TABLE %n %r", newName, sqlescape.RawSQL(strings.Join(clauses, ", "))))
}

// sameIndexes reports whether a and b have the same indexes, in every detail
// the parsed definition holds. Their order is not compared: adding a foreign
// key can move an index it does not change.
func sameIndexes(a, b *statement.CreateTable) bool {
	byName := func(x, y statement.Index) int {
		return strings.Compare(strings.ToLower(x.Name), strings.ToLower(y.Name))
	}
	ia, ib := slices.Clone(a.Indexes), slices.Clone(b.Indexes)
	slices.SortFunc(ia, byName)
	slices.SortFunc(ib, byName)
	return sameJSON(ia, ib)
}

// sameDefinition reports whether a and b have the same columns, indexes and
// partitioning, whatever their names, table options and other constraints.
func sameDefinition(a, b *statement.CreateTable) bool {
	return sameJSON(a.Columns, b.Columns) && sameIndexes(a, b) && sameJSON(a.Partition, b.Partition)
}

// sameJSON reports whether a and b encode to the same JSON. The parsed
// definition leaves out of its encoding only the AST it was parsed from.
func sameJSON(a, b any) bool {
	ja, errA := json.Marshal(a)
	jb, errB := json.Marshal(b)
	return errA == nil && errB == nil && string(ja) == string(jb)
}

// foreignKeyRenames maps the lower-cased name of each foreign key of table
// tableName to the name of its copy on the new table.
func foreignKeyRenames(ctx context.Context, db *sql.DB, tableName string) (map[string]string, error) {
	source, err := tableDefinition(ctx, db, tableName)
	if err != nil {
		return nil, err
	}
	renames := make(map[string]string)
	for _, fk := range source.GetConstraints() {
		if fk.Type == "FOREIGN KEY" {
			renames[strings.ToLower(fk.Name)] = utils.NewForeignKeyName(tableName, fk.Name)
		}
	}
	return renames, nil
}

// restoreForeignKeyNames renames the foreign keys of tableName, which the
// cutover has just put in place, back to the names of the foreign keys they
// copy. The old table must no longer hold those names.
//
// The names to restore are read off the table. Every foreign key of the
// table is a copy (the ALTER cannot add one), so a name of the form _<name>_new
// is the copy of <name>; one with the table's generated prefix was renamed by
// the cutover already, and one with the new table's generated prefix should
// have been.
//
// exec must run with foreign key checks off, so the ALTER drops and re-adds
// each foreign key as a metadata change rather than copying the table, without
// checking the rows: the same foreign key held them a moment earlier, under
// the other name. It returns the names it renamed from.
func restoreForeignKeyNames(ctx context.Context, db *sql.DB, tableName string, exec execFunc) ([]string, error) {
	def, err := tableDefinition(ctx, db, tableName)
	if err != nil {
		return nil, err
	}
	newName := utils.NewTableName(tableName)
	var clauses, names []string
	for _, fk := range def.GetConstraints() {
		if fk.Type != "FOREIGN KEY" {
			continue
		}
		original := utils.RenamedForeignKeyName(fk.Name, newName, tableName)
		if original == fk.Name {
			// Not a generated name the cutover failed to rename: the copy of
			// a named foreign key, or a generated name the cutover renamed.
			trimmed, hasPrefix := strings.CutPrefix(fk.Name, "_")
			name, hasSuffix := strings.CutSuffix(trimmed, "_new")
			if !hasPrefix || !hasSuffix || name == "" || strings.HasPrefix(fk.Name, tableName+"_ibfk_") {
				continue
			}
			original = name
		}
		clause, err := restoreForeignKey(fk, original, nil, "")
		if err != nil {
			return nil, err
		}
		clauses = append(clauses, sqlescape.MustEscapeSQL("DROP FOREIGN KEY %n", fk.Name), "ADD "+clause)
		names = append(names, fk.Name)
	}
	if len(clauses) == 0 {
		return nil, nil
	}
	if err := exec(ctx, sqlescape.MustEscapeSQL("ALTER TABLE %n %r, ALGORITHM=INPLACE, LOCK=NONE",
		tableName, sqlescape.RawSQL(strings.Join(clauses, ", ")))); err != nil {
		return nil, err
	}
	return names, nil
}

// restoreForeignKey returns the definition of foreign key fk, as an ALTER
// TABLE .. ADD takes it, named name, with its columns renamed through renames
// (lower-cased old name to new name). With parent, it references that table,
// in the current schema, instead.
func restoreForeignKey(fk statement.Constraint, name string, renames map[string]string, parent string) (string, error) {
	if fk.Raw == nil {
		return "", fmt.Errorf("foreign key %s has no parsed definition", fk.Name)
	}
	raw := *fk.Raw
	raw.Name = name
	raw.Keys = make([]*ast.IndexPartSpecification, len(fk.Raw.Keys))
	for i, key := range fk.Raw.Keys {
		k := *key
		if k.Column != nil {
			if to, ok := renames[k.Column.Name.L]; ok {
				col := *k.Column
				col.Name = ast.NewCIStr(to)
				k.Column = &col
			}
		}
		raw.Keys[i] = &k
	}
	if parent != "" {
		if raw.Refer == nil {
			return "", fmt.Errorf("foreign key %s has no parsed reference", fk.Name)
		}
		refer := *raw.Refer
		refer.Table = &ast.TableName{Name: ast.NewCIStr(parent)}
		raw.Refer = &refer
	}
	var sb strings.Builder
	if err := raw.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
		return "", fmt.Errorf("could not restore foreign key %s: %w", fk.Name, err)
	}
	return sb.String(), nil
}
