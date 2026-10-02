# Statement

The statement package provides SQL statement parsing and analysis capabilities for Spirit. It wraps [pkg/parser](../parser/README.md) (Spirit's MySQL-only fork of the TiDB parser) to extract structured information from DDL statements and determine their safety characteristics for online schema changes.

## Design Philosophy

Spirit needs to understand DDL statements to:
1. **Validate** that statements are supported for online migration (i.e., not an INSERT statement)
2. **Extract** table names, schema names, and ALTER clauses
3. **Analyze** whether operations are safe for INPLACE algorithm
4. **Transform** statements (e.g., rewrite `CREATE INDEX` to `ALTER TABLE`)
5. **Parse** CREATE TABLE statements into structured data for comparison
6. **Normalize** parsed CREATE TABLE definitions to MySQL's canonical form so equivalent schemas compare equal (see [Normalization](#normalization))

Rather than implementing a parser from scratch, Spirit maintains a fork of the TiDB parser (see [pkg/parser](../parser/README.md)), which provides:
- Battle-tested SQL parsing compatible with MySQL syntax
- AST (Abstract Syntax Tree) representation of statements
- Ability to restore modified ASTs back to SQL

The statement package adds Spirit-specific logic on top of the parser, such as safety analysis and structured CREATE TABLE parsing.

## Core Types

### AbstractStatement

`AbstractStatement` represents a parsed DDL statement with extracted metadata:

```go
type AbstractStatement struct {
    Schema    string          // Schema name (if fully qualified)
    Table     string          // Table name
    Alter     string          // ALTER clause (empty for non-ALTER statements)
    Statement string          // Original SQL statement
    StmtNode  *ast.StmtNode   // Parsed AST node
}
```

**Key Points:**
- For multi-table statements (e.g., `DROP TABLE t1, t2`), only the first table is stored in `Table`
- `Alter` contains the normalized ALTER clause without `ALTER TABLE table_name` prefix
- `StmtNode` provides access to the full AST for advanced operations

### CreateTable

`CreateTable` represents a parsed CREATE TABLE statement with structured access to all components:

```go
type CreateTable struct {
    Raw          *ast.CreateTableStmt
    TableName    string
    Temporary    bool
    IfNotExists  bool
    Columns      Columns
    Indexes      Indexes
    Constraints  Constraints
    TableOptions *TableOptions
    Partition    *PartitionOptions
}
```

This structured representation makes it easy to:
- Compare table definitions
- Extract specific columns or indexes
- Generate modified CREATE TABLE statements
- Validate table structure

## Supported Statements

### ALTER TABLE

The primary statement type for Spirit migrations:

```go
stmts, err := statement.New("ALTER TABLE t1 ADD COLUMN c INT")
// stmts[0].Table = "t1"
// stmts[0].Alter = "ADD COLUMN `c` INT"
```

**Features:**
- Normalizes ALTER clauses (adds backticks, standardizes formatting)
- Supports fully qualified table names (`schema.table`)
- Can parse multiple ALTER statements in one call
- Parses ALGORITHM and LOCK clauses but does not reject them; callers should invoke `AlterContainsUnsupportedClause` on the resulting `AbstractStatement` if they need to enforce that these clauses are not present (Spirit manages these)
- Detects column renames via `ColumnRenameMap()`, which returns a map of old→new column names for both `RENAME COLUMN` and `CHANGE COLUMN` syntax

### CREATE TABLE

Supports CREATE TABLE for table creation operations:

```go
stmts, err := statement.New("CREATE TABLE t1 (id INT PRIMARY KEY)")
// stmts[0].Table = "t1"
// stmts[0].Alter = "" (empty for non-ALTER)
```

For structured parsing:

```go
ct, err := statement.ParseCreateTable("CREATE TABLE t1 (id INT PRIMARY KEY)")
// ct.TableName = "t1"
// ct.Columns[0].Name = "id"
// ct.Columns[0].Type = "int"
// ct.Columns[0].PrimaryKey = true
```

### CREATE INDEX

Automatically rewritten to ALTER TABLE:

```go
stmts, err := statement.New("CREATE INDEX idx ON t1 (a)")
// stmts[0].Table = "t1"
// stmts[0].Alter = "ADD INDEX idx (a)"
// stmts[0].Statement = "/* rewritten from CREATE INDEX */ ALTER TABLE `t1` ADD INDEX idx (a)"
```

**Limitations:**
- Functional indexes cannot be converted (use `ALTER TABLE ADD INDEX` directly). See [issue 444](https://github.com/block/spirit/issues/444).

### DROP TABLE

Supports DROP TABLE operations:

```go
stmts, err := statement.New("DROP TABLE t1")
// stmts[0].Table = "t1"
// stmts[0].Alter = "" (empty for non-ALTER)
```

**Validation:**
- Multi-table drops must use the same schema (e.g., `DROP TABLE test.t1, test.t2` is valid, but `DROP TABLE test.t1, prod.t2` is not)

### RENAME TABLE

Supports RENAME TABLE operations:

```go
stmts, err := statement.New("RENAME TABLE t1 TO t2")
// stmts[0].Table = "t1"
// stmts[0].Alter = "" (empty for non-ALTER)
```

**Validation:**
- Cannot rename across schemas (e.g., `RENAME TABLE test.t1 TO prod.t2` is rejected)

## Safety Analysis

The statement package provides methods to determine if ALTER operations are safe for online execution.

### AlgorithmInplaceConsideredSafe

Determines if an ALTER statement can use MySQL's INPLACE algorithm safely:

```go
stmt := statement.MustNew("ALTER TABLE t1 RENAME INDEX a TO b")[0]
err := stmt.AlgorithmInplaceConsideredSafe()
// err == nil (safe - metadata-only operation)

stmt = statement.MustNew("ALTER TABLE t1 ADD COLUMN c INT")[0]
err = stmt.AlgorithmInplaceConsideredSafe()
// err == ErrUnsafeForInplace (unsafe - requires table rebuild)
```

This feature exists because some DDL changes in MySQL only respond to the `INPLACE` DDL assertion, even though they are actually `INSTANT` operations (metadata-only). Since not all `INPLACE` operations are safe for online execution, we explicitly parse the statement to identify only known safe operations. See [https://bugs.mysql.com/bug.php?id=113355](https://bugs.mysql.com/bug.php?id=113355).

### AlterContainsUnsupportedClause

Checks for clauses that conflict with Spirit's operation:

```go
stmt := statement.MustNew("ALTER TABLE t1 ADD INDEX (a), ALGORITHM=INPLACE")[0]
err := stmt.AlterContainsUnsupportedClause()
// err != nil (ALGORITHM clause not allowed)
```

**Unsupported Clauses:**
- `ALGORITHM=...` (Spirit manages algorithm selection)
- `LOCK=...` (Spirit manages locking strategy)

### AlterContainsAddUnique

Detects if an ALTER adds a UNIQUE index:

```go
stmt := statement.MustNew("ALTER TABLE t1 ADD UNIQUE INDEX (email)")[0]
err := stmt.AlterContainsAddUnique()
// err == ErrAlterContainsUnique
```

This is used to customize the error message if a checksum operation fails. This is because adding a `UNIQUE` index on non-unique data will result in a checksum failure, and it's helpful to hint this out to the user.

## CREATE TABLE Parsing

The package provides detailed parsing of CREATE TABLE statements into structured data. This is extensively used by the `lint` package.

### Basic Usage

```go
ct, err := statement.ParseCreateTable(`
    CREATE TABLE users (
        id INT PRIMARY KEY AUTO_INCREMENT,
        email VARCHAR(255) NOT NULL UNIQUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        status ENUM('active', 'inactive') DEFAULT 'active'
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
`)

// Access columns
for _, col := range ct.Columns {
    fmt.Printf("%s: %s\n", col.Name, col.Type)
}

// Access indexes
for _, idx := range ct.Indexes {
    fmt.Printf("%s (%s): %v\n", idx.Name, idx.Type, idx.Columns)
}

// Access table options
if ct.TableOptions.Engine != nil {
    fmt.Printf("Engine: %s\n", *ct.TableOptions.Engine)
}
```

### Column Information

Each `Column` provides detailed information:

```go
type Column struct {
    Raw        *ast.ColumnDef    // Raw AST node from parser
    Name       string
    Type       string            // "int", "varchar", "decimal", etc.
    Length     *int              // For VARCHAR(100), Length = 100
    Precision  *int              // For DECIMAL(10,2), Precision = 10
    Scale      *int              // For DECIMAL(10,2), Scale = 2
    Unsigned   *bool
    EnumValues []string          // For ENUM('a','b'), EnumValues = ["a", "b"]
    SetValues  []string          // For SET('x','y'), SetValues = ["x", "y"]
    Nullable   bool
    Default    *string           // the value MySQL stores, as SHOW CREATE TABLE reports it (normalized; compared)
    DefaultAsWritten *DefaultLiteral // the literal as the schema spelled it (emitted); nil for an expression default
    OnUpdate   *string           // ON UPDATE CURRENT_TIMESTAMP[(n)] for TIMESTAMP/DATETIME
    GeneratedExpr   *string      // Expression for GENERATED ALWAYS AS (...) columns
    GeneratedStored bool         // true = STORED, false = VIRTUAL
    Checks     []ColumnCheck     // Column-level CHECKs (name, expression, NOT ENFORCED); hoisted into Constraints
    SRID       *uint32           // SRID attribute for spatial columns
    Invisible  bool              // INVISIBLE column (8.0.23+); explicit VISIBLE is not recorded
    NotSecondary bool            // NOT SECONDARY
    ColumnFormat *string         // COLUMN_FORMAT FIXED|DYNAMIC; DEFAULT is not recorded
    Storage    *string           // STORAGE DISK|MEMORY; DEFAULT is not recorded
    SecondaryEngineAttribute *string // SECONDARY_ENGINE_ATTRIBUTE JSON as written; compared as JSON
    AutoInc    bool
    PrimaryKey bool              // Column-level PRIMARY KEY
    Unique     bool              // Column-level UNIQUE
    Comment    *string
    Charset    *string
    Collation  *string
    Options    map[string]string // Additional column options
}
```

**Example:**

```go
col := ct.Columns.ByName("email")
// col.Name = "email"
// col.Type = "varchar"
// col.Length = 255
// col.Nullable = false
// col.Unique = true
```

### Index Information

Each `Index` provides:

```go
type Index struct {
    Raw          *ast.Constraint   // Raw AST node from parser
    Name         string
    Type         string            // "PRIMARY KEY", "UNIQUE", "INDEX", "FULLTEXT", "SPATIAL"
    Columns      []string
    Invisible    *bool
    Using        *string           // "BTREE", "HASH", "RTREE"
    Comment      *string
    KeyBlockSize *uint64
    ParserName   *string           // For FULLTEXT indexes
    Options      map[string]string // Additional index options
    SecondaryEngineAttribute *string // JSON text as written; compared as JSON
}
```

### Table Options

`TableOptions` holds every table option `SHOW CREATE TABLE` reports. An
option MySQL treats as unset (`STATS_PERSISTENT=DEFAULT`, `KEY_BLOCK_SIZE=0`,
`SECONDARY_ENGINE_ATTRIBUTE=''`, `ROW_FORMAT=DEFAULT`, ...) parses as nil/false,
and `Diff` emits that same value to clear an option the target no longer
declares. `ROW_FORMAT` (and with it `KEY_BLOCK_SIZE`) is only compared under
`IgnoreRowFormat: false`; the clearing `ROW_FORMAT=DEFAULT` rebuilds the table,
as every row format change does, and fires once. A table `KEY_BLOCK_SIZE`
change on a table that stays compressed does not converge: MySQL keeps the old
size on every existing index and reports it there. Compressed tables are out of
scope (Spirit targets Aurora, which does not support them), so the diff does not
re-create the indexes for it. If a follow-up diff finds the old size on the
primary key, it returns an unsupported-primary-key-options error and no
statements. An option-only change to a primary key's `KEY_BLOCK_SIZE` or
`SECONDARY_ENGINE_ATTRIBUTE` is likewise rejected: MySQL ignores it in a combined
`DROP PRIMARY KEY, ADD PRIMARY KEY`, and a primary key cannot use the temporary
name swap used for secondary indexes. Primary key comments still use the
combined statement, which MySQL applies.

```go
type TableOptions struct {
    Engine, Charset, Collation, Comment, RowFormat *string
    AutoIncrement    *uint64
    KeyBlockSize     *uint64 // compressed page size; diffed with ROW_FORMAT (IgnoreRowFormat)
    AutoextendSize   *uint64 // bytes: AUTOEXTEND_SIZE=4M parses as 4194304
    StatsPersistent  *bool
    StatsAutoRecalc  *bool
    StatsSamplePages *uint64
    PackKeys         *bool
    Checksum         bool
    DelayKeyWrite    bool
    AvgRowLength, MinRows, MaxRows *uint64
    SecondaryEngineAttribute *string // JSON text as written; compared as JSON
}
```

**Example:**

```go
idx := ct.Indexes.ByName("PRIMARY")
// idx.Type = "PRIMARY KEY"
// idx.Columns = ["id"]

idx = ct.Indexes.ByName("email")
// idx.Type = "UNIQUE"
// idx.Columns = ["email"]
```

### Constraint Information

Each `Constraint` represents CHECK or FOREIGN KEY constraints:

```go
type Constraint struct {
    Raw         *ast.Constraint      // Raw AST node from parser
    Name        string
    Type        string                // "CHECK", "FOREIGN KEY"
    Columns     []string
    Expression  *string               // For CHECK constraints
    References  *ForeignKeyReference  // For FOREIGN KEY constraints
    Definition  *string               // Full constraint definition
    NotEnforced bool                  // For CHECK constraints: true when NOT ENFORCED
    Options     map[string]any        // Additional constraint options
}
```

A `ForeignKeyReference` records the referenced `Schema` when the reference is qualified (`REFERENCES db.parent`) and leaves it empty otherwise. MySQL qualifies a reference in `SHOW CREATE TABLE` only when the parent is in another schema, and a reference qualified with the table's own schema reads back unqualified, so an empty `Schema` means the table's own schema — which a parsed `CREATE TABLE` does not know. Two references therefore differ on schema only when both are qualified and name different schemas; a desired `REFERENCES db2.parent` is not told apart from a live `REFERENCES parent`.

### Partition Information

For partitioned tables, `PartitionOptions` provides:

```go
type PartitionOptions struct {
    Type         string                // "RANGE", "LIST", "HASH", "KEY", "SYSTEM_TIME"
    Expression   *string               // For HASH, RANGE and LIST
    Columns      []string              // For KEY, RANGE COLUMNS, LIST COLUMNS
    Linear       bool
    KeyAlgorithm uint64                // For KEY: 1, or 0 for MySQL's default (2)
    Partitions   uint64
    Definitions  []PartitionDefinition
    SubPartition *SubPartitionOptions
}
```

Partitioning is compared as a whole, and a difference is emitted as one clause:

| Change | Clause |
|---|---|
| HASH/KEY partition count only | `ADD PARTITION PARTITIONS n` / `COALESCE PARTITION n` |
| RANGE/LIST partitions appended after the existing ones | `ADD PARTITION (...)` (in-place, metadata-only) |
| A contiguous run of RANGE/LIST partitions split, merged, renamed or re-bounded, when the run keeps its outer RANGE bound or its set of LIST values | `REORGANIZE PARTITION ... INTO (...)` |
| Anything else (type, expression, subpartitioning, a shrunk range, a dropped LIST value) | a complete `PARTITION BY`, which replaces the existing partitioning |
| Partitioning removed | `REMOVE PARTITIONING` |

`ADD PARTITION`, `COALESCE PARTITION` and `REORGANIZE PARTITION` can't share an `ALTER TABLE` with other clauses. When columns, indexes or table options change too, an `ADD PARTITION (...)` append is emitted as a second statement (it is still metadata-only), unless a column the partitioning reads changes (directly, or through a generated column it reads, followed transitively): the first statement could then convert a stored value past the last existing partition before the second adds the new one. That case, and the other standalone clauses, become a `PARTITION BY` in the same statement. `PARTITION BY` and `REMOVE PARTITIONING` go last in that statement, separated by a space: MySQL rejects them after a comma.

`DROP PARTITION` is never emitted, because it deletes the partition's rows. A LIST `REORGANIZE` that leaves out a value deletes the rows holding it without an error, so it is only emitted when the value set is unchanged and every value in it is a constant. A value left as an expression (see `partitionBoundConstantNormalizer` below) disqualifies the run even when its text is unchanged: `UNIX_TIMESTAMP` reads the session time zone, so the same text can name a different value when the `REORGANIZE` runs. Otherwise the change is a `PARTITION BY`, which fails with error 1526 if a row has no partition to go to.

A `PARTITION BY` carries the `SUBPARTITION BY` clause, partition and subpartition comments and storage options (`DATA DIRECTORY`, `INDEX DIRECTORY`, `MAX_ROWS`, `MIN_ROWS`, `TABLESPACE`, `NODEGROUP`), and any explicitly named subpartitions; `ADD PARTITION` and `REORGANIZE PARTITION` carry the same per-partition options. InnoDB rejects `INDEX DIRECTORY` (error 1031) and any tablespace other than `innodb_file_per_table` (error 1478); they are compared and emitted anyway, so a desired schema that sets one fails rather than being silently dropped. The per-partition `ENGINE` clause is the one thing deliberately **not** compared: MySQL requires every partition to use the table's engine, so it carries no information, yet `SHOW CREATE TABLE` always prints it while authored SQL does not.

### Statement Planning

`Diff` folds every change into one `ALTER TABLE` where MySQL lets it, with the clauses in the order columns (`DROP`, then `ADD`/`MODIFY` in target order), indexes, constraints, table options, partitioning. The exceptions are the changes MySQL rejects or silently ignores in that shape. Each is planned so that the emitted statements apply in order and a second diff against the result is empty:

| Change | Plan | Why |
|---|---|---|
| A column changing to or from a `VIRTUAL` generated column (`VIRTUAL` ↔ `STORED`, regular → `VIRTUAL`) | `DROP COLUMN` and `ADD COLUMN` in the same `ALTER`, at the column's target position | MySQL refuses the `MODIFY` (error 3106, "Changing the STORED status"). Nothing is lost: a generated column holds no data of its own and MySQL recomputes it. Regular ↔ `STORED` stays a `MODIFY` |
| A `VIRTUAL` generated column becoming a regular column | Two statements: the first rebuilds it as a `STORED` generated column with its old expression (the `DROP`+`ADD` above, with whatever reads it); the second is the ordinary diff from that intermediate table, in which the column is a `MODIFY` that carries its type and attribute changes, next to everything else the diff emits | A `DROP`+`ADD` of the regular column would leave it `NULL`, because a `VIRTUAL` column has no data for the added column to inherit. MySQL fills a `STORED` column from its expression when it is added, and keeps the values of a `STORED` column that a `MODIFY` turns into a regular one, even in the same `ALTER` as the `DROP` of a column the expression read |
| A functional index or `CHECK` constraint that reads a rebuilt column | `DROP INDEX`/`DROP CHECK` and the matching `ADD` in the same `ALTER`, even when the definition is unchanged | Either blocks the `DROP COLUMN` (errors 3837 and 3959) unless the same statement drops it. An index that names the column as a plain key part survives the rebuild on its own. A generated column that reads a rebuilt column is rebuilt with it (error 3108), unless the target makes it a regular column: then it is a `MODIFY` in the same `ALTER`, which keeps a `STORED` column's values. A foreign key on a rebuilt column is left alone, so MySQL's error 1828 surfaces rather than a referential constraint being dropped |
| An `SRID` attribute added, removed or changed on a column with a spatial index | `DROP INDEX` as a statement of its own **before** the primary `ALTER`; the `MODIFY COLUMN` and the `ADD SPATIAL INDEX` from the target share the primary `ALTER` | MySQL refuses the SRID change while the index exists, even when the same `ALTER` drops it (error 3644). Any other change to the column is a plain `MODIFY` |
| An index whose column list is unchanged but whose `WITH PARSER`, `KEY_BLOCK_SIZE` or `SECONDARY_ENGINE_ATTRIBUTE` differs | A swap after the primary `ALTER`: `ADD INDEX` under a temporary name (`_<name>_new`, numbered when that is taken) and `DROP INDEX` of the old one, every swapped index in one statement (a `FULLTEXT` one in a statement of its own, error 1795), then a final statement that `RENAME INDEX`es every replacement back | MySQL pairs a same-name, same-columns `DROP`+`ADD` in one `ALTER` and keeps the old index, ignoring the option change. Adding the replacement before dropping the old index is what gets the change through where a standalone `DROP INDEX` is refused: the only index on an `AUTO_INCREMENT` column (error 1075) or the index a foreign key depends on (error 1553). A run that stops between the two statements leaves the index under the temporary name; the next diff drops it and adds the target's. A primary key with one of these option changes returns an unsupported-change error and no statements: a same-column `DROP PRIMARY KEY, ADD PRIMARY KEY` ignores the option, and a primary key cannot take a temporary name |
| More than one `FULLTEXT` index added (or rebuilt) | The first `ADD FULLTEXT INDEX` stays in the primary `ALTER`; each further one is a statement of its own after it | InnoDB builds one FULLTEXT index per `ALTER TABLE` (error 1795) |
| A foreign key whose definition changes under the same name, or a name differing only in case | Re-created under a replacement name in the primary `ALTER`: `DROP FOREIGN KEY` of the old constraint and `ADD CONSTRAINT` of the new definition as `_<name>_new` (numbered when that is taken), which it keeps. A foreign key changed under a new name fits the primary `ALTER` under its own | MySQL rejects a same-name `DROP`+`ADD FOREIGN KEY` in one `ALTER` (error 1826, "Duplicate foreign key constraint name"), and foreign key names are case-insensitive and unique per schema. Dropping in one statement and re-adding in the next would leave the child table unconstrained in between: a row with no parent inserted then is accepted, and the re-add fails on it. Moving the new definition to its own name afterwards would be a second `ADD FOREIGN KEY`, a table copy when `foreign_key_checks` is on, for a name the diff does not compare: a foreign key is paired by definition whatever its name, so the next diff finds the table converged, and a later change under the desired name fits one `ALTER` under the two names and brings the name back. The replacement name avoids every index and constraint name on either side (MySQL names the index it creates for a foreign key after the constraint); one that collides with a foreign key of another table in the schema fails the `ALTER` with error 1826, and nothing has changed |
| `ADD PARTITION`, `COALESCE PARTITION`, `REORGANIZE PARTITION` alongside other changes | see the previous section | MySQL does not accept them next to other clauses |

## Normalization

MySQL rewrites many constructs when it stores a table definition, so the form a human writes rarely matches what `SHOW CREATE TABLE` reports. Left unhandled, this produces **spurious diffs** — a schema file that says `active BOOLEAN` would appear to differ from the live `active tinyint(1)`, and a diff would emit a pointless `MODIFY COLUMN`. To prevent this, `ParseCreateTable` runs a pipeline of **normalization rules** over the parsed `CreateTable` before returning it, canonicalizing both sides so `Diff` compares like with like.

Normalization decides what is *compared*, not what is *emitted*. A literal `DEFAULT` is emitted as the schema spelled it (`Column.DefaultAsWritten`), while the rules rewrite `Column.Default` to the value MySQL stores for it. MySQL reads the written literal in a `MODIFY COLUMN` exactly as it did in the `CREATE TABLE`, so the emitted DDL stores the same value, where the text `SHOW CREATE TABLE` reports would not: it prints a `float` with six significant digits, so a `MODIFY` that carried the live `'1234570'` would store a different value than the declared `1234567`. A reading is also never allowed to establish an equality the literals do not have. A rule reads a literal exactly or not at all: a `float` is compared by its exact stored value, not by the six digits `SHOW CREATE TABLE` prints (under which `1234567` and `1234568` would be one default), and a temporal fraction that MySQL rounds under the default `sql_mode` but truncates under `TIME_TRUNCATE_FRACTIONAL` is left as written rather than read under an assumed mode. A literal left as written keeps diffing against the live table — the safe direction: the same `MODIFY` is emitted again rather than a change missed. The one rule that rewrites the emitted form on purpose is `charUTF8MB4DefaultNormalizer`, whose `_utf8mb4 x'…'` form is what keeps a live hex reading correct on a column of another charset.

Charset introducers in stored expressions (generated columns, `CHECK`s, functional indexes, expression defaults, partition expressions) follow the same principle. MySQL keeps a literal's introducer in the expression it stores, and an introducer can change the value even of an ASCII literal: `UPPER(_latin5'i')` is `'İ'` where `UPPER('i')` is `'I'`, and `STRCMP(_latin1'a', _latin1'a ')` is `0` where the bare pair is `-1`. Spirit does not evaluate expressions, so `restoreExprText` keeps every introducer as written except `_utf8mb4`, which a bare literal parses to and which MySQL writes on every bare literal stored from a utf8mb4 session. Two consequences, both in the safe direction: an expression stored from a session of another charset reports that charset's introducer (`_latin1` from the `mysql` client's default, `_utf8mb3` from an older client), so a schema spelling the literal bare diffs once, and the emitted statement, stored from Spirit's utf8mb4 session, makes the two agree; and an expression MySQL rewrites on storage, such as `CONCAT(_latin1'x', 'y')` stored as `concat(_utf8mb4'x',_utf8mb4'y')`, never converges until the schema spells what MySQL stores. The parser spells `_utf8mb3` as `_utf8`, an alias MySQL accepts.

Two layers of canonicalization apply:

1. **The parser** already folds most type *aliases* before Spirit sees them: `BOOL`/`BOOLEAN` → `tinyint(1)`, `SERIAL` → `bigint unsigned NOT NULL AUTO_INCREMENT UNIQUE`, `INTEGER` → `int`, `DEC` → `decimal`. Nothing in Spirit is needed for these. `NCHAR`/`NVARCHAR` are an exception: the parser folds them to `char`/`varchar` but drops the `utf8mb3` character set MySQL stores them with ([#1299](https://github.com/block/spirit/issues/1299)).
2. **Spirit's normalization rules** handle the canonicalizations the parser does *not* — each mirrors something MySQL does when storing the table:

   | Rule (`normalize_*.go`) | Canonicalization |
   |---|---|
   | `primaryKeyNormalizer` | inline `id INT PRIMARY KEY` → table-level `PRIMARY KEY` index |
   | `primaryKeyNotNullNormalizer` | marks every primary key column `NOT NULL`, as MySQL stores it: `a INT, PRIMARY KEY (a)` → `a int NOT NULL`. The promotion is implicit only: a key column that explicitly declares `NULL` or `DEFAULT NULL`, which MySQL refuses to create (error 1171), stays nullable, and `Diff` and `DeclarativeToImperative` reject a target schema in that state |
   | `autoIncrementNotNullNormalizer` | marks an `AUTO_INCREMENT` column `NOT NULL` and drops its `DEFAULT NULL`, as MySQL stores it: `id INT AUTO_INCREMENT`, `id INT NULL AUTO_INCREMENT` and `id INT AUTO_INCREMENT DEFAULT NULL` are all reported as `id int NOT NULL AUTO_INCREMENT`, and without the rule each diffed to a `MODIFY COLUMN ... NULL AUTO_INCREMENT` (a full copy) on every run. The implication is positional, as MySQL applies the attributes in order: a `NULL` written after the `AUTO_INCREMENT` (`id INT AUTO_INCREMENT NULL`) keeps the column nullable, and `formatColumn` writes such a column as `AUTO_INCREMENT NULL` so MySQL stores it that way. A nullable `AUTO_INCREMENT` column cannot converge, though: `SHOW CREATE TABLE` reports it as `int AUTO_INCREMENT`, which as `CREATE TABLE` input means `NOT NULL`, so the live column reads back `NOT NULL`. A primary key column is left to `primaryKeyNotNullNormalizer`, since MySQL rejects an explicit `NULL` there in any position (error 1171) |
   | `indexNormalizer` | inline `c INT UNIQUE` → table-level `UNIQUE KEY`; assigns MySQL's default names to unnamed indexes |
   | `indexDefaultsNormalizer` | drops the index options MySQL accepts and does not store, so a definition that spells them out matches the `SHOW CREATE TABLE` that omits them: `VISIBLE` on any index (including `PRIMARY KEY (id) VISIBLE`, which used to emit an `ALTER INDEX` with an empty name); an index `KEY_BLOCK_SIZE` equal to the table's own, which `SHOW CREATE TABLE` omits on any engine; on InnoDB (named or default engine) `USING HASH`, which InnoDB builds as a B-tree and does not report (an explicit `USING BTREE` is reported and kept), and an index `KEY_BLOCK_SIZE` on a table that is not compressed (`ROW_FORMAT=COMPRESSED` or a table-level `KEY_BLOCK_SIZE`), which InnoDB drops on creation. Other engines keep their options |
   | `columnCheckNormalizer` | hoists every column-level `CHECK` into a table-level constraint, keeping its name and `NOT ENFORCED`: `c INT CHECK (c > 0) CHECK (c < 10)` is two constraints |
   | `expressionParenNormalizer` | rewrites expression-`DEFAULT`, `CHECK`, generated-column, functional-index, partition and subpartition expressions into a canonical parenthesization, keeping only the parentheses the expression's own precedence does not already imply: MySQL stores them fully parenthesized and the parser preserves input parens verbatim, so `CHECK ((a=1) OR ((b=2) AND (c=3)))` and `CHECK (a=1 OR b=2 AND c=3)` both canonicalize to the latter, the `KEY k (((c + 1)))` MySQL reports for a functional index matches the authored `KEY k ((c+1))` instead of being dropped and re-added on every run, and the `DEFAULT (-(1))` MySQL reports for an expression default matches the authored `DEFAULT (-1)` instead of emitting a `MODIFY COLUMN` on every run. It also drops every unary plus, which MySQL discards when it parses the expression (`CHECK (c > +1)` is stored as `CHECK ((`c` > 1))`, `DEFAULT (+1)` as `DEFAULT (1)`); an expression default that this leaves a bare literal takes the literal's kind, so `DEFAULT (+'1')` compares equal to the `DEFAULT (_utf8mb4'1')` MySQL reports. A string-literal expression default (`DEFAULT ('{}')`) holds a value, not an expression, and is left alone. Only a nested `AND` or `OR` is regrouped (`a AND (b AND c)` and `(a AND b) AND c` both canonicalize to `a AND b AND c`, which is also how MySQL stores them); every other operator keeps the written grouping, because it decides the value — the bitwise `&`, `\|` and `^` operate on binary strings when both operands are binary strings and on integers otherwise, so `_binary'12' & (_binary'21' & 7)` is 4 where `(_binary'12' & _binary'21') & 7` is 0 |
   | `functionAliasNormalizer` | rewrites a function name to the one MySQL stores, in expression `DEFAULT`s, generated columns, `CHECK`s, functional indexes and partition expressions: `STRING_TO_VECTOR` → `to_vector`, `LCASE` → `lower`, `SUBSTRING`/`MID` → `substr`, `DAY` → `dayofmonth`, and the timestamp family inside an expression default → `now()` |
   | `columnReferenceCaseNormalizer` | respells a column reference in a generated column, `CHECK`, functional index, partition or subpartition expression to the case the column is declared in, as MySQL reports it: `g INT AS (C + 1)` over a column `c` reads back as `((`c` + 1))`, and without the rule diffed to a `MODIFY COLUMN` (a full copy) on every run; a functional index to a `DROP`+`ADD`, a partition expression to a `PARTITION BY`. MySQL keeps the written case in a `CHECK` at `CREATE TABLE` but re-renders it in declared case after a later `ALTER`, so both sides are respelled. A name that is not a declared column is left as written. Expression `DEFAULT`s cannot reference a column |
   | `timestampFspZeroNormalizer` | drops an explicit fractional-seconds precision of 0 from the timestamp functions (`CURRENT_TIMESTAMP`, `NOW`, `LOCALTIME`, `LOCALTIMESTAMP`, `CURTIME`, `CURRENT_TIME`, `UTC_TIMESTAMP`, `UTC_TIME`, `SYSDATE`), in a literal `DEFAULT`, `ON UPDATE` and expression `DEFAULT`s, as MySQL stores them: `DEFAULT CURRENT_TIMESTAMP(0)` → `DEFAULT CURRENT_TIMESTAMP`, `DEFAULT (NOW(0) + INTERVAL 1 DAY)` → `DEFAULT ((now() + interval 1 day))`. A non-zero fsp is kept |
   | `binaryAttributeNormalizer` | resolves the legacy `BINARY` column attribute to the column charset's `_bin` collation. A `COLLATE` in the same definition is overridden when the column declares no charset of its own, and kept when it does (as every `NCHAR`/`NVARCHAR` does) |
   | `defaultCollationNormalizer` | fills in the charset's default collation on a column or table that declares a charset without a `COLLATE`, for every charset whose default is fixed: `CHARACTER SET latin1` → `latin1_swedish_ci`, `ascii` → `ascii_general_ci`, utf8mb3 (including `NCHAR`/`NVARCHAR`, which always use it) → `utf8mb3_general_ci`. `SHOW CREATE TABLE` writes the collation out on a column whose charset was declared, so without this the two sides disagree. The table default is filled too, so `DEFAULT CHARSET=latin1` means `latin1_swedish_ci` and a table on `latin1_bin` is converged onto it. utf8mb4 is excluded, because its default depends on the server version and `default_collation_for_utf8mb4`: MySQL gives a table declared `DEFAULT CHARSET=utf8mb4` without a `COLLATE` the server's `default_collation_for_utf8mb4`, whatever the schema default is, and that variable accepts only `utf8mb4_0900_ai_ci` and `utf8mb4_general_ci`. Such a table therefore matches those two collations and no others: against a table on another collation (such as `utf8mb4_bin`), the diff emits `DEFAULT CHARSET=utf8mb4` alone, which MySQL resolves to the variable's value, and MODIFYs each inheriting column in the same ALTER. A MODIFY of an inheriting column in that ALTER is written with `CHARACTER SET utf8mb4`, because MySQL resolves a MODIFY without a charset there to `utf8mb4_0900_ai_ci` even when the variable is `utf8mb4_general_ci`. A table with no charset clause inherits the schema default, which can be any collation, so it stays underdetermined. A column that names `CHARACTER SET utf8mb4` without a `COLLATE` also takes the server's `default_collation_for_utf8mb4`, not the table's collation, so whatever the table default is it matches the same two collations and no others. A column with the `BINARY` attribute is left to `binaryAttributeNormalizer`, which selects the `_bin` collation. The binary charset needs nothing here: the parser gives every type that declares `CHARACTER SET binary` the `binary` collation, including `enum`/`set`, on which `SHOW CREATE TABLE` writes it out |
   | `binaryCharsetNormalizer` | rewrites a character column whose charset resolves to `binary` to its binary type, as MySQL stores it: `varchar` → `varbinary`, `char` → `binary`, `text` → `blob` (and the other `*text` sizes). Covers a column that inherits a `DEFAULT CHARSET=binary` or `DEFAULT COLLATE=binary` table default and one that writes `COLLATE binary`; the parser already handles an explicit `CHARACTER SET binary`. `enum`/`set` keep their type, and a column that declares its own charset or collation is not rewritten. The table default is canonicalized to `DEFAULT CHARSET=binary` with no `COLLATE`, as `SHOW CREATE TABLE` reports it |
   | `textBlobLengthNormalizer` | resolves a `TEXT(M)`/`BLOB(M)` column to the type MySQL stores: the smallest of `tiny`/plain/`medium`/`long` that holds M bytes (255, 65535, 16777215), where M is bytes for `BLOB` and characters at the charset's maximum bytes per character for `TEXT`: `blob(100)` → `tinyblob`, `text(0)` → `tinytext`, `text(64)` on utf8mb4 → `text`, `text(100) CHARACTER SET latin1` → `tinytext`. A table that names no charset takes the database default, so its `TEXT(M)` is rewritten only when every charset (1 to 4 bytes per character) gives the same size. Otherwise the column keeps its written length (`Column.Length`) and is emitted as `text(M)`, so MySQL resolves the size at the charset the column gets; a plain `text` would narrow `text(20000)` on a utf8mb4 schema (stored as `mediumtext`) or widen `text(64)` on a latin1 one (stored as `tinytext`). `Diff` compares such a column at the other side's charset, the only side that determines it: `text(20000)` matches a utf8mb4 `mediumtext` and differs from a utf8mb4 `text`, with or without `IgnoreCharsetCollation` (which stops charset differences being diffed but not a MODIFY storing the column at a real charset). Two such columns match when their lengths give the same size at every width: `text(20000)` and `text(20001)` |
   | `binaryDefaultBytesNormalizer` | rewrites the literal `DEFAULT` of a `binary(N)` or `varbinary(N)` column to the bytes MySQL stores. Every literal form is stored as bytes: a string, `TRUE`/`FALSE`, an integer of any size (as its decimal digits, so `+007` is `'7'`), a hex literal and a bit literal (which stores one byte per 8 digits written). On `binary(N)` the bytes are right-padded with NULs to the column width: `binary(3) DEFAULT 'a'` is reported as `DEFAULT 'a\0\0'`; `varbinary(4) DEFAULT x'61'` is reported as `DEFAULT 'a'`. The value is recorded as a string, or as a hex literal when its bytes are not valid utf8mb3, since `SHOW CREATE TABLE` then reports it as hex (`binary(3) DEFAULT x'ff'` → `DEFAULT 0xFF0000`). Also covers a `char`/`varchar` column that the binary charset makes `binary`/`varbinary`. Left alone: `binary(0)`, whose only default `''` has nothing to pad (a width-less `binary` is `binary(1)` and is padded), `binary` wider than 255 and a default longer than the width (MySQL rejects both), decimal and float literals (`numericDefaultNormalizer` formats those and pads them on `binary`), and expression defaults |
   | `charDefaultSpacesNormalizer` | rewrites the string `DEFAULT` of a `char(N)` or `varchar(N)` column to the value `SHOW CREATE TABLE` reports. A `char` value is read back with every trailing space stripped, in every charset and collation (NO PAD collations included): `char(4) DEFAULT 'a  '` is reported as `DEFAULT 'a'`, `DEFAULT '    '` as `DEFAULT ''`. Only U+0020 is stripped; a tab or NUL is data. A `varchar` default keeps its trailing spaces, except spaces past the column width, which MySQL drops (`varchar(4) DEFAULT 'ab      '` → `DEFAULT 'ab  '`). Stripping both sides also converges a reading taken under the `PAD_CHAR_TO_FULL_LENGTH` sql_mode, which reports the `char` default padded to the width. A hex or bit literal default is read as the string its bytes form wherever `charBinaryLiteralDefaultNormalizer` reads it as one, then handled the same way (`char(4) DEFAULT x'612020'` → `DEFAULT 'a'`), so the two rules agree in either order. Left alone: a `char`/`varchar` whose charset resolves to `binary` (spaces are data there; see `binaryDefaultBytesNormalizer`), `varchar` spaces past the width unless the charset resolves to utf8mb4/utf8mb3/latin1/ascii (MySQL rejects them in utf16/utf32/ucs2, and a charset the statement does not determine is inherited from the schema, which may be one of those), other whitespace past the width, and expression defaults |
   | `integerBinaryLiteralDefaultNormalizer` | rewrites a hex or bit literal `DEFAULT` on an integer or unscaled `decimal` column to the integer MySQL stores, which reads the literal's bytes as a big-endian unsigned integer: `int DEFAULT 0x1A` is reported as `DEFAULT '26'`, `int DEFAULT b'1010'` as `DEFAULT '10'`. Left alone: an empty literal and one longer than 8 bytes (MySQL rejects both), a value of 2^63 or more on `decimal` (MySQL rejects it as hex, but not in decimal), expression defaults, and scaled `decimal`/`year`/`float`/`double`, which store the value in their own form (`decimal(5,2) DEFAULT 0x1A` is `'26.00'`, `year DEFAULT 0x07` is `'2007'`; `numericDefaultNormalizer` folds the first three, `yearDefaultNormalizer` the last) |
   | `zerofillDefaultNormalizer` | rewrites the literal `DEFAULT` of a `ZEROFILL` integer column to the value MySQL stores, left-padded with zeros to the display width: `int(10) zerofill DEFAULT 5` is reported as `DEFAULT '0000000005'`. A width that is unwritten or 0 is the type's unsigned default width (`int` 10, `bigint` 20, ...). Every literal form is converted first: hex and bit literals, `TRUE`/`FALSE`, decimals and strings (rounded half up, exactly), and floats (the double's exact value, rounded half to even). A string skips leading spaces and tabs and ignores trailing whitespace. A negative value is converted only where MySQL accepts it: a decimal that is exactly zero (`-0.0`), or a float or string that rounds to zero (`-0.5e0`, `'-0.49'`). A value longer than the width is not truncated. Left alone: expression defaults, other negative values, a string that is not a number, and `ZEROFILL` on `decimal`/`float`/`double` |
   | `numericDefaultNormalizer` | rewrites a literal `DEFAULT` to the value MySQL stores for it on the column's type, in the text `SHOW CREATE TABLE` reports, so `decimal(6,2) DEFAULT 1.2` (reported as `DEFAULT '1.20'`) stops emitting a `MODIFY COLUMN` that stores `'1.20'` again on every run. Integer types round to an integer — half away from zero for a decimal literal or a string (`int DEFAULT 2.5` → `'3'`, `'001'` → `'1'`, `' 7 '` → `'7'`), half to even for a float literal (`2.5e0` → `'2'`) — within the type's range. `decimal(M,D)` rounds half away from zero to D places and pads (`1.235` → `'1.24'`, `1` → `'1.00'`, `TRUE` → `'1.00'`, `0x1A` → `'26.00'`; a float literal through its shortest round-trip text, so `0.1e0` is `'0.10'` at any scale) within M−D integer digits. `double` stores the nearest binary value, read as MySQL prints a double: the shortest round-trip text, fixed notation up to 15 integer digits and down to 14 leading zeros, exponent notation past that in MySQL's spelling (`1e2` → `'100'`, `1.0E-7` → `'0.0000001'`, `1e15` → `'1e15'`, `123456789012345678` → `'1.2345678901234568e17'`); `float` by the exact value it stores, printed as a double (`0.1` → `'0.10000000149011612'`, `1234567` → `'1234567'`), not by the six significant digits `SHOW CREATE TABLE` prints (`'0.1'`, `'1234570'`), under which `1234567` and `1234568` would compare equal; a `float` literal whose six-digit text does not read back as the same float (`1234567`, `1.23456789`) therefore keeps diffing until the schema spells one that does (`1234570`, `1.23457`), while `0.1`, `1e-45` (`'1.4013e-45'`) and `1e38` converge; `float(M,D)`/`double(M,D)` round the fraction in double arithmetic and print D decimals (`double(10,3) DEFAULT 2.0005` → `'2.001'`). `char`/`varchar`/`binary`/`varbinary` store a numeric literal's canonical text (`-007` → `'-7'`, `1.50` → `'1.50'`, `+.5` → `'0.5'`, `5.` → `'5'`, `1.5E+2` → `'150'`; `binary` padded with NULs) when it fits the width. A string on a numeric column skips leading spaces and tabs and trailing whitespace. Left alone: expression defaults, `NULL`, anything MySQL rejects (not a number, out of range, a negative value on an unsigned column, longer than a string width), `ZEROFILL` columns, and `year`/`bit`/`enum`/`set` and the temporal types, which have rules of their own (`yearDefaultNormalizer`, `bitDefaultNormalizer`, `enumSetDefaultNormalizer`, `temporalDefaultNormalizer`) |
   | `temporalDefaultNormalizer` | rewrites a literal `DEFAULT` on a `date`, `datetime`, `timestamp` or `time` column to the value MySQL stores, in the text `SHOW CREATE TABLE` reports — `'YYYY-MM-DD'`, `'YYYY-MM-DD HH:MM:SS'` or `'[-]HH:MM:SS'` with exactly the column's fractional digits — so `datetime DEFAULT '2020-1-1'` (reported as `DEFAULT '2020-01-01 00:00:00'`) stops emitting a `MODIFY COLUMN` on every run. Reads every spelling MySQL reads unambiguously: a date without a time, fields of any width, a two-digit year (1970-2069), any punctuation between fields and a `T`, spaces or punctuation between the date and the time, a trailing separator, the compact `YYMMDD`/`YYYYMMDD`/`YYMMDDHHMMSS`/`YYYYMMDDHHMMSS` strings, and a number (`20200101`, `101` → `'2000-01-01 00:00:00'`); for `time`, `'[-][D ]H:M[:S]'`, a right-aligned digit string and a number (`'1 2:3:4.5'` → `'26:03:05'`, `100` → `'00:01:00'`). A fraction past the column's precision is read only where MySQL's two ways of shortening it agree: the default `sql_mode` rounds it half up in MySQL's two steps (the digit past the microseconds first — the seventh digit of a datetime string, the last digit of a time string) and carries through the seconds and the date, while `TIME_TRUNCATE_FRACTIONAL` truncates it, and the rule cannot see the session of the `CREATE`; so `datetime(3) DEFAULT '2020-01-01 10:00:00.1234'` → `'2020-01-01 10:00:00.123'` and `time(1) DEFAULT 1.54` → `'00:00:01.5'` are read, while `'2020-01-01 23:59:59.9'` (`'2020-01-02 00:00:00'` under one mode, `'2020-01-01 23:59:59'` under the other) and `1.55` are left as written; a `date` keeps its date when nothing carries into it; a negative zero time drops its sign. The value is recorded as a string, the form `SHOW CREATE TABLE` reports. Left alone: anything MySQL rejects (an invalid date, a zero month or day, a field out of range, a carry past `9999-12-31` or `838:59:59`), a time-zone suffix (converted to the session time zone), a float literal (read through a double), the spellings MySQL reads by rules not worth reproducing (a compact digit string of another width, a thirteen-digit number, a `time` string starting with a colon, with a date part (`'1999-12-29 12:00'` stores `'12:00:00'`) or long enough to be read as a datetime first), hex and bit literals, `TRUE`/`FALSE`, expression defaults and `NULL`. A `timestamp` default is also converted through the session time zones of the `CREATE` and of the `SHOW CREATE TABLE`, which no rule can undo. A literal left as written keeps diffing against the live table on every run; the `MODIFY` carries it as written, so what it stores is what the `CREATE` stored under the session's own mode. Every carry into year `0000` is left as written too: MySQL does not carry it but stores the zero date (`datetime DEFAULT '0000-12-09 23:59:59.5'` → `'0000-00-00 00:00:00'`, and `'0000-12-09 23:59:59'` under `TIME_TRUNCATE_FRACTIONAL`) |
   | `bitDefaultNormalizer` | rewrites the literal `DEFAULT` of a `bit(N)` column to the bit literal MySQL reports: the stored value in binary with no leading zeros. Covers hex literals (`bit(8) DEFAULT x'61'` → `b'1100001'`), bit literals, non-negative integers (`bit(1) DEFAULT 0` → `b'0'`) and strings, which are read as their bytes (`bit(8) DEFAULT '0'` → `b'110000'`). Left alone: a value that does not fit in N bits, an empty hex or bit literal, more than 8 bytes and negative integers (MySQL rejects all of them), decimals and floats (MySQL rounds them), `TRUE`/`FALSE` (folded by `booleanKeywordDefaultNormalizer`), and expression defaults |
   | `charBinaryLiteralDefaultNormalizer` | rewrites a hex or bit literal `DEFAULT` on a `char`/`varchar` column to the string its bytes spell: `varchar(4) DEFAULT 0x61` is reported as `DEFAULT 'a'`. Only where the bytes are reported unchanged: on utf8mb4/utf8mb3 when they are valid utf8mb3 (otherwise MySQL reports hex, which the written literal already matches, or rejects them), and, when every byte is ASCII, on the charsets that map those bytes unchanged (`asciiCompatibleCharsets`: latin1, ascii, gbk, sjis, and the others measured). Left alone: a column whose charset the statement does not determine (it takes the database default, which can be utf16: `char(2) DEFAULT x'6162'` there is U+6162), other charsets (utf16 and the other wide charsets pad, and swe7 maps `x'5b'` to `Ä`), bytes above 0x7F outside UTF-8 (latin1 transcodes them), a value longer than the width, expression defaults, `enum`/`set`, and a column the binary charset makes `binary`/`varbinary` (handled by `binaryDefaultBytesNormalizer`) |
   | `charUTF8MB4DefaultNormalizer` | records a `DEFAULT` on a utf8mb4 `char`/`varchar` column that is not valid utf8mb3 as the bytes `SHOW CREATE TABLE` reports (MySQL 8.0.33+): `char(4) DEFAULT '😀'` → `DEFAULT 0xF09F9880`. Covers strings and hex/bit literals. A `char` default's trailing spaces are stripped first, and a `varchar` default's spaces past the width are truncated, as MySQL stores them. The value is recorded, and a live hex default on such a column rewritten, as `_utf8mb4 x'f09f9880'`: the introducer makes the emitted `MODIFY` store the character on whatever charset the column has (a bare hex literal on a utf16 column, reachable with `IgnoreCharsetCollation`, stores different characters). Left alone: other charsets (utf16, utf32 and gb18030 report the bytes of their own encoding), a column whose charset the table does not determine, a charset introducer other than `_utf8mb4`/`_binary` (MySQL reads the bytes in that charset: `_latin1'😀'` is `'ðŸ˜€'`), `enum`/`set` (whose non-utf8mb3 members are reported as `'?'`), a value longer than the width, and expression defaults |
   | `enumSetDefaultNormalizer` | rewrites the string `DEFAULT` of an `enum` or `set` column to the member text `SHOW CREATE TABLE` reports. MySQL stores the default as the member(s) it names: it strips the trailing spaces (U+0020 only, NO PAD collations included), then looks the rest up under the column's collation, so `enum('a','b') DEFAULT 'b '` is reported as `DEFAULT 'b'` and `enum('a','B') DEFAULT 'b'` on `utf8mb4_0900_ai_ci` as `DEFAULT 'B'`. A `set` default is split on commas after the strip, and its members are reported once each in definition order (`set('a','b') DEFAULT 'b,a '` → `DEFAULT 'a,b'`). The rule resolves only a member equal byte for byte, or, on a collation known to fold ASCII case (`utf8mb4_0900_ai_ci`/`_as_ci`, `latin1_swedish_ci`, and the `_general_ci`, `_general_mysql500_ci`, `_unicode_ci` and `_unicode_520_ci` families except `cp866_general_ci` and `latin7_general_ci`), a member that differs only in the case of ASCII letters; MySQL rejects collation-duplicate members, so that member is the one it stores. Tailored collations are not folded: their contractions (Danish `aa`, Czech `ch`, ...) and Turkish dotless i compare ASCII case pairs unequal. The legacy `BINARY` attribute selects `_bin`, which strips spaces but does not fold. Left alone: a column whose charset is `binary` (spaces are data), a column whose charset and collation the definition does not determine (the database default may be `binary` or case-sensitive), a `set` default of only spaces (MySQL rejects it), a `set` default naming the empty member (reported the same as the empty set), a default MySQL matches through its collation alone (an accent, a non-ASCII case pair, an ignorable character, a contraction), numeric, `TRUE`/`FALSE`, hex and bit literal defaults, and expression defaults |
   | `enumSetMemberSpacesNormalizer` | strips the trailing spaces (U+0020 only) from each member of an `enum` or `set` column whose charset is not `binary`, as MySQL does when it stores the column: `enum('a','b ')` → `enum('a','b')`. Under the binary charset — the column's own `CHARACTER SET binary` or `COLLATE binary`, or a `DEFAULT CHARSET=binary`/`DEFAULT COLLATE=binary` table default — the spaces are data and are kept, so a `MODIFY COLUMN` emitted for the column writes the member MySQL stores. The parser keeps every member as written, because the grammar rule for the type cannot see a `COLLATE` or the table default. Left alone: a column whose charset the definition does not determine (no charset or collation of its own and no table default), since the database default it inherits may be `binary`. `Diff` decides such a column's members under the table default the emitted `ALTER` runs under: the target's when the `ALTER` sets it, otherwise the source's (as with a schema file that omits `DEFAULT CHARSET`, or under `IgnoreCharsetCollation`) |
   | `integerDisplayWidthNormalizer` | strips deprecated integer display widths (`int(11)` → `int`), keeping signed `tinyint(1)` and `ZEROFILL` (`tinyint(1) unsigned` → `tinyint unsigned`, as MySQL stores it). A `ZEROFILL` column declared without a width, or with a width of 0, gets the type's default unsigned width, as MySQL stores it (`int zerofill` and `int(0) zerofill` → `int(10) unsigned zerofill`, where the parser fills in the signed `int(11)` for the width-less form); any other explicit width, such as `int(11) zerofill`, is kept |
   | `decimalZeroPrecisionNormalizer` | rewrites a zero-precision `DECIMAL` to the default MySQL stores for it (`decimal(0)`, `decimal(0,0)` → `decimal(10,0)`) |
   | `booleanKeywordDefaultNormalizer` | folds a bare `TRUE`/`FALSE` keyword `DEFAULT` to the `1`/`0` MySQL stores, and records the literal form Spirit emits it in: a bare number on a numeric column (`DEFAULT 0`), a quoted string on a string one (`DEFAULT '0'`), a bit literal on a `bit` one (`DEFAULT b'1'`). `SHOW CREATE TABLE` quotes the value on numeric and string columns alike, and only `bit` reports a literal of its own — but quotedness is not part of column identity on a numeric column, so a folded bare `0` and the live `'0'` compare equal. Folds on the integer types, unscaled `decimal`, `double`/`float`, `varchar`/`char`, `varbinary` and `bit`. Left alone where the keyword is stored as something else: scaled `decimal` pads to its scale (folded by `numericDefaultNormalizer`), `YEAR` reads it as a year (folded by `yearDefaultNormalizer`), `binary` pads to the column width with NULs (folded by `binaryDefaultBytesNormalizer`), and `enum`/`set` resolve the keyword differently across supported server versions |
   | `vectorDimensionNormalizer` | fills in the default dimension of a `VECTOR` column declared without one (`vector` → `vector(2048)`, MySQL 9.7+) |
   | `yearDisplayWidthNormalizer` | drops the deprecated display width from `YEAR`: `year(4)` → `year`, as MySQL stores it (`YEAR(4)` is the only width MySQL 8.0 accepts) |
   | `yearDefaultNormalizer` | rewrites a literal `DEFAULT` on a `YEAR` column to the four-digit year MySQL stores: 1-69 → 2001-2069 and 70-99 → 1970-1999, from a number, a digit-only string, a hex or bit literal, or `TRUE`/`FALSE` (`year DEFAULT 99` is reported as `DEFAULT '1999'`). Zero depends on the form: a number (`0`, `FALSE`, `0x00`) and the four-character string `'0000'` store `'0000'`, any other string zero (`'0'`, `'00'`) stores `'2000'`. A number's sign and leading zeros are dropped first (`+5` → `'2005'`, `-0` → `'0000'`). Left alone: values MySQL rejects (100-1900, above 2155, negative), fractions and exponents (MySQL rounds them first), strings with whitespace or a sign, and expression defaults |
   | `charsetlessTypeNormalizer` | drops charset/collation from the types that cannot carry one (`VECTOR`, spatial) — both the parser's synthetic `binary` charset and one an author wrote by hand, which MySQL accepts and silently discards |
   | `partitionBoundConstantNormalizer` | folds a constant expression in a `VALUES LESS THAN`/`VALUES IN` value into the integer MySQL stores: `10+10` → `20`, `7 DIV 2 * 100` → `300`, `MOD(20, 7)` → `6`, `TO_DAYS('2030-01-01')` → `741443`, and likewise `TO_SECONDS` and `YEAR` of a `'YYYY-MM-DD'` or `'YYYY-MM-DD HH:MM:SS'` literal. Integer arithmetic follows MySQL's per-operator typing (`MOD` takes the dividend's type, the others are unsigned if either operand is), and is folded only when the default `sql_mode` and `NO_UNSIGNED_SUBTRACTION` give the same value; `TestPartitionBoundConstantFoldingMatchesMySQL` checks this against the server. Left alone, and emitted as written for MySQL to evaluate or reject: anything MySQL rejects (an out-of-range intermediate result, division by zero, a `DECIMAL` result such as `-(-5)`), `/`, a datetime with a fractional second (MySQL rounds it first), other date formats, and `UNIX_TIMESTAMP`, whose result depends on the session time zone. The valid ones apply but do not converge: the live table stores the integer, so every later diff against it emits a `REORGANIZE` or `PARTITION BY` again, and spirit runs each as a full table copy. Write such a bound as the integer it evaluates to |
   | `partitionListValueOrderNormalizer` | sorts each `VALUES IN` list, whose order has no meaning: `NULL`, then numbers by value, then string literals, then unfolded expressions by text; a `LIST COLUMNS` tuple sorts element by element. MySQL keeps the written order (`LIST (expr)` moves `NULL` first), so without this a desired schema listing values in another order re-emits a `REORGANIZE` on every run. A foldable expression sorts by its folded value, so the order does not depend on `partitionBoundConstantNormalizer` |
   | `partitionOptionsNormalizer` | rewrites partition and subpartition options to the form `SHOW CREATE TABLE` prints: a partition's options (`COMMENT` included) move onto each of its named subpartitions that does not set its own; an empty `COMMENT`, `MAX_ROWS = 0` and `MIN_ROWS = 0` are dropped (`NODEGROUP = 0` is kept, as MySQL prints it); `TABLESPACE = innodb_file_per_table` is dropped, because MySQL prints it on every partition once one sets it and `REORGANIZE` cannot remove it (it only has an effect with `innodb_file_per_table=OFF`, and the live definition cannot show where); trailing slashes are dropped from `DATA DIRECTORY`/`INDEX DIRECTORY`, which MySQL prints with or without one depending on the statement that set it |
   | `partitionKeyAlgorithmNormalizer` | folds `KEY ALGORITHM=2`, MySQL's default, into the omitted form `SHOW CREATE TABLE` prints, on partitioning and subpartitioning. `ALGORITHM=1` is a different row placement and is kept |
   | `partitionCommentNormalizer` | pushes a partition-level `COMMENT` down onto explicitly named subpartitions that have none, and clears it from the partition — what MySQL stores for `PARTITION p0 ... COMMENT 'c' (SUBPARTITION s0, SUBPARTITION s1)`. A partition comment on implicit subpartitions (`SUBPARTITIONS n`) stays on the partition |

### Pipeline

Rules implement the `Normalizer` interface (`normalize.go`):

```go
type Normalizer interface {
    Name() string
    Normalize(ct *CreateTable) *CreateTable
}
```

- Each rule lives in its own `normalize_<name>.go` file and self-registers via `init()` calling `registerNormalizer(...)` — the same registration pattern `pkg/lint` uses for linters, so a new rule is added by dropping in a file with no change to `Diff` or the parser.
- `runNormalizers` applies every registered rule at the tail of `parseToStruct`, **after** all fields are populated. Rules therefore see the whole struct and are **order-independent**.
- Rules rewrite the **structured** fields of `CreateTable` (`Columns`, `Indexes`, …), never `Raw`. Code that reads `Column.Raw` / `CreateTable.Raw` (e.g. AST `Restore`, some linters) bypasses normalization.

Because canonicalization happens at parse time, **`Diff` assumes normalized input**: two `CreateTable`s obtained from `ParseCreateTable` are always canonical, so the diff logic compares them structurally without re-deriving equivalences (inline vs. table-level keys, unnamed indexes, etc.). A `CreateTable` built by hand — without going through `ParseCreateTable` — is *not* normalized.

### Relationship to `spirit fmt`

Normalization is an **offline, best-effort** approximation of what MySQL does: it needs no database and covers the common cases. [`spirit fmt`](../../docs/fmt.md) is the **ground-truth** canonicalizer — it round-trips a `CREATE TABLE` through a live MySQL server and reads back `SHOW CREATE TABLE`, so it captures *every* transformation, including ones normalization does not implement (e.g. the national character set of `NCHAR`/`NVARCHAR` columns, and the expression rewrites that restructure rather than rename — `MOD(a,b)` → `(a % b)`, `INSTR(a,b)` → `locate(b,a)`, `WEEKOFYEAR(d)` → `week(d,3)`). Use `spirit fmt` to canonicalize schema files on disk; normalization keeps in-memory parsing and diffing accurate without a server.

## Helper Functions

### RemoveSecondaryIndexes

Removes regular secondary indexes from a CREATE TABLE statement while preserving PRIMARY KEY, UNIQUE, FULLTEXT, and SPATIAL indexes, plus one regular index if required to support AUTO_INCREMENT. Among supporting regular indexes, it prefers the fewest key parts:

```go
original := `CREATE TABLE t1 (
    id INT PRIMARY KEY,
    email VARCHAR(255) UNIQUE,
    name VARCHAR(100),
    description TEXT,
    INDEX idx_name (name),
    FULLTEXT idx_description (description)
)`

modified, err := statement.RemoveSecondaryIndexes(original)
// Result: CREATE TABLE with PRIMARY KEY, UNIQUE, and FULLTEXT preserved, but without idx_name
```

**What's Preserved:**
- PRIMARY KEY (fundamental to table structure)
- UNIQUE indexes (enforce data integrity constraints)
- FULLTEXT and SPATIAL indexes (specialized index types)
- One regular index leading with AUTO_INCREMENT when no retained PRIMARY or UNIQUE key already supports it

**What's Removed:**
- Regular INDEX (non-unique secondary indexes), except the required AUTO_INCREMENT support index

This functionality is used by move tables operations to defer regular index creation until after data is copied, improving copy performance.

For example, `PRIMARY KEY(p), KEY wide(id,p), KEY narrow(id)` on a table with
`id INT AUTO_INCREMENT` retains `narrow` and defers `wide`.

`RemoveSecondaryIndexesForComparison` removes **all** regular indexes so schema
comparisons can ignore equivalent AUTO_INCREMENT support under different names
or with different trailing columns. Its output is for comparison only and may
not be executable DDL.

### GetMissingSecondaryIndexes

Compares two CREATE TABLE statements and generates ALTER TABLE to add missing indexes:

```go
source := `CREATE TABLE t1 (
    id INT PRIMARY KEY,
    email VARCHAR(255),
    INDEX idx_email (email),
    INDEX idx_created (created_at)
)`

target := `CREATE TABLE t1 (
    id INT PRIMARY KEY,
    email VARCHAR(255),
    INDEX idx_email (email)
)`

alterStmt, err := statement.GetMissingSecondaryIndexes(source, target, "t1")
// alterStmt = "ALTER TABLE `t1` ADD INDEX `idx_created` (`created_at`)"
```

This is used in combination with `RemoveSecondaryIndexes` to re-add secondary indexes in move tables operations.

## Usage Examples

### Basic Statement Parsing

```go
stmts, err := statement.New("ALTER TABLE users ADD COLUMN age INT")
if err != nil {
    return err
}

for _, stmt := range stmts {
    fmt.Printf("Table: %s\n", stmt.Table)
    fmt.Printf("Alter: %s\n", stmt.Alter)
    
    if stmt.IsAlterTable() {
        // Perform safety checks
        if err := stmt.AlgorithmInplaceConsideredSafe(); err != nil {
            fmt.Println("Requires Spirit migration")
        } else {
            fmt.Println("Can use native INPLACE")
        }
    }
}
```

### Multiple Statements

```go
sql := `
    ALTER TABLE t1 ADD COLUMN c1 INT;
    ALTER TABLE t2 ADD INDEX (c2);
    ALTER TABLE t3 RENAME INDEX old TO new;
`

stmts, err := statement.New(sql)
if err != nil {
    return err
}

// Process each statement
for _, stmt := range stmts {
    fmt.Printf("Processing %s.%s: %s\n", stmt.Schema, stmt.Table, stmt.Alter)
}
```

### CREATE TABLE Analysis

```go
// Get canonical CREATE TABLE from database
var tableName string
var createStmt string
err := db.QueryRow("SHOW CREATE TABLE users").Scan(&tableName, &createStmt)
if err != nil {
    return err
}

// Parse into structured format
ct, err := statement.ParseCreateTable(createStmt)
if err != nil {
    return err
}

// Check for invisible indexes
if ct.Indexes.HasInvisible() {
    fmt.Println("Table has invisible indexes")
}

// Check for foreign keys
if ct.Constraints.HasForeignKeys() {
    fmt.Println("Table has foreign key constraints")
}

// Find specific column
col := ct.Columns.ByName("email")
if col != nil && col.Unique {
    fmt.Println("Email column has UNIQUE constraint")
}
```

### Safety Validation

```go
stmt := statement.MustNew("ALTER TABLE t1 ADD INDEX (email)")[0]

// Check if safe for INPLACE
if err := stmt.AlgorithmInplaceConsideredSafe(); err != nil {
    switch err {
    case statement.ErrUnsafeForInplace:
        fmt.Println("Requires table rebuild - use Spirit migration")
    case statement.ErrMultipleAlterClauses:
        fmt.Println("Multiple clauses with mixed safety - split into separate ALTERs")
    }
}

// Check for unsupported clauses
if err := stmt.AlterContainsUnsupportedClause(); err != nil {
    fmt.Println("Statement contains ALGORITHM or LOCK clause - remove it")
}

// Check for UNIQUE index
if err := stmt.AlterContainsAddUnique(); err == nil {
    fmt.Println("No UNIQUE index detected")
} else {
    fmt.Println("UNIQUE index detected - may fail if duplicates exist")
}
```

### Table Comparison

```go
// Get source and target CREATE TABLE statements
sourceCreate := getCreateTable(db, "source_table")
targetCreate := getCreateTable(db, "target_table")

// Find missing indexes
alterStmt, err := statement.GetMissingSecondaryIndexes(sourceCreate, targetCreate, "target_table")
if err != nil {
    return err
}

if alterStmt != "" {
    fmt.Println("Need to add indexes:", alterStmt)
    // Execute alterStmt to bring target in sync with source
}
```

## Limitations

1. **Functional Indexes**: `CREATE INDEX` with functional expressions cannot be converted to `ALTER TABLE`
2. **Single Schema**: Multi-table operations must use the same schema
3. **SPATIAL Indexes**: Not fully supported in some helper functions
4. **Statements must be parseable by pkg/parser**: unparseable DDL cannot be migrated. The most commonly occurring scenarios tend to be complex DEFAULT or CHECK expressions; since the parser is part of this repo, fixes land here directly.

5. **FLOAT default precision:** `SHOW CREATE TABLE` and `INFORMATION_SCHEMA.COLUMNS.COLUMN_DEFAULT` print unscaled `FLOAT` defaults with only six significant digits. Distinct stored values can therefore have identical metadata. Comparing the declared literal exactly prevents additional rounding, but cannot recover precision already lost by the server: a live default of `1234567` reports `1234570`, so a desired default of `1234570` can still produce no diff. Apply that default change explicitly when exact values matter.
6. **Implicit foreign-key indexes:** when writing a desired schema by hand, declare each foreign key's supporting index explicitly. Otherwise a later diff can try to drop the implicit index shown by MySQL, and MySQL refuses the drop while the foreign key needs it.

## Best Practices

1. **Use SHOW CREATE TABLE**: Always parse the output of `SHOW CREATE TABLE` rather than user-provided CREATE statements. We refer to this in some places as "the canonical show create table".
2. **Use ALTER TABLE**: Use `ALTER TABLE` syntax over `CREATE/DROP INDEX` syntax. The rewriting of `CREATE INDEX` is best-effort and does not support complex expressions.

## See Also

- [pkg/parser](../parser/README.md) - Spirit's SQL parser (MySQL-only fork of the TiDB parser)
- [MySQL 8.0 Online DDL Operations](https://dev.mysql.com/doc/refman/8.0/en/innodb-online-ddl-operations.html)
- [pkg/table](../table/README.md) - Uses statement parsing for table metadata
- [pkg/migration](../migration/README.md) - Uses safety analysis to determine migration strategy
