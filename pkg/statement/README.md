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
    Default    *string
    OnUpdate   *string           // ON UPDATE CURRENT_TIMESTAMP[(n)] for TIMESTAMP/DATETIME
    GeneratedExpr   *string      // Expression for GENERATED ALWAYS AS (...) columns
    GeneratedStored bool         // true = STORED, false = VIRTUAL
    Check      *string           // Column-level CHECK (...) expression
    SRID       *uint32           // SRID attribute for spatial columns
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

### Partition Information

For partitioned tables, `PartitionOptions` provides:

```go
type PartitionOptions struct {
    Type         string                // "RANGE", "LIST", "HASH", "KEY", "SYSTEM_TIME"
    Expression   *string               // For HASH and RANGE
    Columns      []string              // For KEY, RANGE COLUMNS, LIST COLUMNS
    Linear       bool
    Partitions   uint64
    Definitions  []PartitionDefinition
    SubPartition *SubPartitionOptions
}
```

Partitioning is compared as a whole: MySQL cannot alter a partition method in place, so any difference other than a HASH/KEY partition-count change is emitted as `REMOVE PARTITIONING` followed by a complete `PARTITION BY` — including its `SUBPARTITION BY` clause, partition comments, and any explicitly named subpartitions. The per-partition `ENGINE` clause is the one thing deliberately **not** compared: MySQL requires every partition to use the table's engine, so it carries no information, yet `SHOW CREATE TABLE` always prints it while authored SQL does not.

## Normalization

MySQL rewrites many constructs when it stores a table definition, so the form a human writes rarely matches what `SHOW CREATE TABLE` reports. Left unhandled, this produces **spurious diffs** — a schema file that says `active BOOLEAN` would appear to differ from the live `active tinyint(1)`, and a diff would emit a pointless `MODIFY COLUMN`. To prevent this, `ParseCreateTable` runs a pipeline of **normalization rules** over the parsed `CreateTable` before returning it, canonicalizing both sides so `Diff` compares like with like.

Two layers of canonicalization apply:

1. **The parser** already folds most type *aliases* before Spirit sees them: `BOOL`/`BOOLEAN` → `tinyint(1)`, `SERIAL` → `bigint unsigned NOT NULL AUTO_INCREMENT UNIQUE`, `INTEGER` → `int`, `DEC` → `decimal`. Nothing in Spirit is needed for these. `NCHAR`/`NVARCHAR` are an exception: the parser folds them to `char`/`varchar` but drops the `utf8mb3` character set MySQL stores them with ([#1299](https://github.com/block/spirit/issues/1299)).
2. **Spirit's normalization rules** handle the canonicalizations the parser does *not* — each mirrors something MySQL does when storing the table:

   | Rule (`normalize_*.go`) | Canonicalization |
   |---|---|
   | `primaryKeyNormalizer` | inline `id INT PRIMARY KEY` → table-level `PRIMARY KEY` index |
   | `primaryKeyNotNullNormalizer` | marks every primary key column `NOT NULL`, as MySQL stores it: `a INT, PRIMARY KEY (a)` → `a int NOT NULL`. The promotion is implicit only: a key column that explicitly declares `NULL` or `DEFAULT NULL`, which MySQL refuses to create (error 1171), stays nullable, and `Diff` and `DeclarativeToImperative` reject a target schema in that state |
   | `indexNormalizer` | inline `c INT UNIQUE` → table-level `UNIQUE KEY`; assigns MySQL's default names to unnamed indexes |
   | `columnCheckNormalizer` | hoists a column-level `CHECK` into a table-level constraint |
   | `expressionParenNormalizer` | rewrites `CHECK` and generated-column expressions into a canonical parenthesization, keeping only the parentheses the expression's own precedence does not already imply: MySQL stores them fully parenthesized and the parser preserves input parens verbatim, so `CHECK ((a=1) OR ((b=2) AND (c=3)))` and `CHECK (a=1 OR b=2 AND c=3)` both canonicalize to the latter |
   | `functionAliasNormalizer` | rewrites a function name to the one MySQL stores, in expression `DEFAULT`s, generated columns, `CHECK`s, functional indexes and partition expressions: `STRING_TO_VECTOR` → `to_vector`, `LCASE` → `lower`, `SUBSTRING`/`MID` → `substr`, `DAY` → `dayofmonth`, and the timestamp family inside an expression default → `now()` |
   | `binaryAttributeNormalizer` | resolves the legacy `BINARY` column attribute to the column charset's `_bin` collation. A `COLLATE` in the same definition is overridden when the column declares no charset of its own, and kept when it does (as every `NCHAR`/`NVARCHAR` does) |
   | `defaultCollationNormalizer` | fills in the charset's default collation on a column or table that declares a charset without a `COLLATE`, for every charset whose default is fixed: `CHARACTER SET latin1` → `latin1_swedish_ci`, `ascii` → `ascii_general_ci`, utf8mb3 (including `NCHAR`/`NVARCHAR`, which always use it) → `utf8mb3_general_ci`. `SHOW CREATE TABLE` writes the collation out on a column whose charset was declared, so without this the two sides disagree. The table default is filled too, so `DEFAULT CHARSET=latin1` means `latin1_swedish_ci` and a table on `latin1_bin` is converged onto it. utf8mb4 is excluded, because its default depends on the server version and `default_collation_for_utf8mb4`: MySQL gives a table declared `DEFAULT CHARSET=utf8mb4` without a `COLLATE` the server's `default_collation_for_utf8mb4`, whatever the schema default is, and that variable accepts only `utf8mb4_0900_ai_ci` and `utf8mb4_general_ci`. Such a table therefore matches those two collations and no others: against a table on another collation (such as `utf8mb4_bin`), the diff emits `DEFAULT CHARSET=utf8mb4` alone, which MySQL resolves to the variable's value, and MODIFYs each inheriting column in the same ALTER. A MODIFY of an inheriting column in that ALTER is written with `CHARACTER SET utf8mb4`, because MySQL resolves a MODIFY without a charset there to `utf8mb4_0900_ai_ci` even when the variable is `utf8mb4_general_ci`. A table with no charset clause inherits the schema default, which can be any collation, so it stays underdetermined. A column that names `CHARACTER SET utf8mb4` without a `COLLATE` also takes the server's `default_collation_for_utf8mb4`, not the table's collation, so whatever the table default is it matches the same two collations and no others. A column with the `BINARY` attribute is left to `binaryAttributeNormalizer`, which selects the `_bin` collation |
   | `binaryCharsetNormalizer` | rewrites a character column whose charset resolves to `binary` to its binary type, as MySQL stores it: `varchar` → `varbinary`, `char` → `binary`, `text` → `blob` (and the other `*text` sizes). Covers a column that inherits a `DEFAULT CHARSET=binary` or `DEFAULT COLLATE=binary` table default and one that writes `COLLATE binary`; the parser already handles an explicit `CHARACTER SET binary`. `enum`/`set` keep their type, and a column that declares its own charset or collation is not rewritten. The table default is canonicalized to `DEFAULT CHARSET=binary` with no `COLLATE`, as `SHOW CREATE TABLE` reports it |
   | `binaryDefaultBytesNormalizer` | rewrites the literal `DEFAULT` of a `binary(N)` or `varbinary(N)` column to the bytes MySQL stores. Every literal form is stored as bytes: a string, `TRUE`/`FALSE`, an integer of any size (as its decimal digits, so `+007` is `'7'`), a hex literal and a bit literal (which stores one byte per 8 digits written). On `binary(N)` the bytes are right-padded with NULs to the column width: `binary(3) DEFAULT 'a'` is reported as `DEFAULT 'a\0\0'`; `varbinary(4) DEFAULT x'61'` is reported as `DEFAULT 'a'`. The value is recorded as a string, or as a hex literal when its bytes are not valid utf8mb3, since `SHOW CREATE TABLE` then reports it as hex (`binary(3) DEFAULT x'ff'` → `DEFAULT 0xFF0000`). Also covers a `char`/`varchar` column that the binary charset makes `binary`/`varbinary`. Left alone: `binary(0)`, whose only default `''` has nothing to pad (a width-less `binary` is `binary(1)` and is padded), `binary` wider than 255 and a default longer than the width (MySQL rejects both), decimal and float defaults, and expression defaults |
   | `charDefaultSpacesNormalizer` | rewrites the string `DEFAULT` of a `char(N)` or `varchar(N)` column to the value `SHOW CREATE TABLE` reports. A `char` value is read back with every trailing space stripped, in every charset and collation (NO PAD collations included): `char(4) DEFAULT 'a  '` is reported as `DEFAULT 'a'`, `DEFAULT '    '` as `DEFAULT ''`. Only U+0020 is stripped; a tab or NUL is data. A `varchar` default keeps its trailing spaces, except spaces past the column width, which MySQL drops (`varchar(4) DEFAULT 'ab      '` → `DEFAULT 'ab  '`). Stripping both sides also converges a reading taken under the `PAD_CHAR_TO_FULL_LENGTH` sql_mode, which reports the `char` default padded to the width. A hex or bit literal default is read as the string its bytes form wherever `charBinaryLiteralDefaultNormalizer` reads it as one, then handled the same way (`char(4) DEFAULT x'612020'` → `DEFAULT 'a'`), so the two rules agree in either order. Left alone: a `char`/`varchar` whose charset resolves to `binary` (spaces are data there; see `binaryDefaultBytesNormalizer`), `varchar` spaces past the width unless the charset resolves to utf8mb4/utf8mb3/latin1/ascii (MySQL rejects them in utf16/utf32/ucs2, and a charset the statement does not determine is inherited from the schema, which may be one of those), other whitespace past the width, and expression defaults |
   | `integerBinaryLiteralDefaultNormalizer` | rewrites a hex or bit literal `DEFAULT` on an integer or unscaled `decimal` column to the integer MySQL stores, which reads the literal's bytes as a big-endian unsigned integer: `int DEFAULT 0x1A` is reported as `DEFAULT '26'`, `int DEFAULT b'1010'` as `DEFAULT '10'`. Left alone: an empty literal and one longer than 8 bytes (MySQL rejects both), a value of 2^63 or more on `decimal` (MySQL rejects it as hex, but not in decimal), expression defaults, and scaled `decimal`/`year`/`float`/`double`, which store the value in their own form (`decimal(5,2) DEFAULT 0x1A` is `'26.00'`, `year DEFAULT 0x07` is `'2007'`) |
   | `bitDefaultNormalizer` | rewrites the literal `DEFAULT` of a `bit(N)` column to the bit literal MySQL reports: the stored value in binary with no leading zeros. Covers hex literals (`bit(8) DEFAULT x'61'` → `b'1100001'`), bit literals, non-negative integers (`bit(1) DEFAULT 0` → `b'0'`) and strings, which are read as their bytes (`bit(8) DEFAULT '0'` → `b'110000'`). Left alone: a value that does not fit in N bits, an empty hex or bit literal, more than 8 bytes and negative integers (MySQL rejects all of them), decimals and floats (MySQL rounds them), `TRUE`/`FALSE` (folded by `booleanKeywordDefaultNormalizer`), and expression defaults |
   | `charBinaryLiteralDefaultNormalizer` | rewrites a hex or bit literal `DEFAULT` on a `char`/`varchar` column to the string its bytes spell: `varchar(4) DEFAULT 0x61` is reported as `DEFAULT 'a'`. Only where the bytes are reported unchanged: on utf8mb4/utf8mb3 when they are valid utf8mb3 (otherwise MySQL reports hex, which the written literal already matches, or rejects them), and, when every byte is ASCII, on the charsets that map those bytes unchanged (`asciiCompatibleCharsets`: latin1, ascii, gbk, sjis, and the others measured). Left alone: a column whose charset the statement does not determine (it takes the database default, which can be utf16: `char(2) DEFAULT x'6162'` there is U+6162), other charsets (utf16 and the other wide charsets pad, and swe7 maps `x'5b'` to `Ä`), bytes above 0x7F outside UTF-8 (latin1 transcodes them), a value longer than the width, expression defaults, `enum`/`set`, and a column the binary charset makes `binary`/`varbinary` (handled by `binaryDefaultBytesNormalizer`) |
   | `charUTF8MB4DefaultNormalizer` | records a `DEFAULT` on a utf8mb4 `char`/`varchar` column that is not valid utf8mb3 as the bytes `SHOW CREATE TABLE` reports (MySQL 8.0.33+): `char(4) DEFAULT '😀'` → `DEFAULT 0xF09F9880`. Covers strings and hex/bit literals. A `char` default's trailing spaces are stripped first, and a `varchar` default's spaces past the width are truncated, as MySQL stores them. The value is recorded, and a live hex default on such a column rewritten, as `_utf8mb4 x'f09f9880'`: the introducer makes the emitted `MODIFY` store the character on whatever charset the column has (a bare hex literal on a utf16 column, reachable with `IgnoreCharsetCollation`, stores different characters). Left alone: other charsets (utf16, utf32 and gb18030 report the bytes of their own encoding), a column whose charset the table does not determine, a charset introducer other than `_utf8mb4`/`_binary` (MySQL reads the bytes in that charset: `_latin1'😀'` is `'ðŸ˜€'`), `enum`/`set` (whose non-utf8mb3 members are reported as `'?'`), a value longer than the width, and expression defaults |
   | `enumSetDefaultNormalizer` | rewrites the string `DEFAULT` of an `enum` or `set` column to the member text `SHOW CREATE TABLE` reports. MySQL stores the default as the member(s) it names: it strips the trailing spaces (U+0020 only, NO PAD collations included), then looks the rest up under the column's collation, so `enum('a','b') DEFAULT 'b '` is reported as `DEFAULT 'b'` and `enum('a','B') DEFAULT 'b'` on `utf8mb4_0900_ai_ci` as `DEFAULT 'B'`. A `set` default is split on commas after the strip, and its members are reported once each in definition order (`set('a','b') DEFAULT 'b,a '` → `DEFAULT 'a,b'`). The rule resolves only a member equal byte for byte, or, on a collation known to fold ASCII case (`utf8mb4_0900_ai_ci`/`_as_ci`, `latin1_swedish_ci`, and the `_general_ci`, `_general_mysql500_ci`, `_unicode_ci` and `_unicode_520_ci` families except `cp866_general_ci` and `latin7_general_ci`), a member that differs only in the case of ASCII letters; MySQL rejects collation-duplicate members, so that member is the one it stores. Tailored collations are not folded: their contractions (Danish `aa`, Czech `ch`, ...) and Turkish dotless i compare ASCII case pairs unequal. The legacy `BINARY` attribute selects `_bin`, which strips spaces but does not fold. Left alone: a column whose charset is `binary` (spaces are data), a column whose charset and collation the definition does not determine (the database default may be `binary` or case-sensitive), a `set` default of only spaces (MySQL rejects it), a `set` default naming the empty member (reported the same as the empty set), a default MySQL matches through its collation alone (an accent, a non-ASCII case pair, an ignorable character, a contraction), numeric, `TRUE`/`FALSE`, hex and bit literal defaults, and expression defaults |
   | `integerDisplayWidthNormalizer` | strips deprecated integer display widths (`int(11)` → `int`), keeping signed `tinyint(1)` and `ZEROFILL` (`tinyint(1) unsigned` → `tinyint unsigned`, as MySQL stores it). A `ZEROFILL` column declared without a width, or with a width of 0, gets the type's default unsigned width, as MySQL stores it (`int zerofill` and `int(0) zerofill` → `int(10) unsigned zerofill`, where the parser fills in the signed `int(11)` for the width-less form); any other explicit width, such as `int(11) zerofill`, is kept |
   | `decimalZeroPrecisionNormalizer` | rewrites a zero-precision `DECIMAL` to the default MySQL stores for it (`decimal(0)`, `decimal(0,0)` → `decimal(10,0)`) |
   | `booleanKeywordDefaultNormalizer` | folds a bare `TRUE`/`FALSE` keyword `DEFAULT` to the `1`/`0` MySQL stores, and records the literal form Spirit emits it in: a bare number on a numeric column (`DEFAULT 0`), a quoted string on a string one (`DEFAULT '0'`), a bit literal on a `bit` one (`DEFAULT b'1'`). `SHOW CREATE TABLE` quotes the value on numeric and string columns alike, and only `bit` reports a literal of its own — but quotedness is not part of column identity on a numeric column, so a folded bare `0` and the live `'0'` compare equal. Folds on the integer types, unscaled `decimal`, `double`/`float`, `varchar`/`char`, `varbinary` and `bit`. Left alone where the keyword is stored as something else: scaled `decimal` pads to its scale, `YEAR` reads it as a year, `binary` pads to the column width with NULs (folded by `binaryDefaultBytesNormalizer`), and `enum`/`set` resolve the keyword differently across supported server versions |
   | `vectorDimensionNormalizer` | fills in the default dimension of a `VECTOR` column declared without one (`vector` → `vector(2048)`, MySQL 9.7+) |
   | `charsetlessTypeNormalizer` | drops charset/collation from the types that cannot carry one (`VECTOR`, spatial) — both the parser's synthetic `binary` charset and one an author wrote by hand, which MySQL accepts and silently discards |
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

## Best Practices

1. **Use SHOW CREATE TABLE**: Always parse the output of `SHOW CREATE TABLE` rather than user-provided CREATE statements. We refer to this in some places as "the canonical show create table".
2. **Use ALTER TABLE**: Use `ALTER TABLE` syntax over `CREATE/DROP INDEX` syntax. The rewriting of `CREATE INDEX` is best-effort and does not support complex expressions.

## See Also

- [pkg/parser](../parser/README.md) - Spirit's SQL parser (MySQL-only fork of the TiDB parser)
- [MySQL 8.0 Online DDL Operations](https://dev.mysql.com/doc/refman/8.0/en/innodb-online-ddl-operations.html)
- [pkg/table](../table/README.md) - Uses statement parsing for table metadata
- [pkg/migration](../migration/README.md) - Uses safety analysis to determine migration strategy
