//nolint:noinlineerr,exhaustive
package statement

// This file provides structured parsing of CREATE TABLE statements.
// The CreateTable struct and related types use pointer fields for optional elements

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/parser"
	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/parser/charset"
	"github.com/block/spirit/pkg/parser/format"
	"github.com/block/spirit/pkg/parser/mysql"
	"github.com/block/spirit/pkg/parser/types"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
)

// CreateTable represents a parsed CREATE TABLE statement with structured data
type CreateTable struct {
	Raw          *ast.CreateTableStmt `json:"-"`
	TableName    string               `json:"table_name"`
	Temporary    bool                 `json:"temporary"`
	IfNotExists  bool                 `json:"if_not_exists"`
	Columns      Columns              `json:"columns"`
	Indexes      Indexes              `json:"indexes"`
	Constraints  Constraints          `json:"constraints"`
	TableOptions *TableOptions        `json:"table_options,omitempty"`
	Partition    *PartitionOptions    `json:"partition,omitempty"`
}

// Column represents a table column definition
type Column struct {
	Raw             *ast.ColumnDef `json:"-"`
	Name            string         `json:"name"`
	Type            string         `json:"type"`
	Length          *int           `json:"length,omitempty"` // nil = no width; 0 is a real width (varchar(0))
	Precision       *int           `json:"precision,omitempty"`
	Scale           *int           `json:"scale,omitempty"`
	Unsigned        *bool          `json:"unsigned,omitempty"`
	Zerofill        *bool          `json:"zerofill,omitempty"`    // ZEROFILL display attribute (implies unsigned)
	EnumValues      []string       `json:"enum_values,omitempty"` // Permitted values for ENUM type
	SetValues       []string       `json:"set_values,omitempty"`  // Permitted values for SET type
	Nullable        bool           `json:"nullable"`
	Default         *string        `json:"default,omitempty"`
	DefaultIsExpr   bool           `json:"default_is_expr,omitempty"`  // true when default is an expression (needs parens), e.g. DEFAULT (json_object())
	DefaultKind     DefaultKind    `json:"default_kind,omitempty"`     // the literal form the default was written as, read off the AST — see DefaultKind
	OnUpdate        *string        `json:"on_update,omitempty"`        // ON UPDATE expression for TIMESTAMP/DATETIME, e.g. "current_timestamp"
	GeneratedExpr   *string        `json:"generated_expr,omitempty"`   // Expression for GENERATED ALWAYS AS (...) columns
	GeneratedStored bool           `json:"generated_stored,omitempty"` // true = STORED, false = VIRTUAL (only meaningful when GeneratedExpr is set)
	// DefaultAsWritten is the literal DEFAULT as the schema spelled it, kept
	// for emission. Default holds the value MySQL stores for that literal, as
	// the normalization rules read it (the text SHOW CREATE TABLE reports
	// where that text is exact), and Diff compares it. The two can name
	// different values: MySQL reports a float with six significant digits, so
	// the live `float DEFAULT 1234567` reads back as '1234570', and a MODIFY
	// that emitted that text would store 1234570. The written literal is the
	// one MySQL reads itself, so emitting it stores exactly what a CREATE
	// TABLE with it would have.
	// Nil for an expression default, which is emitted from Default, and when a
	// rule rewrites the emitted form on purpose (charUTF8MB4DefaultNormalizer).
	// Not compared: see columnFieldsNotCompared.
	DefaultAsWritten *DefaultLiteral `json:"default_as_written,omitempty"`
	Checks           []ColumnCheck   `json:"checks,omitempty"`        // Column-level CHECK constraints, in declaration order; hoisted into Constraints by columnCheckNormalizer
	SRID             *uint32         `json:"srid,omitempty"`          // SRID attribute for spatial columns
	Invisible        bool            `json:"invisible,omitempty"`     // INVISIBLE (MySQL 8.0.23+); VISIBLE is the default and is not recorded
	NotSecondary     bool            `json:"not_secondary,omitempty"` // NOT SECONDARY: excluded from the secondary engine
	ColumnFormat     *string         `json:"column_format,omitempty"` // COLUMN_FORMAT FIXED|DYNAMIC; DEFAULT is not recorded
	Storage          *string         `json:"storage,omitempty"`       // STORAGE DISK|MEMORY; DEFAULT is not recorded
	// SecondaryEngineAttribute is the SECONDARY_ENGINE_ATTRIBUTE JSON text as
	// written. MySQL reports it re-serialized, so it is compared as JSON
	// (engineAttributeEqual) and emitted as written.
	SecondaryEngineAttribute *string           `json:"secondary_engine_attribute,omitempty"`
	AutoInc                  bool              `json:"auto_increment"`
	PrimaryKey               bool              `json:"primary_key"`
	Unique                   bool              `json:"unique"`
	Comment                  *string           `json:"comment,omitempty"`
	Charset                  *string           `json:"charset,omitempty"`
	Collation                *string           `json:"collation,omitempty"`
	Options                  map[string]string `json:"options,omitempty"`
}

// ColumnCheck is a column-level CHECK constraint as written in a column
// definition: `c INT [CONSTRAINT name] CHECK (expr) [NOT ENFORCED]`. A column
// can carry any number of them. MySQL stores each as a table-level constraint,
// which is how SHOW CREATE TABLE reports them, so columnCheckNormalizer moves
// them to CreateTable.Constraints at parse time and the slice is empty on a
// parsed CreateTable.
type ColumnCheck struct {
	Name        string `json:"name,omitempty"` // the CONSTRAINT name, or "" for MySQL to number it
	Expression  string `json:"expression"`
	NotEnforced bool   `json:"not_enforced,omitempty"`
}

// IndexColumn represents a column or expression in an index
type IndexColumn struct {
	Name       string  `json:"name,omitempty"`       // Column name (empty for expression indexes)
	Expression *string `json:"expression,omitempty"` // Expression for functional indexes
	Length     *int    `json:"length,omitempty"`     // Prefix length for string columns
	Desc       bool    `json:"desc,omitempty"`       // Descending key part (MySQL 8.0+), e.g. KEY (a DESC)
}

// Index represents an index definition
type Index struct {
	Raw        *ast.Constraint `json:"-"`
	Name       string          `json:"name"`
	Type       string          `json:"type"`                  // PRIMARY KEY, UNIQUE, INDEX, FULLTEXT, SPATIAL
	Columns    []string        `json:"columns"`               // Deprecated: use ColumnList for full details
	ColumnList []IndexColumn   `json:"column_list,omitempty"` // Full column specifications including prefix/expression
	Invisible  *bool           `json:"invisible,omitempty"`
	Using      *string         `json:"using,omitempty"` // BTREE, HASH, RTREE
	Comment    *string         `json:"comment,omitempty"`
	// KeyBlockSize is the index's own KEY_BLOCK_SIZE; nil when none is set or
	// it equals the table's, which SHOW CREATE TABLE omits
	// (indexDefaultsNormalizer).
	KeyBlockSize *uint64           `json:"key_block_size,omitempty"`
	ParserName   *string           `json:"parser_name,omitempty"`
	Options      map[string]string `json:"options,omitempty"`
	// SecondaryEngineAttribute is the index's SECONDARY_ENGINE_ATTRIBUTE JSON
	// text as written; compared as JSON (engineAttributeEqual) because MySQL
	// reports it re-serialized, and emitted as written.
	SecondaryEngineAttribute *string `json:"secondary_engine_attribute,omitempty"`

	// InlineDerived marks a UNIQUE index that indexNormalizer synthesized
	// from an inline column-level UNIQUE (`c INT UNIQUE`). Its name is only a
	// guess at the server-assigned one (the column name, suffixed on collision),
	// so diffIndexes pairs it with an equivalent unique index on the other side
	// by column set even when the names differ, and compares the pair under
	// that side's name (see pairInlineUniqueNames) rather than emitting a
	// spurious DROP+ADD. Not serialized: it is a diff-time hint, not part of
	// the logical schema.
	InlineDerived bool `json:"-"`
}

// Constraint represents a table constraint
type Constraint struct {
	Raw         *ast.Constraint      `json:"-"`
	Name        string               `json:"name"`
	Type        string               `json:"type"` // CHECK, FOREIGN KEY, etc.
	Columns     []string             `json:"columns,omitempty"`
	Expression  *string              `json:"expression,omitempty"`
	References  *ForeignKeyReference `json:"references,omitempty"`
	Definition  *string              `json:"definition,omitempty"`   // Generated definition string for compatibility
	NotEnforced bool                 `json:"not_enforced,omitempty"` // CHECK constraints only: true when NOT ENFORCED
	Options     map[string]any       `json:"options,omitempty"`
}

type Indexes []Index
type Columns []Column
type Constraints []Constraint

// HasName is a type constraint for types that have a Name field
type HasName interface {
	GetName() string
}

func (indexes Indexes) HasInvisible() bool {
	for _, idx := range indexes {
		if idx.Invisible != nil && *idx.Invisible {
			return true
		}
	}

	return false
}

func (constraints Constraints) HasForeignKeys() bool {
	for _, c := range constraints {
		if c.Type == "FOREIGN KEY" {
			return true
		}
	}

	return false
}

// ByName is a generic function that finds an element by name in any slice of types with Name field
// NOTE: This function assumes that names are unique within the slice! That will be true for
// "canonical" CREATE TABLE statements as returned by SHOW CREATE TABLE, but may not be true for
// arbitrary input.
func ByName[T HasName](slice []T, name string) *T {
	for _, item := range slice {
		if item.GetName() == name {
			return &item
		}
	}

	return nil
}

func (i Index) GetName() string {
	return i.Name
}

func (c Column) GetName() string {
	return c.Name
}

func (c Constraint) GetName() string {
	return c.Name
}

func (indexes Indexes) ByName(name string) *Index {
	return ByName(indexes, name)
}

func (columns Columns) ByName(name string) *Column {
	return ByName(columns, name)
}

func (constraints Constraints) ByName(name string) *Constraint {
	return ByName(constraints, name)
}

// CarriesCharset reports whether the column's type stores text, and therefore
// has a charset and collation that participate in comparisons. Numeric, date,
// binary, JSON and spatial types are excluded: they carry at most a synthetic
// "binary" charset that is identical for any two columns of the same type.
func (c *Column) CarriesCharset() bool {
	return charsetCarryingTypes[strings.ToLower(c.Type)]
}

// EffectiveCharsetCollation returns the charset and collation the column
// actually compares under, given the table that owns it. It resolves the
// column's own clauses against the table defaults exactly as MySQL does (see
// resolvedCharsetCollation), and then fills in the charset's *default*
// collation when no COLLATE was written anywhere. That last step matters
// because SHOW CREATE TABLE can omit COLLATE when it is the charset default, so
// a table spelled `DEFAULT CHARSET=latin1` means latin1_swedish_ci and must
// compare unequal to one that spells `COLLATE=latin1_bin`.
//
// For utf8mb4 that default is an assumption. A server resolves utf8mb4 named
// without a collation to its default_collation_for_utf8mb4, which can be
// utf8mb4_general_ci, while this always answers utf8mb4_0900_ai_ci, MySQL 8.0's
// default. A caller that refuses a statement on the strength of the answer
// must not rely on that guess, and uses determinedCharsetCollation instead.
//
// Either return value is "" when the statement does not determine it: a table
// with no DEFAULT CHARSET at all (only reachable from hand-written DDL, since
// SHOW CREATE TABLE always emits one) inherits the schema/server default, and
// a charset this parser does not know has no default collation to look up.
// Callers must treat "" as "unknown" rather than as a value that can differ.
//
// Names are returned in MySQL 8.0's spelling: the legacy utf8/utf8_* forms are
// folded onto utf8mb3/utf8mb3_*, so the two spellings of the same charset
// compare equal.
//
// The diff does not use this: it deliberately treats an unwritten collation as
// a match (see charsetCollationEqual) so it never emits a MODIFY it cannot
// prove converged. Only utf8mb4 reaches it that way: every other charset's
// default collation is fixed, and defaultCollationNormalizer writes it in at
// parse time. A linter has the opposite bias — it reports a difference it can
// prove, and stays silent otherwise.
func (c *Column) EffectiveCharsetCollation(table *CreateTable) (cs, collation string) {
	cs, collation = resolvedCharsetCollation(c, table)
	if collation == "" && cs != "" {
		if def, ok := charset.MySQLDefaultCollation(cs); ok {
			collation = strings.ToLower(def)
		}
	}
	return NormalizeCharsetName(cs), normalizeCollationName(collation)
}

// determinedCharsetCollation returns the charset and collation the column
// compares under, each "" when the definition does not decide it. It differs
// from EffectiveCharsetCollation in one respect: where the definition names
// only a charset, it supplies that charset's default collation only when every
// server agrees on it (see charsetDefaultCollationIsFixed). A caller that
// refuses a statement on the strength of the answer needs that certainty; a
// linter does not.
func (c *Column) determinedCharsetCollation(table *CreateTable) (cs, collation string) {
	cs, collation = resolvedCharsetCollation(c, table)
	cs = NormalizeCharsetName(cs)
	if collation != "" {
		return cs, normalizeCollationName(collation)
	}
	if !charsetDefaultCollationIsFixed(cs) {
		return cs, ""
	}
	if _, def, ok := DefaultCollationForCharset(cs); ok {
		return cs, def
	}
	return cs, ""
}

// declaresNull reports whether the column definition explicitly permits NULL,
// with a NULL attribute or a literal DEFAULT NULL. It reads the AST because
// Nullable cannot tell an explicit NULL apart from an omitted NOT NULL. A
// column built without a Raw definition declares nothing.
//
// Any NULL attribute counts, even one followed by NOT NULL: MySQL rejects
// `a INT NULL NOT NULL` in a primary key rather than letting the last attribute
// win. An expression default, DEFAULT (NULL), does not count: MySQL accepts it
// on a key column and stores the column NOT NULL.
func (c *Column) declaresNull() bool {
	if c.Raw == nil {
		return false
	}
	for _, opt := range c.Raw.Options {
		switch opt.Tp { //nolint:exhaustive
		case ast.ColumnOptionNull:
			return true
		case ast.ColumnOptionDefaultValue:
			if v, ok := opt.Expr.(*ast.ValueExpr); ok && v.Kind() == ast.KindNull {
				return true
			}
		}
	}
	return false
}

// declaresNullAfterAutoIncrement reports whether a NULL attribute follows the
// AUTO_INCREMENT attribute in the column definition. MySQL applies the
// attributes in order — AUTO_INCREMENT implies NOT NULL and a later NULL
// clears it — so this is the one spelling of a nullable AUTO_INCREMENT column
// (see autoIncrementNotNullNormalizer). A column built without a Raw
// definition declares nothing.
func (c *Column) declaresNullAfterAutoIncrement() bool {
	if c.Raw == nil {
		return false
	}
	seenAutoInc, nullAfter := false, false
	for _, opt := range c.Raw.Options {
		switch opt.Tp { //nolint:exhaustive
		case ast.ColumnOptionAutoIncrement:
			seenAutoInc, nullAfter = true, false
		case ast.ColumnOptionNull:
			nullAfter = seenAutoInc
		}
	}
	return nullAfter
}

// ForeignKeyReference represents a foreign key reference.
//
// Schema is the referenced table's schema when the reference is qualified
// (REFERENCES db.parent) and empty when it is not. SHOW CREATE TABLE
// qualifies a reference only when the parent is in another schema, and a
// reference qualified with the table's own schema reads back unqualified, so
// an empty Schema means the table's own schema — which a parsed CREATE TABLE
// does not know. Two references therefore compare equal unless both are
// qualified and name different schemas (see
// constraintsEqualIgnoreNameAndEnforcement); a desired REFERENCES db2.parent
// is not told apart from a live REFERENCES parent, whichever schema that is.
type ForeignKeyReference struct {
	Schema   string   `json:"schema,omitempty"`
	Table    string   `json:"table"`
	Columns  []string `json:"columns"`
	OnDelete *string  `json:"on_delete,omitempty"`
	OnUpdate *string  `json:"on_update,omitempty"`
}

// referencedTable returns the referenced table as a definition spells it,
// schema-qualified when the reference is.
func (r *ForeignKeyReference) referencedTable() string {
	if r.Schema != "" {
		return r.Schema + "." + r.Table
	}
	return r.Table
}

// TableOptions represents table-level options
type TableOptions struct {
	Engine        *string `json:"engine,omitempty"`
	Charset       *string `json:"charset,omitempty"`
	Collation     *string `json:"collation,omitempty"`
	Comment       *string `json:"comment,omitempty"`
	AutoIncrement *uint64 `json:"auto_increment,omitempty"`
	RowFormat     *string `json:"row_format,omitempty"`
	// KeyBlockSize is the table-level KEY_BLOCK_SIZE, the compressed page
	// size in KiB. It belongs with ROW_FORMAT (it implies COMPRESSED and
	// InnoDB rejects it with any other row format), so Diff treats the two
	// together under DiffOptions.IgnoreRowFormat. 0 means unset.
	KeyBlockSize *uint64 `json:"key_block_size,omitempty"`
	// AutoextendSize is AUTOEXTEND_SIZE in bytes. MySQL accepts it with a
	// K/M/G suffix (4M) and reports it in bytes (4194304). 0 means unset.
	AutoextendSize *uint64 `json:"autoextend_size,omitempty"`
	// The following are reported by SHOW CREATE TABLE only when set; each
	// has a value MySQL treats as "unset" that Diff emits to clear it.
	StatsPersistent  *bool   `json:"stats_persistent,omitempty"`   // STATS_PERSISTENT=0|1; DEFAULT is not recorded
	StatsAutoRecalc  *bool   `json:"stats_auto_recalc,omitempty"`  // STATS_AUTO_RECALC=0|1; DEFAULT is not recorded
	StatsSamplePages *uint64 `json:"stats_sample_pages,omitempty"` // STATS_SAMPLE_PAGES=n; 0 and DEFAULT are not recorded
	PackKeys         *bool   `json:"pack_keys,omitempty"`          // PACK_KEYS=0|1; DEFAULT is not recorded
	Checksum         bool    `json:"checksum,omitempty"`           // CHECKSUM=1
	DelayKeyWrite    bool    `json:"delay_key_write,omitempty"`    // DELAY_KEY_WRITE=1
	AvgRowLength     *uint64 `json:"avg_row_length,omitempty"`     // 0 is not recorded
	MinRows          *uint64 `json:"min_rows,omitempty"`           // 0 is not recorded
	MaxRows          *uint64 `json:"max_rows,omitempty"`           // 0 is not recorded
	// SecondaryEngineAttribute is the table's SECONDARY_ENGINE_ATTRIBUTE JSON
	// text as written; compared as JSON (engineAttributeEqual) because MySQL
	// reports it re-serialized, and emitted as written. '' is not recorded.
	SecondaryEngineAttribute *string `json:"secondary_engine_attribute,omitempty"`
}

// PartitionOptions represents table partitioning configuration
type PartitionOptions struct {
	Type         string                `json:"type"`                    // RANGE, LIST, HASH, KEY
	Expression   *string               `json:"expression,omitempty"`    // For HASH, RANGE and LIST
	Columns      []string              `json:"columns,omitempty"`       // For KEY, RANGE COLUMNS, LIST COLUMNS
	Linear       bool                  `json:"linear,omitempty"`        // For LINEAR HASH/KEY
	KeyAlgorithm uint64                `json:"key_algorithm,omitempty"` // For KEY: ALGORITHM=1; 0 is MySQL's default (2)
	Partitions   uint64                `json:"partitions,omitempty"`    // Number of partitions
	Definitions  []PartitionDefinition `json:"definitions,omitempty"`   // Individual partition definitions
	SubPartition *SubPartitionOptions  `json:"subpartition,omitempty"`  // Subpartitioning options
}

// PartitionDefinition represents a single partition definition
type PartitionDefinition struct {
	Name    string           `json:"name"`
	Values  *PartitionValues `json:"values,omitempty"` // VALUES LESS THAN or VALUES IN
	Comment *string          `json:"comment,omitempty"`
	Engine  *string          `json:"engine,omitempty"`
	PartitionStorage
	Options       map[string]any           `json:"options,omitempty"` // Options MySQL does not accept on a partition
	SubPartitions []SubPartitionDefinition `json:"subpartitions,omitempty"`
}

// PartitionStorage holds the storage options of a partition or subpartition
// definition. A partition with named subpartitions holds none: MySQL stores
// them on each subpartition (see partitionOptionsNormalizer).
type PartitionStorage struct {
	DataDirectory  *string `json:"data_directory,omitempty"`
	IndexDirectory *string `json:"index_directory,omitempty"` // InnoDB rejects it (error 1031)
	MaxRows        *uint64 `json:"max_rows,omitempty"`
	MinRows        *uint64 `json:"min_rows,omitempty"`
	Tablespace     *string `json:"tablespace,omitempty"`
	Nodegroup      *uint64 `json:"nodegroup,omitempty"`
}

// parsePartitionStorageOption stores opt in s if it is a storage option, and
// reports whether it was one.
func parsePartitionStorageOption(opt *ast.TableOption, s *PartitionStorage) bool {
	str, n := opt.StrValue, opt.UintValue
	switch opt.Tp { //nolint:exhaustive // every other option is not a storage option
	case ast.TableOptionDataDirectory:
		s.DataDirectory = &str
	case ast.TableOptionIndexDirectory:
		s.IndexDirectory = &str
	case ast.TableOptionMaxRows:
		s.MaxRows = &n
	case ast.TableOptionMinRows:
		s.MinRows = &n
	case ast.TableOptionTablespace:
		s.Tablespace = &str
	case ast.TableOptionNodegroup:
		s.Nodegroup = &n
	default:
		return false
	}
	return true
}

// PartitionValues represents the VALUES clause in partition definitions
type PartitionValues struct {
	Type   string `json:"type"`   // "LESS_THAN", "IN", "MAXVALUE"
	Values []any  `json:"values"` // The actual values
}

// partitionStringLiteral wraps a partition value that originated from a
// quoted string literal (e.g. LIST COLUMNS on a VARCHAR column:
// VALUES IN ('2020', 'asia')). Wrapping it in a distinct type preserves
// the "this was a string" fact through the []any storage so emission can
// quote it unconditionally — without it, a numeric-looking string value
// like '2020' would be rendered bare and rejected by MySQL (error 1654).
// Numeric/expression partition values remain plain Go strings.
type partitionStringLiteral string

// partitionMaxValue is a sentinel representing the MAXVALUE keyword inside a
// partition VALUES LESS THAN value list (e.g. the tuple elements of
// VALUES LESS THAN (10, MAXVALUE) in multi-column RANGE COLUMNS). Without a
// distinct type, MAXVALUE would be stored as the plain string "MAXVALUE" and
// emitted as the quoted string literal 'MAXVALUE', which MySQL rejects
// (error 1697). The single-expression form (VALUES LESS THAN MAXVALUE, or
// its parenthesized spelling) is normalized further, to
// PartitionValues.Type == "MAXVALUE" (see parsePartitionClause), matching
// SHOW CREATE TABLE's bare-keyword form.
type partitionMaxValue struct{}

// partitionNullValue is a sentinel for the NULL literal in a LIST partition's
// VALUES IN list. Stored as the plain string "NULL" it would be emitted as
// the string literal 'NULL', which is a different value: a REORGANIZE built
// from it moves the NULL rows into no partition, and MySQL deletes them
// without an error.
type partitionNullValue struct{}

// partitionValueTuple is one multi-column value of a LIST COLUMNS partition,
// e.g. each of (1, 2) and (3, 4) in VALUES IN ((1, 2), (3, 4)). Keeping the
// tuple as one element of PartitionValues.Values preserves which values go
// together: flattened to 1, 2, 3, 4, the clause can't be emitted (MySQL
// error 1653) and a regrouping of the same values compares equal.
type partitionValueTuple []any

// partitionExprValue is a partition value written as an expression rather
// than a literal, e.g. 10+10 or TO_DAYS('2030-01-01'). It renders bare:
// quoted, it would be the string '10+10', which MySQL rejects (error 1697).
// MySQL evaluates the expression when it stores the partition, and the
// partition-bound-constants rule folds the ones it can evaluate offline into
// the literal MySQL reports.
type partitionExprValue string

// SubPartitionOptions represents subpartitioning configuration
type SubPartitionOptions struct {
	Type         string   `json:"type"`                    // HASH, KEY
	Expression   *string  `json:"expression,omitempty"`    // For HASH
	Columns      []string `json:"columns,omitempty"`       // For KEY
	Linear       bool     `json:"linear,omitempty"`        // For LINEAR HASH/KEY
	KeyAlgorithm uint64   `json:"key_algorithm,omitempty"` // For KEY: ALGORITHM=1; 0 is MySQL's default (2)
	Count        uint64   `json:"count,omitempty"`         // Number of subpartitions
}

// SubPartitionDefinition represents a single subpartition definition
type SubPartitionDefinition struct {
	Name    string  `json:"name"`
	Comment *string `json:"comment,omitempty"`
	Engine  *string `json:"engine,omitempty"`
	PartitionStorage
	Options map[string]any `json:"options,omitempty"` // Options MySQL does not accept on a subpartition
}

// tableSchema represents a parsed CREATE TABLE statement with flexible access
/*
type tableSchema struct {
	raw    *ast.CreateTableStmt
	parsed *CreateTable
}

*/

// ParseCreateTable parses a CREATE TABLE statement and returns an analyzer
// This function is particularly designed to be used with the output of SHOW CREATE TABLE,
// which we consider to be the "canonical" form of a CREATE TABLE statement.
//
// Because there's so much variation in the ways a human might write a CREATE TABLE statement,
// from index names being auto-generated to column attributes being turned into table
// options, you should consider use of this function on non-canonical CREATE statements
// to be experimental at best.
//
// Note also that this parser does not attempt to validate the SQL beyond what the
// underlying parser does. For example, it will not check that a PRIMARY KEY column is NOT NULL,
// or that column names are unique, or that indexed columns exist.
func ParseCreateTable(sql string) (*CreateTable, error) {
	p := parser.New()

	stmts, _, err := p.Parse(sql, "", "")
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}

	if len(stmts) != 1 {
		return nil, fmt.Errorf("expected exactly one statement, got %d", len(stmts))
	}

	createStmt, ok := stmts[0].(*ast.CreateTableStmt)
	if !ok {
		return nil, fmt.Errorf("expected CREATE TABLE statement, got %T", stmts[0])
	}

	// Parse into structured format
	ct := &CreateTable{
		Raw: createStmt,
	}
	// Parse into structured format
	ct.parseToStruct()
	if err != nil {
		return nil, fmt.Errorf("failed to parse CREATE TABLE: %w", err)
	}
	return ct, nil
}

// Implementation of CreateTable interface

func (ct *CreateTable) GetCreateTable() *CreateTable {
	return ct
}

func (ct *CreateTable) GetTableName() string {
	return ct.TableName
}

func (ct *CreateTable) GetColumns() Columns {
	return ct.Columns
}

func (ct *CreateTable) GetIndexes() Indexes {
	indexList := make([]Index, 0, len(ct.Indexes))

	// Add table-level indexes
	for _, index := range ct.Indexes {
		if index.Type == "PRIMARY KEY" {
			if index.Name == "" {
				index.Name = "PRIMARY"
			}
		}
		indexList = append(indexList, index)
	}

	// Add column-level constraints that turn into indexes (PRIMARY KEY, UNIQUE)
	for _, col := range ct.Columns {
		if col.PrimaryKey {
			indexList = append(indexList, Index{
				Name:    "PRIMARY",
				Type:    "PRIMARY KEY",
				Columns: []string{col.Name},
			})
		}

		if col.Unique {
			indexList = append(indexList, Index{
				// The real name of this index is computed by the server
				Name:    "UNIQUE " + col.Name,
				Type:    "UNIQUE",
				Columns: []string{col.Name},
			})
		}
	}

	return indexList
}

func (ct *CreateTable) GetConstraints() Constraints {
	return ct.Constraints
}

func (ct *CreateTable) GetTableOptions() map[string]any {
	options := make(map[string]any)

	if ct.TableOptions != nil {
		opts := ct.TableOptions
		if opts.Engine != nil {
			options["engine"] = *opts.Engine
		}

		if opts.Charset != nil {
			options["charset"] = *opts.Charset
		}

		if opts.Collation != nil {
			options["collation"] = *opts.Collation
		}

		if opts.Comment != nil {
			options["comment"] = *opts.Comment
		}

		if opts.AutoIncrement != nil {
			options["auto_increment"] = *opts.AutoIncrement
		}

		if opts.RowFormat != nil {
			options["row_format"] = *opts.RowFormat
		}
	}

	return options
}

// TableDefault returns the charset and collation a column declared without
// either takes in this table. The collation is empty when the definition does
// not determine it — DEFAULT CHARSET=utf8mb4 alone takes the server's default
// for it — and both are empty when the definition declares no default at all,
// which leaves it to the schema. SHOW CREATE TABLE always spells both out.
func (ct *CreateTable) TableDefault() CharsetCollation {
	var d CharsetCollation
	if collation := ct.TableOptions.getCollation(); collation != nil {
		d.Collation = *collation
	}
	if charset := ct.TableOptions.getCharset(); charset != nil {
		d.Charset = *charset
	}
	d = d.normalized()
	if d.Collation == "" && charsetDefaultCollationIsFixed(d.Charset) {
		if _, collation, ok := DefaultCollationForCharset(d.Charset); ok {
			d.Collation = collation
		}
	}
	return d
}

func (ct *CreateTable) GetPartition() *PartitionOptions {
	return ct.Partition
}

// getPrimaryKeyIndex returns the PRIMARY KEY index if it exists (table-level PK), nil otherwise
func (ct *CreateTable) getPrimaryKeyIndex() *Index {
	for i := range ct.Indexes {
		if ct.Indexes[i].Type == "PRIMARY KEY" {
			return &ct.Indexes[i]
		}
	}
	return nil
}

// ToTableInfo builds a connection-less table.TableInfo from the parsed CREATE
// TABLE, carrying the column types, charsets and collations, the table's
// default charset and collation, and the primary key columns that Spirit's
// checks read from table metadata. schemaName names the schema the table lives
// in; it is only used for error messages and by checks that query MySQL, which
// cannot run against the returned TableInfo anyway (see
// table.NewTableInfoFromMeta).
//
// This lets a caller holding a table's DDL — typically its SHOW CREATE TABLE —
// supply check.Resources.Table without opening a connection.
func (ct *CreateTable) ToTableInfo(schemaName string) (*table.TableInfo, error) {
	columns := make([]table.ColumnMeta, 0, len(ct.Columns))
	for i := range ct.Columns {
		col := &ct.Columns[i]
		meta := table.ColumnMeta{
			Name:      col.Name,
			MySQLType: formatColumnTypeAsMetadata(col),
			Generated: col.GeneratedExpr != nil,
		}
		if col.CarriesCharset() {
			meta.Charset, meta.Collation = col.determinedCharsetCollation(ct)
			meta.CollationUnknown = meta.Collation == ""
		}
		columns = append(columns, meta)
	}
	ti, err := table.NewTableInfoFromMeta(schemaName, ct.TableName, columns, ct.primaryKeyColumns())
	if err != nil {
		return nil, fmt.Errorf("build table metadata for %q: %w", ct.TableName, err)
	}
	defaults := ct.TableDefault()
	ti.DefaultCharset, ti.DefaultCollation = defaults.Charset, defaults.Collation
	return ti, nil
}

// primaryKeyColumns returns the table's primary key columns in key order, or
// nil when the table has no primary key. An inline column-level PRIMARY KEY has
// already been materialized into a table-level index by primaryKeyNormalizer,
// so reading the PRIMARY KEY index covers both spellings.
func (ct *CreateTable) primaryKeyColumns() []string {
	pk := ct.getPrimaryKeyIndex()
	if pk == nil {
		return nil
	}
	return pk.Columns
}

// ToTableSchema converts a parsed CreateTable back to a table.TableSchema
// by restoring the AST to SQL. This is useful when callers have already parsed
// schemas (e.g. for linting) but need to pass them to DeclarativeToImperative.
func (ct *CreateTable) ToTableSchema() (table.TableSchema, error) {
	var sb strings.Builder
	rCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	if err := ct.Raw.Restore(rCtx); err != nil {
		return table.TableSchema{}, fmt.Errorf("failed to restore CREATE TABLE for %q: %w", ct.TableName, err)
	}
	return table.TableSchema{
		Name:   ct.TableName,
		Schema: sb.String(),
	}, nil
}

// parseToStruct converts the AST into a structured CreateTable
func (ct *CreateTable) parseToStruct() {
	ct.TableName = ct.Raw.Table.Name.String()
	ct.IfNotExists = ct.Raw.IfNotExists
	ct.Temporary = ct.Raw.TemporaryKeyword != 0
	ct.Columns = make([]Column, 0, len(ct.Raw.Cols))
	ct.Indexes = make([]Index, 0)
	ct.Constraints = make([]Constraint, 0)

	// Parse columns
	for _, col := range ct.Raw.Cols {
		column := ct.parseColumn(col)
		ct.Columns = append(ct.Columns, column)
	}

	// Parse constraints/indexes
	for _, constraint := range ct.Raw.Constraints {
		switch constraint.Tp {
		case ast.ConstraintCheck:
			ct.Constraints = append(ct.Constraints, ct.parseConstraint(constraint))
		case ast.ConstraintForeignKey:
			ct.Constraints = append(ct.Constraints, ct.parseConstraint(constraint))
		default:
			// Other constraints are treated as indexes
			ct.Indexes = append(ct.Indexes, ct.parseIndex(constraint))
		}
	}

	// Parse table options
	if len(ct.Raw.Options) > 0 {
		ct.TableOptions = ct.parseTableOptions(ct.Raw.Options)
	}

	// Parse partition options
	if ct.Raw.Partition != nil {
		ct.Partition = ct.parsePartitionOptions(ct.Raw.Partition)
	}

	// Apply the normalization rules now that every field is populated. This
	// includes the long-standing normalizers (column-CHECK hoisting, unnamed
	// index naming, BINARY-attribute resolution) plus any registered by an
	// init() in a normalize_*.go file. See normalize.go. Copy the result back
	// onto the receiver so a rule that returns a new instance is honored.
	*ct = *runNormalizers(ct)
}

// parseColumn converts a column definition to a Column struct
func (ct *CreateTable) parseColumn(col *ast.ColumnDef) Column {
	column := Column{
		Raw:      col,
		Name:     col.Name.Name.String(),
		Type:     types.TypeStr(col.Tp.GetType()),
		Nullable: true, // Default to nullable
		Options:  make(map[string]string),
	}

	// Spatial types are parsed as TypeGeometry with a subtype; recover the
	// specific type name (point, polygon, ...) so it round-trips correctly
	// when emitting MODIFY/ADD COLUMN.
	if col.Tp.GetType() == mysql.TypeGeometry {
		geoType := col.Tp.GetGeometryType()
		if geoStr := geoType.String(); geoStr != "" {
			column.Type = geoStr
		}
	}

	// Check if this is a binary type (VARBINARY, BLOB, etc.)
	// The TiDB parser converts binary types to their text equivalents,
	// so we need to check the binary flag and convert back. True binary
	// types carry the special "binary" charset; the binary flag with any
	// other (or no) charset is the legacy BINARY column *attribute*
	// (e.g. varchar(100) BINARY), which does NOT change the data type —
	// MySQL canonicalizes it to the binary collation of the column's
	// charset (varchar(100) COLLATE utf8mb4_bin). That case is resolved by
	// binaryAttributeNormalizer once table options are known; converting it
	// here would emit a destructive varchar -> varbinary type change. A
	// column that inherits a binary table default, or writes COLLATE binary,
	// is converted by binaryCharsetNormalizer.
	if mysql.HasBinaryFlag(col.Tp.GetFlag()) && col.Tp.GetCharset() == "binary" {
		if binType, ok := binaryTypeOf(column.Type); ok {
			column.Type = binType
		}
	}

	// Extract type information
	typeStr := col.Tp.String()
	if length, ok := extractLengthFromTypeString(typeStr); ok {
		column.Length = &length
	}

	// Parse precision and scale for decimal types
	if precision, scale := extractPrecisionScaleFromTypeString(typeStr); precision > 0 {
		column.Precision = &precision
		if scale > 0 {
			column.Scale = &scale
		}
	}

	// Check if the column type is unsigned
	if mysql.HasUnsignedFlag(col.Tp.GetFlag()) {
		unsigned := true
		column.Unsigned = &unsigned
	}

	// Check if the column type is zerofill. MySQL still prints the attribute
	// in SHOW CREATE TABLE (e.g. int(10) unsigned zerofill), so dropping it
	// here would both hide a real difference from Diff and silently strip
	// the attribute from MODIFY COLUMN emission. Note the parser mirrors
	// MySQL in adding the unsigned flag automatically for zerofill columns.
	if mysql.HasZerofillFlag(col.Tp.GetFlag()) {
		zerofill := true
		column.Zerofill = &zerofill
	}

	// Extract charset and collation from the type itself
	// (they may be overridden by column options later).
	// Spatial and VECTOR types carry a synthetic "binary" charset/collation
	// here that is not valid SQL to emit; charsetlessTypeNormalizer strips
	// it — along with any the author wrote by hand — once parsing is done.
	if charset := col.Tp.GetCharset(); charset != "" {
		column.Charset = &charset
	}
	if collation := col.Tp.GetCollate(); collation != "" {
		column.Collation = &collation
	}

	// Extract ENUM/SET permitted values
	if col.Tp.GetType() == mysql.TypeEnum {
		if elems := col.Tp.GetElems(); len(elems) > 0 {
			column.EnumValues = elems
		}
	} else if col.Tp.GetType() == mysql.TypeSet {
		if elems := col.Tp.GetElems(); len(elems) > 0 {
			column.SetValues = elems
		}
	}

	// Process column options
	for _, opt := range col.Options {
		switch opt.Tp {
		case ast.ColumnOptionNotNull:
			column.Nullable = false
		case ast.ColumnOptionNull:
			column.Nullable = true
		case ast.ColumnOptionAutoIncrement:
			column.AutoInc = true
		case ast.ColumnOptionPrimaryKey:
			column.PrimaryKey = true
			column.Nullable = false // PRIMARY KEY implies NOT NULL
		case ast.ColumnOptionUniqKey:
			column.Unique = true
		case ast.ColumnOptionDefaultValue:
			if opt.Expr != nil {
				// Detect expression defaults: the TiDB parser wraps non-CURRENT_TIMESTAMP
				// function calls in outer parentheses (e.g., DEFAULT (json_object())).
				// We track this so we can reproduce the correct syntax when generating ALTERs.
				column.DefaultIsExpr = isExpressionDefault(opt.Expr)

				// The parenthesized/bare distinction is captured above;
				// extract the value from inside any parentheses so emission
				// (which re-adds parens from DefaultIsExpr) doesn't double
				// them, e.g. DEFAULT ('{}') stores the string {}.
				defaultExpr := unwrapParenExpr(opt.Expr)

				// Record which literal form the default was written as while
				// the AST is still in hand. Restoring it to text collapses
				// distinctions the characters cannot carry — the TRUE keyword
				// against the 1 it aliases, a bit literal against a string
				// that happens to spell one — and both emission and the
				// normalization rules need them back.
				column.DefaultKind = classifyDefaultLiteral(defaultExpr)

				if literal, isStr := stringLiteralValue(defaultExpr); isStr {
					// Quoted string literal default. Store the true, raw
					// (fully-unescaped) value off the AST; the recorded kind
					// is what re-quotes it on emission — even if the value
					// looks like a keyword (TRUE/NULL) or a number. Escaping
					// happens exactly once, at emit time.
					column.Default = &literal
				} else {
					// Non-string defaults (numeric, functions, expressions):
					// keep the Restored text representation. Only a
					// literal-style default takes MySQL's bare-keyword
					// spelling of CURRENT_TIMESTAMP; inside the parentheses of
					// an expression default the call form is canonical (MySQL
					// stores DEFAULT (CURRENT_TIMESTAMP) as DEFAULT (now())).
					defaultRaw := fmt.Sprintf("%v", restoreValueExprText(defaultExpr, !column.DefaultIsExpr))
					column.Default = &defaultRaw
				}
				if !column.DefaultIsExpr {
					// Keep the literal for emission before the normalization
					// rules rewrite Default to MySQL's reading of it.
					column.DefaultAsWritten = &DefaultLiteral{Text: *column.Default, Kind: column.DefaultKind}
				}
			}
		case ast.ColumnOptionComment:
			if opt.Expr != nil {
				// A column comment is always a string literal; read its true
				// value directly off the AST so quotes/backslashes survive
				// the round-trip and are escaped exactly once on emission.
				if literal, isStr := stringLiteralValue(opt.Expr); isStr && literal != "" {
					column.Comment = &literal
				}
			}
		case ast.ColumnOptionCollate:
			if opt.StrValue != "" {
				column.Collation = &opt.StrValue
			}
		case ast.ColumnOptionOnUpdate:
			// ON UPDATE CURRENT_TIMESTAMP[(n)] — only valid for TIMESTAMP/DATETIME.
			// Reuse parseExpression so the stored form matches DEFAULT handling:
			// lowercased, with "()" stripped from zero-arg timestamp functions.
			if opt.Expr != nil {
				if exprStr, ok := ct.parseExpression(opt.Expr).(string); ok && exprStr != "" {
					column.OnUpdate = &exprStr
				}
			}
		case ast.ColumnOptionGenerated:
			// GENERATED ALWAYS AS (expr) [STORED|VIRTUAL]
			if opt.Expr != nil {
				if exprStr, ok := restoreExpressionText(opt.Expr); ok {
					column.GeneratedExpr = &exprStr
					column.GeneratedStored = opt.Stored
				}
			}
		case ast.ColumnOptionCheck:
			// Column-level CHECK (expr). MySQL reports these as table-level
			// constraints in SHOW CREATE TABLE, so this is only seen when
			// parsing user-written statements. A column may carry several,
			// each with its own name and enforcement; every one is kept, in
			// order, for columnCheckNormalizer to hoist.
			if opt.Expr != nil {
				if exprStr, ok := restoreExpressionText(opt.Expr); ok {
					column.Checks = append(column.Checks, ColumnCheck{
						Name:        opt.ConstraintName,
						Expression:  exprStr,
						NotEnforced: !opt.Enforced,
					})
				}
			}
		case ast.ColumnOptionSrid:
			// SRID n — spatial reference system id for spatial columns.
			// SHOW CREATE TABLE emits this as /*!80003 SRID n */ which the
			// parser unwraps as a regular column option.
			srid := opt.Srid
			column.SRID = &srid
		case ast.ColumnOptionVisibility:
			// VISIBLE is the default and MySQL reports nothing for it, so
			// only INVISIBLE is recorded and an explicit VISIBLE compares
			// equal to its absence. SHOW CREATE TABLE emits INVISIBLE as
			// /*!80023 INVISIBLE */. The last one written wins.
			column.Invisible = strings.EqualFold(opt.StrValue, "INVISIBLE")
		case ast.ColumnOptionNotSecondary:
			column.NotSecondary = true
		case ast.ColumnOptionColumnFormat:
			column.ColumnFormat = nonDefaultKeyword(opt.StrValue)
		case ast.ColumnOptionStorage:
			column.Storage = nonDefaultKeyword(opt.StrValue)
		case ast.ColumnOptionSecondaryEngineAttribute:
			// SECONDARY_ENGINE_ATTRIBUTE='' clears the attribute; MySQL then
			// reports nothing, so the empty string is recorded as absent.
			column.SecondaryEngineAttribute = nil
			if opt.StrValue != "" {
				attr := opt.StrValue
				column.SecondaryEngineAttribute = &attr
			}
		default:
			// Store unknown options for flexibility
			column.Options[fmt.Sprintf("option_%d", opt.Tp)] = opt.StrValue
		}
	}

	// Clean up options map if empty
	if len(column.Options) == 0 {
		column.Options = nil
	}

	return column
}

// nonDefaultKeyword returns the uppercased keyword of a COLUMN_FORMAT or
// STORAGE option, or nil for DEFAULT: that keyword means the option is unset,
// and MySQL reports nothing for it.
func nonDefaultKeyword(keyword string) *string {
	upper := strings.ToUpper(keyword)
	if upper == "DEFAULT" {
		return nil
	}
	return &upper
}

// parseIndex converts a constraint to an Index struct
func (ct *CreateTable) parseIndex(constraint *ast.Constraint) Index {
	index := Index{
		Raw:        constraint,
		Name:       constraint.Name,
		Columns:    ct.parseIndexColumns(constraint.Keys),
		ColumnList: ct.parseIndexColumnList(constraint.Keys),
		Options:    make(map[string]string),
	}

	switch constraint.Tp {
	case ast.ConstraintPrimaryKey:
		index.Type = "PRIMARY KEY"
		// MySQL ignores user-specified names on PRIMARY KEYs; SHOW CREATE TABLE
		// never includes one. Normalize to empty so that a named PK
		// (e.g. PRIMARY KEY `version` (`version`)) compares equal to an
		// unnamed PK (PRIMARY KEY (`version`)) during diff.
		index.Name = ""
	case ast.ConstraintKey, ast.ConstraintIndex:
		index.Type = "INDEX"
	case ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
		index.Type = "UNIQUE"
	case ast.ConstraintFulltext:
		index.Type = "FULLTEXT"
	case ast.ConstraintSpatial:
		index.Type = "SPATIAL"
	default:
		panic(fmt.Sprintf("unknown constraint type: %d", constraint.Tp))
	}

	// Parse index options
	if constraint.Option != nil {
		opt := constraint.Option

		// Visibility (VISIBLE/INVISIBLE)
		switch opt.Visibility {
		case ast.IndexVisibilityInvisible:
			invisible := true
			index.Invisible = &invisible
		case ast.IndexVisibilityVisible:
			visible := false
			index.Invisible = &visible
		}

		// Index type (USING BTREE/HASH/RTREE)
		if opt.Tp != ast.IndexTypeInvalid && opt.Tp.String() != "" {
			using := opt.Tp.String()
			index.Using = &using
		}

		// Comment
		if opt.Comment != "" {
			index.Comment = &opt.Comment
		}

		// Key block size
		if opt.KeyBlockSize > 0 {
			index.KeyBlockSize = &opt.KeyBlockSize
		}

		// Parser name (for FULLTEXT indexes)
		if opt.ParserName.L != "" {
			parserName := opt.ParserName.String()
			index.ParserName = &parserName
		}

		if opt.SecondaryEngineAttr != "" {
			attr := opt.SecondaryEngineAttr
			index.SecondaryEngineAttribute = &attr
		}
	}

	// Clean up options map if empty
	if len(index.Options) == 0 {
		index.Options = nil
	}

	return index
}

// parseConstraint converts a constraint to a Constraint struct
func (ct *CreateTable) parseConstraint(constraint *ast.Constraint) Constraint {
	constr := Constraint{
		Raw:     constraint,
		Name:    constraint.Name,
		Columns: ct.parseIndexColumns(constraint.Keys),
		Options: make(map[string]any),
	}

	switch constraint.Tp {
	case ast.ConstraintCheck:
		constr.Type = "CHECK"

		// Capture the enforcement state. The parser defaults Enforced to
		// true, so an absent keyword and an explicit ENFORCED both parse as
		// enforced — matching MySQL, which omits ENFORCED (the default) from
		// SHOW CREATE TABLE and renders the non-default state inside a
		// versioned comment: /*!80016 NOT ENFORCED */. The parser processes
		// that versioned-comment form too (it is above its minimum version),
		// so MySQL's canonical output parses with Enforced=false.
		constr.NotEnforced = !constraint.Enforced

		if constraint.Expr != nil {
			// Use restoreExpressionText (not parseExpression) because CHECK
			// expressions may contain case-sensitive string literals: the
			// result is not lowercased and literals keep their quotes. The
			// stored text is then rewritten into canonical parenthesization
			// by expressionParenNormalizer when the normalization rules run,
			// so MySQL's fully-parenthesized SHOW CREATE TABLE form and
			// user-written DDL compare equal when — and only when — the
			// expressions are structurally identical.
			if exprStr, ok := restoreExpressionText(constraint.Expr); ok {
				constr.Expression = &exprStr
				// Generate definition string
				definition := fmt.Sprintf("CHECK (%s)", exprStr)
				if constr.NotEnforced {
					definition += " NOT ENFORCED"
				}
				constr.Definition = &definition
			}
		}
	case ast.ConstraintForeignKey:
		constr.Type = "FOREIGN KEY"
		if constraint.Refer != nil {
			fkRef := &ForeignKeyReference{
				Schema:  constraint.Refer.Table.Schema.String(),
				Table:   constraint.Refer.Table.Name.String(),
				Columns: ct.parseIndexColumns(constraint.Refer.IndexPartSpecifications),
			}

			// Parse ON DELETE / ON UPDATE actions (check if ReferOpt is
			// non-empty). An explicit NO ACTION is normalized to absent:
			// NO ACTION is MySQL's default referential action (and a synonym
			// for RESTRICT in InnoDB), and SHOW CREATE TABLE omits it. Keeping
			// it verbatim would make a desired schema spelling out NO ACTION
			// forever differ from the live table, re-emitting the same
			// DROP+ADD FOREIGN KEY on every declarative run. RESTRICT is NOT
			// normalized: SHOW CREATE TABLE prints it, so it round-trips.
			if constraint.Refer.OnDelete != nil && constraint.Refer.OnDelete.ReferOpt.String() != "" &&
				constraint.Refer.OnDelete.ReferOpt != ast.ReferOptionNoAction {
				onDelete := constraint.Refer.OnDelete.ReferOpt.String()
				fkRef.OnDelete = &onDelete
			}

			if constraint.Refer.OnUpdate != nil && constraint.Refer.OnUpdate.ReferOpt.String() != "" &&
				constraint.Refer.OnUpdate.ReferOpt != ast.ReferOptionNoAction {
				onUpdate := constraint.Refer.OnUpdate.ReferOpt.String()
				fkRef.OnUpdate = &onUpdate
			}

			constr.References = fkRef

			// Generate definition string
			definition := fmt.Sprintf("FOREIGN KEY (%s) REFERENCES %s (%s)",
				strings.Join(constr.Columns, ", "),
				constr.References.referencedTable(),
				strings.Join(constr.References.Columns, ", "))
			if fkRef.OnDelete != nil {
				definition += fmt.Sprintf(" ON DELETE %s", *fkRef.OnDelete)
			}
			if fkRef.OnUpdate != nil {
				definition += fmt.Sprintf(" ON UPDATE %s", *fkRef.OnUpdate)
			}
			constr.Definition = &definition
		}
	}

	// Clean up options map if empty
	if len(constr.Options) == 0 {
		constr.Options = nil
	}

	return constr
}

// parseIndexColumns extracts column names from index specifications
func (ct *CreateTable) parseIndexColumns(keys []*ast.IndexPartSpecification) []string {
	columns := make([]string, 0, len(keys))
	for _, key := range keys {
		if key.Column != nil {
			columns = append(columns, key.Column.Name.String())
		}
	}

	return columns
}

// parseIndexColumnList extracts full column specifications including prefix lengths and expressions
func (ct *CreateTable) parseIndexColumnList(keys []*ast.IndexPartSpecification) []IndexColumn {
	columns := make([]IndexColumn, 0, len(keys))
	for _, key := range keys {
		// Desc applies to both column and expression key parts,
		// e.g. KEY (a DESC) and KEY ((lower(b)) DESC).
		col := IndexColumn{Desc: key.Desc}

		// Check if this is a column reference or an expression
		if key.Column != nil {
			// Regular column reference
			col.Name = key.Column.Name.String()

			// Add prefix length if specified
			if key.Length > 0 {
				length := int(key.Length)
				col.Length = &length
			}
		} else if key.Expr != nil {
			// Expression index (functional index)
			if expr, ok := restoreExprText(key.Expr, format.DefaultRestoreFlags); ok {
				col.Expression = &expr
			}
		}

		columns = append(columns, col)
	}

	return columns
}

// parseTableOptions converts table options to a TableOptions struct
func (ct *CreateTable) parseTableOptions(options []*ast.TableOption) *TableOptions {
	tableOpts := &TableOptions{}
	hasOptions := false

	for _, option := range options {
		switch option.Tp {
		case ast.TableOptionEngine:
			if option.StrValue != "" {
				tableOpts.Engine = &option.StrValue
				hasOptions = true
			}
		case ast.TableOptionCharset:
			if option.StrValue != "" {
				tableOpts.Charset = &option.StrValue
				hasOptions = true
			}
		case ast.TableOptionCollate:
			if option.StrValue != "" {
				tableOpts.Collation = &option.StrValue
				hasOptions = true
			}
		case ast.TableOptionComment:
			if option.StrValue != "" {
				tableOpts.Comment = &option.StrValue
				hasOptions = true
			}
		case ast.TableOptionAutoIncrement:
			if option.UintValue > 0 {
				tableOpts.AutoIncrement = &option.UintValue
				hasOptions = true
			}
		case ast.TableOptionKeyBlockSize:
			if option.UintValue > 0 {
				tableOpts.KeyBlockSize = &option.UintValue
				hasOptions = true
			}
		case ast.TableOptionAutoextendSize:
			// The grammar yields the bare byte count in UintValue and a
			// suffixed size (4M) in StrValue. A suffixed value MySQL would
			// not accept is left unset: the CREATE TABLE itself is invalid.
			size := option.UintValue
			if option.StrValue != "" {
				parsed, err := utils.ParseSizeNumber(option.StrValue)
				if err != nil {
					break
				}
				size = parsed
			}
			if size > 0 {
				tableOpts.AutoextendSize = &size
				hasOptions = true
			}
		case ast.TableOptionStatsPersistent:
			if !option.Default {
				tableOpts.StatsPersistent = new(option.UintValue != 0)
				hasOptions = true
			}
		case ast.TableOptionStatsAutoRecalc:
			if !option.Default {
				tableOpts.StatsAutoRecalc = new(option.UintValue != 0)
				hasOptions = true
			}
		case ast.TableOptionStatsSamplePages:
			if !option.Default && option.UintValue > 0 {
				tableOpts.StatsSamplePages = &option.UintValue
				hasOptions = true
			}
		case ast.TableOptionPackKeys:
			if !option.Default {
				tableOpts.PackKeys = new(option.UintValue != 0)
				hasOptions = true
			}
		case ast.TableOptionCheckSum:
			if option.UintValue != 0 {
				tableOpts.Checksum = true
				hasOptions = true
			}
		case ast.TableOptionDelayKeyWrite:
			if option.UintValue != 0 {
				tableOpts.DelayKeyWrite = true
				hasOptions = true
			}
		case ast.TableOptionAvgRowLength:
			if option.UintValue > 0 {
				tableOpts.AvgRowLength = &option.UintValue
				hasOptions = true
			}
		case ast.TableOptionMinRows:
			if option.UintValue > 0 {
				tableOpts.MinRows = &option.UintValue
				hasOptions = true
			}
		case ast.TableOptionMaxRows:
			if option.UintValue > 0 {
				tableOpts.MaxRows = &option.UintValue
				hasOptions = true
			}
		case ast.TableOptionSecondaryEngineAttribute:
			if option.StrValue != "" {
				tableOpts.SecondaryEngineAttribute = &option.StrValue
				hasOptions = true
			}
		case ast.TableOptionRowFormat:
			// ROW_FORMAT=DEFAULT is the absence of a row format: MySQL stores
			// nothing for it, SHOW CREATE TABLE omits it, and the table takes
			// the engine default (innodb_default_row_format), so it is parsed
			// as no row format, the same as a definition that leaves the
			// option out.
			if option.UintValue > 0 && option.UintValue != ast.RowFormatDefault {
				var rowFormat string

				switch option.UintValue {
				case ast.RowFormatDynamic:
					rowFormat = "DYNAMIC"
				case ast.RowFormatFixed:
					rowFormat = "FIXED"
				case ast.RowFormatCompressed:
					rowFormat = "COMPRESSED"
				case ast.RowFormatRedundant:
					rowFormat = "REDUNDANT"
				case ast.RowFormatCompact:
					rowFormat = "COMPACT"
				default:
					rowFormat = fmt.Sprintf("UNKNOWN_%d", option.UintValue)
				}

				tableOpts.RowFormat = &rowFormat
				hasOptions = true
			}
		}
	}

	if !hasOptions {
		return nil
	}

	return tableOpts
}

// Helper methods for TableOptions to handle nil safely
func (to *TableOptions) getEngine() *string {
	if to == nil {
		return nil
	}
	return to.Engine
}

func (to *TableOptions) getCharset() *string {
	if to == nil {
		return nil
	}
	return to.Charset
}

func (to *TableOptions) getCollation() *string {
	if to == nil {
		return nil
	}
	return to.Collation
}

func (to *TableOptions) getComment() *string {
	if to == nil {
		return nil
	}
	return to.Comment
}

func (to *TableOptions) getRowFormat() *string {
	if to == nil {
		return nil
	}
	return to.RowFormat
}

// deref returns the options by value, or the zero value for a nil receiver
// (a table with no options at all), so callers can read the fields without
// a nil check per option.
func (to *TableOptions) deref() TableOptions {
	if to == nil {
		return TableOptions{}
	}
	return *to
}

func (to *TableOptions) getAutoIncrement() *string {
	if to == nil || to.AutoIncrement == nil {
		return nil
	}
	s := strconv.FormatUint(*to.AutoIncrement, 10)
	return &s
}

// parsePartitionOptions converts partition options to a PartitionOptions struct
func (ct *CreateTable) parsePartitionOptions(partition *ast.PartitionOptions) *PartitionOptions {
	if partition == nil {
		return nil
	}

	partOpts := &PartitionOptions{
		Linear:      partition.Linear,
		Partitions:  partition.Num,
		Definitions: make([]PartitionDefinition, 0, len(partition.Definitions)),
	}

	// Parse partition type
	switch partition.Tp {
	case ast.PartitionTypeRange:
		partOpts.Type = "RANGE"
	case ast.PartitionTypeHash:
		partOpts.Type = "HASH"
	case ast.PartitionTypeKey:
		partOpts.Type = "KEY"
	case ast.PartitionTypeList:
		partOpts.Type = "LIST"
	default:
		partOpts.Type = fmt.Sprintf("UNKNOWN_%d", partition.Tp)
	}

	if partition.KeyAlgorithm != nil {
		partOpts.KeyAlgorithm = partition.KeyAlgorithm.Type
	}

	// Parse expression for HASH and RANGE
	if partition.Expr != nil {
		// Restore the full expression using the AST
		if expr, ok := restoreExprText(partition.Expr, format.DefaultRestoreFlags); ok {
			partOpts.Expression = &expr
		}
	}

	// Parse column names for KEY, RANGE COLUMNS, LIST COLUMNS
	if len(partition.ColumnNames) > 0 {
		partOpts.Columns = make([]string, 0, len(partition.ColumnNames))
		for _, colName := range partition.ColumnNames {
			partOpts.Columns = append(partOpts.Columns, colName.Name.String())
		}
	}

	// Parse individual partition definitions
	for _, def := range partition.Definitions {
		partDef := ct.parsePartitionDefinition(def)
		partOpts.Definitions = append(partOpts.Definitions, partDef)
	}

	// Parse subpartitioning if present
	if partition.Sub != nil {
		partOpts.SubPartition = ct.parseSubPartitionOptions(partition.Sub)
	}

	return partOpts
}

// parsePartitionDefinition converts a partition definition to a PartitionDefinition struct
func (ct *CreateTable) parsePartitionDefinition(def *ast.PartitionDefinition) PartitionDefinition {
	partDef := PartitionDefinition{
		Name:          def.Name.String(),
		Options:       make(map[string]any),
		SubPartitions: make([]SubPartitionDefinition, 0, len(def.Sub)),
	}

	// Parse partition values clause
	if def.Clause != nil {
		partDef.Values = ct.parsePartitionClause(def.Clause)
	}

	// Parse partition options
	for _, opt := range def.Options {
		switch {
		case opt.Tp == ast.TableOptionComment:
			if opt.StrValue != "" {
				partDef.Comment = &opt.StrValue
			}
		case opt.Tp == ast.TableOptionEngine:
			if opt.StrValue != "" {
				partDef.Engine = &opt.StrValue
			}
		case parsePartitionStorageOption(opt, &partDef.PartitionStorage):
			// Stored by parsePartitionStorageOption.
		default:
			// Store other options in the options map
			partDef.Options[fmt.Sprintf("option_%d", opt.Tp)] = opt.StrValue
		}
	}

	// Parse subpartitions
	for _, sub := range def.Sub {
		subDef := ct.parseSubPartitionDefinition(sub)
		partDef.SubPartitions = append(partDef.SubPartitions, subDef)
	}

	// Clean up options map if empty
	if len(partDef.Options) == 0 {
		partDef.Options = nil
	}

	return partDef
}

// parsePartitionClause converts a partition clause to PartitionValues
func (ct *CreateTable) parsePartitionClause(clause ast.PartitionDefinitionClause) *PartitionValues {
	switch c := clause.(type) {
	case *ast.PartitionDefinitionClauseLessThan:
		values := &PartitionValues{
			Type:   "LESS_THAN",
			Values: make([]any, 0, len(c.Exprs)),
		}
		for _, expr := range c.Exprs {
			values.Values = append(values.Values, ct.parsePartitionValue(expr))
		}

		// Normalize the single-expression MAXVALUE form (VALUES LESS THAN
		// MAXVALUE, or its parenthesized spelling VALUES LESS THAN (MAXVALUE)
		// as printed by SHOW CREATE TABLE for RANGE COLUMNS) to the dedicated
		// "MAXVALUE" type so that emission produces the bare keyword. MySQL
		// accepts the bare form for both RANGE and single-column RANGE
		// COLUMNS, and both spellings parse to the same representation here,
		// so they always compare equal (verified against MySQL 8.0.45).
		if len(values.Values) == 1 {
			if _, isMax := values.Values[0].(partitionMaxValue); isMax {
				return &PartitionValues{Type: "MAXVALUE", Values: []any{}}
			}
		}

		return values
	case *ast.PartitionDefinitionClauseIn:
		values := &PartitionValues{
			Type:   "IN",
			Values: make([]any, 0, len(c.Values)),
		}
		for _, valList := range c.Values {
			if len(valList) == 1 {
				values.Values = append(values.Values, ct.parsePartitionValue(valList[0]))
			} else {
				// A multi-column LIST COLUMNS value: keep it as one tuple.
				tuple := make(partitionValueTuple, 0, len(valList))
				for _, expr := range valList {
					tuple = append(tuple, ct.parsePartitionValue(expr))
				}
				values.Values = append(values.Values, tuple)
			}
		}

		return values
	default:
		return nil
	}
}

// parsePartitionValue parses a single partition value expression. The
// MAXVALUE keyword becomes the partitionMaxValue sentinel so it is emitted
// bare (never as the string literal 'MAXVALUE', which MySQL rejects with
// error 1697), and NULL becomes partitionNullValue for the same reason.
// String literals (LIST/RANGE COLUMNS on a string column) are wrapped in
// partitionStringLiteral carrying their true raw value, so emission can
// quote them unconditionally. Numeric literals become their text as plain
// strings, and anything else (e.g. 10+10, TO_DAYS('2030-01-01')) becomes a
// partitionExprValue.
//
// Parentheses around a value carry no meaning, so they are dropped first:
// otherwise ('y') would be read as an expression rather than the string
// 'y', and (NULL) as something other than NULL.
func (ct *CreateTable) parsePartitionValue(expr ast.ExprNode) any {
	expr = unwrapParenExpr(expr)
	if _, isMax := expr.(*ast.MaxValueExpr); isMax {
		return partitionMaxValue{}
	}
	if v, ok := expr.(*ast.ValueExpr); ok && v.Kind() == ast.KindNull {
		return partitionNullValue{}
	}
	if literal, isStr := stringLiteralValue(expr); isStr {
		return partitionStringLiteral(literal)
	}
	if _, isLiteral := expr.(*ast.ValueExpr); isLiteral {
		return ct.parseExpression(expr)
	}
	if text, ok := restoreExpressionText(expr); ok {
		return partitionExprValue(text)
	}
	return ct.parseExpression(expr)
}

// parseSubPartitionOptions converts subpartition options to SubPartitionOptions
func (ct *CreateTable) parseSubPartitionOptions(sub *ast.PartitionMethod) *SubPartitionOptions {
	if sub == nil {
		return nil
	}

	subOpts := &SubPartitionOptions{
		Linear: sub.Linear,
		Count:  sub.Num,
	}
	if sub.KeyAlgorithm != nil {
		subOpts.KeyAlgorithm = sub.KeyAlgorithm.Type
	}

	// Parse subpartition type
	switch sub.Tp {
	case ast.PartitionTypeHash:
		subOpts.Type = "HASH"
	case ast.PartitionTypeKey:
		subOpts.Type = "KEY"
	default:
		subOpts.Type = fmt.Sprintf("UNKNOWN_%d", sub.Tp)
	}

	// Parse expression for HASH
	if sub.Expr != nil {
		if exprStr, ok := restoreExpressionText(sub.Expr); ok && exprStr != "" {
			subOpts.Expression = &exprStr
		}
	}

	// Parse column names for KEY
	if len(sub.ColumnNames) > 0 {
		subOpts.Columns = make([]string, 0, len(sub.ColumnNames))
		for _, colName := range sub.ColumnNames {
			subOpts.Columns = append(subOpts.Columns, colName.Name.String())
		}
	}

	return subOpts
}

// parseSubPartitionDefinition converts a subpartition definition to SubPartitionDefinition
func (ct *CreateTable) parseSubPartitionDefinition(sub *ast.SubPartitionDefinition) SubPartitionDefinition {
	subDef := SubPartitionDefinition{
		Name:    sub.Name.String(),
		Options: make(map[string]any),
	}

	// Parse subpartition options. An empty COMMENT is kept: it stops the
	// partition's comment from applying, and partitionOptionsNormalizer drops
	// it after that.
	for _, opt := range sub.Options {
		switch {
		case opt.Tp == ast.TableOptionComment:
			comment := opt.StrValue
			subDef.Comment = &comment
		case opt.Tp == ast.TableOptionEngine:
			if opt.StrValue != "" {
				subDef.Engine = &opt.StrValue
			}
		case parsePartitionStorageOption(opt, &subDef.PartitionStorage):
			// Stored by parsePartitionStorageOption.
		default:
			// Store other options in the options map
			subDef.Options[fmt.Sprintf("option_%d", opt.Tp)] = opt.StrValue
		}
	}

	// Clean up options map if empty
	if len(subDef.Options) == 0 {
		subDef.Options = nil
	}

	return subDef
}

// parseExpression converts an expression to a string representation, in the
// form MySQL reports for a literal-style DEFAULT / ON UPDATE / partition
// expression. See restoreValueExprText for the bare-keyword caveat.
func (ct *CreateTable) parseExpression(expr ast.ExprNode) any {
	return restoreValueExprText(expr, true)
}

// Diff compares this CreateTable (source) with another CreateTable (target)
// and returns ALTER TABLE statements needed to transform source into target.
// Most changes produce a single statement, but some require multiple
// sequential statements: a spatial index dropped before the primary ALTER
// changes its column's SRID; an option-only index rebuild, a foreign key
// added back under the name the primary ALTER drops, or a partition clause
// that cannot share an ALTER after it. See pkg/statement/README.md,
// "Statement Planning".
// Returns nil if the tables are identical.
// Returns an error if target has a primary key column that declares NULL, or
// changes primary key options MySQL would ignore in a combined DROP and ADD.
// If opts is nil, NewDiffOptions() defaults are used.
func (ct *CreateTable) Diff(target *CreateTable, opts *DiffOptions) ([]*AbstractStatement, error) {
	if opts == nil {
		opts = NewDiffOptions()
	}
	if ct.TableName != target.TableName {
		return nil, fmt.Errorf("cannot diff tables with different names: %s vs %s", ct.TableName, target.TableName)
	}
	if err := checkPrimaryKeyNullability(target); err != nil {
		return nil, fmt.Errorf("invalid target table %q: %w", target.TableName, err)
	}

	// A VIRTUAL generated column that the target makes a regular column is
	// converted in two statements so that it keeps its values: the first
	// rebuilds it as a STORED generated column, which MySQL fills from the
	// expression; the second is this diff from that intermediate table, in
	// which the column is a MODIFY (see virtualToRegularIntermediate).
	if intermediate := ct.virtualToRegularIntermediate(target); intermediate != nil {
		first, err := ct.Diff(intermediate, opts)
		if err != nil {
			return nil, err
		}
		rest, err := intermediate.Diff(target, opts)
		if err != nil {
			return nil, err
		}
		return append(first, rest...), nil
	}

	var alterClauses []string

	// The columns MySQL cannot MODIFY into their target definition are
	// dropped and added back instead, and the index and constraint diffs
	// re-add what reads them (see rebuiltColumns).
	rebuilt := ct.rebuiltColumns(target)

	// 1. Diff columns (DROP, ADD, MODIFY)
	columnClauses := ct.diffColumns(target, opts, rebuilt)
	alterClauses = append(alterClauses, columnClauses...)

	// 2. Diff indexes (DROP, ADD). An index whose options alone change (same
	// column list, different WITH PARSER / KEY_BLOCK_SIZE / etc.) is replaced
	// in statements of its own after the primary ALTER, because MySQL no-ops
	// a combined DROP+ADD of the same index in a single ALTER (see
	// diffIndexes). A spatial index on a column whose SRID changes
	// is dropped in a statement of its own before the primary ALTER, because
	// MySQL rejects the SRID change while the index exists, even when the
	// same ALTER drops it (error 3644); the target's index is added back in
	// the primary ALTER.
	var preStatements [][]string
	spatialDrops := ct.spatialIndexesBlockingSRIDChange(target)
	if len(spatialDrops) > 0 {
		drops := make([]string, 0, len(spatialDrops))
		for name := range spatialDrops {
			drops = append(drops, fmt.Sprintf("DROP INDEX %s", sqlescape.EscapeIdentifier(name)))
		}
		slices.Sort(drops)
		preStatements = append(preStatements, drops)
	}
	// A foreign key can create an implicit index in the primary ALTER.
	// Share reservations between both planners so later index swaps avoid
	// those indexes, including ones named after a replacement foreign key.
	takenNames := make(map[string]bool)
	for _, schema := range []*CreateTable{ct, target} {
		for _, idx := range schema.GetIndexes() {
			takenNames[strings.ToLower(idx.Name)] = true
		}
		for _, constraint := range schema.Constraints {
			takenNames[strings.ToLower(constraint.Name)] = true
			if constraint.Type == "FOREIGN KEY" && constraint.Name == "" && len(constraint.Columns) > 0 {
				takenNames[strings.ToLower(constraint.Columns[0])] = true
			}
		}
	}
	indexClauses, separateIndexStatements, err := ct.diffIndexes(target, rebuilt, spatialDrops, takenNames)
	if err != nil {
		return nil, err
	}
	alterClauses = append(alterClauses, indexClauses...)

	// 3. Diff constraints (DROP, ADD). A foreign key added back under a name
	// the same diff drops takes a replacement name, because MySQL rejects the
	// same-name DROP and ADD in one ALTER (error 1826).
	constraintClauses := ct.diffConstraints(target, rebuilt, takenNames)
	alterClauses = append(alterClauses, constraintClauses...)

	// 4. Diff table options
	tableOptionClauses := ct.diffTableOptions(target, opts)
	alterClauses = append(alterClauses, tableOptionClauses...)

	// 5. Diff partition options. MySQL's grammar puts a partition clause
	// after the alter list, separated by a space rather than a comma, and
	// some partition clauses can't share an ALTER with anything else. See
	// partitionDiff.
	var partitionClause string
	var additionalStatements [][]string
	if !opts.IgnorePartitioning {
		pd := ct.diffPartitionOptions(target)
		switch {
		case pd.standalone != "" && len(alterClauses) == 0:
			alterClauses = []string{pd.standalone}
		case pd.standalone != "" && pd.standaloneInplace && !ct.partitionKeyColumnsChanged(target, opts):
			// The cheap clause is metadata-only, so running it as its own
			// statement costs less than folding a repartition (a full table
			// copy) into the primary ALTER. It runs after the primary ALTER,
			// so that ALTER must not change a column the partitioning reads:
			// a converted value (e.g. a DECIMAL rounded up) could then fall
			// past the last existing partition before the new one is added.
			additionalStatements = append(additionalStatements, []string{pd.standalone})
		default:
			partitionClause = pd.repartition
		}
	}

	// Index replacements run as their own ALTER statements, after the
	// primary ALTER so they observe any column or table option change the
	// replacement depends on.
	additionalStatements = append(additionalStatements, separateIndexStatements...)

	// Build the result
	var results []*AbstractStatement

	// Statements the primary ALTER depends on (a spatial index drop)
	for _, clauses := range preStatements {
		stmt, err := ct.buildAlterStatement(clauses, "")
		if err != nil {
			return nil, err
		}
		results = append(results, stmt)
	}

	// Primary statement (columns, indexes, constraints, table options, and
	// any partition clause that can share an ALTER with them)
	if len(alterClauses) > 0 || partitionClause != "" {
		stmt, err := ct.buildAlterStatement(alterClauses, partitionClause)
		if err != nil {
			return nil, err
		}
		results = append(results, stmt)
	}

	// Additional statements (e.g. ADD PARTITION alongside a column change)
	for _, clauses := range additionalStatements {
		stmt, err := ct.buildAlterStatement(clauses, "")
		if err != nil {
			return nil, err
		}
		results = append(results, stmt)
	}

	if len(results) == 0 {
		return nil, nil
	}

	return results, nil
}

// buildAlterStatement constructs and parses an ALTER TABLE statement from
// clauses. A non-empty partitionClause (PARTITION BY or REMOVE PARTITIONING)
// is appended after the comma-separated clauses with a space, the only
// position MySQL accepts it in when there are other clauses.
func (ct *CreateTable) buildAlterStatement(clauses []string, partitionClause string) (*AbstractStatement, error) {
	alter := strings.Join(clauses, ", ")
	if partitionClause != "" {
		alter = strings.TrimSpace(alter + " " + partitionClause)
	}
	alterStmt := fmt.Sprintf("ALTER TABLE %s %s", sqlescape.EscapeIdentifier(ct.TableName), alter)

	p := parser.New()
	stmtNodes, _, err := p.Parse(alterStmt, "", "")
	if err != nil {
		return nil, fmt.Errorf("failed to parse generated ALTER statement: %w (SQL: %s)", err, alterStmt)
	}

	if len(stmtNodes) != 1 {
		return nil, fmt.Errorf("expected exactly one statement, got %d", len(stmtNodes))
	}

	return &AbstractStatement{
		Table:     ct.TableName,
		Alter:     alter,
		Statement: alterStmt,
		StmtNode:  &stmtNodes[0],
	}, nil
}

// rebuiltColumns returns the lowercased names of the columns, present in both
// tables, that the ALTER has to drop and add back rather than MODIFY. MySQL
// refuses to change a column to or from a VIRTUAL generated column in place
// (error 3106, "Changing the STORED status"), in every direction: VIRTUAL to
// STORED, STORED to VIRTUAL, VIRTUAL to a regular column and a regular column
// to VIRTUAL. Only the regular/STORED pair is a MODIFY. A generated column
// holds no data of its own, so dropping one loses nothing: MySQL recomputes
// it from the new expression when it adds the column. A VIRTUAL column that
// becomes a regular column is the exception, because the regular column has
// to keep the values the expression produced; Diff never asks this function
// about that transition directly but stages it through a STORED intermediate
// first (see virtualToRegularIntermediate), from which the regular column is
// a MODIFY that keeps the values.
//
// A generated column that reads a rebuilt column blocks its DROP (error 3108)
// and is rebuilt with it, transitively — unless the target makes it a regular
// column. Then it is left to the MODIFY, which MySQL accepts in the same
// ALTER as the DROP of the column it read, because the modified column no
// longer reads anything; the rebuild would have dropped a STORED column's
// values for nothing. The index and constraint diffs drop and re-add the
// functional indexes and CHECK constraints that read a rebuilt column (errors
// 3837 and 3959); an index that names the column as a plain key part survives
// the rebuild on its own. A foreign key on a rebuilt column is not handled:
// MySQL rejects the DROP (error 1828), and the diff lets that error surface
// rather than drop a referential constraint.
func (ct *CreateTable) rebuiltColumns(target *CreateTable) map[string]bool {
	targetColumns := make(map[string]*Column, len(target.Columns))
	for i := range target.Columns {
		targetColumns[strings.ToLower(target.Columns[i].Name)] = &target.Columns[i]
	}
	rebuilt := make(map[string]bool)
	for i := range ct.Columns {
		sourceCol := &ct.Columns[i]
		name := strings.ToLower(sourceCol.Name)
		if targetCol, ok := targetColumns[name]; ok && isVirtualGenerated(sourceCol) != isVirtualGenerated(targetCol) {
			rebuilt[name] = true
		}
	}
	if len(rebuilt) == 0 {
		return rebuilt
	}
	p := parser.New()
	for changed := true; changed; {
		changed = false
		for i := range ct.Columns {
			sourceCol := &ct.Columns[i]
			name := strings.ToLower(sourceCol.Name)
			if rebuilt[name] || sourceCol.GeneratedExpr == nil {
				continue
			}
			if targetCol, kept := targetColumns[name]; !kept || targetCol.GeneratedExpr == nil {
				continue // dropped anyway, or modified into a regular column
			}
			if expressionReadsAny(p, *sourceCol.GeneratedExpr, rebuilt) {
				rebuilt[name] = true
				changed = true
			}
		}
	}
	return rebuilt
}

// virtualToRegularIntermediate returns the table ct becomes once every VIRTUAL
// generated column that target declares as a regular column has been rebuilt
// as a STORED generated column with its source expression, or nil when there
// is no such column. Diff converts those columns through it in two statements.
//
// MySQL refuses the direct MODIFY (error 3106) and a DROP+ADD of the regular
// column leaves it NULL: a VIRTUAL column holds no data, so there is nothing
// for the added column to inherit, and the values the expression produced are
// lost (a row with c=40 under `g INT AS (c+1) VIRTUAL` reads g=41 before and
// NULL after). A STORED column is filled from the expression when it is added,
// and MySQL does keep the values of a STORED column that a MODIFY turns into a
// regular one. So the first statement rebuilds the column STORED, at its place
// and with whatever reads it (the ordinary rebuild, see rebuiltColumns), and
// the second MODIFYs it into the target definition, type and attribute changes
// included, alongside everything else the diff emits. The intermediate differs
// from ct only in the STORED keyword of those columns, so the first statement
// changes nothing else, and the expression still reads the source columns,
// which the second statement is free to drop or change.
func (ct *CreateTable) virtualToRegularIntermediate(target *CreateTable) *CreateTable {
	targetColumns := make(map[string]*Column, len(target.Columns))
	for i := range target.Columns {
		targetColumns[strings.ToLower(target.Columns[i].Name)] = &target.Columns[i]
	}
	var intermediate *CreateTable
	for i := range ct.Columns {
		sourceCol := &ct.Columns[i]
		targetCol, ok := targetColumns[strings.ToLower(sourceCol.Name)]
		if !ok || !isVirtualGenerated(sourceCol) || targetCol.GeneratedExpr != nil {
			continue
		}
		if intermediate == nil {
			copied := *ct
			copied.Columns = slices.Clone(ct.Columns)
			intermediate = &copied
		}
		intermediate.Columns[i].GeneratedStored = true
	}
	return intermediate
}

// isVirtualGenerated reports whether col is a VIRTUAL generated column.
func isVirtualGenerated(col *Column) bool {
	return col.GeneratedExpr != nil && !col.GeneratedStored
}

// diffColumns compares columns and returns ALTER clauses for differences.
// rebuilt names the columns that are dropped and added back instead of
// modified (see rebuiltColumns).
func (ct *CreateTable) diffColumns(target *CreateTable, opts *DiffOptions, rebuilt map[string]bool) []string {
	var clauses []string

	// Build maps for easier lookup. Keys are lowercased so identifier
	// matching is case-insensitive — MySQL treats column names that
	// differ only in case as the same column.
	sourceColumns := make(map[string]*Column)
	for i := range ct.Columns {
		sourceColumns[strings.ToLower(ct.Columns[i].Name)] = &ct.Columns[i]
	}

	targetColumns := make(map[string]*Column)
	for i := range target.Columns {
		targetColumns[strings.ToLower(target.Columns[i].Name)] = &target.Columns[i]
	}

	// Collect DROP operations and sort by name for deterministic output. A
	// rebuilt column is dropped here and leaves the source map, so the target
	// walk below adds it back as it would a new column, position included.
	var dropClauses []string
	for _, sourceCol := range ct.Columns {
		name := strings.ToLower(sourceCol.Name)
		if _, exists := targetColumns[name]; !exists || rebuilt[name] {
			dropClauses = append(dropClauses, fmt.Sprintf("DROP COLUMN %s", sqlescape.EscapeIdentifier(sourceCol.Name)))
		}
		if rebuilt[name] {
			delete(sourceColumns, name)
		}
	}
	slices.Sort(dropClauses)
	clauses = append(clauses, dropClauses...)

	// Determine which columns need explicit positioning: every new column,
	// and every existing column that is not where the target wants it once
	// the clauses before it have been applied (see calculateColumnPositioning).
	needsExplicitPosition := ct.calculateColumnPositioning(target, sourceColumns, targetColumns)

	// Whether this ALTER sets the table default to the server's utf8mb4
	// default collation; see modifiedColumn.
	resetsTableDefault := !opts.IgnoreCharsetCollation && resetsToServerUTF8MB4Default(ct, target)

	// Generate the ALTER clauses in target order
	var prevColumn string
	defaults := alterDefaults(ct, target, opts)
	for i, targetCol := range target.Columns {
		sourceCol, existsInSource := sourceColumns[strings.ToLower(targetCol.Name)]
		// Compare and write an enum or set column with the members the ALTER
		// stores, which depend on the table default it runs under.
		targetCol = withMembersUnder(targetCol, defaults)

		if !existsInSource {
			// ADD new column
			clause := fmt.Sprintf("ADD COLUMN %s", formatColumnDefinition(&targetCol))
			// Add positioning - only if not at the end
			isLastColumn := i == len(target.Columns)-1
			if prevColumn == "" {
				clause += " FIRST"
			} else if !isLastColumn {
				clause += fmt.Sprintf(" AFTER %s", sqlescape.EscapeIdentifier(prevColumn))
			}
			// If it's the last column, omit AFTER clause (implicit)
			clauses = append(clauses, clause)
		} else {
			// MODIFY existing column if:
			// 1. Column definition changed
			// 2. Column needs explicit positioning
			// needsExplicitPosition is keyed by lowercased column name, so
			// look up with the same normalization to avoid missing a
			// position-only change when the target's spelling is in mixed
			// or upper case.
			//
			// A column leaving the primary key gets no special case: adding a
			// PRIMARY KEY implicitly makes its columns NOT NULL, but DROP
			// PRIMARY KEY never reverts that, so a former PK column the target
			// declares nullable needs its own MODIFY.
			needsModify := !ct.columnsEqualWithContext(sourceCol, &targetCol, target, opts) ||
				needsExplicitPosition[strings.ToLower(targetCol.Name)]

			if needsModify {
				definition := withChangedCollationNamed(sourceCol, &targetCol, ct, target, opts)
				clause := fmt.Sprintf("MODIFY COLUMN %s", formatColumnDefinition(modifiedColumn(definition, resetsTableDefault)))
				if needsExplicitPosition[strings.ToLower(targetCol.Name)] {
					if prevColumn == "" {
						clause += " FIRST"
					} else {
						clause += fmt.Sprintf(" AFTER %s", sqlescape.EscapeIdentifier(prevColumn))
					}
				}
				clauses = append(clauses, clause)
			}
		}

		prevColumn = targetCol.Name
	}

	return clauses
}

// modifiedColumn returns the definition a MODIFY COLUMN renders for col. When
// the same ALTER sets the table default to DEFAULT CHARSET=utf8mb4 without a
// COLLATE (resetsTableDefault), a column that inherits it is rendered with
// CHARACTER SET utf8mb4. MySQL resolves that to default_collation_for_utf8mb4,
// as it does the table option, but resolves a MODIFY that names no charset in
// that ALTER to utf8mb4_0900_ai_ci. On a server whose variable is
// utf8mb4_general_ci, the column would then differ from the table it was
// declared to inherit from, and a re-diff would emit a second MODIFY.
func modifiedColumn(col *Column, resetsTableDefault bool) *Column {
	if !resetsTableDefault || col.Charset != nil || col.Collation != nil || !charsetCarryingTypes[strings.ToLower(col.Type)] {
		return col
	}
	withCharset := *col
	withCharset.Charset = new(charset.CharsetUTF8MB4)
	return &withCharset
}

// withChangedCollationNamed returns the definition a MODIFY COLUMN renders for
// col with its collation written out when col inherits its table's default and
// the MODIFY moves it onto a different collation than source has. The charset
// is written too when it changes as well. MySQL resolves an inheriting MODIFY
// against the table default the same ALTER sets (see alterDefaults), so naming
// that default stores the same column. Without it, a MODIFY that changes the
// collation of every row reads like a restatement of the live column, because
// the live form of the old collation is the only place it appears. A collation
// that cannot be determined from the statement alone (see
// resolvedCharsetCollation) is left unwritten, as is every column when
// IgnoreCharsetCollation keeps the table default out of the ALTER.
func withChangedCollationNamed(source, col *Column, sourceTable, targetTable *CreateTable, opts *DiffOptions) *Column {
	if opts.IgnoreCharsetCollation || col.Charset != nil || col.Collation != nil || !charsetCarryingTypes[strings.ToLower(col.Type)] {
		return col
	}
	targetCharset, targetCollation := resolvedCharsetCollation(col, targetTable)
	if targetCollation == "" {
		return col
	}
	sourceCharset, sourceCollation := resolvedCharsetCollation(source, sourceTable)
	if sourceCollation == targetCollation {
		return col
	}
	named := *col
	named.Collation = &targetCollation
	if sourceCharset != targetCharset {
		named.Charset = &targetCharset
	}
	return &named
}

// calculateColumnPositioning decides which target columns need an explicit
// FIRST/AFTER clause. It returns the set of their names, lowercased to match
// the source/target column maps built by the caller (MySQL column identifiers
// are case-insensitive). sourceColumns holds the source columns the ALTER
// keeps: a rebuilt column (see rebuiltColumns) is absent from it, and is
// simulated as the DROP followed by the ADD the caller emits for it.
//
// It simulates what MySQL does with the clauses diffColumns emits. MySQL first
// removes the dropped columns and replaces the definitions of the modified
// ones in place, then processes the positioned clauses (ADD/MODIFY with FIRST
// or AFTER) one at a time in statement order, each one taking the column out
// of wherever it currently is and re-inserting it at the named place. Walking
// the target in order against that evolving list, a column already at its
// target position needs no clause; one that is not is moved there, which the
// caller renders as FIRST or AFTER the preceding target column. The invariant
// is that after step i the first i+1 columns of the simulated list are the
// first i+1 target columns, so every later AFTER names a column that is
// already where it belongs.
//
// Comparing each column's predecessor between source and target, the previous
// approach, did not follow the clauses through: dropping a column counted as
// an "implicit" move for its successor, so `(id, a, b, c, d)` to `(id, d, b)`
// emitted no position for d and left the live table as `(id, b, d)`.
func (ct *CreateTable) calculateColumnPositioning(target *CreateTable, sourceColumns, targetColumns map[string]*Column) map[string]bool {
	needsExplicitPosition := make(map[string]bool)

	// The surviving source columns in source order: the list as it stands
	// once the DROP COLUMN clauses have taken effect.
	current := make([]string, 0, len(target.Columns))
	for _, col := range ct.Columns {
		name := strings.ToLower(col.Name)
		if _, kept := targetColumns[name]; !kept {
			continue
		}
		if _, kept := sourceColumns[name]; !kept {
			continue // rebuilt: dropped, then added by the target walk
		}
		current = append(current, name)
	}

	for pos, targetCol := range target.Columns {
		name := strings.ToLower(targetCol.Name)
		if _, existsInSource := sourceColumns[name]; !existsInSource {
			// ADD COLUMN takes its place in the list; at the end the caller
			// omits the AFTER clause, which appends, and the simulation here
			// is the same either way.
			current = slices.Insert(current, pos, name)
			needsExplicitPosition[name] = true
			continue
		}
		if pos < len(current) && current[pos] == name {
			continue
		}
		// Out of place: MySQL takes it out and re-inserts it after the
		// preceding target column, which by the invariant is at pos-1.
		idx := slices.Index(current, name)
		current = slices.Delete(current, idx, idx+1)
		current = slices.Insert(current, pos, name)
		needsExplicitPosition[name] = true
	}

	return needsExplicitPosition
}

// pairInlineUniqueNames reconciles the names of unique indexes that one side
// declared inline (`c INT UNIQUE`). indexNormalizer names such an index after
// its column, which is only a guess at the name the server assigned: the live
// table may call it c_2, or whatever an earlier definition left behind. A
// unique index on the same column set whose name differs, when at least one
// side's name is a guess, is the same index, so the guessed side takes the
// other side's name and the two then meet in diffIndexes' name-keyed walk,
// where their options and visibility are compared like any other pair.
//
// The pair used to be left out of the diff altogether, which hid an option
// difference: a live `UNIQUE KEY c_2 (c) COMMENT 'x'` matched an inline
// `c INT UNIQUE` with nothing emitted, so a declaration could never clear a
// comment or INVISIBLE from the live index. Two explicitly named unique
// indexes that differ only in name are not paired: that is a real rename
// (DROP + ADD). When only one side's name is a guess it takes the explicit
// one; when both are, the target takes the source's, so a live name is never
// changed. The lists are modified in place.
func pairInlineUniqueNames(sourceIdxList, targetIdxList []Index) {
	sourceNames := make(map[string]bool, len(sourceIdxList))
	for i := range sourceIdxList {
		sourceNames[sourceIdxList[i].Name] = true
	}
	targetNames := make(map[string]bool, len(targetIdxList))
	for i := range targetIdxList {
		targetNames[targetIdxList[i].Name] = true
	}
	paired := make(map[int]bool) // target positions already paired
	for i := range sourceIdxList {
		sourceIdx := &sourceIdxList[i]
		if sourceIdx.Type != "UNIQUE" || targetNames[sourceIdx.Name] {
			continue // not unique, or already met by name
		}
		for j := range targetIdxList {
			targetIdx := &targetIdxList[j]
			if targetIdx.Type != "UNIQUE" || sourceNames[targetIdx.Name] || paired[j] {
				continue
			}
			if !sourceIdx.InlineDerived && !targetIdx.InlineDerived {
				continue // both explicitly named: a genuine rename
			}
			if !indexColumnsIdenticalIgnoreName(sourceIdx, targetIdx) {
				continue
			}
			paired[j] = true
			if targetIdx.InlineDerived {
				targetIdx.Name = sourceIdx.Name
			} else {
				sourceIdx.Name = targetIdx.Name
			}
			break
		}
	}
}

// spatialIndexesBlockingSRIDChange returns the names of the source's spatial
// indexes on a column whose SRID attribute the target changes: added, removed
// or different. MySQL refuses to change a column's SRID while a spatial index
// is on it (error 3644), even when the same ALTER drops the index, so Diff
// drops them in a statement of its own before the primary ALTER and passes
// them to diffIndexes as already gone, which adds the target's index on the
// column, if any, back in the primary ALTER. Any other change to such a
// column, and an SRID change on a column with no spatial index, is a plain
// MODIFY.
func (ct *CreateTable) spatialIndexesBlockingSRIDChange(target *CreateTable) map[string]bool {
	targetColumns := make(map[string]*Column, len(target.Columns))
	for i := range target.Columns {
		targetColumns[strings.ToLower(target.Columns[i].Name)] = &target.Columns[i]
	}
	sridChanged := make(map[string]bool)
	for i := range ct.Columns {
		sourceCol := &ct.Columns[i]
		name := strings.ToLower(sourceCol.Name)
		if targetCol, ok := targetColumns[name]; ok && !ptrEqual(sourceCol.SRID, targetCol.SRID) {
			sridChanged[name] = true
		}
	}
	if len(sridChanged) == 0 {
		return nil
	}
	blocking := make(map[string]bool)
	for i := range ct.Indexes {
		idx := &ct.Indexes[i]
		if idx.Type != "SPATIAL" {
			continue
		}
		for _, part := range idx.ColumnList {
			if sridChanged[strings.ToLower(part.Name)] {
				blocking[idx.Name] = true
			}
		}
	}
	return blocking
}

// diffIndexes compares indexes and returns ALTER clauses for differences.
//
// Most index changes are emitted into the combined ALTER (the returned
// []string). An index whose column list is identical but whose options differ
// (WITH PARSER, KEY_BLOCK_SIZE, SECONDARY_ENGINE_ATTRIBUTE) cannot be changed
// by a combined `DROP INDEX x, ADD INDEX x (<same cols>)` in a single ALTER:
// MySQL pairs the two clauses up and keeps the existing index, silently
// ignoring the option change. Such an index is swapped for a replacement in
// statements of its own (see swap below), returned via the second value as
// standalone clause-lists (each becomes its own ALTER statement) that run
// after the combined ALTER.
//
// rebuilt names the columns dropped and added back by diffColumns (see
// rebuiltColumns). A functional index that reads one blocks the DROP COLUMN
// (error 3837) unless the same ALTER drops it, so it is dropped and added back
// from the target's definition in the combined ALTER. An index that names the
// column as a plain key part survives the rebuild on its own.
//
// droppedBefore names the source indexes a statement before this ALTER has
// already dropped (see spatialIndexesBlockingSRIDChange). They are diffed as
// absent from the source: a same-named target index is a plain ADD.
func (ct *CreateTable) diffIndexes(target *CreateTable, rebuilt, droppedBefore, takenNames map[string]bool) (clauses []string, separateStatements [][]string, err error) {
	// Inline column-level UNIQUE / PRIMARY KEY have already been materialized
	// into ct.Indexes by normalization (see indexNormalizer, primaryKeyNormalizer),
	// so both index sets can be walked directly. The lists are copied because
	// pairInlineUniqueNames renames entries, and the caller's tables must not
	// change under a diff.
	sourceIdxList := slices.DeleteFunc(slices.Clone(ct.Indexes), func(idx Index) bool {
		return droppedBefore[idx.Name]
	})
	targetIdxList := slices.Clone(target.Indexes)
	pairInlineUniqueNames(sourceIdxList, targetIdxList)

	var p *parser.Parser
	readsRebuilt := func(idx *Index) bool {
		if len(rebuilt) == 0 {
			return false
		}
		if p == nil {
			p = parser.New()
		}
		for _, part := range idx.ColumnList {
			if part.Expression != nil && expressionReadsAny(p, *part.Expression, rebuilt) {
				return true
			}
		}
		return false
	}
	// rebuiltIndexes are the source indexes dropped in the primary ALTER to
	// be added back below from the target's definition, whatever it is:
	// those reading a rebuilt column.
	rebuiltIndexes := make(map[string]bool)

	// Build maps for easier lookup
	sourceIndexes := make(map[string]*Index)
	for i := range sourceIdxList {
		sourceIndexes[sourceIdxList[i].Name] = &sourceIdxList[i]
	}

	targetIndexes := make(map[string]*Index)
	for i := range targetIdxList {
		targetIndexes[targetIdxList[i].Name] = &targetIdxList[i]
	}

	// Collect DROP operations and sort by name for deterministic output
	var dropClauses []string

	// The primary key is a table-level index on both sides after normalization
	// (see primaryKeyNormalizer), so its add/drop/change falls out of the normal
	// index diff below — no inline-PK special cases are needed. pkDropAdded just
	// guards against emitting "DROP PRIMARY KEY" twice from the source loop.
	pkDropAdded := false

	// replaced tracks the indexes re-created outside the primary ALTER:
	// those whose column list is unchanged but whose options differ (WITH
	// PARSER, KEY_BLOCK_SIZE, SECONDARY_ENGINE_ATTRIBUTE). MySQL pairs a
	// same-name, same-columns DROP+ADD in one ALTER and keeps the old index,
	// so each is re-created by swap instead; they are routed out of
	// dropClauses, the additions and the visibility clauses below.
	replaced := make(map[string]bool)

	// swap re-creates an index as the target defines it, after the primary
	// ALTER: the replacement is added under a temporary name and the old
	// index dropped in the same statement, and a final statement renames
	// every replacement back. Every swapped index shares one statement, so
	// the table is rebuilt once for all of them, except a FULLTEXT index,
	// which takes a statement of its own (InnoDB builds one FULLTEXT index
	// per ALTER, error 1795). Adding before dropping is what gets the swap
	// through where a standalone DROP INDEX is refused: the only index on an
	// AUTO_INCREMENT column (error 1075) or the index a foreign key depends
	// on (error 1553). The temporary name avoids every reserved index and
	// constraint name, compared case-insensitively as MySQL does. A run that stops
	// between the statements leaves the index under the temporary name; the
	// next diff drops that one and adds the target's.
	var swapClauses []string     // every swap but a FULLTEXT one: one statement
	var fulltextSwaps [][]string // one statement per FULLTEXT swap
	var renames []string
	swap := func(sourceIdx, targetIdx *Index) {
		replaced[sourceIdx.Name] = true
		tmp := replacementName(sourceIdx.Name, takenNames)
		replacement := *targetIdx
		replacement.Name = tmp
		pair := []string{
			formatAddIndex(&replacement),
			fmt.Sprintf("DROP INDEX %s", sqlescape.EscapeIdentifier(sourceIdx.Name)),
		}
		if targetIdx.Type == "FULLTEXT" {
			fulltextSwaps = append(fulltextSwaps, pair)
		} else {
			swapClauses = append(swapClauses, pair...)
		}
		renames = append(renames, fmt.Sprintf("RENAME INDEX %s TO %s",
			sqlescape.EscapeIdentifier(tmp), sqlescape.EscapeIdentifier(sourceIdx.Name)))
	}

	for i := range sourceIdxList {
		sourceIdx := &sourceIdxList[i]
		targetIdx, existsInTarget := targetIndexes[sourceIdx.Name]

		switch {
		case !existsInTarget:
			// Index removed completely
			if sourceIdx.Type == "PRIMARY KEY" {
				if !pkDropAdded {
					dropClauses = append(dropClauses, "DROP PRIMARY KEY")
					pkDropAdded = true
				}
			} else {
				dropClauses = append(dropClauses, fmt.Sprintf("DROP INDEX %s", sqlescape.EscapeIdentifier(sourceIdx.Name)))
			}
		case readsRebuilt(sourceIdx):
			rebuiltIndexes[sourceIdx.Name] = true
			dropClauses = append(dropClauses, fmt.Sprintf("DROP INDEX %s", sqlescape.EscapeIdentifier(sourceIdx.Name)))
		case !indexesEqual(sourceIdx, targetIdx) && !indexesEqualIgnoreVisibility(sourceIdx, targetIdx):
			// Index exists but changed (and not just visibility) - need to drop and re-add
			switch {
			case sourceIdx.Type == "PRIMARY KEY":
				// MySQL keeps these options when the same columns are dropped
				// and added together. A primary key cannot use the temporary
				// name swap used for secondary indexes, so fail before returning
				// any statements instead of planning a perpetual no-op.
				if indexNeedsSeparateRebuild(sourceIdx, targetIdx) {
					return nil, nil, fmt.Errorf("cannot diff table %q: changing PRIMARY KEY options without changing its columns is unsupported (KEY_BLOCK_SIZE, WITH PARSER, or SECONDARY_ENGINE_ATTRIBUTE)", ct.TableName)
				}
				// Only add if not already added above
				if !pkDropAdded {
					dropClauses = append(dropClauses, "DROP PRIMARY KEY")
					pkDropAdded = true
				}
			case indexNeedsSeparateRebuild(sourceIdx, targetIdx):
				// A no-op-prone option (WITH PARSER / KEY_BLOCK_SIZE /
				// SECONDARY_ENGINE_ATTRIBUTE) changed on an unchanged column
				// list. A combined DROP+ADD in one ALTER would be a MySQL
				// no-op, so the index is swapped for a replacement.
				swap(sourceIdx, targetIdx)
			default:
				dropClauses = append(dropClauses, fmt.Sprintf("DROP INDEX %s", sqlescape.EscapeIdentifier(sourceIdx.Name)))
			}
		}
	}
	slices.Sort(dropClauses)
	clauses = append(clauses, dropClauses...)
	if len(swapClauses) > 0 {
		separateStatements = append(separateStatements, swapClauses)
	}
	separateStatements = append(separateStatements, fulltextSwaps...)
	if len(renames) > 0 {
		separateStatements = append(separateStatements, renames)
	}

	// Collect ADD operations and sort by clause text for deterministic output
	type addition struct {
		clause   string
		fulltext bool
	}
	var additions []addition
	add := func(idx *Index) {
		additions = append(additions, addition{clause: formatAddIndex(idx), fulltext: idx.Type == "FULLTEXT"})
	}
	for i := range targetIdxList {
		targetIdx := &targetIdxList[i]
		sourceIdx, existsInSource := sourceIndexes[targetIdx.Name]

		switch {
		case !existsInSource:
			// New index - add it
			add(targetIdx)
		case rebuiltIndexes[targetIdx.Name]:
			// Dropped above for a column rebuild; add it back as the target
			// defines it.
			add(targetIdx)
		case !indexesEqual(sourceIdx, targetIdx):
			// Index exists but changed - check if only visibility changed
			if indexesEqualIgnoreVisibility(sourceIdx, targetIdx) {
				// Only visibility changed - skip for now, handle in ALTER INDEX section
				continue
			}
			// A replaced index is re-created by its swap statements above.
			if replaced[targetIdx.Name] {
				continue
			}
			// Other changes - need to drop and re-add (drop already handled above)
			add(targetIdx)
		}
	}
	slices.SortFunc(additions, func(a, b addition) int { return strings.Compare(a.clause, b.clause) })
	// InnoDB builds one FULLTEXT index per ALTER TABLE (error 1795, "InnoDB
	// presently supports one FULLTEXT index creation at a time"), however the
	// statement is otherwise shaped. The first FULLTEXT add stays in the
	// combined ALTER; each further one runs as a statement of its own after
	// it.
	fulltextAdded := false
	for _, a := range additions {
		if a.fulltext && fulltextAdded {
			separateStatements = append(separateStatements, []string{a.clause})
			continue
		}
		fulltextAdded = fulltextAdded || a.fulltext
		clauses = append(clauses, a.clause)
	}

	// Collect ALTER INDEX operations for visibility changes (must come after DROP/ADD)
	var alterClauses []string
	for _, targetIdx := range targetIdxList {
		sourceIdx, existsInSource := sourceIndexes[targetIdx.Name]

		if existsInSource && !rebuiltIndexes[targetIdx.Name] && !replaced[targetIdx.Name] &&
			!indexesEqual(sourceIdx, &targetIdx) && indexesEqualIgnoreVisibility(sourceIdx, &targetIdx) {
			// Only visibility changed
			targetVisible := targetIdx.Invisible == nil || !*targetIdx.Invisible
			if targetVisible {
				alterClauses = append(alterClauses, fmt.Sprintf("ALTER INDEX %s VISIBLE", sqlescape.EscapeIdentifier(targetIdx.Name)))
			} else {
				alterClauses = append(alterClauses, fmt.Sprintf("ALTER INDEX %s INVISIBLE", sqlescape.EscapeIdentifier(targetIdx.Name)))
			}
		}
	}
	slices.Sort(alterClauses)
	clauses = append(clauses, alterClauses...)

	return clauses, separateStatements, nil
}

// replacementName names the replacement of an index swap (see diffIndexes),
// which is renamed back afterwards, or of a foreign key re-created under its
// own name (see diffConstraints), which keeps it: the object's own name
// wrapped as _<name>_new, like the shadow table, cut to MySQL's 64-character
// identifier limit and numbered until it is free. taken holds the lowercased
// names in use (MySQL compares them case-insensitively), and the chosen name
// is added to it.
func replacementName(name string, taken map[string]bool) string {
	const maxLen = mysql.MaxIndexIdentifierLen
	for n := 1; ; n++ {
		suffix := "_new"
		if n > 1 {
			suffix += strconv.Itoa(n)
		}
		base := []rune(name)
		if room := maxLen - 1 - len(suffix); len(base) > room {
			base = base[:room]
		}
		candidate := "_" + string(base) + suffix
		if key := strings.ToLower(candidate); !taken[key] {
			taken[key] = true
			return candidate
		}
	}
}

// diffConstraints compares constraints and returns the ALTER clauses for the
// differences, plus the clauses that cannot share the primary ALTER, each
// inner slice one statement to run after it.
//
// rebuilt names the columns dropped and added back by diffColumns (see
// rebuiltColumns). A CHECK constraint that reads one blocks the DROP COLUMN
// (error 3959) unless the same ALTER drops it, so the source's is dropped and
// the target's added back in the combined ALTER, each decided on its own text
// and outside the pairing below, which would otherwise find the pair equal and
// emit nothing. MySQL accepts the DROP CHECK and the ADD CONSTRAINT under the
// same name in one statement.
//
// It does not accept that for a foreign key: a DROP FOREIGN KEY and an ADD
// CONSTRAINT under the same name in one ALTER fail with error 1826, "Duplicate
// foreign key constraint name", and foreign key names are case-insensitive
// (and unique per schema), so a name differing only in case fails the same
// way. Such a foreign key is re-created under a replacement name instead: the
// ALTER drops the old constraint and adds the new definition as _<name>_new
// (see replacementName) in the same statement, so the table is never without
// it. Dropping in one statement and re-adding in the next would leave the
// child table unconstrained in between: a row with no parent inserted then is
// accepted, and the re-add fails on it. The foreign key keeps the replacement
// name. Moving it to its own name afterwards would be a second ADD FOREIGN
// KEY, a table copy when foreign_key_checks is on, for a name the diff does
// not compare: a foreign key is paired by definition whatever its name
// (constraintsEqualIgnoreName, below), so the next diff finds the table
// converged, and a later change under the desired name fits one ALTER under
// the two names and brings the name back. The replacement name avoids every
// constraint and index name on either side of the diff (MySQL names the index
// it creates for a foreign key after the constraint when the columns have
// none); a collision with a foreign key of another table in the schema fails
// the ALTER with error 1826, and nothing has changed.
func (ct *CreateTable) diffConstraints(target *CreateTable, rebuilt, takenNames map[string]bool) (clauses []string) {

	var p *parser.Parser
	readsRebuilt := func(c *Constraint) bool {
		if len(rebuilt) == 0 || c.Type != "CHECK" || c.Expression == nil {
			return false
		}
		if p == nil {
			p = parser.New()
		}
		return expressionReadsAny(p, *c.Expression, rebuilt)
	}
	// droppedForRebuild are the source CHECKs dropped for a column rebuild;
	// a same-named target constraint is added back whatever its text.
	droppedForRebuild := make(map[string]bool)
	// droppedForeignKeys are the lowercased names of the foreign keys this
	// diff drops; a target foreign key under one of them is added under a
	// replacement name (see above).
	droppedForeignKeys := make(map[string]bool)

	// Build maps for easier lookup
	sourceConstraints := make(map[string]*Constraint)
	for i := range ct.Constraints {
		sourceConstraints[ct.Constraints[i].Name] = &ct.Constraints[i]
	}

	targetConstraints := make(map[string]*Constraint)
	for i := range target.Constraints {
		targetConstraints[target.Constraints[i].Name] = &target.Constraints[i]
	}

	// Build a set of source constraint names that have an equivalent match in the
	// target under a different name. This handles the case where MySQL generates
	// different auto-names for CHECK constraints whose original expression text
	// differs only cosmetically (e.g. charset introducers like _utf8mb3 that are
	// stripped during parsing). Without this, such constraints would produce a
	// spurious DROP + ADD.
	matchedSourceByExpression := make(map[string]bool) // source name -> matched
	matchedTargetByExpression := make(map[string]bool) // target name -> matched
	for i := range ct.Constraints {
		sourceConstr := &ct.Constraints[i]
		if _, exactMatch := targetConstraints[sourceConstr.Name]; exactMatch {
			continue // will be handled by the normal name-based path
		}
		if readsRebuilt(sourceConstr) {
			continue // dropped and re-added for the column rebuild
		}
		// No exact name match — look for an expression-equivalent target constraint
		for j := range target.Constraints {
			targetConstr := &target.Constraints[j]
			if _, exactMatch := sourceConstraints[targetConstr.Name]; exactMatch {
				continue // this target constraint already has a name match in source
			}
			if matchedTargetByExpression[targetConstr.Name] || readsRebuilt(targetConstr) {
				continue // already paired with another source constraint, or re-added for a rebuild
			}
			if constraintsEqualIgnoreName(sourceConstr, targetConstr) {
				matchedSourceByExpression[sourceConstr.Name] = true
				matchedTargetByExpression[targetConstr.Name] = true
				break
			}
		}
	}

	// Collect DROP operations and sort by name for deterministic output.
	// CHECK constraints that differ only in enforcement are not dropped;
	// they are flipped in place with ALTER CHECK (collected separately).
	var dropClauses []string
	var enforcementClauses []string
	for i := range ct.Constraints {
		sourceConstr := &ct.Constraints[i]
		if matchedSourceByExpression[sourceConstr.Name] {
			continue // equivalent constraint exists in target under a different name
		}
		targetConstr, exists := targetConstraints[sourceConstr.Name]
		rebuild := readsRebuilt(sourceConstr)
		if rebuild {
			droppedForRebuild[sourceConstr.Name] = true
		}

		// Drop if constraint doesn't exist in target OR if it changed
		if rebuild || !exists || !constraintsEqual(sourceConstr, targetConstr) {
			if !rebuild && exists && constraintsEqualExceptEnforcement(sourceConstr, targetConstr) {
				// Only the [NOT] ENFORCED state changed: use MySQL's targeted
				// ALTER CHECK clause instead of DROP+ADD. Flipping to NOT
				// ENFORCED is then metadata-only (INSTANT-capable); flipping
				// to ENFORCED validates existing rows either way, exactly as
				// an enforced re-ADD would.
				if targetConstr.NotEnforced {
					enforcementClauses = append(enforcementClauses, fmt.Sprintf("ALTER CHECK %s NOT ENFORCED", sqlescape.EscapeIdentifier(sourceConstr.Name)))
				} else {
					enforcementClauses = append(enforcementClauses, fmt.Sprintf("ALTER CHECK %s ENFORCED", sqlescape.EscapeIdentifier(sourceConstr.Name)))
				}
				continue
			}
			switch sourceConstr.Type {
			case "FOREIGN KEY":
				dropClauses = append(dropClauses, fmt.Sprintf("DROP FOREIGN KEY %s", sqlescape.EscapeIdentifier(sourceConstr.Name)))
				droppedForeignKeys[strings.ToLower(sourceConstr.Name)] = true
			case "CHECK":
				dropClauses = append(dropClauses, fmt.Sprintf("DROP CHECK %s", sqlescape.EscapeIdentifier(sourceConstr.Name)))
			}
		}
	}
	slices.Sort(dropClauses)
	clauses = append(clauses, dropClauses...)
	slices.Sort(enforcementClauses)
	clauses = append(clauses, enforcementClauses...)

	// Collect ADD operations and sort by name for deterministic output. A
	// foreign key added back under a name this ALTER drops is added under a
	// replacement name (error 1826, see above).
	var addClauses []string
	for _, targetConstr := range target.Constraints {
		if matchedTargetByExpression[targetConstr.Name] {
			continue // equivalent constraint exists in source under a different name
		}
		sourceConstr, existsInSource := sourceConstraints[targetConstr.Name]
		rebuild := droppedForRebuild[targetConstr.Name] || readsRebuilt(&targetConstr)

		if rebuild || !existsInSource || !constraintsEqual(sourceConstr, &targetConstr) {
			if !rebuild && existsInSource && constraintsEqualExceptEnforcement(sourceConstr, &targetConstr) {
				continue // enforcement-only change; handled by ALTER CHECK above
			}
			if targetConstr.Type == "FOREIGN KEY" && droppedForeignKeys[strings.ToLower(targetConstr.Name)] {
				replacement := targetConstr
				replacement.Name = replacementName(targetConstr.Name, takenNames)
				addClauses = append(addClauses, formatAddConstraint(&replacement))
				continue
			}
			addClauses = append(addClauses, formatAddConstraint(&targetConstr))
		}
	}
	slices.Sort(addClauses)
	clauses = append(clauses, addClauses...)

	return clauses
}

// diffTableOptions compares table options and returns ALTER clauses for differences.
// The opts parameter controls which table options are compared.
func (ct *CreateTable) diffTableOptions(target *CreateTable, opts *DiffOptions) []string {
	var clauses []string

	// Compare ENGINE
	if !opts.IgnoreEngine {
		if !ptrEqual(ct.TableOptions.getEngine(), target.TableOptions.getEngine()) {
			if engine := target.TableOptions.getEngine(); engine != nil {
				clauses = append(clauses, fmt.Sprintf("ENGINE=%s", *engine))
			}
		}
	}

	// Compare CHARSET and COLLATION
	if !opts.IgnoreCharsetCollation {
		// A DEFAULT CHARSET=utf8mb4 without a COLLATE also differs from a
		// utf8mb4 table on a collation default_collation_for_utf8mb4 cannot
		// hold, such as utf8mb4_bin. The clause is emitted alone: MySQL
		// resolves it to that variable's value, which is what CREATE TABLE
		// would give the declared table, while naming either collation would
		// only be right on some servers.
		if !ptrEqual(ct.TableOptions.getCharset(), target.TableOptions.getCharset()) ||
			resetsToServerUTF8MB4Default(ct, target) {
			if charset := target.TableOptions.getCharset(); charset != nil {
				clauses = append(clauses, fmt.Sprintf("DEFAULT CHARSET=%s", *charset))
			}
		}

		if !ptrEqual(ct.TableOptions.getCollation(), target.TableOptions.getCollation()) {
			if collation := target.TableOptions.getCollation(); collation != nil {
				clauses = append(clauses, fmt.Sprintf("COLLATE=%s", *collation))
			}
		}
	}

	// Compare COMMENT (always compared — not controlled by an ignore option)
	if !ptrEqual(ct.TableOptions.getComment(), target.TableOptions.getComment()) {
		if comment := target.TableOptions.getComment(); comment != nil {
			clauses = append(clauses, fmt.Sprintf("COMMENT='%s'", sqlescape.EscapeString(*comment)))
		} else {
			// The source has a comment but the target does not: emit an
			// explicit empty comment to clear it. Without this clause the
			// difference would be detected but silently dropped.
			clauses = append(clauses, "COMMENT=''")
		}
	}

	source, dest := ct.TableOptions.deref(), target.TableOptions.deref()

	// optionClause appends name=<target value> when the target sets the
	// option, or name=<reset> when only the source does. ALTER TABLE has no
	// way to leave a table option out, so each one is cleared by the value
	// MySQL reads back as unset.
	optionClause := func(name string, sourceValue, targetValue *string, reset string) {
		if ptrEqual(sourceValue, targetValue) {
			return
		}
		if targetValue != nil {
			clauses = append(clauses, name+"="+*targetValue)
		} else {
			clauses = append(clauses, name+"="+reset)
		}
	}

	// Compare ROW_FORMAT and, with it, the table-level KEY_BLOCK_SIZE: the
	// compressed page size that implies ROW_FORMAT=COMPRESSED. The two have
	// to move together — InnoDB rejects an ALTER to another row format while
	// a KEY_BLOCK_SIZE is set, so the clearing KEY_BLOCK_SIZE=0 goes in the
	// same statement as the ROW_FORMAT.
	if !opts.IgnoreRowFormat {
		if !ptrEqual(ct.TableOptions.getRowFormat(), target.TableOptions.getRowFormat()) {
			if rowFormat := target.TableOptions.getRowFormat(); rowFormat != nil {
				clauses = append(clauses, fmt.Sprintf("ROW_FORMAT=%s", *rowFormat))
			} else {
				// The target names no row format, so the table it describes
				// has the engine default, which is what ROW_FORMAT=DEFAULT
				// sets: it clears the stored option, and SHOW CREATE TABLE
				// then omits it like the target does. Without it a table
				// stuck on ROW_FORMAT=COMPACT could never be brought back.
				clauses = append(clauses, "ROW_FORMAT=DEFAULT")
			}
		}
		optionClause("KEY_BLOCK_SIZE", uintText(source.KeyBlockSize), uintText(dest.KeyBlockSize), "0")
	}

	// Compare AUTO_INCREMENT
	// Note: AUTO_INCREMENT is ignored by default (like vitess schemadiff)
	if !opts.IgnoreAutoIncrement {
		if !ptrEqual(ct.TableOptions.getAutoIncrement(), target.TableOptions.getAutoIncrement()) {
			if autoInc := target.TableOptions.getAutoIncrement(); autoInc != nil {
				clauses = append(clauses, fmt.Sprintf("AUTO_INCREMENT=%s", *autoInc))
			}
		}
	}

	// The remaining options are always compared. They are declared schema
	// (SHOW CREATE TABLE reports each one that is set), and before they were
	// modelled a declared STATS_PERSISTENT=0 or SECONDARY_ENGINE_ATTRIBUTE
	// was silently never applied. Emitted in the order SHOW CREATE TABLE
	// reports them.
	optionClause("MIN_ROWS", uintText(source.MinRows), uintText(dest.MinRows), "0")
	optionClause("MAX_ROWS", uintText(source.MaxRows), uintText(dest.MaxRows), "0")
	optionClause("AVG_ROW_LENGTH", uintText(source.AvgRowLength), uintText(dest.AvgRowLength), "0")
	optionClause("PACK_KEYS", boolText(source.PackKeys), boolText(dest.PackKeys), "DEFAULT")
	optionClause("STATS_PERSISTENT", boolText(source.StatsPersistent), boolText(dest.StatsPersistent), "DEFAULT")
	optionClause("STATS_AUTO_RECALC", boolText(source.StatsAutoRecalc), boolText(dest.StatsAutoRecalc), "DEFAULT")
	optionClause("STATS_SAMPLE_PAGES", uintText(source.StatsSamplePages), uintText(dest.StatsSamplePages), "DEFAULT")
	optionClause("CHECKSUM", flagText(source.Checksum), flagText(dest.Checksum), "0")
	optionClause("DELAY_KEY_WRITE", flagText(source.DelayKeyWrite), flagText(dest.DelayKeyWrite), "0")
	optionClause("AUTOEXTEND_SIZE", uintText(source.AutoextendSize), uintText(dest.AutoextendSize), "0")
	if !engineAttributeEqual(source.SecondaryEngineAttribute, dest.SecondaryEngineAttribute) {
		attr := ""
		if dest.SecondaryEngineAttribute != nil {
			attr = sqlescape.EscapeString(*dest.SecondaryEngineAttribute)
		}
		clauses = append(clauses, "SECONDARY_ENGINE_ATTRIBUTE='"+attr+"'")
	}

	return clauses
}

// uintText, boolText and flagText render an optional table option value as
// the text its ALTER clause carries, or nil when the option is unset, so that
// every option diffs through the same optionClause path in diffTableOptions.
func uintText(v *uint64) *string {
	if v == nil {
		return nil
	}
	return new(strconv.FormatUint(*v, 10))
}

func boolText(v *bool) *string {
	if v == nil {
		return nil
	}
	if *v {
		return new("1")
	}
	return new("0")
}

func flagText(v bool) *string {
	if !v {
		return nil
	}
	return new("1")
}

// columnsEqualWithContext checks if two columns are equal, considering table
// context for charset/collation: a column with no explicit charset/collation
// inherits its owning table's defaults, so those attributes are compared on
// their resolved values (see charsetCollationEqual).
func (ct *CreateTable) columnsEqualWithContext(a, b *Column, target *CreateTable, opts *DiffOptions) bool {
	// Column names are case-insensitive in MySQL, so `id` and `ID` refer
	// to the same column.
	if !strings.EqualFold(a.Name, b.Name) {
		return false
	}
	if equal, handled := textLengthTypeEqual(a, b, ct, target); handled {
		if !equal {
			return false
		}
	} else {
		if a.Type != b.Type {
			return false
		}
		if !ptrEqual(a.Length, b.Length) {
			return false
		}
	}
	if !ptrEqual(a.Precision, b.Precision) {
		return false
	}
	if !ptrEqual(a.Scale, b.Scale) {
		return false
	}
	if !ptrEqual(a.Unsigned, b.Unsigned) {
		return false
	}
	if !ptrEqual(a.Zerofill, b.Zerofill) {
		return false
	}
	// A nullability difference is normally a real change. With
	// IgnoreNotNullRelaxation it is forgiven in exactly one direction: `a` is
	// NOT NULL and `b` permits NULL, so the only thing the MODIFY would do is
	// weaken the column. The opposite direction — a difference the MODIFY
	// would need to *tighten* — stays a real change.
	//
	// `a` belongs to the receiver and `b` to the diffed-to table, which for a
	// DiffCreateTables caller means `a` is the schema under validation and `b`
	// the reference: this forgives a validated schema that is stricter, which
	// is the direction the option documents.
	if a.Nullable != b.Nullable {
		forgivenRelaxation := opts.IgnoreNotNullRelaxation && !a.Nullable && b.Nullable
		if !forgivenRelaxation {
			return false
		}
	}
	// Normalize default values for nullable columns:
	// For nullable columns, nil and the NULL *keyword* are semantically
	// equivalent. User might write `VARCHAR(255) NULL` but MySQL outputs
	// `VARCHAR(255) DEFAULT NULL`. A quoted string literal 'NULL'
	// (DefaultKindString) is NOT the keyword and must not collapse — it is a
	// real default that differs from no-default.
	//
	// Each side is normalized on its OWN nullability. Until
	// IgnoreNotNullRelaxation existed the two were always equal here (the check
	// above returned early otherwise), so this is the same normalization as
	// before for every other comparison. It matters for a forgiven relaxation:
	// there the nullable side renders `DEFAULT NULL` while the NOT NULL side
	// carries no default, and gating on one side's nullability alone would
	// report that as a default difference — reintroducing the very MODIFY the
	// option exists to suppress.
	sourceDefault := a.Default
	targetDefault := b.Default
	if a.Nullable && sourceDefault != nil && *sourceDefault == "NULL" && a.DefaultKind != DefaultKindString {
		sourceDefault = nil
	}
	if b.Nullable && targetDefault != nil && *targetDefault == "NULL" && b.DefaultKind != DefaultKindString {
		targetDefault = nil
	}
	if !ptrEqual(sourceDefault, targetDefault) {
		return false
	}
	if a.DefaultIsExpr != b.DefaultIsExpr {
		return false
	}
	if !columnExtendedAttributesEqual(a, b) {
		return false
	}
	// On a string column a quoted string literal default ('TRUE') is a
	// different value than the same text as a keyword/number default (TRUE),
	// so the literal form is part of column identity. On a numeric column it is
	// not: MySQL always renders the default quoted, so `DEFAULT 0` (bare) and
	// `DEFAULT '0'` (the SHOW CREATE TABLE form) are the same default and must
	// compare equal — the value itself is already compared above.
	//
	// Two spellings of one default reach here already folded to the same kind
	// (see the normalization rules), so a kind difference at this point is a
	// difference in the default itself.
	if a.DefaultKind != b.DefaultKind && !isNumericColumnType(a.Type) {
		return false
	}
	if !opts.IgnoreColumnAutoIncrement && a.AutoInc != b.AutoInc {
		return false
	}
	if a.PrimaryKey != b.PrimaryKey {
		return false
	}
	// Unique is intentionally NOT compared: a column-level UNIQUE is
	// representation, not state. MySQL canonicalizes `c int UNIQUE` into a
	// table-level UNIQUE KEY (that is what SHOW CREATE TABLE reports), and a
	// MODIFY COLUMN cannot express uniqueness anyway (formatColumnDefinition
	// never emits UNIQUE). Inline uniques are materialized into table-level
	// indexes by normalization — see indexNormalizer / diffIndexes.
	if !ptrEqual(a.Comment, b.Comment) {
		return false
	}

	if !charsetCollationEqual(a, b, ct, target, opts) {
		return false
	}

	if !slices.Equal(a.EnumValues, b.EnumValues) {
		return false
	}
	if !slices.Equal(a.SetValues, b.SetValues) {
		return false
	}
	return true
}

// partitionKeyColumnsChanged reports whether any column the target's
// partitioning reads (its COLUMNS list, or the columns in its expression)
// differs between ct and target. A generated column counts as reading the
// columns its expression reads, transitively: RANGE (g) with g AS (FLOOR(d))
// moves rows between partitions when d changes type, even though g's own
// definition is unchanged. It returns true when the columns can't be
// determined, so callers fall back to the conservative path.
func (ct *CreateTable) partitionKeyColumnsChanged(target *CreateTable, opts *DiffOptions) bool {
	if target.Partition == nil {
		return false
	}
	p := parser.New()
	pending := slices.Clone(target.Partition.Columns)
	if target.Partition.Expression != nil {
		var ok bool
		pending, ok = expressionColumnNames(p, *target.Partition.Expression)
		if !ok {
			return true
		}
	}
	// KEY () reads the primary key, and an expression without a column is
	// not valid partitioning: neither names its columns here.
	if len(pending) == 0 {
		return true
	}
	seen := make(map[string]bool)
	for len(pending) > 0 {
		name := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if seen[strings.ToLower(name)] {
			continue
		}
		seen[strings.ToLower(name)] = true
		sourceCol, targetCol := findColumn(ct.Columns, name), findColumn(target.Columns, name)
		if sourceCol == nil || targetCol == nil || !ct.columnsEqualWithContext(sourceCol, targetCol, target, opts) {
			return true
		}
		for _, col := range []*Column{sourceCol, targetCol} {
			if col.GeneratedExpr == nil {
				continue
			}
			deps, ok := expressionColumnNames(p, *col.GeneratedExpr)
			if !ok {
				return true
			}
			pending = append(pending, deps...)
		}
	}
	return false
}

// findColumn returns the column named name (case-insensitively, as MySQL
// compares column names), or nil.
func findColumn(cols Columns, name string) *Column {
	for i := range cols {
		if strings.EqualFold(cols[i].Name, name) {
			return &cols[i]
		}
	}
	return nil
}

// partitionDiff is the partition change needed to move a table from its
// current partitioning to the target's.
//
// MySQL's ALTER TABLE grammar has two kinds of partition clause:
//   - PARTITION BY and REMOVE PARTITIONING can follow other alter clauses,
//     but only as the last clause and separated by a space, not a comma.
//     PARTITION BY also works on an already-partitioned table, replacing its
//     partitioning (including its type) in one copy.
//   - ADD PARTITION, COALESCE PARTITION and REORGANIZE PARTITION are
//     standalone: they cannot share an ALTER with any other alter clause.
//     When other clauses change too, the repartition is folded into their
//     ALTER instead, unless the standalone clause is metadata-only.
type partitionDiff struct {
	// repartition is the general clause for the change: PARTITION BY ... or
	// REMOVE PARTITIONING. Empty when partitioning is unchanged.
	repartition string
	// standalone is a cheaper clause for the same change, used when it can
	// run on its own. Empty when there is none.
	standalone string
	// standaloneInplace is set when standalone is metadata-only (appending
	// RANGE/LIST partitions), so it is worth a separate statement even when
	// other clauses change too.
	standaloneInplace bool
}

// diffPartitionOptions compares partition options and returns the change
// needed to make the source's partitioning match the target's.
func (ct *CreateTable) diffPartitionOptions(target *CreateTable) partitionDiff {
	sourcePartition := ct.Partition
	targetPartition := target.Partition

	switch {
	case sourcePartition == nil && targetPartition == nil:
		return partitionDiff{}
	case targetPartition == nil:
		return partitionDiff{repartition: "REMOVE PARTITIONING"}
	case partitionOptionsEqual(sourcePartition, targetPartition):
		return partitionDiff{}
	}

	pd := partitionDiff{repartition: formatPartitionOptions(targetPartition)}
	if sourcePartition == nil {
		return pd
	}

	// HASH/KEY partitions where only the count changed (no explicit
	// definitions): ADD PARTITION / COALESCE PARTITION. Both redistribute
	// every row, so they are no cheaper than a repartition once other
	// clauses already force a copy.
	if isCountOnly, countDiff := isPartitionCountOnlyChange(sourcePartition, targetPartition); isCountOnly {
		if countDiff > 0 {
			pd.standalone = fmt.Sprintf("ADD PARTITION PARTITIONS %d", countDiff)
		} else {
			pd.standalone = fmt.Sprintf("COALESCE PARTITION %d", -countDiff)
		}
		return pd
	}

	// RANGE/LIST partitions appended after the existing ones: ADD PARTITION
	// is in-place and metadata-only.
	if added := appendedPartitions(sourcePartition, targetPartition); len(added) > 0 {
		defs := make([]string, 0, len(added))
		for i := range added {
			defs = append(defs, formatPartitionDefinition(&added[i]))
		}
		pd.standalone = fmt.Sprintf("ADD PARTITION (%s)", strings.Join(defs, ", "))
		pd.standaloneInplace = true
		return pd
	}

	// A contiguous run of RANGE/LIST partitions split, merged or redefined
	// (e.g. splitting a new month out of a MAXVALUE partition): REORGANIZE
	// PARTITION. MySQL only rewrites the reorganized partitions, but spirit
	// still copies the whole table: REORGANIZE rejects LOCK=NONE.
	if names, into := reorganizedPartitions(sourcePartition, targetPartition); len(names) > 0 {
		defs := make([]string, 0, len(into))
		for i := range into {
			defs = append(defs, formatPartitionDefinition(&into[i]))
		}
		pd.standalone = fmt.Sprintf("REORGANIZE PARTITION %s INTO (%s)",
			sqlescape.EscapeIdentifierList(names), strings.Join(defs, ", "))
		return pd
	}

	// Any other change (partition type, expression, dropped trailing
	// partitions, subpartitioning) is a repartition. DROP PARTITION is never
	// used: it deletes the partition's rows, where a repartition fails loudly
	// (error 1526) if a row no longer has a partition to live in.
	return pd
}
