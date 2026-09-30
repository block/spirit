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
	Raw             *ast.ColumnDef    `json:"-"`
	Name            string            `json:"name"`
	Type            string            `json:"type"`
	Length          *int              `json:"length,omitempty"` // nil = no width; 0 is a real width (varchar(0))
	Precision       *int              `json:"precision,omitempty"`
	Scale           *int              `json:"scale,omitempty"`
	Unsigned        *bool             `json:"unsigned,omitempty"`
	Zerofill        *bool             `json:"zerofill,omitempty"`    // ZEROFILL display attribute (implies unsigned)
	EnumValues      []string          `json:"enum_values,omitempty"` // Permitted values for ENUM type
	SetValues       []string          `json:"set_values,omitempty"`  // Permitted values for SET type
	Nullable        bool              `json:"nullable"`
	Default         *string           `json:"default,omitempty"`
	DefaultIsExpr   bool              `json:"default_is_expr,omitempty"`  // true when default is an expression (needs parens), e.g. DEFAULT (json_object())
	DefaultKind     DefaultKind       `json:"default_kind,omitempty"`     // the literal form the default was written as, read off the AST — see DefaultKind
	OnUpdate        *string           `json:"on_update,omitempty"`        // ON UPDATE expression for TIMESTAMP/DATETIME, e.g. "current_timestamp"
	GeneratedExpr   *string           `json:"generated_expr,omitempty"`   // Expression for GENERATED ALWAYS AS (...) columns
	GeneratedStored bool              `json:"generated_stored,omitempty"` // true = STORED, false = VIRTUAL (only meaningful when GeneratedExpr is set)
	Check           *string           `json:"check,omitempty"`            // Column-level CHECK (...) constraint expression
	SRID            *uint32           `json:"srid,omitempty"`             // SRID attribute for spatial columns
	AutoInc         bool              `json:"auto_increment"`
	PrimaryKey      bool              `json:"primary_key"`
	Unique          bool              `json:"unique"`
	Comment         *string           `json:"comment,omitempty"`
	Charset         *string           `json:"charset,omitempty"`
	Collation       *string           `json:"collation,omitempty"`
	Options         map[string]string `json:"options,omitempty"`
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
	Raw          *ast.Constraint   `json:"-"`
	Name         string            `json:"name"`
	Type         string            `json:"type"`                  // PRIMARY KEY, UNIQUE, INDEX, FULLTEXT, SPATIAL
	Columns      []string          `json:"columns"`               // Deprecated: use ColumnList for full details
	ColumnList   []IndexColumn     `json:"column_list,omitempty"` // Full column specifications including prefix/expression
	Invisible    *bool             `json:"invisible,omitempty"`
	Using        *string           `json:"using,omitempty"` // BTREE, HASH, RTREE
	Comment      *string           `json:"comment,omitempty"`
	KeyBlockSize *uint64           `json:"key_block_size,omitempty"`
	ParserName   *string           `json:"parser_name,omitempty"`
	Options      map[string]string `json:"options,omitempty"`

	// InlineDerived marks a UNIQUE index that indexNormalizer synthesized
	// from an inline column-level UNIQUE (`c INT UNIQUE`). Its name is only a
	// guess at the server-assigned one (the column name, suffixed on collision),
	// so diffIndexes pairs it with an equivalent live unique index by column set
	// even when the names differ, rather than emitting a spurious DROP+ADD.
	// Not serialized: it is a diff-time hint, not part of the logical schema.
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

// ForeignKeyReference represents a foreign key reference
type ForeignKeyReference struct {
	Table    string   `json:"table"`
	Columns  []string `json:"columns"`
	OnDelete *string  `json:"on_delete,omitempty"`
	OnUpdate *string  `json:"on_update,omitempty"`
}

// TableOptions represents table-level options
type TableOptions struct {
	Engine        *string `json:"engine,omitempty"`
	Charset       *string `json:"charset,omitempty"`
	Collation     *string `json:"collation,omitempty"`
	Comment       *string `json:"comment,omitempty"`
	AutoIncrement *uint64 `json:"auto_increment,omitempty"`
	RowFormat     *string `json:"row_format,omitempty"`
}

// PartitionOptions represents table partitioning configuration
type PartitionOptions struct {
	Type         string                `json:"type"`                   // RANGE, LIST, HASH, KEY
	Expression   *string               `json:"expression,omitempty"`   // For HASH and RANGE
	Columns      []string              `json:"columns,omitempty"`      // For KEY, RANGE COLUMNS, LIST COLUMNS
	Linear       bool                  `json:"linear,omitempty"`       // For LINEAR HASH/KEY
	Partitions   uint64                `json:"partitions,omitempty"`   // Number of partitions
	Definitions  []PartitionDefinition `json:"definitions,omitempty"`  // Individual partition definitions
	SubPartition *SubPartitionOptions  `json:"subpartition,omitempty"` // Subpartitioning options
}

// PartitionDefinition represents a single partition definition
type PartitionDefinition struct {
	Name          string                   `json:"name"`
	Values        *PartitionValues         `json:"values,omitempty"` // VALUES LESS THAN or VALUES IN
	Comment       *string                  `json:"comment,omitempty"`
	Engine        *string                  `json:"engine,omitempty"`
	Options       map[string]any           `json:"options,omitempty"`
	SubPartitions []SubPartitionDefinition `json:"subpartitions,omitempty"`
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

// SubPartitionOptions represents subpartitioning configuration
type SubPartitionOptions struct {
	Type       string   `json:"type"`                 // HASH, KEY
	Expression *string  `json:"expression,omitempty"` // For HASH
	Columns    []string `json:"columns,omitempty"`    // For KEY
	Linear     bool     `json:"linear,omitempty"`     // For LINEAR HASH/KEY
	Count      uint64   `json:"count,omitempty"`      // Number of subpartitions
}

// SubPartitionDefinition represents a single subpartition definition
type SubPartitionDefinition struct {
	Name    string         `json:"name"`
	Comment *string        `json:"comment,omitempty"`
	Engine  *string        `json:"engine,omitempty"`
	Options map[string]any `json:"options,omitempty"`
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
			// Column-level CHECK (expr). Note that MySQL normalizes these to
			// table-level constraints in SHOW CREATE TABLE output, so this is
			// only seen when parsing user-written (non-canonical) statements.
			if opt.Expr != nil {
				if exprStr, ok := restoreExpressionText(opt.Expr); ok {
					column.Check = &exprStr
				}
			}
		case ast.ColumnOptionSrid:
			// SRID n — spatial reference system id for spatial columns.
			// SHOW CREATE TABLE emits this as /*!80003 SRID n */ which the
			// parser unwraps as a regular column option.
			srid := opt.Srid
			column.SRID = &srid
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
				constr.References.Table,
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
			var sb strings.Builder
			rCtx := format.NewRestoreCtx(format.DefaultRestoreFlags|format.RestoreStringWithoutCharset, &sb)
			if err := key.Expr.Restore(rCtx); err == nil {
				expr := sb.String()
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
		case ast.TableOptionRowFormat:
			if option.UintValue > 0 {
				var rowFormat string

				switch option.UintValue {
				case 1: // RowFormatDefault
					rowFormat = "DEFAULT"
				case 2: // RowFormatDynamic
					rowFormat = "DYNAMIC"
				case 3: // RowFormatFixed
					rowFormat = "FIXED"
				case 4: // RowFormatCompressed
					rowFormat = "COMPRESSED"
				case 5: // RowFormatRedundant
					rowFormat = "REDUNDANT"
				case 6: // RowFormatCompact
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

	// Parse expression for HASH and RANGE
	if partition.Expr != nil {
		// Restore the full expression using the AST
		var sb strings.Builder
		rCtx := format.NewRestoreCtx(format.DefaultRestoreFlags|format.RestoreStringWithoutCharset, &sb)
		if err := partition.Expr.Restore(rCtx); err == nil {
			expr := sb.String()
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
		switch opt.Tp {
		case ast.TableOptionComment:
			if opt.StrValue != "" {
				partDef.Comment = &opt.StrValue
			}
		case ast.TableOptionEngine:
			if opt.StrValue != "" {
				partDef.Engine = &opt.StrValue
			}
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
				// Multiple values in a single clause
				subValues := make([]any, 0, len(valList))
				for _, expr := range valList {
					subValues = append(subValues, ct.parsePartitionValue(expr))
				}

				values.Values = append(values.Values, subValues...)
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
// error 1697). String literals (LIST/RANGE COLUMNS on a string column) are
// wrapped in partitionStringLiteral carrying their true raw value, so
// emission can quote them unconditionally. Numeric literals and expressions
// (e.g. YEAR(col)) fall back to the Restored text form as plain strings.
func (ct *CreateTable) parsePartitionValue(expr ast.ExprNode) any {
	if _, isMax := expr.(*ast.MaxValueExpr); isMax {
		return partitionMaxValue{}
	}
	if literal, isStr := stringLiteralValue(expr); isStr {
		return partitionStringLiteral(literal)
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
		expr := ct.parseExpression(sub.Expr)
		if exprStr, ok := expr.(string); ok && exprStr != "" {
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

	// Parse subpartition options
	for _, opt := range sub.Options {
		switch opt.Tp {
		case ast.TableOptionComment:
			if opt.StrValue != "" {
				subDef.Comment = &opt.StrValue
			}
		case ast.TableOptionEngine:
			if opt.StrValue != "" {
				subDef.Engine = &opt.StrValue
			}
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
// Most changes produce a single statement, but some (e.g. changing partition type)
// require multiple sequential statements.
// Returns nil if the tables are identical.
// Returns an error if target has a primary key column that declares NULL, a
// table MySQL refuses to create.
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

	var alterClauses []string

	// 1. Diff columns (DROP, ADD, MODIFY)
	columnClauses := ct.diffColumns(target, opts)
	alterClauses = append(alterClauses, columnClauses...)

	// 2. Diff indexes (DROP, ADD). Option-only index changes (same column
	// list, different WITH PARSER / KEY_BLOCK_SIZE / etc.) are returned as
	// separate statements because MySQL no-ops a combined DROP+ADD of the same
	// index in a single ALTER.
	indexClauses, separateIndexStatements := ct.diffIndexes(target)
	alterClauses = append(alterClauses, indexClauses...)

	// 3. Diff constraints (DROP, ADD)
	constraintClauses := ct.diffConstraints(target)
	alterClauses = append(alterClauses, constraintClauses...)

	// 4. Diff table options
	tableOptionClauses := ct.diffTableOptions(target, opts)
	alterClauses = append(alterClauses, tableOptionClauses...)

	// 5. Diff partition options — may produce additional statements
	var additionalStatements [][]string
	if !opts.IgnorePartitioning {
		partitionClauses, extraStatements := ct.diffPartitionOptions(target)
		alterClauses = append(alterClauses, partitionClauses...)
		additionalStatements = extraStatements
	}

	// Option-only index changes run as their own ALTER statements, after the
	// primary ALTER so they observe any column changes the re-add depends on.
	additionalStatements = append(additionalStatements, separateIndexStatements...)

	// Build the result
	var results []*AbstractStatement

	// Primary statement (columns, indexes, constraints, table options, and simple partition changes)
	if len(alterClauses) > 0 {
		stmt, err := ct.buildAlterStatement(alterClauses)
		if err != nil {
			return nil, err
		}
		results = append(results, stmt)
	}

	// Additional statements (e.g. second ALTER for partition type changes)
	for _, clauses := range additionalStatements {
		stmt, err := ct.buildAlterStatement(clauses)
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

// buildAlterStatement constructs and parses an ALTER TABLE statement from clauses.
func (ct *CreateTable) buildAlterStatement(clauses []string) (*AbstractStatement, error) {
	alter := strings.Join(clauses, ", ")
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

// diffColumns compares columns and returns ALTER clauses for differences
func (ct *CreateTable) diffColumns(target *CreateTable, opts *DiffOptions) []string {
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

	// Collect DROP operations and sort by name for deterministic output
	var dropClauses []string
	for _, sourceCol := range ct.Columns {
		if _, exists := targetColumns[strings.ToLower(sourceCol.Name)]; !exists {
			dropClauses = append(dropClauses, fmt.Sprintf("DROP COLUMN %s", sqlescape.EscapeIdentifier(sourceCol.Name)))
		}
	}
	slices.Sort(dropClauses)
	clauses = append(clauses, dropClauses...)

	// Determine which columns need explicit positioning
	// A column needs explicit positioning if:
	// 1. It's a new column (ADD) - always needs position
	// 2. Its previous column changed (explicit reorder)
	needsExplicitPosition := ct.calculateColumnPositioning(target, sourceColumns, targetColumns)

	// Whether this ALTER sets the table default to the server's utf8mb4
	// default collation; see modifiedColumn.
	resetsTableDefault := !opts.IgnoreCharsetCollation && resetsToServerUTF8MB4Default(ct, target)

	// Generate the ALTER clauses in target order
	var prevColumn string
	for i, targetCol := range target.Columns {
		sourceCol, existsInSource := sourceColumns[strings.ToLower(targetCol.Name)]

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
				clause := fmt.Sprintf("MODIFY COLUMN %s", formatColumnDefinition(modifiedColumn(&targetCol, resetsTableDefault)))
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

// calculateColumnPositioning determines which columns need explicit positioning (FIRST/AFTER).
// Returns a map of column names (lowercased) that need explicit positioning.
// Map keys are lowercased to match the source/target column maps built by
// the caller, since column identifiers in MySQL are case-insensitive.
func (ct *CreateTable) calculateColumnPositioning(target *CreateTable, sourceColumns, targetColumns map[string]*Column) map[string]bool {
	needsExplicitPosition := make(map[string]bool)

	var prevColumn string
	for _, targetCol := range target.Columns {
		_, existsInSource := sourceColumns[strings.ToLower(targetCol.Name)]

		if !existsInSource {
			// New columns always need explicit positioning
			needsExplicitPosition[strings.ToLower(targetCol.Name)] = true
		} else {
			// Existing column - check if its position changed
			sourcePrevCol := getPreviousColumn(ct.Columns, targetCol.Name)

			// Check if this is an implicit or explicit position change
			_, prevColExistedInSource := sourceColumns[strings.ToLower(prevColumn)]
			_, sourcePrevColStillExists := targetColumns[strings.ToLower(sourcePrevCol)]

			implicitChange := false
			switch {
			case prevColumn == "" && sourcePrevCol == "":
				// Both first, no real change
				implicitChange = true
			case prevColumn != "" && !prevColExistedInSource:
				// Previous column is new, position change is implicit
				implicitChange = true
			case sourcePrevCol != "" && !sourcePrevColStillExists:
				// Previous column was dropped, position change is implicit
				implicitChange = true
			case strings.EqualFold(prevColumn, sourcePrevCol):
				// Same previous column, check if we need cascading
				// Cascading happens if the previous column was repositioned
				if prevColumn != "" && needsExplicitPosition[strings.ToLower(prevColumn)] && prevColExistedInSource {
					// Previous column was repositioned, so this column needs repositioning too
					needsExplicitPosition[strings.ToLower(targetCol.Name)] = true
				}
				implicitChange = true
			default:
				// Explicit reorder - previous column changed and both exist
				implicitChange = false
			}

			if !implicitChange {
				needsExplicitPosition[strings.ToLower(targetCol.Name)] = true
			}
		}

		prevColumn = targetCol.Name
	}

	return needsExplicitPosition
}

// diffIndexes compares indexes and returns ALTER clauses for differences.
//
// Most index changes are emitted into the combined ALTER (the returned
// []string). However, an index whose column list is identical but whose
// options differ (e.g. WITH PARSER or KEY_BLOCK_SIZE) cannot be changed by a
// combined `DROP INDEX x, ADD INDEX x (<same cols>)` in a single ALTER: MySQL
// pairs the two clauses up and keeps the existing index, silently ignoring the
// option change. To make such a change actually take effect, the DROP and ADD
// must run as two separate ALTER statements. Those are returned via the second
// value as standalone clause-lists (each becomes its own ALTER statement).
func (ct *CreateTable) diffIndexes(target *CreateTable) (clauses []string, separateStatements [][]string) {
	// Inline column-level UNIQUE / PRIMARY KEY have already been materialized
	// into ct.Indexes by normalization (see indexNormalizer, primaryKeyNormalizer),
	// so both index sets can be walked directly.
	sourceIdxList := ct.Indexes
	targetIdxList := target.Indexes

	// Build maps for easier lookup
	sourceIndexes := make(map[string]*Index)
	for i := range sourceIdxList {
		sourceIndexes[sourceIdxList[i].Name] = &sourceIdxList[i]
	}

	targetIndexes := make(map[string]*Index)
	for i := range targetIdxList {
		targetIndexes[targetIdxList[i].Name] = &targetIdxList[i]
	}

	// Safety net for inline-derived names: the synthesized name is only a
	// guess at what the server assigned. Pair unique indexes that cover the
	// same column set but carry different names whenever at least one side's
	// name came from an inline declaration, so we never DROP a live unique
	// index (or ADD a duplicate) that the other side's inline UNIQUE already
	// expresses. Two explicitly named unique indexes that differ only in name
	// are NOT paired — that is a real rename (DROP + ADD).
	matchedSourceUnique := make(map[string]bool) // source name -> matched
	matchedTargetUnique := make(map[string]bool) // target name -> matched
	for i := range sourceIdxList {
		sourceIdx := &sourceIdxList[i]
		if sourceIdx.Type != "UNIQUE" {
			continue
		}
		if _, exactMatch := targetIndexes[sourceIdx.Name]; exactMatch {
			continue // handled by the normal name-based path
		}
		for j := range targetIdxList {
			targetIdx := &targetIdxList[j]
			if targetIdx.Type != "UNIQUE" {
				continue
			}
			if _, exactMatch := sourceIndexes[targetIdx.Name]; exactMatch {
				continue // this target index already has a name match in source
			}
			if matchedTargetUnique[targetIdx.Name] {
				continue // already paired with another source index
			}
			if !sourceIdx.InlineDerived && !targetIdx.InlineDerived {
				continue // both explicitly named: a genuine rename
			}
			if indexColumnsIdenticalIgnoreName(sourceIdx, targetIdx) {
				matchedSourceUnique[sourceIdx.Name] = true
				matchedTargetUnique[targetIdx.Name] = true
				break
			}
		}
	}

	// Collect DROP operations and sort by name for deterministic output
	var dropClauses []string

	// The primary key is a table-level index on both sides after normalization
	// (see primaryKeyNormalizer), so its add/drop/change falls out of the normal
	// index diff below — no inline-PK special cases are needed. pkDropAdded just
	// guards against emitting "DROP PRIMARY KEY" twice from the source loop.
	pkDropAdded := false

	// optionOnlyChanged tracks index names whose column list is unchanged but
	// whose options differ (e.g. WITH PARSER / KEY_BLOCK_SIZE). These must be
	// emitted as separate DROP + ADD statements rather than combined into one
	// ALTER, because MySQL no-ops a combined DROP+ADD of the same name and
	// column list. Such indexes are routed out of dropClauses/addClauses below.
	optionOnlyChanged := make(map[string]bool)

	for i := range sourceIdxList {
		sourceIdx := &sourceIdxList[i]
		if matchedSourceUnique[sourceIdx.Name] {
			continue // equivalent unique index exists in target under an inline-derived pairing
		}
		targetIdx, existsInTarget := targetIndexes[sourceIdx.Name]

		if !existsInTarget {
			// Index removed completely
			if sourceIdx.Type == "PRIMARY KEY" {
				if !pkDropAdded {
					dropClauses = append(dropClauses, "DROP PRIMARY KEY")
					pkDropAdded = true
				}
			} else {
				dropClauses = append(dropClauses, fmt.Sprintf("DROP INDEX %s", sqlescape.EscapeIdentifier(sourceIdx.Name)))
			}
		} else if !indexesEqual(sourceIdx, targetIdx) && !indexesEqualIgnoreVisibility(sourceIdx, targetIdx) {
			// Index exists but changed (and not just visibility) - need to drop and re-add
			switch {
			case sourceIdx.Type == "PRIMARY KEY":
				// Only add if not already added above
				if !pkDropAdded {
					dropClauses = append(dropClauses, "DROP PRIMARY KEY")
					pkDropAdded = true
				}
			case indexNeedsSeparateRebuild(sourceIdx, targetIdx):
				// A no-op-prone option (WITH PARSER / KEY_BLOCK_SIZE) changed on
				// an unchanged column list. A combined DROP+ADD in one ALTER
				// would be a MySQL no-op, so emit two separate statements that
				// MySQL will actually apply.
				optionOnlyChanged[sourceIdx.Name] = true
				separateStatements = append(separateStatements,
					[]string{fmt.Sprintf("DROP INDEX %s", sqlescape.EscapeIdentifier(sourceIdx.Name))},
					[]string{formatAddIndex(targetIdx)},
				)
			default:
				dropClauses = append(dropClauses, fmt.Sprintf("DROP INDEX %s", sqlescape.EscapeIdentifier(sourceIdx.Name)))
			}
		}
	}
	slices.Sort(dropClauses)
	clauses = append(clauses, dropClauses...)

	// Collect ADD operations and sort by name for deterministic output
	var addClauses []string
	for _, targetIdx := range targetIdxList {
		if matchedTargetUnique[targetIdx.Name] {
			continue // equivalent unique index exists in source under an inline-derived pairing
		}
		sourceIdx, existsInSource := sourceIndexes[targetIdx.Name]

		if !existsInSource {
			// New index - add it
			addClauses = append(addClauses, formatAddIndex(&targetIdx))
		} else if !indexesEqual(sourceIdx, &targetIdx) {
			// Index exists but changed - check if only visibility changed
			if indexesEqualIgnoreVisibility(sourceIdx, &targetIdx) {
				// Only visibility changed - skip for now, handle in ALTER INDEX section
				continue
			}
			// Option-only changes are emitted as separate statements above.
			if optionOnlyChanged[targetIdx.Name] {
				continue
			}
			// Other changes - need to drop and re-add (drop already handled above)
			addClauses = append(addClauses, formatAddIndex(&targetIdx))
		}
	}
	slices.Sort(addClauses)
	clauses = append(clauses, addClauses...)

	// Collect ALTER INDEX operations for visibility changes (must come after DROP/ADD)
	var alterClauses []string
	for _, targetIdx := range targetIdxList {
		sourceIdx, existsInSource := sourceIndexes[targetIdx.Name]

		if existsInSource && !indexesEqual(sourceIdx, &targetIdx) && indexesEqualIgnoreVisibility(sourceIdx, &targetIdx) {
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

	return clauses, separateStatements
}

// diffConstraints compares constraints and returns ALTER clauses for differences
func (ct *CreateTable) diffConstraints(target *CreateTable) []string {
	var clauses []string

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
		// No exact name match — look for an expression-equivalent target constraint
		for j := range target.Constraints {
			targetConstr := &target.Constraints[j]
			if _, exactMatch := sourceConstraints[targetConstr.Name]; exactMatch {
				continue // this target constraint already has a name match in source
			}
			if matchedTargetByExpression[targetConstr.Name] {
				continue // already paired with another source constraint
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

		// Drop if constraint doesn't exist in target OR if it changed
		if !exists || !constraintsEqual(sourceConstr, targetConstr) {
			if exists && constraintsEqualExceptEnforcement(sourceConstr, targetConstr) {
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
			case "CHECK":
				dropClauses = append(dropClauses, fmt.Sprintf("DROP CHECK %s", sqlescape.EscapeIdentifier(sourceConstr.Name)))
			}
		}
	}
	slices.Sort(dropClauses)
	clauses = append(clauses, dropClauses...)
	slices.Sort(enforcementClauses)
	clauses = append(clauses, enforcementClauses...)

	// Collect ADD operations and sort by name for deterministic output
	var addClauses []string
	for _, targetConstr := range target.Constraints {
		if matchedTargetByExpression[targetConstr.Name] {
			continue // equivalent constraint exists in source under a different name
		}
		sourceConstr, existsInSource := sourceConstraints[targetConstr.Name]

		if !existsInSource || !constraintsEqual(sourceConstr, &targetConstr) {
			if existsInSource && constraintsEqualExceptEnforcement(sourceConstr, &targetConstr) {
				continue // enforcement-only change; handled by ALTER CHECK above
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

	// Compare ROW_FORMAT
	if !opts.IgnoreRowFormat {
		if !ptrEqual(ct.TableOptions.getRowFormat(), target.TableOptions.getRowFormat()) {
			if rowFormat := target.TableOptions.getRowFormat(); rowFormat != nil {
				clauses = append(clauses, fmt.Sprintf("ROW_FORMAT=%s", *rowFormat))
			}
		}
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

	return clauses
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
	if a.Type != b.Type {
		return false
	}
	if !ptrEqual(a.Length, b.Length) {
		return false
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

// diffPartitionOptions compares partition options and returns ALTER clauses for differences.
// The first return value contains clauses for the primary ALTER statement.
// The second return value contains clause sets for additional ALTER statements needed
// when a change cannot be expressed in a single statement (e.g. changing partition type
// requires REMOVE PARTITIONING followed by a separate PARTITION BY).
func (ct *CreateTable) diffPartitionOptions(target *CreateTable) ([]string, [][]string) {
	sourcePartition := ct.Partition
	targetPartition := target.Partition

	// Case 1: No partitioning in either table - no changes
	if sourcePartition == nil && targetPartition == nil {
		return nil, nil
	}

	// Case 2: Remove partitioning (source has partitioning, target doesn't)
	if sourcePartition != nil && targetPartition == nil {
		return []string{"REMOVE PARTITIONING"}, nil
	}

	// Case 3: Add partitioning (source doesn't have partitioning, target does)
	if sourcePartition == nil && targetPartition != nil {
		return []string{formatPartitionOptions(targetPartition)}, nil
	}

	// Case 4: Both have partitioning - check if they're different
	if !partitionOptionsEqual(sourcePartition, targetPartition) {
		// Special case: For HASH/KEY partitions where only the partition count changed
		// (no explicit definitions), we can use ADD PARTITION or COALESCE PARTITION
		if isCountOnly, countDiff := isPartitionCountOnlyChange(sourcePartition, targetPartition); isCountOnly {
			if countDiff > 0 {
				return []string{fmt.Sprintf("ADD PARTITION PARTITIONS %d", countDiff)}, nil
			}
			return []string{fmt.Sprintf("COALESCE PARTITION %d", -countDiff)}, nil
		}

		// For all other partition changes (e.g. changing partition type from HASH to RANGE),
		// MySQL requires two separate ALTER TABLE statements:
		// 1. REMOVE PARTITIONING
		// 2. PARTITION BY ...
		// The first goes into the primary statement, the second is returned as an additional statement.
		return []string{"REMOVE PARTITIONING"}, [][]string{{formatPartitionOptions(targetPartition)}}
	}

	return nil, nil
}
