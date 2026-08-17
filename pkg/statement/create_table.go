//nolint:noinlineerr,exhaustive
package statement

// This file provides structured parsing of CREATE TABLE statements.
// The CreateTable struct and related types use pointer fields for optional elements

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/parser"
	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/parser/format"
	"github.com/block/spirit/pkg/parser/mysql"
	"github.com/block/spirit/pkg/parser/types"
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
	Length          *int              `json:"length,omitempty"`
	Precision       *int              `json:"precision,omitempty"`
	Scale           *int              `json:"scale,omitempty"`
	Unsigned        *bool             `json:"unsigned,omitempty"`
	Zerofill        *bool             `json:"zerofill,omitempty"`    // ZEROFILL display attribute (implies unsigned)
	EnumValues      []string          `json:"enum_values,omitempty"` // Permitted values for ENUM type
	SetValues       []string          `json:"set_values,omitempty"`  // Permitted values for SET type
	Nullable        bool              `json:"nullable"`
	Default         *string           `json:"default,omitempty"`
	DefaultIsExpr   bool              `json:"default_is_expr,omitempty"`   // true when default is an expression (needs parens), e.g. DEFAULT (json_object())
	DefaultIsString bool              `json:"default_is_string,omitempty"` // true when the default is a quoted string literal (so it must be re-quoted on emission, even if it looks like a keyword/number)
	OnUpdate        *string           `json:"on_update,omitempty"`         // ON UPDATE expression for TIMESTAMP/DATETIME, e.g. "current_timestamp"
	GeneratedExpr   *string           `json:"generated_expr,omitempty"`    // Expression for GENERATED ALWAYS AS (...) columns
	GeneratedStored bool              `json:"generated_stored,omitempty"`  // true = STORED, false = VIRTUAL (only meaningful when GeneratedExpr is set)
	Check           *string           `json:"check,omitempty"`             // Column-level CHECK (...) constraint expression
	SRID            *uint32           `json:"srid,omitempty"`              // SRID attribute for spatial columns
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
	Type         string            `json:"type"`                  // PRIMARY, UNIQUE, INDEX, FULLTEXT, SPATIAL
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

func (ct *CreateTable) GetPartition() *PartitionOptions {
	return ct.Partition
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
	// here would emit a destructive varchar -> varbinary type change.
	if mysql.HasBinaryFlag(col.Tp.GetFlag()) && col.Tp.GetCharset() == "binary" {
		switch column.Type {
		case "varchar":
			column.Type = "varbinary"
		case "char":
			column.Type = "binary"
		case "text":
			column.Type = "blob"
		case "tinytext":
			column.Type = "tinyblob"
		case "mediumtext":
			column.Type = "mediumblob"
		case "longtext":
			column.Type = "longblob"
		}
	}

	// Extract type information
	typeStr := col.Tp.String()
	if length := extractLengthFromTypeString(typeStr); length > 0 {
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

				if literal, isStr := stringLiteralValue(defaultExpr); isStr {
					// Quoted string literal default. Store the true, raw
					// (fully-unescaped) value off the AST and remember it
					// was a string so we re-quote it on emission — even if
					// the value looks like a keyword (TRUE/NULL) or a
					// number. Escaping happens exactly once, at emit time.
					column.Default = &literal
					column.DefaultIsString = true
				} else {
					// Non-string defaults (numeric, functions, expressions):
					// keep the Restored text representation.
					defaultRaw := fmt.Sprintf("%v", ct.parseExpression(defaultExpr))
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

// parseExpression converts an expression to a string representation
func (ct *CreateTable) parseExpression(expr ast.ExprNode) any {
	if expr == nil {
		return nil
	}

	// Handle different expression types
	switch e := expr.(type) {
	case *ast.FuncCallExpr:
		// Handle function calls like CURRENT_TIMESTAMP, CURRENT_TIMESTAMP(3), UUID(), etc.
		// We use Restore to preserve function arguments (e.g. precision in CURRENT_TIMESTAMP(3)).
		// RestoreKeyWordLowercase renders the function name and any keywords
		// inside its arguments in lowercase — matching MySQL's canonical
		// SHOW CREATE TABLE form (e.g. DEFAULT (concat(...))) so that
		// function-name case never causes a spurious diff — while leaving
		// string-literal arguments byte-exact. The previous strings.ToLower
		// over the whole Restored text corrupted literal case:
		// DEFAULT (concat('A')) round-tripped to concat('a'), emitting a
		// different default value and making defaults that differ only in
		// literal case compare equal.
		var sb strings.Builder
		rCtx := format.NewRestoreCtx(format.RestoreStringSingleQuotes|format.RestoreKeyWordLowercase|
			format.RestoreNameBackQuotes|format.RestoreStringWithoutCharset, &sb)
		if err := e.Restore(rCtx); err != nil {
			return e.FnName.L // fallback to function name on error
		}
		restored := sb.String()
		// Normalize: MySQL's canonical SHOW CREATE TABLE uses "CURRENT_TIMESTAMP" (no parens)
		// when there is no fractional seconds precision, but the parser's Restore always adds "()".
		// We only strip parens for timestamp-family functions; other functions like json_object()
		// need to keep their parens as they represent actual function calls.
		name := strings.ToUpper(e.FnName.L)
		isTimestampFunc := name == "CURRENT_TIMESTAMP" || name == "NOW" ||
			name == "LOCALTIME" || name == "LOCALTIMESTAMP" || name == "UTC_TIMESTAMP"
		if isTimestampFunc && len(e.Args) == 0 && strings.HasSuffix(restored, "()") {
			restored = strings.TrimSuffix(restored, "()")
		}
		return restored
	default:
		// For other types, fall back to text representation
		var sb strings.Builder
		sb.Reset()
		rCtx := format.NewRestoreCtx(format.DefaultRestoreFlags|format.RestoreStringWithoutCharset, &sb)
		if err := expr.Restore(rCtx); err != nil {
			return "<error>"
		}
		str := sb.String()
		// if the string is quoted, remove quotes
		if strings.HasPrefix(str, "'") && strings.HasSuffix(str, "'") {
			str = str[1 : len(str)-1]
		}
		return str
	}
}
