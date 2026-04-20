//nolint:noinlineerr,exhaustive
package statement

// This file provides structured parsing of CREATE TABLE statements.
// The CreateTable struct and related types use pointer fields for optional elements

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
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
	Raw           *ast.ColumnDef    `json:"-"`
	Name          string            `json:"name"`
	Type          string            `json:"type"`
	Length        *int              `json:"length,omitempty"`
	Precision     *int              `json:"precision,omitempty"`
	Scale         *int              `json:"scale,omitempty"`
	Unsigned      *bool             `json:"unsigned,omitempty"`
	EnumValues    []string          `json:"enum_values,omitempty"` // Permitted values for ENUM type
	SetValues     []string          `json:"set_values,omitempty"`  // Permitted values for SET type
	Nullable      bool              `json:"nullable"`
	Default       *string           `json:"default,omitempty"`
	DefaultIsExpr bool              `json:"default_is_expr,omitempty"` // true when default is an expression (needs parens), e.g. DEFAULT (json_object())
	AutoInc       bool              `json:"auto_increment"`
	PrimaryKey    bool              `json:"primary_key"`
	Unique        bool              `json:"unique"`
	Comment       *string           `json:"comment,omitempty"`
	Charset       *string           `json:"charset,omitempty"`
	Collation     *string           `json:"collation,omitempty"`
	Options       map[string]string `json:"options,omitempty"`
}

// IndexColumn represents a column or expression in an index
type IndexColumn struct {
	Name       string  `json:"name,omitempty"`       // Column name (empty for expression indexes)
	Expression *string `json:"expression,omitempty"` // Expression for functional indexes
	Length     *int    `json:"length,omitempty"`     // Prefix length for string columns
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
}

// Constraint represents a table constraint
type Constraint struct {
	Raw        *ast.Constraint      `json:"-"`
	Name       string               `json:"name"`
	Type       string               `json:"type"` // CHECK, FOREIGN KEY, etc.
	Columns    []string             `json:"columns,omitempty"`
	Expression *string              `json:"expression,omitempty"`
	References *ForeignKeyReference `json:"references,omitempty"`
	Definition *string              `json:"definition,omitempty"` // Generated definition string for compatibility
	Options    map[string]any       `json:"options,omitempty"`
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
	Type         string                `json:"type"`                   // RANGE, LIST, HASH, KEY, SYSTEM_TIME
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

	// Auto-generate MySQL default names for unnamed indexes
	ct.autoNameIndexes()

	// Parse table options
	if len(ct.Raw.Options) > 0 {
		ct.TableOptions = ct.parseTableOptions(ct.Raw.Options)
	}

	// Parse partition options
	if ct.Raw.Partition != nil {
		ct.Partition = ct.parsePartitionOptions(ct.Raw.Partition)
	}
}

// autoNameIndexes assigns MySQL's default naming convention to any unnamed
// non-PRIMARY KEY indexes. MySQL names indexes after the first column in the
// index. If that name is already taken, it appends _2, _3, etc.
// This ensures that unnamed indexes like INDEX (col) are treated identically
// to their MySQL-normalized form KEY `col` (`col`) during diff comparisons.
func (ct *CreateTable) autoNameIndexes() {
	// Track all used index names (including already-named indexes)
	usedNames := make(map[string]int) // name -> highest suffix used (0 = base name)
	for i := range ct.Indexes {
		if ct.Indexes[i].Name != "" {
			usedNames[ct.Indexes[i].Name] = 0
		}
	}

	for i := range ct.Indexes {
		idx := &ct.Indexes[i]
		if idx.Name != "" || idx.Type == "PRIMARY KEY" {
			continue
		}

		// Determine the base name from the first column.
		// For expression indexes the first column name may be empty;
		// fall back to a functional_index convention similar to MySQL 8.0.
		var baseName string
		switch {
		case len(idx.Columns) > 0 && idx.Columns[0] != "":
			baseName = idx.Columns[0]
		case len(idx.ColumnList) > 0 && idx.ColumnList[0].Name != "":
			baseName = idx.ColumnList[0].Name
		case len(idx.ColumnList) > 0:
			baseName = "functional_index"
		default:
			baseName = "idx"
		}

		// Find a unique name following MySQL's convention:
		// first try baseName, then baseName_2, baseName_3, ...
		candidate := baseName
		if _, taken := usedNames[candidate]; taken {
			// Start from _2 and increment
			suffix := 2
			if prev, ok := usedNames[baseName]; ok && prev >= 2 {
				suffix = prev + 1
			}
			for {
				candidate = fmt.Sprintf("%s_%d", baseName, suffix)
				if _, taken := usedNames[candidate]; !taken {
					break
				}
				suffix++
			}
		}

		idx.Name = candidate
		usedNames[candidate] = 0
		// Track the highest suffix used for this base name
		if candidate != baseName {
			// Extract suffix number
			var suffixNum int
			if _, err := fmt.Sscanf(candidate, baseName+"_%d", &suffixNum); err == nil {
				if current, ok := usedNames[baseName]; !ok || suffixNum > current {
					usedNames[baseName] = suffixNum
				}
			}
		}
	}
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

	// Check if this is a binary type (VARBINARY, BLOB, etc.)
	// The TiDB parser converts binary types to their text equivalents,
	// so we need to check the binary flag and convert back
	if mysql.HasBinaryFlag(col.Tp.GetFlag()) {
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

	// Extract charset and collation from the type itself
	// (they may be overridden by column options later)
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

				defaultVal := ct.parseExpression(opt.Expr)
				if defaultStr, ok := defaultVal.(string); ok && defaultStr != "" {
					// Remove surrounding quotes if present for string literals
					if len(defaultStr) >= 2 && defaultStr[0] == '\'' && defaultStr[len(defaultStr)-1] == '\'' {
						defaultStr = defaultStr[1 : len(defaultStr)-1]
					}

					column.Default = &defaultStr
				} else {
					// For non-string defaults (e.g., numeric, functions), use the raw expression
					defaultRaw := fmt.Sprintf("%v", defaultVal)
					column.Default = &defaultRaw
				}
			}
		case ast.ColumnOptionComment:
			if opt.Expr != nil {
				comment := ct.parseExpression(opt.Expr)
				if commentStr, ok := comment.(string); ok && commentStr != "" {
					// Remove surrounding quotes if present
					if len(commentStr) >= 2 && commentStr[0] == '\'' && commentStr[len(commentStr)-1] == '\'' {
						commentStr = commentStr[1 : len(commentStr)-1]
					}

					column.Comment = &commentStr
				}
			}
		case ast.ColumnOptionCollate:
			if opt.StrValue != "" {
				column.Collation = &opt.StrValue
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

		if constraint.Expr != nil {
			expr := ct.parseExpression(constraint.Expr)
			if exprStr, ok := expr.(string); ok {
				constr.Expression = &exprStr
				// Generate definition string
				definition := fmt.Sprintf("CHECK (%s)", exprStr)
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

			// Parse ON DELETE action (check if ReferOpt is non-empty)
			if constraint.Refer.OnDelete != nil && constraint.Refer.OnDelete.ReferOpt.String() != "" {
				onDelete := constraint.Refer.OnDelete.ReferOpt.String()
				fkRef.OnDelete = &onDelete
			}

			// Parse ON UPDATE action (check if ReferOpt is non-empty)
			if constraint.Refer.OnUpdate != nil && constraint.Refer.OnUpdate.ReferOpt.String() != "" {
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
		col := IndexColumn{}

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
	case ast.PartitionTypeSystemTime:
		partOpts.Type = "SYSTEM_TIME"
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
			val := ct.parseExpression(expr)
			values.Values = append(values.Values, val)
		}

		return values
	case *ast.PartitionDefinitionClauseIn:
		values := &PartitionValues{
			Type:   "IN",
			Values: make([]any, 0, len(c.Values)),
		}
		for _, valList := range c.Values {
			if len(valList) == 1 {
				val := ct.parseExpression(valList[0])
				values.Values = append(values.Values, val)
			} else {
				// Multiple values in a single clause
				subValues := make([]any, 0, len(valList))
				for _, expr := range valList {
					val := ct.parseExpression(expr)
					subValues = append(subValues, val)
				}

				values.Values = append(values.Values, subValues...)
			}
		}

		return values
	case *ast.PartitionDefinitionClauseHistory:
		if c.Current {
			return &PartitionValues{Type: "CURRENT", Values: []any{}}
		} else {
			return &PartitionValues{Type: "HISTORY", Values: []any{}}
		}
	default:
		return nil
	}
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

// isExpressionDefault returns true when the default value expression should be
// wrapped in parentheses in the generated DDL. MySQL requires expression defaults
// (as opposed to literal defaults) to be enclosed in parens, e.g. DEFAULT (json_object()).
// This mirrors the logic in the TiDB parser's ColumnOption.Restore for ColumnOptionDefaultValue:
// non-CURRENT_TIMESTAMP function calls and column name expressions get outer parentheses.
func isExpressionDefault(expr ast.ExprNode) bool {
	if expr == nil {
		return false
	}
	switch e := expr.(type) {
	case *ast.FuncCallExpr:
		// CURRENT_TIMESTAMP (and aliases NOW, LOCALTIME, etc.) are literal-style defaults
		// that don't need parens. Everything else is an expression default.
		switch e.FnName.L {
		case "current_timestamp", "now", "localtime", "localtimestamp", "utc_timestamp":
			return false
		}
		return true
	case *ast.ColumnNameExpr:
		return true
	default:
		return false
	}
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
		var sb strings.Builder
		rCtx := format.NewRestoreCtx(format.DefaultRestoreFlags|format.RestoreStringWithoutCharset, &sb)
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
		return strings.ToLower(restored)
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

// extractLengthFromTypeString extracts length from type string like "varchar(100)"
func extractLengthFromTypeString(typeStr string) int {
	// Simple regex-like parsing for common cases
	if strings.Contains(typeStr, "(") && strings.Contains(typeStr, ")") {
		start := strings.Index(typeStr, "(")

		end := strings.Index(typeStr, ")")
		if start < end && start != -1 && end != -1 {
			lengthStr := typeStr[start+1 : end]
			// Handle cases like "decimal(10,2)" - take the first number
			if commaIdx := strings.Index(lengthStr, ","); commaIdx != -1 {
				lengthStr = lengthStr[:commaIdx]
			}

			var length int
			if n, err := fmt.Sscanf(lengthStr, "%d", &length); n == 1 && err == nil {
				return length
			}
		}
	}

	return 0
}

// extractPrecisionScaleFromTypeString extracts precision and scale from type string like "decimal(10,2)"
func extractPrecisionScaleFromTypeString(typeStr string) (int, int) {
	if strings.Contains(typeStr, "(") && strings.Contains(typeStr, ")") {
		start := strings.Index(typeStr, "(")

		end := strings.Index(typeStr, ")")
		if start < end && start != -1 && end != -1 {
			paramStr := typeStr[start+1 : end]
			if precisionStr, scaleStr, found := strings.Cut(paramStr, ","); found {
				precisionStr = strings.TrimSpace(precisionStr)
				scaleStr = strings.TrimSpace(scaleStr)

				var precision, scale int
				if n, err := fmt.Sscanf(precisionStr, "%d", &precision); n == 1 && err == nil {
					if n, err := fmt.Sscanf(scaleStr, "%d", &scale); n == 1 && err == nil {
						return precision, scale
					}

					return precision, 0
				}
			}
		}
	}

	return 0, 0
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

// Helper function to create string pointer
func stringPtr(s string) *string {
	return &s
}

// RemoveSecondaryIndexes takes a CREATE TABLE statement and returns a modified version
// without secondary indexes (regular INDEX only). PRIMARY KEY, UNIQUE, and FULLTEXT
// indexes are preserved.
func RemoveSecondaryIndexes(createStmt string) (string, error) {
	ct, err := ParseCreateTable(createStmt)
	if err != nil {
		return "", fmt.Errorf("failed to parse CREATE TABLE: %w", err)
	}

	// Filter out regular INDEX entries from the constraints
	filteredConstraints := make([]*ast.Constraint, 0)
	for _, constraint := range ct.Raw.Constraints {
		// Keep everything except regular INDEX
		switch constraint.Tp {
		case ast.ConstraintKey, ast.ConstraintIndex:
			// Skip regular indexes
			continue
		default:
			// Keep PRIMARY KEY, UNIQUE, FULLTEXT, SPATIAL, etc.
			filteredConstraints = append(filteredConstraints, constraint)
		}
	}

	// Update the AST with filtered constraints
	ct.Raw.Constraints = filteredConstraints

	// Restore the modified AST back to SQL
	var sb strings.Builder
	rCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	if err := ct.Raw.Restore(rCtx); err != nil {
		return "", fmt.Errorf("failed to restore CREATE TABLE: %w", err)
	}

	return sb.String(), nil
}

// GetMissingSecondaryIndexes compares two CREATE TABLE statements (source and target)
// and returns an ALTER TABLE statement that adds any missing secondary indexes.
// Returns an empty string if no indexes need to be added.
// Considers UNIQUE, FULLTEXT, and regular INDEX types. PRIMARY KEY is excluded as it's fundamental to table structure.
// Note: SPATIAL indexes are not currently supported by the TiDB parser constraint types used in this implementation.
func GetMissingSecondaryIndexes(sourceCreateTable, targetCreateTable, tableName string) (string, error) {
	// Parse both CREATE TABLE statements
	sourceCT, err := ParseCreateTable(sourceCreateTable)
	if err != nil {
		return "", fmt.Errorf("failed to parse source CREATE TABLE: %w", err)
	}

	targetCT, err := ParseCreateTable(targetCreateTable)
	if err != nil {
		return "", fmt.Errorf("failed to parse target CREATE TABLE: %w", err)
	}

	// Build a map of existing indexes on the target (all secondary indexes)
	targetIndexes := make(map[string]bool)
	for _, constraint := range targetCT.Raw.Constraints {
		// Skip PRIMARY KEY, but include all other index types
		if constraint.Tp == ast.ConstraintPrimaryKey {
			continue
		}
		targetIndexes[constraint.Name] = true
	}

	// Find missing indexes from source
	var missingIndexes []*ast.Constraint
	for _, constraint := range sourceCT.Raw.Constraints {
		// Consider all secondary indexes (UNIQUE, FULLTEXT, and regular INDEX)
		// Skip only PRIMARY KEY as it's fundamental to table structure
		if constraint.Tp == ast.ConstraintPrimaryKey {
			continue
		}
		// Include: UNIQUE, FULLTEXT, and regular INDEX
		// Note: SPATIAL indexes are not currently supported by the TiDB parser constraint types
		if constraint.Tp != ast.ConstraintKey &&
			constraint.Tp != ast.ConstraintIndex &&
			constraint.Tp != ast.ConstraintUniq &&
			constraint.Tp != ast.ConstraintUniqKey &&
			constraint.Tp != ast.ConstraintUniqIndex &&
			constraint.Tp != ast.ConstraintFulltext {
			continue
		}

		// Check if this index exists on target
		if !targetIndexes[constraint.Name] {
			missingIndexes = append(missingIndexes, constraint)
		}
	}

	// If no missing indexes, return empty string
	if len(missingIndexes) == 0 {
		return "", nil
	}

	// Build a single ALTER TABLE statement with all missing indexes
	var alterClauses []string
	for _, constraint := range missingIndexes {
		var sb strings.Builder

		// Add appropriate keyword based on constraint type
		switch constraint.Tp {
		case ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
			sb.WriteString("ADD UNIQUE INDEX")
		case ast.ConstraintFulltext:
			sb.WriteString("ADD FULLTEXT INDEX")
		default: // ast.ConstraintKey, ast.ConstraintIndex
			sb.WriteString("ADD INDEX")
		}

		// Add index name if present
		if constraint.Name != "" {
			fmt.Fprintf(&sb, " `%s`", constraint.Name)
		}

		// Add columns
		sb.WriteString(" (")
		for i, key := range constraint.Keys {
			if i > 0 {
				sb.WriteString(", ")
			}
			fmt.Fprintf(&sb, "`%s`", key.Column.Name.String())
			// Add length if specified
			if key.Length > 0 {
				fmt.Fprintf(&sb, "(%d)", key.Length)
			}
		}
		sb.WriteString(")")

		// Add index options if present
		if constraint.Option != nil {
			opt := constraint.Option

			// Add USING clause
			if opt.Tp != ast.IndexTypeInvalid && opt.Tp.String() != "" {
				sb.WriteString(" USING " + opt.Tp.String())
			}

			// Add KEY_BLOCK_SIZE
			if opt.KeyBlockSize > 0 {
				fmt.Fprintf(&sb, " KEY_BLOCK_SIZE=%d", opt.KeyBlockSize)
			}

			// Add COMMENT
			if opt.Comment != "" {
				fmt.Fprintf(&sb, " COMMENT '%s'", sqlescape.EscapeString(opt.Comment))
			}

			// Add VISIBLE/INVISIBLE
			if opt.Visibility == ast.IndexVisibilityInvisible {
				sb.WriteString(" INVISIBLE")
			}
		}
		alterClauses = append(alterClauses, sb.String())
	}
	// Combine all ADD INDEX clauses into a single ALTER TABLE statement
	return fmt.Sprintf("ALTER TABLE `%s` %s", tableName, strings.Join(alterClauses, ", ")), nil
}
