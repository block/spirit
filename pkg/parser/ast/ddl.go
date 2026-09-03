// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package ast

import (
	"errors"
	"fmt"

	"github.com/block/spirit/pkg/parser/auth"
	"github.com/block/spirit/pkg/parser/format"
	"github.com/block/spirit/pkg/parser/mysql"
	"github.com/block/spirit/pkg/parser/types"
)

var (
	_ DDLNode = &AlterTableStmt{}
	_ DDLNode = &CreateDatabaseStmt{}
	_ DDLNode = &CreateJSONDualityViewStmt{}
	_ DDLNode = &CreateIndexStmt{}
	_ DDLNode = &CreateTableStmt{}
	_ DDLNode = &CreateViewStmt{}
	_ DDLNode = &DropDatabaseStmt{}
	_ DDLNode = &DropIndexStmt{}
	_ DDLNode = &DropTableStmt{}
	_ DDLNode = &OptimizeTableStmt{}
	_ DDLNode = &RenameTableStmt{}
	_ DDLNode = &TruncateTableStmt{}

	_ Node = &AlterTableSpec{}
	_ Node = &ColumnDef{}
	_ Node = &ColumnOption{}
	_ Node = &ColumnPosition{}
	_ Node = &Constraint{}
	_ Node = &IndexPartSpecification{}
	_ Node = &ReferenceDef{}
)

// NullString represents a string that may be nil.
type NullString struct {
	String string
	Empty  bool // Empty is true if String is empty backtick.
}

// DatabaseOptionType is the type for database options.
type DatabaseOptionType int

// Database option types.
const (
	DatabaseOptionNone DatabaseOptionType = iota
	DatabaseOptionCharset
	DatabaseOptionCollate
	DatabaseOptionEncryption
	DatabaseOptionReadOnly
)

// DatabaseOption represents database option.
type DatabaseOption struct {
	Tp        DatabaseOptionType
	Value     string
	UintValue uint64
}

// Restore implements Node interface.
func (n *DatabaseOption) Restore(ctx *format.RestoreCtx) error {
	switch n.Tp { //nolint:exhaustive
	case DatabaseOptionCharset:
		ctx.WriteKeyWord("CHARACTER SET")
		ctx.WritePlain(" = ")
		ctx.WritePlain(n.Value)
	case DatabaseOptionCollate:
		ctx.WriteKeyWord("COLLATE")
		ctx.WritePlain(" = ")
		ctx.WritePlain(n.Value)
	case DatabaseOptionEncryption:
		ctx.WriteKeyWord("ENCRYPTION")
		ctx.WritePlain(" = ")
		ctx.WriteString(n.Value)
	case DatabaseOptionReadOnly:
		ctx.WriteKeyWord("READ ONLY")
		ctx.WritePlain(" = ")
		if n.Value != "" {
			ctx.WriteKeyWord(n.Value) // DEFAULT
		} else {
			ctx.WritePlainf("%d", n.UintValue)
		}
	default:
		return fmt.Errorf("invalid DatabaseOptionType: %d", n.Tp)
	}
	return nil
}

// CreateDatabaseStmt is a statement to create a database.
// See https://dev.mysql.com/doc/refman/5.7/en/create-database.html
type CreateDatabaseStmt struct {
	ddlNode

	IfNotExists bool
	Name        CIStr
	Options     []*DatabaseOption
}

// Restore implements Node interface.
func (n *CreateDatabaseStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE DATABASE ")
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}
	ctx.WriteName(n.Name.O)
	for i, option := range n.Options {
		ctx.WritePlain(" ")
		err := option.Restore(ctx)
		if err != nil {
			return fmt.Errorf("an error occurred while splicing CreateDatabaseStmt DatabaseOption: [%v]: %w", i, err)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *CreateDatabaseStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreateDatabaseStmt)
	return v.Leave(n)
}

// AlterDatabaseStmt is a statement to change the structure of a database.
// See https://dev.mysql.com/doc/refman/5.7/en/alter-database.html
type AlterDatabaseStmt struct {
	ddlNode

	Name                 CIStr
	AlterDefaultDatabase bool
	Options              []*DatabaseOption
}

// Restore implements Node interface.
func (n *AlterDatabaseStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ALTER DATABASE")
	if !n.AlterDefaultDatabase {
		ctx.WritePlain(" ")
		ctx.WriteName(n.Name.O)
	}
	for i, option := range n.Options {
		ctx.WritePlain(" ")
		err := option.Restore(ctx)
		if err != nil {
			return fmt.Errorf("an error occurred while splicing AlterDatabaseStmt DatabaseOption: [%v]: %w", i, err)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *AlterDatabaseStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AlterDatabaseStmt)
	return v.Leave(n)
}

// DropDatabaseStmt is a statement to drop a database and all tables in the database.
// See https://dev.mysql.com/doc/refman/5.7/en/drop-database.html
type DropDatabaseStmt struct {
	ddlNode

	IfExists bool
	Name     CIStr
}

// Restore implements Node interface.
func (n *DropDatabaseStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DROP DATABASE ")
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}
	ctx.WriteName(n.Name.O)
	return nil
}

// Accept implements Node Accept interface.
func (n *DropDatabaseStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DropDatabaseStmt)
	return v.Leave(n)
}

// IndexPartSpecifications is used for parsing index column name or index expression from SQL.
type IndexPartSpecification struct {
	node

	Column *ColumnName
	Length int
	// Order is parsed but should be ignored because MySQL v5.7 doesn't support it.
	Desc bool
	Expr ExprNode
}

// Restore implements Node interface.
func (n *IndexPartSpecification) Restore(ctx *format.RestoreCtx) error {
	if n.Expr != nil {
		ctx.WritePlain("(")
		if err := n.Expr.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while splicing IndexPartSpecifications: %w", err)
		}
		ctx.WritePlain(")")
		if n.Desc {
			ctx.WritePlain(" DESC")
		}
		return nil
	}
	if err := n.Column.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while splicing IndexPartSpecifications: %w", err)
	}
	if n.Length > 0 {
		ctx.WritePlainf("(%d)", n.Length)
	}
	if n.Desc {
		ctx.WritePlain(" DESC")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *IndexPartSpecification) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*IndexPartSpecification)
	if n.Expr != nil {
		node, ok := n.Expr.Accept(v)
		if !ok {
			return n, false
		}
		n.Expr = node.(ExprNode)
		return v.Leave(n)
	}
	node, ok := n.Column.Accept(v)
	if !ok {
		return n, false
	}
	n.Column = node.(*ColumnName)
	return v.Leave(n)
}

// MatchType is the type for reference match type.
type MatchType int

// match type
const (
	MatchNone MatchType = iota
	MatchFull
	MatchPartial
	MatchSimple
)

// ReferenceDef is used for parsing foreign key reference option from SQL.
// See http://dev.mysql.com/doc/refman/5.7/en/create-table-foreign-keys.html
type ReferenceDef struct {
	node

	Table                   *TableName
	IndexPartSpecifications []*IndexPartSpecification
	OnDelete                *OnDeleteOpt
	OnUpdate                *OnUpdateOpt
	Match                   MatchType
}

// Restore implements Node interface.
func (n *ReferenceDef) Restore(ctx *format.RestoreCtx) error {
	if n.Table != nil {
		ctx.WriteKeyWord("REFERENCES ")
		if err := n.Table.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while splicing ReferenceDef: %w", err)
		}
	}

	if n.IndexPartSpecifications != nil {
		ctx.WritePlain("(")
		for i, indexColNames := range n.IndexPartSpecifications {
			if i > 0 {
				ctx.WritePlain(", ")
			}
			if err := indexColNames.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while splicing IndexPartSpecifications: [%v]: %w", i, err)
			}
		}
		ctx.WritePlain(")")
	}

	if n.Match != MatchNone {
		ctx.WriteKeyWord(" MATCH ")
		switch n.Match { //nolint:exhaustive
		case MatchFull:
			ctx.WriteKeyWord("FULL")
		case MatchPartial:
			ctx.WriteKeyWord("PARTIAL")
		case MatchSimple:
			ctx.WriteKeyWord("SIMPLE")
		}
	}
	if n.OnDelete.ReferOpt != ReferOptionNoOption {
		ctx.WritePlain(" ")
		if err := n.OnDelete.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while splicing OnDelete: %w", err)
		}
	}
	if n.OnUpdate.ReferOpt != ReferOptionNoOption {
		ctx.WritePlain(" ")
		if err := n.OnUpdate.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while splicing OnUpdate: %w", err)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *ReferenceDef) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ReferenceDef)
	if n.Table != nil {
		node, ok := n.Table.Accept(v)
		if !ok {
			return n, false
		}
		n.Table = node.(*TableName)
	}
	for i, val := range n.IndexPartSpecifications {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.IndexPartSpecifications[i] = node.(*IndexPartSpecification)
	}
	onDelete, ok := n.OnDelete.Accept(v)
	if !ok {
		return n, false
	}
	n.OnDelete = onDelete.(*OnDeleteOpt)
	onUpdate, ok := n.OnUpdate.Accept(v)
	if !ok {
		return n, false
	}
	n.OnUpdate = onUpdate.(*OnUpdateOpt)
	return v.Leave(n)
}

// OnDeleteOpt is used for optional on delete clause.
type OnDeleteOpt struct {
	node
	ReferOpt ReferOptionType
}

// Restore implements Node interface.
func (n *OnDeleteOpt) Restore(ctx *format.RestoreCtx) error {
	if n.ReferOpt != ReferOptionNoOption {
		ctx.WriteKeyWord("ON DELETE ")
		ctx.WriteKeyWord(n.ReferOpt.String())
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *OnDeleteOpt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*OnDeleteOpt)
	return v.Leave(n)
}

// OnUpdateOpt is used for optional on update clause.
type OnUpdateOpt struct {
	node
	ReferOpt ReferOptionType
}

// Restore implements Node interface.
func (n *OnUpdateOpt) Restore(ctx *format.RestoreCtx) error {
	if n.ReferOpt != ReferOptionNoOption {
		ctx.WriteKeyWord("ON UPDATE ")
		ctx.WriteKeyWord(n.ReferOpt.String())
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *OnUpdateOpt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*OnUpdateOpt)
	return v.Leave(n)
}

// ColumnOptionType is the type for ColumnOption.
type ColumnOptionType int

// ColumnOption types.
const (
	ColumnOptionNoOption ColumnOptionType = iota
	ColumnOptionPrimaryKey
	ColumnOptionNotNull
	ColumnOptionAutoIncrement
	ColumnOptionDefaultValue
	ColumnOptionUniqKey
	ColumnOptionNull
	ColumnOptionOnUpdate // For Timestamp and Datetime only.
	ColumnOptionComment
	ColumnOptionGenerated
	ColumnOptionReference
	ColumnOptionCollate
	ColumnOptionCheck
	ColumnOptionColumnFormat
	ColumnOptionStorage
	ColumnOptionSecondaryEngineAttribute
	ColumnOptionSrid
	ColumnOptionVisibility
	ColumnOptionEngineAttribute
	ColumnOptionNotSecondary
)

var (
	invalidOptionForGeneratedColumn = map[ColumnOptionType]string{
		ColumnOptionAutoIncrement: "AUTO_INCREMENT",
		ColumnOptionOnUpdate:      "ON UPDATE",
		ColumnOptionDefaultValue:  "DEFAULT",
	}
)

// ColumnOptionList stores column options.
type ColumnOptionList struct {
	HasCollateOption bool
	Options          []*ColumnOption
}

// ColumnOption is used for parsing column constraint info from SQL.
type ColumnOption struct {
	node

	Tp ColumnOptionType
	// Expr is used for ColumnOptionDefaultValue/ColumnOptionOnUpdateColumnOptionGenerated.
	// For ColumnOptionDefaultValue or ColumnOptionOnUpdate, it's the target value.
	// For ColumnOptionGenerated, it's the target expression.
	Expr ExprNode
	// Stored is only for ColumnOptionGenerated, default is false.
	Stored bool
	// Refer is used for foreign key.
	Refer    *ReferenceDef
	StrValue string
	// Enforced is only for Check, default is true.
	Enforced bool
	// Name is only used for Check Constraint name.
	ConstraintName      string
	SecondaryEngineAttr string
	Srid                uint32
}

// Restore implements Node interface.
func (n *ColumnOption) Restore(ctx *format.RestoreCtx) error {
	switch n.Tp {
	case ColumnOptionNoOption:
		return nil
	case ColumnOptionPrimaryKey:
		ctx.WriteKeyWord("PRIMARY KEY")
	case ColumnOptionNotNull:
		ctx.WriteKeyWord("NOT NULL")
	case ColumnOptionAutoIncrement:
		ctx.WriteKeyWord("AUTO_INCREMENT")
	case ColumnOptionDefaultValue:
		ctx.WriteKeyWord("DEFAULT ")
		printOuterParentheses := false
		if funcCallExpr, ok := n.Expr.(*FuncCallExpr); ok {
			if name := funcCallExpr.FnName.L; name != CurrentTimestamp {
				printOuterParentheses = true
			}
		}
		if _, ok := n.Expr.(*ColumnNameExpr); ok {
			printOuterParentheses = true
		}
		if printOuterParentheses {
			ctx.WritePlain("(")
		}
		if err := n.Expr.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while splicing ColumnOption DefaultValue Expr: %w", err)
		}
		if printOuterParentheses {
			ctx.WritePlain(")")
		}
	case ColumnOptionUniqKey:
		ctx.WriteKeyWord("UNIQUE KEY")
		if n.StrValue == "Global" {
			ctx.WriteKeyWord(" GLOBAL")
		}
	case ColumnOptionNull:
		ctx.WriteKeyWord("NULL")
	case ColumnOptionOnUpdate:
		ctx.WriteKeyWord("ON UPDATE ")
		if err := n.Expr.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while splicing ColumnOption ON UPDATE Expr: %w", err)
		}
	case ColumnOptionComment:
		ctx.WriteKeyWord("COMMENT ")
		if err := n.Expr.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while splicing ColumnOption COMMENT Expr: %w", err)
		}
	case ColumnOptionGenerated:
		ctx.WriteKeyWord("GENERATED ALWAYS AS")
		ctx.WritePlain("(")
		if err := n.Expr.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while splicing ColumnOption GENERATED ALWAYS Expr: %w", err)
		}
		ctx.WritePlain(")")
		if n.Stored {
			ctx.WriteKeyWord(" STORED")
		} else {
			ctx.WriteKeyWord(" VIRTUAL")
		}
	case ColumnOptionReference:
		if err := n.Refer.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while splicing ColumnOption ReferenceDef: %w", err)
		}
	case ColumnOptionCollate:
		if n.StrValue == "" {
			return errors.New("empty ColumnOption COLLATE")
		}
		ctx.WriteKeyWord("COLLATE ")
		ctx.WritePlain(n.StrValue)
	case ColumnOptionCheck:
		if n.ConstraintName != "" {
			ctx.WriteKeyWord("CONSTRAINT ")
			ctx.WriteName(n.ConstraintName)
			ctx.WritePlain(" ")
		}
		ctx.WriteKeyWord("CHECK")
		ctx.WritePlain("(")
		if err := n.Expr.Restore(ctx); err != nil {
			return err
		}
		ctx.WritePlain(")")
		if n.Enforced {
			ctx.WriteKeyWord(" ENFORCED")
		} else {
			ctx.WriteKeyWord(" NOT ENFORCED")
		}
	case ColumnOptionColumnFormat:
		ctx.WriteKeyWord("COLUMN_FORMAT ")
		ctx.WriteKeyWord(n.StrValue)
	case ColumnOptionStorage:
		ctx.WriteKeyWord("STORAGE ")
		ctx.WriteKeyWord(n.StrValue)
	case ColumnOptionSecondaryEngineAttribute:
		ctx.WriteKeyWord("SECONDARY_ENGINE_ATTRIBUTE")
		ctx.WritePlain(" = ")
		ctx.WriteString(n.StrValue)
	case ColumnOptionSrid:
		ctx.WritePlainf("/*!80003 SRID %d */", n.Srid)
	case ColumnOptionVisibility:
		// VISIBLE or INVISIBLE (MySQL 8.0.23+ invisible columns).
		ctx.WriteKeyWord(n.StrValue)
	case ColumnOptionEngineAttribute:
		ctx.WriteKeyWord("ENGINE_ATTRIBUTE")
		ctx.WritePlain(" = ")
		ctx.WriteString(n.StrValue)
	case ColumnOptionNotSecondary:
		// Excludes the column from the HeatWave secondary engine.
		ctx.WriteKeyWord("NOT SECONDARY")
	default:
		return errors.New("an error occurred while splicing ColumnOption")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *ColumnOption) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ColumnOption)
	if n.Expr != nil {
		node, ok := n.Expr.Accept(v)
		if !ok {
			return n, false
		}
		n.Expr = node.(ExprNode)
	}
	return v.Leave(n)
}

// IndexVisibility is the option for index visibility.
type IndexVisibility int

// IndexVisibility options.
const (
	IndexVisibilityDefault IndexVisibility = iota
	IndexVisibilityVisible
	IndexVisibilityInvisible
)

// IndexOption is the index options.
//
//	  KEY_BLOCK_SIZE [=] value
//	| index_type
//	| WITH PARSER parser_name
//	| COMMENT 'string'
//	| GLOBAL
//
// See http://dev.mysql.com/doc/refman/5.7/en/create-table.html
// with the addition of Global Index
type IndexOption struct {
	node

	KeyBlockSize        uint64
	Tp                  IndexType
	Comment             string
	ParserName          CIStr
	Visibility          IndexVisibility
	EngineAttr          string
	SecondaryEngineAttr string
}

// IsEmpty is true if only default options are given
// and it should not be added to the output
func (n *IndexOption) IsEmpty() bool {
	if n.KeyBlockSize > 0 ||
		n.Tp != IndexTypeInvalid ||
		len(n.ParserName.O) > 0 ||
		n.Comment != "" ||
		n.Visibility != IndexVisibilityDefault ||
		len(n.EngineAttr) > 0 ||
		len(n.SecondaryEngineAttr) > 0 {
		return false
	}
	return true
}

// Restore implements Node interface.
func (n *IndexOption) Restore(ctx *format.RestoreCtx) error {
	hasPrevOption := false

	if n.KeyBlockSize > 0 {
		if hasPrevOption {
			ctx.WritePlain(" ")
		}
		ctx.WriteKeyWord("KEY_BLOCK_SIZE")
		ctx.WritePlainf("=%d", n.KeyBlockSize)
		hasPrevOption = true
	}

	if n.Tp != IndexTypeInvalid {
		if hasPrevOption {
			ctx.WritePlain(" ")
		}
		ctx.WriteKeyWord("USING ")
		ctx.WritePlain(n.Tp.String())
		hasPrevOption = true
	}

	if len(n.ParserName.O) > 0 {
		if hasPrevOption {
			ctx.WritePlain(" ")
		}
		ctx.WriteKeyWord("WITH PARSER ")
		ctx.WriteName(n.ParserName.O)
		hasPrevOption = true
	}

	if n.Comment != "" {
		if hasPrevOption {
			ctx.WritePlain(" ")
		}
		ctx.WriteKeyWord("COMMENT ")
		ctx.WriteString(n.Comment)
		hasPrevOption = true
	}

	if n.Visibility != IndexVisibilityDefault {
		if hasPrevOption {
			ctx.WritePlain(" ")
		}
		switch n.Visibility { //nolint:exhaustive
		case IndexVisibilityVisible:
			ctx.WriteKeyWord("VISIBLE")
		case IndexVisibilityInvisible:
			ctx.WriteKeyWord("INVISIBLE")
		}
		hasPrevOption = true
	}

	if n.EngineAttr != "" {
		if hasPrevOption {
			ctx.WritePlain(" ")
		}
		ctx.WriteKeyWord("ENGINE_ATTRIBUTE")
		ctx.WritePlain(" = ")
		ctx.WriteString(n.EngineAttr)
		hasPrevOption = true
	}

	if n.SecondaryEngineAttr != "" {
		if hasPrevOption {
			ctx.WritePlain(" ")
		}
		ctx.WriteKeyWord("SECONDARY_ENGINE_ATTRIBUTE")
		ctx.WritePlain(" = ")
		ctx.WriteString(n.SecondaryEngineAttr)
		// If a new option is added after, set hasPrevOption = true here.
	}

	return nil
}

// Accept implements Node Accept interface.
func (n *IndexOption) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*IndexOption)
	return v.Leave(n)
}

// ConstraintType is the type for Constraint.
type ConstraintType int

// ConstraintTypes
const (
	ConstraintNoConstraint ConstraintType = iota
	ConstraintPrimaryKey
	ConstraintKey
	ConstraintIndex
	ConstraintUniq
	ConstraintUniqKey
	ConstraintUniqIndex
	ConstraintForeignKey
	// ConstraintFulltext is only used in AST.
	// It will be rewritten into ConstraintIndex after preprocessor phase.
	ConstraintFulltext
	ConstraintCheck
	ConstraintSpatial
)

// Constraint is constraint for table definition.
type Constraint struct {
	node

	Tp   ConstraintType
	Name string

	Keys []*IndexPartSpecification // Used for PRIMARY KEY, UNIQUE, ......

	Refer *ReferenceDef // Used for foreign key.

	Option *IndexOption // Index Options

	Expr ExprNode // Used for Check

	Enforced bool // Used for Check

	InColumn bool // Used for Check

	InColumnName string // Used for Check
	IsEmptyIndex bool   // Used for Check
}

// Restore implements Node interface.
func (n *Constraint) Restore(ctx *format.RestoreCtx) error {
	switch n.Tp { //nolint:exhaustive
	case ConstraintNoConstraint:
		return nil
	case ConstraintPrimaryKey:
		ctx.WriteKeyWord("PRIMARY KEY")
	case ConstraintKey:
		ctx.WriteKeyWord("KEY")
	case ConstraintIndex:
		ctx.WriteKeyWord("INDEX")
	case ConstraintUniq:
		ctx.WriteKeyWord("UNIQUE")
	case ConstraintUniqKey:
		ctx.WriteKeyWord("UNIQUE KEY")
	case ConstraintUniqIndex:
		ctx.WriteKeyWord("UNIQUE INDEX")
	case ConstraintFulltext:
		ctx.WriteKeyWord("FULLTEXT")
	case ConstraintSpatial:
		ctx.WriteKeyWord("SPATIAL")
	case ConstraintCheck:
		if n.Name != "" {
			ctx.WriteKeyWord("CONSTRAINT ")
			ctx.WriteName(n.Name)
			ctx.WritePlain(" ")
		}
		ctx.WriteKeyWord("CHECK")
		ctx.WritePlain("(")
		if err := n.Expr.Restore(ctx); err != nil {
			return err
		}
		ctx.WritePlain(") ")
		if n.Enforced {
			ctx.WriteKeyWord("ENFORCED")
		} else {
			ctx.WriteKeyWord("NOT ENFORCED")
		}
		return nil
	}

	if n.Tp == ConstraintForeignKey {
		ctx.WriteKeyWord("CONSTRAINT ")
		if n.Name != "" {
			ctx.WriteName(n.Name)
			ctx.WritePlain(" ")
		}
		ctx.WriteKeyWord("FOREIGN KEY ")
	} else if n.Name != "" || n.IsEmptyIndex {
		ctx.WritePlain(" ")
		ctx.WriteName(n.Name)
	}

	ctx.WritePlain("(")
	for i, keys := range n.Keys {
		if i > 0 {
			ctx.WritePlain(", ")
		}
		if err := keys.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while splicing Constraint Keys: [%v]: %w", i, err)
		}
	}
	ctx.WritePlain(")")

	if n.Refer != nil {
		ctx.WritePlain(" ")
		if err := n.Refer.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while splicing Constraint Refer: %w", err)
		}
	}

	if n.Option != nil && !n.Option.IsEmpty() {
		ctx.WritePlain(" ")
		if err := n.Option.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while splicing Constraint Option: %w", err)
		}
	}

	return nil
}

// Accept implements Node Accept interface.
func (n *Constraint) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*Constraint)
	for i, val := range n.Keys {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Keys[i] = node.(*IndexPartSpecification)
	}
	if n.Refer != nil {
		node, ok := n.Refer.Accept(v)
		if !ok {
			return n, false
		}
		n.Refer = node.(*ReferenceDef)
	}
	if n.Option != nil {
		node, ok := n.Option.Accept(v)
		if !ok {
			return n, false
		}
		n.Option = node.(*IndexOption)
	}
	if n.Expr != nil {
		node, ok := n.Expr.Accept(v)
		if !ok {
			return n, false
		}
		n.Expr = node.(ExprNode)
	}
	return v.Leave(n)
}

// ColumnDef is used for parsing column definition from SQL.
type ColumnDef struct {
	node

	Name    *ColumnName
	Tp      *types.FieldType
	Options []*ColumnOption
}

// Restore implements Node interface.
func (n *ColumnDef) Restore(ctx *format.RestoreCtx) error {
	if err := n.Name.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while splicing ColumnDef Name: %w", err)
	}
	if n.Tp != nil {
		ctx.WritePlain(" ")
		if err := n.Tp.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while splicing ColumnDef Type: %w", err)
		}
	}
	for i, options := range n.Options {
		ctx.WritePlain(" ")
		if err := options.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while splicing ColumnDef ColumnOption: [%v]: %w", i, err)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *ColumnDef) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ColumnDef)
	node, ok := n.Name.Accept(v)
	if !ok {
		return n, false
	}
	n.Name = node.(*ColumnName)
	for i, val := range n.Options {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Options[i] = node.(*ColumnOption)
	}
	return v.Leave(n)
}

// Validate checks if a column definition is legal.
// For example, generated column definitions that contain such
// column options as `ON UPDATE`, `AUTO_INCREMENT`, `DEFAULT`
// are illegal.
func (n *ColumnDef) Validate() error {
	generatedCol := false
	var illegalOpt4gc string
	for _, opt := range n.Options {
		if opt.Tp == ColumnOptionGenerated {
			generatedCol = true
		}
		msg, found := invalidOptionForGeneratedColumn[opt.Tp]
		if found {
			illegalOpt4gc = msg
		}
	}
	if generatedCol && illegalOpt4gc != "" {
		return ErrWrongUsage.GenByArgs(illegalOpt4gc, "generated column")
	}
	return nil
}

type TemporaryKeyword int

const (
	TemporaryNone TemporaryKeyword = iota
	TemporaryGlobal
	TemporaryLocal
)

// CreateTableStmt is a statement to create a table.
// See https://dev.mysql.com/doc/refman/5.7/en/create-table.html
type CreateTableStmt struct {
	ddlNode

	IfNotExists bool
	TemporaryKeyword
	// Meanless when TemporaryKeyword is not TemporaryGlobal.
	// ON COMMIT DELETE ROWS => true
	// ON COMMIT PRESERVE ROW => false
	OnCommitDelete bool
	Table          *TableName
	ReferTable     *TableName
	Cols           []*ColumnDef
	Constraints    []*Constraint
	Options        []*TableOption
	Partition      *PartitionOptions
	OnDuplicate    OnDuplicateKeyHandlingType
	Select         ResultSetNode
	// StartTransaction is the trailing START TRANSACTION clause that MySQL
	// 8.0.21+ writes to the binary log in place of the SELECT part of
	// CREATE TABLE ... SELECT under row-based replication. Mutually
	// exclusive with Select.
	StartTransaction bool
}

// Restore implements Node interface.
func (n *CreateTableStmt) Restore(ctx *format.RestoreCtx) error {
	switch n.TemporaryKeyword {
	case TemporaryNone:
		ctx.WriteKeyWord("CREATE TABLE ")
	case TemporaryGlobal:
		ctx.WriteKeyWord("CREATE GLOBAL TEMPORARY TABLE ")
	case TemporaryLocal:
		ctx.WriteKeyWord("CREATE TEMPORARY TABLE ")
	}
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}

	if err := n.Table.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while splicing CreateTableStmt Table: %w", err)
	}

	if n.ReferTable != nil {
		ctx.WriteKeyWord(" LIKE ")
		if err := n.ReferTable.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while splicing CreateTableStmt ReferTable: %w", err)
		}
	}
	lenCols := len(n.Cols)
	lenConstraints := len(n.Constraints)
	if lenCols+lenConstraints > 0 {
		ctx.WritePlain(" (")
		for i, col := range n.Cols {
			if i > 0 {
				ctx.WritePlain(",")
			}
			if err := col.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while splicing CreateTableStmt ColumnDef: [%v]: %w", i, err)
			}
		}
		for i, constraint := range n.Constraints {
			if i > 0 || lenCols >= 1 {
				ctx.WritePlain(",")
			}
			if err := constraint.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while splicing CreateTableStmt Constraints: [%v]: %w", i, err)
			}
		}
		ctx.WritePlain(")")
	}

	for i, option := range n.Options {
		ctx.WritePlain(" ")
		if err := option.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while splicing CreateTableStmt TableOption: [%v]: %w", i, err)
		}
	}

	if n.Partition != nil {
		ctx.WritePlain(" ")
		if err := n.Partition.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while splicing CreateTableStmt Partition: %w", err)
		}
	}

	if n.Select != nil {
		switch n.OnDuplicate {
		case OnDuplicateKeyHandlingError:
			ctx.WriteKeyWord(" AS ")
		case OnDuplicateKeyHandlingIgnore:
			ctx.WriteKeyWord(" IGNORE AS ")
		case OnDuplicateKeyHandlingReplace:
			ctx.WriteKeyWord(" REPLACE AS ")
		}

		if err := n.Select.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while splicing CreateTableStmt Select: %w", err)
		}
	}

	if n.StartTransaction {
		ctx.WriteKeyWord(" START TRANSACTION")
	}

	if n.TemporaryKeyword == TemporaryGlobal {
		if n.OnCommitDelete {
			ctx.WriteKeyWord(" ON COMMIT DELETE ROWS")
		} else {
			ctx.WriteKeyWord(" ON COMMIT PRESERVE ROWS")
		}
	}

	return nil
}

// Accept implements Node Accept interface.
func (n *CreateTableStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreateTableStmt)
	node, ok := n.Table.Accept(v)
	if !ok {
		return n, false
	}
	n.Table = node.(*TableName)
	if n.ReferTable != nil {
		node, ok = n.ReferTable.Accept(v)
		if !ok {
			return n, false
		}
		n.ReferTable = node.(*TableName)
	}
	for i, val := range n.Cols {
		node, ok = val.Accept(v)
		if !ok {
			return n, false
		}
		n.Cols[i] = node.(*ColumnDef)
	}
	for i, val := range n.Constraints {
		node, ok = val.Accept(v)
		if !ok {
			return n, false
		}
		n.Constraints[i] = node.(*Constraint)
	}
	if n.Select != nil {
		node, ok := n.Select.Accept(v)
		if !ok {
			return n, false
		}
		n.Select = node.(ResultSetNode)
	}
	if n.Partition != nil {
		node, ok := n.Partition.Accept(v)
		if !ok {
			return n, false
		}
		n.Partition = node.(*PartitionOptions)
	}
	for i, option := range n.Options {
		node, ok = option.Accept(v)
		if !ok {
			return n, false
		}
		n.Options[i] = node.(*TableOption)
	}

	return v.Leave(n)
}

// DropTableStmt is a statement to drop one or more tables.
// See https://dev.mysql.com/doc/refman/5.7/en/drop-table.html
type DropTableStmt struct {
	ddlNode

	IfExists         bool
	Tables           []*TableName
	IsView           bool
	TemporaryKeyword // make sense ONLY if/when IsView == false
}

// Restore implements Node interface.
func (n *DropTableStmt) Restore(ctx *format.RestoreCtx) error {
	if n.IsView {
		ctx.WriteKeyWord("DROP VIEW ")
	} else {
		switch n.TemporaryKeyword {
		case TemporaryNone:
			ctx.WriteKeyWord("DROP TABLE ")
		case TemporaryGlobal:
			ctx.WriteKeyWord("DROP GLOBAL TEMPORARY TABLE ")
		case TemporaryLocal:
			ctx.WriteKeyWord("DROP TEMPORARY TABLE ")
		}
	}
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}

	for index, table := range n.Tables {
		if index != 0 {
			ctx.WritePlain(", ")
		}
		if err := table.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore DropTableStmt.Tables[%d]: %w", index, err)
		}
	}

	return nil
}

// Accept implements Node Accept interface.
func (n *DropTableStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DropTableStmt)
	for i, val := range n.Tables {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Tables[i] = node.(*TableName)
	}
	return v.Leave(n)
}

type OptimizeTableStmt struct {
	ddlNode

	NoWriteToBinLog bool
	Tables          []*TableName
}

func (n *OptimizeTableStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("OPTIMIZE ")
	if n.NoWriteToBinLog {
		ctx.WriteKeyWord("NO_WRITE_TO_BINLOG ")
	}
	ctx.WriteKeyWord("TABLE ")

	for index, table := range n.Tables {
		if index != 0 {
			ctx.WritePlain(", ")
		}
		if err := table.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore OptimizeTableStmt.Tables[%d]: %w", index, err)
		}
	}
	return nil
}

func (n *OptimizeTableStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*OptimizeTableStmt)
	return v.Leave(n)
}

// RepairTableStmt is a statement to repair tables.
// See https://dev.mysql.com/doc/refman/8.4/en/repair-table.html
type RepairTableStmt struct {
	ddlNode

	NoWriteToBinLog bool
	Tables          []*TableName
	Quick           bool
	Extended        bool
	UseFrm          bool
}

// Restore implements Node interface.
func (n *RepairTableStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("REPAIR ")
	if n.NoWriteToBinLog {
		ctx.WriteKeyWord("NO_WRITE_TO_BINLOG ")
	}
	ctx.WriteKeyWord("TABLE ")
	for index, table := range n.Tables {
		if index != 0 {
			ctx.WritePlain(", ")
		}
		if err := table.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore RepairTableStmt.Tables[%d]: %w", index, err)
		}
	}
	if n.Quick {
		ctx.WriteKeyWord(" QUICK")
	}
	if n.Extended {
		ctx.WriteKeyWord(" EXTENDED")
	}
	if n.UseFrm {
		ctx.WriteKeyWord(" USE_FRM")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *RepairTableStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*RepairTableStmt)
	for i, val := range n.Tables {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Tables[i] = node.(*TableName)
	}
	return v.Leave(n)
}

// RenameTableStmt is a statement to rename a table.
// See http://dev.mysql.com/doc/refman/5.7/en/rename-table.html
type RenameTableStmt struct {
	ddlNode

	TableToTables []*TableToTable
}

// Restore implements Node interface.
func (n *RenameTableStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("RENAME TABLE ")
	for index, table2table := range n.TableToTables {
		if index != 0 {
			ctx.WritePlain(", ")
		}
		if err := table2table.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore RenameTableStmt.TableToTables: %w", err)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *RenameTableStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*RenameTableStmt)

	for i, t := range n.TableToTables {
		node, ok := t.Accept(v)
		if !ok {
			return n, false
		}
		n.TableToTables[i] = node.(*TableToTable)
	}

	return v.Leave(n)
}

// TableToTable represents renaming old table to new table used in RenameTableStmt.
type TableToTable struct {
	node
	OldTable *TableName
	NewTable *TableName
}

// Restore implements Node interface.
func (n *TableToTable) Restore(ctx *format.RestoreCtx) error {
	if err := n.OldTable.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore TableToTable.OldTable: %w", err)
	}
	ctx.WriteKeyWord(" TO ")
	if err := n.NewTable.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore TableToTable.NewTable: %w", err)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *TableToTable) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*TableToTable)
	node, ok := n.OldTable.Accept(v)
	if !ok {
		return n, false
	}
	n.OldTable = node.(*TableName)
	node, ok = n.NewTable.Accept(v)
	if !ok {
		return n, false
	}
	n.NewTable = node.(*TableName)
	return v.Leave(n)
}

// CreateViewStmt is a statement to create a View.
// See https://dev.mysql.com/doc/refman/5.7/en/create-view.html
type CreateViewStmt struct {
	ddlNode

	OrReplace   bool
	IfNotExists bool
	ViewName    *TableName
	Cols        []CIStr
	Select      StmtNode
	SchemaCols  []CIStr
	Algorithm   ViewAlgorithm
	Definer     *auth.UserIdentity
	Security    ViewSecurity
	CheckOption ViewCheckOption
}

// Restore implements Node interface.
func (n *CreateViewStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE ")
	if n.OrReplace {
		ctx.WriteKeyWord("OR REPLACE ")
	}
	ctx.WriteKeyWord("ALGORITHM")
	ctx.WritePlain(" = ")
	ctx.WriteKeyWord(n.Algorithm.String())
	ctx.WriteKeyWord(" DEFINER")
	ctx.WritePlain(" = ")

	// todo Use n.Definer.Restore(ctx) to replace this part
	if n.Definer.CurrentUser {
		ctx.WriteKeyWord("current_user")
	} else {
		ctx.WriteName(n.Definer.Username)
		if n.Definer.Hostname != "" {
			ctx.WritePlain("@")
			ctx.WriteName(n.Definer.Hostname)
		}
	}

	ctx.WriteKeyWord(" SQL SECURITY ")
	ctx.WriteKeyWord(n.Security.String())
	ctx.WriteKeyWord(" VIEW ")
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}

	if err := n.ViewName.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while create CreateViewStmt.ViewName: %w", err)
	}

	for i, col := range n.Cols {
		if i == 0 {
			ctx.WritePlain(" (")
		} else {
			ctx.WritePlain(",")
		}
		ctx.WriteName(col.O)
		if i == len(n.Cols)-1 {
			ctx.WritePlain(")")
		}
	}

	ctx.WriteKeyWord(" AS ")

	if err := n.Select.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while create CreateViewStmt.Select: %w", err)
	}

	if n.CheckOption != CheckOptionCascaded {
		ctx.WriteKeyWord(" WITH ")
		ctx.WriteKeyWord(n.CheckOption.String())
		ctx.WriteKeyWord(" CHECK OPTION")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *CreateViewStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreateViewStmt)
	node, ok := n.ViewName.Accept(v)
	if !ok {
		return n, false
	}
	n.ViewName = node.(*TableName)
	selnode, ok := n.Select.Accept(v)
	if !ok {
		return n, false
	}
	n.Select = selnode.(StmtNode)
	return v.Leave(n)
}

// CreateJSONDualityViewStmt is a statement to create a JSON relational
// duality view. See https://dev.mysql.com/doc/refman/9.4/en/create-json-duality-view.html
type CreateJSONDualityViewStmt struct {
	ddlNode

	OrReplace   bool
	Algorithm   ViewAlgorithm
	Definer     *auth.UserIdentity
	Security    ViewSecurity
	Relational  bool
	IfNotExists bool
	ViewName    *TableName
	Select      StmtNode
}

// Restore implements Node interface.
func (n *CreateJSONDualityViewStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE ")
	if n.OrReplace {
		ctx.WriteKeyWord("OR REPLACE ")
	}
	// The view attributes are only written when they differ from what an
	// absent clause parses to, so unattributed statements restore without
	// them.
	if n.Algorithm != AlgorithmUndefined {
		ctx.WriteKeyWord("ALGORITHM")
		ctx.WritePlain(" = ")
		ctx.WriteKeyWord(n.Algorithm.String())
		ctx.WritePlain(" ")
	}
	if n.Definer != nil && !n.Definer.CurrentUser {
		ctx.WriteKeyWord("DEFINER")
		ctx.WritePlain(" = ")
		ctx.WriteName(n.Definer.Username)
		if n.Definer.Hostname != "" {
			ctx.WritePlain("@")
			ctx.WriteName(n.Definer.Hostname)
		}
		ctx.WritePlain(" ")
	}
	if n.Security != SecurityDefiner {
		ctx.WriteKeyWord("SQL SECURITY ")
		ctx.WriteKeyWord(n.Security.String())
		ctx.WritePlain(" ")
	}
	ctx.WriteKeyWord("JSON ")
	if n.Relational {
		ctx.WriteKeyWord("RELATIONAL ")
	}
	ctx.WriteKeyWord("DUALITY VIEW ")
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}
	if err := n.ViewName.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore CreateJSONDualityViewStmt.ViewName: %w", err)
	}
	ctx.WriteKeyWord(" AS ")
	if err := n.Select.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore CreateJSONDualityViewStmt.Select: %w", err)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *CreateJSONDualityViewStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreateJSONDualityViewStmt)
	node, ok := n.ViewName.Accept(v)
	if !ok {
		return n, false
	}
	n.ViewName = node.(*TableName)
	selnode, ok := n.Select.Accept(v)
	if !ok {
		return n, false
	}
	n.Select = selnode.(StmtNode)
	return v.Leave(n)
}

// AlterViewStmt is a statement to alter a View.
// See https://dev.mysql.com/doc/refman/8.4/en/alter-view.html
type AlterViewStmt struct {
	ddlNode

	ViewName    *TableName
	Cols        []CIStr
	Select      StmtNode
	Algorithm   ViewAlgorithm
	Definer     *auth.UserIdentity
	Security    ViewSecurity
	CheckOption ViewCheckOption
}

// Restore implements Node interface.
func (n *AlterViewStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ALTER ")
	ctx.WriteKeyWord("ALGORITHM")
	ctx.WritePlain(" = ")
	ctx.WriteKeyWord(n.Algorithm.String())
	ctx.WriteKeyWord(" DEFINER")
	ctx.WritePlain(" = ")
	if n.Definer.CurrentUser {
		ctx.WriteKeyWord("current_user")
	} else {
		ctx.WriteName(n.Definer.Username)
		if n.Definer.Hostname != "" {
			ctx.WritePlain("@")
			ctx.WriteName(n.Definer.Hostname)
		}
	}
	ctx.WriteKeyWord(" SQL SECURITY ")
	ctx.WriteKeyWord(n.Security.String())
	ctx.WriteKeyWord(" VIEW ")
	if err := n.ViewName.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore AlterViewStmt.ViewName: %w", err)
	}
	for i, col := range n.Cols {
		if i == 0 {
			ctx.WritePlain(" (")
		} else {
			ctx.WritePlain(",")
		}
		ctx.WriteName(col.O)
		if i == len(n.Cols)-1 {
			ctx.WritePlain(")")
		}
	}
	ctx.WriteKeyWord(" AS ")
	if err := n.Select.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore AlterViewStmt.Select: %w", err)
	}
	if n.CheckOption != CheckOptionCascaded {
		ctx.WriteKeyWord(" WITH ")
		ctx.WriteKeyWord(n.CheckOption.String())
		ctx.WriteKeyWord(" CHECK OPTION")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *AlterViewStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AlterViewStmt)
	node, ok := n.ViewName.Accept(v)
	if !ok {
		return n, false
	}
	n.ViewName = node.(*TableName)
	selnode, ok := n.Select.Accept(v)
	if !ok {
		return n, false
	}
	n.Select = selnode.(StmtNode)
	return v.Leave(n)
}

// SRSAttributeType is the attribute kind in CREATE SPATIAL REFERENCE SYSTEM.
type SRSAttributeType int

// SRS attribute types.
const (
	SRSAttrName SRSAttributeType = iota
	SRSAttrDefinition
	SRSAttrOrganization
	SRSAttrDescription
)

// SRSAttribute is one attribute of CREATE SPATIAL REFERENCE SYSTEM.
type SRSAttribute struct {
	Tp    SRSAttributeType
	Value string
	OrgID uint64 // ORGANIZATION ... IDENTIFIED BY
}

// Restore implements Node interface.
func (n *SRSAttribute) Restore(ctx *format.RestoreCtx) error {
	switch n.Tp {
	case SRSAttrName:
		ctx.WriteKeyWord("NAME ")
	case SRSAttrDefinition:
		ctx.WriteKeyWord("DEFINITION ")
	case SRSAttrOrganization:
		ctx.WriteKeyWord("ORGANIZATION ")
	case SRSAttrDescription:
		ctx.WriteKeyWord("DESCRIPTION ")
	default:
		return fmt.Errorf("invalid SRSAttributeType: %d", n.Tp)
	}
	ctx.WriteString(n.Value)
	if n.Tp == SRSAttrOrganization {
		ctx.WriteKeyWord(" IDENTIFIED BY ")
		ctx.WritePlainf("%d", n.OrgID)
	}
	return nil
}

// CreateSpatialRefSysStmt is a statement to create a spatial reference system.
// See https://dev.mysql.com/doc/refman/8.4/en/create-spatial-reference-system.html
type CreateSpatialRefSysStmt struct {
	ddlNode

	OrReplace   bool
	IfNotExists bool
	SRID        uint64
	Attributes  []*SRSAttribute
}

// Restore implements Node interface.
func (n *CreateSpatialRefSysStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE ")
	if n.OrReplace {
		ctx.WriteKeyWord("OR REPLACE ")
	}
	ctx.WriteKeyWord("SPATIAL REFERENCE SYSTEM ")
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}
	ctx.WritePlainf("%d", n.SRID)
	for i, attr := range n.Attributes {
		ctx.WritePlain(" ")
		if err := attr.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore CreateSpatialRefSysStmt.Attributes[%d]: %w", i, err)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *CreateSpatialRefSysStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreateSpatialRefSysStmt)
	return v.Leave(n)
}

// DropSpatialRefSysStmt is a statement to drop a spatial reference system.
// See https://dev.mysql.com/doc/refman/8.4/en/drop-spatial-reference-system.html
type DropSpatialRefSysStmt struct {
	ddlNode

	IfExists bool
	SRID     uint64
}

// Restore implements Node interface.
func (n *DropSpatialRefSysStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DROP SPATIAL REFERENCE SYSTEM ")
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}
	ctx.WritePlainf("%d", n.SRID)
	return nil
}

// Accept implements Node Accept interface.
func (n *DropSpatialRefSysStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DropSpatialRefSysStmt)
	return v.Leave(n)
}

// CreateLibraryStmt is a statement to create a library (MySQL 9.x).
// See https://dev.mysql.com/doc/refman/9.4/en/create-library.html
type CreateLibraryStmt struct {
	ddlNode

	IfNotExists bool
	Library     *TableName
	HasComment  bool
	Comment     string
	Language    string
	Body        string
}

// Restore implements Node interface.
func (n *CreateLibraryStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE LIBRARY ")
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}
	if err := n.Library.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore CreateLibraryStmt.Library: %w", err)
	}
	if n.HasComment {
		ctx.WriteKeyWord(" COMMENT ")
		ctx.WriteString(n.Comment)
	}
	ctx.WriteKeyWord(" LANGUAGE ")
	ctx.WriteKeyWord(n.Language)
	ctx.WriteKeyWord(" AS ")
	ctx.WriteString(n.Body)
	return nil
}

// Accept implements Node Accept interface.
func (n *CreateLibraryStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreateLibraryStmt)
	node, ok := n.Library.Accept(v)
	if !ok {
		return n, false
	}
	n.Library = node.(*TableName)
	return v.Leave(n)
}

// AlterLibraryStmt is a statement to alter a library's comment (MySQL 9.x).
// See https://dev.mysql.com/doc/refman/9.4/en/alter-library.html
type AlterLibraryStmt struct {
	ddlNode

	Library *TableName
	Comment string
}

// Restore implements Node interface.
func (n *AlterLibraryStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ALTER LIBRARY ")
	if err := n.Library.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore AlterLibraryStmt.Library: %w", err)
	}
	ctx.WriteKeyWord(" COMMENT ")
	ctx.WriteString(n.Comment)
	return nil
}

// Accept implements Node Accept interface.
func (n *AlterLibraryStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AlterLibraryStmt)
	node, ok := n.Library.Accept(v)
	if !ok {
		return n, false
	}
	n.Library = node.(*TableName)
	return v.Leave(n)
}

// DropLibraryStmt is a statement to drop a library (MySQL 9.x).
// See https://dev.mysql.com/doc/refman/9.4/en/drop-library.html
type DropLibraryStmt struct {
	ddlNode

	IfExists bool
	Library  *TableName
}

// Restore implements Node interface.
func (n *DropLibraryStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DROP LIBRARY ")
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}
	if err := n.Library.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore DropLibraryStmt.Library: %w", err)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *DropLibraryStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DropLibraryStmt)
	node, ok := n.Library.Accept(v)
	if !ok {
		return n, false
	}
	n.Library = node.(*TableName)
	return v.Leave(n)
}

// IndexLockAndAlgorithm stores the algorithm option and the lock option.
type IndexLockAndAlgorithm struct {
	node

	LockTp      LockType
	AlgorithmTp AlgorithmType
}

// Restore implements Node interface.
func (n *IndexLockAndAlgorithm) Restore(ctx *format.RestoreCtx) error {
	hasPrevOption := false
	if n.AlgorithmTp != AlgorithmTypeDefault {
		ctx.WriteKeyWord("ALGORITHM")
		ctx.WritePlain(" = ")
		ctx.WriteKeyWord(n.AlgorithmTp.String())
		hasPrevOption = true
	}

	if n.LockTp != LockTypeDefault {
		if hasPrevOption {
			ctx.WritePlain(" ")
		}
		ctx.WriteKeyWord("LOCK")
		ctx.WritePlain(" = ")
		ctx.WriteKeyWord(n.LockTp.String())
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *IndexLockAndAlgorithm) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*IndexLockAndAlgorithm)
	return v.Leave(n)
}

// IndexKeyType is the type for index key.
type IndexKeyType int

// Index key types.
const (
	IndexKeyTypeNone IndexKeyType = iota
	IndexKeyTypeUnique
	IndexKeyTypeSpatial
	// IndexKeyTypeFulltext is only used in AST.
	// It will be rewritten into IndexKeyTypeFulltext after preprocessor phase.
	IndexKeyTypeFulltext
)

// CreateIndexStmt is a statement to create an index.
// See https://dev.mysql.com/doc/refman/5.7/en/create-index.html
type CreateIndexStmt struct {
	ddlNode

	IndexName               string
	Table                   *TableName
	IndexPartSpecifications []*IndexPartSpecification
	IndexOption             *IndexOption
	KeyType                 IndexKeyType
	LockAlg                 *IndexLockAndAlgorithm
}

// Restore implements Node interface.
func (n *CreateIndexStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE ")
	switch n.KeyType { //nolint:exhaustive
	case IndexKeyTypeUnique:
		ctx.WriteKeyWord("UNIQUE ")
	case IndexKeyTypeSpatial:
		ctx.WriteKeyWord("SPATIAL ")
	case IndexKeyTypeFulltext:
		ctx.WriteKeyWord("FULLTEXT ")
	}
	ctx.WriteKeyWord("INDEX ")
	ctx.WriteName(n.IndexName)
	ctx.WriteKeyWord(" ON ")
	if err := n.Table.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore CreateIndexStmt.Table: %w", err)
	}

	ctx.WritePlain(" (")
	for i, indexColName := range n.IndexPartSpecifications {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := indexColName.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore CreateIndexStmt.IndexPartSpecifications: [%v]: %w", i, err)
		}
	}
	ctx.WritePlain(")")

	if n.IndexOption != nil && !n.IndexOption.IsEmpty() {
		ctx.WritePlain(" ")
		if err := n.IndexOption.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore CreateIndexStmt.IndexOption: %w", err)
		}
	}

	if n.LockAlg != nil {
		ctx.WritePlain(" ")
		if err := n.LockAlg.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore CreateIndexStmt.LockAlg: %w", err)
		}
	}

	return nil
}

// Accept implements Node Accept interface.
func (n *CreateIndexStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreateIndexStmt)
	node, ok := n.Table.Accept(v)
	if !ok {
		return n, false
	}
	n.Table = node.(*TableName)
	for i, val := range n.IndexPartSpecifications {
		node, ok = val.Accept(v)
		if !ok {
			return n, false
		}
		n.IndexPartSpecifications[i] = node.(*IndexPartSpecification)
	}
	if n.IndexOption != nil {
		node, ok := n.IndexOption.Accept(v)
		if !ok {
			return n, false
		}
		n.IndexOption = node.(*IndexOption)
	}
	if n.LockAlg != nil {
		node, ok := n.LockAlg.Accept(v)
		if !ok {
			return n, false
		}
		n.LockAlg = node.(*IndexLockAndAlgorithm)
	}
	return v.Leave(n)
}

// DropIndexStmt is a statement to drop the index.
// See https://dev.mysql.com/doc/refman/5.7/en/drop-index.html
type DropIndexStmt struct {
	ddlNode

	IndexName string
	Table     *TableName
	LockAlg   *IndexLockAndAlgorithm
}

// Restore implements Node interface.
func (n *DropIndexStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DROP INDEX ")
	ctx.WriteName(n.IndexName)
	ctx.WriteKeyWord(" ON ")

	if err := n.Table.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while add index: %w", err)
	}

	if n.LockAlg != nil {
		ctx.WritePlain(" ")
		if err := n.LockAlg.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore CreateIndexStmt.LockAlg: %w", err)
		}
	}

	return nil
}

// Accept implements Node Accept interface.
func (n *DropIndexStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DropIndexStmt)
	node, ok := n.Table.Accept(v)
	if !ok {
		return n, false
	}
	n.Table = node.(*TableName)
	if n.LockAlg != nil {
		node, ok := n.LockAlg.Accept(v)
		if !ok {
			return n, false
		}
		n.LockAlg = node.(*IndexLockAndAlgorithm)
	}
	return v.Leave(n)
}

// LockTablesStmt is a statement to lock tables.
type LockTablesStmt struct {
	ddlNode

	TableLocks []TableLock
}

// TableLock contains the table name and lock type.
type TableLock struct {
	Table *TableName
	// Alias is the optional [AS] alias of the locked table.
	Alias CIStr
	Type  TableLockType
}

// Accept implements Node Accept interface.
func (n *LockTablesStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*LockTablesStmt)
	for i := range n.TableLocks {
		node, ok := n.TableLocks[i].Table.Accept(v)
		if !ok {
			return n, false
		}
		n.TableLocks[i].Table = node.(*TableName)
	}
	return v.Leave(n)
}

// Restore implements Node interface.
func (n *LockTablesStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("LOCK TABLES ")
	for i, tl := range n.TableLocks {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := tl.Table.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while add index: %w", err)
		}
		if tl.Alias.O != "" {
			ctx.WriteKeyWord(" AS ")
			ctx.WriteName(tl.Alias.O)
		}
		ctx.WriteKeyWord(" " + tl.Type.String())
	}
	return nil
}

// UnlockTablesStmt is a statement to unlock tables.
type UnlockTablesStmt struct {
	ddlNode
}

// Accept implements Node Accept interface.
func (n *UnlockTablesStmt) Accept(v Visitor) (Node, bool) {
	_, _ = v.Enter(n)
	return v.Leave(n)
}

// Restore implements Node interface.
func (n *UnlockTablesStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("UNLOCK TABLES")
	return nil
}

type StatsOptionType int

const (
	StatsOptionBuckets StatsOptionType = 0x5000 + iota
	StatsOptionTopN
	StatsOptionColsChoice
	StatsOptionColList
	StatsOptionSampleRate
)

// TableOptionType is the type for TableOption
type TableOptionType int

// TableOption types.
const (
	TableOptionNone TableOptionType = iota
	TableOptionEngine
	TableOptionCharset
	TableOptionCollate
	TableOptionAutoIdCache //nolint:revive
	TableOptionAutoIncrement
	TableOptionComment
	TableOptionAvgRowLength
	TableOptionCheckSum
	TableOptionCompression
	TableOptionConnection
	TableOptionPassword
	TableOptionKeyBlockSize
	TableOptionMaxRows
	TableOptionMinRows
	TableOptionDelayKeyWrite
	TableOptionRowFormat
	TableOptionStatsPersistent
	TableOptionStatsAutoRecalc
	TableOptionPackKeys
	TableOptionTablespace
	TableOptionNodegroup
	TableOptionDataDirectory
	TableOptionIndexDirectory
	TableOptionStorageMedia
	TableOptionStatsSamplePages
	TableOptionSecondaryEngine
	TableOptionSecondaryEngineNull
	TableOptionInsertMethod
	TableOptionUnion
	TableOptionEncryption
	TableOptionEngineAttribute
	TableOptionSecondaryEngineAttribute
	TableOptionAutoextendSize
)

// RowFormat types
const (
	RowFormatDefault uint64 = iota + 1
	RowFormatDynamic
	RowFormatFixed
	RowFormatCompressed
	RowFormatRedundant
	RowFormatCompact
)

// OnDuplicateKeyHandlingType is the option that handle unique key values in 'CREATE TABLE ... SELECT' or `LOAD DATA`.
// See https://dev.mysql.com/doc/refman/5.7/en/create-table-select.html
// See https://dev.mysql.com/doc/refman/5.7/en/load-data.html
type OnDuplicateKeyHandlingType int

// OnDuplicateKeyHandling types
const (
	OnDuplicateKeyHandlingError OnDuplicateKeyHandlingType = iota
	OnDuplicateKeyHandlingIgnore
	OnDuplicateKeyHandlingReplace
)

const (
	TableOptionCharsetWithoutConvertTo uint64 = 0
	TableOptionCharsetWithConvertTo    uint64 = 1
)

const (
	// TableAffinityLevelNone means no affinity.
	TableAffinityLevelNone = "none"
	// TableAffinityLevelTable means table-level affinity.
	TableAffinityLevelTable = "table"
	// TableAffinityLevelPartition means partition-level affinity.
	TableAffinityLevelPartition = "partition"
)

// TableOption is used for parsing table option from SQL.
type TableOption struct {
	node
	Tp            TableOptionType
	Default       bool
	StrValue      string
	UintValue     uint64
	BoolValue     bool
	TimeUnitValue *TimeUnitExpr
	Value         *ValueExpr
	TableNames    []*TableName
	ColumnName    *ColumnName
}

func (n *TableOption) Restore(ctx *format.RestoreCtx) error {
	switch n.Tp { //nolint:exhaustive
	case TableOptionEngine:
		ctx.WriteKeyWord("ENGINE ")
		ctx.WritePlain("= ")
		if n.StrValue != "" {
			ctx.WritePlain(n.StrValue)
		} else {
			ctx.WritePlain("''")
		}
	case TableOptionCharset:
		if n.UintValue == TableOptionCharsetWithConvertTo {
			ctx.WriteKeyWord("CONVERT TO ")
		} else {
			ctx.WriteKeyWord("DEFAULT ")
		}
		ctx.WriteKeyWord("CHARACTER SET ")
		if n.UintValue == TableOptionCharsetWithoutConvertTo {
			ctx.WriteKeyWord("= ")
		}
		if n.Default {
			ctx.WriteKeyWord("DEFAULT")
		} else {
			ctx.WriteKeyWord(n.StrValue)
		}
	case TableOptionCollate:
		ctx.WriteKeyWord("DEFAULT COLLATE ")
		ctx.WritePlain("= ")
		ctx.WriteKeyWord(n.StrValue)
	case TableOptionAutoIncrement:
		ctx.WriteKeyWord("AUTO_INCREMENT ")
		ctx.WritePlain("= ")
		ctx.WritePlainf("%d", n.UintValue)
	case TableOptionComment:
		ctx.WriteKeyWord("COMMENT ")
		ctx.WritePlain("= ")
		ctx.WriteString(n.StrValue)
	case TableOptionAvgRowLength:
		ctx.WriteKeyWord("AVG_ROW_LENGTH ")
		ctx.WritePlain("= ")
		ctx.WritePlainf("%d", n.UintValue)
	case TableOptionCheckSum:
		ctx.WriteKeyWord("CHECKSUM ")
		ctx.WritePlain("= ")
		ctx.WritePlainf("%d", n.UintValue)
	case TableOptionCompression:
		ctx.WriteKeyWord("COMPRESSION ")
		ctx.WritePlain("= ")
		ctx.WriteString(n.StrValue)
	case TableOptionConnection:
		ctx.WriteKeyWord("CONNECTION ")
		ctx.WritePlain("= ")
		ctx.WriteString(n.StrValue)
	case TableOptionPassword:
		ctx.WriteKeyWord("PASSWORD ")
		ctx.WritePlain("= ")
		ctx.WriteString(n.StrValue)
	case TableOptionKeyBlockSize:
		ctx.WriteKeyWord("KEY_BLOCK_SIZE ")
		ctx.WritePlain("= ")
		ctx.WritePlainf("%d", n.UintValue)
	case TableOptionMaxRows:
		ctx.WriteKeyWord("MAX_ROWS ")
		ctx.WritePlain("= ")
		ctx.WritePlainf("%d", n.UintValue)
	case TableOptionMinRows:
		ctx.WriteKeyWord("MIN_ROWS ")
		ctx.WritePlain("= ")
		ctx.WritePlainf("%d", n.UintValue)
	case TableOptionDelayKeyWrite:
		ctx.WriteKeyWord("DELAY_KEY_WRITE ")
		ctx.WritePlain("= ")
		ctx.WritePlainf("%d", n.UintValue)
	case TableOptionRowFormat:
		ctx.WriteKeyWord("ROW_FORMAT ")
		ctx.WritePlain("= ")
		switch n.UintValue {
		case RowFormatDefault:
			ctx.WriteKeyWord("DEFAULT")
		case RowFormatDynamic:
			ctx.WriteKeyWord("DYNAMIC")
		case RowFormatFixed:
			ctx.WriteKeyWord("FIXED")
		case RowFormatCompressed:
			ctx.WriteKeyWord("COMPRESSED")
		case RowFormatRedundant:
			ctx.WriteKeyWord("REDUNDANT")
		case RowFormatCompact:
			ctx.WriteKeyWord("COMPACT")
		default:
			return fmt.Errorf("invalid TableOption: TableOptionRowFormat: %d", n.UintValue)
		}
	case TableOptionStatsPersistent:
		// TODO: not support
		ctx.WriteKeyWord("STATS_PERSISTENT ")
		ctx.WritePlain("= ")
		ctx.WriteKeyWord("DEFAULT")
		ctx.WritePlain(" /* TableOptionStatsPersistent is not supported */ ")
	case TableOptionStatsAutoRecalc:
		ctx.WriteKeyWord("STATS_AUTO_RECALC ")
		ctx.WritePlain("= ")
		if n.Default {
			ctx.WriteKeyWord("DEFAULT")
		} else {
			ctx.WritePlainf("%d", n.UintValue)
		}
	case TableOptionPackKeys:
		// TODO: not support
		ctx.WriteKeyWord("PACK_KEYS ")
		ctx.WritePlain("= ")
		ctx.WriteKeyWord("DEFAULT")
		ctx.WritePlain(" /* TableOptionPackKeys is not supported */ ")
	case TableOptionTablespace:
		ctx.WriteKeyWord("TABLESPACE ")
		ctx.WritePlain("= ")
		ctx.WriteName(n.StrValue)
	case TableOptionNodegroup:
		ctx.WriteKeyWord("NODEGROUP ")
		ctx.WritePlainf("= %d", n.UintValue)
	case TableOptionDataDirectory:
		ctx.WriteKeyWord("DATA DIRECTORY ")
		ctx.WritePlain("= ")
		ctx.WriteString(n.StrValue)
	case TableOptionIndexDirectory:
		ctx.WriteKeyWord("INDEX DIRECTORY ")
		ctx.WritePlain("= ")
		ctx.WriteString(n.StrValue)
	case TableOptionStorageMedia:
		ctx.WriteKeyWord("STORAGE ")
		ctx.WriteKeyWord(n.StrValue)
	case TableOptionStatsSamplePages:
		ctx.WriteKeyWord("STATS_SAMPLE_PAGES ")
		ctx.WritePlain("= ")
		if n.Default {
			ctx.WriteKeyWord("DEFAULT")
		} else {
			ctx.WritePlainf("%d", n.UintValue)
		}
	case TableOptionSecondaryEngine:
		ctx.WriteKeyWord("SECONDARY_ENGINE ")
		ctx.WritePlain("= ")
		ctx.WriteString(n.StrValue)
	case TableOptionSecondaryEngineNull:
		ctx.WriteKeyWord("SECONDARY_ENGINE ")
		ctx.WritePlain("= ")
		ctx.WriteKeyWord("NULL")
	case TableOptionSecondaryEngineAttribute:
		ctx.WriteKeyWord("SECONDARY_ENGINE_ATTRIBUTE ")
		ctx.WritePlain("= ")
		ctx.WriteString(n.StrValue)
	case TableOptionInsertMethod:
		ctx.WriteKeyWord("INSERT_METHOD ")
		ctx.WritePlain("= ")
		ctx.WriteKeyWord(n.StrValue)
	case TableOptionUnion:
		ctx.WriteKeyWord("UNION ")
		ctx.WritePlain("= (")
		for i, tableName := range n.TableNames {
			if i != 0 {
				ctx.WritePlain(",")
			}
			if err := tableName.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while splicing TableOption.TableNames: %w", err)
			}
		}
		ctx.WritePlain(")")
	case TableOptionEncryption:
		ctx.WriteKeyWord("ENCRYPTION ")
		ctx.WritePlain("= ")
		ctx.WriteString(n.StrValue)
	case TableOptionAutoextendSize:
		ctx.WriteKeyWord("AUTOEXTEND_SIZE ")
		ctx.WritePlain("= ")
		if n.StrValue != "" {
			ctx.WritePlain(n.StrValue) // e.g. '4M'
		} else {
			ctx.WritePlainf("%d", n.UintValue)
		}

	default:
		return fmt.Errorf("invalid TableOption: %d", n.Tp)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *TableOption) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*TableOption)
	if n.Value != nil {
		node, ok := n.Value.Accept(v)
		if !ok {
			return n, false
		}
		n.Value = node.(*ValueExpr)
	}
	if n.TimeUnitValue != nil {
		node, ok := n.TimeUnitValue.Accept(v)
		if !ok {
			return n, false
		}
		n.TimeUnitValue = node.(*TimeUnitExpr)
	}
	return v.Leave(n)
}

// ColumnPositionType is the type for ColumnPosition.
type ColumnPositionType int

// ColumnPosition Types
const (
	ColumnPositionNone ColumnPositionType = iota
	ColumnPositionFirst
	ColumnPositionAfter
)

// ColumnPosition represent the position of the newly added column
type ColumnPosition struct {
	node
	// Tp is either ColumnPositionNone, ColumnPositionFirst or ColumnPositionAfter.
	Tp ColumnPositionType
	// RelativeColumn is the column the newly added column after if type is ColumnPositionAfter
	RelativeColumn *ColumnName
}

// Restore implements Node interface.
func (n *ColumnPosition) Restore(ctx *format.RestoreCtx) error {
	switch n.Tp {
	case ColumnPositionNone:
		// do nothing
	case ColumnPositionFirst:
		ctx.WriteKeyWord("FIRST")
	case ColumnPositionAfter:
		ctx.WriteKeyWord("AFTER ")
		if err := n.RelativeColumn.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore ColumnPosition.RelativeColumn: %w", err)
		}
	default:
		return fmt.Errorf("invalid ColumnPositionType: %d", n.Tp)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *ColumnPosition) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ColumnPosition)
	if n.RelativeColumn != nil {
		node, ok := n.RelativeColumn.Accept(v)
		if !ok {
			return n, false
		}
		n.RelativeColumn = node.(*ColumnName)
	}
	return v.Leave(n)
}

// AlterTableType is the type for AlterTableSpec.
type AlterTableType int

// AlterTable types.
const (
	AlterTableOption AlterTableType = iota + 1
	AlterTableAddColumns
	AlterTableAddConstraint
	AlterTableDropColumn
	AlterTableDropPrimaryKey
	AlterTableDropIndex
	AlterTableDropForeignKey
	AlterTableModifyColumn
	AlterTableChangeColumn
	AlterTableRenameColumn
	AlterTableRenameTable
	AlterTableAlterColumn
	AlterTableLock
	AlterTableAlgorithm
	AlterTableRenameIndex
	AlterTableForce
	AlterTableAddPartitions
	AlterTableCoalescePartitions
	AlterTableDropPartition
	AlterTableTruncatePartition
	AlterTablePartition
	AlterTableEnableKeys
	AlterTableDisableKeys
	AlterTableRemovePartitioning
	AlterTableWithValidation
	AlterTableWithoutValidation
	AlterTableSecondaryLoad
	AlterTableSecondaryUnload
	AlterTableRebuildPartition
	AlterTableReorganizePartition
	AlterTableCheckPartitions
	AlterTableAnalyzePartitions
	AlterTableExchangePartition
	AlterTableOptimizePartition
	AlterTableRepairPartition
	AlterTableImportPartitionTablespace
	AlterTableDiscardPartitionTablespace
	AlterTableAlterCheck
	AlterTableDropCheck
	// AlterTableDropConstraint is DROP CONSTRAINT, which MySQL accepts for a
	// CHECK, FOREIGN KEY or UNIQUE constraint of that name. It is kept
	// distinct from AlterTableDropCheck (which only drops a check constraint)
	// so that restoring the statement reproduces the keyword the user wrote:
	// rendering DROP CONSTRAINT as DROP CHECK turns a statement MySQL accepts
	// into one it rejects with error 3821.
	AlterTableDropConstraint
	AlterTableImportTablespace
	AlterTableDiscardTablespace
	AlterTableIndexInvisible
	AlterTableAlterColumnVisibility
	// TODO: Add more actions
	AlterTableOrderByColumns
)

// LockType is the type for AlterTableSpec.
// See https://dev.mysql.com/doc/refman/5.7/en/alter-table.html#alter-table-concurrency
type LockType byte

func (n LockType) String() string {
	switch n {
	case LockTypeNone:
		return "NONE"
	case LockTypeDefault:
		return "DEFAULT"
	case LockTypeShared:
		return "SHARED"
	case LockTypeExclusive:
		return "EXCLUSIVE"
	}
	return ""
}

// Lock Types.
const (
	LockTypeNone LockType = iota + 1
	LockTypeDefault
	LockTypeShared
	LockTypeExclusive
)

// AlgorithmType is the algorithm of the DDL operations.
// See https://dev.mysql.com/doc/refman/8.0/en/alter-table.html#alter-table-performance.
type AlgorithmType byte

// DDL algorithms.
const (
	AlgorithmTypeDefault AlgorithmType = iota
	AlgorithmTypeCopy
	AlgorithmTypeInplace
	AlgorithmTypeInstant
)

func (a AlgorithmType) String() string {
	switch a {
	case AlgorithmTypeDefault:
		return "DEFAULT"
	case AlgorithmTypeCopy:
		return "COPY"
	case AlgorithmTypeInplace:
		return "INPLACE"
	case AlgorithmTypeInstant:
		return "INSTANT"
	default:
		return "DEFAULT"
	}
}

// AlterTableSpec represents alter table specification.
type AlterTableSpec struct {
	node

	NoWriteToBinlog bool
	OnAllPartitions bool

	Tp              AlterTableType
	Name            string
	IndexName       CIStr
	Constraint      *Constraint
	Options         []*TableOption
	OrderByList     []*AlterOrderItem
	NewTable        *TableName
	NewColumns      []*ColumnDef
	NewConstraints  []*Constraint
	OldColumnName   *ColumnName
	NewColumnName   *ColumnName
	Position        *ColumnPosition
	LockType        LockType
	Algorithm       AlgorithmType
	Comment         string
	FromKey         CIStr
	ToKey           CIStr
	Partition       *PartitionOptions
	PartitionNames  []CIStr
	PartDefinitions []*PartitionDefinition
	WithValidation  bool
	Num             uint64
	Visibility      IndexVisibility
}

// AlterOrderItem represents an item in order by at alter table stmt.
type AlterOrderItem struct {
	node
	Column *ColumnName
	Desc   bool
}

// Restore implements Node interface.
func (n *AlterOrderItem) Restore(ctx *format.RestoreCtx) error {
	if err := n.Column.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore AlterOrderItem.Column: %w", err)
	}
	if n.Desc {
		ctx.WriteKeyWord(" DESC")
	}
	return nil
}

func restoreSecondaryPartitionList(ctx *format.RestoreCtx, names []CIStr) {
	if len(names) == 0 {
		return
	}
	ctx.WriteKeyWord(" PARTITION ")
	ctx.WritePlain("(")
	for i, name := range names {
		if i != 0 {
			ctx.WritePlain(",")
		}
		ctx.WriteName(name.O)
	}
	ctx.WritePlain(")")
}

// Restore implements Node interface.
func (n *AlterTableSpec) Restore(ctx *format.RestoreCtx) error {
	switch n.Tp { //nolint:exhaustive
	case AlterTableOption:
		switch {
		case len(n.Options) == 2 && n.Options[0].Tp == TableOptionCharset && n.Options[1].Tp == TableOptionCollate:
			if n.Options[0].UintValue == TableOptionCharsetWithConvertTo {
				ctx.WriteKeyWord("CONVERT TO ")
			}
			ctx.WriteKeyWord("CHARACTER SET ")
			if n.Options[0].Default {
				ctx.WriteKeyWord("DEFAULT")
			} else {
				ctx.WriteKeyWord(n.Options[0].StrValue)
			}
			ctx.WriteKeyWord(" COLLATE ")
			ctx.WriteKeyWord(n.Options[1].StrValue)
		case n.Options[0].Tp == TableOptionCharset && n.Options[0].Default:
			if n.Options[0].UintValue == TableOptionCharsetWithConvertTo {
				ctx.WriteKeyWord("CONVERT TO ")
			}
			ctx.WriteKeyWord("CHARACTER SET DEFAULT")
		default:
			for i, opt := range n.Options {
				if i != 0 {
					ctx.WritePlain(" ")
				}
				if err := opt.Restore(ctx); err != nil {
					return fmt.Errorf("an error occurred while restore AlterTableSpec.Options[%d]: %w", i, err)
				}
			}
		}
	case AlterTableAddColumns:
		ctx.WriteKeyWord("ADD COLUMN ")
		if n.Position != nil && len(n.NewColumns) == 1 {
			if err := n.NewColumns[0].Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore AlterTableSpec.NewColumns[%d]: %w", 0, err)
			}
			if n.Position.Tp != ColumnPositionNone {
				ctx.WritePlain(" ")
			}
			if err := n.Position.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore AlterTableSpec.Position: %w", err)
			}
		} else {
			lenCols := len(n.NewColumns)
			ctx.WritePlain("(")
			for i, col := range n.NewColumns {
				if i != 0 {
					ctx.WritePlain(", ")
				}
				if err := col.Restore(ctx); err != nil {
					return fmt.Errorf("an error occurred while restore AlterTableSpec.NewColumns[%d]: %w", i, err)
				}
			}
			for i, constraint := range n.NewConstraints {
				if i != 0 || lenCols >= 1 {
					ctx.WritePlain(", ")
				}
				if err := constraint.Restore(ctx); err != nil {
					return fmt.Errorf("an error occurred while restore AlterTableSpec.NewConstraints[%d]: %w", i, err)
				}
			}
			ctx.WritePlain(")")
		}
	case AlterTableAddConstraint:
		ctx.WriteKeyWord("ADD ")
		if err := n.Constraint.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore AlterTableSpec.Constraint: %w", err)
		}
	case AlterTableDropColumn:
		ctx.WriteKeyWord("DROP COLUMN ")
		if err := n.OldColumnName.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore AlterTableSpec.OldColumnName: %w", err)
		}
	// TODO: RestrictOrCascadeOpt not support
	case AlterTableDropPrimaryKey:
		ctx.WriteKeyWord("DROP PRIMARY KEY")
	case AlterTableDropIndex:
		ctx.WriteKeyWord("DROP INDEX ")
		ctx.WriteName(n.Name)
	case AlterTableDropForeignKey:
		ctx.WriteKeyWord("DROP FOREIGN KEY ")
		ctx.WriteName(n.Name)
	case AlterTableModifyColumn:
		ctx.WriteKeyWord("MODIFY COLUMN ")
		if err := n.NewColumns[0].Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore AlterTableSpec.NewColumns[0]: %w", err)
		}
		if n.Position.Tp != ColumnPositionNone {
			ctx.WritePlain(" ")
		}
		if err := n.Position.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore AlterTableSpec.Position: %w", err)
		}
	case AlterTableChangeColumn:
		ctx.WriteKeyWord("CHANGE COLUMN ")
		if err := n.OldColumnName.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore AlterTableSpec.OldColumnName: %w", err)
		}
		ctx.WritePlain(" ")
		if err := n.NewColumns[0].Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore AlterTableSpec.NewColumns[0]: %w", err)
		}
		if n.Position.Tp != ColumnPositionNone {
			ctx.WritePlain(" ")
		}
		if err := n.Position.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore AlterTableSpec.Position: %w", err)
		}
	case AlterTableRenameColumn:
		ctx.WriteKeyWord("RENAME COLUMN ")
		if err := n.OldColumnName.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore AlterTableSpec.OldColumnName: %w", err)
		}
		ctx.WriteKeyWord(" TO ")
		if err := n.NewColumnName.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore AlterTableSpec.NewColumnName: %w", err)
		}
	case AlterTableRenameTable:
		ctx.WriteKeyWord("RENAME AS ")
		if err := n.NewTable.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore AlterTableSpec.NewTable: %w", err)
		}
	case AlterTableAlterColumn:
		ctx.WriteKeyWord("ALTER COLUMN ")
		if err := n.NewColumns[0].Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore AlterTableSpec.NewColumns[0]: %w", err)
		}
		if len(n.NewColumns[0].Options) == 1 {
			ctx.WriteKeyWord("SET DEFAULT ")
			expr := n.NewColumns[0].Options[0].Expr
			switch expr.(type) {
			case *ValueExpr, *ParenthesesExpr:
				// Literals restore bare. A ParenthesesExpr prints its own
				// parentheses: the grammar preserves them to keep expression
				// defaults (SET DEFAULT ('{}')) distinct from literal
				// defaults (SET DEFAULT '{}').
				if err := expr.Restore(ctx); err != nil {
					return fmt.Errorf("an error occurred while restore AlterTableSpec.NewColumns[0].Options[0].Expr: %w", err)
				}
			default:
				// A programmatically built AST may hold a bare expression;
				// MySQL requires parentheses around non-literal defaults.
				ctx.WritePlain("(")
				if err := expr.Restore(ctx); err != nil {
					return fmt.Errorf("an error occurred while restore AlterTableSpec.NewColumns[0].Options[0].Expr: %w", err)
				}
				ctx.WritePlain(")")
			}
		} else {
			ctx.WriteKeyWord(" DROP DEFAULT")
		}
	case AlterTableLock:
		ctx.WriteKeyWord("LOCK ")
		ctx.WritePlain("= ")
		ctx.WriteKeyWord(n.LockType.String())
	case AlterTableOrderByColumns:
		ctx.WriteKeyWord("ORDER BY ")
		for i, alterOrderItem := range n.OrderByList {
			if i != 0 {
				ctx.WritePlain(",")
			}
			if err := alterOrderItem.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore AlterTableSpec.OrderByList[%d]: %w", i, err)
			}
		}
	case AlterTableAlgorithm:
		ctx.WriteKeyWord("ALGORITHM ")
		ctx.WritePlain("= ")
		ctx.WriteKeyWord(n.Algorithm.String())
	case AlterTableRenameIndex:
		ctx.WriteKeyWord("RENAME INDEX ")
		ctx.WriteName(n.FromKey.O)
		ctx.WriteKeyWord(" TO ")
		ctx.WriteName(n.ToKey.O)
	case AlterTableForce:
		// TODO: not support
		ctx.WriteKeyWord("FORCE")
		ctx.WritePlain(" /* AlterTableForce is not supported */ ")
	case AlterTableAddPartitions:
		ctx.WriteKeyWord("ADD PARTITION")
		if n.NoWriteToBinlog {
			ctx.WriteKeyWord(" NO_WRITE_TO_BINLOG")
		}
		if n.PartDefinitions != nil {
			ctx.WritePlain(" (")
			for i, def := range n.PartDefinitions {
				if i != 0 {
					ctx.WritePlain(", ")
				}
				if err := def.Restore(ctx); err != nil {
					return fmt.Errorf("an error occurred while restore AlterTableSpec.PartDefinitions[%d]: %w", i, err)
				}
			}
			ctx.WritePlain(")")
		} else if n.Num != 0 {
			ctx.WriteKeyWord(" PARTITIONS ")
			ctx.WritePlainf("%d", n.Num)
		}
	case AlterTableCoalescePartitions:
		ctx.WriteKeyWord("COALESCE PARTITION ")
		if n.NoWriteToBinlog {
			ctx.WriteKeyWord("NO_WRITE_TO_BINLOG ")
		}
		ctx.WritePlainf("%d", n.Num)
	case AlterTableDropPartition:
		ctx.WriteKeyWord("DROP PARTITION ")
		for i, name := range n.PartitionNames {
			if i != 0 {
				ctx.WritePlain(",")
			}
			ctx.WriteName(name.O)
		}
	case AlterTableTruncatePartition:
		ctx.WriteKeyWord("TRUNCATE PARTITION ")
		if n.OnAllPartitions {
			ctx.WriteKeyWord("ALL")
			return nil
		}
		for i, name := range n.PartitionNames {
			if i != 0 {
				ctx.WritePlain(",")
			}
			ctx.WriteName(name.O)
		}
	case AlterTableCheckPartitions:
		ctx.WriteKeyWord("CHECK PARTITION ")
		if n.OnAllPartitions {
			ctx.WriteKeyWord("ALL")
			return nil
		}
		for i, name := range n.PartitionNames {
			if i != 0 {
				ctx.WritePlain(",")
			}
			ctx.WriteName(name.O)
		}
	case AlterTableAnalyzePartitions:
		ctx.WriteKeyWord("ANALYZE PARTITION ")
		if n.NoWriteToBinlog {
			ctx.WriteKeyWord("NO_WRITE_TO_BINLOG ")
		}
		if n.OnAllPartitions {
			ctx.WriteKeyWord("ALL")
			return nil
		}
		for i, name := range n.PartitionNames {
			if i != 0 {
				ctx.WritePlain(",")
			}
			ctx.WriteName(name.O)
		}
	case AlterTableOptimizePartition:
		ctx.WriteKeyWord("OPTIMIZE PARTITION ")
		if n.NoWriteToBinlog {
			ctx.WriteKeyWord("NO_WRITE_TO_BINLOG ")
		}
		if n.OnAllPartitions {
			ctx.WriteKeyWord("ALL")
			return nil
		}
		for i, name := range n.PartitionNames {
			if i != 0 {
				ctx.WritePlain(",")
			}
			ctx.WriteName(name.O)
		}
	case AlterTableRepairPartition:
		ctx.WriteKeyWord("REPAIR PARTITION ")
		if n.NoWriteToBinlog {
			ctx.WriteKeyWord("NO_WRITE_TO_BINLOG ")
		}
		if n.OnAllPartitions {
			ctx.WriteKeyWord("ALL")
			return nil
		}
		for i, name := range n.PartitionNames {
			if i != 0 {
				ctx.WritePlain(",")
			}
			ctx.WriteName(name.O)
		}
	case AlterTableImportPartitionTablespace:
		ctx.WriteKeyWord("IMPORT PARTITION ")
		if n.OnAllPartitions {
			ctx.WriteKeyWord("ALL")
		} else {
			for i, name := range n.PartitionNames {
				if i != 0 {
					ctx.WritePlain(",")
				}
				ctx.WriteName(name.O)
			}
		}
		ctx.WriteKeyWord(" TABLESPACE")
	case AlterTableDiscardPartitionTablespace:
		ctx.WriteKeyWord("DISCARD PARTITION ")
		if n.OnAllPartitions {
			ctx.WriteKeyWord("ALL")
		} else {
			for i, name := range n.PartitionNames {
				if i != 0 {
					ctx.WritePlain(",")
				}
				ctx.WriteName(name.O)
			}
		}
		ctx.WriteKeyWord(" TABLESPACE")
	case AlterTablePartition:
		if err := n.Partition.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore AlterTableSpec.Partition: %w", err)
		}
	case AlterTableEnableKeys:
		ctx.WriteKeyWord("ENABLE KEYS")
	case AlterTableDisableKeys:
		ctx.WriteKeyWord("DISABLE KEYS")
	case AlterTableRemovePartitioning:
		ctx.WriteKeyWord("REMOVE PARTITIONING")
	case AlterTableWithValidation:
		ctx.WriteKeyWord("WITH VALIDATION")
	case AlterTableWithoutValidation:
		ctx.WriteKeyWord("WITHOUT VALIDATION")
	case AlterTableRebuildPartition:
		ctx.WriteKeyWord("REBUILD PARTITION ")
		if n.NoWriteToBinlog {
			ctx.WriteKeyWord("NO_WRITE_TO_BINLOG ")
		}
		if n.OnAllPartitions {
			ctx.WriteKeyWord("ALL")
			return nil
		}
		for i, name := range n.PartitionNames {
			if i != 0 {
				ctx.WritePlain(",")
			}
			ctx.WriteName(name.O)
		}
	case AlterTableReorganizePartition:
		ctx.WriteKeyWord("REORGANIZE PARTITION")
		if n.NoWriteToBinlog {
			ctx.WriteKeyWord(" NO_WRITE_TO_BINLOG")
		}
		if n.OnAllPartitions {
			return nil
		}
		for i, name := range n.PartitionNames {
			if i != 0 {
				ctx.WritePlain(",")
			} else {
				ctx.WritePlain(" ")
			}
			ctx.WriteName(name.O)
		}
		ctx.WriteKeyWord(" INTO ")
		if n.PartDefinitions != nil {
			ctx.WritePlain("(")
			for i, def := range n.PartDefinitions {
				if i != 0 {
					ctx.WritePlain(", ")
				}
				if err := def.Restore(ctx); err != nil {
					return fmt.Errorf("an error occurred while restore AlterTableSpec.PartDefinitions[%d]: %w", i, err)
				}
			}
			ctx.WritePlain(")")
		}
	case AlterTableExchangePartition:
		ctx.WriteKeyWord("EXCHANGE PARTITION ")
		ctx.WriteName(n.PartitionNames[0].O)
		ctx.WriteKeyWord(" WITH TABLE ")
		if err := n.NewTable.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore AlterTableSpec.NewTable: %w", err)
		}
		if !n.WithValidation {
			ctx.WriteKeyWord(" WITHOUT VALIDATION")
		}
	case AlterTableSecondaryLoad:
		ctx.WriteKeyWord("SECONDARY_LOAD")
		restoreSecondaryPartitionList(ctx, n.PartitionNames)
	case AlterTableSecondaryUnload:
		ctx.WriteKeyWord("SECONDARY_UNLOAD")
		restoreSecondaryPartitionList(ctx, n.PartitionNames)
	case AlterTableAlterCheck:
		ctx.WriteKeyWord("ALTER CHECK ")
		ctx.WriteName(n.Constraint.Name)
		if !n.Constraint.Enforced {
			ctx.WriteKeyWord(" NOT")
		}
		ctx.WriteKeyWord(" ENFORCED")
	case AlterTableDropCheck:
		ctx.WriteKeyWord("DROP CHECK ")
		ctx.WriteName(n.Constraint.Name)
	case AlterTableDropConstraint:
		ctx.WriteKeyWord("DROP CONSTRAINT ")
		ctx.WriteName(n.Constraint.Name)
	case AlterTableImportTablespace:
		ctx.WriteKeyWord("IMPORT TABLESPACE")
	case AlterTableDiscardTablespace:
		ctx.WriteKeyWord("DISCARD TABLESPACE")
	case AlterTableIndexInvisible:
		ctx.WriteKeyWord("ALTER INDEX ")
		ctx.WriteName(n.IndexName.O)
		switch n.Visibility { //nolint:exhaustive
		case IndexVisibilityVisible:
			ctx.WriteKeyWord(" VISIBLE")
		case IndexVisibilityInvisible:
			ctx.WriteKeyWord(" INVISIBLE")
		}
	case AlterTableAlterColumnVisibility:
		ctx.WriteKeyWord("ALTER COLUMN ")
		if err := n.OldColumnName.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore AlterTableSpec.OldColumnName: %w", err)
		}
		switch n.Visibility { //nolint:exhaustive // the grammar only produces VISIBLE or INVISIBLE
		case IndexVisibilityVisible:
			ctx.WriteKeyWord(" SET VISIBLE")
		case IndexVisibilityInvisible:
			ctx.WriteKeyWord(" SET INVISIBLE")
		}
	default:
		// TODO: not support
		ctx.WritePlainf(" /* AlterTableType(%d) is not supported */ ", n.Tp)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *AlterTableSpec) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AlterTableSpec)
	if n.Constraint != nil {
		node, ok := n.Constraint.Accept(v)
		if !ok {
			return n, false
		}
		n.Constraint = node.(*Constraint)
	}
	if n.NewTable != nil {
		node, ok := n.NewTable.Accept(v)
		if !ok {
			return n, false
		}
		n.NewTable = node.(*TableName)
	}
	for i, col := range n.NewColumns {
		node, ok := col.Accept(v)
		if !ok {
			return n, false
		}
		n.NewColumns[i] = node.(*ColumnDef)
	}
	for i, constraint := range n.NewConstraints {
		node, ok := constraint.Accept(v)
		if !ok {
			return n, false
		}
		n.NewConstraints[i] = node.(*Constraint)
	}
	if n.OldColumnName != nil {
		node, ok := n.OldColumnName.Accept(v)
		if !ok {
			return n, false
		}
		n.OldColumnName = node.(*ColumnName)
	}
	if n.Position != nil {
		node, ok := n.Position.Accept(v)
		if !ok {
			return n, false
		}
		n.Position = node.(*ColumnPosition)
	}
	if n.Partition != nil {
		node, ok := n.Partition.Accept(v)
		if !ok {
			return n, false
		}
		n.Partition = node.(*PartitionOptions)
	}
	for i, option := range n.Options {
		node, ok := option.Accept(v)
		if !ok {
			return n, false
		}
		n.Options[i] = node.(*TableOption)
	}
	for _, def := range n.PartDefinitions {
		if !def.acceptInPlace(v) {
			return n, false
		}
	}
	return v.Leave(n)
}

// AlterTableStmt is a statement to change the structure of a table.
// See https://dev.mysql.com/doc/refman/5.7/en/alter-table.html
type AlterTableStmt struct {
	ddlNode

	Table *TableName
	Specs []*AlterTableSpec
}

// Restore implements Node interface.
func (n *AlterTableStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ALTER TABLE ")
	if err := n.Table.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore AlterTableStmt.Table: %w", err)
	}
	specs := n.Specs
	for i, spec := range specs {
		if i == 0 || spec.Tp == AlterTablePartition || spec.Tp == AlterTableRemovePartitioning || spec.Tp == AlterTableImportTablespace || spec.Tp == AlterTableDiscardTablespace {
			ctx.WritePlain(" ")
		} else {
			ctx.WritePlain(", ")
		}
		if err := spec.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore AlterTableStmt.Specs[%d]: %w", i, err)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *AlterTableStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AlterTableStmt)
	node, ok := n.Table.Accept(v)
	if !ok {
		return n, false
	}
	n.Table = node.(*TableName)
	for i, val := range n.Specs {
		node, ok = val.Accept(v)
		if !ok {
			return n, false
		}
		n.Specs[i] = node.(*AlterTableSpec)
	}
	return v.Leave(n)
}

// TruncateTableStmt is a statement to empty a table completely.
// See https://dev.mysql.com/doc/refman/5.7/en/truncate-table.html
type TruncateTableStmt struct {
	ddlNode

	Table *TableName
}

// Restore implements Node interface.
func (n *TruncateTableStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("TRUNCATE TABLE ")
	if err := n.Table.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore TruncateTableStmt.Table: %w", err)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *TruncateTableStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*TruncateTableStmt)
	node, ok := n.Table.Accept(v)
	if !ok {
		return n, false
	}
	n.Table = node.(*TableName)
	return v.Leave(n)
}

var (
	ErrNoParts                      = mysql.NewStdErr("ddl", mysql.ErrNoParts)
	ErrPartitionColumnList          = mysql.NewStdErr("ddl", mysql.ErrPartitionColumnList)
	ErrPartitionRequiresValues      = mysql.NewStdErr("ddl", mysql.ErrPartitionRequiresValues)
	ErrPartitionsMustBeDefined      = mysql.NewStdErr("ddl", mysql.ErrPartitionsMustBeDefined)
	ErrPartitionWrongNoPart         = mysql.NewStdErr("ddl", mysql.ErrPartitionWrongNoPart)
	ErrPartitionWrongNoSubpart      = mysql.NewStdErr("ddl", mysql.ErrPartitionWrongNoSubpart)
	ErrPartitionWrongValues         = mysql.NewStdErr("ddl", mysql.ErrPartitionWrongValues)
	ErrRowSinglePartitionField      = mysql.NewStdErr("ddl", mysql.ErrRowSinglePartitionField)
	ErrSubpartition                 = mysql.NewStdErr("ddl", mysql.ErrSubpartition)
	ErrTooManyValues                = mysql.NewStdErr("ddl", mysql.ErrTooManyValues)
	ErrUnknownCharacterSet          = mysql.NewStdErr("ddl", mysql.ErrUnknownCharacterSet)
	ErrCoalescePartitionNoPartition = mysql.NewStdErr("ddl", mysql.ErrCoalescePartitionNoPartition)
	ErrWrongUsage                   = mysql.NewStdErr("ddl", mysql.ErrWrongUsage)
)

type SubPartitionDefinition struct {
	Name    CIStr
	Options []*TableOption
}

func (spd *SubPartitionDefinition) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("SUBPARTITION ")
	ctx.WriteName(spd.Name.O)
	for i, opt := range spd.Options {
		ctx.WritePlain(" ")
		if err := opt.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore SubPartitionDefinition.Options[%d]: %w", i, err)
		}
	}
	return nil
}

type PartitionDefinitionClause interface {
	restore(ctx *format.RestoreCtx) error
	acceptInPlace(v Visitor) bool
	// Validate checks if the clause is consistent with the given options.
	// `pt` can be 0 and `columns` can be -1 to skip checking the clause against
	// the partition type or number of columns in the expression list.
	Validate(pt PartitionType, columns int) error
}

type PartitionDefinitionClauseNone struct{}

func (*PartitionDefinitionClauseNone) restore(_ *format.RestoreCtx) error {
	return nil
}

func (*PartitionDefinitionClauseNone) acceptInPlace(_ Visitor) bool {
	return true
}

func (*PartitionDefinitionClauseNone) Validate(pt PartitionType, _ int) error {
	switch pt { //nolint:exhaustive
	case 0:
	case PartitionTypeRange:
		return ErrPartitionRequiresValues.GenByArgs("RANGE", "LESS THAN")
	case PartitionTypeList:
		return ErrPartitionRequiresValues.GenByArgs("LIST", "IN")
	}
	return nil
}

type PartitionDefinitionClauseLessThan struct {
	Exprs []ExprNode
}

func (n *PartitionDefinitionClauseLessThan) restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord(" VALUES LESS THAN ")
	ctx.WritePlain("(")
	for i, expr := range n.Exprs {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := expr.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore PartitionDefinitionClauseLessThan.Exprs[%d]: %w", i, err)
		}
	}
	ctx.WritePlain(")")
	return nil
}

func (n *PartitionDefinitionClauseLessThan) acceptInPlace(v Visitor) bool {
	for i, expr := range n.Exprs {
		newExpr, ok := expr.Accept(v)
		if !ok {
			return false
		}
		n.Exprs[i] = newExpr.(ExprNode)
	}
	return true
}

func (n *PartitionDefinitionClauseLessThan) Validate(pt PartitionType, columns int) error {
	switch pt { //nolint:exhaustive
	case PartitionTypeRange, 0:
	default:
		return ErrPartitionWrongValues.GenByArgs("RANGE", "LESS THAN")
	}

	switch {
	case columns == 0 && len(n.Exprs) != 1:
		return ErrTooManyValues.GenByArgs("RANGE")
	case columns > 0 && len(n.Exprs) != columns:
		return ErrPartitionColumnList
	}
	return nil
}

type PartitionDefinitionClauseIn struct {
	Values [][]ExprNode
}

func (n *PartitionDefinitionClauseIn) restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord(" VALUES IN ")
	ctx.WritePlain("(")
	for i, valList := range n.Values {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if len(valList) == 1 {
			if err := valList[0].Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore PartitionDefinitionClauseIn.Values[%d][0]: %w", i, err)
			}
		} else {
			ctx.WritePlain("(")
			for j, val := range valList {
				if j != 0 {
					ctx.WritePlain(", ")
				}
				if err := val.Restore(ctx); err != nil {
					return fmt.Errorf("an error occurred while restore PartitionDefinitionClauseIn.Values[%d][%d]: %w", i, j, err)
				}
			}
			ctx.WritePlain(")")
		}
	}
	ctx.WritePlain(")")
	return nil
}

func (n *PartitionDefinitionClauseIn) acceptInPlace(v Visitor) bool {
	for _, valList := range n.Values {
		for j, val := range valList {
			newVal, ok := val.Accept(v)
			if !ok {
				return false
			}
			valList[j] = newVal.(ExprNode)
		}
	}
	return true
}

func (n *PartitionDefinitionClauseIn) Validate(pt PartitionType, columns int) error {
	switch pt { //nolint:exhaustive
	case PartitionTypeList, 0:
	default:
		return ErrPartitionWrongValues.GenByArgs("LIST", "IN")
	}

	if len(n.Values) == 0 {
		return nil
	}

	expectedColCount := len(n.Values[0])
	for _, val := range n.Values[1:] {
		if len(val) != expectedColCount {
			return ErrPartitionColumnList
		}
	}

	switch {
	case columns == 0 && expectedColCount != 1:
		return ErrRowSinglePartitionField
	case columns > 0 && expectedColCount != columns:
		if len(n.Values) == 1 && expectedColCount == 1 {
			if _, ok := n.Values[0][0].(*DefaultExpr); ok {
				// Only one value, which is DEFAULT, which is OK
				return nil
			}
		}
		return ErrPartitionColumnList
	}
	return nil
}

// PartitionDefinition defines a single partition.
type PartitionDefinition struct {
	Name    CIStr
	Clause  PartitionDefinitionClause
	Options []*TableOption
	Sub     []*SubPartitionDefinition
}

// Comment returns the comment option given to this definition.
// The second return value indicates if the comment option exists.
func (n *PartitionDefinition) Comment() (string, bool) {
	for _, opt := range n.Options {
		if opt.Tp == TableOptionComment {
			return opt.StrValue, true
		}
	}
	return "", false
}

func (n *PartitionDefinition) acceptInPlace(v Visitor) bool {
	return n.Clause.acceptInPlace(v)
}

// Restore implements Node interface.
func (n *PartitionDefinition) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("PARTITION ")
	ctx.WriteName(n.Name.O)

	if err := n.Clause.restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore PartitionDefinition.Clause: %w", err)
	}

	for i, opt := range n.Options {
		ctx.WritePlain(" ")
		if err := opt.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore PartitionDefinition.Options[%d]: %w", i, err)
		}
	}

	if len(n.Sub) > 0 {
		ctx.WritePlain(" (")
		for i, spd := range n.Sub {
			if i != 0 {
				ctx.WritePlain(",")
			}
			if err := spd.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore PartitionDefinition.Sub[%d]: %w", i, err)
			}
		}
		ctx.WritePlain(")")
	}

	return nil
}

// PartitionMethod describes how partitions or subpartitions are constructed.
type PartitionMethod struct {
	// To be able to get original text and replace the syntactic sugar with generated
	// partition definitions
	node
	// Tp is the type of the partition function
	Tp PartitionType
	// Linear is a modifier to the HASH and KEY type for choosing a different
	// algorithm
	Linear bool
	// Expr is an expression used as argument of HASH, RANGE AND LIST types
	Expr ExprNode
	// ColumnNames is a list of column names used as argument of KEY,
	// RANGE COLUMNS and LIST COLUMNS types
	ColumnNames []*ColumnName

	// Num is the number of (sub)partitions required by the method.
	Num uint64

	// KeyAlgorithm is the optional hash algorithm type for `PARTITION BY [LINEAR] KEY` syntax.
	KeyAlgorithm *PartitionKeyAlgorithm
}

type PartitionKeyAlgorithm struct {
	Type uint64
}

// Restore implements the Node interface
func (n *PartitionMethod) Restore(ctx *format.RestoreCtx) error {
	if n.Linear {
		ctx.WriteKeyWord("LINEAR ")
	}
	ctx.WriteKeyWord(n.Tp.String())

	if n.KeyAlgorithm != nil {
		ctx.WriteKeyWord(" ALGORITHM")
		ctx.WritePlainf(" = %d", n.KeyAlgorithm.Type)
	}

	switch {
	case n.Expr != nil:
		ctx.WritePlain(" (")
		if err := n.Expr.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore PartitionMethod.Expr: %w", err)
		}
		ctx.WritePlain(")")

	default:
		if n.Tp == PartitionTypeRange || n.Tp == PartitionTypeList {
			ctx.WriteKeyWord(" COLUMNS")
		}
		ctx.WritePlain(" (")
		for i, col := range n.ColumnNames {
			if i > 0 {
				ctx.WritePlain(",")
			}
			if err := col.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while splicing PartitionMethod.ColumnName[%d]: %w", i, err)
			}
		}
		ctx.WritePlain(")")
	}

	return nil
}

// acceptInPlace is like Node.Accept but does not allow replacing the node itself.
func (n *PartitionMethod) acceptInPlace(v Visitor) bool {
	if n.Expr != nil {
		expr, ok := n.Expr.Accept(v)
		if !ok {
			return false
		}
		n.Expr = expr.(ExprNode)
	}
	for i, colName := range n.ColumnNames {
		newColName, ok := colName.Accept(v)
		if !ok {
			return false
		}
		n.ColumnNames[i] = newColName.(*ColumnName)
	}
	return true
}

// PartitionOptions specifies the partition options.
type PartitionOptions struct {
	PartitionMethod
	Sub         *PartitionMethod
	Definitions []*PartitionDefinition
}

// Validate checks if the partition is well-formed.
func (n *PartitionOptions) Validate() error {
	// if both a partition list and the partition numbers are specified, their values must match
	if n.Num != 0 && len(n.Definitions) != 0 && n.Num != uint64(len(n.Definitions)) {
		return ErrPartitionWrongNoPart
	}
	// now check the subpartition count
	if len(n.Definitions) > 0 {
		// ensure the subpartition count for every partitions are the same
		// then normalize n.Num and n.Sub.Num so equality comparison works.
		n.Num = uint64(len(n.Definitions))

		subDefCount := len(n.Definitions[0].Sub)
		for _, pd := range n.Definitions[1:] {
			if len(pd.Sub) != subDefCount {
				return ErrPartitionWrongNoSubpart
			}
		}
		if n.Sub != nil {
			if n.Sub.Num != 0 && subDefCount != 0 && n.Sub.Num != uint64(subDefCount) {
				return ErrPartitionWrongNoSubpart
			}
			if subDefCount != 0 {
				n.Sub.Num = uint64(subDefCount)
			}
		} else if subDefCount != 0 {
			return ErrSubpartition
		}
	}

	switch n.Tp { //nolint:exhaustive
	case PartitionTypeHash, PartitionTypeKey:
		if n.Num == 0 {
			n.Num = 1
		}
	case PartitionTypeRange, PartitionTypeList:
		if len(n.Definitions) == 0 {
			return ErrPartitionsMustBeDefined.GenByArgs(n.Tp)
		}
	}

	for _, pd := range n.Definitions {
		// ensure the partition definition types match the methods,
		// e.g. RANGE partitions only allows VALUES LESS THAN
		if err := pd.Clause.Validate(n.Tp, len(n.ColumnNames)); err != nil {
			return err
		}
	}

	return nil
}

func (n *PartitionOptions) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("PARTITION BY ")
	if err := n.PartitionMethod.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore PartitionOptions.PartitionMethod: %w", err)
	}

	if n.Num > 0 && len(n.Definitions) == 0 {
		ctx.WriteKeyWord(" PARTITIONS ")
		ctx.WritePlainf("%d", n.Num)
	}

	if n.Sub != nil {
		ctx.WriteKeyWord(" SUBPARTITION BY ")
		if err := n.Sub.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore PartitionOptions.Sub: %w", err)
		}
		if n.Sub.Num > 0 {
			ctx.WriteKeyWord(" SUBPARTITIONS ")
			ctx.WritePlainf("%d", n.Sub.Num)
		}
	}

	if len(n.Definitions) > 0 {
		ctx.WritePlain(" (")
		for i, def := range n.Definitions {
			if i > 0 {
				ctx.WritePlain(",")
			}
			if err := def.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore PartitionOptions.Definitions[%d]: %w", i, err)
			}
		}
		ctx.WritePlain(")")
	}

	return nil
}

func (n *PartitionOptions) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}

	n = newNode.(*PartitionOptions)
	if !n.acceptInPlace(v) {
		return n, false
	}
	if n.Sub != nil && !n.Sub.acceptInPlace(v) {
		return n, false
	}
	for _, def := range n.Definitions {
		if !def.acceptInPlace(v) {
			return n, false
		}
	}
	return v.Leave(n)
}
