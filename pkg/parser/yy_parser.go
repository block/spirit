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

package parser

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"unicode"

	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/parser/auth"
	"github.com/block/spirit/pkg/parser/charset"
	"github.com/block/spirit/pkg/parser/mysql"
	"github.com/block/spirit/pkg/parser/types"
)

var (
	// ErrSyntax returns for sql syntax error.
	ErrSyntax = mysql.NewStdErr("parser", mysql.ErrSyntax)
	// ErrParse returns for sql parse error.
	ErrParse = mysql.NewStdErr("parser", mysql.ErrParse)
	// ErrUnknownCharacterSet returns for no character set found error.
	ErrUnknownCharacterSet = mysql.NewStdErr("parser", mysql.ErrUnknownCharacterSet)
	// ErrInvalidYearColumnLength returns for illegal column length for year type.
	ErrInvalidYearColumnLength = mysql.NewStdErr("parser", mysql.ErrInvalidYearColumnLength)
	// ErrWrongArguments returns for illegal argument.
	ErrWrongArguments = mysql.NewStdErr("parser", mysql.ErrWrongArguments)
	// ErrWrongFieldTerminators returns for illegal field terminators.
	ErrWrongFieldTerminators = mysql.NewStdErr("parser", mysql.ErrWrongFieldTerminators)
	// ErrTooBigDisplayWidth returns for data display width exceed limit .
	ErrTooBigDisplayWidth = mysql.NewStdErr("parser", mysql.ErrTooBigDisplaywidth)
	// ErrTooBigPrecision returns for data precision exceed limit.
	ErrTooBigPrecision = mysql.NewStdErr("parser", mysql.ErrTooBigPrecision)
	// ErrUnknownAlterLock returns for no alter lock type found error.
	ErrUnknownAlterLock = mysql.NewStdErr("parser", mysql.ErrUnknownAlterLock)
	// ErrUnknownAlterAlgorithm returns for no alter algorithm found error.
	ErrUnknownAlterAlgorithm = mysql.NewStdErr("parser", mysql.ErrUnknownAlterAlgorithm)
	// ErrWrongValue returns for wrong value
	ErrWrongValue = mysql.NewStdErr("parser", mysql.ErrWrongValue)
	// ErrWarnDeprecatedSyntax return when the syntax was deprecated
	ErrWarnDeprecatedSyntax = mysql.NewStdErr("parser", mysql.ErrWarnDeprecatedSyntax)
	// ErrWarnDeprecatedSyntaxNoReplacement return when the syntax was deprecated and there is no replacement.
	ErrWarnDeprecatedSyntaxNoReplacement = mysql.NewStdErr("parser", mysql.ErrWarnDeprecatedSyntaxNoReplacement)
	// ErrWrongUsage returns for incorrect usages.
	ErrWrongUsage = mysql.NewStdErr("parser", mysql.ErrWrongUsage)
	// ErrWrongDBName returns for incorrect DB name.
	ErrWrongDBName = mysql.NewStdErr("parser", mysql.ErrWrongDBName)
	// ErrDataOutOfRange returns for incorrect range.
	ErrDataOutOfRange = mysql.NewStdErr("parser", mysql.ErrDataOutOfRange)
)

//revive:disable:exported

// ParserConfig is the parser config.
type ParserConfig struct {
	EnableWindowFunction        bool
	EnableStrictDoubleTypeCheck bool
	SkipPositionRecording       bool
}

const (
	// maxASTDepthStmtOverhead leaves room for statement wrapper nodes on the
	// visitor path, for example SelectStmt -> FieldList -> SelectField.
	maxASTDepthStmtOverhead = 64
	// maxASTDepth bounds user-controlled AST nesting before recursive visitors run.
	maxASTDepth = maxParenthesesDepth + maxASTDepthStmtOverhead
)

//revive:enable:exported

// Parser represents a parser instance. Some temporary objects are stored in it to reduce object allocation during Parse function.
type Parser struct {
	charset    string
	collation  string
	result     []ast.StmtNode
	src        string
	lexer      Scanner
	hintParser *hintParser

	explicitCharset       bool
	strictDoubleFieldType bool

	// the following fields are used by yyParse to reduce allocation.
	cache  []yySymType
	yylval yySymType
	yyVAL  *yySymType
}

// setNodeText sets the raw text on a parsed AST node and propagates the
// NO_BACKSLASH_ESCAPES SQL mode so that Text() can correctly handle
// backslash escapes when converting binary string literals to hex.
func (parser *Parser) setNodeText(n interface {
	SetText(charset.Encoding, string)
}, text string) {
	n.SetText(parser.lexer.client, text)
	if setter, ok := n.(interface{ SetNoBackslashEscapes(bool) }); ok {
		setter.SetNoBackslashEscapes(parser.lexer.sqlMode.HasNoBackslashEscapesMode())
	}
}

func yySetOffset(yyVAL *yySymType, offset int) {
	if yyVAL.expr != nil {
		yyVAL.expr.SetOriginTextPosition(offset)
	}
}

func yyhintSetOffset(_ *yyhintSymType, _ int) {
}

type stmtTexter interface {
	stmtText() string
}

// New returns a Parser object with default SQL mode.
func New() *Parser {
	p := &Parser{
		cache: make([]yySymType, 200),
	}
	p.reset()
	return p
}

// Reset resets the parser.
func (parser *Parser) Reset() {
	clear(parser.cache)
	parser.reset()
}

func (parser *Parser) reset() {
	parser.explicitCharset = false
	parser.strictDoubleFieldType = false
	parser.EnableWindowFunc(true)
	parser.SetStrictDoubleTypeCheck(true)
	mode, _ := mysql.GetSQLMode(mysql.DefaultSQLMode)
	parser.SetSQLMode(mode)
}

// SetStrictDoubleTypeCheck enables/disables strict double type check.
func (parser *Parser) SetStrictDoubleTypeCheck(val bool) {
	parser.strictDoubleFieldType = val
}

// SetParserConfig sets the parser config.
func (parser *Parser) SetParserConfig(config ParserConfig) {
	parser.EnableWindowFunc(config.EnableWindowFunction)
	parser.SetStrictDoubleTypeCheck(config.EnableStrictDoubleTypeCheck)
	parser.lexer.skipPositionRecording = config.SkipPositionRecording
}

// ParseSQL parses a query string to raw ast.StmtNode.
func (parser *Parser) ParseSQL(sql string, params ...ParseParam) (stmt []ast.StmtNode, warns []error, err error) {
	resetParams(parser)
	parser.lexer.reset(sql)
	for _, p := range params {
		if err := p.ApplyOn(parser); err != nil {
			return nil, nil, err
		}
	}
	parser.src = sql
	parser.result = parser.result[:0]

	var l yyLexer = &parser.lexer
	yyParse(l, parser)

	warns, errs := l.Errors()
	if len(warns) > 0 {
		warns = slices.Clone(warns)
	} else {
		warns = nil
	}
	if len(errs) != 0 {
		return nil, warns, errs[0]
	}
	for _, stmt := range parser.result {
		if err := checkASTDepth(stmt); err != nil {
			return nil, warns, err
		}
	}
	return parser.result, warns, nil
}

// Parse parses a query string to raw ast.StmtNode.
// If charset or collation is "", default charset and collation will be used.
func (parser *Parser) Parse(sql, charset, collation string) (stmt []ast.StmtNode, warns []error, err error) {
	return parser.ParseSQL(sql, CharsetConnection(charset), CollationConnection(collation))
}

func (parser *Parser) lastErrorAsWarn() {
	parser.lexer.lastErrorAsWarn()
}

func checkASTDepth(stmt ast.StmtNode) error {
	checker := astDepthChecker{}
	_, _ = stmt.Accept(&checker)
	if checker.exceeded {
		return ErrParse.GenByArgs("AST nesting depth exceeds maximum", strconv.Itoa(maxASTDepth))
	}
	return nil
}

type astDepthChecker struct {
	depth    int
	exceeded bool
}

func (c *astDepthChecker) Enter(in ast.Node) (ast.Node, bool) {
	c.depth++
	if c.depth > maxASTDepth {
		c.exceeded = true
		return in, true
	}
	return in, false
}

func (c *astDepthChecker) Leave(in ast.Node) (ast.Node, bool) {
	c.depth--
	return in, !c.exceeded
}

// ParseOneStmt parses a query and returns an ast.StmtNode.
// The query must have one statement, otherwise ErrSyntax is returned.
func (parser *Parser) ParseOneStmt(sql, charset, collation string) (ast.StmtNode, error) {
	stmts, _, err := parser.ParseSQL(sql, CharsetConnection(charset), CollationConnection(collation))
	if err != nil {
		return nil, err
	}
	if len(stmts) != 1 {
		return nil, ErrSyntax
	}
	return stmts[0], nil
}

// SetSQLMode sets the SQL mode for parser.
func (parser *Parser) SetSQLMode(mode mysql.SQLMode) {
	parser.lexer.SetSQLMode(mode)
}

// EnableWindowFunc controls whether the parser to parse syntax related with window function.
func (parser *Parser) EnableWindowFunc(val bool) {
	parser.lexer.EnableWindowFunc(val)
}

// ParseErrorWith returns "You have a syntax error near..." error message compatible with mysql.
func ParseErrorWith(errstr string, lineno int) error {
	if len(errstr) > mysql.ErrTextLength {
		errstr = errstr[:mysql.ErrTextLength]
	}
	return fmt.Errorf("near '%-.80s' at line %d", errstr, lineno)
}

// The select statement is not at the end of the whole statement, if the last
// field text was set from its offset to the end of the src string, update
// the last field text.
func (parser *Parser) setLastSelectFieldText(st *ast.SelectStmt, lastEnd int) {
	if st.Kind != ast.SelectStmtKindSelect {
		return
	}
	lastField := st.Fields.Fields[len(st.Fields.Fields)-1]
	if lastField.Offset+len(lastField.OriginalText()) >= len(parser.src)-1 {
		lastField.SetText(parser.lexer.client, parser.src[lastField.Offset:lastEnd])
	}
}

func (*Parser) startOffset(v *yySymType) int {
	return v.offset
}

func (parser *Parser) endOffset(v *yySymType) int {
	offset := v.offset
	for offset > 0 && unicode.IsSpace(rune(parser.src[offset-1])) {
		offset--
	}
	return offset
}

func (parser *Parser) parseHint(input string) ([]*ast.TableOptimizerHint, []error) {
	if parser.hintParser == nil {
		parser.hintParser = newHintParser()
	}
	return parser.hintParser.parse(input, parser.lexer.GetSQLMode(), parser.lexer.lastHintPos)
}

func toInt(l yyLexer, lval *yySymType, str string) int {
	n, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		if errors.Is(err, strconv.ErrRange) {
			// TODO: toDecimal maybe out of range still.
			// This kind of error should be throw to higher level, because truncated data maybe legal.
			// For example, this SQL returns error:
			// create table test (id decimal(30, 0));
			// insert into test values(123456789012345678901234567890123094839045793405723406801943850);
			// While this SQL:
			// select 1234567890123456789012345678901230948390457934057234068019438509023041874359081325875128590860234789847359871045943057;
			// get value 99999999999999999999999999999999999999999999999999999999999999999
			return toDecimal(l, lval, str)
		}
		l.AppendError(l.Errorf("integer literal: %v", err))
		return invalid
	}

	switch {
	case n <= math.MaxInt64:
		lval.item = int64(n)
	default:
		lval.item = n
	}
	return intLit
}

func toDecimal(l yyLexer, lval *yySymType, str string) int {
	dec, err := ast.NewDecimal(str)
	if err != nil {
		switch {
		case errors.Is(err, types.ErrDataOutOfRange):
			// Integer part exceeds the 65-digit maximum: MySQL clamps the
			// literal to the maximum decimal value with a truncation warning.
			l.AppendWarn(types.ErrTruncatedWrongValue.GenByArgs("DECIMAL", dec))
			dec, _ = ast.NewDecimal(mysql.DefaultDecimal)
		case errors.Is(err, types.ErrTruncatedWrongValue):
			// Fractional part was too long and got truncated; keep the
			// truncated value like MySQL does.
			l.AppendWarn(types.ErrTruncatedWrongValue.GenByArgs("DECIMAL", dec))
		default:
			l.AppendError(l.Errorf("decimal literal: %v", err))
		}
	}
	lval.item = dec
	return decLit
}

func toFloat(l yyLexer, lval *yySymType, str string) int {
	n, err := strconv.ParseFloat(str, 64)
	if err != nil {
		if errors.Is(err, strconv.ErrRange) {
			l.AppendError(types.ErrIllegalValueForType.GenByArgs("double", str))
			return invalid
		}
		l.AppendError(l.Errorf("float literal: %v", err))
		return invalid
	}

	lval.item = n
	return floatLit
}

// See https://dev.mysql.com/doc/refman/5.7/en/hexadecimal-literals.html
func toHex(l yyLexer, lval *yySymType, str string) int {
	h, err := ast.NewHexLiteral(str)
	if err != nil {
		l.AppendError(l.Errorf("hex literal: %v", err))
		return invalid
	}
	lval.item = h
	return hexLit
}

// See https://dev.mysql.com/doc/refman/5.7/en/bit-type.html
func toBit(l yyLexer, lval *yySymType, str string) int {
	b, err := ast.NewBitLiteral(str)
	if err != nil {
		l.AppendError(l.Errorf("bit literal: %v", err))
		return invalid
	}
	lval.item = b
	return bitLit
}

func getUint64FromNUM(num interface{}) uint64 {
	switch v := num.(type) {
	case int64:
		return uint64(v)
	case uint64:
		return v
	}
	return 0
}

func getInt64FromNUM(num interface{}) (val int64, errMsg string) {
	switch v := num.(type) {
	case int64:
		return v, ""
	default:
		return -1, fmt.Sprintf("%d is out of range [–9223372036854775808,9223372036854775807]", num)
	}
}

func isRevokeAllGrant(roleOrPrivList []*ast.RoleOrPriv) bool {
	if len(roleOrPrivList) != 2 {
		return false
	}
	priv, err := roleOrPrivList[0].ToPriv()
	if err != nil {
		return false
	}
	if priv.Priv != mysql.AllPriv {
		return false
	}
	priv, err = roleOrPrivList[1].ToPriv()
	if err != nil {
		return false
	}
	if priv.Priv != mysql.GrantPriv {
		return false
	}
	return true
}

// convertToRole tries to convert elements of roleOrPrivList to RoleIdentity
func convertToRole(roleOrPrivList []*ast.RoleOrPriv) ([]*auth.RoleIdentity, error) {
	var roles []*auth.RoleIdentity
	for _, elem := range roleOrPrivList {
		role, err := elem.ToRole()
		if err != nil {
			return nil, err
		}
		roles = append(roles, role)
	}
	return roles, nil
}

// convertToPriv tries to convert elements of roleOrPrivList to PrivElem
func convertToPriv(roleOrPrivList []*ast.RoleOrPriv) ([]*ast.PrivElem, error) {
	var privileges []*ast.PrivElem
	for _, elem := range roleOrPrivList {
		priv, err := elem.ToPriv()
		if err != nil {
			return nil, err
		}
		privileges = append(privileges, priv)
	}
	return privileges, nil
}

var (
	_ ParseParam = CharsetConnection("")
	_ ParseParam = CollationConnection("")
)

// parenQueryToSetOpr wraps a parenthesized query expression in a
// SetOprStmt, preserving inner WITH/ORDER BY/LIMIT as a nested select
// list — the same shape the SubSelect OrderBy / SelectStmtLimit
// statement alternatives build inline.
func parenQueryToSetOpr(sub *ast.SubqueryExpr) *ast.SetOprStmt {
	var setOprList []ast.Node
	switch x := sub.Query.(type) {
	case *ast.SelectStmt:
		setOprList = []ast.Node{&ast.SetOprSelectList{Selects: []ast.Node{x}, With: x.With}}
	case *ast.SetOprStmt:
		setOprList = []ast.Node{&ast.SetOprSelectList{Selects: x.SelectList.Selects, With: x.With, Limit: x.Limit, OrderBy: x.OrderBy}}
	}
	return &ast.SetOprStmt{SelectList: &ast.SetOprSelectList{Selects: setOprList}}
}

func resetParams(p *Parser) {
	p.charset = mysql.DefaultCharset
	p.collation = mysql.DefaultCollationName
}

// ParseParam represents the parameter of parsing.
type ParseParam interface {
	ApplyOn(*Parser) error
}

// CharsetConnection is used for literals specified without a character set.
type CharsetConnection string

// ApplyOn implements ParseParam interface.
func (c CharsetConnection) ApplyOn(p *Parser) error {
	if c == "" {
		p.charset = mysql.DefaultCharset
	} else {
		p.charset = string(c)
	}
	p.lexer.connection = charset.FindEncoding(string(c))
	return nil
}

// CollationConnection is used for literals specified without a collation.
type CollationConnection string

// ApplyOn implements ParseParam interface.
func (c CollationConnection) ApplyOn(p *Parser) error {
	if c == "" {
		p.collation = mysql.DefaultCollationName
	} else {
		p.collation = string(c)
	}
	return nil
}

// selectLockIntoHolder carries the trailing locking clauses and INTO clause of
// a SELECT while the grammar reduces them; it never appears in the final AST.
type selectLockIntoHolder struct {
	locks []*ast.SelectLockInfo
	into  *ast.SelectIntoOption
}

// jsonValueReturningHolder carries the RETURNING clause of JSON_VALUE while
// the grammar reduces it; it never appears in the final AST.
type jsonValueReturningHolder struct {
	tp              *types.FieldType
	explicitCharset bool
}

// jsonValueOnHolder carries the ON EMPTY/ON ERROR clauses of JSON_VALUE while
// the grammar reduces them; it never appears in the final AST.
type jsonValueOnHolder struct {
	onEmpty *ast.JSONValueOnBehavior
	onError *ast.JSONValueOnBehavior
}
