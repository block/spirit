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

package parser_test

import (
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/block/spirit/pkg/parser"
	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/parser/charset"
	. "github.com/block/spirit/pkg/parser/format"
	"github.com/block/spirit/pkg/parser/mysql"
	"github.com/block/spirit/pkg/parser/opcode"
	"github.com/stretchr/testify/require"
)

func TestSpecialComments(t *testing.T) {
	p := parser.New()

	// 1. Make sure /*! ... */ respects the same SQL mode.
	_, err := p.ParseOneStmt(`SELECT /*! '\' */;`, "", "")
	require.Error(t, err)

	p.SetSQLMode(mysql.ModeNoBackslashEscapes)
	st, err := p.ParseOneStmt(`SELECT /*! '\' */;`, "", "")
	require.NoError(t, err)
	require.IsType(t, &ast.SelectStmt{}, st)

	// 2. Make sure multiple statements inside /*! ... */ will not crash
	// (this is issue #330)
	stmts, _, err := p.Parse("/*! SET x = 1; SELECT 2 */", "", "")
	require.NoError(t, err)
	require.Len(t, stmts, 2)
	require.IsType(t, &ast.SetStmt{}, stmts[0])
	require.Equal(t, "/*! SET x = 1;", stmts[0].Text())
	require.IsType(t, &ast.SelectStmt{}, stmts[1])
	require.Equal(t, " SELECT 2 */", stmts[1].Text())
	// ^ not sure if correct approach; having multiple statements in MySQL is a syntax error.

	// 3. Make sure invalid text won't cause infinite loop
	// (this is issue #336)
	st, err = p.ParseOneStmt("SELECT /*+ 😅 */ SLEEP(1);", "", "")
	require.NoError(t, err)
	sel, ok := st.(*ast.SelectStmt)
	require.True(t, ok)
	require.Empty(t, sel.TableHints)
}

type testCase struct {
	src     string
	ok      bool
	restore string
}

type testErrMsgCase struct {
	src string
	err error
}

func RunTest(t *testing.T, table []testCase, enableWindowFunc bool) {
	p := parser.New()
	p.EnableWindowFunc(enableWindowFunc)
	for _, tbl := range table {
		_, _, err := p.Parse(tbl.src, "", "")
		if !tbl.ok {
			require.Errorf(t, err, "source %v, error %v", tbl.src, err)
			continue
		}
		require.NoErrorf(t, err, "source:\n%v\nerror:\n%v", tbl.src, err)
		// restore correctness test
		if tbl.ok {
			RunRestoreTest(t, tbl.src, tbl.restore, enableWindowFunc)
		}
	}
}

func RunRestoreTest(t *testing.T, sourceSQLs, expectSQLs string, enableWindowFunc bool) {
	var sb strings.Builder
	p := parser.New()
	p.EnableWindowFunc(enableWindowFunc)
	comment := fmt.Sprintf("source %v", sourceSQLs)
	stmts, _, err := p.Parse(sourceSQLs, "", "")
	require.NoErrorf(t, err, "source %v", sourceSQLs)
	restoreSQLs := ""
	for _, stmt := range stmts {
		sb.Reset()
		err = stmt.Restore(NewRestoreCtx(DefaultRestoreFlags, &sb))
		require.NoError(t, err, comment)
		restoreSQL := sb.String()
		comment = fmt.Sprintf("source %v; restore %v", sourceSQLs, restoreSQL)
		restoreStmt, err := p.ParseOneStmt(restoreSQL, "", "")
		require.NoError(t, err, comment)
		CleanNodeText(stmt)
		CleanNodeText(restoreStmt)
		require.Equal(t, stmt, restoreStmt, comment)
		if restoreSQLs != "" {
			restoreSQLs += "; "
		}
		restoreSQLs += restoreSQL
	}
	require.Equalf(t, expectSQLs, restoreSQLs, "restore %v; expect %v", restoreSQLs, expectSQLs)
}

// errorsEqual reports whether two errors match, either via errors.Is or by
// rendering to the same message.
func errorsEqual(err1, err2 error) bool {
	if errors.Is(err1, err2) {
		return true
	}
	if err1 == nil || err2 == nil {
		return false
	}
	return err1.Error() == err2.Error()
}

func RunErrMsgTest(t *testing.T, table []testErrMsgCase) {
	p := parser.New()
	for _, tbl := range table {
		_, _, err := p.Parse(tbl.src, "", "")
		comment := fmt.Sprintf("source %v", tbl.src)
		if tbl.err != nil {
			require.True(t, errorsEqual(err, tbl.err), comment)
		} else {
			require.NoError(t, err, comment)
		}
	}
}

func TestSetVariable(t *testing.T) {
	table := []struct {
		Input      string
		Name       string
		IsGlobal   bool
		IsInstance bool
		IsSystem   bool
	}{

		// Set system variable xx.xx, although xx.xx isn't a system variable, the parser should accept it.
		{"set xx.xx = 666", "xx.xx", false, false, true},
		// Set session system variable xx.xx
		{"set session xx.xx = 666", "xx.xx", false, false, true},
		{"set local xx.xx = 666", "xx.xx", false, false, true},
		{"set global xx.xx = 666", "xx.xx", true, false, true},
		{"set instance xx.xx = 666", "xx.xx", false, true, true},

		{"set @@xx.xx = 666", "xx.xx", false, false, true},
		{"set @@session.xx.xx = 666", "xx.xx", false, false, true},
		{"set @@local.xx.xx = 666", "xx.xx", false, false, true},
		{"set @@global.xx.xx = 666", "xx.xx", true, false, true},
		{"set @@instance.xx.xx = 666", "xx.xx", false, true, true},

		// Set user defined variable xx.xx
		{"set @xx.xx = 666", "xx.xx", false, false, false},
	}

	p := parser.New()
	for _, tbl := range table {
		stmt, err := p.ParseOneStmt(tbl.Input, "", "")
		require.NoError(t, err)

		setStmt, ok := stmt.(*ast.SetStmt)
		require.True(t, ok)
		require.Len(t, setStmt.Variables, 1)

		v := setStmt.Variables[0]
		require.Equal(t, tbl.Name, v.Name)
		require.Equal(t, tbl.IsGlobal, v.IsGlobal)
		require.Equal(t, tbl.IsInstance, v.IsInstance)
		require.Equal(t, tbl.IsSystem, v.IsSystem)
	}

	_, err := p.ParseOneStmt("set xx.xx.xx = 666", "", "")
	require.Error(t, err)
}

func TestFlushTable(t *testing.T) {
	p := parser.New()
	stmt, _, err := p.Parse("flush local tables tbl1,tbl2 with read lock", "", "")
	require.NoError(t, err)
	flushTable := stmt[0].(*ast.FlushStmt)
	require.Equal(t, ast.FlushTables, flushTable.Tp)
	require.Equal(t, "tbl1", flushTable.Tables[0].Name.L)
	require.Equal(t, "tbl2", flushTable.Tables[1].Name.L)
	require.True(t, flushTable.NoWriteToBinLog)
	require.True(t, flushTable.ReadLock)
}

func TestFlushPrivileges(t *testing.T) {
	p := parser.New()
	stmt, _, err := p.Parse("flush privileges", "", "")
	require.NoError(t, err)
	flushPrivilege := stmt[0].(*ast.FlushStmt)
	require.Equal(t, ast.FlushPrivileges, flushPrivilege.Tp)
}

func TestHintError(t *testing.T) {
	p := parser.New()
	stmt, warns, err := p.Parse("select /*+ unknown_hint(T1,t2) */ c1, c2 from t1, t2 where t1.c1 = t2.c1", "", "")
	require.NoError(t, err)
	require.Len(t, warns, 1)
	require.Equal(t, `[parser:8061]Optimizer hint unknown_hint is not supported and is ignored`, warns[0].Error())
	require.Empty(t, stmt[0].(*ast.SelectStmt).TableHints)
	stmt, warns, err = p.Parse("select /*+ HASH_JOIN(t1, T2) unknown_hint(T1,t2, 1) */ c1, c2 from t1, t2 where t1.c1 = t2.c1", "", "")
	require.Len(t, stmt[0].(*ast.SelectStmt).TableHints, 1)
	require.NoError(t, err)
	require.Len(t, warns, 1)
	require.Equal(t, `[parser:8061]Optimizer hint unknown_hint is not supported and is ignored`, warns[0].Error())
	_, _, err = p.Parse("select c1, c2 from /*+ unknown_hint(T1,t2) */ t1, t2 where t1.c1 = t2.c1", "", "")
	require.NoError(t, err) // Hints are ignored after the "FROM" keyword!
	_, _, err = p.Parse("select1 /*+ HASH_JOIN(t1, T2) */ c1, c2 from t1, t2 where t1.c1 = t2.c1", "", "")
	require.EqualError(t, err, "line 1 column 7 near \"select1 /*+ HASH_JOIN(t1, T2) */ c1, c2 from t1, t2 where t1.c1 = t2.c1\" ")
	_, _, err = p.Parse("select /*+ HASH_JOIN(t1, T2) */ c1, c2 fromt t1, t2 where t1.c1 = t2.c1", "", "")
	require.EqualError(t, err, "line 1 column 47 near \"t1, t2 where t1.c1 = t2.c1\" ")
	_, _, err = p.Parse("SELECT 1 FROM DUAL WHERE 1 IN (SELECT /*+ DEBUG_HINT3 */ 1)", "", "")
	require.NoError(t, err)
	stmt, _, err = p.Parse("insert into t select /*+ MAX_EXECUTION_TIME(1000) */ * from t;", "", "")
	require.NoError(t, err)
	require.Empty(t, stmt[0].(*ast.InsertStmt).TableHints)
	require.Len(t, stmt[0].(*ast.InsertStmt).Select.(*ast.SelectStmt).TableHints, 1)
	stmt, _, err = p.Parse("insert /*+ MAX_EXECUTION_TIME(1000) */ into t select * from t;", "", "")
	require.NoError(t, err)
	require.Len(t, stmt[0].(*ast.InsertStmt).TableHints, 1)

	_, warns, err = p.Parse("SELECT id FROM tbl WHERE id = 0 FOR UPDATE /*+ xyz */", "", "")
	require.NoError(t, err)
	require.Len(t, warns, 1)
	require.Regexp(t, `near '/\*\+' at line 1$`, warns[0].Error())

}

// TestDefaultValueParenthesesRoundTrip verifies that the parenthesized form
// of a default value survives a parse/restore round trip. MySQL 8.0.13+
// distinguishes literal defaults (DEFAULT '{}') from expression defaults
// (DEFAULT ('{}')); BLOB/TEXT/JSON/GEOMETRY columns only accept the
// parenthesized form, so dropping the parentheses produces DDL that MySQL
// rejects with Error 1101 (block/spirit#542).
func TestDefaultValueParenthesesRoundTrip(t *testing.T) {
	p := parser.New()
	testCases := []struct {
		sql      string
		expected string
	}{
		// The exact statement from issue #542.
		{
			"alter table t1 add column j json default ('{}')",
			"ALTER TABLE `t1` ADD COLUMN `j` JSON DEFAULT ('{}')",
		},
		// Parenthesized literals keep their parentheses...
		{
			"create table t2 (j json default ('{}'))",
			"CREATE TABLE `t2` (`j` JSON DEFAULT ('{}'))",
		},
		{
			"create table t2 (b blob default ('abc'))",
			"CREATE TABLE `t2` (`b` BLOB DEFAULT ('abc'))",
		},
		{
			"create table t2 (i int default (1))",
			"CREATE TABLE `t2` (`i` INT DEFAULT (1))",
		},
		{
			"create table t2 (d date default (null))",
			"CREATE TABLE `t2` (`d` DATE DEFAULT (NULL))",
		},
		// ...while bare literals stay bare.
		{
			"create table t2 (v varchar(10) default '{}')",
			"CREATE TABLE `t2` (`v` VARCHAR(10) DEFAULT '{}')",
		},
		{
			"create table t2 (i int default 1)",
			"CREATE TABLE `t2` (`i` INT DEFAULT 1)",
		},
		// ALTER ... SET DEFAULT keeps the same distinction.
		{
			"alter table t1 alter column j set default ('{}')",
			"ALTER TABLE `t1` ALTER COLUMN `j` SET DEFAULT ('{}')",
		},
		{
			"alter table t1 alter column i set default 5",
			"ALTER TABLE `t1` ALTER COLUMN `i` SET DEFAULT 5",
		},
		{
			"alter table t1 alter column u set default (uuid())",
			"ALTER TABLE `t1` ALTER COLUMN `u` SET DEFAULT (UUID())",
		},
		// Parenthesized function defaults are unchanged: the parentheses
		// come back from ColumnOption.Restore, not a ParenthesesExpr.
		{
			"create table t2 (u varchar(36) default (uuid()))",
			"CREATE TABLE `t2` (`u` VARCHAR(36) DEFAULT (UUID()))",
		},
	}

	for _, tc := range testCases {
		stmt, err := p.ParseOneStmt(tc.sql, "", "")
		require.NoError(t, err, tc.sql)
		var sb strings.Builder
		err = stmt.Restore(NewRestoreCtx(DefaultRestoreFlags|RestoreStringWithoutCharset, &sb))
		require.NoError(t, err, tc.sql)
		require.Equal(t, tc.expected, sb.String(), tc.sql)

		// The restored SQL must re-parse and restore to the same text
		// (no parentheses gained or lost on further round trips).
		stmt2, err := p.ParseOneStmt(sb.String(), "", "")
		require.NoError(t, err, sb.String())
		var sb2 strings.Builder
		err = stmt2.Restore(NewRestoreCtx(DefaultRestoreFlags|RestoreStringWithoutCharset, &sb2))
		require.NoError(t, err, sb.String())
		require.Equal(t, sb.String(), sb2.String(), tc.sql)
	}
}

// TestKeywordFunctionCalls covers functions whose names lex as keyword
// tokens rather than plain identifiers (block/spirit#1128). The spatial
// constructors are keywords because they double as column types, so
// without dedicated grammar productions they were unparsable as function
// calls anywhere; the remaining FunctionNameConflict members were
// unparsable specifically in DEFAULT expressions. MySQL accepts all of
// these forms and SHOW CREATE TABLE emits them.
func TestKeywordFunctionCalls(t *testing.T) {
	p := parser.New()
	testCases := []struct {
		sql      string
		expected string
	}{
		// DEFAULT (Point(0,0)) is the MySQL manual's own example of an
		// expression default.
		{
			"create table t (g geometry default (point(0,0)))",
			"CREATE TABLE `t` (`g` GEOMETRY DEFAULT (POINT(0, 0)))",
		},
		{
			"alter table t add column g point default (point(0,0))",
			"ALTER TABLE `t` ADD COLUMN `g` POINT DEFAULT (POINT(0, 0))",
		},
		{
			"create table t (g geometry default (linestring(point(0,0), point(1,1))))",
			"CREATE TABLE `t` (`g` GEOMETRY DEFAULT (LINESTRING(POINT(0, 0), POINT(1, 1))))",
		},
		// Non-spatial FunctionNameConflict members in DEFAULT: upstream
		// special-cased only REPLACE, missing the rest of the class.
		{
			"create table t (v varchar(10) default (repeat('a',3)))",
			"CREATE TABLE `t` (`v` VARCHAR(10) DEFAULT (REPEAT('a', 3)))",
		},
		{
			"create table t (v varchar(10) default (left('abc',2)))",
			"CREATE TABLE `t` (`v` VARCHAR(10) DEFAULT (LEFT('abc', 2)))",
		},
		// Spatial constructors in ordinary expression contexts: broken
		// before because only POINT was in FunctionNameConflict.
		{
			"select linestring(point(0,0), point(1,1))",
			"SELECT LINESTRING(POINT(0, 0), POINT(1, 1))",
		},
		{
			"select multipoint(point(0,0)), multilinestring(linestring(point(0,0), point(1,1)))",
			"SELECT MULTIPOINT(POINT(0, 0)),MULTILINESTRING(LINESTRING(POINT(0, 0), POINT(1, 1)))",
		},
		{
			"select geometrycollection(point(0,0), multipolygon(polygon(linestring(point(0,0), point(1,1), point(0,1), point(0,0)))))",
			"SELECT GEOMETRYCOLLECTION(POINT(0, 0), MULTIPOLYGON(POLYGON(LINESTRING(POINT(0, 0), POINT(1, 1), POINT(0, 1), POINT(0, 0)))))",
		},
		{
			"create table t (p point, q point, g geometry generated always as (linestring(p, q)) stored)",
			"CREATE TABLE `t` (`p` POINT,`q` POINT,`g` GEOMETRY GENERATED ALWAYS AS(LINESTRING(`p`, `q`)) STORED)",
		},
		{
			"alter table t add constraint c check (st_length(linestring(p, q)) > 0)",
			"ALTER TABLE `t` ADD CONSTRAINT `c` CHECK(ST_LENGTH(LINESTRING(`p`, `q`))>0) ENFORCED",
		},
		// Parenthesized NOW is an expression default and keeps its shape
		// (MySQL prints DEFAULT (now()) for it); only the bare form folds
		// to CURRENT_TIMESTAMP.
		{
			"create table t (ts datetime default (now()))",
			"CREATE TABLE `t` (`ts` DATETIME DEFAULT (NOW()))",
		},
		{
			"create table t (ts datetime default now())",
			"CREATE TABLE `t` (`ts` DATETIME DEFAULT CURRENT_TIMESTAMP())",
		},
		// Keyword function names still work as plain identifiers.
		{
			"create table t (point int)",
			"CREATE TABLE `t` (`point` INT)",
		},
		{
			"select point from t",
			"SELECT `point` FROM `t`",
		},
	}

	for _, tc := range testCases {
		stmt, err := p.ParseOneStmt(tc.sql, "", "")
		require.NoError(t, err, tc.sql)
		var sb strings.Builder
		err = stmt.Restore(NewRestoreCtx(DefaultRestoreFlags|RestoreStringWithoutCharset, &sb))
		require.NoError(t, err, tc.sql)
		require.Equal(t, tc.expected, sb.String(), tc.sql)

		// The restored SQL must re-parse and restore to the same text.
		stmt2, err := p.ParseOneStmt(sb.String(), "", "")
		require.NoError(t, err, sb.String())
		var sb2 strings.Builder
		err = stmt2.Restore(NewRestoreCtx(DefaultRestoreFlags|RestoreStringWithoutCharset, &sb2))
		require.NoError(t, err, sb.String())
		require.Equal(t, sb.String(), sb2.String(), tc.sql)
	}
}

func TestGroupConcatSeparatorCharsetCollation(t *testing.T) {
	p := parser.New()
	testCases := []struct {
		sql     string
		charset string
		collate string
		sep     string
	}{
		{
			sql:     "select group_concat('x')",
			charset: charset.CharsetLatin1,
			collate: charset.CollationLatin1,
			sep:     ",",
		},
		{
			sql:     "select group_concat('x' separator ';')",
			charset: charset.CharsetLatin1,
			collate: charset.CollationLatin1,
			sep:     ";",
		},
		{
			sql:     "select group_concat('x')",
			charset: mysql.DefaultCharset,
			collate: mysql.DefaultCollationName,
			sep:     ",",
		},
	}

	for _, tc := range testCases {
		stmt, err := p.ParseOneStmt(tc.sql, tc.charset, tc.collate)
		require.NoError(t, err)

		sel, ok := stmt.(*ast.SelectStmt)
		require.True(t, ok)
		require.Len(t, sel.Fields.Fields, 1)

		agg, ok := sel.Fields.Fields[0].Expr.(*ast.AggregateFuncExpr)
		require.True(t, ok)
		require.Equal(t, ast.AggFuncGroupConcat, agg.F)
		require.GreaterOrEqual(t, len(agg.Args), 2)

		separator, ok := agg.Args[len(agg.Args)-1].(*ast.ValueExpr)
		require.True(t, ok)
		require.Equal(t, tc.sep, separator.GetString())
		require.Equal(t, tc.charset, separator.GetType().GetCharset())
		require.Equal(t, tc.collate, separator.GetType().GetCollate())

		// The separator must restore as a plain string literal (no charset
		// introducer): the grammar only accepts `SEPARATOR stringLit`.
		var sb strings.Builder
		require.NoError(t, agg.Restore(NewRestoreCtx(DefaultRestoreFlags, &sb)))
		require.Contains(t, sb.String(), "SEPARATOR '"+tc.sep+"'")
		_, err = p.ParseOneStmt("select "+sb.String(), tc.charset, tc.collate)
		require.NoError(t, err)
	}
}

func TestErrorMsg(t *testing.T) {
	p := parser.New()
	_, _, err := p.Parse("select1 1", "", "")
	require.EqualError(t, err, "line 1 column 7 near \"select1 1\" ")
	_, _, err = p.Parse("select 1 from1 dual", "", "")
	require.EqualError(t, err, "line 1 column 19 near \"dual\" ")
	_, _, err = p.Parse("select * from t1 join t2 from t1.a = t2.a;", "", "")
	require.EqualError(t, err, "line 1 column 29 near \"from t1.a = t2.a;\" ")
	_, _, err = p.Parse("select * from t1 join t2 one t1.a = t2.a;", "", "")
	require.EqualError(t, err, "line 1 column 31 near \"t1.a = t2.a;\" ")
	_, _, err = p.Parse("select * from t1 join t2 on t1.a >>> t2.a;", "", "")
	require.EqualError(t, err, "line 1 column 36 near \"> t2.a;\" ")

	_, _, err = p.Parse("create table t(f_year year(5))ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;", "", "")
	require.EqualError(t, err, "[parser:1818]Supports only YEAR or YEAR(4) column")

	_, _, err = p.Parse("create table ``.t (id int);", "", "")
	require.EqualError(t, err, "[parser:1102]Incorrect database name ''")

	_, _, err = p.Parse("create table ` `.t (id int);", "", "")
	require.EqualError(t, err, "[parser:1102]Incorrect database name ' '")

	// Multi-character ESCAPE arguments are grammatically valid in MySQL
	// (rejected with ER_WRONG_ARGUMENTS at execution time, not 1064), so the
	// parser keeps them as expressions instead of erroring.
	_, _, err = p.Parse("select ifnull(a,0) & ifnull(a,0) like '55' ESCAPE '\\\\a' from t;", "", "")
	require.NoError(t, err)

	_, _, err = p.Parse("load data infile 'aaa' into table aaa FIELDS  Enclosed by '\\\\b';", "", "")
	require.EqualError(t, err, "[parser:1083]Field separator argument is not what is expected; check the manual")

	_, _, err = p.Parse("load data infile 'aaa' into table aaa FIELDS  Escaped by '\\\\b';", "", "")
	require.EqualError(t, err, "[parser:1083]Field separator argument is not what is expected; check the manual")

	_, _, err = p.Parse("load data infile 'aaa' into table aaa FIELDS  Enclosed by '\\\\b' Escaped by '\\\\b' ;", "", "")
	require.EqualError(t, err, "[parser:1083]Field separator argument is not what is expected; check the manual")

	// Unknown character sets and collations are execution-time errors in
	// MySQL (ER_UNKNOWN_CHARACTER_SET 1115 / ER_UNKNOWN_COLLATION 1273, not
	// 1064 syntax errors), e.g. user-defined LDML collations, so the
	// grammar accepts any name here.
	_, _, err = p.Parse("ALTER DATABASE `` CHARACTER SET = ''", "", "")
	require.NoError(t, err)

	_, _, err = p.Parse("ALTER DATABASE t CHARACTER SET = ''", "", "")
	require.NoError(t, err)

	_, _, err = p.Parse("ALTER SCHEMA t CHARACTER SET = 'SOME_INVALID_CHARSET'", "", "")
	require.NoError(t, err)

	_, _, err = p.Parse("ALTER DATABASE t COLLATE = ''", "", "")
	require.NoError(t, err)

	_, _, err = p.Parse("ALTER SCHEMA t COLLATE = 'SOME_INVALID_COLLATION'", "", "")
	require.NoError(t, err)

	_, _, err = p.Parse("ALTER DATABASE CHARSET = 'utf8mb4' COLLATE = 'utf8_bin'", "", "")
	require.EqualError(t, err, "line 1 column 24 near \"= 'utf8mb4' COLLATE = 'utf8_bin'\" ")

	// MySQL enforces Y/N inside its grammar action (ER_WRONG_VALUE 1525, not a
	// 1064 syntax error); we accept any string at parse time like the TABLE
	// option, whose value the storage engine validates at execution instead.
	_, _, err = p.Parse("ALTER DATABASE t ENCRYPTION = ''", "", "")
	require.NoError(t, err)

	_, _, err = p.Parse("ALTER DATABASE", "", "")
	require.EqualError(t, err, "line 1 column 14 near \"\" ")

	_, _, err = p.Parse("ALTER SCHEMA `ANY_DB_NAME`", "", "")
	require.EqualError(t, err, "line 1 column 26 near \"\" ")

	_, _, err = p.Parse("alter table t partition by range FIELDS(a)", "", "")
	require.EqualError(t, err, "[ddl:1492]For RANGE partitions each partition must be defined")

	_, _, err = p.Parse("alter table t partition by list FIELDS(a)", "", "")
	require.EqualError(t, err, "[ddl:1492]For LIST partitions each partition must be defined")

	_, _, err = p.Parse("alter table t partition by list FIELDS(a)", "", "")
	require.EqualError(t, err, "[ddl:1492]For LIST partitions each partition must be defined")

	_, _, err = p.Parse("alter table t partition by list FIELDS(a,b,c)", "", "")
	require.EqualError(t, err, "[ddl:1492]For LIST partitions each partition must be defined")

	_, _, err = p.Parse("alter table t lock = first", "", "")
	require.EqualError(t, err, "[parser:1801]Unknown LOCK type 'first'")

	_, _, err = p.Parse("alter table t lock = start", "", "")
	require.EqualError(t, err, "[parser:1801]Unknown LOCK type 'start'")

	_, _, err = p.Parse("alter table t lock = commit", "", "")
	require.EqualError(t, err, "[parser:1801]Unknown LOCK type 'commit'")

	_, _, err = p.Parse("alter table t lock = binlog", "", "")
	require.EqualError(t, err, "[parser:1801]Unknown LOCK type 'binlog'")

	_, _, err = p.Parse("alter table t lock = randomStr123", "", "")
	require.EqualError(t, err, "[parser:1801]Unknown LOCK type 'randomStr123'")

	// The UNICODE column attribute means CHARACTER SET ucs2; every charset
	// MySQL knows is accepted at parse level.
	_, _, err = p.Parse("create table t (a longtext unicode)", "", "")
	require.NoError(t, err)

	_, _, err = p.Parse("create table t (a long byte, b text unicode)", "", "")
	require.NoError(t, err)

	_, _, err = p.Parse("create table t (a long ascii, b long unicode)", "", "")
	require.NoError(t, err)

	_, _, err = p.Parse("create table t (a text unicode, b mediumtext ascii, c int)", "", "")
	require.NoError(t, err)

	// Unknown collations are execution-time errors (1273), not parse errors.
	_, _, err = p.Parse("select 1 collate some_unknown_collation", "", "")
	require.NoError(t, err)
}

func TestOptimizerHints(t *testing.T) {
	p := parser.New()
	// Test ORDER_INDEX
	stmt, _, err := p.Parse("select /*+ ORDER_INDEX(T1,T2), order_index(t3,t4) */ c1, c2 from t1, t2 where t1.c1 = t2.c1", "", "")
	require.NoError(t, err)
	selectStmt := stmt[0].(*ast.SelectStmt)

	hints := selectStmt.TableHints
	require.Len(t, hints, 2)
	require.Equal(t, "order_index", hints[0].HintName.L)
	require.Len(t, hints[0].Tables, 1)
	require.Equal(t, "t1", hints[0].Tables[0].TableName.L)
	require.Len(t, hints[0].Indexes, 1)
	require.Equal(t, "t2", hints[0].Indexes[0].L)

	require.Equal(t, "order_index", hints[1].HintName.L)
	require.Len(t, hints[1].Tables, 1)
	require.Equal(t, "t3", hints[1].Tables[0].TableName.L)
	require.Len(t, hints[1].Indexes, 1)
	require.Equal(t, "t4", hints[1].Indexes[0].L)

	// Test NO_ORDER_INDEX and RESOURCE_GROUP
	stmt, _, err = p.Parse("select /*+ NO_ORDER_INDEX(T1,T2), no_order_index(t3,t4) RESOURCE_GROUP(rg1)*/ c1, c2 from t1, t2 where t1.c1 = t2.c1", "", "")
	require.NoError(t, err)
	selectStmt = stmt[0].(*ast.SelectStmt)

	hints = selectStmt.TableHints
	require.Len(t, hints, 3)
	require.Equal(t, "no_order_index", hints[0].HintName.L)
	require.Len(t, hints[0].Tables, 1)
	require.Equal(t, "t1", hints[0].Tables[0].TableName.L)
	require.Len(t, hints[0].Indexes, 1)
	require.Equal(t, "t2", hints[0].Indexes[0].L)

	require.Equal(t, "no_order_index", hints[1].HintName.L)
	require.Len(t, hints[1].Tables, 1)
	require.Equal(t, "t3", hints[1].Tables[0].TableName.L)
	require.Len(t, hints[1].Indexes, 1)
	require.Equal(t, "t4", hints[1].Indexes[0].L)

	require.Equal(t, "resource_group", hints[2].HintName.L)
	require.Equal(t, "rg1", hints[2].HintData)

	// Test HASH_JOIN and NO_HASH_JOIN
	stmt, _, err = p.Parse("select /*+ HASH_JOIN(t1, T2), no_hash_join(t3, t4) */ c1, c2 from t1, t2 where t1.c1 = t2.c1", "", "")
	require.NoError(t, err)
	selectStmt = stmt[0].(*ast.SelectStmt)

	hints = selectStmt.TableHints
	require.Len(t, hints, 2)
	require.Equal(t, "hash_join", hints[0].HintName.L)
	require.Len(t, hints[0].Tables, 2)
	require.Equal(t, "t1", hints[0].Tables[0].TableName.L)
	require.Equal(t, "t2", hints[0].Tables[1].TableName.L)

	require.Equal(t, "no_hash_join", hints[1].HintName.L)
	require.Len(t, hints[1].Tables, 2)
	require.Equal(t, "t3", hints[1].Tables[0].TableName.L)
	require.Equal(t, "t4", hints[1].Tables[1].TableName.L)

	// Test MERGE
	stmt, _, err = p.Parse("select /*+ MERGE(), merge(@qb1) */ c1, c2 from t1, t2 where t1.c1 = t2.c1", "", "")
	require.NoError(t, err)
	selectStmt = stmt[0].(*ast.SelectStmt)

	hints = selectStmt.TableHints
	require.Len(t, hints, 2)
	require.Equal(t, "merge", hints[0].HintName.L)
	require.Equal(t, "merge", hints[1].HintName.L)
	require.Equal(t, "qb1", hints[1].QBName.L)

	// Test NO_INDEX_MERGE
	stmt, _, err = p.Parse("select /*+ NO_INDEX_MERGE(), no_index_merge() */ c1, c2 from t1, t2 where t1.c1 = t2.c1", "", "")
	require.NoError(t, err)
	selectStmt = stmt[0].(*ast.SelectStmt)

	hints = selectStmt.TableHints
	require.Len(t, hints, 2)
	require.Equal(t, "no_index_merge", hints[0].HintName.L)
	require.Equal(t, "no_index_merge", hints[1].HintName.L)

	// Test MAX_EXECUTION_TIME
	queries := []string{
		"SELECT /*+ MAX_EXECUTION_TIME(1000) */ * FROM t1 INNER JOIN t2 where t1.c1 = t2.c1",
		"SELECT /*+ MAX_EXECUTION_TIME(1000) */ 1",
		"SELECT /*+ MAX_EXECUTION_TIME(1000) */ SLEEP(20)",
		"SELECT /*+ MAX_EXECUTION_TIME(1000) */ 1 FROM DUAL",
	}
	for i, query := range queries {
		stmt, _, err = p.Parse(query, "", "")
		require.NoError(t, err)
		selectStmt = stmt[0].(*ast.SelectStmt)
		hints = selectStmt.TableHints
		require.Lenf(t, hints, 1, "case %d", i)
		require.Equal(t, "max_execution_time", hints[0].HintName.L)
		require.Equal(t, uint64(1000), hints[0].HintData.(uint64))
	}

	// Test QB_NAME
	stmt, _, err = p.Parse("select /*+ QB_NAME(qb1) */ c1, c2 from t1, t2 where t1.c1 = t2.c1", "", "")
	require.NoError(t, err)
	selectStmt = stmt[0].(*ast.SelectStmt)
	hints = selectStmt.TableHints
	require.Len(t, hints, 1)
	require.Equal(t, "qb_name", hints[0].HintName.L)
	require.Equal(t, "qb1", hints[0].QBName.L)

	// Test SET_VAR
	stmt, _, err = p.Parse("select /*+ SET_VAR(sql_mode = 'ANSI') */ c1, c2 from t1, t2 where t1.c1 = t2.c1", "", "")
	require.NoError(t, err)
	selectStmt = stmt[0].(*ast.SelectStmt)
	hints = selectStmt.TableHints
	require.Len(t, hints, 1)
	require.Equal(t, "set_var", hints[0].HintName.L)
	require.Equal(t, ast.HintSetVar{VarName: "sql_mode", Value: "ANSI"}, hints[0].HintData)

	// MySQL hints that the parser recognizes but does not model produce a
	// warning and are dropped from the AST.
	stmt, warns, err := p.Parse("select /*+ BKA(t1), NO_BNL(t2), JOIN_ORDER(t3, t4), MRR(t5 idx1), SEMIJOIN(FIRSTMATCH) */ c1 from t1", "", "")
	require.NoError(t, err)
	require.Len(t, warns, 5)
	selectStmt = stmt[0].(*ast.SelectStmt)
	require.Empty(t, selectStmt.TableHints)

	// TiDB-only hint names are unknown identifiers now, and also warn.
	stmt, warns, err = p.Parse("select /*+ INL_JOIN(t1, t2), HASH_AGG(), READ_FROM_STORAGE(TIFLASH[t1]) */ c1 from t1", "", "")
	require.NoError(t, err)
	require.NotEmpty(t, warns)
	selectStmt = stmt[0].(*ast.SelectStmt)
	require.Empty(t, selectStmt.TableHints)
}

func TestParserErrMsg(t *testing.T) {
	commentMsgCases := []testErrMsgCase{
		{"delete from t where a = 7 or 1=1/*' and b = 'p'", errors.New("near '/*' and b = 'p'' at line 1")},
		{"delete from t where a = 7 or\n 1=1/*' and b = 'p'", errors.New("near '/*' and b = 'p'' at line 2")},
		{"select 1/*", errors.New("near '/*' at line 1")},
		{"select 1/* comment */", nil},
	}
	funcCallMsgCases := []testErrMsgCase{
		{"select a.b()", nil},
		{"SELECT foo.bar('baz');", nil},
	}
	RunErrMsgTest(t, commentMsgCases)
	RunErrMsgTest(t, funcCallMsgCases)
}

func checkOrderBy(t *testing.T, s ast.Node, hasOrderBy []bool, i int) int {
	switch x := s.(type) {
	case *ast.SelectStmt:
		require.Equal(t, hasOrderBy[i], x.OrderBy != nil)
		return i + 1
	case *ast.SetOprSelectList:
		for _, sel := range x.Selects {
			i = checkOrderBy(t, sel, hasOrderBy, i)
		}
		return i
	}
	return i
}

func TestUnionOrderBy(t *testing.T) {
	p := parser.New()
	p.EnableWindowFunc(false)

	tests := []struct {
		src        string
		hasOrderBy []bool
	}{
		{"select 2 as a from dual union select 1 as b from dual order by a", []bool{false, false, true}},
		{"select 2 as a from dual union (select 1 as b from dual order by a)", []bool{false, true, false}},
		{"(select 2 as a from dual order by a) union select 1 as b from dual order by a", []bool{true, false, true}},
		{"select 1 a, 2 b from dual order by a", []bool{true}},
		{"select 1 a, 2 b from dual", []bool{false}},
	}

	for _, tbl := range tests {
		stmt, _, err := p.Parse(tbl.src, "", "")
		require.NoError(t, err)
		us, ok := stmt[0].(*ast.SetOprStmt)
		if ok {
			var i int
			for _, s := range us.SelectList.Selects {
				i = checkOrderBy(t, s, tbl.hasOrderBy, i)
			}
			require.Equal(t, tbl.hasOrderBy[i], us.OrderBy != nil)
		}
		ss, ok := stmt[0].(*ast.SelectStmt)
		if ok {
			require.Equal(t, tbl.hasOrderBy[0], ss.OrderBy != nil)
		}
	}
}

func TestSQLNoCache(t *testing.T) {
	table := []testCase{
		{`select SQL_NO_CACHE * from t`, false, ""},
		{`select SQL_CACHE * from t`, true, "SELECT * FROM `t`"},
		{`select * from t`, true, "SELECT * FROM `t`"},
	}

	p := parser.New()
	for _, tbl := range table {
		stmt, _, err := p.Parse(tbl.src, "", "")
		require.NoError(t, err)

		sel := stmt[0].(*ast.SelectStmt)
		require.Equal(t, tbl.ok, sel.SQLCache)
	}
}

func TestFuncCallExprOffset(t *testing.T) {
	// Test case for offset field on func call expr.
	p := parser.New()
	stmt, _, err := p.Parse("SELECT s.a(), b();", "", "")
	require.NoError(t, err)
	ss := stmt[0].(*ast.SelectStmt)
	fields := ss.Fields.Fields
	require.Len(t, fields, 2)

	{
		// s.a()
		expr := fields[0].Expr
		f, ok := expr.(*ast.FuncCallExpr)
		require.True(t, ok)
		require.Equal(t, 7, f.OriginTextPosition())
	}

	{
		// b()
		expr := fields[1].Expr
		f, ok := expr.(*ast.FuncCallExpr)
		require.True(t, ok)
		require.Equal(t, 14, f.OriginTextPosition())
	}
}

func TestSQLModeANSIQuotes(t *testing.T) {
	p := parser.New()
	p.SetSQLMode(mysql.ModeANSIQuotes)
	tests := []string{
		`CREATE TABLE "table" ("id" int)`,
		`select * from t "tt"`,
	}
	for _, test := range tests {
		_, _, err := p.Parse(test, "", "")
		require.NoError(t, err)
	}
}

func TestDDLStatements(t *testing.T) {
	p := parser.New()
	// Tests that whatever the charset it is define, we always assign utf8 charset and utf8_bin collate.
	createTableStr := `CREATE TABLE t (
		a varchar(64) binary,
		b char(10) charset utf8 collate utf8_general_ci,
		c text charset latin1) ENGINE=innoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin`
	stmts, _, err := p.Parse(createTableStr, "", "")
	require.NoError(t, err)
	stmt := stmts[0].(*ast.CreateTableStmt)
	require.True(t, mysql.HasBinaryFlag(stmt.Cols[0].Tp.GetFlag()))
	for _, colDef := range stmt.Cols[1:] {
		require.False(t, mysql.HasBinaryFlag(colDef.Tp.GetFlag()))
	}
	for _, tblOpt := range stmt.Options {
		switch tblOpt.Tp { //nolint:exhaustive
		case ast.TableOptionCharset:
			require.Equal(t, "utf8", tblOpt.StrValue)
		case ast.TableOptionCollate:
			require.Equal(t, "utf8_bin", tblOpt.StrValue)
		}
	}
	createTableStr = `CREATE TABLE t (
		a varbinary(64),
		b binary(10),
		c blob)`
	stmts, _, err = p.Parse(createTableStr, "", "")
	require.NoError(t, err)
	stmt = stmts[0].(*ast.CreateTableStmt)
	for _, colDef := range stmt.Cols {
		require.Equal(t, charset.CharsetBin, colDef.Tp.GetCharset())
		require.Equal(t, charset.CollationBin, colDef.Tp.GetCollate())
		require.True(t, mysql.HasBinaryFlag(colDef.Tp.GetFlag()))
	}
	// Test set collate for all column types
	createTableStr = `CREATE TABLE t (
		c_int int collate utf8_bin,
		c_real real collate utf8_bin,
		c_float float collate utf8_bin,
		c_bool bool collate utf8_bin,
		c_char char collate utf8_bin,
		c_binary binary collate utf8_bin,
		c_varchar varchar(2) collate utf8_bin,
		c_year year collate utf8_bin,
		c_date date collate utf8_bin,
		c_time time collate utf8_bin,
		c_datetime datetime collate utf8_bin,
		c_timestamp timestamp collate utf8_bin,
		c_tinyblob tinyblob collate utf8_bin,
		c_blob blob collate utf8_bin,
		c_mediumblob mediumblob collate utf8_bin,
		c_longblob longblob collate utf8_bin,
		c_bit bit collate utf8_bin,
		c_long_varchar long varchar collate utf8_bin,
		c_tinytext tinytext collate utf8_bin,
		c_text text collate utf8_bin,
		c_mediumtext mediumtext collate utf8_bin,
		c_longtext longtext collate utf8_bin,
		c_decimal decimal collate utf8_bin,
		c_numeric numeric collate utf8_bin,
		c_enum enum('1') collate utf8_bin,
		c_set set('1') collate utf8_bin,
		c_json json collate utf8_bin)`
	_, _, err = p.Parse(createTableStr, "", "")
	require.NoError(t, err)

	createTableStr = `CREATE TABLE t (c_double double(10))`
	_, _, err = p.Parse(createTableStr, "", "")
	require.EqualError(t, err, "[parser:1149]You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use")
	p.SetStrictDoubleTypeCheck(false)
	_, _, err = p.Parse(createTableStr, "", "")
	require.NoError(t, err)
	p.SetStrictDoubleTypeCheck(true)

	createTableStr = `CREATE TABLE t (c_double double(10, 2))`
	_, _, err = p.Parse(createTableStr, "", "")
	require.NoError(t, err)

	// GLOBAL TEMPORARY tables are a TiDB extension and no longer parse.
	createTableStr = `create global temporary table t010(local_01 int, local_03 varchar(20)) on commit preserve rows`
	_, _, err = p.Parse(createTableStr, "", "")
	require.Error(t, err)
}

func TestGeneratedColumn(t *testing.T) {
	tests := []struct {
		input string
		ok    bool
		expr  string
	}{
		{"create table t (c int, d int generated always as (c + 1) virtual)", true, "c + 1"},
		{"create table t (c int, d int as (   c + 1   ) virtual)", true, "c + 1"},
		{"create table t (c int, d int as (1 + 1) stored)", true, "1 + 1"},
	}
	p := parser.New()
	for _, tbl := range tests {
		stmtNodes, _, err := p.Parse(tbl.input, "", "")
		if tbl.ok {
			require.NoError(t, err)
			stmtNode := stmtNodes[0]
			for _, col := range stmtNode.(*ast.CreateTableStmt).Cols {
				for _, opt := range col.Options {
					if opt.Tp == ast.ColumnOptionGenerated {
						require.Equal(t, tbl.expr, opt.Expr.Text())
					}
				}
			}
		} else {
			require.Error(t, err)
		}
	}

	_, _, err := p.Parse("create table t1 (a int, b int as (a + 1) default 10);", "", "")
	require.EqualError(t, err, "[ddl:1221]Incorrect usage of DEFAULT and generated column")
	_, _, err = p.Parse("create table t1 (a int, b int as (a + 1) on update now());", "", "")
	require.EqualError(t, err, "[ddl:1221]Incorrect usage of ON UPDATE and generated column")
	_, _, err = p.Parse("create table t1 (a int, b int as (a + 1) auto_increment);", "", "")
	require.EqualError(t, err, "[ddl:1221]Incorrect usage of AUTO_INCREMENT and generated column")
}

func TestSideEffect(t *testing.T) {
	// This test cover a bug that parse an error SQL doesn't leave the parser in a
	// clean state, cause the following SQL parse fail.
	p := parser.New()
	_, err := p.ParseOneStmt("create table t /*!50100 'abc', 'abc' */;", "", "")
	require.Error(t, err)

	_, err = p.ParseOneStmt("show tables;", "", "")
	require.NoError(t, err)
}

func TestTablePartitionNameList(t *testing.T) {
	table := []testCase{
		{`select * from t partition (p0,p1)`, true, ""},
	}

	p := parser.New()
	for _, tbl := range table {
		stmt, _, err := p.Parse(tbl.src, "", "")
		require.NoError(t, err)

		sel := stmt[0].(*ast.SelectStmt)
		source, ok := sel.From.TableRefs.Left.(*ast.TableSource)
		require.True(t, ok)
		tableName, ok := source.Source.(*ast.TableName)
		require.True(t, ok)
		require.Len(t, tableName.PartitionNames, 2)
		require.Equal(t, ast.CIStr{O: "p0", L: "p0"}, tableName.PartitionNames[0])
		require.Equal(t, ast.CIStr{O: "p1", L: "p1"}, tableName.PartitionNames[1])
	}
}

func TestNotExistsSubquery(t *testing.T) {
	table := []testCase{
		{`select * from t1 where not exists (select * from t2 where t1.a = t2.a)`, true, ""},
	}

	p := parser.New()
	for _, tbl := range table {
		stmt, _, err := p.Parse(tbl.src, "", "")
		require.NoError(t, err)

		sel := stmt[0].(*ast.SelectStmt)
		exists, ok := sel.Where.(*ast.ExistsSubqueryExpr)
		require.True(t, ok)
		require.Equal(t, tbl.ok, exists.Not)
	}
}

// For issue #51
// See https://github.com/pingcap/parser/pull/51 for details
func TestFieldText(t *testing.T) {
	p := parser.New()
	stmts, _, err := p.Parse("select a from t", "", "")
	require.NoError(t, err)
	tmp := stmts[0].(*ast.SelectStmt)
	require.Equal(t, "a", tmp.Fields.Fields[0].Text())

}

// See https://github.com/pingcap/parser/issue/94
func TestQuotedSystemVariables(t *testing.T) {
	p := parser.New()

	st, err := p.ParseOneStmt(
		"select @@Sql_Mode, @@`SQL_MODE`, @@session.`sql_mode`, @@global.`s ql``mode`, @@session.'sql\\nmode', @@local.\"sql\\\"mode\", @@instance.sql_mode;",
		"",
		"",
	)
	require.NoError(t, err)
	ss := st.(*ast.SelectStmt)
	expected := []*ast.VariableExpr{
		{
			Name:          "sql_mode",
			IsGlobal:      false,
			IsSystem:      true,
			ExplicitScope: false,
		},
		{
			Name:          "sql_mode",
			IsGlobal:      false,
			IsSystem:      true,
			ExplicitScope: false,
		},
		{
			Name:          "sql_mode",
			IsGlobal:      false,
			IsSystem:      true,
			ExplicitScope: true,
		},
		{
			Name:          "s ql`mode",
			IsGlobal:      true,
			IsSystem:      true,
			ExplicitScope: true,
		},
		{
			Name:          "sql\nmode",
			IsGlobal:      false,
			IsSystem:      true,
			ExplicitScope: true,
		},
		{
			Name:          `sql"mode`,
			IsGlobal:      false,
			IsSystem:      true,
			ExplicitScope: true,
		},
		{
			Name:          "sql_mode",
			IsGlobal:      false,
			IsSystem:      true,
			IsInstance:    true,
			ExplicitScope: true,
		},
	}

	require.Len(t, ss.Fields.Fields, len(expected))
	for i, field := range ss.Fields.Fields {
		ve := field.Expr.(*ast.VariableExpr)
		comment := fmt.Sprintf("field %d, ve = %v", i, ve)
		require.Equal(t, expected[i].Name, ve.Name, comment)
		require.Equal(t, expected[i].IsGlobal, ve.IsGlobal, comment)
		require.Equal(t, expected[i].IsInstance, ve.IsInstance, comment)
		require.Equal(t, expected[i].IsSystem, ve.IsSystem, comment)
		require.Equal(t, expected[i].ExplicitScope, ve.ExplicitScope, comment)
	}
}

// See https://github.com/pingcap/parser/issue/95
func TestQuotedVariableColumnName(t *testing.T) {
	p := parser.New()

	st, err := p.ParseOneStmt(
		"select @abc, @`abc`, @'aBc', @\"AbC\", @6, @`6`, @'6', @\"6\", @@sql_mode, @@`sql_mode`, @;",
		"",
		"",
	)
	require.NoError(t, err)
	ss := st.(*ast.SelectStmt)
	expected := []string{
		"@abc",
		"@`abc`",
		"@'aBc'",
		`@"AbC"`,
		"@6",
		"@`6`",
		"@'6'",
		`@"6"`,
		"@@sql_mode",
		"@@`sql_mode`",
		"@",
	}

	require.Len(t, ss.Fields.Fields, len(expected))
	for i, field := range ss.Fields.Fields {
		require.Equal(t, expected[i], field.Text())
	}
}

func TestCharset(t *testing.T) {
	p := parser.New()

	st, err := p.ParseOneStmt("ALTER SCHEMA GLOBAL DEFAULT CHAR SET utf8mb4", "", "")
	require.NoError(t, err)
	require.NotNil(t, st.(*ast.AlterDatabaseStmt))
	st, err = p.ParseOneStmt("ALTER DATABASE CHAR SET = utf8mb4", "", "")
	require.NoError(t, err)
	require.NotNil(t, st.(*ast.AlterDatabaseStmt))
	st, err = p.ParseOneStmt("ALTER DATABASE DEFAULT CHAR SET = utf8mb4", "", "")
	require.NoError(t, err)
	require.NotNil(t, st.(*ast.AlterDatabaseStmt))
}

func TestUnderscoreCharset(t *testing.T) {
	p := parser.New()
	tests := []struct {
		cs        string
		parseFail bool
	}{
		// Every charset MySQL knows works as an introducer; a name the lexer
		// does not recognize is just an identifier, so the string literal
		// after it is a syntax error.
		{"utf8", false},
		{"gbk", false},
		{"ujis", false},
		{"ucs2", false},
		{"latin2", false},
		{"utf16le", false},
		{"gbk1", true},
		{"ujisx", true},
	}
	for _, tt := range tests {
		sql := fmt.Sprintf("select hex(_%s '3F')", tt.cs)
		_, err := p.ParseOneStmt(sql, "", "")
		if tt.parseFail {
			require.EqualError(t, err, fmt.Sprintf("line 1 column %d near \"'3F')\" ", len(tt.cs)+17))
		} else {
			require.NoError(t, err)
		}
	}
}

func TestFulltextSearch(t *testing.T) {
	p := parser.New()

	st, err := p.ParseOneStmt("SELECT * FROM fulltext_test WHERE MATCH(content) AGAINST('search')", "", "")
	require.NoError(t, err)
	require.NotNil(t, st.(*ast.SelectStmt))

	st, err = p.ParseOneStmt("SELECT * FROM fulltext_test WHERE MATCH() AGAINST('search')", "", "")
	require.Error(t, err)
	require.Nil(t, st)

	st, err = p.ParseOneStmt("SELECT * FROM fulltext_test WHERE MATCH(content) AGAINST()", "", "")
	require.Error(t, err)
	require.Nil(t, st)

	st, err = p.ParseOneStmt("SELECT * FROM fulltext_test WHERE MATCH(content) AGAINST('search' IN)", "", "")
	require.Error(t, err)
	require.Nil(t, st)

	st, err = p.ParseOneStmt("SELECT * FROM fulltext_test WHERE MATCH(content) AGAINST('search' IN BOOLEAN MODE WITH QUERY EXPANSION)", "", "")
	require.Error(t, err)
	require.Nil(t, st)

	restoreWhere := func(st ast.StmtNode) string {
		var sb strings.Builder
		require.NoError(t, st.(*ast.SelectStmt).Where.Restore(NewRestoreCtx(DefaultRestoreFlags, &sb)))
		return sb.String()
	}

	st, err = p.ParseOneStmt("SELECT * FROM fulltext_test WHERE MATCH(title,content) AGAINST('search' IN NATURAL LANGUAGE MODE)", "", "")
	require.NoError(t, err)
	require.NotNil(t, st.(*ast.SelectStmt))
	require.Equal(t, "MATCH (`title`,`content`) AGAINST (_UTF8MB4'search')", restoreWhere(st))

	st, err = p.ParseOneStmt("SELECT * FROM fulltext_test WHERE MATCH(title,content) AGAINST('search' IN BOOLEAN MODE)", "", "")
	require.NoError(t, err)
	require.NotNil(t, st.(*ast.SelectStmt))
	require.Equal(t, "MATCH (`title`,`content`) AGAINST (_UTF8MB4'search' IN BOOLEAN MODE)", restoreWhere(st))

	st, err = p.ParseOneStmt("SELECT * FROM fulltext_test WHERE MATCH(title,content) AGAINST('search' WITH QUERY EXPANSION)", "", "")
	require.NoError(t, err)
	require.NotNil(t, st.(*ast.SelectStmt))
	require.Equal(t, "MATCH (`title`,`content`) AGAINST (_UTF8MB4'search' WITH QUERY EXPANSION)", restoreWhere(st))
}

func TestSignedInt64OutOfRange(t *testing.T) {
	p := parser.New()
	cases := []string{
		"create user abc@def with max_queries_per_hour 18446744073709551612",
	}

	for _, s := range cases {
		_, err := p.ParseOneStmt(s, "", "")
		require.Error(t, err)
		require.Contains(t, err.Error(), "out of range")
	}
}

// CleanNodeText set the text of node and all child node empty.
// For test only.
type nodeTextCleaner struct {
}

// Enter implements Visitor interface.
func (checker *nodeTextCleaner) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	in.SetText(nil, "")
	in.SetOriginTextPosition(0)
	if v, ok := in.(*ast.ValueExpr); ok && v != nil {
		tpFlag := v.GetType().GetFlag()
		if tpFlag&mysql.UnderScoreCharsetFlag != 0 {
			// ignore underscore charset flag to let `'abc' = _utf8'abc'` pass
			tpFlag ^= mysql.UnderScoreCharsetFlag
			v.GetType().SetFlag(tpFlag)
		}
		if v.Kind() == ast.KindMysqlDecimal {
			_ = v.GetMysqlDecimal().FromString(v.GetMysqlDecimal().ToString())
		}
	}

	switch node := in.(type) {
	case *ast.PatternLikeExpr:
		if node.Escape == '\\' {
			node.EscapeExplicit = false
		}
	case *ast.CreateTableStmt:
		for _, opt := range node.Options {
			switch opt.Tp { //nolint:exhaustive
			case ast.TableOptionCharset:
				opt.StrValue = strings.ToUpper(opt.StrValue)
			case ast.TableOptionCollate:
				opt.StrValue = strings.ToUpper(opt.StrValue)
			}
		}
		for _, col := range node.Cols {
			col.Tp.SetCharset(strings.ToUpper(col.Tp.GetCharset()))
			col.Tp.SetCollate(strings.ToUpper(col.Tp.GetCollate()))

			for i, option := range col.Options {
				if option.Tp == 0 && option.Expr == nil && !option.Stored && option.Refer == nil {
					col.Options = slices.Delete(col.Options, i, i+1)
				}
			}
		}
	case *ast.DeleteStmt:
		for _, tableHint := range node.TableHints {
			tableHint.HintName.O = ""
		}
	case *ast.UpdateStmt:
		for _, tableHint := range node.TableHints {
			tableHint.HintName.O = ""
		}
	case *ast.Constraint:
		if node.Option != nil {
			if node.Option.KeyBlockSize == 0x0 && node.Option.Tp == 0 && node.Option.Comment == "" {
				node.Option = nil
			}
		}
	case *ast.FuncCallExpr:
		node.FnName.O = strings.ToLower(node.FnName.O)
		node.SetOriginTextPosition(0)
	case *ast.AggregateFuncExpr:
		node.F = strings.ToLower(node.F)
	case *ast.SelectField:
		node.Offset = 0
	case *ast.GrantStmt:
		var privs []*ast.PrivElem
		for _, v := range node.Privs {
			if v.Priv != 0 {
				privs = append(privs, v)
			}
		}
		node.Privs = privs
	case *ast.AlterTableStmt:
		var specs []*ast.AlterTableSpec
		for _, v := range node.Specs {
			if v.Tp != 0 && (v.Tp != ast.AlterTableOption || len(v.Options) != 0) {
				specs = append(specs, v)
			}
		}
		node.Specs = specs
	case *ast.Join:
		node.ExplicitParens = false
	case *ast.ColumnDef:
		node.Tp.CleanElemIsBinaryLit()
	}
	return in, false
}

// CleanNodeText set the text of node and all child node empty.
func CleanNodeText(node ast.Node) {
	var cleaner nodeTextCleaner
	node.Accept(&cleaner)
}

func (checker *nodeTextCleaner) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}

// For BRIE
func TestHighNotPrecedenceMode(t *testing.T) {
	p := parser.New()
	var sb strings.Builder

	sms, _, err := p.Parse("SELECT NOT 1 BETWEEN -5 AND 5", "", "")
	require.NoError(t, err)
	v, ok := sms[0].(*ast.SelectStmt)
	require.True(t, ok)
	v1, ok := v.Fields.Fields[0].Expr.(*ast.UnaryOperationExpr)
	require.True(t, ok)
	require.Equal(t, opcode.Not, v1.Op)
	err = sms[0].Restore(NewRestoreCtx(DefaultRestoreFlags, &sb))
	require.NoError(t, err)
	restoreSQL := sb.String()
	require.Equal(t, "SELECT NOT 1 BETWEEN -5 AND 5", restoreSQL)
	sb.Reset()

	sms, _, err = p.Parse("SELECT !1 BETWEEN -5 AND 5", "", "")
	require.NoError(t, err)
	v, ok = sms[0].(*ast.SelectStmt)
	require.True(t, ok)
	_, ok = v.Fields.Fields[0].Expr.(*ast.BetweenExpr)
	require.True(t, ok)
	err = sms[0].Restore(NewRestoreCtx(DefaultRestoreFlags, &sb))
	require.NoError(t, err)
	restoreSQL = sb.String()
	require.Equal(t, "SELECT !1 BETWEEN -5 AND 5", restoreSQL)
	sb.Reset()

	p = parser.New()
	p.SetSQLMode(mysql.ModeHighNotPrecedence)
	sms, _, err = p.Parse("SELECT NOT 1 BETWEEN -5 AND 5", "", "")
	require.NoError(t, err)
	v, ok = sms[0].(*ast.SelectStmt)
	require.True(t, ok)
	_, ok = v.Fields.Fields[0].Expr.(*ast.BetweenExpr)
	require.True(t, ok)
	err = sms[0].Restore(NewRestoreCtx(DefaultRestoreFlags, &sb))
	require.NoError(t, err)
	restoreSQL = sb.String()
	require.Equal(t, "SELECT !1 BETWEEN -5 AND 5", restoreSQL)
}

// For CTE
func TestWithoutCharsetFlags(t *testing.T) {
	type testCaseWithFlag struct {
		src     string
		ok      bool
		restore string
		flag    RestoreFlags
	}

	flag := RestoreStringSingleQuotes | RestoreSpacesAroundBinaryOperation | RestoreBracketAroundBinaryOperation | RestoreNameBackQuotes
	cases := []testCaseWithFlag{
		{"select 'a'", true, "SELECT 'a'", flag | RestoreStringWithoutCharset},
		{"select _utf8'a'", true, "SELECT 'a'", flag | RestoreStringWithoutCharset},
		{"select _utf8mb4'a'", true, "SELECT 'a'", flag | RestoreStringWithoutCharset},
		{"select _utf8 X'D0B1'", true, "SELECT x'd0b1'", flag | RestoreStringWithoutCharset},

		{"select _utf8mb4'a'", true, "SELECT 'a'", flag | RestoreStringWithoutDefaultCharset},
		{"select _utf8'a'", true, "SELECT _utf8'a'", flag | RestoreStringWithoutDefaultCharset},
		{"select _utf8'a'", true, "SELECT _utf8'a'", flag | RestoreStringWithoutDefaultCharset},
		{"select _utf8 X'D0B1'", true, "SELECT _utf8 x'd0b1'", flag | RestoreStringWithoutDefaultCharset},
	}

	p := parser.New()
	p.EnableWindowFunc(false)
	for _, tbl := range cases {
		stmts, _, err := p.Parse(tbl.src, "", "")
		if !tbl.ok {
			require.Error(t, err)
			continue
		}
		require.NoError(t, err)
		// restore correctness test
		var sb strings.Builder
		restoreSQLs := ""
		for _, stmt := range stmts {
			sb.Reset()
			ctx := NewRestoreCtx(tbl.flag, &sb)
			ctx.DefaultDB = "test"
			err = stmt.Restore(ctx)
			require.NoError(t, err)
			restoreSQL := sb.String()
			if restoreSQLs != "" {
				restoreSQLs += "; "
			}
			restoreSQLs += restoreSQL
		}
		require.Equal(t, tbl.restore, restoreSQLs)
	}
}

func TestRestoreBinOpWithBrackets(t *testing.T) {
	cases := []testCase{
		{"select mod(a+b, 4)+1", true, "SELECT (((`a` + `b`) % 4) + 1)"},
		{"SELECT MOD(10, 2 BETWEEN 0 and 5)", true, "SELECT (10 % (2 BETWEEN 0 AND 5))"}, // issue #59000
		{"select mod( year(a) - abs(weekday(a) + dayofweek(a)), 4) + 1", true, "SELECT (((year(`a`) - abs((weekday(`a`) + dayofweek(`a`)))) % 4) + 1)"},
	}

	p := parser.New()
	p.EnableWindowFunc(false)
	for _, tbl := range cases {
		_, _, err := p.Parse(tbl.src, "", "")
		comment := fmt.Sprintf("source %v", tbl.src)
		if !tbl.ok {
			require.Error(t, err, comment)
			continue
		}
		require.NoError(t, err, comment)
		// restore correctness test
		if tbl.ok {
			var sb strings.Builder
			comment := fmt.Sprintf("source %v", tbl.src)
			stmts, _, err := p.Parse(tbl.src, "", "")
			require.NoError(t, err, comment)
			restoreSQLs := ""
			for _, stmt := range stmts {
				sb.Reset()
				ctx := NewRestoreCtx(RestoreStringSingleQuotes|RestoreSpacesAroundBinaryOperation|RestoreBracketAroundBinaryOperation|RestoreStringWithoutCharset|RestoreNameBackQuotes, &sb)
				ctx.DefaultDB = "test"
				err = stmt.Restore(ctx)
				require.NoError(t, err, comment)
				restoreSQL := sb.String()
				comment = fmt.Sprintf("source %v; restore %v", tbl.src, restoreSQL)
				if restoreSQLs != "" {
					restoreSQLs += "; "
				}
				restoreSQLs += restoreSQL
			}
			comment = fmt.Sprintf("restore %v; expect %v", restoreSQLs, tbl.restore)
			require.Equal(t, tbl.restore, restoreSQLs, comment)
		}
	}
}

// For CTE bindings.
func TestCTEBindings(t *testing.T) {
	table := []testCase{
		{"WITH `cte` AS (SELECT * from t) SELECT `col1`,`col2` FROM `cte`", true, "WITH `cte` AS (SELECT * FROM `test`.`t`) SELECT `col1`,`col2` FROM `cte`"},
		{"WITH `cte` (col1, col2) AS (SELECT * from t UNION ALL SELECT 3,4) SELECT col1, col2 FROM cte;", true, "WITH `cte` (`col1`, `col2`) AS (SELECT * FROM `test`.`t` UNION ALL SELECT 3,4) SELECT `col1`,`col2` FROM `cte`"},
		{"WITH `cte` AS (SELECT * from t), cte2 as (select * from cte) SELECT `col1`,`col2` FROM `cte`", true, "WITH `cte` AS (SELECT * FROM `test`.`t`), `cte2` AS (SELECT * FROM `cte`) SELECT `col1`,`col2` FROM `cte`"},
		{"WITH RECURSIVE cte (n) AS (  SELECT * from t  UNION ALL  SELECT n + 1 FROM cte WHERE n < 5)SELECT * FROM cte;", true, "WITH RECURSIVE `cte` (`n`) AS (SELECT * FROM `test`.`t` UNION ALL SELECT `n` + 1 FROM `cte` WHERE `n` < 5) SELECT * FROM `cte`"},
		{"with cte(a) as (select * from t) update t, cte set t.a=1  where t.a=cte.a;", true, "WITH `cte` (`a`) AS (SELECT * FROM `test`.`t`) UPDATE (`test`.`t`) JOIN `cte` SET `t`.`a`=1 WHERE `t`.`a` = `cte`.`a`"},
		{"with cte(a) as (select * from t) delete t from t, cte where t.a=cte.a;", true, "WITH `cte` (`a`) AS (SELECT * FROM `test`.`t`) DELETE `test`.`t` FROM (`test`.`t`) JOIN `cte` WHERE `t`.`a` = `cte`.`a`"},
		{"WITH cte1 AS (SELECT * from t) SELECT * FROM (WITH cte2 AS (SELECT * from cte1) SELECT * FROM cte2 JOIN cte1) AS dt;", true, "WITH `cte1` AS (SELECT * FROM `test`.`t`) SELECT * FROM (WITH `cte2` AS (SELECT * FROM `cte1`) SELECT * FROM `cte2` JOIN `cte1`) AS `dt`"},
		{"WITH cte AS (SELECT * from t) SELECT /*+ MAX_EXECUTION_TIME(1000) */ * FROM cte;", true, "WITH `cte` AS (SELECT * FROM `test`.`t`) SELECT /*+ MAX_EXECUTION_TIME(1000)*/ * FROM `cte`"},
		{"with cte as (table t) table cte;", true, "WITH `cte` AS (TABLE `test`.`t`) TABLE `cte`"},
		{"with cte as (select * from t) select 1 union with cte as (select * from t) select * from cte;", false, ""},
		{"with cte as (select * from t) (select * from t);", true, "WITH `cte` AS (SELECT * FROM `test`.`t`) (SELECT * FROM `test`.`t`)"},
		{"with cte as (select 1) (select 1 union select * from t)", true, "WITH `cte` AS (SELECT 1) (SELECT 1 UNION SELECT * FROM `test`.`t`)"},
		{"select * from (with cte as (select * from t) select 1 union select * from t) qn", true, "SELECT * FROM (WITH `cte` AS (SELECT * FROM `test`.`t`) SELECT 1 UNION SELECT * FROM `test`.`t`) AS `qn`"},
		{"select * from t where 1 > (with cte as (select * from t) select * from cte)", true, "SELECT * FROM `test`.`t` WHERE 1 > (WITH `cte` AS (SELECT * FROM `test`.`t`) SELECT * FROM `cte`)"},
		{"( with cte(n) as ( select * from t )  select n+1 from cte  union select n+2 from cte) union select 1", true, "(WITH `cte` (`n`) AS (SELECT * FROM `test`.`t`) SELECT `n` + 1 FROM `cte` UNION SELECT `n` + 2 FROM `cte`) UNION SELECT 1"},
		{"( with cte(n) as ( select * from t )  select n+1 from cte) union select * from t", true, "(WITH `cte` (`n`) AS (SELECT * FROM `test`.`t`) SELECT `n` + 1 FROM `cte`) UNION SELECT * FROM `test`.`t`"},
		{"with cte as (select * from t union select * from cte) select * from cte", true, "WITH `cte` AS (SELECT * FROM `test`.`t` UNION SELECT * FROM `test`.`cte`) SELECT * FROM `cte`"},
	}

	p := parser.New()
	p.EnableWindowFunc(false)
	for _, tbl := range table {
		_, _, err := p.Parse(tbl.src, "", "")
		comment := fmt.Sprintf("source %v", tbl.src)
		if !tbl.ok {
			require.Error(t, err, comment)
			continue
		}
		require.NoError(t, err, comment)
		// restore correctness test
		if tbl.ok {
			var sb strings.Builder
			comment := fmt.Sprintf("source %v", tbl.src)
			stmts, _, err := p.Parse(tbl.src, "", "")
			require.NoError(t, err, comment)
			restoreSQLs := ""
			for _, stmt := range stmts {
				sb.Reset()
				ctx := NewRestoreCtx(RestoreStringSingleQuotes|RestoreSpacesAroundBinaryOperation|RestoreStringWithoutCharset|RestoreNameBackQuotes, &sb)
				ctx.DefaultDB = "test"
				err = stmt.Restore(ctx)
				require.NoError(t, err, comment)
				restoreSQL := sb.String()
				comment = fmt.Sprintf("source %v; restore %v", tbl.src, restoreSQL)
				if restoreSQLs != "" {
					restoreSQLs += "; "
				}
				restoreSQLs += restoreSQL
			}
			comment = fmt.Sprintf("restore %v; expect %v", restoreSQLs, tbl.restore)
			require.Equal(t, tbl.restore, restoreSQLs, comment)
		}
	}
}

func TestInsertStatementMemoryAllocation(t *testing.T) {
	sql := "insert t values (1)" + strings.Repeat(",(1)", 1000)
	var oldStats, newStats runtime.MemStats
	runtime.ReadMemStats(&oldStats)
	_, err := parser.New().ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	runtime.ReadMemStats(&newStats)
	require.Less(t, int(newStats.TotalAlloc-oldStats.TotalAlloc), 1024*500)
}

func TestCharsetIntroducer(t *testing.T) {
	p := parser.New()
	// Any character set MySQL knows works as an introducer, on all three
	// literal forms (string, hex, bit).
	_, _, err := p.Parse("select _gbk 'a';", "", "")
	require.NoError(t, err)
	_, _, err = p.Parse("select _gbk 0x1234;", "", "")
	require.NoError(t, err)
	_, _, err = p.Parse("select _gbk 0b101001;", "", "")
	require.NoError(t, err)
	_, _, err = p.Parse("select _ucs2 0x0078;", "", "")
	require.NoError(t, err)
}

func TestIssue45898(t *testing.T) {
	p := parser.New()
	_, _, _ = p.ParseSQL("a.") // intentionally invalid; must not poison the next parse
	stmts, _, err := p.ParseSQL("select count(1) from t")
	require.NoError(t, err)
	var sb strings.Builder
	restoreCtx := NewRestoreCtx(DefaultRestoreFlags, &sb)
	sb.Reset()
	require.NoError(t, stmts[0].Restore(restoreCtx))
	require.Equal(t, "SELECT COUNT(1) FROM `t`", sb.String())
}

func TestMultiStmt(t *testing.T) {
	p := parser.New()
	stmts, _, err := p.Parse("SELECT 'foo'; SELECT 'foo;bar','baz'; select 'foo' , 'bar' , 'baz' ;select 1", "", "")
	require.NoError(t, err)
	require.Len(t, stmts, 4)
	stmt1 := stmts[0].(*ast.SelectStmt)
	stmt2 := stmts[1].(*ast.SelectStmt)
	stmt3 := stmts[2].(*ast.SelectStmt)
	stmt4 := stmts[3].(*ast.SelectStmt)
	require.Equal(t, "'foo'", stmt1.Fields.Fields[0].Text())
	require.Equal(t, "'foo;bar'", stmt2.Fields.Fields[0].Text())
	require.Equal(t, "'baz'", stmt2.Fields.Fields[1].Text())
	require.Equal(t, "'foo'", stmt3.Fields.Fields[0].Text())
	require.Equal(t, "'bar'", stmt3.Fields.Fields[1].Text())
	require.Equal(t, "'baz'", stmt3.Fields.Fields[2].Text())
	require.Equal(t, "1", stmt4.Fields.Fields[0].Text())
}

// https://dev.mysql.com/doc/refman/8.1/en/other-vendor-data-types.html

func TestMaxParenthesesDepth(t *testing.T) {
	p := parser.New()
	nestedExpr := func(depth int) string {
		return "select " + strings.Repeat("(", depth) + "1" + strings.Repeat(")", depth)
	}
	nestedFuncExpr := func(depth int) string {
		return "select " + strings.Repeat("f(", depth) + "1" + strings.Repeat(")", depth)
	}
	nestedParenHint := func(depth int) string {
		return "select /*+ HASH_JOIN(" + strings.Repeat("(", depth) + "t" + strings.Repeat(")", depth) + ") */ * from t"
	}

	_, err := p.ParseOneStmt(nestedExpr(10000), "", "")
	require.NoError(t, err)

	_, err = p.ParseOneStmt(nestedExpr(10001), "", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "parentheses nesting depth exceeds maximum 10000")

	_, err = p.ParseOneStmt(nestedFuncExpr(10001), "", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "parentheses nesting depth exceeds maximum 10000")

	// An over-nested optimizer hint is not valid hint syntax; it is dropped
	// with a warning and the statement itself still parses.
	_, err = p.ParseOneStmt(nestedParenHint(10000), "", "")
	require.NoError(t, err)
}

func TestMaxASTDepth(t *testing.T) {
	p := parser.New()
	nestedCaseExpr := func(depth int) string {
		return "select " + strings.Repeat("case when true then ", depth) + "1" + strings.Repeat(" else 0 end", depth)
	}
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{
			name: "binary operation chain",
			sql:  "select " + strings.Repeat("1+", 11000) + "1",
		},
		{
			name: "unary operation chain",
			sql:  "select " + strings.Repeat("!", 11000) + "1",
		},
		{
			name: "case expression chain",
			sql:  nestedCaseExpr(11000),
		},
	} {
		_, err := p.ParseOneStmt(tc.sql, "", "")
		require.Error(t, err, tc.name)
		require.Contains(t, err.Error(), "AST nesting depth exceeds maximum", tc.name)
	}
}

// TestInsertRowAlias covers the MySQL 8.0.19+ row alias syntax for
// INSERT ... ON DUPLICATE KEY UPDATE.
func TestInsertRowAlias(t *testing.T) {
	table := []testCase{
		{"INSERT INTO t (a,b,c) VALUES (1,2,3) AS new ON DUPLICATE KEY UPDATE c=new.a+new.b;", true, "INSERT INTO `t` (`a`,`b`,`c`) VALUES (1,2,3) AS `new` ON DUPLICATE KEY UPDATE `c`=`new`.`a`+`new`.`b`"},
		{"INSERT INTO t (a,b,c) VALUES (1,2,3),(4,5,6) AS new(m,n,p) ON DUPLICATE KEY UPDATE c=m+n;", true, "INSERT INTO `t` (`a`,`b`,`c`) VALUES (1,2,3),(4,5,6) AS `new`(`m`, `n`, `p`) ON DUPLICATE KEY UPDATE `c`=`m`+`n`"},
		{"INSERT INTO t VALUES (1,2) AS new ON DUPLICATE KEY UPDATE b=new.b;", true, "INSERT INTO `t` VALUES (1,2) AS `new` ON DUPLICATE KEY UPDATE `b`=`new`.`b`"},
		{"INSERT INTO t SET a=1,b=2 AS new ON DUPLICATE KEY UPDATE b=new.a+new.b;", true, "INSERT INTO `t` SET `a`=1,`b`=2 AS `new` ON DUPLICATE KEY UPDATE `b`=`new`.`a`+`new`.`b`"},
		{"INSERT INTO t SET a=1,b=2 AS new(m,n) ON DUPLICATE KEY UPDATE b=m+n;", true, "INSERT INTO `t` SET `a`=1,`b`=2 AS `new`(`m`, `n`) ON DUPLICATE KEY UPDATE `b`=`m`+`n`"},
		// row alias without ON DUPLICATE KEY UPDATE
		{"INSERT INTO t VALUES (1,2) AS new;", true, "INSERT INTO `t` VALUES (1,2) AS `new`"},
		{"INSERT INTO t VALUES (1,2) AS new(a,b);", true, "INSERT INTO `t` VALUES (1,2) AS `new`(`a`, `b`)"},
		// row alias is not supported for REPLACE
		{"REPLACE INTO t VALUES (1,2) AS new;", false, ""},
		{"REPLACE INTO t SET a=1,b=2 AS new;", false, ""},
	}
	RunTest(t, table, false)
}

func TestDualPassword(t *testing.T) {
	table := []testCase{
		// MySQL 8.0.14+ dual passwords: RETAIN CURRENT PASSWORD / DISCARD OLD PASSWORD.
		{"ALTER USER 'u1'@'%' IDENTIFIED BY 'new' RETAIN CURRENT PASSWORD", true, "ALTER USER `u1`@`%` IDENTIFIED BY 'new' RETAIN CURRENT PASSWORD"},
		{"ALTER USER 'u1'@'%' IDENTIFIED WITH 'mysql_native_password' BY 'new' RETAIN CURRENT PASSWORD", true, "ALTER USER `u1`@`%` IDENTIFIED WITH 'mysql_native_password' BY 'new' RETAIN CURRENT PASSWORD"},
		{"ALTER USER 'u1'@'%' IDENTIFIED BY 'p2', 'u2'@'%' IDENTIFIED BY 'q2' RETAIN CURRENT PASSWORD", true, "ALTER USER `u1`@`%` IDENTIFIED BY 'p2', `u2`@`%` IDENTIFIED BY 'q2' RETAIN CURRENT PASSWORD"},
		{"ALTER USER 'u1'@'%' IDENTIFIED BY 'p2' RETAIN CURRENT PASSWORD, 'u2'@'%' IDENTIFIED BY 'q2'", true, "ALTER USER `u1`@`%` IDENTIFIED BY 'p2' RETAIN CURRENT PASSWORD, `u2`@`%` IDENTIFIED BY 'q2'"},
		{"ALTER USER 'u1'@'%' DISCARD OLD PASSWORD", true, "ALTER USER `u1`@`%` DISCARD OLD PASSWORD"},
		{"ALTER USER 'u1'@'%' DISCARD OLD PASSWORD, 'u2'@'%'", true, "ALTER USER `u1`@`%` DISCARD OLD PASSWORD, `u2`@`%`"},
		{"SET PASSWORD = 'new' RETAIN CURRENT PASSWORD", true, "SET PASSWORD='new' RETAIN CURRENT PASSWORD"},
		{"SET PASSWORD FOR 'u1'@'%' = 'new' RETAIN CURRENT PASSWORD", true, "SET PASSWORD FOR `u1`@`%`='new' RETAIN CURRENT PASSWORD"},
		// The current-user (USER()) form accepts both clauses.
		{"ALTER USER USER() IDENTIFIED BY 'p1' RETAIN CURRENT PASSWORD", true, "ALTER USER USER() IDENTIFIED BY 'p1' RETAIN CURRENT PASSWORD"},
		{"ALTER USER USER() DISCARD OLD PASSWORD", true, "ALTER USER USER() DISCARD OLD PASSWORD"},
		{"ALTER USER IF EXISTS USER() IDENTIFIED BY 'p1' RETAIN CURRENT PASSWORD", true, "ALTER USER IF EXISTS USER() IDENTIFIED BY 'p1' RETAIN CURRENT PASSWORD"},
		// Negative: RETAIN needs a cleartext password to promote, so the hashed
		// AS-form, the bare plugin form, and the no-auth form are all rejected.
		{"ALTER USER 'u1'@'%' IDENTIFIED WITH 'mysql_native_password' AS '*B50FBDB37F1256824274912F2A1CE648082C3F1F' RETAIN CURRENT PASSWORD", false, ""},
		{"ALTER USER 'u1'@'%' IDENTIFIED WITH 'mysql_native_password' RETAIN CURRENT PASSWORD", false, ""},
		{"ALTER USER 'u1'@'%' RETAIN CURRENT PASSWORD", false, ""},
		// Negative: CREATE USER does not accept either clause per MySQL grammar.
		{"CREATE USER 'u1'@'%' IDENTIFIED BY 'p1' RETAIN CURRENT PASSWORD", false, ""},
		{"CREATE USER 'u1'@'%' DISCARD OLD PASSWORD", false, ""},
		// Negative: DISCARD coexisting with an auth option is invalid.
		{"ALTER USER 'u1'@'%' IDENTIFIED BY 'p1' DISCARD OLD PASSWORD", false, ""},
	}
	RunTest(t, table, false)
}
