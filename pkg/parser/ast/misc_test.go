// Copyright 2016 PingCAP, Inc.
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

package ast_test

import (
	"strings"
	"testing"

	"github.com/block/spirit/pkg/parser"
	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/parser/format"
	"github.com/stretchr/testify/require"
)

type visitor struct{}

func (v visitor) Enter(in ast.Node) (ast.Node, bool) {
	return in, false
}

func (v visitor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

type visitor1 struct {
	visitor
}

func (visitor1) Enter(in ast.Node) (ast.Node, bool) {
	return in, true
}

func TestDDLVisitorCoverMisc(t *testing.T) {
	sql := `
create table t (c1 smallint unsigned, c2 int unsigned);
alter table t add column a smallint unsigned after b;
alter table t add column (a int, constraint check (a > 0));
create index t_i on t (id);
create database test character set utf8;
drop database test;
drop index t_i on t;
drop table t;
truncate t;
create table t (
jobAbbr char(4) not null,
constraint foreign key (jobabbr) references ffxi_jobtype (jobabbr) on delete cascade on update cascade
);
`
	parse := parser.New()
	stmts, _, err := parse.Parse(sql, "", "")
	require.NoError(t, err)
	for _, stmt := range stmts {
		stmt.Accept(visitor{})
		stmt.Accept(visitor1{})
	}
}

func TestDMLVistorCover(t *testing.T) {
	sql := `delete from somelog where user = 'jcole' order by timestamp_column limit 1;
delete t1, t2 from t1 inner join t2 inner join t3 where t1.id=t2.id and t2.id=t3.id;
select * from t where exists(select * from t k where t.c = k.c having sum(c) = 1);
insert into t_copy select * from t where t.x > 5;
(select /*+ TIDB_INLJ(t1) */ a from t1 where a=10 and b=1) union (select /*+ TIDB_SMJ(t2) */ a from t2 where a=11 and b=2) order by a limit 10;
update t1 set col1 = col1 + 1, col2 = col1;
show create table t;
load data infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b'`

	p := parser.New()
	stmts, _, err := p.Parse(sql, "", "")
	require.NoError(t, err)
	for _, stmt := range stmts {
		stmt.Accept(visitor{})
		stmt.Accept(visitor1{})
	}
}

func TestTableOptimizerHintRestore(t *testing.T) {
	testCases := []NodeRestoreTestCase{
		{"ORDER_INDEX(t1 c1)", "ORDER_INDEX(`t1` `c1`)"},
		{"ORDER_INDEX(test.t1 c1)", "ORDER_INDEX(`test`.`t1` `c1`)"},
		{"ORDER_INDEX(@sel_1 t1 c1)", "ORDER_INDEX(@`sel_1` `t1` `c1`)"},
		{"ORDER_INDEX(t1@sel_1 c1)", "ORDER_INDEX(`t1`@`sel_1` `c1`)"},
		{"ORDER_INDEX(test.t1@sel_1 c1)", "ORDER_INDEX(`test`.`t1`@`sel_1` `c1`)"},
		{"NO_ORDER_INDEX(t1 c1)", "NO_ORDER_INDEX(`t1` `c1`)"},
		{"NO_ORDER_INDEX(test.t1 c1)", "NO_ORDER_INDEX(`test`.`t1` `c1`)"},
		{"NO_ORDER_INDEX(@sel_1 t1 c1)", "NO_ORDER_INDEX(@`sel_1` `t1` `c1`)"},
		{"NO_ORDER_INDEX(t1@sel_1 c1)", "NO_ORDER_INDEX(`t1`@`sel_1` `c1`)"},
		{"NO_ORDER_INDEX(test.t1@sel_1 c1)", "NO_ORDER_INDEX(`test`.`t1`@`sel_1` `c1`)"},
		{"HASH_JOIN(`t1`)", "HASH_JOIN(`t1`)"},
		{"HASH_JOIN(t1)", "HASH_JOIN(`t1`)"},
		{"HASH_JOIN(t1,t2)", "HASH_JOIN(`t1`, `t2`)"},
		{"HASH_JOIN(@sel1 t1,t2)", "HASH_JOIN(@`sel1` `t1`, `t2`)"},
		{"HASH_JOIN(t1@sel1,t2@sel2)", "HASH_JOIN(`t1`@`sel1`, `t2`@`sel2`)"},
		{"NO_HASH_JOIN(t1,t2)", "NO_HASH_JOIN(`t1`, `t2`)"},
		{"NO_HASH_JOIN(@sel1 t1,t2)", "NO_HASH_JOIN(@`sel1` `t1`, `t2`)"},
		{"MAX_EXECUTION_TIME(3000)", "MAX_EXECUTION_TIME(3000)"},
		{"MAX_EXECUTION_TIME(@sel1 3000)", "MAX_EXECUTION_TIME(@`sel1` 3000)"},
		{"MERGE()", "MERGE()"},
		{"MERGE(@sel1)", "MERGE(@`sel1`)"},
		{"NO_INDEX_MERGE()", "NO_INDEX_MERGE()"},
		{"NO_INDEX_MERGE(@sel1)", "NO_INDEX_MERGE(@`sel1`)"},
		{"QB_NAME(sel1)", "QB_NAME(`sel1`)"},
		{"SET_VAR(sql_mode = 'ANSI')", "SET_VAR(sql_mode = 'ANSI')"},
		{"RESOURCE_GROUP(rg1)", "RESOURCE_GROUP(`rg1`)"},
		{"RESOURCE_GROUP(`default`)", "RESOURCE_GROUP(`default`)"},

		// Join-order hints.
		{"JOIN_FIXED_ORDER()", "JOIN_FIXED_ORDER()"},
		{"JOIN_FIXED_ORDER(@sel1)", "JOIN_FIXED_ORDER(@`sel1`)"},
		{"JOIN_ORDER(t1, t2)", "JOIN_ORDER(`t1`, `t2`)"},
		{"JOIN_ORDER(@sel1 t1, t2)", "JOIN_ORDER(@`sel1` `t1`, `t2`)"},
		{"JOIN_ORDER(t1@sel1, t2@sel1)", "JOIN_ORDER(`t1`@`sel1`, `t2`@`sel1`)"},
		{"JOIN_PREFIX(t1)", "JOIN_PREFIX(`t1`)"},
		{"JOIN_PREFIX(@sel1 t1, t2)", "JOIN_PREFIX(@`sel1` `t1`, `t2`)"},
		{"JOIN_SUFFIX(t2)", "JOIN_SUFFIX(`t2`)"},
		{"JOIN_SUFFIX(t1@sel1, t2@sel1)", "JOIN_SUFFIX(`t1`@`sel1`, `t2`@`sel1`)"},

		// Table-level hints.
		{"BKA()", "BKA()"},
		{"BKA(t1)", "BKA(`t1`)"},
		{"BKA(@sel1)", "BKA(@`sel1`)"},
		{"BKA(@sel1 t1, t2)", "BKA(@`sel1` `t1`, `t2`)"},
		{"NO_BKA(t1, t2)", "NO_BKA(`t1`, `t2`)"},
		{"NO_BKA(t1@sel1)", "NO_BKA(`t1`@`sel1`)"},
		{"BNL(t1)", "BNL(`t1`)"},
		{"NO_BNL(@sel1 t2)", "NO_BNL(@`sel1` `t2`)"},
		{"DERIVED_CONDITION_PUSHDOWN(dt)", "DERIVED_CONDITION_PUSHDOWN(`dt`)"},
		{"DERIVED_CONDITION_PUSHDOWN()", "DERIVED_CONDITION_PUSHDOWN()"},
		{"NO_DERIVED_CONDITION_PUSHDOWN(@sel1 dt)", "NO_DERIVED_CONDITION_PUSHDOWN(@`sel1` `dt`)"},
		{"MERGE(dt)", "MERGE(`dt`)"},
		{"MERGE(@sel1 dt1, dt2)", "MERGE(@`sel1` `dt1`, `dt2`)"},
		{"NO_MERGE()", "NO_MERGE()"},
		{"NO_MERGE(dt)", "NO_MERGE(`dt`)"},
		{"NO_MERGE(dt@sel1)", "NO_MERGE(`dt`@`sel1`)"},

		// Index-level hints.
		{"GROUP_INDEX(t1 i1)", "GROUP_INDEX(`t1` `i1`)"},
		{"NO_GROUP_INDEX(t1 i1, i2)", "NO_GROUP_INDEX(`t1` `i1`, `i2`)"},
		{"INDEX(t1)", "INDEX(`t1`)"},
		{"INDEX(t1 i1)", "INDEX(`t1` `i1`)"},
		{"INDEX(@sel1 t1 i1, PRIMARY)", "INDEX(@`sel1` `t1` `i1`, `PRIMARY`)"},
		{"INDEX(t1@sel1 i1)", "INDEX(`t1`@`sel1` `i1`)"},
		{"NO_INDEX(t1 i1)", "NO_INDEX(`t1` `i1`)"},
		{"INDEX_MERGE(t1)", "INDEX_MERGE(`t1`)"},
		{"INDEX_MERGE(t1 i1, i2)", "INDEX_MERGE(`t1` `i1`, `i2`)"},
		{"NO_INDEX_MERGE(t2)", "NO_INDEX_MERGE(`t2`)"},
		{"NO_INDEX_MERGE(@sel1 t2 i1)", "NO_INDEX_MERGE(@`sel1` `t2` `i1`)"},
		{"JOIN_INDEX(t1 i1)", "JOIN_INDEX(`t1` `i1`)"},
		{"NO_JOIN_INDEX(t1@sel1 i1, i2)", "NO_JOIN_INDEX(`t1`@`sel1` `i1`, `i2`)"},
		{"MRR(t1)", "MRR(`t1`)"},
		{"MRR(t1 i1)", "MRR(`t1` `i1`)"},
		{"NO_MRR(t1 i1, i2)", "NO_MRR(`t1` `i1`, `i2`)"},
		{"NO_ICP(t1)", "NO_ICP(`t1`)"},
		{"NO_ICP(t1 i1)", "NO_ICP(`t1` `i1`)"},
		{"NO_RANGE_OPTIMIZATION(t1)", "NO_RANGE_OPTIMIZATION(`t1`)"},
		{"NO_RANGE_OPTIMIZATION(@sel1 t1 i1)", "NO_RANGE_OPTIMIZATION(@`sel1` `t1` `i1`)"},
		{"SKIP_SCAN(t1)", "SKIP_SCAN(`t1`)"},
		{"SKIP_SCAN(t1 i1)", "SKIP_SCAN(`t1` `i1`)"},
		{"NO_SKIP_SCAN(t1@sel1 i1)", "NO_SKIP_SCAN(`t1`@`sel1` `i1`)"},
		{"ORDER_INDEX(t1)", "ORDER_INDEX(`t1`)"},

		// Subquery hints.
		{"SEMIJOIN()", "SEMIJOIN()"},
		{"SEMIJOIN(@sel1)", "SEMIJOIN(@`sel1`)"},
		{"SEMIJOIN(FIRSTMATCH)", "SEMIJOIN(FIRSTMATCH)"},
		{"SEMIJOIN(@sel1 FIRSTMATCH, LOOSESCAN)", "SEMIJOIN(@`sel1` FIRSTMATCH, LOOSESCAN)"},
		{"NO_SEMIJOIN(DUPSWEEDOUT, MATERIALIZATION)", "NO_SEMIJOIN(DUPSWEEDOUT, MATERIALIZATION)"},
		{"NO_SEMIJOIN(@sel1)", "NO_SEMIJOIN(@`sel1`)"},
		{"SUBQUERY(INTOEXISTS)", "SUBQUERY(INTOEXISTS)"},
		{"SUBQUERY(@sel1 MATERIALIZATION)", "SUBQUERY(@`sel1` MATERIALIZATION)"},

		// SET_VAR values are written back as MySQL reads them: MySQL's hint
		// lexer processes no backslash escapes, so they are not added.
		{"SET_VAR(sql_mode = 'a*\\/ x')", "SET_VAR(sql_mode = 'a*\\/ x')"},
		{"SET_VAR(sql_mode = 'ANSI\\')", "SET_VAR(sql_mode = 'ANSI\\')"},
		{"SET_VAR(sql_mode = 'AN''SI')", "SET_VAR(sql_mode = 'AN''SI')"},
		{"SET_VAR(sql_mode = \"ANSI\")", "SET_VAR(sql_mode = 'ANSI')"},
		// Unquoted values stay unquoted: MySQL reads a bare number (with an
		// optional K/M/G suffix) as a number, and rejects it as a string for
		// a numeric variable.
		{"SET_VAR(sort_buffer_size = 1048576)", "SET_VAR(sort_buffer_size = 1048576)"},
		{"SET_VAR(sort_buffer_size = 16M)", "SET_VAR(sort_buffer_size = 16M)"},
		{"SET_VAR(timestamp = 1.5)", "SET_VAR(timestamp = 1.5)"},
		{"SET_VAR(foreign_key_checks = OFF)", "SET_VAR(foreign_key_checks = OFF)"},
		{"SET_VAR(foreign_key_checks = `OFF`)", "SET_VAR(foreign_key_checks = 'OFF')"},
		// MySQL rejects a signed value; it keeps its sign so the hint
		// restores unchanged.
		{"SET_VAR(sort_buffer_size = -1)", "SET_VAR(sort_buffer_size = -1)"},
		{"SET_VAR(sort_buffer_size = +1)", "SET_VAR(sort_buffer_size = +1)"},
		{"SET_VAR(sort_buffer_size = -0)", "SET_VAR(sort_buffer_size = -0)"},
		{"SET_VAR(x = -9223372036854775808)", "SET_VAR(x = -9223372036854775808)"},
		{"SET_VAR(x = -0.3)", "SET_VAR(x = -0.3)"},
		// A quoted variable name that is not a plain word is written back
		// quoted, so it stays one name.
		{"SET_VAR(`sort_buffer_size` = 1)", "SET_VAR(sort_buffer_size = 1)"},
		{"SET_VAR(`a b` = 1)", "SET_VAR(`a b` = 1)"},
		{"SET_VAR(`a``b` = 1)", "SET_VAR(`a``b` = 1)"},
		// QB_NAME with a view list (kept from the upstream parser).
		{"QB_NAME(qb1, v1@sel_1 . v2)", "QB_NAME(`qb1`, `v1`@`sel_1`. `v2`)"},
	}
	extractNodeFunc := func(node ast.Node) ast.Node {
		return node.(*ast.SelectStmt).TableHints[0]
	}
	runNodeRestoreTest(t, testCases, "select /*+ %s */ * from t1 join t2", extractNodeFunc)
}

// TestOptimizerHintsRestoreStatement restores a whole statement, the way a
// caller that rewrites queries does: every hint in the comment survives.
func TestOptimizerHintsRestoreStatement(t *testing.T) {
	testCases := []struct {
		sql    string
		expect string
	}{
		{
			"SELECT /*+ BKA(t1) NO_INDEX_MERGE(t2) */ * FROM t1 JOIN t2",
			"SELECT /*+ BKA(`t1`) NO_INDEX_MERGE(`t2`)*/ * FROM `t1` JOIN `t2`",
		},
		{
			"SELECT /*+ QB_NAME(qb1) JOIN_ORDER(@qb1 t2, t1) INDEX(t1 i1) MAX_EXECUTION_TIME(1000) */ * FROM t1 JOIN t2",
			"SELECT /*+ QB_NAME(`qb1`) JOIN_ORDER(@`qb1` `t2`, `t1`) INDEX(`t1` `i1`) MAX_EXECUTION_TIME(1000)*/ * FROM `t1` JOIN `t2`",
		},
		{
			"UPDATE /*+ NO_RANGE_OPTIMIZATION(t1 PRIMARY) */ t1 SET a = 1 WHERE b IN (SELECT /*+ SEMIJOIN(LOOSESCAN) */ b FROM t2)",
			"UPDATE /*+ NO_RANGE_OPTIMIZATION(`t1` `PRIMARY`)*/ `t1` SET `a`=1 WHERE `b` IN (SELECT /*+ SEMIJOIN(LOOSESCAN)*/ `b` FROM `t2`)",
		},
		{
			"SELECT /*+ SET_VAR(sql_mode='a*\\/ x') */ 1",
			"SELECT /*+ SET_VAR(sql_mode = 'a*\\/ x')*/ 1",
		},
	}
	p := parser.New()
	for _, tc := range testCases {
		stmts, warns, err := p.Parse(tc.sql, "", "")
		require.NoError(t, err, tc.sql)
		require.Empty(t, warns, tc.sql)
		require.Len(t, stmts, 1, tc.sql)
		var sb strings.Builder
		require.NoError(t, stmts[0].Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)), tc.sql)
		require.Equal(t, tc.expect, sb.String(), tc.sql)
	}
}

// TestOptimizerHintRestoreKeepsCommentClosed restores hints whose quoted
// values spell */ through a backslash. MySQL's hint lexer reads 'a*\/ x' as
// a*\/ x, so restoring it as 'a*/ x' would end the hint comment early and
// turn the rest of the value into SQL. The only */ in the restored statement
// must be the one that closes the hint comment.
func TestOptimizerHintRestoreKeepsCommentClosed(t *testing.T) {
	testCases := []struct {
		sql    string
		expect string
	}{
		{
			"SELECT /*+ SET_VAR(sql_mode='a*\\/ UNION SELECT 2 -- ') */ 1 FROM t LIMIT 10",
			"SELECT /*+ SET_VAR(sql_mode = 'a*\\/ UNION SELECT 2 -- ')*/ 1 FROM `t` LIMIT 10",
		},
		{
			"SELECT /*+ SET_VAR(sql_mode=\"a*\\/ x\") RESOURCE_GROUP(`rg*\\/`) */ 1",
			"SELECT /*+ SET_VAR(sql_mode = 'a*\\/ x') RESOURCE_GROUP(`rg*\\/`)*/ 1",
		},
	}
	p := parser.New()
	for _, tc := range testCases {
		stmts, _, err := p.Parse(tc.sql, "", "")
		require.NoError(t, err, tc.sql)
		require.Len(t, stmts, 1, tc.sql)
		var sb strings.Builder
		require.NoError(t, stmts[0].Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)), tc.sql)
		restored := sb.String()
		require.Equal(t, tc.expect, restored, tc.sql)
		require.Equal(t, 1, strings.Count(restored, "*/"), restored)
	}
}

// TestOptimizerHintSetVarNameCannotAddHints checks that a quoted SET_VAR
// variable name holding hint syntax restores as one hint, not several.
func TestOptimizerHintSetVarNameCannotAddHints(t *testing.T) {
	p := parser.New()
	stmts, _, err := p.Parse("SELECT /*+ SET_VAR(`a = 1) MAX_EXECUTION_TIME(5000) SET_VAR(b` = 2) */ 1", "", "")
	require.NoError(t, err)
	require.Len(t, stmts[0].(*ast.SelectStmt).TableHints, 1)

	var sb strings.Builder
	require.NoError(t, stmts[0].Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)))
	require.Equal(t, "SELECT /*+ SET_VAR(`a = 1) MAX_EXECUTION_TIME(5000) SET_VAR(b` = 2)*/ 1", sb.String())

	again, _, err := p.Parse(sb.String(), "", "")
	require.NoError(t, err)
	require.Len(t, again[0].(*ast.SelectStmt).TableHints, 1, "restored: %s", sb.String())
}

// TestOptimizerHintRestoreStringFlags covers the hint string writer under
// each string-quoting restore flag: only the quote character is doubled.
func TestOptimizerHintRestoreStringFlags(t *testing.T) {
	h := &ast.TableOptimizerHint{
		HintName: ast.NewCIStr("SET_VAR"),
		HintData: ast.HintSetVar{VarName: "x", Value: `a'b"c\d`, Quoted: true},
	}
	testCases := []struct {
		flags  format.RestoreFlags
		expect string
	}{
		{format.RestoreStringSingleQuotes, `SET_VAR(x = 'a''b"c\d')`},
		{format.RestoreStringDoubleQuotes, `SET_VAR(x = "a'b""c\d")`},
		{0, `SET_VAR(x = a'b"c\d)`},
	}
	for _, tc := range testCases {
		var sb strings.Builder
		require.NoError(t, h.Restore(format.NewRestoreCtx(tc.flags, &sb)))
		require.Equal(t, tc.expect, sb.String())
	}
}

// TestOptimizerHintRestoreUnexpectedData checks that a hand-built hint whose
// HintData has the wrong type for its name fails Restore instead of
// panicking.
func TestOptimizerHintRestoreUnexpectedData(t *testing.T) {
	hints := []*ast.TableOptimizerHint{
		{HintName: ast.NewCIStr("MAX_EXECUTION_TIME"), HintData: "1000"},
		{HintName: ast.NewCIStr("MAX_EXECUTION_TIME")},
		{HintName: ast.NewCIStr("RESOURCE_GROUP"), HintData: uint64(1)},
		{HintName: ast.NewCIStr("SET_VAR"), HintData: "x"},
		{HintName: ast.NewCIStr("SEMIJOIN"), HintData: "FIRSTMATCH"},
		{HintName: ast.NewCIStr("SUBQUERY"), HintData: uint64(1)},
	}
	for _, h := range hints {
		var sb strings.Builder
		require.ErrorContains(t, h.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)), "unexpected data", "%#v", h)
	}
}

// TestOptimizerHintRestoreSetVarValue covers hand-built SET_VAR values: an
// unquoted value that is not a single bare token is quoted, so the restored
// hint stays one value.
func TestOptimizerHintRestoreSetVarValue(t *testing.T) {
	testCases := []struct {
		value  ast.HintSetVar
		expect string
	}{
		{ast.HintSetVar{VarName: "x", Value: "1048576"}, "SET_VAR(x = 1048576)"},
		{ast.HintSetVar{VarName: "x", Value: "1048576", Quoted: true}, "SET_VAR(x = '1048576')"},
		{ast.HintSetVar{VarName: "x", Value: "1.5e-3"}, "SET_VAR(x = 1.5e-3)"},
		{ast.HintSetVar{VarName: "x", Value: "a b"}, "SET_VAR(x = 'a b')"},
		{ast.HintSetVar{VarName: "x", Value: "a-b"}, "SET_VAR(x = 'a-b')"},
		{ast.HintSetVar{VarName: "x", Value: "a.b"}, "SET_VAR(x = 'a.b')"},
		{ast.HintSetVar{VarName: "x", Value: "REPEATABLE-READ"}, "SET_VAR(x = 'REPEATABLE-READ')"},
		{ast.HintSetVar{VarName: "x", Value: "1.2.3"}, "SET_VAR(x = '1.2.3')"},
		{ast.HintSetVar{VarName: "x", Value: "1e"}, "SET_VAR(x = 1e)"},
		{ast.HintSetVar{VarName: "x", Value: "1e+"}, "SET_VAR(x = '1e+')"},
		{ast.HintSetVar{VarName: "x", Value: "-"}, "SET_VAR(x = '-')"},
		{ast.HintSetVar{VarName: "x", Value: "."}, "SET_VAR(x = '.')"},
		{ast.HintSetVar{VarName: "x", Value: "-0"}, "SET_VAR(x = -0)"},
		{ast.HintSetVar{VarName: "x", Value: ".5"}, "SET_VAR(x = .5)"},
		{ast.HintSetVar{VarName: "x", Value: "-1.5E+3"}, "SET_VAR(x = -1.5E+3)"},
		{ast.HintSetVar{VarName: "x", Value: "16M"}, "SET_VAR(x = 16M)"},
		{ast.HintSetVar{VarName: "x", Value: ""}, "SET_VAR(x = '')"},
		{ast.HintSetVar{VarName: "x", Value: "it's"}, "SET_VAR(x = 'it''s')"},
		{ast.HintSetVar{VarName: "x", Value: `a\b`, Quoted: true}, `SET_VAR(x = 'a\b')`},
	}
	for _, tc := range testCases {
		h := &ast.TableOptimizerHint{HintName: ast.NewCIStr("SET_VAR"), HintData: tc.value}
		var sb strings.Builder
		require.NoError(t, h.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)))
		require.Equal(t, tc.expect, sb.String(), "%#v", tc.value)
	}
}

// TestOptimizerHintRestoreRejectsCommentEnd checks that a hint whose text
// would end the /*+ ... */ comment is refused rather than restored. The
// parser cannot produce one, so the hints are built by hand.
func TestOptimizerHintRestoreRejectsCommentEnd(t *testing.T) {
	hints := []*ast.TableOptimizerHint{
		{HintName: ast.NewCIStr("SET_VAR"), HintData: ast.HintSetVar{VarName: "sql_mode", Value: "a*/ x"}},
		{HintName: ast.NewCIStr("SET_VAR"), HintData: ast.HintSetVar{VarName: "a*/b", Value: "1"}},
		{HintName: ast.NewCIStr("RESOURCE_GROUP"), HintData: "rg*/"},
		{HintName: ast.NewCIStr("QB_NAME"), QBName: ast.NewCIStr("q*/b")},
		{HintName: ast.NewCIStr("BKA"), QBName: ast.NewCIStr("q*/b")},
		{HintName: ast.NewCIStr("BKA"), Tables: []ast.HintTable{{TableName: ast.NewCIStr("t*/1")}}},
		{HintName: ast.NewCIStr("BKA"), Tables: []ast.HintTable{{DBName: ast.NewCIStr("d*/"), TableName: ast.NewCIStr("t1")}}},
		{HintName: ast.NewCIStr("BKA"), Tables: []ast.HintTable{{TableName: ast.NewCIStr("t1"), QBName: ast.NewCIStr("*/")}}},
		{HintName: ast.NewCIStr("INDEX"), Tables: []ast.HintTable{{TableName: ast.NewCIStr("t1")}}, Indexes: []ast.CIStr{ast.NewCIStr("i*/")}},
		{HintName: ast.NewCIStr("SEMIJOIN"), HintData: []string{"*/"}},
		{HintName: ast.NewCIStr("B*/KA")},
	}
	for _, h := range hints {
		var sb strings.Builder
		err := h.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
		require.ErrorContains(t, err, "would end the hint comment", "%#v", h)

		// The same hint inside a statement fails the statement's Restore.
		stmt, err := parser.New().ParseOneStmt("SELECT 1", "", "")
		require.NoError(t, err)
		stmt.(*ast.SelectStmt).TableHints = []*ast.TableOptimizerHint{h}
		sb.Reset()
		require.Error(t, stmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)), "%#v", h)
	}
}
