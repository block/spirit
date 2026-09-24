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
	"testing"

	"github.com/block/spirit/pkg/parser"
	"github.com/block/spirit/pkg/parser/ast"
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
	}
	extractNodeFunc := func(node ast.Node) ast.Node {
		return node.(*ast.SelectStmt).TableHints[0]
	}
	runNodeRestoreTest(t, testCases, "select /*+ %s */ * from t1 join t2", extractNodeFunc)
}
