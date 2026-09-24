// Copyright 2017 PingCAP, Inc.
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
	. "github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/parser/format"
	"github.com/block/spirit/pkg/parser/mysql"
	"github.com/stretchr/testify/require"
)

func TestFunctionsVisitorCover(t *testing.T) {
	valueExpr := NewValueExpr(42, mysql.DefaultCharset, mysql.DefaultCollationName)
	stmts := []Node{
		&AggregateFuncExpr{Args: []ExprNode{valueExpr}},
		&FuncCallExpr{Args: []ExprNode{valueExpr}},
		&FuncCastExpr{Expr: valueExpr},
		&WindowFuncExpr{Spec: WindowSpec{}},
	}

	for _, stmt := range stmts {
		stmt.Accept(visitor{})
		stmt.Accept(visitor1{})
	}
}

func TestAggregateFuncExprRestore(t *testing.T) {
	testCases := []NodeRestoreTestCase{
		{"AVG(test_score)", "AVG(`test_score`)"},
		{"AVG(distinct test_score)", "AVG(DISTINCT `test_score`)"},
		{"BIT_AND(test_score)", "BIT_AND(`test_score`)"},
		{"BIT_OR(test_score)", "BIT_OR(`test_score`)"},
		{"BIT_XOR(test_score)", "BIT_XOR(`test_score`)"},
		{"COUNT(test_score)", "COUNT(`test_score`)"},
		{"COUNT(*)", "COUNT(1)"},
		{"COUNT(DISTINCT scores, results)", "COUNT(DISTINCT `scores`, `results`)"},
		{"MIN(test_score)", "MIN(`test_score`)"},
		{"MIN(DISTINCT test_score)", "MIN(DISTINCT `test_score`)"},
		{"MAX(test_score)", "MAX(`test_score`)"},
		{"MAX(DISTINCT test_score)", "MAX(DISTINCT `test_score`)"},
		{"STD(test_score)", "STDDEV_POP(`test_score`)"},
		{"STDDEV(test_score)", "STDDEV_POP(`test_score`)"},
		{"STDDEV_POP(test_score)", "STDDEV_POP(`test_score`)"},
		{"STDDEV_SAMP(test_score)", "STDDEV_SAMP(`test_score`)"},
		{"SUM(test_score)", "SUM(`test_score`)"},
		{"SUM(DISTINCT test_score)", "SUM(DISTINCT `test_score`)"},
		{"VAR_POP(test_score)", "VAR_POP(`test_score`)"},
		{"VAR_SAMP(test_score)", "VAR_SAMP(`test_score`)"},
		{"VARIANCE(test_score)", "VAR_POP(`test_score`)"},
		{"JSON_OBJECTAGG(test_score, results)", "JSON_OBJECTAGG(`test_score`, `results`)"},
		{"GROUP_CONCAT(a)", "GROUP_CONCAT(`a` SEPARATOR ',')"},
		{"GROUP_CONCAT(a separator '--')", "GROUP_CONCAT(`a` SEPARATOR '--')"},
		{"GROUP_CONCAT(a order by b desc, c)", "GROUP_CONCAT(`a` ORDER BY `b` DESC,`c` SEPARATOR ',')"},
		{"GROUP_CONCAT(a order by b desc, c separator '--')", "GROUP_CONCAT(`a` ORDER BY `b` DESC,`c` SEPARATOR '--')"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).Fields.Fields[0].Expr
	}
	runNodeRestoreTest(t, testCases, "select %s", extractNodeFunc)
}

func TestConvert(t *testing.T) {
	// Test case for CONVERT(expr USING transcoding_name).
	cases := []struct {
		SQL          string
		CharsetName  string
		ErrorMessage string
	}{
		{`SELECT CONVERT("abc" USING "latin1")`, "latin1", ""},
		{`SELECT CONVERT("abc" USING laTiN1)`, "latin1", ""},
		{`SELECT CONVERT("abc" USING "binary")`, "binary", ""},
		{`SELECT CONVERT("abc" USING biNaRy)`, "binary", ""},
		// Unknown charset names parse; MySQL reports 1115 at execution.
		{`SELECT CONVERT(a USING a)`, "a", ""},
		// A function call is not a transcoding name: syntax error at '('.
		{`SELECT CONVERT("abc" USING CONCAT("utf", "8"))`, "", `line 1 column 34 near "("utf", "8"))" `},
	}
	for _, testCase := range cases {
		stmt, err := parser.New().ParseOneStmt(testCase.SQL, "", "")
		if testCase.ErrorMessage != "" {
			require.EqualError(t, err, testCase.ErrorMessage)
			continue
		}
		require.NoError(t, err)

		st := stmt.(*SelectStmt)
		expr := st.Fields.Fields[0].Expr.(*FuncCallExpr)
		charsetArg := expr.Args[1].(*ValueExpr)
		require.Equal(t, testCase.CharsetName, charsetArg.GetString())
	}
}

func TestChar(t *testing.T) {
	// Test case for CHAR(N USING charset_name)
	cases := []struct {
		SQL          string
		CharsetName  string
		ErrorMessage string
	}{
		{`SELECT CHAR("abc" USING "latin1")`, "latin1", ""},
		{`SELECT CHAR("abc" USING laTiN1)`, "latin1", ""},
		{`SELECT CHAR("abc" USING "binary")`, "binary", ""},
		{`SELECT CHAR("abc" USING binary)`, "binary", ""},
		// Unknown charset names parse; MySQL reports 1115 at execution.
		{`SELECT CHAR(a USING a)`, "a", ""},
		// A function call is not a charset name: syntax error at '('.
		{`SELECT CHAR("abc" USING CONCAT("utf", "8"))`, "", `line 1 column 31 near "("utf", "8"))" `},
	}
	for _, testCase := range cases {
		stmt, err := parser.New().ParseOneStmt(testCase.SQL, "", "")
		if testCase.ErrorMessage != "" {
			require.EqualError(t, err, testCase.ErrorMessage)
			continue
		}
		require.NoError(t, err)

		st := stmt.(*SelectStmt)
		expr := st.Fields.Fields[0].Expr.(*FuncCallExpr)
		charsetArg := expr.Args[1].(*ValueExpr)
		require.Equal(t, testCase.CharsetName, charsetArg.GetString())
	}
}

func TestWindowFuncExprRestore(t *testing.T) {
	testCases := []NodeRestoreTestCase{
		{"RANK() OVER w", "RANK() OVER `w`"},
		{"RANK() OVER (PARTITION BY a)", "RANK() OVER (PARTITION BY `a`)"},
		{"MAX(DISTINCT a) OVER (PARTITION BY a)", "MAX(DISTINCT `a`) OVER (PARTITION BY `a`)"},
		{"MAX(DISTINCTROW a) OVER (PARTITION BY a)", "MAX(DISTINCT `a`) OVER (PARTITION BY `a`)"},
		{"MAX(DISTINCT ALL a) OVER (PARTITION BY a)", "MAX(DISTINCT `a`) OVER (PARTITION BY `a`)"},
		{"MAX(ALL a) OVER (PARTITION BY a)", "MAX(`a`) OVER (PARTITION BY `a`)"},
		{"FIRST_VALUE(val) IGNORE NULLS OVER (w)", "FIRST_VALUE(`val`) IGNORE NULLS OVER (`w`)"},
		{"FIRST_VALUE(val) RESPECT NULLS OVER w", "FIRST_VALUE(`val`) OVER `w`"},
		{"NTH_VALUE(val, 233) FROM LAST IGNORE NULLS OVER w", "NTH_VALUE(`val`, 233) FROM LAST IGNORE NULLS OVER `w`"},
		{"NTH_VALUE(val, 233) FROM FIRST IGNORE NULLS OVER (w)", "NTH_VALUE(`val`, 233) IGNORE NULLS OVER (`w`)"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).Fields.Fields[0].Expr
	}
	runNodeRestoreTest(t, testCases, "select %s from t", extractNodeFunc)
}

func TestGenericFuncRestore(t *testing.T) {
	testCases := []NodeRestoreTestCase{
		{"s.a()", "`s`.`a`()"},
		{"`s`.`a`()", "`s`.`a`()"},
		{"now()", "NOW()"},
		{"`s`.`now`()", "`s`.`now`()"},
		// FIXME: expectSQL should be `generic_func()`.
		{"generic_func()", "GENERIC_FUNC()"},
		{"`ident.1`.`ident.2`()", "`ident.1`.`ident.2`()"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).Fields.Fields[0].Expr
	}
	runNodeRestoreTest(t, testCases, "select %s from t", extractNodeFunc)
}

func TestRestoreWithError(t *testing.T) {
	testCases := []string{
		"json_memberof()",
	}

	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).Fields.Fields[0].Expr
	}

	for _, c := range testCases {
		sql := "select " + c
		p := parser.New()
		stmt, err := p.ParseOneStmt(sql, "", "")
		require.NoError(t, err)
		var sb strings.Builder
		require.Error(t, extractNodeFunc(stmt).Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)))
	}
}
