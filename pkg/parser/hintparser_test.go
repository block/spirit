// Copyright 2020 PingCAP, Inc.
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
	"strings"
	"testing"

	"github.com/block/spirit/pkg/parser"
	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/parser/mysql"
	"github.com/stretchr/testify/require"
)

func TestParseHint(t *testing.T) {
	testCases := []struct {
		input  string
		mode   mysql.SQLMode
		output []*ast.TableOptimizerHint
		errs   []string
	}{
		{
			input: "",
			errs:  []string{`Optimizer hint syntax error at line 1 `},
		},
		{
			input: "QB_NAME(qb1) QB_NAME(`qb2`), QB_NAME(TRUE) QB_NAME(\"ANSI quoted\") QB_NAME(_utf8), QB_NAME(0b10) QB_NAME(0x1a)",
			mode:  mysql.ModeANSIQuotes,
			output: []*ast.TableOptimizerHint{
				{
					HintName: ast.NewCIStr("QB_NAME"),
					QBName:   ast.NewCIStr("qb1"),
				},
				{
					HintName: ast.NewCIStr("QB_NAME"),
					QBName:   ast.NewCIStr("qb2"),
				},
				{
					HintName: ast.NewCIStr("QB_NAME"),
					QBName:   ast.NewCIStr("TRUE"),
				},
				{
					HintName: ast.NewCIStr("QB_NAME"),
					QBName:   ast.NewCIStr("ANSI quoted"),
				},
				{
					HintName: ast.NewCIStr("QB_NAME"),
					QBName:   ast.NewCIStr("_utf8"),
				},
				{
					HintName: ast.NewCIStr("QB_NAME"),
					QBName:   ast.NewCIStr("0b10"),
				},
				{
					HintName: ast.NewCIStr("QB_NAME"),
					QBName:   ast.NewCIStr("0x1a"),
				},
			},
		},
		{
			input: "QB_NAME(1)",
			errs:  []string{`Optimizer hint syntax error at line 1 `},
		},
		{
			input: "QB_NAME(1.5)",
			errs: []string{
				`Cannot use decimal number`,
				`Optimizer hint syntax error at line 1 `,
			},
		},
		{
			input: "QB_NAME('string literal')",
			errs:  []string{`Optimizer hint syntax error at line 1 `},
		},
		{
			input: "QB_NAME(many identifiers)",
			errs:  []string{`Optimizer hint syntax error at line 1 `},
		},
		{
			input: "QB_NAME(@qb1)",
			errs:  []string{`Optimizer hint syntax error at line 1 `},
		},
		{
			input: "QB_NAME(b'10')",
			errs: []string{
				`Cannot use bit-value literal`,
				`Optimizer hint syntax error at line 1 `,
			},
		},
		{
			input: "QB_NAME(x'1a')",
			errs: []string{
				`Cannot use hexadecimal literal`,
				`Optimizer hint syntax error at line 1 `,
			},
		},
		{
			// MySQL hints TiDB never implemented parse but are reported
			// (and dropped) as unsupported.
			input: "JOIN_FIXED_ORDER() BKA()",
			errs: []string{
				`Optimizer hint JOIN_FIXED_ORDER is not supported`,
				`Optimizer hint BKA is not supported`,
			},
		},
		{
			input: "SEMIJOIN(@qb1 FIRSTMATCH, LOOSESCAN) NO_SEMIJOIN(DUPSWEEDOUT, MATERIALIZATION)",
			errs: []string{
				`Optimizer hint SEMIJOIN is not supported`,
				`Optimizer hint NO_SEMIJOIN is not supported`,
			},
		},
		{
			input: "HASH_JOIN() NO_HASH_JOIN(x, `y y`.z@qb) MERGE(@qb1)",
			output: []*ast.TableOptimizerHint{
				{
					HintName: ast.NewCIStr("HASH_JOIN"),
				},
				{
					HintName: ast.NewCIStr("NO_HASH_JOIN"),
					Tables: []ast.HintTable{
						{TableName: ast.NewCIStr("x")},
						{DBName: ast.NewCIStr("y y"), TableName: ast.NewCIStr("z"), QBName: ast.NewCIStr("qb")},
					},
				},
				{
					HintName: ast.NewCIStr("MERGE"),
					QBName:   ast.NewCIStr("qb1"),
				},
			},
		},
		{
			input: "ORDER_INDEX(@qb1 tbl1 x, y, z) NO_ORDER_INDEX(tbl2@qb2 c1)",
			output: []*ast.TableOptimizerHint{
				{
					HintName: ast.NewCIStr("ORDER_INDEX"),
					Tables:   []ast.HintTable{{TableName: ast.NewCIStr("tbl1")}},
					QBName:   ast.NewCIStr("qb1"),
					Indexes:  []ast.CIStr{ast.NewCIStr("x"), ast.NewCIStr("y"), ast.NewCIStr("z")},
				},
				{
					HintName: ast.NewCIStr("NO_ORDER_INDEX"),
					Tables:   []ast.HintTable{{TableName: ast.NewCIStr("tbl2"), QBName: ast.NewCIStr("qb2")}},
					Indexes:  []ast.CIStr{ast.NewCIStr("c1")},
				},
			},
		},
		{
			input: "MRR(tbl1 idx1) NO_ICP(tbl2) INDEX_MERGE(tbl3 x, y)",
			errs: []string{
				`Optimizer hint MRR is not supported`,
				`Optimizer hint NO_ICP is not supported`,
				`Optimizer hint INDEX_MERGE is not supported`,
			},
		},
		{
			input: "MAX_EXECUTION_TIME(1000) MAX_EXECUTION_TIME(@qb1 3000)",
			output: []*ast.TableOptimizerHint{
				{
					HintName: ast.NewCIStr("MAX_EXECUTION_TIME"),
					HintData: uint64(1000),
				},
				{
					HintName: ast.NewCIStr("MAX_EXECUTION_TIME"),
					QBName:   ast.NewCIStr("qb1"),
					HintData: uint64(3000),
				},
			},
		},
		{
			input: "NO_INDEX_MERGE() RESOURCE_GROUP(rg1)",
			output: []*ast.TableOptimizerHint{
				{
					HintName: ast.NewCIStr("NO_INDEX_MERGE"),
				},
				{
					HintName: ast.NewCIStr("RESOURCE_GROUP"),
					HintData: "rg1",
				},
			},
		},
		{
			input: `SET_VAR(sbs = 16M) SET_VAR(fkc=OFF) SET_VAR(os="mcb=off") set_var(abc=1) set_var(os2='mcb2=off') set_var(sel=0.3) set_var(sel_plus=+0.3) set_var(sel_minus=-0.3)`,
			output: []*ast.TableOptimizerHint{
				{
					HintName: ast.NewCIStr("SET_VAR"),
					HintData: ast.HintSetVar{
						VarName: "sbs",
						Value:   "16M",
					},
				},
				{
					HintName: ast.NewCIStr("SET_VAR"),
					HintData: ast.HintSetVar{
						VarName: "fkc",
						Value:   "OFF",
					},
				},
				{
					HintName: ast.NewCIStr("SET_VAR"),
					HintData: ast.HintSetVar{
						VarName: "os",
						Value:   "mcb=off",
					},
				},
				{
					HintName: ast.NewCIStr("set_var"),
					HintData: ast.HintSetVar{
						VarName: "abc",
						Value:   "1",
					},
				},
				{
					HintName: ast.NewCIStr("set_var"),
					HintData: ast.HintSetVar{
						VarName: "os2",
						Value:   "mcb2=off",
					},
				},
				{
					HintName: ast.NewCIStr("set_var"),
					HintData: ast.HintSetVar{
						VarName: "sel",
						Value:   "0.3",
					},
				},
				{
					HintName: ast.NewCIStr("set_var"),
					HintData: ast.HintSetVar{
						VarName: "sel_plus",
						Value:   "0.3",
					},
				},
				{
					HintName: ast.NewCIStr("set_var"),
					HintData: ast.HintSetVar{
						VarName: "sel_minus",
						Value:   "-0.3",
					},
				},
			},
		},
		{
			// TiDB-only hints are no longer recognized: hints with a plain
			// identifier list degrade to an "unsupported" warning, ...
			input: "INL_JOIN(x, z) USE_INDEX(tbl3, PRIMARY)",
			errs: []string{
				`Optimizer hint INL_JOIN is not supported`,
				`Optimizer hint USE_INDEX is not supported`,
			},
		},
		{
			// ... and TiDB-specific argument shapes are syntax errors.
			input: "MEMORY_QUOTA(8 MB)",
			errs:  []string{`Optimizer hint syntax error at line 1 `},
		},
		{
			input: "READ_FROM_STORAGE(TIKV[a, b])",
			errs:  []string{`Optimizer hint syntax error at line 1 `},
		},
		{
			input: "LEADING(a, (b, c))",
			errs:  []string{`Optimizer hint syntax error at line 1 `},
		},
		{
			input: "TIME_RANGE('2020-02-20 12:12:12','2020-02-20 13:12:12')",
			errs:  []string{`Optimizer hint syntax error at line 1 `},
		},
		{
			input: "unknown_hint()",
			errs:  []string{`Optimizer hint syntax error at line 1 `},
		},
		{
			input: "set_var(timestamp = 1.5)",
			output: []*ast.TableOptimizerHint{
				{
					HintName: ast.NewCIStr("set_var"),
					HintData: ast.HintSetVar{
						VarName: "timestamp",
						Value:   "1.5",
					},
				},
			},
		},
		{
			input: "set_var(timestamp = _utf8mb4'1234')", // Optimizer hint doesn't recognize _charset'strings'.
			errs:  []string{`Optimizer hint syntax error at line 1 `},
		},
		{
			input: "set_var(timestamp = 9999999999999999999999999999999999999)",
			errs: []string{
				`integer value is out of range`,
				`Optimizer hint syntax error at line 1 `,
			},
		},
	}

	for _, tc := range testCases {
		output, errs := parser.ParseHint("/*+"+tc.input+"*/", tc.mode, parser.Pos{Line: 1})
		require.Lenf(t, errs, len(tc.errs), "input = %s,\n... errs = %q", tc.input, errs)
		for i, err := range errs {
			require.Errorf(t, err, "input = %s, i = %d", tc.input, i)
			require.Containsf(t, err.Error(), tc.errs[i], "input = %s, i = %d", tc.input, i)
		}
		require.Equalf(t, tc.output, output, "input = %s,\n... output = %q", tc.input, output)
	}
}

func TestMaxOptimizerHintDepth(t *testing.T) {
	// No hint accepts nested parentheses anymore (LEADING, the one TiDB
	// hint that did, is gone), so deeply nested input degrades to a syntax
	// error rather than reaching the lexer's depth guard.
	input := "/*+HASH_JOIN(" + strings.Repeat("(", 10000) + "t" + strings.Repeat(")", 10000) + ")*/"
	mode, err := mysql.GetSQLMode(mysql.DefaultSQLMode)
	require.NoError(t, err)
	output, errs := parser.ParseHint(input, mode, parser.Pos{Line: 1})
	require.NotEmpty(t, errs)
	require.Contains(t, errs[0].Error(), "Optimizer hint syntax error")
	require.Empty(t, output)
}
