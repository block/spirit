%{
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

package parser

import (
	"strconv"

	"github.com/block/spirit/pkg/parser/ast"
)

%}

%union {
	offset  int
	ident   string
	number  uint64
	hint    *ast.TableOptimizerHint
	hints []*ast.TableOptimizerHint
	table 	ast.HintTable
	modelIdents []ast.CIStr
	strategies []string
	setVar     ast.HintSetVar
}

%token	<number>

	/*yy:token "%d" */
	hintIntLit "a 64-bit unsigned integer"

%token	<ident>

	/*yy:token "%c" */
	hintIdentifier

	/*yy:token "`%c`" */
	hintQuotedIdentifier "a quoted identifier"
	hintInvalid          "a special token never used by parser, used by lexer to indicate error"

	/*yy:token "@%c" */
	hintSingleAtIdentifier "identifier with single leading at"

	/*yy:token "'%c'" */
	hintStringLit

	/* SET_VAR-only decimal/float literal. Integer values still use hintIntLit. */
	hintNumericLit

	/* MySQL 8.0 hint names */
	hintJoinFixedOrder        "JOIN_FIXED_ORDER"
	hintJoinOrder             "JOIN_ORDER"
	hintJoinPrefix            "JOIN_PREFIX"
	hintJoinSuffix            "JOIN_SUFFIX"
	hintBKA                   "BKA"
	hintNoBKA                 "NO_BKA"
	hintBNL                   "BNL"
	hintNoBNL                 "NO_BNL"
	hintHashJoin              "HASH_JOIN"
	hintNoHashJoin            "NO_HASH_JOIN"
	hintMerge                 "MERGE"
	hintNoMerge               "NO_MERGE"
	hintIndexMerge            "INDEX_MERGE"
	hintNoIndexMerge          "NO_INDEX_MERGE"
	hintMRR                   "MRR"
	hintNoMRR                 "NO_MRR"
	hintNoICP                 "NO_ICP"
	hintNoRangeOptimization   "NO_RANGE_OPTIMIZATION"
	hintSkipScan              "SKIP_SCAN"
	hintNoSkipScan            "NO_SKIP_SCAN"
	hintSemijoin              "SEMIJOIN"
	hintNoSemijoin            "NO_SEMIJOIN"
	hintOrderIndex            "ORDER_INDEX"
	hintNoOrderIndex          "NO_ORDER_INDEX"
	hintMaxExecutionTime      "MAX_EXECUTION_TIME"
	hintDerivedCondPushdown   "DERIVED_CONDITION_PUSHDOWN"
	hintNoDerivedCondPushdown "NO_DERIVED_CONDITION_PUSHDOWN"
	hintGroupIndex            "GROUP_INDEX"
	hintNoGroupIndex          "NO_GROUP_INDEX"
	hintIndex                 "INDEX"
	hintNoIndex               "NO_INDEX"
	hintJoinIndex             "JOIN_INDEX"
	hintNoJoinIndex           "NO_JOIN_INDEX"
	hintSubquery              "SUBQUERY"
	hintSetVar                "SET_VAR"
	hintResourceGroup         "RESOURCE_GROUP"
	hintQBName                "QB_NAME"

	/* SEMIJOIN() strategies */
	hintDupsWeedOut     "DUPSWEEDOUT"
	hintFirstMatch      "FIRSTMATCH"
	hintLooseScan       "LOOSESCAN"
	hintMaterialization "MATERIALIZATION"

	/* SUBQUERY() strategies (besides MATERIALIZATION) */
	hintIntoExists "INTOEXISTS"

%type	<ident>
	Identifier                  "identifier (including keywords)"
	UnquotedIdentifier          "unquoted identifier (including keywords)"
	QueryBlockOpt               "Query block identifier optional"
	JoinOrderOptimizerHintName
	TableLevelOptimizerHintName
	IndexLevelOptimizerHintName
	SemijoinOptimizerHintName
	SemijoinStrategy
	SubqueryStrategy

%type	<number>
	CommaOpt "optional ','"

%type	<hints>
	OptimizerHintList "optimizer hint list"

%type	<hint>
	TableOptimizerHintOpt "optimizer hint"
	HintTableList         "table list in optimizer hint"
	HintTableListOpt      "optional table list in optimizer hint"
	HintIndexList         "table name with index list in optimizer hint"
	HintIndexListOpt      "optional table name with index list in optimizer hint"
	IndexNameList         "index list in optimizer hint"
	IndexNameListOpt      "optional index list in optimizer hint"
	ViewNameList          "view name list in optimizer hint"

%type	<table>
	HintTable "Table in optimizer hint"
	ViewName  "View name in optimizer hint"

%type	<modelIdents>
	PartitionList "partition name list in optimizer hint"

%type	<setVar>
	Value "the value in the SET_VAR() hint"

%type	<strategies>
	SemijoinStrategies    "semijoin strategies"
	SemijoinStrategiesOpt "optional semijoin strategies"


%start	Start

%%

Start:
	OptimizerHintList
	{
		parser.result = $1
	}

OptimizerHintList:
	TableOptimizerHintOpt
	{
		if $1 != nil {
			$$ = []*ast.TableOptimizerHint{$1}
		}
	}
|	OptimizerHintList CommaOpt TableOptimizerHintOpt
	{
		if $3 != nil {
			$$ = append($1, $3)
		} else {
			$$ = $1
		}
	}

TableOptimizerHintOpt:
	"JOIN_FIXED_ORDER" '(' QueryBlockOpt ')'
	{
		$$ = &ast.TableOptimizerHint{
			HintName: ast.NewCIStr($1),
			QBName:   ast.NewCIStr($3),
		}
	}
|	JoinOrderOptimizerHintName '(' HintTableList ')'
	{
		h := $3
		h.HintName = ast.NewCIStr($1)
		$$ = h
	}
|	TableLevelOptimizerHintName '(' HintTableListOpt ')'
	{
		h := $3
		h.HintName = ast.NewCIStr($1)
		$$ = h
	}
|	IndexLevelOptimizerHintName '(' HintIndexListOpt ')'
	{
		h := $3
		h.HintName = ast.NewCIStr($1)
		$$ = h
	}
|	SemijoinOptimizerHintName '(' QueryBlockOpt SemijoinStrategiesOpt ')'
	{
		h := &ast.TableOptimizerHint{
			HintName: ast.NewCIStr($1),
			QBName:   ast.NewCIStr($3),
		}
		if len($4) > 0 {
			h.HintData = $4
		}
		$$ = h
	}
|	"SUBQUERY" '(' QueryBlockOpt SubqueryStrategy ')'
	{
		$$ = &ast.TableOptimizerHint{
			HintName: ast.NewCIStr($1),
			QBName:   ast.NewCIStr($3),
			HintData: []string{$4},
		}
	}
|	"MAX_EXECUTION_TIME" '(' QueryBlockOpt hintIntLit ')'
	{
		$$ = &ast.TableOptimizerHint{
			HintName: ast.NewCIStr($1),
			QBName:   ast.NewCIStr($3),
			HintData: $4,
		}
	}
|	"SET_VAR" '(' Identifier '=' Value ')'
	{
		v := $5
		v.VarName = $3
		$$ = &ast.TableOptimizerHint{
			HintName: ast.NewCIStr($1),
			HintData: v,
		}
	}
|	"RESOURCE_GROUP" '(' Identifier ')'
	{
		$$ = &ast.TableOptimizerHint{
			HintName: ast.NewCIStr($1),
			HintData: $3,
		}
	}
|	"QB_NAME" '(' Identifier ')'
	{
		$$ = &ast.TableOptimizerHint{
			HintName: ast.NewCIStr($1),
			QBName:   ast.NewCIStr($3),
		}
	}
|	"QB_NAME" '(' Identifier ',' ViewNameList ')'
	{
		$$ = &ast.TableOptimizerHint{
			HintName: ast.NewCIStr($1),
			QBName:   ast.NewCIStr($3),
			Tables:   $5.Tables,
		}
	}
|	hintIdentifier '(' QueryBlockOpt hintIntLit ')'
	/* The hints below are pseudo hint. They are unsupported hints */
	{
		parser.warnUnsupportedHint($1)
		$$ = nil
	}
|	hintIdentifier '(' PartitionList ')'
	{
		parser.warnUnsupportedHint($1)
		$$ = nil
	}
|	hintIdentifier '(' PartitionList CommaOpt hintIntLit ')'
	{
		parser.warnUnsupportedHint($1)
		$$ = nil
	}
|	hintIdentifier '(' Identifier '=' Value ')'
	{
		parser.warnUnsupportedHint($1)
		$$ = nil
	}

QueryBlockOpt:
	/* empty */
	{
		$$ = ""
	}
|	hintSingleAtIdentifier

CommaOpt:
	/*empty*/
	{}
|	','
	{}

PartitionList:
	Identifier
	{
		$$ = []ast.CIStr{ast.NewCIStr($1)}
	}
|	PartitionList CommaOpt Identifier
	{
		$$ = append($1, ast.NewCIStr($3))
	}

/**
 * HintTableListOpt:
 *
 *	[@query_block_name] [tbl_name [, tbl_name] ...]
 *	[tbl_name@query_block_name [, tbl_name@query_block_name] ...]
 *
 */
HintTableListOpt:
	HintTableList
|	QueryBlockOpt
	{
		$$ = &ast.TableOptimizerHint{
			QBName: ast.NewCIStr($1),
		}
	}

HintTableList:
	QueryBlockOpt HintTable
	{
		$$ = &ast.TableOptimizerHint{
			Tables: []ast.HintTable{$2},
			QBName: ast.NewCIStr($1),
		}
	}
|	HintTableList ',' HintTable
	{
		h := $1
		h.Tables = append(h.Tables, $3)
		$$ = h
	}

HintTable:
	Identifier QueryBlockOpt
	{
		$$ = ast.HintTable{
			TableName: ast.NewCIStr($1),
			QBName:    ast.NewCIStr($2),
		}
	}
|	Identifier '.' Identifier QueryBlockOpt
	{
		$$ = ast.HintTable{
			DBName:    ast.NewCIStr($1),
			TableName: ast.NewCIStr($3),
			QBName:    ast.NewCIStr($4),
		}
	}

ViewNameList:
	ViewNameList '.' ViewName
	{
		h := $1
		h.Tables = append(h.Tables, $3)
		$$ = h
	}
|	ViewName
	{
		$$ = &ast.TableOptimizerHint{
			Tables: []ast.HintTable{$1},
		}
	}

ViewName:
	Identifier QueryBlockOpt
	{
		$$ = ast.HintTable{
			TableName: ast.NewCIStr($1),
			QBName:    ast.NewCIStr($2),
		}
	}
|	QueryBlockOpt
	{
		$$ = ast.HintTable{
			QBName: ast.NewCIStr($1),
		}
	}

/**
 * HintIndexList:
 *
 *	[@query_block_name] tbl_name [index_name [, index_name] ...]
 *	tbl_name@query_block_name [index_name [, index_name] ...]
 */
HintIndexListOpt:
	HintIndexList
|	QueryBlockOpt
	{
		$$ = &ast.TableOptimizerHint{
			QBName: ast.NewCIStr($1),
		}
	}

HintIndexList:
	QueryBlockOpt HintTable CommaOpt IndexNameListOpt
	{
		h := $4
		h.Tables = []ast.HintTable{$2}
		h.QBName = ast.NewCIStr($1)
		$$ = h
	}

IndexNameListOpt:
	/* empty */
	{
		$$ = &ast.TableOptimizerHint{}
	}
|	IndexNameList

IndexNameList:
	Identifier
	{
		$$ = &ast.TableOptimizerHint{
			Indexes: []ast.CIStr{ast.NewCIStr($1)},
		}
	}
|	IndexNameList ',' Identifier
	{
		h := $1
		h.Indexes = append(h.Indexes, ast.NewCIStr($3))
		$$ = h
	}

/**
 * Miscellaneous rules
 */
SemijoinStrategiesOpt:
	/* empty */
	{
		$$ = nil
	}
|	SemijoinStrategies

SemijoinStrategies:
	SemijoinStrategy
	{
		$$ = []string{$1}
	}
|	SemijoinStrategies ',' SemijoinStrategy
	{
		$$ = append($1, $3)
	}

/**
 * Value: MySQL reads a quoted value (a string literal or a quoted identifier)
 * and an unquoted word as a string, and a bare number, which may carry a
 * K/M/G suffix and then lexes as an identifier, as a number. Quoted records
 * which one it was so that Restore writes it back the same way. A signed
 * value is a syntax error in MySQL; it keeps its sign so that it restores
 * unchanged.
 */
Value:
	hintStringLit
	{
		$$ = ast.HintSetVar{Value: $1, Quoted: true}
	}
|	hintQuotedIdentifier
	{
		$$ = ast.HintSetVar{Value: $1, Quoted: true}
	}
|	UnquotedIdentifier
	{
		$$ = ast.HintSetVar{Value: $1}
	}
|	hintNumericLit
	{
		$$ = ast.HintSetVar{Value: $1}
	}
|	hintIntLit
	{
		$$ = ast.HintSetVar{Value: strconv.FormatUint($1, 10)}
	}
|	'+' hintNumericLit
	{
		$$ = ast.HintSetVar{Value: "+" + $2}
	}
|	'-' hintNumericLit
	{
		$$ = ast.HintSetVar{Value: "-" + $2}
	}
|	'+' hintIntLit
	{
		$$ = ast.HintSetVar{Value: "+" + strconv.FormatUint($2, 10)}
	}
|	'-' hintIntLit
	{
		if $2 > 9223372036854775808 {
			yylex.AppendError(yylex.Errorf("the Signed Value should be at the range of [-9223372036854775808, 9223372036854775807]."))
			return 1
		}
		// Keep the text as written, so -0 does not lose its sign.
		$$ = ast.HintSetVar{Value: "-" + strconv.FormatUint($2, 10)}
	}

JoinOrderOptimizerHintName:
	"JOIN_ORDER"
|	"JOIN_PREFIX"
|	"JOIN_SUFFIX"

/**
 * Table-level hints:
 *
 *	hint_name([@query_block_name] [tbl_name [, tbl_name] ...])
 *	hint_name([tbl_name@query_block_name [, tbl_name@query_block_name] ...])
 */
TableLevelOptimizerHintName:
	"BKA"
|	"NO_BKA"
|	"BNL"
|	"NO_BNL"
|	"DERIVED_CONDITION_PUSHDOWN"
|	"NO_DERIVED_CONDITION_PUSHDOWN"
|	"HASH_JOIN"
|	"NO_HASH_JOIN"
|	"MERGE"
|	"NO_MERGE"

/**
 * Index-level hints:
 *
 *	hint_name([@query_block_name] tbl_name [index_name [, index_name] ...])
 *	hint_name(tbl_name@query_block_name [index_name [, index_name] ...])
 *
 * The table is optional here (MySQL requires it) so that existing
 * NO_INDEX_MERGE() and NO_INDEX_MERGE(@qb) input keeps parsing; such a hint
 * restores unchanged.
 */
IndexLevelOptimizerHintName:
	"GROUP_INDEX"
|	"NO_GROUP_INDEX"
|	"INDEX"
|	"NO_INDEX"
|	"INDEX_MERGE"
|	"NO_INDEX_MERGE"
|	"JOIN_INDEX"
|	"NO_JOIN_INDEX"
|	"MRR"
|	"NO_MRR"
|	"NO_ICP"
|	"NO_RANGE_OPTIMIZATION"
|	"ORDER_INDEX"
|	"NO_ORDER_INDEX"
|	"SKIP_SCAN"
|	"NO_SKIP_SCAN"

SemijoinOptimizerHintName:
	"SEMIJOIN"
|	"NO_SEMIJOIN"

SemijoinStrategy:
	"DUPSWEEDOUT"
|	"FIRSTMATCH"
|	"LOOSESCAN"
|	"MATERIALIZATION"

SubqueryStrategy:
	"INTOEXISTS"
|	"MATERIALIZATION"

Identifier:
	UnquotedIdentifier
|	hintQuotedIdentifier

UnquotedIdentifier:
	hintIdentifier
/* MySQL 8.0 hint names */
|	"JOIN_FIXED_ORDER"
|	"JOIN_ORDER"
|	"JOIN_PREFIX"
|	"JOIN_SUFFIX"
|	"BKA"
|	"NO_BKA"
|	"BNL"
|	"NO_BNL"
|	"HASH_JOIN"
|	"NO_HASH_JOIN"
|	"MERGE"
|	"NO_MERGE"
|	"INDEX_MERGE"
|	"NO_INDEX_MERGE"
|	"MRR"
|	"NO_MRR"
|	"NO_ICP"
|	"NO_RANGE_OPTIMIZATION"
|	"SKIP_SCAN"
|	"NO_SKIP_SCAN"
|	"SEMIJOIN"
|	"NO_SEMIJOIN"
|	"ORDER_INDEX"
|	"NO_ORDER_INDEX"
|	"MAX_EXECUTION_TIME"
|	"DERIVED_CONDITION_PUSHDOWN"
|	"NO_DERIVED_CONDITION_PUSHDOWN"
|	"GROUP_INDEX"
|	"NO_GROUP_INDEX"
|	"INDEX"
|	"NO_INDEX"
|	"JOIN_INDEX"
|	"NO_JOIN_INDEX"
|	"SUBQUERY"
|	"SET_VAR"
|	"RESOURCE_GROUP"
|	"QB_NAME"
/* SEMIJOIN() strategies */
|	"DUPSWEEDOUT"
|	"FIRSTMATCH"
|	"LOOSESCAN"
|	"MATERIALIZATION"
|	"INTOEXISTS"
%%
