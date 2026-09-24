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
}

%token	<number>

	/*yy:token "%d" */
	hintIntLit "a 64-bit unsigned integer"

%token	<ident>

	/*yy:token "%c" */
	hintIdentifier
	hintInvalid    "a special token never used by parser, used by lexer to indicate error"

	/*yy:token "@%c" */
	hintSingleAtIdentifier "identifier with single leading at"

	/*yy:token "'%c'" */
	hintStringLit

	/* SET_VAR-only decimal/float literal. Integer values still use hintIntLit. */
	hintNumericLit

	/* MySQL 8.0 hint names */
	hintJoinFixedOrder      "JOIN_FIXED_ORDER"
	hintJoinOrder           "JOIN_ORDER"
	hintJoinPrefix          "JOIN_PREFIX"
	hintJoinSuffix          "JOIN_SUFFIX"
	hintBKA                 "BKA"
	hintNoBKA               "NO_BKA"
	hintBNL                 "BNL"
	hintNoBNL               "NO_BNL"
	hintHashJoin            "HASH_JOIN"
	hintNoHashJoin          "NO_HASH_JOIN"
	hintMerge               "MERGE"
	hintNoMerge             "NO_MERGE"
	hintIndexMerge          "INDEX_MERGE"
	hintNoIndexMerge        "NO_INDEX_MERGE"
	hintMRR                 "MRR"
	hintNoMRR               "NO_MRR"
	hintNoICP               "NO_ICP"
	hintNoRangeOptimization "NO_RANGE_OPTIMIZATION"
	hintSkipScan            "SKIP_SCAN"
	hintNoSkipScan          "NO_SKIP_SCAN"
	hintSemijoin            "SEMIJOIN"
	hintNoSemijoin          "NO_SEMIJOIN"
	hintOrderIndex          "ORDER_INDEX"
	hintNoOrderIndex        "NO_ORDER_INDEX"
	hintMaxExecutionTime    "MAX_EXECUTION_TIME"
	hintSetVar              "SET_VAR"
	hintResourceGroup       "RESOURCE_GROUP"
	hintQBName              "QB_NAME"

	/* SEMIJOIN() strategies */
	hintDupsWeedOut     "DUPSWEEDOUT"
	hintFirstMatch      "FIRSTMATCH"
	hintLooseScan       "LOOSESCAN"
	hintMaterialization "MATERIALIZATION"

%type	<ident>
	Identifier                             "identifier (including keywords)"
	QueryBlockOpt                          "Query block identifier optional"
	JoinOrderOptimizerHintName
	UnsupportedTableLevelOptimizerHintName
	SupportedTableLevelOptimizerHintName
	UnsupportedIndexLevelOptimizerHintName
	SupportedIndexLevelOptimizerHintName
	SubqueryOptimizerHintName
	NullaryHintName                        "name of hints which take no input"
	SubqueryStrategy
	Value                                  "the value in the SET_VAR() hint"

%type	<number>
	CommaOpt "optional ','"

%type	<hints>
	OptimizerHintList "optimizer hint list"

%type	<hint>
	TableOptimizerHintOpt "optimizer hint"
	HintTableList         "table list in optimizer hint"
	HintTableListOpt      "optional table list in optimizer hint"
	HintIndexList         "table name with index list in optimizer hint"
	IndexNameList         "index list in optimizer hint"
	IndexNameListOpt      "optional index list in optimizer hint"
	ViewNameList          "view name list in optimizer hint"
	SubqueryStrategies    "subquery strategies"
	SubqueryStrategiesOpt "optional subquery strategies"

%type	<table>
	HintTable "Table in optimizer hint"
	ViewName  "View name in optimizer hint"

%type	<modelIdents>
	PartitionList "partition name list in optimizer hint"


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
		parser.warnUnsupportedHint($1)
		$$ = nil
	}
|	JoinOrderOptimizerHintName '(' HintTableList ')'
	{
		parser.warnUnsupportedHint($1)
		$$ = nil
	}
|	UnsupportedTableLevelOptimizerHintName '(' HintTableListOpt ')'
	{
		parser.warnUnsupportedHint($1)
		$$ = nil
	}
|	SupportedTableLevelOptimizerHintName '(' HintTableListOpt ')'
	{
		h := $3
		h.HintName = ast.NewCIStr($1)
		$$ = h
	}
|	UnsupportedIndexLevelOptimizerHintName '(' HintIndexList ')'
	{
		parser.warnUnsupportedHint($1)
		$$ = nil
	}
|	SupportedIndexLevelOptimizerHintName '(' HintIndexList ')'
	{
		h := $3
		h.HintName = ast.NewCIStr($1)
		$$ = h
	}
|	SubqueryOptimizerHintName '(' QueryBlockOpt SubqueryStrategiesOpt ')'
	{
		parser.warnUnsupportedHint($1)
		$$ = nil
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
		$$ = &ast.TableOptimizerHint{
			HintName: ast.NewCIStr($1),
			HintData: ast.HintSetVar{
				VarName: $3,
				Value:   $5,
			},
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
|	NullaryHintName '(' QueryBlockOpt ')'
	{
		$$ = &ast.TableOptimizerHint{
			HintName: ast.NewCIStr($1),
			QBName:   ast.NewCIStr($3),
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
SubqueryStrategiesOpt:
	/* empty */
	{}
|	SubqueryStrategies

SubqueryStrategies:
	SubqueryStrategy
	{}
|	SubqueryStrategies ',' SubqueryStrategy

Value:
	hintStringLit
|	Identifier
|	hintNumericLit
|	hintIntLit
	{
		$$ = strconv.FormatUint($1, 10)
	}
|	'+' hintNumericLit
	{
		$$ = $2
	}
|	'-' hintNumericLit
	{
		$$ = "-" + $2
	}
|	'+' hintIntLit
	{
		$$ = strconv.FormatUint($2, 10)
	}
|	'-' hintIntLit
	{
		if $2 > 9223372036854775808 {
			yylex.AppendError(yylex.Errorf("the Signed Value should be at the range of [-9223372036854775808, 9223372036854775807]."))
			return 1
		} else if $2 == 9223372036854775808 {
			signed_one := int64(1)
			$$ = strconv.FormatInt(signed_one<<63, 10)
		} else {
			$$ = strconv.FormatInt(-int64($2), 10)
		}
	}

JoinOrderOptimizerHintName:
	"JOIN_ORDER"
|	"JOIN_PREFIX"
|	"JOIN_SUFFIX"

UnsupportedTableLevelOptimizerHintName:
	"BKA"
|	"NO_BKA"
|	"BNL"
|	"NO_BNL"
|	"NO_MERGE"

SupportedTableLevelOptimizerHintName:
	"MERGE"
|	"HASH_JOIN"
|	"NO_HASH_JOIN"

UnsupportedIndexLevelOptimizerHintName:
	"INDEX_MERGE"
/* NO_INDEX_MERGE is accepted only in nullary form */
|	"MRR"
|	"NO_MRR"
|	"NO_ICP"
|	"NO_RANGE_OPTIMIZATION"
|	"SKIP_SCAN"
|	"NO_SKIP_SCAN"

SupportedIndexLevelOptimizerHintName:
	"ORDER_INDEX"
|	"NO_ORDER_INDEX"

SubqueryOptimizerHintName:
	"SEMIJOIN"
|	"NO_SEMIJOIN"

SubqueryStrategy:
	"DUPSWEEDOUT"
|	"FIRSTMATCH"
|	"LOOSESCAN"
|	"MATERIALIZATION"

NullaryHintName:
	"NO_INDEX_MERGE"

Identifier:
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
|	"SET_VAR"
|	"RESOURCE_GROUP"
|	"QB_NAME"
/* SEMIJOIN() strategies */
|	"DUPSWEEDOUT"
|	"FIRSTMATCH"
|	"LOOSESCAN"
|	"MATERIALIZATION"
%%
