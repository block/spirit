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

//go:build reserved_words_test
// +build reserved_words_test

// This file ensures that the set of reserved keywords is the same as that of
// MySQL. To run:
//
//  1. Set up a MySQL server listening at 127.0.0.1:3306 using root and no password
//  2. Run this test with:
//
//		go test -tags reserved_words_test -run '^TestCompareReservedWordsWithMySQL$'

package parser

import (
	// needed to connect to MySQL
	dbsql "database/sql"
	gio "io"
	"os"
	"testing"

	_ "github.com/block/mysql"
	"github.com/block/spirit/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func TestCompareReservedWordsWithMySQL(t *testing.T) {
	parserFilename := "parser.y"
	parserFile, err := os.Open(parserFilename)
	require.NoError(t, err)
	data, err := gio.ReadAll(parserFile)
	require.NoError(t, err)
	content := string(data)

	reservedKeywordStartMarker := "\t/* The following tokens belong to ReservedKeyword. Notice: make sure these tokens are contained in ReservedKeyword. */"
	unreservedKeywordStartMarker := "\t/* The following tokens belong to UnReservedKeyword. Notice: make sure these tokens are contained in UnReservedKeyword. */"
	notKeywordTokenStartMarker := "\t/* The following tokens belong to NotKeywordToken. Notice: make sure these tokens are contained in NotKeywordToken. */"
	identTokenEndMarker := "%token\t<item>"

	reservedKeywords := extractKeywords(content, reservedKeywordStartMarker, unreservedKeywordStartMarker)
	unreservedKeywords := extractKeywords(content, unreservedKeywordStartMarker, notKeywordTokenStartMarker)
	notKeywordTokens := extractKeywords(content, notKeywordTokenStartMarker, identTokenEndMarker)

	p := New()
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		dsn = "root@tcp(127.0.0.1:3306)/"
	}
	db, err := dbsql.Open("block-mysql", dsn)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	for _, kw := range reservedKeywords {
		switch kw {
		case "CURRENT_ROLE", // reserved here, not reserved in MySQL
			"ARRAY": // added in 8.0.17 (reserved); became nonreserved in 8.0.19
			// special cases: we do reserve these words but MySQL didn't,
			// and unreserving them causes legit parser conflicts.
			continue
		}

		query := "do (select 1 as " + kw + ")"
		errRegexp := ".*" + kw + ".*"

		var err error

		if _, ok := windowFuncTokenMap[kw]; !ok {
			// window function tokens parse in this position despite being reserved.
			_, _, err = p.Parse(query, "", "")
			require.Error(t, err)
			require.Regexp(t, errRegexp, err.Error())
		}
		_, err = db.Exec(query)
		require.Error(t, err, query)
		require.Regexp(t, errRegexp, err.Error(), "MySQL suggests that '%s' should *not* be reserved!", kw)
	}

	for _, kws := range [][]string{unreservedKeywords, notKeywordTokens} {
		for _, kw := range kws {
			switch kw {
			case "FUNCTION", // Reserved in MySQL 8.0.1
				"PURGE",     // Reserved in MySQL
				"SYSTEM",    // Reserved in MySQL 8.0.3
				"SEPARATOR", // Reserved in MySQL
				"DECLARE":   // Reserved in MySQL
				continue
			}

			query := "do (select 1 as " + kw + ")"

			stmts, _, err := p.Parse(query, "", "")
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			require.IsType(t, &ast.DoStmt{}, stmts[0])

			_, err = db.Exec(query)
			require.NoErrorf(t, err, "MySQL suggests that '%s' should be reserved!", kw)
		}
	}
}
