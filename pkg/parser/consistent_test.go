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

package parser

import (
	gio "io"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKeywordConsistent(t *testing.T) {
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

	for k, v := range aliases {
		require.NotEqual(t, k, v)
		require.Equal(t, tokenMap[v], tokenMap[k])
	}
	keywordCount := len(reservedKeywords) + len(unreservedKeywords) + len(notKeywordTokens)
	require.Equal(t, keywordCount-len(windowFuncTokenMap), len(tokenMap)-len(aliases))

	// UnReservedKeyword is defined as LabelKeyword plus NonLabelKeyword, the
	// latter being the keywords that cannot open a compound statement label.
	// Together they must still cover exactly the unreserved token block.
	unreservedCollectionDef := extractKeywordsFromCollectionDef(content, "\nLabelKeyword:")
	unreservedCollectionDef = append(unreservedCollectionDef,
		extractKeywordsFromCollectionDef(content, "\nNonLabelKeyword:")...)
	sort.Strings(unreservedCollectionDef)
	require.Equal(t, unreservedCollectionDef, unreservedKeywords, "UnReservedKeyword")

	notKeywordTokensCollectionDef := extractKeywordsFromCollectionDef(content, "\nNotKeywordToken:")
	require.Equal(t, notKeywordTokensCollectionDef, notKeywordTokens, "NotKeywordToken")

}

func extractMiddle(str, startMarker, endMarker string) string {
	_, str, ok := strings.Cut(str, startMarker)
	if !ok {
		return ""
	}
	middle, _, ok := strings.Cut(str, endMarker)
	if !ok {
		return ""
	}
	return middle
}

func extractQuotedWords(strs []string) []string {
	//nolint: prealloc
	var words []string
	for _, str := range strs {
		word := extractMiddle(str, "\"", "\"")
		if word == "" {
			continue
		}
		words = append(words, word)
	}
	sort.Strings(words)
	return words
}

func extractKeywords(content, startMarker, endMarker string) []string {
	keywordSection := extractMiddle(content, startMarker, endMarker)
	lines := strings.Split(keywordSection, "\n")
	return extractQuotedWords(lines)
}

func extractKeywordsFromCollectionDef(content, startMarker string) []string {
	keywordSection := extractMiddle(content, startMarker, "\n\n")
	words := strings.Split(keywordSection, "|")
	return extractQuotedWords(words)
}
