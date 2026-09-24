// Copyright 2021 PingCAP, Inc.
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

package charset_test

import (
	"fmt"
	"testing"
	"unicode/utf8"

	"github.com/block/spirit/pkg/parser/charset"
	"github.com/stretchr/testify/require"
)

func TestFindEncoding(t *testing.T) {
	// Charsets with a real transcoder.
	for _, chs := range []string{charset.CharsetUTF8MB4, charset.CharsetUTF8, charset.CharsetLatin1, charset.CharsetASCII, charset.CharsetBin} {
		enc := charset.FindEncoding(chs)
		require.NotEqual(t, charset.EncodingTpNone, enc.Tp(), chs)
	}

	// Charsets without one (including gbk/gb18030, whose transcoders were
	// dropped in the fork) fall back to the pass-through binary encoding.
	for _, chs := range []string{"gbk", "gb18030", "big5", "latin2", ""} {
		enc := charset.FindEncoding(chs)
		require.Equal(t, charset.CharsetBin, enc.Name(), chs)
	}
}

func TestEncodingValidate(t *testing.T) {
	oxfffefd := string([]byte{0xff, 0xfe, 0xfd})
	testCases := []struct {
		chs      string
		str      string
		expected string
		nSrc     int
		ok       bool
	}{
		{charset.CharsetASCII, "", "", 0, true},
		{charset.CharsetASCII, "qwerty", "qwerty", 6, true},
		{charset.CharsetASCII, "qwÊrty", "qw?rty", 2, false},
		{charset.CharsetASCII, "中文", "??", 0, false},
		{charset.CharsetASCII, "中文?qwert", "???qwert", 0, false},
		{charset.CharsetUTF8MB4, "", "", 0, true},
		{charset.CharsetUTF8MB4, "qwerty", "qwerty", 6, true},
		{charset.CharsetUTF8MB4, "qwÊrty", "qwÊrty", 7, true},
		{charset.CharsetUTF8MB4, "qwÊ合法字符串", "qwÊ合法字符串", 19, true},
		{charset.CharsetUTF8MB4, "😂", "😂", 4, true},
		{charset.CharsetUTF8MB4, oxfffefd, "???", 0, false},
		{charset.CharsetUTF8MB4, "中文" + oxfffefd, "中文???", 6, false},
		{charset.CharsetUTF8MB4, string(utf8.RuneError), "�", 3, true},
		{charset.CharsetUTF8, "", "", 0, true},
		{charset.CharsetUTF8, "qwerty", "qwerty", 6, true},
		{charset.CharsetUTF8, "qwÊrty", "qwÊrty", 7, true},
		{charset.CharsetUTF8, "qwÊ合法字符串", "qwÊ合法字符串", 19, true},
		{charset.CharsetUTF8, "😂", "?", 0, false},
		{charset.CharsetUTF8, "valid_str😂", "valid_str?", 9, false},
		{charset.CharsetUTF8, oxfffefd, "???", 0, false},
		{charset.CharsetUTF8, "中文" + oxfffefd, "中文???", 6, false},
		{charset.CharsetUTF8, string(utf8.RuneError), "�", 3, true},
	}
	for _, tc := range testCases {
		msg := fmt.Sprintf("%v", tc)
		enc := charset.FindEncoding(tc.chs)
		if tc.chs == charset.CharsetUTF8 {
			enc = charset.EncodingUTF8MB3StrictImpl
		}
		strBytes := []byte(tc.str)
		require.Equal(t, tc.ok, enc.IsValid(strBytes), msg)
		replace, _ := enc.Transform(nil, strBytes, charset.OpReplaceNoErr)
		require.Equal(t, tc.expected, string(replace), msg)
	}
}
