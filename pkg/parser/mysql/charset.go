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

package mysql

// MySQL collation information.
const (
	UTF8Charset    = "utf8"
	UTF8MB4Charset = "utf8mb4"
	DefaultCharset = UTF8MB4Charset

	UTF8MB4DefaultCollation = "utf8mb4_bin"
	DefaultCollationName    = UTF8MB4DefaultCollation
)

// IsUTF8Charset checks if charset is utf8, utf8mb4.
func IsUTF8Charset(charset string) bool {
	return charset == UTF8Charset || charset == UTF8MB4Charset
}
