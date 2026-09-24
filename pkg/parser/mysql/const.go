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

package mysql

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/parser/format"
)

func newInvalidModeErr(s string) error {
	// The capitalization matches MySQL's ER_WRONG_VALUE_FOR_VAR message.
	return fmt.Errorf("Variable 'sql_mode' can't be set to the value of '%s'", s) //nolint:staticcheck
}

// Identifier length limitations.
// See https://dev.mysql.com/doc/refman/5.7/en/identifiers.html
const (
	// MaxPayloadLen is the max packet payload length.
	MaxPayloadLen = 1<<24 - 1
	// MaxTableNameLength is max length of table name identifier.
	MaxTableNameLength = 64
	// MaxDatabaseNameLength is max length of database name identifier.
	MaxDatabaseNameLength = 64
	// MaxColumnNameLength is max length of column name identifier.
	MaxColumnNameLength = 64
	// MaxKeyParts is max length of key parts.
	MaxKeyParts = 16
	// MaxIndexIdentifierLen is max length of index identifier.
	MaxIndexIdentifierLen = 64
	// MaxForeignKeyIdentifierLen is max length of foreign key identifier.
	MaxForeignKeyIdentifierLen = 64
	// MaxConstraintIdentifierLen is max length of constrain identifier.
	MaxConstraintIdentifierLen = 64
	// MaxViewIdentifierLen is max length of view identifier.
	MaxViewIdentifierLen = 64
	// MaxAliasIdentifierLen is max length of alias identifier.
	MaxAliasIdentifierLen = 256
	// MaxUserDefinedVariableLen is max length of user-defined variable.
	MaxUserDefinedVariableLen = 64
)

// ErrTextLength error text length limit.
const ErrTextLength = 80

// Auth name information.
const (
	AuthNativePassword = "mysql_native_password" // #nosec G101
)

// MySQL type maximum length.
const (
	// NotFixedDec For arguments that have no fixed number of decimals, the decimals value is set to 31,
	// which is 1 more than the maximum number of decimals permitted for the DECIMAL, FLOAT, and DOUBLE data types.
	NotFixedDec = 31

	MaxDecimalScale          = 30
	MaxDecimalWidth          = 65
	MaxDatetimeWidthNoFsp    = 19 // YYYY-MM-DD HH:MM:SS
	MaxDurationWidthNoFsp    = 10 // HH:MM:SS
	MaxFloatPrecisionLength  = 24
	MaxDoublePrecisionLength = 53
)

// MaxTypeSetMembers is the number of set members.
const MaxTypeSetMembers = 64

// DefaultSQLMode for GLOBAL_VARIABLES
const DefaultSQLMode = "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"

// DefaultLengthOfMysqlTypes is the map for default physical length of MySQL data types.
// See http://dev.mysql.com/doc/refman/5.7/en/storage-requirements.html
var DefaultLengthOfMysqlTypes = map[byte]int{
	TypeYear:      1,
	TypeDate:      3,
	TypeDuration:  3,
	TypeDatetime:  8,
	TypeTimestamp: 4,

	TypeTiny:     1,
	TypeShort:    2,
	TypeInt24:    3,
	TypeLong:     4,
	TypeLonglong: 8,
	TypeFloat:    4,
	TypeDouble:   8,

	TypeEnum:   2,
	TypeString: 1,
	TypeSet:    8,
}

// DefaultLengthOfTimeFraction is the map for default physical length of time fractions.
var DefaultLengthOfTimeFraction = map[int]int{
	0: 0,

	1: 1,
	2: 1,

	3: 2,
	4: 2,

	5: 3,
	6: 3,
}

// SQLMode is the type for MySQL sql_mode.
// See https://dev.mysql.com/doc/refman/5.7/en/sql-mode.html
type SQLMode int64

// HasNoZeroDateMode detects if 'NO_ZERO_DATE' mode is set in SQLMode
func (m SQLMode) HasNoZeroDateMode() bool {
	return m&ModeNoZeroDate == ModeNoZeroDate
}

// HasNoZeroInDateMode detects if 'NO_ZERO_IN_DATE' mode is set in SQLMode
func (m SQLMode) HasNoZeroInDateMode() bool {
	return m&ModeNoZeroInDate == ModeNoZeroInDate
}

// HasErrorForDivisionByZeroMode detects if 'ERROR_FOR_DIVISION_BY_ZERO' mode is set in SQLMode
func (m SQLMode) HasErrorForDivisionByZeroMode() bool {
	return m&ModeErrorForDivisionByZero == ModeErrorForDivisionByZero
}

// HasOnlyFullGroupBy detects if 'ONLY_FULL_GROUP_BY' mode is set in SQLMode
func (m SQLMode) HasOnlyFullGroupBy() bool {
	return m&ModeOnlyFullGroupBy == ModeOnlyFullGroupBy
}

// HasStrictMode detects if 'STRICT_TRANS_TABLES' or 'STRICT_ALL_TABLES' mode is set in SQLMode
func (m SQLMode) HasStrictMode() bool {
	return m&ModeStrictTransTables == ModeStrictTransTables || m&ModeStrictAllTables == ModeStrictAllTables
}

// HasPipesAsConcatMode detects if 'PIPES_AS_CONCAT' mode is set in SQLMode
func (m SQLMode) HasPipesAsConcatMode() bool {
	return m&ModePipesAsConcat == ModePipesAsConcat
}

// HasNoUnsignedSubtractionMode detects if 'NO_UNSIGNED_SUBTRACTION' mode is set in SQLMode
func (m SQLMode) HasNoUnsignedSubtractionMode() bool {
	return m&ModeNoUnsignedSubtraction == ModeNoUnsignedSubtraction
}

// HasHighNotPrecedenceMode detects if 'HIGH_NOT_PRECEDENCE' mode is set in SQLMode
func (m SQLMode) HasHighNotPrecedenceMode() bool {
	return m&ModeHighNotPrecedence == ModeHighNotPrecedence
}

// HasANSIQuotesMode detects if 'ANSI_QUOTES' mode is set in SQLMode
func (m SQLMode) HasANSIQuotesMode() bool {
	return m&ModeANSIQuotes == ModeANSIQuotes
}

// HasRealAsFloatMode detects if 'REAL_AS_FLOAT' mode is set in SQLMode
func (m SQLMode) HasRealAsFloatMode() bool {
	return m&ModeRealAsFloat == ModeRealAsFloat
}

// HasPadCharToFullLengthMode detects if 'PAD_CHAR_TO_FULL_LENGTH' mode is set in SQLMode
func (m SQLMode) HasPadCharToFullLengthMode() bool {
	return m&ModePadCharToFullLength == ModePadCharToFullLength
}

// HasNoBackslashEscapesMode detects if 'NO_BACKSLASH_ESCAPES' mode is set in SQLMode
func (m SQLMode) HasNoBackslashEscapesMode() bool {
	return m&ModeNoBackslashEscapes == ModeNoBackslashEscapes
}

// HasIgnoreSpaceMode detects if 'IGNORE_SPACE' mode is set in SQLMode
func (m SQLMode) HasIgnoreSpaceMode() bool {
	return m&ModeIgnoreSpace == ModeIgnoreSpace
}

// HasNoAutoCreateUserMode detects if 'NO_AUTO_CREATE_USER' mode is set in SQLMode
func (m SQLMode) HasNoAutoCreateUserMode() bool {
	return m&ModeNoAutoCreateUser == ModeNoAutoCreateUser
}

// HasAllowInvalidDatesMode detects if 'ALLOW_INVALID_DATES' mode is set in SQLMode
func (m SQLMode) HasAllowInvalidDatesMode() bool {
	return m&ModeAllowInvalidDates == ModeAllowInvalidDates
}

// consts for sql modes.
// see https://dev.mysql.com/doc/internals/en/query-event.html#q-sql-mode-code
const (
	ModeRealAsFloat SQLMode = 1 << iota
	ModePipesAsConcat
	ModeANSIQuotes
	ModeIgnoreSpace
	ModeNotUsed
	ModeOnlyFullGroupBy
	ModeNoUnsignedSubtraction
	ModeNoDirInCreate
	ModePostgreSQL
	ModeOracle
	ModeMsSQL
	ModeDb2
	ModeMaxdb
	ModeNoKeyOptions
	ModeNoTableOptions
	ModeNoFieldOptions
	ModeMySQL323
	ModeMySQL40
	ModeANSI
	ModeNoAutoValueOnZero
	ModeNoBackslashEscapes
	ModeStrictTransTables
	ModeStrictAllTables
	ModeNoZeroInDate
	ModeNoZeroDate
	ModeInvalidDates
	ModeErrorForDivisionByZero
	ModeTraditional
	ModeNoAutoCreateUser
	ModeHighNotPrecedence
	ModeNoEngineSubstitution
	ModePadCharToFullLength
	ModeAllowInvalidDates
)

// GetSQLMode gets the sql mode for string literal. SQL_mode is a list of different modes separated by commas.
func GetSQLMode(s string) (SQLMode, error) {
	strs := strings.Split(s, ",")
	var sqlMode SQLMode
	for i, length := 0, len(strs); i < length; i++ {
		mode, ok := Str2SQLMode[strs[i]]
		if !ok && strs[i] != "" {
			return sqlMode, newInvalidModeErr(strs[i])
		}
		sqlMode |= mode
	}
	return sqlMode, nil
}

// Str2SQLMode is the string represent of sql_mode to sql_mode map.
var Str2SQLMode = map[string]SQLMode{
	"REAL_AS_FLOAT":              ModeRealAsFloat,
	"PIPES_AS_CONCAT":            ModePipesAsConcat,
	"ANSI_QUOTES":                ModeANSIQuotes,
	"IGNORE_SPACE":               ModeIgnoreSpace,
	"NOT_USED":                   ModeNotUsed,
	"ONLY_FULL_GROUP_BY":         ModeOnlyFullGroupBy,
	"NO_UNSIGNED_SUBTRACTION":    ModeNoUnsignedSubtraction,
	"NO_DIR_IN_CREATE":           ModeNoDirInCreate,
	"POSTGRESQL":                 ModePostgreSQL,
	"ORACLE":                     ModeOracle,
	"MSSQL":                      ModeMsSQL,
	"DB2":                        ModeDb2,
	"MAXDB":                      ModeMaxdb,
	"NO_KEY_OPTIONS":             ModeNoKeyOptions,
	"NO_TABLE_OPTIONS":           ModeNoTableOptions,
	"NO_FIELD_OPTIONS":           ModeNoFieldOptions,
	"MYSQL323":                   ModeMySQL323,
	"MYSQL40":                    ModeMySQL40,
	"ANSI":                       ModeANSI,
	"NO_AUTO_VALUE_ON_ZERO":      ModeNoAutoValueOnZero,
	"NO_BACKSLASH_ESCAPES":       ModeNoBackslashEscapes,
	"STRICT_TRANS_TABLES":        ModeStrictTransTables,
	"STRICT_ALL_TABLES":          ModeStrictAllTables,
	"NO_ZERO_IN_DATE":            ModeNoZeroInDate,
	"NO_ZERO_DATE":               ModeNoZeroDate,
	"INVALID_DATES":              ModeInvalidDates,
	"ERROR_FOR_DIVISION_BY_ZERO": ModeErrorForDivisionByZero,
	"TRADITIONAL":                ModeTraditional,
	"NO_AUTO_CREATE_USER":        ModeNoAutoCreateUser,
	"HIGH_NOT_PRECEDENCE":        ModeHighNotPrecedence,
	"NO_ENGINE_SUBSTITUTION":     ModeNoEngineSubstitution,
	"PAD_CHAR_TO_FULL_LENGTH":    ModePadCharToFullLength,
	"ALLOW_INVALID_DATES":        ModeAllowInvalidDates,
}

// PriorityEnum is defined for Priority const values.
type PriorityEnum int

// Priority const values.
// See https://dev.mysql.com/doc/refman/5.7/en/insert.html
const (
	NoPriority PriorityEnum = iota
	LowPriority
	HighPriority
	DelayedPriority
)

// Priority2Str is used to convert the statement priority to string.
var Priority2Str = map[PriorityEnum]string{
	NoPriority:      "NO_PRIORITY",
	LowPriority:     "LOW_PRIORITY",
	HighPriority:    "HIGH_PRIORITY",
	DelayedPriority: "DELAYED",
}

// Restore implements Node interface.
func (n *PriorityEnum) Restore(ctx *format.RestoreCtx) error {
	switch *n {
	case NoPriority:
		return nil
	case LowPriority:
		ctx.WriteKeyWord("LOW_PRIORITY")
	case HighPriority:
		ctx.WriteKeyWord("HIGH_PRIORITY")
	case DelayedPriority:
		ctx.WriteKeyWord("DELAYED")
	default:
		return fmt.Errorf("undefined PriorityEnum Type[%d]", *n)
	}
	return nil
}

const (
	// PrimaryKeyName defines primary key name.
	PrimaryKeyName = "PRIMARY"
	// DefaultDecimal defines the default decimal value when the value out of range.
	DefaultDecimal = "99999999999999999999999999999999999999999999999999999999999999999"
	// PartitionCountLimit is limit of the number of partitions in a table.
	// Reference linking https://dev.mysql.com/doc/refman/5.7/en/partitioning-limitations.html.
	PartitionCountLimit = 8192
)

const (
	// CompressionNone is no compression in use
	CompressionNone = iota
	// CompressionZlib is zlib/deflate
	CompressionZlib
	// CompressionZstd is Facebook's Zstandard
	CompressionZstd
)
