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

package ast

import (
	"encoding/json"
	"strings"
	"unsafe"
)

// TableLockType is the type of the table lock.
type TableLockType byte

const (
	// TableLockNone means this table lock is absent.
	TableLockNone TableLockType = iota
	// TableLockRead means the session with this lock can read the table (but not write it).
	// Multiple sessions can acquire a READ lock for the table at the same time.
	// Other sessions can read the table without explicitly acquiring a READ lock.
	TableLockRead
	// TableLockReadLocal is not supported.
	TableLockReadLocal
	// TableLockReadOnly is used to set a table into read-only status,
	// when the session exits, it will not release its lock automatically.
	TableLockReadOnly
	// TableLockWrite means only the session with this lock has write/read permission.
	// Only the session that holds the lock can access the table. No other session can access it until the lock is released.
	TableLockWrite
	// TableLockWriteLocal means the session with this lock has write/read permission, and the other session still has read permission.
	TableLockWriteLocal
	// TableLockLowPriorityWrite is the deprecated LOW_PRIORITY WRITE type;
	// MySQL still parses it but treats it as WRITE.
	TableLockLowPriorityWrite
)

// String implements fmt.Stringer interface.
func (t TableLockType) String() string {
	switch t {
	case TableLockNone:
		return "NONE"
	case TableLockRead:
		return "READ"
	case TableLockReadLocal:
		return "READ LOCAL"
	case TableLockReadOnly:
		return "READ ONLY"
	case TableLockWriteLocal:
		return "WRITE LOCAL"
	case TableLockWrite:
		return "WRITE"
	case TableLockLowPriorityWrite:
		return "LOW_PRIORITY WRITE"
	}
	return ""
}

// ViewAlgorithm is VIEW's SQL ALGORITHM characteristic.
// See https://dev.mysql.com/doc/refman/5.7/en/view-algorithms.html
type ViewAlgorithm int

// ViewAlgorithm values.
const (
	AlgorithmUndefined ViewAlgorithm = iota
	AlgorithmMerge
	AlgorithmTemptable
)

// String implements fmt.Stringer interface.
func (v *ViewAlgorithm) String() string {
	switch *v {
	case AlgorithmMerge:
		return "MERGE"
	case AlgorithmTemptable:
		return "TEMPTABLE"
	case AlgorithmUndefined:
		return "UNDEFINED"
	default:
		return "UNDEFINED"
	}
}

// ViewSecurity is VIEW's SQL SECURITY characteristic.
// See https://dev.mysql.com/doc/refman/5.7/en/create-view.html
type ViewSecurity int

// ViewSecurity values.
const (
	SecurityDefiner ViewSecurity = iota
	SecurityInvoker
)

// String implements fmt.Stringer interface.
func (v *ViewSecurity) String() string {
	switch *v {
	case SecurityInvoker:
		return "INVOKER"
	case SecurityDefiner:
		return "DEFINER"
	default:
		return "DEFINER"
	}
}

// ViewCheckOption is VIEW's WITH CHECK OPTION clause part.
// See https://dev.mysql.com/doc/refman/5.7/en/view-check-option.html
type ViewCheckOption int

// ViewCheckOption values.
const (
	CheckOptionLocal ViewCheckOption = iota
	CheckOptionCascaded
)

// String implements fmt.Stringer interface.
func (v *ViewCheckOption) String() string {
	switch *v {
	case CheckOptionLocal:
		return "LOCAL"
	case CheckOptionCascaded:
		return "CASCADED"
	default:
		return "CASCADED"
	}
}

// PartitionType is the type for PartitionInfo
type PartitionType int

// PartitionType types.
const (
	// Actually non-partitioned, but during DDL keeping the table as
	// a single partition
	PartitionTypeNone PartitionType = 0

	PartitionTypeRange PartitionType = 1
	PartitionTypeHash  PartitionType = 2
	PartitionTypeList  PartitionType = 3
	PartitionTypeKey   PartitionType = 4
)

// String implements fmt.Stringer interface.
func (p PartitionType) String() string {
	switch p {
	case PartitionTypeRange:
		return "RANGE"
	case PartitionTypeHash:
		return "HASH"
	case PartitionTypeList:
		return "LIST"
	case PartitionTypeKey:
		return "KEY"
	case PartitionTypeNone:
		return "NONE"
	default:
		return ""
	}
}

// IndexType is the type of index
type IndexType int

// String implements Stringer interface.
func (t IndexType) String() string {
	switch t { //nolint:exhaustive
	case IndexTypeBtree:
		return "BTREE"
	case IndexTypeHash:
		return "HASH"
	case IndexTypeRtree:
		return "RTREE"
	case IndexTypeFulltext:
		return "FULLTEXT"
	default:
		return ""
	}
}

// IndexTypes
const (
	IndexTypeInvalid IndexType = iota
	IndexTypeBtree
	IndexTypeHash
	IndexTypeRtree
	IndexTypeFulltext
)

// ReferOptionType is the type for refer options.
type ReferOptionType int

// Refer option types.
const (
	ReferOptionNoOption ReferOptionType = iota
	ReferOptionRestrict
	ReferOptionCascade
	ReferOptionSetNull
	ReferOptionNoAction
	ReferOptionSetDefault
)

// String implements fmt.Stringer interface.
func (r ReferOptionType) String() string {
	switch r { //nolint:exhaustive
	case ReferOptionRestrict:
		return "RESTRICT"
	case ReferOptionCascade:
		return "CASCADE"
	case ReferOptionSetNull:
		return "SET NULL"
	case ReferOptionNoAction:
		return "NO ACTION"
	case ReferOptionSetDefault:
		return "SET DEFAULT"
	}
	return ""
}

// CIStr is case insensitive string.
type CIStr struct {
	O string `json:"O"` // Original string.
	L string `json:"L"` // Lower case string.
}

// String implements fmt.Stringer interface.
func (cis CIStr) String() string {
	return cis.O
}

// NewCIStr creates a new CIStr.
func NewCIStr(s string) (cs CIStr) {
	cs.O = s
	cs.L = strings.ToLower(s)
	return
}

// UnmarshalJSON implements the user defined unmarshal method.
// CIStr can also be unmarshaled from a plain JSON string for backward
// compatibility with older serialized forms.
func (cis *CIStr) UnmarshalJSON(b []byte) error {
	type T CIStr
	if err := json.Unmarshal(b, (*T)(cis)); err == nil {
		return nil
	}

	// Unmarshal CIStr from a single string.
	err := json.Unmarshal(b, &cis.O)
	if err != nil {
		return err
	}
	cis.L = strings.ToLower(cis.O)
	return nil
}

// MemoryUsage return the memory usage of CIStr
func (cis *CIStr) MemoryUsage() (sum int64) {
	if cis == nil {
		return
	}

	return int64(unsafe.Sizeof(cis.O))*2 + int64(len(cis.O)+len(cis.L))
}

// Priority values.
const (
	LowPriorityValue    = 1
	MediumPriorityValue = 8
	HighPriorityValue   = 16
)
