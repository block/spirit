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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"fmt"
)

// ParseError is an error emitted by the parser, carrying a MySQL error
// code. The package-level Err* variables declared across the parser, ast,
// types and charset packages are message templates that double as
// sentinels: errors created from them with GenByArgs or GenByFormat match
// their template via errors.Is.
type ParseError struct {
	class   string
	code    uint16
	message string      // format template on sentinels, final text on instances
	base    *ParseError // template this instance was created from; nil on sentinels
}

// NewStdErr creates a sentinel error for the given MySQL error code, using
// the standard message template from MySQLErrName. class only provides the
// "[class:code]" prefix of the rendered message (e.g. "parser", "ddl").
func NewStdErr(class string, code uint16) *ParseError {
	return &ParseError{class: class, code: code, message: MySQLErrName[code]}
}

// Error implements the error interface.
func (e *ParseError) Error() string {
	return fmt.Sprintf("[%s:%d]%s", e.class, e.code, e.message)
}

// GenByArgs creates an instance of the error with its message template
// filled in.
func (e *ParseError) GenByArgs(args ...any) error {
	msg := e.message
	if len(args) > 0 {
		msg = fmt.Sprintf(e.message, args...)
	}
	return &ParseError{class: e.class, code: e.code, message: msg, base: e.template()}
}

// GenByFormat is GenByArgs with the standard message template replaced.
func (e *ParseError) GenByFormat(format string, args ...any) error {
	return &ParseError{class: e.class, code: e.code, message: fmt.Sprintf(format, args...), base: e.template()}
}

// Is reports whether target is the same error template (or another
// instance of it), so errors.Is(err, ErrSentinel) matches independent of
// the formatted message.
func (e *ParseError) Is(target error) bool {
	t, ok := target.(*ParseError)
	if !ok {
		return false
	}
	return e.template() == t.template()
}

func (e *ParseError) template() *ParseError {
	if e.base != nil {
		return e.base
	}
	return e
}
