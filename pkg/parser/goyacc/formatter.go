// Copyright 2019 PingCAP, Inc.
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

package main

import "io"

// Formatter is an io.Writer extended by a fmt.Printf like function Format.
// It was previously imported from the parser's format package; goyacc keeps a
// local copy so it can live in its own module without depending on the parser.
type Formatter interface {
	io.Writer
	Format(format string, args ...any) (n int, errno error)
}
