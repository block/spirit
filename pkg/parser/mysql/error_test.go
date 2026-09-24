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

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseError(t *testing.T) {
	base := NewStdErr("parser", ErrNoDB)
	require.Equal(t, "[parser:1046]No database selected", base.Error())

	inst := base.GenByArgs()
	require.Equal(t, "[parser:1046]No database selected", inst.Error())
	require.ErrorIs(t, inst, base)

	withArgs := NewStdErr("parser", ErrUnknownCharacterSet).GenByArgs("utf9")
	require.Equal(t, "[parser:1115]Unknown character set: 'utf9'", withArgs.Error())

	custom := base.GenByFormat("something %s happened", "odd")
	require.Equal(t, "[parser:1046]something odd happened", custom.Error())
	require.ErrorIs(t, custom, base)

	other := NewStdErr("parser", ErrParse)
	require.NotErrorIs(t, inst, other)
}
