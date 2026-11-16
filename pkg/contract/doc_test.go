package contract

import (
	"go/build"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// TestImportsOnlyStandardLibrary enforces the package's import rule: contract
// must be importable without depending on the rest of spirit or on any other
// module, because it is meant to move to its own module. Test files may import
// test helpers; only the package's own files are checked.
func TestImportsOnlyStandardLibrary(t *testing.T) {
	pkg, err := build.Default.ImportDir(".", 0)
	require.NoError(t, err)
	require.NotEmpty(t, pkg.GoFiles)
	for _, path := range pkg.Imports {
		imported, err := build.Default.Import(path, ".", build.FindOnly)
		require.NoError(t, err, "resolve import %q", path)
		require.True(t, imported.Goroot, "pkg/contract imports %q, which is not in the standard library", path)
	}
}
