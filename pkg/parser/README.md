# Parser

A MySQL-compatible SQL parser, used by Spirit to parse DDL and understand
schemas.

This package is a hard fork of the
[TiDB parser](https://github.com/pingcap/tidb/tree/master/pkg/parser)
(`github.com/pingcap/tidb/pkg/parser`), taken from upstream master as of
May 2026 (via `block/tidb@e528fd979fc8`, which added spatial type/index
support). We are grateful to PingCAP and the TiDB contributors for
building and maintaining an excellent MySQL-compatible parser for over a
decade — and for accepting many of our compatibility fixes upstream over
the years. The fork retains their Apache-2.0 license and copyright
headers.

## Why fork?

Spirit only needs to parse the MySQL dialect, and only cares about the
syntax a schema-change tool encounters. TiDB's parser necessarily carries
TiDB-specific extensions (TiDB system functions, hints, `ADMIN`/`BRIE`
statements, syntax bound to TiKV/TiFlash concepts) and some MariaDB
syntax. Forking lets us:

- strip everything that is not MySQL, so what remains is auditable
  against the MySQL manual;
- add the functionality we need quickly, without waiting on an upstream
  release cadence;
- keep Spirit's dependency graph small (the parser no longer drags in
  `pingcap/*` modules, `zap`, or their transitive dependencies).

## What changed relative to upstream

- **MySQL-only surface.** TiDB-specific statements, keywords, system
  functions, optimizer hints, and MariaDB syntax (e.g. `SYSTEM_TIME`
  partitioning, `ILIKE`, `FLUSH CLIENT_ERRORS_SUMMARY`) are removed.
  The reserved-word set now matches MySQL 8.0 (see
  [Testing](#testing-against-real-mysql)).
- **Smaller charset catalog.** Only the encodings the lexer actually
  transforms remain (utf8/utf8mb4, ascii, latin1, binary); other charset
  names are still recognized in DDL but are not transcoded. Custom
  charset registration was removed.
- **Modern Go, stdlib errors.** The `terror`/`pingcap/errors` machinery
  was replaced with plain wrapped errors (`errors.Is`/`As` work as
  expected); `zap` logging was removed; the code passes this repo's
  `golangci-lint` configuration.
- **Dead weight removed.** The legacy `Format(io.Writer)` pretty-printer,
  the `driver` indirection, keyword-listing generators, and APIs unused
  by Spirit are deleted. `deadcode -test ./...` reports only
  interface-conformance markers.
- **Upstream fixes ported.** MySQL-compatibility fixes that landed in
  TiDB after the fork base are ported when relevant (e.g. the parser
  depth DoS guard, `INSERT ... AS row_alias`, dual-password syntax,
  `SET_VAR` decimal hints, and `GROUP_CONCAT` separator charset
  handling), as is the opt-in
  `format.RestoreSkipRedundantParentheses` restore flag, which lets
  expression `Restore` drop parentheses that MySQL's precedence and
  associativity rules make unnecessary (`a + (b * c)` restores as
  `a + b * c`; `a - (b - c)` keeps its parentheses). Spirit uses it to
  canonicalize CHECK-constraint and generated-column expressions.
- **Fixes beyond upstream.** Parenthesized default values keep their
  parentheses through a parse/restore round trip (`DEFAULT ('{}')` is an
  ast.ParenthesesExpr): MySQL 8.0.13+ treats `DEFAULT ('{}')` and
  `DEFAULT '{}'` as different DDL, and BLOB/TEXT/JSON/GEOMETRY columns
  only accept the parenthesized form. The upstream parser still restores
  both to the bare form (pingcap/tidb#57768). Keyword-named functions
  also parse in DEFAULT expressions (`DEFAULT (point(0,0))` — the MySQL
  manual's own expression-default example; upstream special-cases only
  REPLACE), and the spatial constructors (`linestring()`, `polygon()`,
  `multipoint()`, ...) parse as function calls in every expression
  context — as keyword tokens they previously only worked as column
  types (block/spirit#1128).

The AST (`ast` package), `format` restore machinery, `charset`, `mysql`
constants, `opcode`, and `types` packages keep their upstream shapes, so
code written against the TiDB parser API generally ports with an import
change.

## Usage

```go
import (
    "github.com/block/spirit/pkg/parser"
    "github.com/block/spirit/pkg/parser/ast"
)

p := parser.New()
stmts, warns, err := p.ParseSQL("ALTER TABLE t1 ADD COLUMN b INT")
_ = warns
if err != nil {
    // handle parse error
}
alter := stmts[0].(*ast.AlterTableStmt)
```

## Regenerating the parser

`parser.go` and `hintparser.go` are generated from `parser.y` and
`hintparser.y` by [goyacc](https://gitlab.com/cznic/goyacc). After
editing a `.y` file:

```bash
cd pkg/parser
make parser
```

The build fails if the grammar introduces shift/reduce or reduce/reduce
conflicts. Generated files are checked in; the `parser-regen` CI job
deletes them, regenerates from the grammar, and fails on any diff, so a
`.y` edit cannot ship without its regenerated output.

## Testing against real MySQL

Most tests run offline. One additional suite compares the grammar's
reserved-word set against a live MySQL server:

```bash
MYSQL_DSN="user:pass@tcp(127.0.0.1:3306)/" \
  go test -tags reserved_words_test -run TestCompareReservedWordsWithMySQL ./pkg/parser
```

## License

Apache License 2.0, same as the upstream TiDB parser. See the LICENSE
file at the repository root and the per-file copyright headers.
