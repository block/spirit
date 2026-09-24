# sqlescape

This package started as a copy of the `sqlescape` package from
[github.com/pingcap/tidb](https://github.com/pingcap/tidb). It is now
maintained as a hard fork and is no longer synced with upstream.

Local extensions over the TiDB original:

- `EscapeIdentifier`: standalone identifier quoting, the single source of
  truth for the `%n` verb.
- `%r` verb: splices a `RawSQL` argument into the SQL verbatim, with no
  quoting and no format interpretation; any other type — including a plain
  `string` — is an error, so every raw splice is an explicit, greppable
  `RawSQL()` conversion. Use it to embed raw user-provided SQL (such as an
  ALTER clause) into a trusted format string. Never concatenate user SQL into
  the format string itself: `%n` / `%?` / `%%` sequences inside it (e.g.
  `COMMENT '100%new'`) would be misinterpreted, failing the escape or silently
  rewriting the statement.
