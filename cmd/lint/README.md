The `lint` command is a simplistic, **experimental** interface to spirit's `lint` package.

It is not intended that this command be incorporated into your workflow at this time. The interface and 
output of this command is likely to change drastically, and it's possible it will be removed entirely.

You can provide the statements via the `--statements` option in one of three ways:
1. In plaintext on the command line (`--statements="CREATE TABLE ..." --statements="ALTER TABLE ..."`)
2. Via a file or directory or glob pattern containing the statements (`--statements=file:/path/to/file.sql`)
3. Via standard input (`--statements=-`)

You can combine these however you like.

All statements provided are considered as a single group.

Because of implementation details of the `lint` package and in order to reduce the complexity of this CLI,
the `CREATE TABLE` and `ALTER TABLE` statements have to be provided in a pretty specific way:

* `CREATE TABLE` statements must be provided one-by-one. One per `--statements` argument, one per file, etc.
* `ALTER TABLE` statements can be provided one-by-one, or multiple `ALTER TABLE` statements can be provided in a single
`--statements` argument or file, separated by semicolons.