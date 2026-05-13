module github.com/block/spirit

go 1.26.1

require (
	github.com/alecthomas/kong v0.7.1
	github.com/go-ini/ini v1.67.0
	github.com/go-mysql-org/go-mysql v1.13.0
	github.com/go-sql-driver/mysql v1.9.3
	github.com/google/uuid v1.6.0
	github.com/pingcap/errors v0.11.5-0.20260310054046-9c8b3586e4b2
	github.com/pingcap/tidb/pkg/parser v0.0.0-20260504140133-511dba1dbe17
	github.com/stretchr/testify v1.11.1
	go.uber.org/goleak v1.3.0
	golang.org/x/sync v0.20.0
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/coreos/go-semver v0.3.1 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/goccy/go-json v0.10.6 // indirect
	github.com/klauspost/compress v1.18.6 // indirect
	github.com/pingcap/failpoint v0.0.0-20260406204437-bbc9d102c19e // indirect
	github.com/pingcap/log v1.1.1-0.20260227082333-572e590d08f1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.28.0 // indirect
	golang.org/x/text v0.36.0 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/pingcap/tidb/pkg/parser => github.com/block/tidb/pkg/parser v0.0.0-20260506200501-e528fd979fc8

// Pinned to a fork with the in-flight RenderJSONAsMySQLText flag for
// type-preserving JSON binlog decoding. Revert to upstream go-mysql once
// the new flag lands. See https://github.com/morgo/go-mysql/tree/mysql-text-json-rendering.
replace github.com/go-mysql-org/go-mysql => github.com/morgo/go-mysql v0.0.0-20260513151644-11038310b494
