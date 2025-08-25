package main

import (
	"github.com/alecthomas/kong"
	"github.com/block/spirit/pkg/migration"
)

var cli struct {
	migration.Migration `cmd:"" help:"Schema change tables"`
}

func main() {
	ctx := kong.Parse(&cli)
	ctx.FatalIfErrorf(ctx.Run())
}
