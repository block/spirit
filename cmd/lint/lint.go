package main

import (
	"github.com/alecthomas/kong"
	"github.com/block/spirit/pkg/lint"
)

var cli struct {
	lint.Lint `cmd:"" help:"Lint CREATE TABLE and ALTER TABLE statements."`
}

func main() {
	ctx := kong.Parse(&cli)
	ctx.FatalIfErrorf(ctx.Run())
}
