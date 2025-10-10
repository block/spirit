package main

import (
	"github.com/alecthomas/kong"
	"github.com/block/spirit/pkg/move"
)

var cli struct {
	move.Move `cmd:"" help:"Move tables between MySQL servers."`
}

func main() {
	ctx := kong.Parse(&cli)
	ctx.FatalIfErrorf(ctx.Run())
}
