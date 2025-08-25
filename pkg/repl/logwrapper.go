package repl

import (
	"fmt"

	"github.com/siddontang/loggers"
)

// If we pass our loggers.Advanced directly,
// it will write a bunch of spam to the log. So we use this hack
// to filter out the noisiest messages.
func NewLogWrapper(logger loggers.Advanced) *LogWrapper {
	return &LogWrapper{
		logger: logger,
	}
}

var _ loggers.Advanced = &LogWrapper{}

type LogWrapper struct {
	logger loggers.Advanced
}

func (c *LogWrapper) Debugf(format string, args ...any) {
	c.logger.Debugf(format, args...)
}

func (c *LogWrapper) Infof(format string, args ...any) {
	switch format {
	case "rotate to %s", "received fake rotate event, next log name is %s", "rotate binlog to %s", "table structure changed, clear table cache: %s.%s\n":
		return
	}
	c.logger.Infof(format, args...)
}

func (c *LogWrapper) Warnf(format string, args ...any) {
	c.logger.Warnf(format, args...)
}

func (c *LogWrapper) Errorf(format string, args ...any) {
	// Noisy bug on close, can be ignored.
	// https://github.com/block/spirit/pull/65
	if len(args) == 1 {
		message := fmt.Sprintf("%s", args[0])
		if format == "canal start sync binlog err: %v" && message == "Sync was closed" {
			return
		}
	}
	c.logger.Errorf(format, args...)
}

func (c *LogWrapper) Fatalf(format string, args ...any) {
	c.logger.Fatalf(format, args...)
}

func (c *LogWrapper) Debug(args ...any) {
	c.logger.Debug(args...)
}

func (c *LogWrapper) Info(args ...any) {
	c.logger.Info(args...)
}

func (c *LogWrapper) Warn(args ...any) {
	c.logger.Warn(args...)
}

func (c *LogWrapper) Error(args ...any) {
	c.logger.Error(args...)
}

func (c *LogWrapper) Fatal(args ...any) {
	c.logger.Fatal(args...)
}

func (c *LogWrapper) Debugln(args ...any) {
	c.logger.Debugln(args...)
}

func (c *LogWrapper) Infoln(args ...any) {
	c.logger.Infoln(args...)
}

func (c *LogWrapper) Warnln(args ...any) {
	c.logger.Warnln(args...)
}

func (c *LogWrapper) Errorln(args ...any) {
	c.logger.Errorln(args...)
}

func (c *LogWrapper) Fatalln(args ...any) {
	c.logger.Fatalln(args...)
}

func (c *LogWrapper) Panic(args ...any) {
	c.logger.Panic(args...)
}

func (c *LogWrapper) Panicf(format string, args ...any) {
	c.logger.Panicf(format, args...)
}

func (c *LogWrapper) Panicln(args ...any) {
	c.logger.Panicln(args...)
}

func (c *LogWrapper) Print(args ...any) {
	c.logger.Print(args...)
}

func (c *LogWrapper) Printf(format string, args ...any) {
	c.logger.Printf(format, args...)
}

func (c *LogWrapper) Println(args ...any) {
	c.logger.Println(args...)
}
