package migration

import (
	"fmt"
	"sync"

	"github.com/sirupsen/logrus"
)

type testLogger struct {
	sync.Mutex
	logrus.FieldLogger
	lastInfof  string
	lastWarnf  string
	lastDebugf string
}

func (l *testLogger) Infof(format string, args ...any) {
	l.Lock()
	defer l.Unlock()
	l.lastInfof = fmt.Sprintf(format, args...)
}

func (l *testLogger) Warnf(format string, args ...any) {
	l.Lock()
	defer l.Unlock()
	l.lastWarnf = fmt.Sprintf(format, args...)
}

func (l *testLogger) Debugf(format string, args ...any) {
	l.Lock()
	defer l.Unlock()
	l.lastDebugf = fmt.Sprintf(format, args...)
}

func newTestLogger() *testLogger {
	return &testLogger{
		FieldLogger: logrus.New(),
	}
}
