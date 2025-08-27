package repl

import (
	"fmt"
	"strings"
	"testing"

	"github.com/siddontang/loggers"
	"github.com/stretchr/testify/assert"
)

// mockLogger implements loggers.Advanced and records all log messages
type mockLogger struct {
	messages []string
}

func (m *mockLogger) Debug(args ...any) { m.messages = append(m.messages, fmt.Sprint(args...)) }
func (m *mockLogger) Info(args ...any)  { m.messages = append(m.messages, fmt.Sprint(args...)) }
func (m *mockLogger) Warn(args ...any)  { m.messages = append(m.messages, fmt.Sprint(args...)) }
func (m *mockLogger) Error(args ...any) { m.messages = append(m.messages, fmt.Sprint(args...)) }
func (m *mockLogger) Fatal(args ...any) { m.messages = append(m.messages, fmt.Sprint(args...)) }
func (m *mockLogger) Debugf(format string, args ...any) {
	m.messages = append(m.messages, fmt.Sprintf(format, args...))
}
func (m *mockLogger) Infof(format string, args ...any) {
	m.messages = append(m.messages, fmt.Sprintf(format, args...))
}
func (m *mockLogger) Warnf(format string, args ...any) {
	m.messages = append(m.messages, fmt.Sprintf(format, args...))
}
func (m *mockLogger) Errorf(format string, args ...any) {
	m.messages = append(m.messages, fmt.Sprintf(format, args...))
}
func (m *mockLogger) Fatalf(format string, args ...any) {
	m.messages = append(m.messages, fmt.Sprintf(format, args...))
}
func (m *mockLogger) Debugln(args ...any) {
	m.messages = append(m.messages, fmt.Sprintln(args...))
}
func (m *mockLogger) Infoln(args ...any) {
	m.messages = append(m.messages, fmt.Sprintln(args...))
}
func (m *mockLogger) Warnln(args ...any) {
	m.messages = append(m.messages, fmt.Sprintln(args...))
}
func (m *mockLogger) Errorln(args ...any) {
	m.messages = append(m.messages, fmt.Sprintln(args...))
}
func (m *mockLogger) Fatalln(args ...any) {
	m.messages = append(m.messages, fmt.Sprintln(args...))
}
func (m *mockLogger) Panic(args ...any) { m.messages = append(m.messages, fmt.Sprint(args...)) }
func (m *mockLogger) Panicf(format string, args ...any) {
	m.messages = append(m.messages, fmt.Sprintf(format, args...))
}
func (m *mockLogger) Panicln(args ...any) {
	m.messages = append(m.messages, fmt.Sprintln(args...))
}
func (m *mockLogger) Print(args ...any) { m.messages = append(m.messages, fmt.Sprint(args...)) }
func (m *mockLogger) Printf(format string, args ...any) {
	m.messages = append(m.messages, fmt.Sprintf(format, args...))
}
func (m *mockLogger) Println(args ...any) {
	m.messages = append(m.messages, fmt.Sprintln(args...))
}

func (m *mockLogger) clear() {
	m.messages = nil
}

func (m *mockLogger) contains(s string) bool {
	for _, msg := range m.messages {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

func TestLogWrapperComprehensive(t *testing.T) {
	mock := &mockLogger{}
	logger := NewLogWrapper(mock)

	// Test that logger implements the interface
	var _ loggers.Advanced = logger

	t.Run("basic logging methods", func(t *testing.T) {
		testCases := []struct {
			name     string
			logFunc  func(...any)
			message  string
			expected string
		}{
			{"Debug", logger.Debug, "debug message", "debug message"},
			{"Info", logger.Info, "info message", "info message"},
			{"Warn", logger.Warn, "warn message", "warn message"},
			{"Error", logger.Error, "error message", "error message"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				mock.clear()
				tc.logFunc(tc.message)
				assert.True(t, mock.contains(tc.expected))
			})
		}
	})

	t.Run("formatted logging methods", func(t *testing.T) {
		testCases := []struct {
			name     string
			logFunc  func(string, ...any)
			format   string
			args     []any
			expected string
		}{
			{"Debugf", logger.Debugf, "debug %s", []any{"formatted"}, "debug formatted"},
			{"Infof", logger.Infof, "info %s", []any{"formatted"}, "info formatted"},
			{"Warnf", logger.Warnf, "warn %s", []any{"formatted"}, "warn formatted"},
			{"Errorf", logger.Errorf, "error %s", []any{"formatted"}, "error formatted"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				mock.clear()
				tc.logFunc(tc.format, tc.args...)
				assert.True(t, mock.contains(tc.expected))
			})
		}
	})

	t.Run("line logging methods", func(t *testing.T) {
		testCases := []struct {
			name     string
			logFunc  func(...any)
			message  string
			expected string
		}{
			{"Debugln", logger.Debugln, "debug line", "debug line"},
			{"Infoln", logger.Infoln, "info line", "info line"},
			{"Warnln", logger.Warnln, "warn line", "warn line"},
			{"Errorln", logger.Errorln, "error line", "error line"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				mock.clear()
				tc.logFunc(tc.message)
				assert.True(t, mock.contains(tc.expected))
			})
		}
	})

	t.Run("print methods", func(t *testing.T) {
		testCases := []struct {
			name     string
			logFunc  any
			args     []any
			format   string
			expected string
		}{
			{"Print", logger.Print, []any{"print message"}, "", "print message"},
			{"Printf", logger.Printf, []any{"formatted"}, "print %s", "print formatted"},
			{"Println", logger.Println, []any{"print line"}, "", "print line"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				mock.clear()
				switch f := tc.logFunc.(type) {
				case func(...any):
					f(tc.args...)
				case func(string, ...any):
					f(tc.format, tc.args...)
				}
				assert.True(t, mock.contains(tc.expected))
			})
		}
	})

	t.Run("filtered messages", func(t *testing.T) {
		filteredFormats := []string{
			"rotate to %s",
			"received fake rotate event, next log name is %s",
			"rotate binlog to %s",
			"table structure changed, clear table cache: %s.%s\n",
		}

		for _, format := range filteredFormats {
			t.Run(format, func(t *testing.T) {
				mock.clear()
				logger.Infof(format, "test")
				assert.Empty(t, mock.messages, "Message should be filtered: "+format)
			})
		}

		// Test error message filtering
		mock.clear()
		logger.Errorf("canal start sync binlog err: %v", "Sync was closed")
		assert.Empty(t, mock.messages, "Sync closed error should be filtered")

		// Test that other error messages are not filtered
		mock.clear()
		logger.Errorf("some other error: %v", "error message")
		assert.NotEmpty(t, mock.messages, "Other error messages should not be filtered")
	})

	// Note: We don't actually test Fatal/Panic methods since they would terminate the program
}
