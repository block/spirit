package lint

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/block/spirit/pkg/statement"
)

func init() {
	Register(&AllowEngine{
		allowedEngines: map[string]struct{}{
			"innodb": {},
		},
	})
}

type AllowEngine struct {
	allowedEngines map[string]struct{}
}

func (l *AllowEngine) Name() string {
	return "allow_engine"
}
func (l *AllowEngine) Description() string {
	return "Restricts which storage engines are allowed"
}

func (l *AllowEngine) Configure(config map[string]string) error {
	// overwrite existing config if any
	l.allowedEngines = make(map[string]struct{})
	for k, v := range config {
		switch k {
		case "allowed_engines":
			for _, engine := range strings.Split(v, ",") {
				engine = strings.TrimSpace(engine)
				if engine != "" {
					l.allowedEngines[strings.ToLower(engine)] = struct{}{}
				}
			}
		default:
			return fmt.Errorf("unknown configuration option %q for linter %q", k, l.Name())
		}
	}
	return nil
}

func (l *AllowEngine) DefaultConfig() map[string]string {
	return map[string]string{
		"allowed_engines": "innodb",
	}
}

func (l *AllowEngine) String() string {
	return Stringer(l)
}

func (l *AllowEngine) Lint(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) (violations []Violation) {
	if l.allowedEngines == nil {
		err := l.Configure(l.DefaultConfig())
		if err != nil {
			panic(err)
		}
	}
	for ct := range CreateTableStatements(existingTables, changes) {
		// Skip tables without explicit engine specification
		if ct.TableOptions == nil || ct.TableOptions.Engine == nil {
			continue
		}

		engineName := *ct.TableOptions.Engine
		if _, ok := l.allowedEngines[strings.ToLower(engineName)]; !ok {
			violations = append(violations, Violation{
				Linter:     l,
				Location:   &Location{Table: ct.TableName},
				Message:    fmt.Sprintf("Table %q uses an unsupported engine %q", ct.TableName, engineName),
				Suggestion: strPtr("Use a supported storage engine: " + strings.Join(slices.Sorted(maps.Keys(l.allowedEngines)), ", ")),
			})
		}
	}
	return violations
}
