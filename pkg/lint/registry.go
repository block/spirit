package lint

import (
	"fmt"
	"sort"
	"sync"
)

// linter represents a registered linter with metadata
type linter struct {
	impl     Linter
	category string
	enabled  bool
}

var (
	linters map[string]linter
	lock    sync.RWMutex
)

// Register registers a linter with the global registry.
// This should be called from init() functions in linter implementations.
// Linters are enabled by default when registered.
func Register(l Linter) {
	lock.Lock()
	defer lock.Unlock()

	if linters == nil {
		linters = make(map[string]linter)
	}

	linters[l.Name()] = linter{
		impl:     l,
		category: l.Category(),
		enabled:  true,
	}
}

// Enable enables specific linters by name.
// Returns an error if the linter is not found.
func Enable(names ...string) error {
	lock.Lock()
	defer lock.Unlock()

	for _, name := range names {
		l, ok := linters[name]
		if !ok {
			return fmt.Errorf("linter %q not found", name)
		}

		l.enabled = true
		linters[name] = l
	}

	return nil
}

// Disable disables specific linters by name.
// Returns an error if the linter is not found.
func Disable(names ...string) error {
	lock.Lock()
	defer lock.Unlock()

	for _, name := range names {
		l, ok := linters[name]
		if !ok {
			return fmt.Errorf("linter %q not found", name)
		}

		l.enabled = false
		linters[name] = l
	}

	return nil
}

// List returns the names of all registered linters in sorted order.
func List() []string {
	lock.RLock()
	defer lock.RUnlock()

	names := make([]string, 0, len(linters))
	for name := range linters {
		names = append(names, name)
	}

	sort.Strings(names)

	return names
}

// ListByCategory returns the names of all linters in a specific category in sorted order.
func ListByCategory(category string) []string {
	lock.RLock()
	defer lock.RUnlock()

	var names []string

	for name, linter := range linters {
		if linter.category == category {
			names = append(names, name)
		}
	}

	sort.Strings(names)

	return names
}

// Get returns a linter by name.
// Returns an error if the linter is not found.
func Get(name string) (Linter, error) {
	lock.RLock()
	defer lock.RUnlock()

	l, ok := linters[name]
	if !ok {
		return nil, fmt.Errorf("linter %q not found", name)
	}

	return l.impl, nil
}

// Reset clears all registered linters.
// This is primarily useful for testing.
func Reset() {
	lock.Lock()
	defer lock.Unlock()

	linters = make(map[string]linter)
}
