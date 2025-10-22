package lint

import (
	"fmt"
	"sort"
	"sync"
)

// linter represents a registered linter with metadata
type linter struct {
	l       Linter
	enabled bool
}

var (
	linters map[string]*linter
	lock    sync.RWMutex
)

// Register registers a linter with the global registry.
// This should be called from init() functions in linter implementations.
// Linters are enabled by default when registered.
func Register(l Linter) {
	lock.Lock()
	defer lock.Unlock()

	if linters == nil {
		linters = make(map[string]*linter)
	}

	linters[l.Name()] = &linter{
		l:       l,
		enabled: true,
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
	}

	return nil
}

// Enabled returns whether a specific linter is enabled.
// Returns an error if the linter is not found.
func Enabled(name string) bool {
	lock.RLock()
	defer lock.RUnlock()

	// if the linter is not found, just return false
	l, ok := linters[name]
	if !ok {
		return false
	}

	return l.enabled
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

// Get returns a linter by name.
// Returns an error if the linter is not found.
func Get(name string) (Linter, error) {
	lock.RLock()
	defer lock.RUnlock()

	l, ok := linters[name]
	if !ok {
		return nil, fmt.Errorf("linter %q not found", name)
	}

	return l.l, nil
}

// Reset clears all registered linters.
// This is primarily useful for testing.
func Reset() {
	lock.Lock()
	defer lock.Unlock()

	linters = make(map[string]*linter)
}
