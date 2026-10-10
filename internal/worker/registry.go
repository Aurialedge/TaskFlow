package worker

import (
	"fmt"
	"strings"
	"sync"
)

// Registry manages the mapping of job type names to their executable HandlerFunc.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]HandlerFunc
}

// NewRegistry constructs an empty Registry.
func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[string]HandlerFunc),
	}
}

// NewDefaultRegistry constructs a Registry pre-populated with standard built-in handlers.
func NewDefaultRegistry() *Registry {
	r := NewRegistry()
	_ = r.Register("echo", EchoHandler)
	_ = r.Register("sleep", SleepHandler)
	_ = r.Register("report.generate", ReportGenerateHandler)
	_ = r.Register("test.fail", FailHandler)
	return r
}

// Register adds a handler for a given job type. Returns an error if already registered.
func (r *Registry) Register(jobType string, fn HandlerFunc) error {
	trimmed := strings.TrimSpace(jobType)
	if trimmed == "" {
		return fmt.Errorf("cannot register handler with empty job type")
	}
	if fn == nil {
		return fmt.Errorf("cannot register nil handler function for type %s", trimmed)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.handlers[trimmed]; exists {
		return fmt.Errorf("handler for job type '%s' is already registered", trimmed)
	}

	r.handlers[trimmed] = fn
	return nil
}

// Get retrieves a handler by job type name.
func (r *Registry) Get(jobType string) (HandlerFunc, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	fn, ok := r.handlers[strings.TrimSpace(jobType)]
	return fn, ok
}

// RegisteredTypes returns a slice of all registered job types.
func (r *Registry) RegisteredTypes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	types := make([]string, 0, len(r.handlers))
	for k := range r.handlers {
		types = append(types, k)
	}
	return types
}
