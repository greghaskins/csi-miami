// Package fake is a systemd.Manager for other packages' tests. It records
// every Start call, answers State from a table the test sets, and fails Start
// on request. It runs nothing: a started unit does not appear in the table
// unless the test puts it there.
package fake

import (
	"context"
	"sync"

	"github.com/greghaskins/csi-miami/internal/systemd"
)

// StartCall is one recorded Start: the unit and the properties the real
// Manager would have set, as a map.
type StartCall struct {
	Unit       systemd.Unit
	Properties map[string]any
}

// Manager is the fake. The zero value is ready to use.
type Manager struct {
	mu       sync.Mutex
	states   map[string]systemd.State
	starts   []StartCall
	stops    []string
	startErr error
}

var _ systemd.Manager = (*Manager)(nil)

// SetState sets what State reports for a unit.
func (m *Manager) SetState(name string, s systemd.State) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.states == nil {
		m.states = map[string]systemd.State{}
	}
	m.states[name] = s
}

// FailStart makes every later Start return err; nil restores success.
func (m *Manager) FailStart(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.startErr = err
}

// Starts returns the recorded Start calls, oldest first, including failed ones.
func (m *Manager) Starts() []StartCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]StartCall(nil), m.starts...)
}

// Stops returns the names Stop was called with, oldest first.
func (m *Manager) Stops() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.stops...)
}

// Start records the call and returns the configured error.
func (m *Manager) Start(_ context.Context, u systemd.Unit) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.starts = append(m.starts, StartCall{Unit: u, Properties: properties(u)})
	return m.startErr
}

// Stop records the call and forgets the unit's state.
func (m *Manager) Stop(_ context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stops = append(m.stops, name)
	delete(m.states, name)
	return nil
}

// State answers from the table; a unit never set is absent.
func (m *Manager) State(_ context.Context, name string) (systemd.State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.states[name], nil
}

// properties mirrors what the real Manager sets on a transient unit.
func properties(u systemd.Unit) map[string]any {
	return map[string]any{
		"Type":           "simple",
		"ExecStart":      append([]string(nil), u.ExecStart...),
		"Environment":    append([]string(nil), u.Environment...),
		"Description":    u.Description,
		"CollectMode":    "inactive-or-failed",
		"KillMode":       "control-group",
		"StandardOutput": "journal",
		"StandardError":  "journal",
	}
}
