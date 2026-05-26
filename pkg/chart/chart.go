// Package chart defines the static configuration types for a finite state
// machine and a YAML loader.
//
// A Chart is the compiled, runnable representation of a machine. It is
// immutable once loaded. Higher-level business-DSL types (with macros) live
// in the compiler package and produce Chart values.
package chart

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Transition declares that a state moves to To when an executor emits the
// event named On. Guard is reserved for the CEL guard layer (Tier 5) and is
// ignored by the current engine.
type Transition struct {
	On    string `yaml:"on"`
	To    string `yaml:"to"`
	Guard string `yaml:"guard,omitempty"`
}

// StateConfig is one node of the FSM.
type StateConfig struct {
	Name        string         `yaml:"name"`
	Executor    string         `yaml:"executor,omitempty"`
	Config      map[string]any `yaml:"config,omitempty"`
	Transitions []Transition   `yaml:"transitions,omitempty"`
	Terminal    bool           `yaml:"terminal,omitempty"`
}

// Chart is a compiled machine. Charts are immutable after Validate has
// completed; the engine reads only.
type Chart struct {
	ID      string        `yaml:"id"`
	Version string        `yaml:"version"`
	Initial string        `yaml:"initial"`
	States  []StateConfig `yaml:"states"`

	index map[string]*StateConfig
}

// State returns a pointer to the named state, or false if it does not exist.
func (c *Chart) State(name string) (*StateConfig, bool) {
	s, ok := c.index[name]
	return s, ok
}

// Validate enforces structural invariants and builds the name → state index.
func (c *Chart) Validate() error {
	if c.ID == "" {
		return fmt.Errorf("chart id required")
	}
	if c.Initial == "" {
		return fmt.Errorf("chart %q: initial state required", c.ID)
	}
	c.index = make(map[string]*StateConfig, len(c.States))
	for i := range c.States {
		s := &c.States[i]
		if s.Name == "" {
			return fmt.Errorf("chart %q: state at index %d has no name", c.ID, i)
		}
		if _, dup := c.index[s.Name]; dup {
			return fmt.Errorf("chart %q: duplicate state %q", c.ID, s.Name)
		}
		c.index[s.Name] = s
	}
	if _, ok := c.index[c.Initial]; !ok {
		return fmt.Errorf("chart %q: initial state %q not defined", c.ID, c.Initial)
	}
	for _, s := range c.States {
		if s.Terminal {
			if len(s.Transitions) > 0 {
				return fmt.Errorf("chart %q: terminal state %q must not declare transitions", c.ID, s.Name)
			}
			continue
		}
		if s.Executor == "" {
			return fmt.Errorf("chart %q: non-terminal state %q must declare an executor", c.ID, s.Name)
		}
		for _, t := range s.Transitions {
			if t.On == "" {
				return fmt.Errorf("chart %q: state %q has transition with empty 'on'", c.ID, s.Name)
			}
			if _, ok := c.index[t.To]; !ok {
				return fmt.Errorf("chart %q: state %q transition on %q targets undefined %q", c.ID, s.Name, t.On, t.To)
			}
		}
	}
	return nil
}

// Load reads and parses a chart from a YAML file.
func Load(path string) (*Chart, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read chart %q: %w", path, err)
	}
	return Parse(data)
}

// Parse decodes a chart from YAML bytes and validates it.
func Parse(data []byte) (*Chart, error) {
	var c Chart
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("parse chart: %w", err)
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}
