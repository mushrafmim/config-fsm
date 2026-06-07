package chart

import (
	"strings"
	"testing"
)

const validYAML = `
id: greeting
version: "1"
initial: start
states:
  - name: start
    executor: emit
    transitions:
      - { on: success, to: end }
  - name: end
    terminal: true
`

func TestParse_Valid(t *testing.T) {
	c, err := Parse([]byte(validYAML))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if c.ID != "greeting" || c.Initial != "start" {
		t.Fatalf("unexpected chart header: %+v", c)
	}
	end, ok := c.State("end")
	if !ok || !end.Terminal {
		t.Fatalf("end state missing or not terminal")
	}
	start, ok := c.State("start")
	if !ok || start.Executor != "emit" || len(start.Transitions) != 1 {
		t.Fatalf("start state malformed: %+v", start)
	}
}

func TestParse_SignalsParsedAndValidated(t *testing.T) {
	const y = `
id: c
version: "1"
initial: wait
states:
  - name: wait
    executor: x
    signals:
      - approve
      - reject
    transitions:
      - { on: approve, to: done }
      - { on: reject,  to: done }
  - name: done
    terminal: true
`
	c, err := Parse([]byte(y))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	wait, _ := c.State("wait")
	if len(wait.Signals) != 2 || wait.Signals[0] != "approve" || wait.Signals[1] != "reject" {
		t.Fatalf("signals = %v, want [approve reject]", wait.Signals)
	}
}

func TestParse_Errors(t *testing.T) {
	cases := []struct {
		name, yaml, want string
	}{
		{
			name: "missing initial",
			yaml: `id: c
version: "1"
states:
  - name: a
    terminal: true`,
			want: "initial state required",
		},
		{
			name: "initial not defined",
			yaml: `id: c
version: "1"
initial: ghost
states:
  - name: a
    terminal: true`,
			want: `initial state "ghost" not defined`,
		},
		{
			name: "unknown transition target",
			yaml: `id: c
version: "1"
initial: a
states:
  - name: a
    executor: x
    transitions:
      - { on: ok, to: nowhere }`,
			want: "targets undefined",
		},
		{
			name: "non-terminal without executor",
			yaml: `id: c
version: "1"
initial: a
states:
  - name: a`,
			want: "must declare an executor",
		},
		{
			name: "terminal with transitions",
			yaml: `id: c
version: "1"
initial: a
states:
  - name: a
    terminal: true
    transitions:
      - { on: ok, to: a }`,
			want: "must not declare transitions",
		},
		{
			name: "duplicate state",
			yaml: `id: c
version: "1"
initial: a
states:
  - name: a
    terminal: true
  - name: a
    terminal: true`,
			want: "duplicate state",
		},
		{
			name: "signal without matching transition",
			yaml: `id: c
version: "1"
initial: a
states:
  - name: a
    executor: x
    signals:
      - approve
    transitions:
      - { on: ok, to: b }
  - name: b
    terminal: true`,
			want: `signal "approve" has no matching transition`,
		},
		{
			name: "duplicate signal",
			yaml: `id: c
version: "1"
initial: a
states:
  - name: a
    executor: x
    signals:
      - ok
      - ok
    transitions:
      - { on: ok, to: b }
  - name: b
    terminal: true`,
			want: `duplicate signal "ok"`,
		},
		{
			name: "terminal with signals",
			yaml: `id: c
version: "1"
initial: a
states:
  - name: a
    terminal: true
    signals:
      - whatever`,
			want: "must not declare signals",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.yaml))
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want substring %q", err.Error(), tc.want)
			}
		})
	}
}
