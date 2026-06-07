package testfixtures

// Canonical chart YAMLs reused by tests. Keep these small and focused on a
// single shape each — tests that need bespoke topologies should declare them
// inline rather than bloating these constants.

// LinearThreeStates is the Tier 1 milestone shape: start → middle → end.
// Both non-terminal states use an executor named "emit".
const LinearThreeStates = `
id: linear
version: "1"
initial: start
states:
  - name: start
    executor: emit
    transitions:
      - { on: success, to: middle }
  - name: middle
    executor: emit
    transitions:
      - { on: success, to: end }
  - name: end
    terminal: true
`

// SuspendThenTerminate parks at the first state until "signal" arrives.
// Used to exercise suspend/resume once Tier 2 lands.
const SuspendThenTerminate = `
id: park
version: "1"
initial: wait
states:
  - name: wait
    executor: park
    signals:
      - signal
    transitions:
      - { on: signal, to: done }
  - name: done
    terminal: true
`

// BranchOnEvent has two outgoing transitions from the same state; the
// executor decides which event to emit. Useful for transition resolution
// tests.
const BranchOnEvent = `
id: branch
version: "1"
initial: choose
states:
  - name: choose
    executor: chooser
    transitions:
      - { on: approve, to: approved }
      - { on: reject,  to: rejected }
  - name: approved
    terminal: true
  - name: rejected
    terminal: true
`
