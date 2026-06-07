// Package builtin provides ready-to-use, generic Executor implementations so
// most charts can be authored without writing custom Go.
//
// Each executor is configured entirely through the state's `config:` block in
// the chart — there is no per-value condition evaluation (no CEL, no field
// mapping). Branching is expressed with the engine's "lift the outcome to an
// event" pattern: an executor either suspends (the chart's `signals:` decide
// what wakes it) or emits an event name that selects an outgoing transition.
//
// The three executors map onto the recurring flow shapes:
//
//   - InteractiveTask  — a human/external step: park, and on resume route on
//     the wake signal. Covers form submission, review, draft (self-loop), etc.
//   - HTTPCall         — a synchronous outbound API call on entry; emits a
//     success or error event by outcome.
//   - RegisterAndWait  — register an external task via HTTP on entry, then
//     suspend until a completion signal arrives.
//
// Register them all on a registry with Register.
package builtin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/mushrafmim/config-fsm/pkg/executor"
)

// Register adds every builtin executor to reg. It stops and returns the first
// error (e.g. a name already taken).
func Register(reg *executor.Registry) error {
	for _, e := range []executor.Executor{
		InteractiveTask{},
		HTTPCall{},
		RegisterAndWait{},
	} {
		if err := reg.Register(e); err != nil {
			return err
		}
	}
	return nil
}

// InteractiveTask models a step that waits for the outside world (a user
// submitting a form, an officer deciding, a payment being made).
//
// On a cold entry (Event.Name == "") it suspends; the chart's `signals:`
// determine which signals may wake it. On resume (Event.Name carries the wake
// signal) it emits that signal name as the transition event, so the chart
// branches on the outcome.
//
// Config (all optional):
//
//	require: [field, ...]   # top-level keys that must be present in the signal
//	                        # payload on resume; a missing one is rejected with
//	                        # executor.ErrInvalidInput (the instance stays
//	                        # suspended and retriable). Presence only — values
//	                        # are not inspected.
//
// The signal's data is filed under the state's namespace by the engine, so this
// executor returns no Output of its own.
type InteractiveTask struct{}

func (InteractiveTask) Name() string { return "interactive_task" }

func (InteractiveTask) Execute(ctx context.Context, e *executor.Event) (executor.Result, error) {
	if e.Name == "" {
		return executor.Result{Suspend: true}, nil
	}
	for _, field := range stringSlice(e.Config["require"]) {
		if _, ok := e.Data[field]; !ok {
			return executor.Result{}, fmt.Errorf("missing required field %q: %w", field, executor.ErrInvalidInput)
		}
	}
	return executor.Result{Event: e.Name}, nil
}

// HTTPCall performs a single synchronous outbound HTTP request when the state
// is entered and routes on the result. A transport error or a non-2xx status
// becomes the error event (NOT a Go error), so the chart decides what happens
// next — keep an outgoing transition for it.
//
// Reliability note: the call is inline and at-most-once. A crash mid-call is
// not retried or made idempotent; that is the job of the (future) outbox.
//
// Config:
//
//	url:           "https://..."     # required
//	method:        "POST"            # default POST
//	headers:       { K: V, ... }     # optional
//	body:          { ... }           # optional JSON body
//	send_payload:  true              # if no body, send the whole instance payload
//	success_event: "success"         # event on 2xx (default "success")
//	error_event:   "error"           # event otherwise (default "error")
//	timeout_seconds: 30              # per-call timeout (default 30)
//
// Output (filed under the state namespace): {status: <int>, body: <parsed|raw>}.
type HTTPCall struct {
	// Client is the HTTP client to use. If nil, a package default with a 30s
	// timeout is used. Inject one in tests or to share transport settings.
	Client *http.Client
}

func (HTTPCall) Name() string { return "http_call" }

func (h HTTPCall) Execute(ctx context.Context, e *executor.Event) (executor.Result, error) {
	successEvent := stringOr(e.Config["success_event"], "success")
	errorEvent := stringOr(e.Config["error_event"], "error")
	ok, out, err := performHTTP(ctx, h.Client, e.Config, e.Payload)
	if err != nil {
		// A misconfiguration (e.g. missing url) is the author's bug, not a
		// runtime outcome — fail the instance rather than route to error.
		return executor.Result{}, err
	}
	if ok {
		return executor.Result{Event: successEvent, Output: out}, nil
	}
	return executor.Result{Event: errorEvent, Output: out}, nil
}

// RegisterAndWait performs an outbound HTTP call to register an external task
// when the state is entered, then suspends until that task reports back via a
// signal. It covers the "hand off to an external system and wait" shape.
//
// On a cold entry it calls the endpoint (see HTTPCall config — same keys):
//   - On success it suspends, filing the response under the state namespace so
//     the wait records what it is waiting on (e.g. an external task id). The
//     chart's `signals:` decide what wakes it.
//   - On a transport error or non-2xx it emits the error event instead, so the
//     chart can route registration failures somewhere (keep an `error`
//     transition).
//
// On resume it emits the wake signal as the transition event, like
// InteractiveTask. The optional `require` config (presence check) applies to
// the completion payload too.
type RegisterAndWait struct {
	Client *http.Client
}

func (RegisterAndWait) Name() string { return "register_and_wait" }

func (r RegisterAndWait) Execute(ctx context.Context, e *executor.Event) (executor.Result, error) {
	if e.Name != "" {
		for _, field := range stringSlice(e.Config["require"]) {
			if _, ok := e.Data[field]; !ok {
				return executor.Result{}, fmt.Errorf("missing required field %q: %w", field, executor.ErrInvalidInput)
			}
		}
		return executor.Result{Event: e.Name}, nil
	}
	errorEvent := stringOr(e.Config["error_event"], "error")
	ok, out, err := performHTTP(ctx, r.Client, e.Config, e.Payload)
	if err != nil {
		return executor.Result{}, err
	}
	if !ok {
		return executor.Result{Event: errorEvent, Output: out}, nil
	}
	// Registration succeeded: record the response and park. The engine files
	// Output under the state namespace before suspending.
	return executor.Result{Suspend: true, Output: out}, nil
}

// defaultClient is used when an executor is constructed without one.
var defaultClient = &http.Client{Timeout: 30 * time.Second}

// performHTTP issues the request described by cfg and reports whether it
// succeeded (transport ok AND 2xx), the response Output, and a non-nil error
// only for author misconfiguration (e.g. a missing url) that should fail the
// instance rather than route to the error event.
func performHTTP(ctx context.Context, client *http.Client, cfg, payload map[string]any) (bool, map[string]any, error) {
	url := stringOr(cfg["url"], "")
	if url == "" {
		return false, nil, fmt.Errorf("http executor: config 'url' is required")
	}
	method := stringOr(cfg["method"], http.MethodPost)
	if client == nil {
		client = defaultClient
	}

	var body io.Reader
	hasBody := false
	if b, ok := cfg["body"].(map[string]any); ok {
		raw, err := json.Marshal(b)
		if err != nil {
			return false, nil, fmt.Errorf("http executor: marshal body: %w", err)
		}
		body, hasBody = bytes.NewReader(raw), true
	} else if truthy(cfg["send_payload"]) {
		raw, err := json.Marshal(payload)
		if err != nil {
			return false, nil, fmt.Errorf("http executor: marshal payload: %w", err)
		}
		body, hasBody = bytes.NewReader(raw), true
	}

	if t := intOr(cfg["timeout_seconds"], 0); t > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(t)*time.Second)
		defer cancel()
	}

	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return false, nil, fmt.Errorf("http executor: build request: %w", err)
	}
	for k, v := range stringMap(cfg["headers"]) {
		req.Header.Set(k, v)
	}
	if hasBody && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := client.Do(req)
	if err != nil {
		// Transport failure is a runtime outcome → route to the error event.
		return false, map[string]any{"error": err.Error()}, nil
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)

	out := map[string]any{"status": resp.StatusCode, "body": decodeBody(raw)}
	return resp.StatusCode >= 200 && resp.StatusCode < 300, out, nil
}

// decodeBody parses a response body as JSON, falling back to the raw string if
// it is not valid JSON (or empty → nil).
func decodeBody(raw []byte) any {
	if len(raw) == 0 {
		return nil
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return string(raw)
	}
	return v
}

// --- config coercion helpers -------------------------------------------------
// Config values arrive as map[string]any from YAML or JSON, so numbers may be
// float64 and lists []any. These coerce defensively and fall back on mismatch.

func stringOr(v any, def string) string {
	if s, ok := v.(string); ok && s != "" {
		return s
	}
	return def
}

func stringSlice(v any) []string {
	raw, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		if s, ok := item.(string); ok && s != "" {
			out = append(out, s)
		}
	}
	return out
}

func stringMap(v any) map[string]string {
	raw, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	out := make(map[string]string, len(raw))
	for k, val := range raw {
		if s, ok := val.(string); ok {
			out[k] = s
		}
	}
	return out
}

func intOr(v any, def int) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	}
	return def
}

func truthy(v any) bool {
	b, ok := v.(bool)
	return ok && b
}
