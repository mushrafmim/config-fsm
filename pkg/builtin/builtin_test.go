package builtin

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mushrafmim/config-fsm/pkg/executor"
)

func TestInteractiveTask_ColdEntrySuspends(t *testing.T) {
	res, err := InteractiveTask{}.Execute(context.Background(), &executor.Event{})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !res.Suspend {
		t.Fatalf("cold entry should suspend, got %+v", res)
	}
}

func TestInteractiveTask_ResumeEmitsSignalAsEvent(t *testing.T) {
	res, err := InteractiveTask{}.Execute(context.Background(), &executor.Event{
		Name: "approve",
		Data: map[string]any{"note": "ok"},
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.Event != "approve" {
		t.Fatalf("event = %q, want approve", res.Event)
	}
}

func TestInteractiveTask_RequireRejectsMissingField(t *testing.T) {
	cfg := map[string]any{"require": []any{"token"}}

	// Missing required field → rejected with ErrInvalidInput.
	_, err := InteractiveTask{}.Execute(context.Background(), &executor.Event{
		Name: "submit", Data: map[string]any{"other": 1}, Config: cfg,
	})
	if !errors.Is(err, executor.ErrInvalidInput) {
		t.Fatalf("err = %v, want ErrInvalidInput", err)
	}

	// Present → accepted.
	res, err := InteractiveTask{}.Execute(context.Background(), &executor.Event{
		Name: "submit", Data: map[string]any{"token": "abc"}, Config: cfg,
	})
	if err != nil || res.Event != "submit" {
		t.Fatalf("res=%+v err=%v, want event submit / nil", res, err)
	}
}

func TestHTTPCall_SuccessEmitsSuccessWithBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"reference_number":"ABC-123"}`))
	}))
	defer srv.Close()

	res, err := HTTPCall{Client: srv.Client()}.Execute(context.Background(), &executor.Event{
		Config: map[string]any{"url": srv.URL},
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.Event != "success" {
		t.Fatalf("event = %q, want success", res.Event)
	}
	if res.Output["status"] != 200 {
		t.Fatalf("status = %v, want 200", res.Output["status"])
	}
	body, ok := res.Output["body"].(map[string]any)
	if !ok || body["reference_number"] != "ABC-123" {
		t.Fatalf("body = %v, want parsed json", res.Output["body"])
	}
}

func TestHTTPCall_Non2xxEmitsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer srv.Close()

	res, err := HTTPCall{Client: srv.Client()}.Execute(context.Background(), &executor.Event{
		Config: map[string]any{"url": srv.URL, "error_event": "failed"},
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.Event != "failed" {
		t.Fatalf("event = %q, want failed", res.Event)
	}
	if res.Output["status"] != 500 {
		t.Fatalf("status = %v, want 500", res.Output["status"])
	}
}

func TestHTTPCall_MissingURLFailsInstance(t *testing.T) {
	// A missing url is an authoring bug, not a runtime outcome — surface a Go
	// error so the engine fails the instance rather than routing to error.
	_, err := HTTPCall{}.Execute(context.Background(), &executor.Event{Config: map[string]any{}})
	if err == nil {
		t.Fatal("expected error for missing url")
	}
}

func TestHTTPCall_SendsBodyAndHeaders(t *testing.T) {
	var gotAuth, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		b := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(b)
		gotBody = string(b)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	_, err := HTTPCall{Client: srv.Client()}.Execute(context.Background(), &executor.Event{
		Config: map[string]any{
			"url":     srv.URL,
			"headers": map[string]any{"Authorization": "Bearer xyz"},
			"body":    map[string]any{"hello": "world"},
		},
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if gotAuth != "Bearer xyz" {
		t.Fatalf("auth header = %q", gotAuth)
	}
	if gotBody != `{"hello":"world"}` {
		t.Fatalf("body = %q", gotBody)
	}
}

func TestRegisterAndWait_RegistersThenSuspends(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"task_id":"T-9"}`))
	}))
	defer srv.Close()

	res, err := RegisterAndWait{Client: srv.Client()}.Execute(context.Background(), &executor.Event{
		Config: map[string]any{"url": srv.URL},
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !called {
		t.Fatal("register endpoint was not called")
	}
	if !res.Suspend {
		t.Fatalf("should suspend after successful register, got %+v", res)
	}
	// The register response is returned as Output so the engine records what
	// the instance is waiting on.
	body, ok := res.Output["body"].(map[string]any)
	if !ok || body["task_id"] != "T-9" {
		t.Fatalf("register response not in Output: %v", res.Output)
	}
}

func TestRegisterAndWait_RegisterFailureRoutesToError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "down", http.StatusBadGateway)
	}))
	defer srv.Close()

	res, err := RegisterAndWait{Client: srv.Client()}.Execute(context.Background(), &executor.Event{
		Config: map[string]any{"url": srv.URL},
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.Suspend {
		t.Fatal("should not suspend when registration fails")
	}
	if res.Event != "error" {
		t.Fatalf("event = %q, want error", res.Event)
	}
}

func TestRegisterAndWait_ResumeEmitsSignal(t *testing.T) {
	res, err := RegisterAndWait{}.Execute(context.Background(), &executor.Event{
		Name: "sample_received",
		Data: map[string]any{"received_at": "2026-06-06"},
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.Event != "sample_received" {
		t.Fatalf("event = %q, want sample_received", res.Event)
	}
}

func TestRegister_AddsAllAndRejectsDuplicates(t *testing.T) {
	reg := executor.NewRegistry()
	if err := Register(reg); err != nil {
		t.Fatalf("Register: %v", err)
	}
	for _, name := range []string{"interactive_task", "http_call", "register_and_wait"} {
		if _, ok := reg.Get(name); !ok {
			t.Fatalf("executor %q not registered", name)
		}
	}
	if err := Register(reg); err == nil {
		t.Fatal("second Register should fail on duplicate")
	}
}
