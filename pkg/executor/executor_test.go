package executor

import (
	"context"
	"testing"
)

func TestRegistry_RegisterAndGet(t *testing.T) {
	r := NewRegistry()
	e := Func{N: "x", Fn: func(ctx context.Context, e *Event) (Result, error) {
		return Result{Event: "ok"}, nil
	}}
	if err := r.Register(e); err != nil {
		t.Fatal(err)
	}
	got, ok := r.Get("x")
	if !ok {
		t.Fatal("Get returned !ok")
	}
	if got.Name() != "x" {
		t.Fatalf("name = %s", got.Name())
	}
}

func TestRegistry_RejectsDuplicates(t *testing.T) {
	r := NewRegistry()
	e := Func{N: "x", Fn: func(ctx context.Context, e *Event) (Result, error) { return Result{}, nil }}
	if err := r.Register(e); err != nil {
		t.Fatal(err)
	}
	if err := r.Register(e); err == nil {
		t.Fatal("expected duplicate error")
	}
}

func TestRegistry_RejectsEmptyName(t *testing.T) {
	r := NewRegistry()
	if err := r.Register(Func{N: ""}); err == nil {
		t.Fatal("expected empty-name error")
	}
}
