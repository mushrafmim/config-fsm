package main

import (
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"

	"github.com/mushrafmim/config-fsm/pkg/builtin"
	"github.com/mushrafmim/config-fsm/pkg/chart"
	"github.com/mushrafmim/config-fsm/pkg/engine"
	"github.com/mushrafmim/config-fsm/pkg/executor"
	"github.com/mushrafmim/config-fsm/pkg/store"
)

func main() {
	// 1. Setup Store and Registry
	memStore := store.NewMemory()
	reg := executor.NewRegistry()

	// 2. Register the generic builtin executors (interactive_task, http_call,
	// register_and_wait). The chart references them by name; no custom Go here.
	if err := builtin.Register(reg); err != nil {
		log.Fatalf("failed to register builtin executors: %v", err)
	}

	// 3. Load the Chart
	c, err := chart.Load("demo/fcau_1_application.yaml")
	if err != nil {
		log.Fatalf("failed to load chart: %v", err)
	}

	// 4. Initialize Engine
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	eng := engine.New(reg, memStore, engine.WithLogger(logger))

	// 5. Setup HTTP Handlers
	mux := http.NewServeMux()

	// POST /start
	// Starts a new instance
	mux.HandleFunc("POST /start", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID string `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		if req.ID == "" {
			http.Error(w, "id is required", http.StatusBadRequest)
			return
		}

		inst, err := eng.Start(r.Context(), c, req.ID, nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(inst)
	})

	// GET /instances/{id}
	// Retrieves the instance state
	mux.HandleFunc("GET /instances/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		inst, err := eng.Instance(r.Context(), id)
		if err != nil {
			if err == store.ErrNotFound {
				http.Error(w, "instance not found", http.StatusNotFound)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(inst)
	})

	// POST /instances/{id}/signal/{signal}
	// Sends a signal to a suspended instance
	mux.HandleFunc("POST /instances/{id}/signal/{signal}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		signal := r.PathValue("signal")

		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "invalid request payload", http.StatusBadRequest)
			return
		}

		inst, err := eng.SignalInstance(r.Context(), id, signal, payload)
		if err != nil {
			if err == store.ErrNotFound {
				http.Error(w, "instance not found", http.StatusNotFound)
			} else if err == store.ErrNotWaiting {
				http.Error(w, "instance not waiting on signal", http.StatusConflict)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(inst)
	})

	// 6. Start Server
	fmt.Println("Starting demo server on :8091...")
	if err := http.ListenAndServe(":8091", mux); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
