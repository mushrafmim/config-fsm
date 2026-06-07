# FCAU Workflow Demo

This directory contains a runnable HTTP server that runs the `fcau/1-application` workflow using the `config-fsm` engine.

## Prerequisites
- Go 1.25+ installed

## Running the Server
Start the demo server:
```bash
go run demo/main.go
```
The server runs on port `8080` by default.

## Testing with Postman or cURL

The server exposes an API to start and signal the finite state machine.

### 1. Start a New Workflow Instance
This endpoint creates a new instance and returns its initial state (`applicant_submission`). It immediately suspends and waits for the `submit` signal.

**cURL:**
```bash
curl -X POST http://localhost:8080/start \
  -H "Content-Type: application/json" \
  -d '{"id": "test-flow-1"}'
```

### 2. View Instance State
You can view the current state and payload at any time.

**cURL:**
```bash
curl http://localhost:8080/instances/test-flow-1
```

### 3. Submit Application (Signal: `submit`)
When the instance is in the `applicant_submission` state, it is suspended waiting for the `submit` signal. Supply the application form payload.

**cURL:**
```bash
curl -X POST http://localhost:8080/instances/test-flow-1/signal/submit \
  -H "Content-Type: application/json" \
  -d '{"exporter_name": "Acme Corp", "exporter_address": "123 Main St"}'
```
Notice that the instance transitions to the `officer_review` state and suspends waiting for the signals: `approve`, `reject`, or `needs_more_info`.

### 4. Officer Review (Signal: Direct Decision)
Rather than a generic `review` signal, the state transitions are driven by sending the specific decision signal directly:

**Option A: Needs More Info (Signal: `needs_more_info`)**
```bash
curl -X POST http://localhost:8080/instances/test-flow-1/signal/needs_more_info \
  -H "Content-Type: application/json" \
  -d '{"comment": "Please provide your LC number."}'
```
This routes the flow back to the `applicant_submission` state.

**Option B: Approve (Signal: `approve`)**
```bash
curl -X POST http://localhost:8080/instances/test-flow-1/signal/approve \
  -H "Content-Type: application/json" \
  -d '{"reference_number": "CERT-12345"}'
```
This routes the flow to the `end` (terminal) state.

**Option C: Reject (Signal: `reject`)**
```bash
curl -X POST http://localhost:8080/instances/test-flow-1/signal/reject \
  -H "Content-Type: application/json" \
  -d '{"rejection_reason": "Invalid exporter address"}'
```
This routes the flow to the `end` (terminal) state.
