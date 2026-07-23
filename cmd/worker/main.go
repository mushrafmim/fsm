// Command worker runs a Temporal worker that hosts the FSM interpreter workflow
// and the task-runner activity. It constructs the Engine, registers plugins,
// and wires the engine onto the worker. Start a Temporal server first (or use an
// existing one on :7233):
//
//	temporal server start-dev
//
// then run this worker:
//
//	go run ./cmd/worker
package main

import (
	"context"
	"log"
	"time"

	"github.com/mushrafmim/fsm"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	// Pin to 127.0.0.1 (not "localhost") to avoid flipping between IPv4/IPv6
	// listeners when more than one Temporal server is up.
	c, err := client.Dial(client.Options{HostPort: "127.0.0.1:7233"})
	if err != nil {
		log.Fatalln("unable to connect to Temporal:", err)
	}
	defer c.Close()

	// Build the engine and register the plugins this worker knows how to run.
	// Model v2: a task runs once. It either completes with a Result (the command
	// to route on) or returns ErrParked to suspend until an external Complete
	// finishes it (DESIGN "Workflow model v2").
	e := fsm.New(fsm.WithClient(c))

	// http-call: automatic — never parks. In the leave-request chart it's the
	// "notify" step, so it completes with command "sent" and contributes a
	// notified_at local output (mapped to global by the transition's writes). A
	// real http-call would use its config to make the request.
	e.Register("http-call", fsm.PluginFunc(func(context.Context, fsm.TaskRequest, []byte) (fsm.Result, error) {
		return fsm.Result{Command: "sent", Data: fsm.Data{"notified_at": time.Now().UTC().Format(time.RFC3339)}}, nil
	}))

	// park suspends the task and waits for an external Complete. The caller
	// learns the (executionID, taskID) to complete via GetStatus; the command it
	// passes to Complete (e.g. "submitted", "approved") selects the transition.
	park := fsm.PluginFunc(func(_ context.Context, req fsm.TaskRequest, _ []byte) (fsm.Result, error) {
		log.Printf("parked: execution=%s task=%s state=%s — awaiting Complete", req.ExecutionID, req.TaskID, req.State)
		return fsm.Result{}, fsm.ErrParked
	})
	e.Register("UserForm", park)
	e.Register("await-approval", park)

	// external-review: on entry, "forward" to the review system (a real plugin
	// would POST to the endpoint in config using req.TaskID/ExecutionID as the
	// callback handle), then park awaiting the verdict. The verdict arrives as
	// the command on Complete (approved / rejected).
	e.Register("external-review", fsm.PluginFunc(func(_ context.Context, req fsm.TaskRequest, _ []byte) (fsm.Result, error) {
		log.Printf("forwarded for review: execution=%s task=%s — awaiting verdict", req.ExecutionID, req.TaskID)
		return fsm.Result{}, fsm.ErrParked
	}))

	w := worker.New(c, fsm.TaskQueue, worker.Options{})
	e.RegisterWorker(w)

	log.Println("worker listening on task queue:", fsm.TaskQueue)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalln("worker stopped:", err)
	}
}
