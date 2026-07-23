// Command starter launches one execution of a chart and drives it to completion
// via the Engine's client-side API (Start / GetStatus / Complete). It
// demonstrates the whole Temporal runtime end to end (model v2).
//
// Prerequisites: a Temporal server on :7233 and the worker (`go run ./cmd/worker`)
// must be running. Then:
//
//	go run ./cmd/starter
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"fsm"

	"go.temporal.io/sdk/client"
)

func main() {
	ctx := context.Background()

	// Load the chart handed to the engine at start (the "configuration chart").
	raw, err := os.ReadFile("charts/leave-request.json")
	if err != nil {
		log.Fatalln("read chart:", err)
	}
	var chart fsm.Chart
	if err := json.Unmarshal(raw, &chart); err != nil {
		log.Fatalln("parse chart:", err)
	}
	if err := chart.Validate(); err != nil {
		log.Fatalln("invalid chart:", err)
	}

	c, err := client.Dial(client.Options{HostPort: "127.0.0.1:7233"})
	if err != nil {
		log.Fatalln("connect to Temporal:", err)
	}
	defer c.Close()

	// The starter only needs the client face of the engine (no plugins).
	e := fsm.New(fsm.WithClient(c))

	executionID := fmt.Sprintf("leave-request-%d", time.Now().Unix())

	run, err := e.Start(ctx, executionID, chart, fsm.Data{})
	if err != nil {
		log.Fatalln("start execution:", err)
	}
	log.Printf("started execution %s (run %s)", run.GetID(), run.GetRunID())

	// collect-details is interactive: it parks. Wait until it's parked, then
	// complete it with the "submitted" command (model v2: completing the task is
	// what advances it — no separate signal).
	taskID := waitParked(ctx, e, executionID, "collect-details")
	complete(ctx, e, executionID, taskID, fsm.Result{
		Command: "submitted",
		Data:    fsm.Data{"days": 3, "reason": "vacation"},
	})

	// manager-approval is interactive: approve it to continue to notify.
	taskID = waitParked(ctx, e, executionID, "manager-approval")
	complete(ctx, e, executionID, taskID, fsm.Result{Command: "approved"})

	// notify is automatic (http-call) and leads to the terminal "done" state.
	var result fsm.Data
	if err := run.Get(ctx, &result); err != nil {
		log.Fatalln("execution failed:", err)
	}
	log.Printf("execution complete; final data = %v", result)
}

// waitParked polls GetStatus until the execution is parked at the expected state
// (its task id is assigned) and returns that task id, so the caller can Complete
// it. This stands in for a real caller that would learn (executionID, taskID)
// from the parked task's own notification.
func waitParked(ctx context.Context, e *fsm.Engine, id string, want fsm.StateName) string {
	const attempts = 50
	var last fsm.Status
	for i := range attempts {
		s, err := e.GetStatus(ctx, id)
		if err == nil {
			last = s
			if s.State == want && s.TaskID != "" {
				log.Printf("parked at %q (task %s)", s.State, s.TaskID)
				return s.TaskID
			}
		}
		time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
	}
	log.Fatalf("execution never parked at %q (last status: %+v)", want, last)
	return ""
}

func complete(ctx context.Context, e *fsm.Engine, id, taskID string, result fsm.Result) {
	if err := e.Complete(ctx, id, taskID, result); err != nil {
		log.Fatalf("complete task %s with %q: %v", taskID, result.Command, err)
	}
	log.Printf("completed task %s with command %q", taskID, result.Command)
}
