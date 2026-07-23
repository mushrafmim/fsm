package fsm

import (
	"context"
	"testing"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

// emit builds a plugin that always completes immediately with a fixed command
// and a fixed local output. Used to drive the interpreter walk deterministically
// (parking is tested separately, in TestRunTask).
func emit(command string, out Data) PluginFunc {
	return func(_ context.Context, _ TaskRequest, _ []byte) (Result, error) {
		return Result{Command: command, Data: out}, nil
	}
}

// testEngine registers the auto-completing plugins the walk chart uses.
func testEngine() *Engine {
	e := New()
	e.Register("emit-submitted", emit("submitted", Data{"days": 3, "reason": "vacation"}))
	e.Register("emit-approve", emit("approve", Data{"decision": "approved"}))
	e.Register("emit-reject", emit("reject", Data{"decision": "rejected"}))
	e.Register("http-call", emit("sent", Data{"notified_at": "t0"}))
	return e
}

// register wires the engine's task-runner activity onto a test environment under
// the same name the worker uses. (The workflow is passed directly to
// ExecuteWorkflow.)
func register(env *testsuite.TestWorkflowEnvironment, e *Engine) {
	env.RegisterActivityWithOptions(e.RunTask, activity.RegisterOptions{Name: RunTaskActivity})
}

// walkChart mirrors leave-request: each task exports selected locals to the
// global "leave.*" bag (writes) and reads them back (input); the approval task's
// command chooses the branch. verdictCmd selects the approval plugin.
func walkChart(verdictCmd string) Chart {
	return Chart{
		Initial: "collect",
		States: []State{
			{Name: "collect", Plugin: "emit-submitted", ConfigRef: "c/form",
				Transitions: []Transition{
					{Command: "submitted", Target: "approval",
						Writes: map[string]string{"days": "leave.days", "reason": "leave.reason"}},
				}},
			{Name: "approval", Plugin: "emit-" + verdictCmd, ConfigRef: "c/appr",
				Input: map[string]string{"leave.days": "days", "leave.reason": "reason"},
				Transitions: []Transition{
					{Command: "approve", Target: "notify",
						Writes: map[string]string{"decision": "leave.decision"}},
					{Command: "reject", Target: "rejected",
						Writes: map[string]string{"decision": "leave.decision"}},
				}},
			{Name: "notify", Plugin: "http-call", ConfigRef: "c/notify",
				Input: map[string]string{"leave.decision": "decision"},
				Transitions: []Transition{
					{Command: "sent", Target: "approved",
						Writes: map[string]string{"notified_at": "leave.notified_at"}},
				}},
			{Name: "approved", End: true},
			{Name: "rejected", End: true},
		},
	}
}

// TestWorkflow_WalkApproved walks collect → approval(approve) → notify → approved,
// checking that each task's writes landed in the global bag.
func TestWorkflow_WalkApproved(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()
	register(env, testEngine())

	env.ExecuteWorkflow((&Engine{}).ExecutionWorkflow, walkChart("approve"), Data{})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow errored: %v", err)
	}
	var out Data
	if err := env.GetWorkflowResult(&out); err != nil {
		t.Fatalf("get result: %v", err)
	}
	if v, ok := getPath(out, "leave.decision"); !ok || v != "approved" {
		t.Errorf("leave.decision = %v,%v; want approved", v, ok)
	}
	// collect wrote leave.reason, and the approved path ran notify (leave.notified_at).
	if _, ok := getPath(out, "leave.reason"); !ok {
		t.Errorf("expected leave.reason in %v", out)
	}
	if _, ok := getPath(out, "leave.notified_at"); !ok {
		t.Errorf("approved path should have run notify (leave.notified_at) in %v", out)
	}
}

// TestWorkflow_WalkRejected takes the rejected branch straight to the terminal
// state — notify never runs, so leave.notified_at is absent.
func TestWorkflow_WalkRejected(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()
	register(env, testEngine())

	env.ExecuteWorkflow((&Engine{}).ExecutionWorkflow, walkChart("reject"), Data{})

	if !env.IsWorkflowCompleted() || env.GetWorkflowError() != nil {
		t.Fatalf("workflow did not complete cleanly: %v", env.GetWorkflowError())
	}
	var out Data
	_ = env.GetWorkflowResult(&out)
	if v, _ := getPath(out, "leave.decision"); v != "rejected" {
		t.Errorf("leave.decision = %v; want rejected", v)
	}
	if _, ok := getPath(out, "leave.notified_at"); ok {
		t.Errorf("rejected path should not have run notify: %v", out)
	}
}

// loopEngine has a plugin that reads its own count from local input, increments,
// and writes it back to global — looping until count reaches 3.
func loopEngine() *Engine {
	e := New()
	e.Register("counter", PluginFunc(func(_ context.Context, req TaskRequest, _ []byte) (Result, error) {
		n := 0
		if v, ok := req.Data["count"].(float64); ok { // round-trips as float64
			n = int(v)
		}
		n++
		cmd := "again"
		if n >= 3 {
			cmd = "done"
		}
		return Result{Command: cmd, Data: Data{"count": n}}, nil
	}))
	return e
}

// TestWorkflow_Loop revisits a state until its task says to stop, threading a
// counter through the global bag (write on the way out, read on the way in).
func TestWorkflow_Loop(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()
	register(env, loopEngine())

	chart := Chart{
		Initial: "loop",
		States: []State{
			{Name: "loop", Plugin: "counter", ConfigRef: "c/loop",
				Input: map[string]string{"prog.count?": "count"}, // absent on the first pass
				Transitions: []Transition{
					{Command: "again", Target: "loop", Writes: map[string]string{"count": "prog.count"}},
					{Command: "done", Target: "end", Writes: map[string]string{"count": "prog.count"}},
				}},
			{Name: "end", End: true},
		},
	}

	env.ExecuteWorkflow((&Engine{}).ExecutionWorkflow, chart, Data{})

	if !env.IsWorkflowCompleted() || env.GetWorkflowError() != nil {
		t.Fatalf("loop workflow did not complete cleanly: %v", env.GetWorkflowError())
	}
	var out Data
	_ = env.GetWorkflowResult(&out)
	if v, _ := getPath(out, "prog.count"); v != float64(3) {
		t.Errorf("expected prog.count==3 after loop, got %v", v)
	}
}

// TestRunTask_Advance: an automatic plugin's Result passes straight through.
func TestRunTask_Advance(t *testing.T) {
	e := New()
	e.Register("auto", emit("done", nil))
	res, err := e.RunTask(context.Background(), TaskRequest{Plugin: "auto"})
	if err != nil {
		t.Fatalf("RunTask errored: %v", err)
	}
	if res.Command != "done" {
		t.Fatalf("expected command done, got %q", res.Command)
	}
}

// TestRunTask_Parks: a plugin returning ErrParked makes the runner report
// ErrResultPending, so the task activity stays open for a later Complete.
func TestRunTask_Parks(t *testing.T) {
	e := New()
	e.Register("interactive", PluginFunc(func(context.Context, TaskRequest, []byte) (Result, error) {
		return Result{}, ErrParked
	}))
	_, err := e.RunTask(context.Background(), TaskRequest{Plugin: "interactive"})
	if err != activity.ErrResultPending {
		t.Fatalf("expected activity.ErrResultPending, got %v", err)
	}
}
