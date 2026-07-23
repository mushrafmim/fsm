package fsm

import (
	"context"
	"testing"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

// emit builds a plugin that always completes immediately with a fixed command
// and records that it ran in the data bag. Used to drive the interpreter walk
// deterministically (parking is tested separately, in TestRunTask).
func emit(command string) PluginFunc {
	return func(_ context.Context, _ TaskRequest, _ []byte) (Result, error) {
		return Result{Command: command, Data: Data{command: true}}, nil
	}
}

// testEngine registers the auto-completing plugins the walk charts use.
func testEngine() *Engine {
	e := New()
	e.Register("emit-submitted", emit("submitted"))
	e.Register("emit-approved", emit("approved"))
	e.Register("emit-rejected", emit("rejected"))
	e.Register("noop", emit("done"))
	e.Register("http-call", emit("done"))
	return e
}

// register wires the engine's task-runner activity onto a test environment under
// the same name the worker uses. (The workflow is passed directly to
// ExecuteWorkflow.)
func register(env *testsuite.TestWorkflowEnvironment, e *Engine) {
	env.RegisterActivityWithOptions(e.RunTask, activity.RegisterOptions{Name: RunTaskActivity})
}

// walkChart routes the manager-approval branch on `verdict`, so one chart drives
// both the approved and rejected paths to the terminal "done".
func walkChart(verdict string) Chart {
	return Chart{
		Initial: "collect-details",
		States: []State{
			{Name: "collect-details", Plugin: "emit-submitted", ConfigRef: "c/form",
				Transitions: []Transition{{Command: "submitted", Target: "manager-approval"}}},
			{Name: "manager-approval", Plugin: "emit-" + verdict, ConfigRef: "c/approval",
				Transitions: []Transition{
					{Command: "approved", Target: "notify"},
					{Command: "rejected", Target: "done"},
				}},
			{Name: "notify", Plugin: "http-call", ConfigRef: "c/notify",
				Transitions: []Transition{{Command: "done", Target: "done"}}},
			{Name: "done", End: true},
		},
	}
}

// TestWorkflow_WalkApproved walks collect → approval(approved) → notify → done.
func TestWorkflow_WalkApproved(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()
	register(env, testEngine())

	env.ExecuteWorkflow((&Engine{}).ExecutionWorkflow, walkChart("approved"), Data{})

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
	// Each task's output is namespaced under its state name, so every state the
	// walk ran a task at appears as a key (the terminal "done" runs no task).
	for _, k := range []string{"collect-details", "manager-approval", "notify"} {
		if _, ok := out[k]; !ok {
			t.Errorf("expected namespace %q in final data, got %v", k, out)
		}
	}
}

// TestWorkflow_WalkRejected takes the rejected branch straight to the terminal
// state (no notify).
func TestWorkflow_WalkRejected(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()
	register(env, testEngine())

	env.ExecuteWorkflow((&Engine{}).ExecutionWorkflow, walkChart("rejected"), Data{})

	if !env.IsWorkflowCompleted() || env.GetWorkflowError() != nil {
		t.Fatalf("workflow did not complete cleanly: %v", env.GetWorkflowError())
	}
	var out Data
	_ = env.GetWorkflowResult(&out)
	if _, ok := out["notify"]; ok {
		t.Errorf("rejected path should not have run notify: %v", out)
	}
}

// loopEngine has a plugin that loops on "again" until the data-bag count reaches
// 3, then completes with "done" — proving a revisited state is a distinct task
// (unique ids) and that the walk terminates.
func loopEngine() *Engine {
	e := New()
	e.Register("counter", PluginFunc(func(_ context.Context, req TaskRequest, _ []byte) (Result, error) {
		// The counter reads its own prior output from its state namespace
		// ("loop"), proving a revisited state overwrites its own namespace.
		n := 0
		if ns, ok := req.Data["loop"].(map[string]any); ok {
			if v, ok := ns["count"].(float64); ok { // round-trips as float64
				n = int(v)
			}
		}
		n++
		cmd := "again"
		if n >= 3 {
			cmd = "done"
		}
		return Result{Command: cmd, Data: Data{"count": n}}, nil
	}))
	e.Register("noop", emit("noop-done"))
	return e
}

// TestWorkflow_Loop revisits a state until its task says to stop.
func TestWorkflow_Loop(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()
	register(env, loopEngine())

	chart := Chart{
		Initial: "loop",
		States: []State{
			{Name: "loop", Plugin: "counter", ConfigRef: "c/loop",
				Transitions: []Transition{
					{Command: "again", Target: "loop"},
					{Command: "done", Target: "end"},
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
	ns, _ := out["loop"].(map[string]any)
	if got, _ := ns["count"].(float64); int(got) != 3 {
		t.Errorf("expected loop.count==3 after loop, got %v", out["loop"])
	}
}

// TestRunTask_Advance: an automatic plugin's Result passes straight through.
func TestRunTask_Advance(t *testing.T) {
	e := New()
	e.Register("auto", emit("done"))
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
