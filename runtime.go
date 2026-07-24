package fsm

import "errors"

// ----------------------------------------------------------------------------
// Runtime types (Temporal runtime, Workflow model v2)
//
// The chart (chart.go) is still our pure-data definition; Temporal drives an
// execution. Under model v2 a task runs ONCE: it either completes (yielding a
// command + data) or PARKS — an open activity that some external input completes
// later by id. There is no signal/re-run loop.
//
//   - an Execution             -> a Temporal Workflow Execution
//   - the engine/registry       -> the Temporal server + a worker
//   - StateStore/HistoryStore   -> Temporal's durable event history (for free)
//   - a task's work             -> a Temporal Activity (RunTask)
//   - parking for input         -> the activity returns ErrResultPending (stays open)
//   - completing a parked task  -> CompleteActivityByID with a Result
//   - inspecting current state  -> a Query
// ----------------------------------------------------------------------------

// DefaultTaskQueue is the task queue an engine uses unless WithTaskQueue
// overrides it. The worker and the engine must agree on the queue; read it back
// from Engine.TaskQueue() so the two can't drift.
const DefaultTaskQueue = "fsm-task-queue"

// StatusQuery returns where an execution currently sits (state + the id of the
// task it is on), so a caller can both inspect it and learn the TaskID it needs
// to Complete a parked task.
const StatusQuery = "status"

// WorkflowName and RunTaskActivity are the stable names the workflow and the
// task-runner activity register under. We register and dispatch by these
// explicit names (rather than reflected method-value names) so behaviour is
// unambiguous.
const (
	WorkflowName          = "ExecutionWorkflow"
	RunTaskActivity       = "RunTask"
	CompletedTaskActivity = "CompletedTask"
)

// CompletionNamespace is the Temporal namespace parked tasks are completed
// against (via CompleteActivityByID). Kept as a constant so the client and any
// external resumer agree.
const CompletionNamespace = "default"

// Data is an execution's own mutable bag of values — form inputs, plugin
// results, anything traveling with this instance. It must be serializable
// (Temporal records it in workflow input, activity args, and completions).
type Data map[string]any

// Result is what a task produces when it completes: the command that selects the
// outgoing transition, plus the data it contributed to the execution. It is
// returned directly by an automatic task, or supplied later (via Complete) by
// whatever external input finishes a parked task.
type Result struct {
	Command string `json:"command"`
	Data    Data   `json:"data,omitempty"`
}

// TaskRequest is what the engine hands the task handler for one task run.
//
//   - TaskTemplateID is the chart node's *reference* — the engine does not know
//     which plugin or config it maps to; the injected handler resolves it (the
//     default handler treats it as a registered plugin name; core's executor
//     resolves it through a template registry).
//   - TaskID is the unique, addressable id of this task's activity: a parked task
//     is resumed by calling Complete with this id, so whoever parks must hand
//     TaskID (with ExecutionID) to whoever will complete it later.
//   - Data is the task's local inputs (already remapped from global by State.Input).
type TaskRequest struct {
	ExecutionID    string    `json:"executionID"`
	TaskID         string    `json:"taskID"`
	State          StateName `json:"state"`
	TaskTemplateID string    `json:"taskTemplateID"`
	Data           Data      `json:"data"`
}

// Status is the snapshot returned by the StatusQuery.
type Status struct {
	State  StateName `json:"state"`
	TaskID string    `json:"taskID"`
}

// ErrParked is returned by a plugin's Run to suspend the task: the engine leaves
// the task activity open (ErrResultPending) and advances only when some external
// input completes it by id. "Automatic" plugins never return it; "interactive"
// ones return it to wait for outside input (DESIGN principle 15, model v2).
var ErrParked = errors.New("task parked: awaiting external completion")
