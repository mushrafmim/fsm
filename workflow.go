package fsm

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/workflow"
)

// ExecutionWorkflow is the generic interpreter: ONE workflow definition that can
// run ANY chart. It is a method on Engine so it can reach the registry, but it
// does no I/O and touches no live dependency — it stays fully deterministic, as
// Temporal workflow code must be.
//
// The chart is passed as workflow input, so Temporal records it in this
// execution's history — that gives us the "execution owns a write-once copy of
// its chart" property for free (DESIGN principle 11).
//
// Model v2 walk: at each non-terminal state it runs the state's task ONCE (as
// the RunTask activity) and waits for it to complete. A parked task is invisible
// here — it is just an activity that takes a long time to finish (it returned
// ErrResultPending and is completed later by Engine.Complete). When the task
// completes it yields a Result; its command selects the outgoing transition and
// its data merges into the bag. No signals, no re-run loop.
func (e *Engine) ExecutionWorkflow(ctx workflow.Context, chart Chart, input Data) (Data, error) {
	logger := workflow.GetLogger(ctx)

	data := input
	if data == nil {
		data = Data{}
	}
	current := chart.Initial
	var currentTaskID string

	// Expose where the execution sits via a Query: its state and the id of the
	// task it is on (callers need the TaskID to Complete a parked task). Asking
	// does not touch progress (principle 5).
	if err := workflow.SetQueryHandler(ctx, StatusQuery, func() (Status, error) {
		return Status{State: current, TaskID: currentTaskID}, nil
	}); err != nil {
		return data, fmt.Errorf("register query handler: %w", err)
	}

	for {
		state, ok := chart.findState(current)
		if !ok {
			return data, fmt.Errorf("execution is at unknown state %q", current)
		}

		// Terminal state (declared): we're done. Terminality is explicit (End),
		// not inferred from missing transitions — Validate rejects unwired
		// non-end states up front. Fire the completion activity (a no-op unless a
		// CompletionHandler is injected) so a host can wake whatever awaits this
		// execution, then return the final global bag.
		if state.End {
			logger.Info("execution complete", "state", current)
			doneCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: time.Minute,
			})
			execID := workflow.GetInfo(ctx).WorkflowExecution.ID
			if err := workflow.ExecuteActivity(doneCtx, CompletedTaskActivity, execID, data).Get(ctx, nil); err != nil {
				return data, fmt.Errorf("completion handler at state %q: %w", current, err)
			}
			return data, nil
		}

		// Assign this task a unique, addressable id. The UUID makes it unique
		// even when a state is revisited (loops), so a re-entered task is a
		// distinct activity that can be completed independently. uuid is
		// non-deterministic, so it must be generated inside a SideEffect.
		var taskID string
		if err := workflow.SideEffect(ctx, func(workflow.Context) any {
			return fmt.Sprintf("%s:%s", current, uuid.NewString())
		}).Get(&taskID); err != nil {
			return data, fmt.Errorf("assign task id at state %q: %w", current, err)
		}
		currentTaskID = taskID

		// Run the task once. Pin the activity id to taskID so Engine.Complete
		// can finish a parked task by that id. A parked task may wait a long time
		// (a user, an external review), so the activity gets a generous
		// schedule-to-close; refine with heartbeating/policies later.
		actCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:             taskID,
			ScheduleToCloseTimeout: 365 * 24 * time.Hour,
		})
		// Build the task's local input from the global bag (State.Input), so it
		// sees only its declared inputs under its own names — never the global
		// bag directly (DESIGN "Workflow model v2": input, global → local).
		localIn, err := applyInput(data, state.Input)
		if err != nil {
			return data, fmt.Errorf("state %q input mapping: %w", current, err)
		}

		req := TaskRequest{
			ExecutionID:    workflow.GetInfo(ctx).WorkflowExecution.ID,
			TaskID:         taskID,
			State:          current,
			TaskTemplateID: state.TaskTemplateID,
			Data:           localIn,
		}
		var result Result
		if err := workflow.ExecuteActivity(actCtx, RunTaskActivity, req).Get(ctx, &result); err != nil {
			return data, fmt.Errorf("task %q at state %q failed: %w", state.TaskTemplateID, state.Name, err)
		}

		// Route on the completion command, then export that command's selected
		// local outputs into the global bag (Transition.Writes, local → global).
		t, ok := state.route(result.Command)
		if !ok {
			return data, fmt.Errorf("state %q: task completed with command %q and no matching transition", current, result.Command)
		}
		if err := applyWrites(result.Data, t.Writes, data); err != nil {
			return data, fmt.Errorf("state %q writes for command %q: %w", current, result.Command, err)
		}
		logger.Info("task advanced", "from", current, "command", result.Command, "to", t.Target)
		current = t.Target
	}
}
