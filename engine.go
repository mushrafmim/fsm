package fsm

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// ----------------------------------------------------------------------------
// Engine — the dependency-injected core of the Temporal runtime (model v2).
//
// Two faces:
//
//   - WORKER side: holds the plugin registry and a ConfigFetcher, and registers
//     the interpreter workflow + the generic task-runner activity (RunTask).
//     RunTask is the one place I/O lives: fetch a state's config, resolve the
//     registered plugin, inject the config, run ONE task. Plugins never fetch
//     their own config (DESIGN principles 2-3).
//
//   - CLIENT side: holds the Temporal client and exposes Start / Complete /
//     GetStatus — the by-id front door (DESIGN principle 5).
//
// Model v2: a task runs once. The plugin's Run returns a Result (advance) or
// ErrParked (suspend). On ErrParked the runner returns activity.ErrResultPending
// so the task activity stays open; an external caller later finishes it with
// Complete (CompleteActivityByID). The workflow reacts only to the completed
// Result — no signal/re-run loop. Live dependencies (client, fetcher) are used
// only in activities / client calls, never in workflow code.
// ----------------------------------------------------------------------------

// Plugin is a unit of work registered with the engine. Run executes ONE task
// with its injected config and the execution data, and either:
//
//   - returns a Result (the command to route on + data it contributed), or
//   - returns ErrParked to suspend the task until some external input completes
//     it by id. A plugin that parks should hand req.TaskID + req.ExecutionID to
//     whoever will complete it (so they can call Engine.Complete).
type Plugin interface {
	Run(ctx context.Context, req TaskRequest, config []byte) (Result, error)
}

// PluginFunc adapts a plain function to a Plugin.
type PluginFunc func(ctx context.Context, req TaskRequest, config []byte) (Result, error)

func (f PluginFunc) Run(ctx context.Context, req TaskRequest, config []byte) (Result, error) {
	return f(ctx, req, config)
}

// TaskHandler runs one task and is the injection seam that keeps the chart
// plugin-agnostic: the chart node names a TaskTemplateID, and the handler
// resolves that reference to real work. It returns the task's Result (command +
// output), or ErrParked to suspend the task for a later Complete.
//
// The default handler is registry-backed (Engine.registryHandler): it treats the
// reference as a registered plugin name. A host that owns its own resolution —
// e.g. core's TaskManager, which maps a template id to a plugin type + config —
// injects its own handler via WithHandler and does not use the registry.
type TaskHandler func(ctx context.Context, req TaskRequest) (Result, error)

// ConfigFetcher resolves a task's reference (TaskTemplateID) into raw config for
// the default registry handler. Injected so it can be swapped (file, HTTP, DB) or
// mocked. Runs only inside the runner activity, never in workflow code.
type ConfigFetcher interface {
	Fetch(ctx context.Context, ref string) ([]byte, error)
}

// NopConfigFetcher returns no config. A sensible default and handy in tests.
type NopConfigFetcher struct{}

func (NopConfigFetcher) Fetch(context.Context, string) ([]byte, error) { return nil, nil }

// Engine drives executions via Temporal.
type Engine struct {
	client  client.Client
	fetcher ConfigFetcher
	plugins map[string]Plugin
	handler TaskHandler // injected work source; nil ⇒ registry-backed default
}

// Option configures an Engine at construction.
type Option func(*Engine)

// WithClient injects the Temporal client used by the client-side API.
func WithClient(c client.Client) Option { return func(e *Engine) { e.client = c } }

// WithConfigFetcher injects the config fetcher used by the default (registry)
// handler.
func WithConfigFetcher(f ConfigFetcher) Option { return func(e *Engine) { e.fetcher = f } }

// WithHandler injects the task handler — the work source. When set, RunTask
// dispatches to it instead of the plugin registry, so the host owns how a
// TaskTemplateID resolves to real work (e.g. core's TaskManager).
func WithHandler(h TaskHandler) Option { return func(e *Engine) { e.handler = h } }

// New builds an Engine. Defaults: a no-op config fetcher and an empty registry.
func New(opts ...Option) *Engine {
	e := &Engine{
		fetcher: NopConfigFetcher{},
		plugins: map[string]Plugin{},
	}
	for _, o := range opts {
		o(e)
	}
	return e
}

// Register adds a plugin under name.
func (e *Engine) Register(name string, p Plugin) {
	e.plugins[name] = p
}

// RegisterWorker wires the workflow and the task-runner activity onto a worker,
// under their stable names.
func (e *Engine) RegisterWorker(w worker.Worker) {
	w.RegisterWorkflowWithOptions(e.ExecutionWorkflow, workflow.RegisterOptions{Name: WorkflowName})
	w.RegisterActivityWithOptions(e.RunTask, activity.RegisterOptions{Name: RunTaskActivity})
}

// RunTask is the generic task runner, executed as a Temporal activity. It
// dispatches the node's TaskTemplateID to the task handler (the injected one, or
// the registry-backed default) — the one place I/O lives. If the handler parks
// (ErrParked) it returns activity.ErrResultPending, leaving the activity open for
// a later Complete; otherwise it returns the handler's Result.
func (e *Engine) RunTask(ctx context.Context, req TaskRequest) (Result, error) {
	handle := e.handler
	if handle == nil {
		handle = e.registryHandler
	}
	result, err := handle(ctx, req)
	if errors.Is(err, ErrParked) {
		// Leave the activity open. The task is resumed later by Complete, which
		// supplies the real Result via CompleteActivityByID.
		return Result{}, activity.ErrResultPending
	}
	if err != nil {
		return Result{}, err
	}
	return result, nil
}

// registryHandler is the default TaskHandler: it treats req.TaskTemplateID as a
// registered plugin name, fetches its config by the same reference, and runs it.
// A host that owns richer resolution (a template id → plugin type + properties)
// injects its own handler with WithHandler instead.
func (e *Engine) registryHandler(ctx context.Context, req TaskRequest) (Result, error) {
	p, ok := e.plugins[req.TaskTemplateID]
	if !ok {
		return Result{}, fmt.Errorf("no plugin registered for %q", req.TaskTemplateID)
	}
	config, err := e.fetcher.Fetch(ctx, req.TaskTemplateID)
	if err != nil {
		return Result{}, fmt.Errorf("fetch config for %q: %w", req.TaskTemplateID, err)
	}
	return p.Run(ctx, req, config)
}

// --- client-side API (DESIGN principle 5: address executions by id only) ------

// Start launches a new execution of chart under id, with the given initial data.
// The initial data seeds the execution's global bag; it is pre-flight checked
// against the chart's declared Inputs so a missing required input is rejected
// before an execution is created.
func (e *Engine) Start(ctx context.Context, id string, chart Chart, input Data) (client.WorkflowRun, error) {
	if err := chart.CheckInputs(input); err != nil {
		return nil, fmt.Errorf("initial inputs: %w", err)
	}
	return e.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        id,
		TaskQueue: TaskQueue,
	}, WorkflowName, chart, input)
}

// Complete finishes a parked task. taskID is the id the workflow assigned the
// task (read it from GetStatus, or it was handed to the plugin as req.TaskID).
// The Result's command selects the outgoing transition; its data merges into the
// execution. This is the resume verb that drives the engine to advance.
func (e *Engine) Complete(ctx context.Context, executionID, taskID string, result Result) error {
	return e.client.CompleteActivityByID(ctx, CompletionNamespace, executionID, "", taskID, result, nil)
}

// GetStatus returns where an execution currently sits — its state and the id of
// the task it is on — without affecting it.
func (e *Engine) GetStatus(ctx context.Context, id string) (Status, error) {
	resp, err := e.client.QueryWorkflow(ctx, id, "", StatusQuery)
	if err != nil {
		return Status{}, err
	}
	var s Status
	if err := resp.Get(&s); err != nil {
		return Status{}, err
	}
	return s, nil
}
