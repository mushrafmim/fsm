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

// ConfigFetcher resolves a state's ConfigRef into raw plugin config. Injected so
// it can be swapped (file, HTTP, DB) or mocked. Runs only inside the runner
// activity, never in workflow code.
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
}

// Option configures an Engine at construction.
type Option func(*Engine)

// WithClient injects the Temporal client used by the client-side API.
func WithClient(c client.Client) Option { return func(e *Engine) { e.client = c } }

// WithConfigFetcher injects the config fetcher used by the task runner.
func WithConfigFetcher(f ConfigFetcher) Option { return func(e *Engine) { e.fetcher = f } }

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

// RunTask is the generic task runner, executed as a Temporal activity. It is
// where all I/O lives: resolve the registered plugin, fetch + inject its config,
// run one task. If the plugin parks (ErrParked) it returns activity.
// ErrResultPending, leaving the activity open for a later Complete; otherwise it
// returns the plugin's Result.
func (e *Engine) RunTask(ctx context.Context, req TaskRequest) (Result, error) {
	p, ok := e.plugins[req.Plugin]
	if !ok {
		return Result{}, fmt.Errorf("no plugin registered for %q", req.Plugin)
	}
	config, err := e.fetcher.Fetch(ctx, req.ConfigRef)
	if err != nil {
		return Result{}, fmt.Errorf("fetch config for plugin %q (ref %q): %w", req.Plugin, req.ConfigRef, err)
	}

	result, err := p.Run(ctx, req, config)
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

// --- client-side API (DESIGN principle 5: address executions by id only) ------

// Start launches a new execution of chart under id, with the given initial data.
func (e *Engine) Start(ctx context.Context, id string, chart Chart, input Data) (client.WorkflowRun, error) {
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
