# FSM Engine — Design

A workflow/finite-state-machine engine. A **Chart** defines a flow as a graph of
**plugins**; an **Execution** is one running instance walking that chart. Many
executions run through the engine concurrently. Some executions **park** to wait
for outside input and resume later when a caller sends a **command**.

This document records the principles and terminology we agree on as we build the
engine piece by piece. It is the shared mental model — code follows it, not the
other way around.

> **Current architecture (implemented): Temporal.** We replaced the hand-rolled
> in-process engine with **Temporal** as the execution runtime (all-in, not the
> pluggable seam we'd earlier sketched). The `Chart` (chart.go) remains our pure
> data definition and is handed in at start; everything that *drives and persists*
> an execution is now Temporal. See **"Runtimes"** below and the roadmap.
> Principles about chart/plugins/commands still hold; the local-engine principles
> (concurrency model, our own StateStore/HistoryStore, the `Runtime` abstraction)
> are **superseded** and kept only as design history.

> **Workflow model v2 (current target).** We are now refactoring toward an
> *execute → complete* task lifecycle with *declarative, condition-based routing*,
> so the engine can drop into the existing `core/taskflow` **TaskManager** as a
> replacement for the `core/workflow` interpreter. This **supersedes** the
> signal-driven re-run model in Principles 7, 9 and 15. See the
> **"Workflow model v2"** section immediately below — it is authoritative; those
> principles are kept as history.

---

## Workflow model v2 — execute → complete, declarative routing (current target)

The driver: the engine must replace the `core/workflow` interpreter **in place** —
keeping `core/taskflow`'s TaskManager and its plugins, swapping only the Temporal
workflow that walks the graph. That fixes the model.

### Task lifecycle: execute → (park → external input) → complete → advance

A task runs **once**. When it runs it either:

- **completes immediately** — returns its output; the engine advances; or
- **parks** — suspends to wait for outside input. Parking is an **open Temporal
  activity** (the task returns `activity.ErrResultPending`), *not* a workflow awaiting
  a signal. The outside world later **completes that activity by id** with the output,
  and the engine advances.

No re-run loop, no per-command re-invocation of the plugin. A parked task is simply an
activity that takes a long time to finish — Temporal's async activity completion
pattern, which is exactly what `TaskManager.TaskDone` drives. The plugin still decides
park-vs-advance **at runtime** (Principle 15's intent survives), but it decides **once,
at execute time**: return output (advance) or return pending (park).

### Routing is declarative — a command string per transition

A state no longer maps a plugin-emitted **outcome** to a target via a map. Each state
owns an **ordered list of transitions**, each guarded by a **command** string. When a
task completes it yields one **command**; the engine fires the first transition whose
command matches:

```
state.Transitions = [
  { command: "approve",         target: "end" },
  { command: "needs_more_info", target: "applicant_submission" },
]
```

- Matched by **string equality**, top to bottom; **first match wins**.
- An **empty command** is the default/always edge (a linear flow is one
  empty-command edge). At most one default edge per state; it sorts last.
- **No transitions ⇒ terminal**: the execution ends and returns the data bag.
- If the task's command matches no transition and there is no default ⇒ error.

We deliberately **start with a single-string command match, not expr-lang** — it's the
simplest thing that routes, needs no expression engine, and already covers the real
configs whose gateways are equality checks on one task-output value (e.g. `cda`'s
`verification_outcome == 'approve'` → the task simply completes with command
`"approve"`). A richer **`condition` (expr-lang over the data bag)** can be added as an
*alternative* transition guard later, for branches that key on accumulated variables or
non-equality tests. Until then that's a known gap vs. `core/workflow`.

> **Determinism note.** The old `map[Outcome]→target` made one-target-per-key
> structural. A command list cannot, so determinism becomes **first-match-wins + a
> `Validate()` check** (≤1 default edge, default edge sorts last).

### Where the command comes from

A task completes with `(command, data)`:

- **automatic task** — the plugin's `Run` returns the command directly (e.g.
  `http-call` returns `"done"`).
- **parked task** — the plugin parks (no completion yet); the external input that
  **completes** the task supplies the command (e.g. the reviewer's `TaskDone` carries
  `"approve"`). This is why command ≠ a synchronous return — it can arrive much later.

### Data namespacing — by state name, not by mapping

Rather than declare per-task `input_mapping` / `output_mapping` (as `core/workflow`
does), we namespace **by convention** using a key that is *already* deterministic: the
**state name** (`Validate` guarantees names are unique within a chart).

- **On completion**, a task's output is stored under its state name:
  `bag[stateName] = output`. No per-field mapping to declare.
- **On entry**, a task receives the whole namespaced bag and reads upstream data by
  path, e.g. `bag["officer_verification"].rejection_reason`.
- **On revisit** (loops), a state overwrites its own namespace with the latest attempt.

This replaces `output_mapping` entirely (namespacing was its real job) and removes
`input_mapping` (tasks read the bag by path). In our command-routed model the output
namespace is only for **carry-forward data** — routing is the completion command, not a
variable — so this is all the data plumbing the engine needs.

> **Tradeoff.** `input_mapping` also *decoupled* a plugin's local input name from the
> workflow variable name. The convention drops that indirection: a task that reads
> upstream data references the **upstream state name** directly. That's the same
> mental model the chart author already uses to wire transitions; a generic plugin
> that shouldn't hardcode a state name can name the path in its own `configRef`
> instead of as first-class chart mapping. Expr-lang conditions, if added later, read
> from this same `bag[stateName].field` structure.

### Integration shape: implement `engine.TemporalManager`

To swap into the TaskManager without touching it, the engine implements the
`engine.TemporalManager` contract (`StartWorkflow` / `TaskDone` / `TaskUpdate` /
`GetStatus` / `RegisterDefinitionHandler` / `StartWorker` / `StopWorker`), and the real
work is reached through the injected **`TaskActivationHandler`** callback — not a local
plugin registry. The two resume verbs map to the two caller intents:

| Caller intent                | TemporalManager call                | Effect                                     |
|------------------------------|-------------------------------------|--------------------------------------------|
| **submit** (done)            | `TaskDone` → `CompleteActivityByID` | completes the open task activity, advances |
| **save-draft** (stay parked) | `TaskUpdate`                        | persists/updates; task stays open          |

So `Command`'s "stay parked vs advance" (Principle 7) becomes the **structural**
distinction update-vs-done, instead of a runtime decision inside a re-run loop.

---

## Terminology

| Term               | Meaning                                                                                                                                                                                                                                                                        |
|--------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Chart**          | The static definition of a flow: its states, the plugin at each state, and the transitions between them. Pure data — nothing executes. Handed to the engine when an execution starts.                                                                                          |
| **State**          | One node in the chart. Names *which* plugin runs there and *where* that plugin's config is fetched from (`ConfigRef`). Does not hold a live plugin.                                                                                                                            |
| **Transition**     | A directed edge owned by a state: `{condition, target}`. After the state's task completes, the engine evaluates each transition's expr-lang **condition** over the data bag; the first true one fires (empty condition = always). *(v2; was an `On: {outcome → target}` map.)* |
| **Condition**      | An expr-lang boolean expression on the data bag that guards a transition (e.g. `verificationform.verification_outcome == 'approve'`). Empty = the default/always edge.                                                                                                         |
| **Mapping**        | Per-state **inputMapping** (data-bag var → task input) and **outputMapping** (task output → data-bag var). Keys may be dotted (nested) and end in `?` (optional). Applied by the runner around the task call.                                                                  |
| **Plugin**         | The unit of work that runs at a state (e.g. `http-call`, `UserForm`). Configured once at init time; runs over (its config, mapped inputs). Produces **output data** and either completes or parks. *(v2: no longer emits an `Outcome`.)*                                       |
| **Execution**      | One running instance of a chart. Has a unique **executionID**, a current state, and its own data. Independent from every other execution.                                                                                                                                      |
| **Engine**         | Owns and drives all live executions. Resolves `executionID → execution`, runs plugins, routes commands, advances state.                                                                                                                                                        |
| **Outcome**        | *(v2: removed as a plugin return value.)* Formerly a label a plugin emitted to pick the next state. Routing is now declarative — the engine evaluates transition **conditions** over the task's output.                                                                        |
| **Command**        | A verb a caller sends *in* to a parked execution, saying what they are doing (e.g. `submit`, `save-draft`), together with user data. Interpreted by the currently-parked plugin.                                                                                               |
| **Park**           | An execution suspends mid-flow to wait for outside input. It stays in its current state until a command resumes (and possibly advances) it.                                                                                                                                    |
| **ConfigRef**      | A reference stored in a state telling the engine where to fetch that plugin's config — used at state-entry time, not at chart-build time.                                                                                                                                      |
| **ExecutionState** | The plain, serializable snapshot of an execution that gets persisted: `ID`, `Status`, `Current`, `Data`, `Version`, and a write-once copy of the `Chart`. Excludes runtime-only things (locks, live plugins).                                                                  |
| **Status**         | An execution's lifecycle stage: `running`, `parked`, or `done`. Lets the store answer "which executions are parked awaiting a command?".                                                                                                                                       |
| **StateStore**     | Persists the current `ExecutionState` — one mutable, versioned record per execution. Interface; `Memory`/`SQLite`/`Postgres` implementations.                                                                                                                                  |
| **HistoryStore**   | Append-only log of everything that happened to executions (an event log). Immutable, write-once, ordered by `seq`. Interface; same set of implementations.                                                                                                                     |
| **Version**        | A counter on an `ExecutionState`, bumped on every step. Used for optimistic concurrency on save, and equals the latest history `seq` for that execution.                                                                                                                       |

---

## Principles

### 1. The chart is pure, static data
A chart only *describes* the flow: states, the plugin and config-reference at
each state, and transitions. It builds nothing and fetches nothing. A chart is
validated up front (`Chart.Validate()`) so a malformed flow is rejected before
any execution runs, never halfway through.

### 2. Plugins are configured once, at init — not by themselves at runtime
A plugin's own code never goes and fetches config while it runs. The **engine**
hands a plugin its config when it constructs the plugin. The plugin then runs
purely over (its config, the execution's data).

### 3. Plugins are initialized lazily, on first arrival at a state
We do not build plugins when the chart is assigned. At start time we don't know
which path an execution will take, so building every plugin up front would be
wasteful and some may never be reached. Instead, when an execution first arrives
at a state, the engine fetches that state's config (via `ConfigRef`) and
constructs the plugin **then**. (Responsibility split: the *engine* fetches +
inits at state entry; the *plugin* only executes.)

### 4. An execution is one instance; many run concurrently
Each execution is an independent instance with its own **executionID**, current
state, data, and its **own private copy of the chart** (see Principle 11). The
engine runs many at once; nothing is shared between executions, so they never
interfere.

### 5. The outside world addresses executions by ID only
Callers hold an **executionID** — nothing else. They do not know or reference the
current state or the live plugin. The engine resolves the ID internally to find
the parked execution and its current plugin.

### 6. Callers talk to parked executions via commands
Input to a parked execution is a **command** (a verb describing what the caller
is doing) plus user data. The engine routes the command to the currently-parked
plugin, which interprets it.

### 7. Command ≠ outcome
> **Superseded by Workflow model v2.** `Outcome` as a plugin return value is gone;
> routing is now declarative (conditions on transitions). The command distinction
> survives but becomes structural: *advance* = `TaskDone` (complete the activity),
> *stay parked* = `TaskUpdate`. Kept below as history.

These are distinct concepts and must not be conflated:

| | Command | Outcome |
|---|---|---|
| Direction | comes *in* from the caller | emitted *out* by the plugin |
| Example | `submit`, `save-draft` | `approved`, `done` |
| Drives a transition? | not directly | yes — chart maps it to `To` |

A command handler resolves to one of two results:
- **Stay parked** — e.g. `save-draft` persists data, emits no outcome, execution
  remains in the same state. (`UserForm`: caller can keep editing.)
- **Advance** — e.g. `submit` accepts the data and emits an outcome; the chart's
  transition fires and the execution moves on.

### 8. A parked plugin declares the commands it accepts
A plugin that parks must declare which commands are valid for it, so an unknown
or invalid command is rejected cleanly rather than mishandled. *(Mechanism to be
designed when we build parking.)*

### 9. Transitions are deterministic
> **Superseded by Workflow model v2.** Transitions are now a condition-ordered list,
> not an `On` map. Determinism becomes *first-match-wins + a `Validate()` check*
> (≤1 default edge, default sorts last) rather than structural. A state with no
> transitions is still **terminal**. Kept below as history.

Given a state and an outcome, the next state is unambiguous. This is guaranteed
*structurally*: a state's outgoing edges live in `On: {outcome → target}`, a map,
so two targets for the same outcome cannot even be expressed. (No runtime check
needed.) A state with no `On` entries is **terminal** — the execution ends there.

### 10. Durable state is separate from the runtime object
What we persist (`ExecutionState`) is a plain, serializable snapshot: `ID`,
`Status`, `Current`, `Data`, `Version`, and the chart copy. What we *don't*
persist: locks, the resolved/live plugin, anything runtime-only. The live
`Execution` is a thin wrapper around a loaded `ExecutionState` plus its mutex.
Plugins are never persisted — they are rebuilt lazily on state entry (Principle
3). One consequence: everything in `Data` must be serializable.

### 11. The engine owns the chart; each execution snapshots its own copy
An execution needs its chart for its entire (possibly very long) lifetime, so it
must not depend on an external or mutable source to resolve it. When a caller
starts an execution they hand in the chart (the "configuration chart"); the
engine validates it and **stores a write-once copy inside that execution's own
record**. The execution is then fully self-contained — recovery is a single read,
immune to later chart edits or deletions.

- The chart column is **write-once**: set at start, never updated. Only
  `Current`/`Data`/`Version` change as the execution runs.
- This makes chart *versioning* unnecessary: a private copy can never change
  underneath a running execution.
- We knowingly accept the **duplication** (chart JSON copied per execution) in
  exchange for isolation and simplicity. Charts are small and executions are
  long-lived, so it's a good trade. Revisit only if charts grow large *and*
  execution volume is huge.
- A central chart *catalog* (listing/managing templates) is an **authoring**
  concern, not runtime. If we ever want it, callers fetch a template *from* it
  and still hand the chart to start — the "execution owns its chart" property is
  untouched. No corner painted.

**Chart vs. config.** We do not ban external dependencies outright — `ConfigRef`
(Principles 2–3) is fetched externally and lazily. The difference is blast
radius: the chart is needed the whole lifetime (own it), whereas a config fetch
is needed only when entering one state, and its failure is local and retryable.

### 12. Persistence is a two-store seam
Two stores, because current state and history have opposite shapes:

|              | StateStore                          | HistoryStore                     |
|--------------|-------------------------------------|----------------------------------|
| Holds        | current snapshot, one per execution | append-only event log            |
| Access       | read + update by ID                 | append + read by execution       |
| Mutability   | mutable, overwritten each step      | immutable, write-once            |
| Concurrency  | optimistic versioning (`Version`)   | none — appends never contend     |
| Growth       | bounded (one row per live exec)     | unbounded (archivable)           |

Both are interfaces in the `fsm` package; `Memory*` now, `SQLite*`/`Postgres*`
later — the engine code does not change when a store is swapped. Ideally both
sit behind **one transactional backend** so a step's state-update and
history-append commit together (atomicity). If they ever live in different
backends, cross-store atomicity is lost and needs an outbox pattern.

### 13. History is an event log, not just transitions
The history store records meaningful events even when the execution does **not**
move — e.g. a `save-draft` command (Principle 7) keeps the execution parked but
is still a real thing a user did. An entry looks like:
`{ executionID, seq, timestamp, eventType, from→to, outcome | command, actor, data snapshot/diff }`,
covering: started, entered-state, plugin-ran, command-received, parked, resumed,
completed, errored.

- **Invariant:** an execution's `Version` equals its latest history `seq`. Each
  step bumps the version and appends exactly one entry, so the two stores are
  cross-checkable.
- Built **audit-first** (state store authoritative, history is the trail), but
  entries are shaped so history *could* become the source of truth (event
  sourcing / replay) later without a rewrite.
- **Atomicity:** when entering a parked state, the state-update + history-append
  must be durable **before** acknowledging the caller, so a crash can't lose the
  fact that the execution parked.

### 14. The runtime is pluggable
How an execution is *driven and made durable* sits behind a `Runtime` interface.
Our own in-process engine is one implementation (`LocalRuntime`); a Temporal-
backed engine (`TemporalRuntime`) is another, addable later without changing
caller code or the domain. The seam is placed at the **orchestrator** altitude,
not at storage:

- **Runtime-agnostic (the stable core):** the `Chart` (pure data), `executionID`
  addressing, the command-vs-outcome model, and the **plugin contract**.
- **Per-runtime (below the interface):** persistence, concurrency, retries,
  timers, transactionality. The `StateStore`/`HistoryStore` seam (Piece 3.5) is a
  `LocalRuntime` concern — Temporal owns its own durable state + event history and
  must NOT be forced onto our stores.

Two constraints this imposes, both of which we already wanted:

- **Plugins must be activity-compatible:** inputs in → outcome + data out, all
  I/O at the edges, no reliance on shared in-process memory. This is exactly the
  Principle 2/3 shape, and it is what lets a plugin run as a direct call locally
  *or* as a Temporal activity.
- **The API is async-first:** `Start` returns an id; results/state are observed
  via query/await. Temporal is inherently durable/async and can't honor a
  "run synchronously and return the answer" API; Local can still implement things
  synchronously underneath.

Decided: build the pluggable seam (not an all-in replacement); when Temporal is
wired up, it runs against a **local dev server** (`temporal server start-dev`).
*(Superseded: we later went all-in on Temporal — see the banner at the top.)*

### 15. Park-vs-advance is a runtime plugin result, not a static kind
> **Refined by Workflow model v2.** The *intent* survives — park-vs-advance is still a
> runtime decision, not a static kind. What changes: the plugin decides **once, at
> execute time** (return output ⇒ advance; return `ErrResultPending` ⇒ park), and the
> resume happens via async activity completion (`TaskDone`), **not** by re-running the
> step for each command. The re-run loop below is gone. Kept as history.

A plugin is **not** classified as automatic or interactive ahead of time. Whether
to continue or to park is decided **when the plugin runs**, because it is often
data- and config-dependent (e.g. auto-approve when `amount < threshold`, else park
for a manager). So a plugin **step** returns one of:

- **Advance(outcome, data)** — done; the engine follows `chart.On[outcome]` and
  carries the returned data forward.
- **Park** — needs input; the engine suspends and waits for a command.

"Automatic" is just a plugin that never parks; "interactive" is one that parks
then advances once a command satisfies it — same code path. The engine re-runs
the plugin step (as an activity) for **each command** while parked, so the plugin
can do I/O (persist a draft → Park again) or validate-and-advance (submit →
Advance) — and can reject unknown commands itself. Because the workflow reacts
only to the activity *result* (in history), this needs no static kind map and
imposes no cross-worker registration constraint. This supersedes the earlier
`PluginKind` / `Automatic()` / `Interactive()` split. It also subsumes Principle 8:
accepted-commands are enforced at runtime by the plugin (and may optionally be
surfaced via the query for UIs).

---

## Runtimes

The public API delegates to a `Runtime`, roughly:

- `Start(chart, input) → executionID`
- `SendCommand(executionID, command, data)` — park/resume
- `GetState(executionID)` — inspect without mutating
- `Await/Subscribe(executionID)` — observe outcomes/completion

### How our model maps onto Temporal

| Our concept                     | Temporal equivalent                                  |
|---------------------------------|------------------------------------------------------|
| Execution (`executionID`)       | Workflow Execution (`workflowID`)                    |
| Engine (registry + driver)      | Temporal server + worker fleet                       |
| StateStore + HistoryStore       | Temporal event history (durable, replayable)         |
| Plugin (work + config fetch)    | Activity (with retries/timeouts)                     |
| Parking for user input          | `workflow.Await` on a Signal                         |
| Command (`submit`/`save-draft`) | Signal sent to the workflow                          |
| Inspect current state by id     | Query                                                |
| Chart (handed in at start)      | Workflow **input** — Temporal records it in history, |
|                                 | giving Principle 11's write-once snapshot for free   |
| Outcome-driven transition       | interpreter-workflow control flow                    |

A `TemporalRuntime` is one generic **interpreter workflow** that walks the chart
(deterministic map lookups), runs each state's plugin as an **activity**, and
`Await`s a signal at parking states. Costs to accept: a running Temporal cluster
(infra dependency), determinism discipline (no I/O in workflow code), and
workflow versioning for in-flight executions.

### The Engine (dependency-injected) — implemented

The runtime is an **`Engine` struct** (engine.go) built with `New(opts...)` and
injected dependencies, rather than free functions + a package-level table. It has
two faces:

- **Worker side:** holds the **plugin registry** (`Register(name, Plugin)`) and a
  **`ConfigFetcher`**. `RegisterWorker(w)` wires the workflow + the generic
  plugin-runner activity (`RunPlugin`) onto a Temporal worker. `RunPlugin` is the
  one place I/O lives: fetch the state's config, resolve the registered plugin,
  **inject the config**, run it (Principles 2–3 — plugins never fetch their own
  config).
- **Client side:** holds the Temporal `client.Client` and exposes `Start` /
  `SendCommand` / `GetState` — the by-id front door (Principle 5).

The workflow is a **method** (`(*Engine).ExecutionWorkflow`) so it can reach the
engine, but it does **no I/O and touches no live dependency**, so it stays
deterministic.

**Park-vs-advance is a runtime plugin result, not a static kind** (Principle 15).
There is no `Automatic`/`Interactive` classification. A plugin step runs (in the
`RunPlugin` activity) and returns either *Advance(outcome, data)* or *Park*. An
"automatic" plugin is simply one that never parks; an "interactive" one parks,
then advances once a command satisfies it. The workflow reacts only to these
activity *results* (recorded in history → deterministic), so there is no static
kind map and no "every worker must register identical kinds" constraint.

---

## Build roadmap

History first, then where we landed.

**Phase A — local engine (built, then replaced).** We built these to understand
the internals, then pivoted to Temporal. The code is removed; the design notes
remain above as history.

1. **Chart** — static state graph + validation. *(done — kept; chart.go)*
2. **Execution** — one instance walking a chart by hand. *(done, then removed)*
3. **Engine** — registry of executions by ID + concurrency. *(done, then removed)*
3.5. **Store seam** — `StateStore`/`HistoryStore`, write-once chart copy,
   `Version`. *(designed, not built — superseded by Temporal's event history)*

**Phase B — Temporal runtime (current implementation).** Temporal replaced the
local engine wholesale.

- **T1. Engine + interpreter workflow** *(done)* — `Engine` (engine.go) is the
  DI core; `(*Engine).ExecutionWorkflow` (workflow.go) is one generic,
  deterministic workflow that walks ANY chart passed as input. Automatic states
  run the plugin-runner **activity**; interactive states **park** on a signal.
  Current state is exposed via a **query**.
- **T2. Plugin runner activity** *(done)* — `(*Engine).RunPlugin` (engine.go):
  the generic runner — fetch config via the injected `ConfigFetcher`, resolve the
  registered plugin, inject config, run it. Sample plugins are registered in
  `cmd/worker` via `Automatic(...)` / `Interactive()`; real implementations are
  the next work.
- **T3. Worker + starter** *(done)* — `cmd/worker` builds the engine, registers
  plugins, and wires it onto a worker; `cmd/starter` uses the engine's client API
  (`Start`/`SendCommand`/`GetState`). Verified end to end against a local Temporal.

**What's next on Temporal:**
- **T4. Runtime park-vs-advance (Principle 15)** *(done)* — replaced the static
  `PluginKind`/`Automatic()`/`Interactive()` split with a single `Plugin.Step`
  returning `StepResult` (*Advance(outcome)* or *Park*, with optional `Data`).
  The workflow does an entry-run, then re-runs the step (as the RunPlugin
  activity) for each command while parked. Plugins are registered via
  `PluginFunc`. Removed the kind map and its determinism constraint. Verified by
  the `form-review` test and live end to end.

- **T5. Workflow model v2 — drop-in for `core/workflow`** *(in progress)* — adopt the
  *execute → (park → external input) → complete → advance* lifecycle with
  **declarative, condition-based routing** and **namespaced input/output mapping**, and
  implement the `engine.TemporalManager` contract so the engine replaces the
  `core/workflow` interpreter inside the existing `core/taskflow` TaskManager (its
  plugins reached via the injected `TaskActivationHandler`). See the **"Workflow model
  v2"** section. Sub-steps: **(a)** chart shape — `Transition{Command,Target}` +
  `Validate()` for first-match-wins *(this cut)*; **(b)** interpreter walks states,
  runs the task once, parks via `ErrResultPending`, routes on the completion command
  *(this cut)*; **(c)** client resume API — `Complete(executionID, taskID, result)`
  via `CompleteActivityByID`, current state/taskID via query *(this cut)*; **(d)** data
  namespacing by state name — no `input_mapping`/`output_mapping`, `bag[stateName] =
  output` *(this cut)*; **(e)** expr-lang `condition` guards as an alternative to a
  command match *(later, optional)*; **(f)** `engine.TemporalManager` facade + injected
  `TaskActivationHandler` to drop into the core TaskManager *(later, cross-module)*.

**What's next:**
- Activity retry/timeout policies (a parked task needs a long `ScheduleToClose` or
  heartbeating, since it may wait days); workflow versioning for in-flight executions.

**Running it locally:**
```
temporal server start-dev      # or use an existing Temporal on :7233
go run ./cmd/worker            # in one terminal
go run ./cmd/starter           # in another
```
Tests use Temporal's in-process test framework (`workflow_test.go`) — no server
needed: `go test ./...`.

---

## Type model (current)

Decisions locked in for now; all revisitable.

- States and events are **readable strings** (`StateName`, `Outcome`), not ints
  or generics — easiest to read, log, and learn from.
- The package is a **library** named `fsm` (no `main`); behavior is exercised via
  tests.
- **Concurrency:** a `RWMutex` on the engine guards the id→execution registry map
  only (held briefly). Each `Execution` carries its own `sync.Mutex` that
  serializes operations on that one instance. So different executions never block
  each other; only same-id operations serialize. The actor-per-execution model
  (one goroutine + channel each) is a possible later evolution.
- **Persistence:** the chart is stored **per execution** as a write-once copy
  inside the `StateStore` record (Option 2) — no separate chart store for now.
  Two store interfaces (`StateStore`, `HistoryStore`), in-memory first, SQL
  later, ideally one transactional backend. Cross-process safety comes from the
  `Version` field (optimistic concurrency), not the in-process mutex.