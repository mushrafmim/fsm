package fsm

import "fmt"

// ----------------------------------------------------------------------------
// Piece 1: the Chart
//
// The Chart is the *static* definition of the whole FSM. It is pure data:
// it names the states, says which plugin sits at each state and where that
// plugin's config can be fetched, and describes how control flows from one
// state to the next.
//
// Nothing here executes. No plugins are built, no config is fetched. That all
// happens later, at runtime, when an Execution actually walks the chart.
// ----------------------------------------------------------------------------

// StateName is the unique name of a state within a chart, e.g. "await-approval".
// We use a named string (instead of a bare string) so the type system reminds
// us what a value means. It's trivial to swap for ints/generics later.
type StateName string

// Transition is one outgoing edge of a state (DESIGN "Workflow model v2"). When
// the state's task completes it yields a single **command** string; the engine
// fires the first transition whose Command equals it.
//
//   - Command: the command this edge fires on, matched by string equality. An
//     empty Command is the **default** edge — it always matches, so a linear
//     hop is just one empty-command transition.
//   - Target: the state to move to when this edge fires.
//
// We deliberately start with a single-string match rather than an expr-lang
// condition: it is the simplest thing that routes and already covers configs
// whose gateways are equality checks on one task-output value. A richer
// expr-lang guard can be added later as an alternative to Command.
type Transition struct {
	Command string    `json:"command,omitempty"`
	Target  StateName `json:"target"`
}

// State is one node in the chart. It does NOT hold a live plugin — only the
// information needed to build one later:
//
//   - Plugin: which *kind* of plugin runs here (e.g. "http-call").
//   - ConfigRef: where the engine fetches this plugin's config when the
//     execution first arrives at this state. The chart only stores the
//     reference; the actual fetch + init is a runtime concern.
//   - Transitions: this state's outgoing edges, evaluated **in order** after the
//     task completes — first matching command wins (see Transition).
//   - End: marks this as a **terminal** state — the execution ends here. Terminal
//     status is *declared*, not inferred from "no transitions", so a state the
//     author forgot to wire is a validation error rather than a silent dead end.
//     An end state runs no task, so it needs no Plugin and must have no
//     transitions.
type State struct {
	Name        StateName    `json:"name"`
	Plugin      string       `json:"plugin,omitempty"`
	ConfigRef   string       `json:"configRef,omitempty"`
	Transitions []Transition `json:"transitions,omitempty"`
	End         bool         `json:"end,omitempty"`
}

// Chart is the static definition of the whole FSM: the starting state and every
// state. Transitions are not a separate list — each state carries its own
// outgoing edges in State.Transitions.
type Chart struct {
	Initial StateName `json:"initial"`
	States  []State   `json:"states"`
}

// Validate checks that the chart is well-formed enough to be run. A malformed
// chart should be rejected here, up front, rather than blowing up halfway
// through an execution. We check:
//
//  1. There is an initial state.
//  2. No two states share a name.
//  3. Every non-end state declares which plugin it runs. (End states run no
//     task, so they need no plugin.)
//  4. The initial state actually exists.
//  5. Terminality is declared: an end state has no transitions; a non-end state
//     has at least one — so a state the author forgot to wire is caught here,
//     rather than silently behaving like a terminal. The chart has ≥1 end state.
//  6. Every outgoing transition points at a state that exists.
//  7. Routing is deterministic: a state has at most one default (empty-command)
//     edge, no duplicate commands, and the default (if any) is listed last.
//
// Points 5 & 7 are what replace the old map's structural guarantees now that
// transitions are an ordered list and terminals are explicit.
func (c Chart) Validate() error {
	if c.Initial == "" {
		return fmt.Errorf("chart has no initial state")
	}

	// Build a set of state names so we can (a) detect duplicates and
	// (b) check that transitions reference states that exist.
	known := make(map[StateName]bool, len(c.States))
	hasEnd := false
	for _, s := range c.States {
		if s.Name == "" {
			return fmt.Errorf("found a state with an empty name")
		}
		if known[s.Name] {
			return fmt.Errorf("duplicate state name: %q", s.Name)
		}
		if !s.End && s.Plugin == "" {
			return fmt.Errorf("state %q declares no plugin", s.Name)
		}
		if s.End {
			hasEnd = true
		}
		known[s.Name] = true
	}

	if !known[c.Initial] {
		return fmt.Errorf("initial state %q is not defined", c.Initial)
	}
	if !hasEnd {
		return fmt.Errorf("chart has no end state (mark a terminal state with \"end\": true)")
	}

	for _, s := range c.States {
		// Declared terminality: end ⇒ no transitions; non-end ⇒ at least one.
		if s.End {
			if len(s.Transitions) > 0 {
				return fmt.Errorf("end state %q must have no transitions", s.Name)
			}
			continue
		}
		if len(s.Transitions) == 0 {
			return fmt.Errorf("state %q has no outgoing transitions; wire it, or mark it \"end\": true if terminal", s.Name)
		}

		seen := make(map[string]bool, len(s.Transitions))
		for i, t := range s.Transitions {
			if !known[t.Target] {
				return fmt.Errorf("state %q transitions on command %q to unknown state %q",
					s.Name, t.Command, t.Target)
			}
			if seen[t.Command] {
				return fmt.Errorf("state %q has duplicate transition for command %q", s.Name, t.Command)
			}
			seen[t.Command] = true
			// A default (empty-command) edge must be last, so it acts as the
			// fallthrough after every specific command has had its chance.
			if t.Command == "" && i != len(s.Transitions)-1 {
				return fmt.Errorf("state %q: default (empty-command) transition must be listed last", s.Name)
			}
		}
	}

	return nil
}

// findState returns the definition of the named state, or false if the chart
// has no such state. It's a linear scan over States, which is fine for charts
// of the sizes we expect; if that ever changes we can build an index.
func (c Chart) findState(name StateName) (State, bool) {
	for _, s := range c.States {
		if s.Name == name {
			return s, true
		}
	}
	return State{}, false
}

// route picks the next state for a completed task's command. It returns the
// target of the first transition whose Command matches (the empty-command edge
// always matches, and Validate guarantees it is last), or false if nothing
// matches — which the caller treats as an error.
func (s State) route(command string) (StateName, bool) {
	for _, t := range s.Transitions {
		if t.Command == command || t.Command == "" {
			return t.Target, true
		}
	}
	return "", false
}
