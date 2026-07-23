package fsm

import "testing"

// sampleChart builds a small valid chart we can reuse in tests:
//
//	fetch ──done──▶ approve ──approved──▶ finish
//	                   │
//	                   └────rejected────▶ finish
func sampleChart() Chart {
	return Chart{
		SchemaVersion: "v1",
		Initial:       "fetch",
		States: []State{
			{Name: "fetch", Plugin: "http-call", ConfigRef: "config/fetch",
				Transitions: []Transition{{Command: "done", Target: "approve"}}},
			{Name: "approve", Plugin: "await-approval", ConfigRef: "config/approve",
				Transitions: []Transition{
					{Command: "approved", Target: "finish"},
					{Command: "rejected", Target: "finish"},
				}},
			{Name: "finish", End: true},
		},
	}
}

// TestValidate_OK proves a well-formed chart passes validation.
func TestValidate_OK(t *testing.T) {
	if err := sampleChart().Validate(); err != nil {
		t.Fatalf("expected valid chart, got error: %v", err)
	}
}

// TestValidate_Errors checks each rule rejects the charts it should. Each case
// starts from a valid chart and breaks exactly one thing.
func TestValidate_Errors(t *testing.T) {
	cases := []struct {
		name   string
		break_ func(c *Chart)
	}{
		{"missing schemaVersion", func(c *Chart) { c.SchemaVersion = "" }},
		{"unknown schemaVersion", func(c *Chart) { c.SchemaVersion = "v99" }},
		{"no initial state", func(c *Chart) { c.Initial = "" }},
		{"initial not defined", func(c *Chart) { c.Initial = "ghost" }},
		{"empty state name", func(c *Chart) { c.States[0].Name = "" }},
		{"duplicate state", func(c *Chart) { c.States[1].Name = "fetch" }},
		{"state without plugin", func(c *Chart) { c.States[0].Plugin = "" }},
		{"transition to unknown", func(c *Chart) {
			c.States[0].Transitions = []Transition{{Command: "done", Target: "ghost"}}
		}},
		{"duplicate command", func(c *Chart) {
			c.States[1].Transitions = []Transition{
				{Command: "approved", Target: "finish"},
				{Command: "approved", Target: "finish"},
			}
		}},
		{"default not last", func(c *Chart) {
			c.States[1].Transitions = []Transition{
				{Command: "", Target: "finish"},
				{Command: "approved", Target: "finish"},
			}
		}},
		{"unwired non-end state", func(c *Chart) { c.States[1].Transitions = nil }},
		{"end state with transitions", func(c *Chart) {
			c.States[2].Transitions = []Transition{{Command: "done", Target: "finish"}}
		}},
		{"no end state", func(c *Chart) {
			c.States[2].End = false
			c.States[2].Transitions = []Transition{{Command: "loop", Target: "finish"}}
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := sampleChart()
			tc.break_(&c)
			if err := c.Validate(); err == nil {
				t.Fatalf("expected validation error for %q, got nil", tc.name)
			}
		})
	}
}

// TestCheckInputs covers required-present, required-missing, and optional-absent.
func TestCheckInputs(t *testing.T) {
	c := Chart{Inputs: map[string]string{
		"leave.applicant_id": "required",
		"leave.department?":  "optional",
	}}

	// Required present, optional absent → ok.
	if err := c.CheckInputs(Data{"leave": map[string]any{"applicant_id": "E1"}}); err != nil {
		t.Fatalf("expected ok, got %v", err)
	}
	// Required missing → error.
	if err := c.CheckInputs(Data{}); err == nil {
		t.Fatal("expected error for missing required input")
	}
	// No declared inputs → nothing to satisfy.
	if err := (Chart{}).CheckInputs(nil); err != nil {
		t.Fatalf("empty inputs should pass, got %v", err)
	}
}

// TestValidate_EmptyInputName rejects an input declaration with no path.
func TestValidate_EmptyInputName(t *testing.T) {
	c := sampleChart()
	c.Inputs = map[string]string{"?": "no path"}
	if err := c.Validate(); err == nil {
		t.Fatal("expected error for empty input name")
	}
}

// TestRoute covers first-match-wins and the default (empty-command) edge.
func TestRoute(t *testing.T) {
	s := State{
		Name: "s",
		Transitions: []Transition{
			{Command: "approved", Target: "yes"},
			{Command: "", Target: "fallback"}, // default, must be last
		},
	}
	if got, ok := s.route("approved"); !ok || got.Target != "yes" {
		t.Fatalf("route(approved) = %q,%v; want yes,true", got.Target, ok)
	}
	// Any unrecognized command falls through to the default edge.
	if got, ok := s.route("anything-else"); !ok || got.Target != "fallback" {
		t.Fatalf("route(anything-else) = %q,%v; want fallback,true", got.Target, ok)
	}

	// Without a default edge, an unmatched command does not route.
	noDefault := State{Transitions: []Transition{{Command: "done", Target: "end"}}}
	if _, ok := noDefault.route("nope"); ok {
		t.Fatal("route(nope) matched but should not have")
	}
}
