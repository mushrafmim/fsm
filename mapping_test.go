package fsm

import "testing"

func TestGetSetPath(t *testing.T) {
	bag := Data{}
	setPath(bag, "leave.days", 3)
	setPath(bag, "leave.reason", "vacation")

	if v, ok := getPath(bag, "leave.days"); !ok || v != 3 {
		t.Fatalf("get leave.days = %v,%v; want 3", v, ok)
	}
	if v, ok := getPath(bag, "leave.reason"); !ok || v != "vacation" {
		t.Fatalf("get leave.reason = %v,%v; want vacation", v, ok)
	}
	if _, ok := getPath(bag, "leave.missing"); ok {
		t.Error("get leave.missing should be absent")
	}
	if _, ok := getPath(bag, "leave.days.nope"); ok {
		t.Error("descending into a scalar should be absent, not panic")
	}
}

// TestApplyInput covers rename, dotted paths, and the "?" optional skip.
func TestApplyInput(t *testing.T) {
	global := Data{"leave": map[string]any{"days": 3, "reason": "vacation"}}

	local, err := applyInput(global, map[string]string{
		"leave.days":              "days",             // rename global→local
		"leave.reason":            "form.reason",      // into a nested local path
		"leave.rejection_reason?": "rejection_reason", // optional, absent → skipped
	})
	if err != nil {
		t.Fatalf("applyInput errored: %v", err)
	}
	if local["days"] != 3 {
		t.Errorf("local days = %v; want 3", local["days"])
	}
	if v, ok := getPath(local, "form.reason"); !ok || v != "vacation" {
		t.Errorf("local form.reason = %v,%v; want vacation", v, ok)
	}
	if _, ok := local["rejection_reason"]; ok {
		t.Error("optional-absent input should be skipped, not set")
	}
}

// TestApplyInput_RequiredMissing: a non-optional source that is absent errors.
func TestApplyInput_RequiredMissing(t *testing.T) {
	_, err := applyInput(Data{}, map[string]string{"leave.days": "days"})
	if err == nil {
		t.Fatal("expected error for missing required input")
	}
}

// TestApplyWrites covers local→global export, dotted destination, and "?" skip.
func TestApplyWrites(t *testing.T) {
	global := Data{}
	local := Data{"decision": "approved"} // no "comment" present

	err := applyWrites(local, map[string]string{
		"decision": "leave.decision",
		"comment?": "leave.manager_comment", // optional, absent → skipped
	}, global)
	if err != nil {
		t.Fatalf("applyWrites errored: %v", err)
	}
	if v, ok := getPath(global, "leave.decision"); !ok || v != "approved" {
		t.Errorf("global leave.decision = %v,%v; want approved", v, ok)
	}
	if _, ok := getPath(global, "leave.manager_comment"); ok {
		t.Error("optional-absent write should be skipped")
	}
}

// TestApplyWrites_RequiredMissing: a non-optional local output that is absent errors.
func TestApplyWrites_RequiredMissing(t *testing.T) {
	err := applyWrites(Data{}, map[string]string{"decision": "leave.decision"}, Data{})
	if err == nil {
		t.Fatal("expected error for missing required write source")
	}
}
