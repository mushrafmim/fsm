package fsm

import (
	"fmt"
	"strings"
)

// ----------------------------------------------------------------------------
// Data mapping between the global bag and a task's local scope.
//
// The execution carries ONE global variable bag (authors namespace inside it by
// convention, e.g. everything under "leave"). A task never touches the global
// bag directly:
//
//   - input (global → local): before the task runs, State.Input remaps selected
//     global paths into the task's own local names.
//   - writes (local → global): after the task completes on a command, that
//     transition's Writes remaps selected local outputs back into global paths.
//
// Every mapping entry reads "<source>": "<destination>". A trailing "?" on the
// source key marks it optional (absent ⇒ skip, no error). Dotted keys are nested
// paths on either side.
// ----------------------------------------------------------------------------

// parseMapKey splits a mapping key into its dotted path and an optional flag. A
// trailing "?" means "skip if the source is absent" rather than erroring.
func parseMapKey(key string) (path string, optional bool) {
	if strings.HasSuffix(key, "?") {
		return strings.TrimSuffix(key, "?"), true
	}
	return key, false
}

// asMap coerces a bag value to a string-keyed map, accepting both Data and the
// plain map[string]any that values become after passing through Temporal's data
// converter.
func asMap(v any) (map[string]any, bool) {
	switch m := v.(type) {
	case map[string]any:
		return m, true
	case Data:
		return map[string]any(m), true
	}
	return nil, false
}

// getPath reads a dotted path (e.g. "leave.days") out of a nested bag, returning
// the value and whether it was present.
func getPath(bag Data, path string) (any, bool) {
	var cur any = map[string]any(bag)
	for _, p := range strings.Split(path, ".") {
		m, ok := asMap(cur)
		if !ok {
			return nil, false
		}
		cur, ok = m[p]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}

// setPath writes val at a dotted path, creating intermediate maps as needed.
func setPath(bag Data, path string, val any) {
	parts := strings.Split(path, ".")
	m := map[string]any(bag)
	for _, p := range parts[:len(parts)-1] {
		next, ok := asMap(m[p])
		if !ok {
			next = map[string]any{}
			m[p] = next
		}
		m = next
	}
	m[parts[len(parts)-1]] = val
}

// applyInput builds a task's local input bag from the global bag per the state's
// input map {globalPath: localPath}. A "?" on the global key makes it optional.
func applyInput(global Data, input map[string]string) (Data, error) {
	local := Data{}
	for globalKey, localPath := range input {
		src, optional := parseMapKey(globalKey)
		v, ok := getPath(global, src)
		if !ok {
			if optional {
				continue
			}
			return nil, fmt.Errorf("input %q not found in global state", src)
		}
		setPath(local, localPath, v)
	}
	return local, nil
}

// applyWrites merges a task's selected local outputs into the global bag per the
// fired transition's writes map {localPath: globalPath}. A "?" on the local key
// makes it optional.
func applyWrites(local Data, writes map[string]string, global Data) error {
	for localKey, globalPath := range writes {
		src, optional := parseMapKey(localKey)
		v, ok := getPath(local, src)
		if !ok {
			if optional {
				continue
			}
			return fmt.Errorf("write source %q not found in task output", src)
		}
		setPath(global, globalPath, v)
	}
	return nil
}
