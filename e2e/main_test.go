package main

import "testing"

func TestRunIDRe(t *testing.T) {
	cases := map[string]bool{
		// valid: lowercase, digits, dashes, underscores
		"20260808-173000": true,
		"run1":            true,
		"my_run-id-2":     true,
		"abc":             true,
		// invalid: path traversal / dots / uppercase / leading dash / empty / separators
		"../../x":    false,
		".hidden":    false,
		"run.id":     false,
		"RunID":      false,
		"-leading":   false,
		"":           false,
		"with/slash": false,
	}
	for in, want := range cases {
		if got := runIDRe.MatchString(in); got != want {
			t.Errorf("runIDRe.MatchString(%q) = %v, want %v", in, got, want)
		}
	}
}
