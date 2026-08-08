package main

import (
	"strings"
	"testing"
)

func TestParseContainerVersion(t *testing.T) {
	cases := map[string]string{
		"container CLI version 1.2.0 (build: release, commit: 6e65319)": "1.2.0",
		"container CLI version 2.0.1 (build: release)":                  "2.0.1",
		"garbage with no version":                                       "",
	}
	for in, want := range cases {
		if got := parseContainerVersion(in); got != want {
			t.Errorf("parseContainerVersion(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestCheckContainerVersion(t *testing.T) {
	if err := checkContainerVersion(pinnedContainerVersion); err != nil {
		t.Errorf("matching version returned error: %v", err)
	}
	err := checkContainerVersion("1.1.0")
	if err == nil {
		t.Fatal("mismatched version did not return an error")
	}
	// Actionable text: names both versions and tells the user how to resolve it.
	msg := err.Error()
	for _, want := range []string{"1.1.0", pinnedContainerVersion, "pin"} {
		if !strings.Contains(msg, want) {
			t.Errorf("mismatch error not actionable: %q missing %q", msg, want)
		}
	}
}

func TestStripCIDR(t *testing.T) {
	if got := stripCIDR("192.168.64.2/24"); got != "192.168.64.2" {
		t.Errorf("stripCIDR with prefix = %q, want 192.168.64.2", got)
	}
	if got := stripCIDR("10.0.0.5"); got != "10.0.0.5" {
		t.Errorf("stripCIDR without prefix = %q, want 10.0.0.5", got)
	}
}

func TestStatusRunning(t *testing.T) {
	out := "FIELD              VALUE\nstatus             running\napiserver.version  container-apiserver version 1.2.0\n"
	if !statusRunning(out) {
		t.Error("statusRunning should be true for a running report")
	}
	if statusRunning("FIELD VALUE\nstatus stopped\n") {
		t.Error("statusRunning should be false when not running")
	}
}
