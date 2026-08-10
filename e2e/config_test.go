package main

import (
	"net"
	"strconv"
	"testing"
)

func TestResolveAPIPort(t *testing.T) {
	// "" is a no-op: config.yaml's value is passed through untouched.
	cfg := publishConfig{"api": "127.0.0.1:10888"}
	got, err := resolveAPIPort(cfg, "")
	if err != nil || got["api"] != "127.0.0.1:10888" {
		t.Fatalf("empty flag: got %q err %v, want 127.0.0.1:10888 unchanged", got["api"], err)
	}

	// An explicit port overrides the host port and keeps the configured host IP.
	got, err = resolveAPIPort(publishConfig{"api": "127.0.0.1:10888"}, "10889")
	if err != nil || got["api"] != "127.0.0.1:10889" {
		t.Fatalf("explicit port: got %q err %v, want 127.0.0.1:10889", got["api"], err)
	}

	// With no prior publish entry the host defaults to the loopback.
	got, err = resolveAPIPort(publishConfig{}, "10890")
	if err != nil || got["api"] != "127.0.0.1:10890" {
		t.Fatalf("no prior host: got %q err %v, want 127.0.0.1:10890", got["api"], err)
	}

	// Invalid values are rejected (only "", "auto", or 1-65535 are accepted).
	for _, bad := range []string{"abc", "0", "-1", "70000", "1.5"} {
		if _, err := resolveAPIPort(publishConfig{"api": "127.0.0.1:10888"}, bad); err == nil {
			t.Errorf("flag %q: want error, got nil", bad)
		}
	}

	// "auto" grabs a free loopback port; the result is a well-formed host:port and
	// the port is in range. (The listener is closed before returning, so this only
	// checks the shape — a concurrent binder could in theory take it between the
	// close and the api binding it, which is the same benign TOCTOU any ":0" pick
	// has and not something a unit test can pin down.)
	got, err = resolveAPIPort(publishConfig{"api": "127.0.0.1:10888"}, "auto")
	if err != nil {
		t.Fatalf("auto: unexpected error: %v", err)
	}
	host, portStr, err := net.SplitHostPort(got["api"])
	if err != nil {
		t.Fatalf("auto: %q is not host:port: %v", got["api"], err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || host != "127.0.0.1" || port < 1 || port > 65535 {
		t.Errorf("auto: got %q (host=%q port=%d err=%v), want 127.0.0.1:<1-65535>", got["api"], host, port, err)
	}
}
