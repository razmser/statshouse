package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// publishConfig maps a service name ("api") to the host-side publish address
// parsed from e2e/config.yaml (e.g. "127.0.0.1:10888"). The harness appends the
// service's container port to form the full -p publish spec
// ("127.0.0.1:10888:10888"). Per spec §2/§6, published ports come ONLY from this
// file; the default map publishes the api alone.
type publishConfig map[string]string

// loadConfig parses e2e/config.yaml with a minimal hand-rolled reader. The
// schema is a single top-level `publish:` map of `name: "host:port"` entries;
// no YAML dependency is added (gopkg.in/yaml.v2 is already in go.mod, but the
// schema is two lines and a bespoke parser keeps the e2e package self-contained
// and tolerant of the committed file's comments).
func loadConfig(path string) (publishConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	cfg := publishConfig{}
	inPublish := false
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		raw := strings.TrimRight(sc.Text(), "\r")
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		// A line with no leading whitespace is a top-level key: it opens a new
		// section. We only consume the `publish:` section.
		indented := raw[0] == ' ' || raw[0] == '\t'
		if !indented {
			section := strings.TrimSpace(strings.TrimSuffix(trimmed, ":"))
			inPublish = strings.HasSuffix(trimmed, ":") && section == "publish"
			continue
		}
		if !inPublish {
			continue
		}
		line := stripInlineComment(raw)
		key, val, ok := splitColon(line)
		if !ok {
			continue
		}
		cfg[strings.TrimSpace(key)] = unquote(strings.TrimSpace(val))
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	// Fail fast on values that parse but break later: an "ip:host:container"
	// form or an empty value would make publishSpec/hostAddr hand queryAPI a
	// malformed URL (or leave the port unpublished) deep in the run.
	for k, v := range cfg {
		if err := validatePublish(k, v); err != nil {
			return nil, err
		}
	}
	return cfg, nil
}

// validatePublish requires a publish value to be exactly host:port (both halves
// non-empty). "127.0.0.1:10888" passes; "127.0.0.1:10888:10888", "", ":10888",
// and "127.0.0.1:" are rejected.
func validatePublish(key, value string) error {
	parts := strings.Split(value, ":")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return fmt.Errorf("publish.%s: %q must be host:port (non-empty host and port)", key, value)
	}
	return nil
}

// publishSpec renders the configured host address as a docker-style -p publish
// spec by appending the service's container port:
//
//	"127.0.0.1:10888" (api, container port 10888) -> "127.0.0.1:10888:10888"
//
// A value already carrying three colon-separated fields (ip:host:container) is
// returned unchanged. ok is false when the service has no published port.
func (c publishConfig) publishSpec(service string, containerPort int) (spec string, ok bool) {
	v, ok := c[service]
	if !ok {
		return "", false
	}
	if strings.Count(v, ":") >= 2 { // already ip:host:container
		return v, true
	}
	return fmt.Sprintf("%s:%d", v, containerPort), true
}

// hostAddr returns the configured host-side address (e.g. "127.0.0.1:10888") for
// a service, or "" when it is not published.
func (c publishConfig) hostAddr(service string) string {
	return c[service]
}

// stripInlineComment cuts a trailing `#...` comment that begins outside a pair of
// double quotes.
func stripInlineComment(s string) string {
	inQuote := false
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '"':
			inQuote = !inQuote
		case '#':
			if !inQuote {
				return s[:i]
			}
		}
	}
	return s
}

func splitColon(s string) (key, val string, ok bool) {
	i := strings.IndexByte(s, ':')
	if i < 0 {
		return "", "", false
	}
	return s[:i], s[i+1:], true
}

func unquote(s string) string {
	if len(s) >= 2 {
		first, last := s[0], s[len(s)-1]
		if (first == '"' && last == '"') || (first == '\'' && last == '\'') {
			return s[1 : len(s)-1]
		}
	}
	return s
}
