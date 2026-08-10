package main

import (
	"context"
	"fmt"
	"os/exec"
	"runtime"
	"strings"
)

// Runtime abstracts the container CLI the harness shells out to. Two
// implementations exist: apple/container (default on macOS) and docker
// (default on Linux). apple/container has no stable Go API, so
// CLI shell-out is the supported path.
type Runtime interface {
	// Name returns "container" or "docker".
	Name() string

	// EnsureSystem makes the runtime ready to run containers. On apple/container
	// it runs `container system status` and auto-starts the services if needed;
	// on docker it verifies the daemon responds to `docker info`.
	EnsureSystem(ctx context.Context) error

	// CheckVersion enforces the pinned CLI version (apple/container only; the
	// spec pins per-release to catch CLI drift). docker is a no-op in v1.
	CheckVersion(ctx context.Context) error

	// NetworkCreate / NetworkRemove / NetworkList manage per-run networks.
	NetworkCreate(ctx context.Context, name string) error
	NetworkRemove(ctx context.Context, name string) error
	NetworkList(ctx context.Context) ([]string, error)

	// Run starts a container (detached for service containers). See RunOpts.
	Run(ctx context.Context, opts RunOpts) error

	// Exec runs a command in a running container and returns stdout and the
	// process exit code. stderr is surfaced only via the returned error when the
	// command fails to launch.
	Exec(ctx context.Context, containerID string, cmd []string) (stdout string, exitCode int, err error)

	// Logs fetches the container's accumulated logs.
	Logs(ctx context.Context, containerID string) (string, error)

	// Stop stops a running container.
	Stop(ctx context.Context, containerID string) error

	// Rm removes a container; force removes it even while running.
	Rm(ctx context.Context, containerID string, force bool) error

	// ContainerList returns the IDs/names of all containers (running or not),
	// used to prune stale e2e-* resources from prior runs.
	ContainerList(ctx context.Context) ([]string, error)

	// InspectIP returns the container's IPv4 on the given network, without the
	// CIDR prefix. All inter-service wiring is by IP: apple/container in-container
	// DNS does not resolve container names (verified on this machine).
	InspectIP(ctx context.Context, containerID, network string) (string, error)
}

// RunOpts configures Runtime.Run. Bind mounts and publish specs use docker-style
// syntax ("src:dst[:ro]" and "[host-ip:]host:container[/proto]"); both CLIs accept it.
type RunOpts struct {
	Name    string   // container name (also its ID on apple/container)
	Image   string   // image reference
	Network string   // network to attach to
	Env     []string // KEY=VAL
	Volumes []string // bind mounts, "src:dst[:ro]"
	Ports   []string // publish specs
	Cmd     []string // image entrypoint arguments
	Detach  bool     // run detached (daemon); the common case for services
	AutoRm  bool     // remove the container when its process exits
}

// buildRunArgs renders RunOpts as the args for the `run` subcommand, common to
// both CLIs (apple/container and docker accept the same flags in this order).
func buildRunArgs(opts RunOpts) []string {
	args := []string{"run"}
	if opts.Detach {
		args = append(args, "-d")
	}
	if opts.AutoRm {
		args = append(args, "--rm")
	}
	if opts.Name != "" {
		args = append(args, "--name", opts.Name)
	}
	if opts.Network != "" {
		args = append(args, "--network", opts.Network)
	}
	for _, e := range opts.Env {
		args = append(args, "-e", e)
	}
	for _, v := range opts.Volumes {
		args = append(args, "-v", v)
	}
	for _, p := range opts.Ports {
		args = append(args, "-p", p)
	}
	args = append(args, opts.Image)
	args = append(args, opts.Cmd...)
	return args
}

// selectRuntime resolves which Runtime to use: an explicit --runtime flag wins,
// otherwise auto-detect by GOOS + binary on PATH (darwin→container, linux→docker).
func selectRuntime(flag string) (Runtime, error) {
	name := flag
	if name == "" {
		name = autoDetectRuntime()
	}
	switch name {
	case "container":
		return &containerRuntime{}, nil
	case "docker":
		return &dockerRuntime{}, nil
	default:
		if flag != "" {
			return nil, fmt.Errorf("unknown runtime %q (want \"container\" or \"docker\")", flag)
		}
		return nil, fmt.Errorf("could not auto-detect a container runtime: install apple/container or docker, or pass --runtime=container|docker")
	}
}

func autoDetectRuntime() string {
	// darwin prefers apple/container; linux prefers docker. The non-preferred
	// binary is used as a fallback if the preferred one is absent.
	switch runtime.GOOS {
	case "darwin":
		if lookPath("container") {
			return "container"
		}
		if lookPath("docker") {
			return "docker"
		}
	case "linux":
		if lookPath("docker") {
			return "docker"
		}
		if lookPath("container") {
			return "container"
		}
	default:
		if lookPath("docker") {
			return "docker"
		}
		if lookPath("container") {
			return "container"
		}
	}
	return ""
}

func lookPath(name string) bool {
	_, err := exec.LookPath(name)
	return err == nil
}

// resourceInList reports whether name appears in the given lister's output. Used
// to make Rm/NetworkRemove idempotent (a missing resource is success) without
// depending on error-message wording, which drifts between CLI releases. A list
// error is surfaced rather than treated as "absent" — otherwise Rm/NetworkRemove
// would silently no-op and leak the resource.
func resourceInList(ctx context.Context, list func(context.Context) ([]string, error), name string) (bool, error) {
	items, err := list(ctx)
	if err != nil {
		return false, err
	}
	for _, n := range items {
		if n == name {
			return true, nil
		}
	}
	return false, nil
}

// execResult captures a finished command's output. err is set only when the
// process could not be launched (binary missing, context cancelled); a non-zero
// exit code is surfaced via exitCode, not err, so callers can inspect it.
type execResult struct {
	stdout   string
	stderr   string
	exitCode int
}

func run(ctx context.Context, name string, args ...string) (execResult, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	var out, errb strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &errb
	err := cmd.Run()
	res := execResult{stdout: out.String(), stderr: errb.String()}
	if err != nil {
		// If the context was cancelled (deadline or signal), say so explicitly —
		// CommandContext kills the process and Run otherwise surfaces a confusing
		// "exit -1" / "signal: killed".
		if ctx.Err() != nil {
			return res, fmt.Errorf("%s: %w", cmdStr(name, args), ctx.Err())
		}
		if ee, ok := err.(*exec.ExitError); ok {
			res.exitCode = ee.ExitCode()
			return res, nil
		}
		return res, fmt.Errorf("%s: %w", cmdStr(name, args), err)
	}
	return res, nil
}

// runOK runs a command and returns its stdout, treating a non-zero exit as an error.
func runOK(ctx context.Context, name string, args ...string) (string, error) {
	res, err := run(ctx, name, args...)
	if err != nil {
		return res.stdout, err
	}
	if res.exitCode != 0 {
		return res.stdout, fmt.Errorf("%s failed (exit %d)\n%s", cmdStr(name, args), res.exitCode, indent(res.stderr))
	}
	return res.stdout, nil
}

func cmdStr(name string, args []string) string {
	return name + " " + strings.Join(args, " ")
}

func indent(s string) string {
	if s == "" {
		return ""
	}
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	for i := range lines {
		lines[i] = "    " + lines[i]
	}
	return strings.Join(lines, "\n")
}
