package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

// This file implements the "UI" path: the opt-in --with-ui path that
// builds the StatsHouse npm UI in a pinned node container and serves it from the
// api's --static-dir. Off by default; without the flag there is no node, no UI
// build, and no implicit work (the placeholder e2e/api-static/index.html is used).
//
// The build path branches on the runtime's network:
//
//	apple/container (NO in-container network) — two-stage offline:
//	  1. populateNpmCache runs `npm install --os=linux --cpu=<arch> --libc=glibc
//	     --ignore-scripts` ON THE HOST (which has internet), staging only package.json
//	     + package-lock.json in a throwaway dir, so the shared tarball cache
//	     (<e2e-cache>/npm) is filled with the CONTAINER platform's native packages
//	     (a plain host `npm install` would cache only darwin tarballs and the
//	     container's offline `npm ci` would miss @swc/core-linux-arm64-gnu et al.).
//	  2. buildUIInContainer runs the pinned node container with that cache mounted and
//	     `npm ci --offline` against it — no container egress required.
//
//	docker (NAT egress; the lima guest has no npm anyway) — single-stage online:
//	  buildUIInContainer runs `npm ci --prefer-online` in the node container, fetching
//	  from the registry directly. The host npm cache is still mounted so it warms as a
//	  side effect, but no host populate runs.
//
// The build OUTPUT (<e2e-cache>/ui/build, index.html at its root) is what gets mounted
// into the api as --static-dir=/ui. Rebuild detection is content-based: a sha256 over
// the whole source tree (see uiSourceFingerprint) plus the pinned node image identity
// is recorded after every successful build, so an unchanged tree skips the (slow)
// container build while any source edit/addition/deletion/rename, a lockfile change,
// or a node-image bump triggers one.

const (
	// nodeBaseImage is the pinned Node toolchain the UI builds in: exact Node patch +
	// explicit Debian variant + verified multiarch digest (the digest is authoritative;
	// the tag carries the patch/variant for human readers). node 20 matches the UI CI
	// (.github/workflows/ci-ui.yml node-version: 20.x).
	//
	// Evidence (all resolve to the same manifest-list/OCI-image-index digest, multiarch
	// linux/amd64 + linux/arm64/v8 + …):
	//   - node v20.20.2, npm 10.8.2, Debian bookworm-slim — `container run node:20-slim node -v`.
	//   - `container image inspect node:20-slim`        → sha256:2cf067cfed83…
	//   - `docker buildx imagetools inspect node:20-slim`             → sha256:2cf067cfed83…
	//   - `docker buildx imagetools inspect node:20.20.2-bookworm-slim` → sha256:2cf067cfed83…
	// The digest-pinned reference resolves OFFLINE on apple/container (verified: it
	// matches the manifest list cached locally under the node:20-slim tag), so a
	// --with-ui run needs no registry egress on that runtime.
	nodeBaseImage = "node:20.20.2-bookworm-slim@sha256:2cf067cfed83d5ea958367df9f966191a942351a2df77d6f0193e162b5febfc0"

	// npmMinMajor is the minimum host npm for the apple/container offline path.
	// populateNpmCache passes --os/--cpu/--libc (to fetch the container platform's
	// native optional deps from a darwin host); those flags landed in npm 7.
	npmMinMajor = 7

	// In-container mount points for the UI build.
	uiSourceMount = "/ui-src"    // the statshouse-ui checkout (ro)
	uiOutputMount = "/ui-out"    // the cached build output (rw)
	npmCacheMount = "/npm-cache" // the host npm tarball cache (rw)

	// uiBuiltMarker is a JSON uiBuildMarker written after every successful container
	// build. Its image + source fingerprint are compared against the current node image
	// and a fresh source scan to decide whether a rebuild is needed. Lives in
	// <e2e-cache>/ui, NOT under build/, so wiping a stale build/ never clears a
	// successful-build marker.
	uiBuiltMarker = ".built"

	// npmCacheFingerprintFile holds the (lockfiles + arch + libc + node image) the npm
	// cache was last populated for, so a lockfile edit, an arch change, or a node-image
	// bump re-populates it instead of the container build failing offline on a missing
	// tarball.
	npmCacheFingerprintFile = ".fingerprint"

	uiLibc = "glibc" // node:*-slim is Debian → glibc (not musl)
)

// uiFingerprintSkipDirs are generated/installed/non-source subtrees excluded from the
// source fingerprint (and only from it) so they never spuriously trigger a rebuild.
var uiFingerprintSkipDirs = map[string]bool{"node_modules": true, "build": true, ".git": true}

// buildUI builds the StatsHouse npm UI and returns the host dir holding build/
// (index.html at its root) to mount into the api as --static-dir. The output is cached
// under <cache>/ui/build and reused verbatim when the build marker (node image + source
// fingerprint) is still valid. On apple/container the container build is offline against
// a host-populated cache; on docker it runs online (NAT egress). containerName is the
// e2e-prefixed name for the one-shot build container (tracked by the caller for crash
// cleanup, and reaped by pruneStale across runs). log receives progress lines.
func buildUI(ctx context.Context, rt Runtime, repoRoot, cache, containerName string, log func(string, ...any)) (string, error) {
	uiDir := filepath.Join(repoRoot, "statshouse-ui")
	outRoot := filepath.Join(cache, "ui")
	outBuild := filepath.Join(outRoot, "build")
	markerPath := filepath.Join(outRoot, uiBuiltMarker)
	if err := os.MkdirAll(outRoot, 0o755); err != nil {
		return "", fmt.Errorf("create ui cache %s: %w", outRoot, err)
	}

	online := rt.HasNetworkEgress()

	fingerprint, err := uiSourceFingerprint(uiDir)
	if err != nil {
		return "", fmt.Errorf("fingerprint ui source: %w", err)
	}
	marker := readBuildMarker(markerPath) // zero on first run / unreadable / legacy → rebuild
	outMissing := !fileExists(filepath.Join(outBuild, "index.html"))
	if !uiNeedsRebuild(outMissing, marker, fingerprint, nodeBaseImage) {
		log("ui build: skipped (source fingerprint unchanged; cached build in %s)", outBuild)
		return outBuild, nil
	}

	// Mounted in both paths; only apple/container (offline) pre-populates it.
	npmCache := filepath.Join(cache, "npm")
	if err := os.MkdirAll(npmCache, 0o755); err != nil {
		return "", fmt.Errorf("create npm cache %s: %w", npmCache, err)
	}
	if online {
		log("ui build: docker runtime — npm ci runs online in the node container (NAT egress; no host cache populate)")
	} else {
		// Refresh the host npm cache for the container platform when its fingerprint
		// (lockfiles + arch + libc + node image) changed. cacache is additive, so a
		// re-populate after an arch switch just adds the new platform's tarballs.
		fpPath := filepath.Join(npmCache, npmCacheFingerprintFile)
		wantFP, err := npmCacheFingerprint(uiDir)
		if err != nil {
			return "", fmt.Errorf("compute npm cache fingerprint: %w", err)
		}
		if readFileTrim(fpPath) != wantFP {
			if err := populateNpmCache(ctx, uiDir, npmCache, log); err != nil {
				return "", err
			}
			if err := os.WriteFile(fpPath, []byte(wantFP+"\n"), 0o644); err != nil {
				return "", fmt.Errorf("write npm cache fingerprint %s: %w", fpPath, err)
			}
		} else {
			log("ui build: npm cache up to date (%s)", npmCache)
		}
	}

	// Wipe any stale build output, then rebuild. A failed build leaves the (absent)
	// output and the OLD marker untouched, so the next run rebuilds — correctness over a
	// half-written cache.
	if err := os.RemoveAll(outBuild); err != nil {
		return "", fmt.Errorf("clear stale ui build output %s: %w", outBuild, err)
	}
	if err := os.MkdirAll(outBuild, 0o755); err != nil {
		return "", fmt.Errorf("recreate ui build output dir %s: %w", outBuild, err)
	}
	if err := buildUIInContainer(ctx, rt, uiDir, outBuild, npmCache, online, containerName, log); err != nil {
		return "", err
	}
	// Record the as-built state. buildUIInContainer verifies index.html before
	// returning, so this is written only after a successful build; a crashed build
	// leaves the OLD marker (or none) and the next run rebuilds.
	if err := writeBuildMarker(markerPath, uiBuildMarker{Image: nodeBaseImage, Fingerprint: fingerprint}); err != nil {
		return "", fmt.Errorf("write ui build marker %s: %w", markerPath, err)
	}
	log("ui build: done (output in %s)", outBuild)
	return outBuild, nil
}

// populateNpmCache runs `npm install` ON THE HOST (which has internet; the apple/container
// container does not) into a throwaway dir, populating the shared tarball cache at
// npmCache with the linux/<arch>/glibc packages the container build consumes offline.
// --os/--cpu/--libc force npm to fetch the CONTAINER platform's native optional deps;
// --ignore-scripts skips native postinstall scripts that cannot run on the wrong OS (the
// build needs only the prebuilt binaries, which ship inside the tarballs).
func populateNpmCache(ctx context.Context, uiDir, npmCache string, log func(string, ...any)) error {
	work, err := os.MkdirTemp("", "statshouse-e2e-npmpop-*")
	if err != nil {
		return fmt.Errorf("create npm cache populate dir: %w", err)
	}
	defer os.RemoveAll(work)
	for _, f := range []string{"package.json", "package-lock.json"} {
		if err := copyFile(filepath.Join(uiDir, f), filepath.Join(work, f)); err != nil {
			return fmt.Errorf("stage %s for npm cache populate: %w", f, err)
		}
	}
	arch := containerNodeArch()
	args := []string{
		"install",
		"--os=linux",
		"--cpu=" + npmCPU(arch),
		"--libc=" + uiLibc,
		"--ignore-scripts",
		"--no-audit",
		"--no-fund",
		"--no-save",
		"--cache=" + npmCache,
	}
	start := time.Now()
	log("ui build: populating npm cache for linux/%s/%s on the host (one-time per lockfile/arch; host has internet, container does not)",
		npmCPU(arch), uiLibc)
	var b strings.Builder
	cmd := exec.CommandContext(ctx, "npm", args...)
	cmd.Dir = work
	cmd.Stdout = &b
	cmd.Stderr = &b
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("npm install (populate cache): %w", ctx.Err())
		}
		return fmt.Errorf("npm install (populate cache): %w\n%s", err, indent(truncate(b.String(), 4000)))
	}
	log("ui build: npm cache populated (%.1fs)", time.Since(start).Seconds())
	return nil
}

// buildUIInContainer runs the pinned node container to build the UI. When online is
// false (apple/container) it installs OFFLINE against the host-populated npm cache; when
// true (docker) it installs ONLINE from the registry (NAT egress). The source is mounted
// read-only and copied to a container-local writable /work (npm ci writes node_modules
// there, the build writes build/ there); only build/ is copied out to the host cache so
// the repo checkout stays clean. Foreground + AutoRm: the container's exit code is
// captured and the container is removed when it returns.
func buildUIInContainer(ctx context.Context, rt Runtime, uiDir, outBuild, npmCache string, online bool, containerName string, log func(string, ...any)) error {
	// Copy the ro-mounted source into a writable /work (container-local), install,
	// build, then copy build/ out to the host-mounted output. The source→/work copy uses
	// `cp -a` then an rm of node_modules/build/.git: a host checkout that ran
	// `npm install`/a build locally would otherwise copy ~500 MB of darwin binaries into
	// the container, which `npm ci` wipes anyway. The build→/ui-out copy uses `cp -r`,
	// NOT `cp -a`: /ui-out is an apple/container bind mount (virtiofs) where preserving
	// timestamps/ownership fails with "Operation not permitted", and GNU coreutils'
	// `cp -a` returns NON-ZERO on that (unlike busybox's, which ignores it), aborting the
	// build under `set -e`; `cp -r` copies data + mode without preserving attrs. The
	// trailing chmod makes the root-owned output + cache writable by the host user: the
	// node container runs as root, so its writes land root-owned on the bind mounts, and
	// on Linux (lima) the next run's os.RemoveAll(outBuild) would EACCES for the non-root
	// lima user without it. `|| true`: on virtiofs chmod can be a no-op and must never
	// abort a successful build under `set -e`.
	npmCI := "npm ci --cache=" + npmCacheMount + " --no-audit --no-fund"
	mode := "offline"
	if online {
		npmCI += " --prefer-online" // fetch from registry; docker has NAT egress
		mode = "online"
	} else {
		npmCI += " --offline" // apple/container: install only from the host cache
	}
	script := "set -e; " +
		"mkdir -p /work && cp -a " + uiSourceMount + "/. /work/ && " +
		"rm -rf /work/node_modules /work/build /work/.git && cd /work && " +
		"echo \"e2e: node=$(node -v) npm=$(npm -v)\" && " +
		npmCI + " && " +
		"echo \"e2e: npm ci ok (" + mode + ")\" && " +
		"npm run build && " +
		"mkdir -p " + uiOutputMount + " && cp -r /work/build/. " + uiOutputMount + "/ && " +
		"( chmod -R a+rwX " + uiOutputMount + " " + npmCacheMount + " 2>/dev/null || true ) && " +
		"echo \"e2e: ui build output copied to " + uiOutputMount + "\""
	opts := RunOpts{
		Name:  containerName, // e2e-<runid>-uibuild; tracked by the caller + reaped by pruneStale
		Image: nodeBaseImage,
		// No e2e network: offline is self-contained; online uses the default bridge's
		// NAT egress (docker). apple/container never reaches here with online=true.
		Volumes: []string{
			uiDir + ":" + uiSourceMount + ":ro",
			outBuild + ":" + uiOutputMount,
			npmCache + ":" + npmCacheMount,
		},
		Cmd:    []string{"/bin/sh", "-c", script},
		AutoRm: true,
	}
	start := time.Now()
	log("ui build: running %s container (%s npm ci + npm run build)", nodeBaseImage, mode)
	res, err := run(ctx, rt.Name(), buildRunArgs(opts)...)
	output := res.stdout + res.stderr
	if err != nil {
		return fmt.Errorf("ui build container launch: %w\n%s", err, indent(truncate(output, 4000)))
	}
	if res.exitCode != 0 {
		return fmt.Errorf("ui build failed (container exit %d)\n%s", res.exitCode, indent(truncate(output, 6000)))
	}
	if !fileExists(filepath.Join(outBuild, "index.html")) {
		return fmt.Errorf("ui build produced no index.html in %s\n%s", outBuild, indent(truncate(output, 4000)))
	}
	log("ui build: container build ok (%.1fs)\n%s", time.Since(start).Seconds(), indent(truncate(strings.TrimSpace(output), 800)))
	return nil
}

// uiBuildMarker records the as-built state of a successful UI build, so the next run can
// decide whether the cached output is still valid. Image is the pinned nodeBaseImage the
// build ran in; Fingerprint is the source-tree content hash recorded for that build.
type uiBuildMarker struct {
	Image       string `json:"image"`
	Fingerprint string `json:"fingerprint"`
}

// uiNeedsRebuild decides whether the cached UI build is stale. Pure → unit-tested. A
// rebuild is required when the build output is missing, when the marker was built under a
// different node image, or when the source fingerprint changed. A zero marker (first run,
// or an unreadable/legacy marker that lacks a fingerprint) always rebuilds: its Image and
// Fingerprint never match the live values.
func uiNeedsRebuild(outputMissing bool, marker uiBuildMarker, fingerprint, image string) bool {
	return outputMissing || marker.Image != image || marker.Fingerprint != fingerprint
}

// uiSourceFingerprint returns a deterministic sha256 over every source file in the
// statshouse-ui tree — each file contributes its relative path (slash-normalized) then
// its contents — excluding generated/installed/non-source dirs (node_modules, build,
// .git). Determinism: paths are sorted, so readdir order and OS do not affect it. Any
// content edit, addition, deletion, or rename changes it; crucially a content change
// with a preserved or older mtime still changes it (the blind spot of an mtime-only
// rule). Lockfile edits change it too (package.json/package-lock.json live in the tree).
// Errors if no source files are found (a real checkout always has some).
func uiSourceFingerprint(uiDir string) (string, error) {
	var paths []string
	walkErr := filepath.WalkDir(uiDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if uiFingerprintSkipDirs[d.Name()] {
				return filepath.SkipDir
			}
			return nil
		}
		paths = append(paths, path)
		return nil
	})
	if walkErr != nil {
		return "", walkErr
	}
	if len(paths) == 0 {
		return "", fmt.Errorf("no source files found under %s", uiDir)
	}
	sort.Strings(paths)
	h := sha256.New()
	for _, p := range paths {
		rel, err := filepath.Rel(uiDir, p)
		if err != nil {
			return "", fmt.Errorf("rel path %s: %w", p, err)
		}
		h.Write([]byte(filepath.ToSlash(rel)))
		h.Write([]byte{0})
		data, err := os.ReadFile(p)
		if err != nil {
			return "", fmt.Errorf("read %s for fingerprint: %w", p, err)
		}
		h.Write(data)
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// uiIndexLooksBuilt reports whether body is the built statshouse-ui index.html rather
// than the e2e/api-static placeholder. The built app's served index.html always carries
// the React mount point (id="root"); the placeholder has none. Pure → unit-tested.
func uiIndexLooksBuilt(body string) bool {
	return strings.Contains(body, `id="root"`)
}

// assertUIServed polls GET / on the api until it returns 200 and demonstrably serves the
// built UI (not the placeholder) — proving --with-ui actually wired the build output into
// the api's --static-dir. Reuses httpGet + poll (the same helpers as /api/query). Returns
// the served body.
func assertUIServed(ctx context.Context, apiAddr string) (string, error) {
	url := "http://" + apiAddr + "/"
	const timeout = 60 * time.Second
	var (
		lastBody string
		lastCode int
	)
	if err := poll(ctx, timeout, 2*time.Second, func() (bool, error) {
		body, code, gerr := httpGet(ctx, url)
		if gerr != nil {
			lastBody = gerr.Error()
			return false, nil
		}
		lastCode, lastBody = code, body
		return code == http.StatusOK && uiIndexLooksBuilt(body), nil
	}); err != nil {
		return "", fmt.Errorf("UI not served at %s within %s (last code=%d, looks-built=%v): %v\nlast body: %s",
			url, timeout, lastCode, uiIndexLooksBuilt(lastBody), err, truncate(lastBody, 1000))
	}
	return lastBody, nil
}

// npmCacheFingerprint returns a hex sha256 over package.json + package-lock.json plus the
// target platform (arch, libc) and the pinned node image. It identifies the exact dep set
// + platform + toolchain the npm cache must hold; a change (lockfile edit, arch switch,
// node-image/digest bump) re-populates the cache. Thin wrapper over
// npmCacheFingerprintFor so the hashing rule is unit-testable with explicit inputs.
func npmCacheFingerprint(uiDir string) (string, error) {
	return npmCacheFingerprintFor(uiDir, containerNodeArch(), uiLibc, nodeBaseImage)
}

// npmCacheFingerprintFor is the pure hashing rule (reads the two lockfiles, then folds in
// arch/libc/image). Separated so a test can vary the image in isolation and prove a node
// bump changes the fingerprint.
func npmCacheFingerprintFor(uiDir, arch, libc, image string) (string, error) {
	h := sha256.New()
	for _, f := range []string{"package.json", "package-lock.json"} {
		data, err := os.ReadFile(filepath.Join(uiDir, f))
		if err != nil {
			return "", fmt.Errorf("read %s for npm cache fingerprint: %w", f, err)
		}
		h.Write(data)
		h.Write([]byte{0})
	}
	fmt.Fprintf(h, "os=linux\ncpu=%s\nlibc=%s\nimage=%s\n", npmCPU(arch), libc, image)
	return hex.EncodeToString(h.Sum(nil)), nil
}

// containerNodeArch is the arch the node build container actually runs as. It is
// runtime.GOARCH (the host/container arch), NOT the --arch flag (which only governs the
// Go daemon cross-compile): apple/container runs the node image at the host arch
// regardless of --arch, so the npm cache must be populated for THIS arch or its native
// tarballs will not match the container's offline `npm ci`.
func containerNodeArch() string { return runtime.GOARCH }

// npmCPU maps a GOARCH to the value npm's --cpu flag expects. npm uses "x64" (not
// "amd64") and "arm64"; unknown arches pass through (realistic targets are arm64/amd64).
// Pure → covered by TestNpmCPU.
func npmCPU(arch string) string {
	switch arch {
	case "amd64":
		return "x64"
	case "386":
		return "ia32"
	default:
		return arch // arm64 -> arm64
	}
}

// npmMajorVersion runs `npm --version` and returns the numeric major, or 0 if npm is
// missing or its version is unparseable. Used by the apple/container preflight: the
// offline cache populate passes npm --os/--cpu/--libc, which require npm >= npmMinMajor.
// Strictly opt-in (called only from the withUI && non-docker preflight).
func npmMajorVersion(ctx context.Context) int {
	res, err := run(ctx, "npm", "--version")
	if err != nil || res.exitCode != 0 {
		return 0
	}
	major, _ := strconv.Atoi(strings.SplitN(strings.TrimSpace(res.stdout), ".", 2)[0])
	return major
}

// readFileTrim returns the file's trimmed contents, or "" if it is absent/unreadable (a
// missing fingerprint simply forces a re-populate). Used for the npm cache fingerprint.
func readFileTrim(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// readBuildMarker reads the last successful build's marker, returning the zero marker
// when it is absent, empty, or unparseable — each of which forces a rebuild via
// uiNeedsRebuild. This deliberately swallows errors: a missing/legacy marker must never
// block a build, only re-trigger one (a legacy marker lacks Fingerprint → rebuild).
func readBuildMarker(path string) uiBuildMarker {
	b, err := os.ReadFile(path)
	if err != nil || strings.TrimSpace(string(b)) == "" {
		return uiBuildMarker{}
	}
	var m uiBuildMarker
	if err := json.Unmarshal(b, &m); err != nil {
		return uiBuildMarker{}
	}
	return m
}

// writeBuildMarker records the as-built marker after a successful build. Written only
// after the build's index.html is verified present, so a crashed build leaves the OLD
// marker (or none) and the next run rebuilds.
func writeBuildMarker(path string, m uiBuildMarker) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

// copyFile copies a single regular file (used to stage package.json/lock into the
// throwaway npm-cache-populate dir). Permissions follow the source.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	fi, err := in.Stat()
	if err != nil {
		return err
	}
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, fi.Mode())
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}
