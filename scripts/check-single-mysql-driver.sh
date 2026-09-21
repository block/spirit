#!/usr/bin/env bash
#
# Fail if more than one MySQL driver is linked.
#
# Spirit connects through github.com/block/mysql. Linking upstream
# github.com/go-sql-driver/mysql as well breaks two things, and both breakages
# are silent:
#
#   - Two TLS registries. A tls= DSN value is a *name*, and a name only resolves
#     inside the registry of the driver *package* that registered it —
#     registries are per-package globals and nothing about them travels in the
#     DSN. pkg/dbconn registers "rds" into block/mysql and hands that name back
#     in every enhanced DSN, including to consumers that reuse
#     EnhanceDSNWithTLS. A pool dialing through any other driver cannot resolve
#     the name and fails to open against an RDS host at all. That is
#     host-shaped: invisible to a suite that never points at an
#     *.rds.amazonaws.com address, which is exactly why it sat unnoticed in
#     SchemaBot's storage pool until the driver rename surfaced it.
#   - Two field-identical but *distinct* *mysql.MySQLError types.
#     errors.AsType against one returns false for the other, and every
#     classifier built on it keeps compiling while quietly answering "no". The
#     two that go silent: pkg/dbconn stops recognizing 1205 lock-wait and 1213
#     deadlock, so retries stop happening — and pkg/change's buffered
#     subscription, which asks dbconn.IsLockContentionError, stops backing off
#     from contention its own flush fan-out causes; pkg/checkpoint's
#     IsIncompatible stops recognizing 1146/1054, so a checkpoint written by an
#     incompatible version reads as a *transient* read error and the run asks
#     for a retry that can never succeed. The configuration checks fail the
#     other way instead of silently: pkg/{migration,move}/check gate on
#     `!ok || Number != 1193`, so `!ok` takes the error branch and the check
#     refuses to run at all against MySQL < 8.0.20.
#
# The depguard rule in .golangci.yaml covers first-party imports. It is an
# AST-level check, so it cannot see a *transitive* dependency that imports
# upstream — which is the more likely way this regresses. That is not
# hypothetical: one internal consumer of this library links upstream today
# through three separate transitive dependencies, with no first-party import
# anywhere, so depguard alone would report nothing there. That is what this
# script is for: it asks the resolved package graph instead.
#
# `go list -deps -test` is the authoritative question. `go mod why` is not — it
# reports the *shortest* path, so a one-hop import from a first-party package
# hides the real transitive cause, and go.mod/go.sum can carry a module that
# nothing links at all.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

readonly BANNED="github.com/go-sql-driver/mysql"

# Build-tagged files are invisible to `go list` without their tags. The tags are
# derived from the repo's own //go:build lines rather than hardcoded, so a new
# tag-gated suite is covered the day it lands and there is no list to keep in
# sync. Every constraint in this repo is a single bare tag with no negation, so
# enabling all of them at once cannot exclude a file that the default build
# would have included.
#
# Note the asymmetry with depguard, which has no equivalent and stays tag-blind:
# .golangci.yaml deliberately sets no run.build-tags, because turning it on
# surfaces 4 pre-existing issues (modernize, noctx) in those test files, and
# fixing unrelated test files does not belong in a change about driver
# enforcement. The exposure is small — nothing in .github/workflows/ or the
# Makefile passes any of these tags, so a driver reaching a tag-gated file never
# reaches a CI binary, and this script covers the transitive case regardless.
# Worth doing if run.build-tags is ever added for other reasons.
tags=$(python3 - <<'PY'
import pathlib, re, subprocess, sys

# GOOS/GOARCH and toolchain terms are constraints, not suite tags. Taken from
# the toolchain rather than hardcoded so this needs no upkeep.
dist = subprocess.run(["go", "tool", "dist", "list"],
                      capture_output=True, text=True, check=True).stdout.split()
ignore = {part for pair in dist for part in pair.split("/")}
ignore |= {"cgo", "race", "msan", "asan", "gc", "gccgo", "unix", "ignore", "tools"}

derived = set()
for path in pathlib.Path(".").rglob("*.go"):
    if "vendor/" in str(path):
        continue
    try:
        head = path.read_text(errors="ignore")[:4000]
    except OSError:
        continue
    for line in re.findall(r"^//go:build (.+)$", head, re.M):
        if "!" in line:
            sys.exit("negated build constraint found (%s); enabling every tag at "
                     "once may now exclude files, so this needs to iterate over "
                     "tag sets instead: %s" % (path, line.strip()))
        derived |= {t for t in re.findall(r"[A-Za-z_][A-Za-z0-9_.]*", line)
                    if t not in ignore}

print(",".join(sorted(derived)))
PY
)

echo "checking the package graph for $BANNED (tags: ${tags:-none})"

# The package list goes to a file, and every check below greps that file
# directly. Do not reintroduce `printf "$deps" | grep -q` here: grep -q exits on
# its first match, printf then takes SIGPIPE, and under `set -o pipefail` the
# *pipeline* reports failure even though the match succeeded. That inverts the
# result. It is invisible locally, where the pipe buffer swallows the whole list
# before grep can exit, and it showed up on the first CI run in the sibling
# strata change. For the banned driver in particular it would have failed in the
# worst direction: an early match reads as "not present", which is a silent
# pass. No pipes, no SIGPIPE.
#
# stderr is kept out of the list, and the exit status is consulted on its own,
# so a load failure cannot be mistaken for a clean graph: an unresolvable module
# makes go list exit non-zero while still printing most of the graph.
deps_file=$(mktemp)
stderr_file=$(mktemp)
trap 'rm -f "$deps_file" "$stderr_file"' EXIT

if ! go list -deps -test -tags "$tags" ./... >"$deps_file" 2>"$stderr_file"; then
    echo "FAIL: go list could not load the package graph, so the check did not run:" >&2
    cat "$stderr_file" >&2
    exit 1
fi

count=$(grep -c . "$deps_file")
if [[ "$count" -lt 100 ]]; then
    echo "FAIL: go list returned only $count packages, which is too few to be this module's graph." >&2
    echo "The check is not meaningfully running. Fix it rather than trusting the pass." >&2
    exit 1
fi

# Guard against the reverse rot: if block/mysql ever stops appearing, this
# script is no longer looking at a graph where the invariant is even meaningful.
if ! grep -qx "github.com/block/mysql" "$deps_file"; then
    echo "FAIL: github.com/block/mysql is not in the package graph." >&2
    echo "Either the driver was replaced (update this check) or the graph is wrong." >&2
    exit 1
fi

if grep -qx "$BANNED" "$deps_file"; then
    echo >&2
    echo "FAIL: $BANNED is linked alongside github.com/block/mysql." >&2
    echo >&2
    # This is the one place that used to discard its own errors, so a go list
    # failure here printed an empty list under a FAIL that was already correct.
    # Report it instead: an empty "Importers:" is otherwise indistinguishable
    # from a diagnostic that could not run.
    #
    # Via a file again, for the reason above plus one more: `grep -v '^$'` exits
    # 1 when every line is blank, so in a pipeline it would report failure for a
    # `go list` that worked fine.
    echo "Importers:" >&2
    importers_file=$(mktemp)
    trap 'rm -f "$deps_file" "$stderr_file" "$importers_file"' EXIT
    if go list -deps -test -tags "$tags" -f \
        '{{range .Imports}}{{if eq . "'"$BANNED"'"}}{{$.ImportPath}}{{end}}{{end}}' \
        ./... >"$importers_file" 2>"$stderr_file"; then
        grep -v '^$' "$importers_file" | sort -u | sed 's/^/  /' >&2 || true
    else
        echo "  (could not resolve importers; go list said:)" >&2
        cat "$stderr_file" >&2
    fi
    echo >&2
    echo "See the comment at the top of this script for why this is not cosmetic." >&2
    echo "For a first-party import, .golangci.yaml's depguard rule has the remediation." >&2
    exit 1
fi

echo "OK: $count packages, exactly one MySQL driver linked"
