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
#     errors.AsType against one returns false for the other, so the classifiers
#     in pkg/dbconn, pkg/checkpoint and pkg/throttler keep compiling and
#     silently stop recognizing deadlocks, lock-wait timeouts and
#     unknown-system-variable.
#
# The depguard rule in .golangci.yaml covers first-party imports. It is an
# AST-level check, so it cannot see a *transitive* dependency that imports
# upstream — which is the more likely way this regresses, and is how it looks
# today in squareup/gap (blip, ods-rds-connector, go-mysql/errors). That is what
# this script is for: it asks the resolved package graph instead.
#
# `go list -deps -test` is the authoritative question. `go mod why` is not — it
# reports the *shortest* path, so a one-hop import from a first-party package
# hides the real transitive cause, and go.mod/go.sum can carry a module that
# nothing links at all.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

readonly BANNED="github.com/go-sql-driver/mysql"

echo "checking the package graph for $BANNED"

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

if ! go list -deps -test ./... >"$deps_file" 2>"$stderr_file"; then
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
    echo "Importers:" >&2
    go list -deps -test -f \
        '{{range .Imports}}{{if eq . "'"$BANNED"'"}}{{$.ImportPath}}{{end}}{{end}}' \
        ./... 2>/dev/null | grep -v '^$' | sort -u | sed 's/^/  /' >&2
    echo >&2
    echo "See the comment at the top of this script for why this is not cosmetic." >&2
    echo "For a first-party import, .golangci.yaml's depguard rule has the remediation." >&2
    exit 1
fi

echo "OK: $count packages, exactly one MySQL driver linked"
