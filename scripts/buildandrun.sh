#!/usr/bin/env bash
set -e

make build

params=(--host="$HOST" --username="$USERNAME" --password="$PASSWORD" --database="$DATABASE" --statement="ALTER TABLE \`$TABLE\` ENGINE=InnoDB")

if [ -n "$REPLICA_DSN" ]; then
  params+=(--replica-dsn="$REPLICA_DSN")
fi

./bin/spirit migrate "${params[@]}"
