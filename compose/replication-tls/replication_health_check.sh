#!/usr/bin/env bash
set -euxo pipefail

replica_status=$(mysql -hmysql_replica -uroot -p"$MYSQL_ROOT_PASSWORD" -e "show replica status\G")

if ! echo "$replica_status" | grep -q 'Replica_IO_Running: Yes'; then
  echo "replica IO not running..."
  exit 1
fi

if ! echo "$replica_status" | grep -q 'Replica_SQL_Running: Yes'; then
  echo "replica SQL not running..."
  exit 1
fi

if ! echo "$replica_status" | grep -q 'Seconds_Behind_Source: 0'; then
  echo "unexpected replication lag..."
  exit 1
fi