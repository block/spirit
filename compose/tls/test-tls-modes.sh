#!/bin/bash

# Script to test Spirit TLS modes with local MySQL

set -e

echo "🚀 Testing Spirit TLS Modes with Local MySQL"
echo "============================================="

# Build Spirit first (from project root)
echo "📦 Building Spirit..."
cd ../../
go build -o compose/tls/spirit ./cmd/spirit
cd compose/tls/

# Test connection configurations
MYSQL_HOST="127.0.0.1:3306"
MYSQL_USER="spirit"
MYSQL_PASSWORD="spirit" 
MYSQL_DATABASE="spirit_test"

echo ""
echo "🔍 Testing TLS Mode: DISABLED"
echo "------------------------------"
./spirit \
  --host="$MYSQL_HOST" \
  --username="$MYSQL_USER" \
  --password="$MYSQL_PASSWORD" \
  --database="$MYSQL_DATABASE" \
  --tls-mode="DISABLED" \
  --table="test_table" \
  --alter="ADD COLUMN test_col_disabled VARCHAR(50)"

echo ""
echo "🔍 Testing TLS Mode: PREFERRED (should fallback to plain)"
echo "---------------------------------------------------------"
./spirit \
  --host="$MYSQL_HOST" \
  --username="$MYSQL_USER" \
  --password="$MYSQL_PASSWORD" \
  --database="$MYSQL_DATABASE" \
  --tls-mode="PREFERRED" \
  --table="test_table" \
  --alter="ADD COLUMN test_col_preferred VARCHAR(50)"

echo ""
echo "🔍 Testing TLS Mode: REQUIRED (should fail)"
echo "-------------------------------------------"
set +e  # Allow this to fail
./spirit \
  --host="$MYSQL_HOST" \
  --username="$MYSQL_USER" \
  --password="$MYSQL_PASSWORD" \
  --database="$MYSQL_DATABASE" \
  --tls-mode="REQUIRED" \
  --table="test_table" \
  --alter="ADD COLUMN test_col_required VARCHAR(50)"
set -e

echo ""
echo "📋 Expected Results:"
echo "✅ DISABLED mode: Should have added 'test_col_disabled' column"
echo "✅ PREFERRED mode: Should have added 'test_col_preferred' column (via fallback)"
echo "❌ REQUIRED mode: Should have failed (no 'test_col_required' column)"

echo ""
echo "🔍 Verifying Final Table Structure"
echo "----------------------------------"
echo "Table structure after TLS mode testing:"

# Connect to MySQL and show the table structure
docker-compose -f 8.0.43-tls-disabled.yml exec -T mysql-tls-test mysql \
  -h localhost \
  -u "$MYSQL_USER" \
  -p"$MYSQL_PASSWORD" \
  -D "$MYSQL_DATABASE" \
  -e "SHOW CREATE TABLE test_table\G" 2>/dev/null || echo "⚠️  Could not retrieve table structure"

echo ""
echo "📊 Final table columns:"
docker-compose -f 8.0.43-tls-disabled.yml exec -T mysql-tls-test mysql \
  -h localhost \
  -u "$MYSQL_USER" \
  -p"$MYSQL_PASSWORD" \
  -D "$MYSQL_DATABASE" \
  -e "DESCRIBE test_table;" 2>/dev/null || echo "⚠️  Could not describe table"

echo ""
echo "If you see both 'test_col_disabled' and 'test_col_preferred' columns,"
echo "then your TLS fallback implementation is working correctly! 🎉"
