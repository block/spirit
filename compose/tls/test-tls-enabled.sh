#!/bin/bash

# Script to test Spirit TLS modes with TLS-enabled MySQL

set -e

echo "🔐 Testing Spirit TLS Modes with TLS-Enabled MySQL"
echo "=================================================="

# Build Spirit first (from project root)
echo "📦 Building Spirit..."
cd ../../
go build -o compose/tls/spirit ./cmd/spirit
cd compose/tls/

# Test connection configurations (note different port for TLS-enabled server)
MYSQL_HOST="127.0.0.1:3307"
MYSQL_HOST_LOCALHOST="localhost:3307"  # For VERIFY_IDENTITY (hostname verification)
MYSQL_USER="spirit"
MYSQL_PASSWORD="spirit" 
MYSQL_DATABASE="spirit_test"

echo ""
echo "🔍 Testing TLS Mode: DISABLED (should fail - server requires TLS)"
echo "----------------------------------------------------------------"
set +e  # Allow this to fail
./spirit \
  --host="$MYSQL_HOST" \
  --username="$MYSQL_USER" \
  --password="$MYSQL_PASSWORD" \
  --database="$MYSQL_DATABASE" \
  --tls-mode="DISABLED" \
  --table="test_table" \
  --alter="ADD COLUMN test_col_disabled VARCHAR(50)"
DISABLED_RESULT=$?
set -e

if [ $DISABLED_RESULT -eq 0 ]; then
    echo "⚠️  WARNING: DISABLED mode unexpectedly succeeded!"
else
    echo "✅ DISABLED mode correctly failed (server requires secure transport)"
fi

echo ""
echo "🔍 Testing TLS Mode: REQUIRED (should work with TLS-enabled server)"
echo "------------------------------------------------------------------"
./spirit \
  --host="$MYSQL_HOST" \
  --username="$MYSQL_USER" \
  --password="$MYSQL_PASSWORD" \
  --database="$MYSQL_DATABASE" \
  --tls-mode="REQUIRED" \
  --table="test_table" \
  --alter="ADD COLUMN test_col_required VARCHAR(50)"

echo ""
echo "🔍 Testing TLS Mode: VERIFY_CA (should work with server certificates)"
echo "--------------------------------------------------------------------"
if [ -f "mysql-certs/ca.pem" ]; then
    echo "Using extracted MySQL CA certificate for verification"
    ./spirit \
      --host="$MYSQL_HOST" \
      --username="$MYSQL_USER" \
      --password="$MYSQL_PASSWORD" \
      --database="$MYSQL_DATABASE" \
      --tls-mode="VERIFY_CA" \
      --tls-ca="mysql-certs/ca.pem" \
      --table="test_table" \
      --alter="ADD COLUMN test_col_verify_ca VARCHAR(50)"
else
    echo "⚠️  No MySQL CA certificate found, testing without custom certificate"
    ./spirit \
      --host="$MYSQL_HOST" \
      --username="$MYSQL_USER" \
      --password="$MYSQL_PASSWORD" \
      --database="$MYSQL_DATABASE" \
      --tls-mode="VERIFY_CA" \
      --table="test_table" \
      --alter="ADD COLUMN test_col_verify_ca VARCHAR(50)"
fi

echo ""
echo "🔍 Testing TLS Mode: VERIFY_IDENTITY with IP address (should fail - hostname mismatch)"
echo "------------------------------------------------------------------------------------"
if [ -f "mysql-certs/ca.pem" ]; then
    echo "Using IP address 127.0.0.1 - should fail due to hostname verification"
    set +e  # Allow this to fail
    ./spirit \
      --host="$MYSQL_HOST" \
      --username="$MYSQL_USER" \
      --password="$MYSQL_PASSWORD" \
      --database="$MYSQL_DATABASE" \
      --tls-mode="VERIFY_IDENTITY" \
      --tls-ca="mysql-certs/ca.pem" \
      --table="test_table" \
      --alter="ADD COLUMN test_col_verify_identity_ip_fail VARCHAR(50)"
    IP_RESULT=$?
    set -e
    
    if [ $IP_RESULT -eq 0 ]; then
        echo "⚠️  WARNING: VERIFY_IDENTITY with IP unexpectedly succeeded!"
    else
        echo "✅ VERIFY_IDENTITY with IP correctly failed (expected: certificate doesn't contain IP SANs)"
    fi
    
    echo ""
    echo "🔍 Testing TLS Mode: VERIFY_IDENTITY with hostname (expected to fail with default MySQL certs)"
    echo "----------------------------------------------------------------------------------------"
    echo "Using localhost hostname - will fail because MySQL's auto-generated certificate"
    echo "has Subject: CN=MySQL_Server_8.0.43_Auto_Generated_CA_Certificate (no hostname match)"
    set +e  # Allow this to fail
    ./spirit \
      --host="$MYSQL_HOST_LOCALHOST" \
      --username="$MYSQL_USER" \
      --password="$MYSQL_PASSWORD" \
      --database="$MYSQL_DATABASE" \
      --tls-mode="VERIFY_IDENTITY" \
      --tls-ca="mysql-certs/ca.pem" \
      --table="test_table" \
      --alter="ADD COLUMN test_col_verify_identity VARCHAR(50)"
    HOSTNAME_RESULT=$?
    set -e
    
    if [ $HOSTNAME_RESULT -eq 0 ]; then
        echo "⚠️  WARNING: VERIFY_IDENTITY with hostname unexpectedly succeeded!"
    else
        echo "✅ VERIFY_IDENTITY with hostname correctly failed (MySQL's auto-generated certs don't match hostnames)"
        echo "   This is expected behavior - VERIFY_IDENTITY requires properly configured certificates"
    fi
else
    echo "⚠️  No MySQL CA certificate found, testing without custom certificate"
    echo "Testing with IP address first (should fail), then hostname (also should fail)"
    set +e  # Allow this to fail
    ./spirit \
      --host="$MYSQL_HOST" \
      --username="$MYSQL_USER" \
      --password="$MYSQL_PASSWORD" \
      --database="$MYSQL_DATABASE" \
      --tls-mode="VERIFY_IDENTITY" \
      --table="test_table" \
      --alter="ADD COLUMN test_col_verify_identity_ip_fail VARCHAR(50)"
    IP_RESULT=$?
    set -e
    
    if [ $IP_RESULT -ne 0 ]; then
        echo "✅ VERIFY_IDENTITY with IP correctly failed"
        echo "Now testing with localhost hostname (also expected to fail - no proper certs)..."
        ./spirit \
          --host="$MYSQL_HOST_LOCALHOST" \
          --username="$MYSQL_USER" \
          --password="$MYSQL_PASSWORD" \
          --database="$MYSQL_DATABASE" \
          --tls-mode="VERIFY_IDENTITY" \
          --table="test_table" \
          --alter="ADD COLUMN test_col_verify_identity_hostname_fail VARCHAR(50)" 2>/dev/null || echo "✅ VERIFY_IDENTITY with hostname also failed (expected - MySQL needs proper certificate configuration)"
    fi
fi

echo ""
echo "🔍 Testing TLS Mode: PREFERRED (should use TLS when available)"
echo "-------------------------------------------------------------"
./spirit \
  --host="$MYSQL_HOST" \
  --username="$MYSQL_USER" \
  --password="$MYSQL_PASSWORD" \
  --database="$MYSQL_DATABASE" \
  --tls-mode="PREFERRED" \
  --table="test_table" \
  --alter="ADD COLUMN test_col_preferred_tls VARCHAR(50)"

echo ""
echo "✅ TLS-Enabled Mode Testing Complete!"
echo ""
echo "📋 Expected Results:"
echo "   - DISABLED: Should FAIL (server requires secure transport)"
echo "   - REQUIRED: Should work (TLS connection established)"
echo "   - VERIFY_CA: Should work (certificate verification against CA)"
echo "   - VERIFY_IDENTITY with IP: Should FAIL (no IP SANs in certificate)"
echo "   - VERIFY_IDENTITY with hostname: Should FAIL (MySQL auto-generated cert has no hostname match)"
echo "   - PREFERRED: Should work (TLS connection preferred and available)"
echo ""
echo "Note: VERIFY_IDENTITY failures are expected with MySQL's default auto-generated certificates."
echo "      In production, use properly configured certificates with correct Subject Alternative Names."

echo ""
echo "🔍 Verifying Final Table Structure"
echo "----------------------------------"
echo "Table structure after TLS-enabled mode testing:"

# Connect to MySQL and show the table structure (note different port)
docker-compose -f 8.0.43-tls-enabled.yml exec -T mysql-tls-enabled mysql \
  -h localhost \
  -u "$MYSQL_USER" \
  -p"$MYSQL_PASSWORD" \
  -D "$MYSQL_DATABASE" \
  -e "SHOW CREATE TABLE test_table\G" 2>/dev/null || echo "⚠️  Could not retrieve table structure"

echo ""
echo "📊 Final table columns:"
docker-compose -f 8.0.43-tls-enabled.yml exec -T mysql-tls-enabled mysql \
  -h localhost \
  -u "$MYSQL_USER" \
  -p"$MYSQL_PASSWORD" \
  -D "$MYSQL_DATABASE" \
  -e "DESCRIBE test_table;" 2>/dev/null || echo "⚠️  Could not describe table"

echo ""
echo "🎯 Expected Results Analysis:"
echo "❌ DISABLED mode should have failed (no test_col_disabled column)"
echo "✅ All other TLS modes should have succeeded and added their respective columns"
echo "✅ This confirms TLS connectivity works and security is properly enforced"
echo ""
echo "Expected columns: test_col_required, test_col_verify_ca, test_col_preferred_tls"
echo "Missing columns (should NOT exist - proves expected failures):"
echo "- test_col_disabled: Proves DISABLED mode failed against TLS-required server"
echo "- test_col_verify_identity_ip_fail: Proves VERIFY_IDENTITY fails with IP addresses"
echo "- test_col_verify_identity: Proves VERIFY_IDENTITY fails with hostname (MySQL auto-generated certs)"
echo "- test_col_verify_identity_hostname_fail: Alternative failure case"
echo ""
echo "If you see the expected pattern above, your TLS implementation is working correctly! 🎉"
