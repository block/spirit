#!/bin/bash

# Script to test Spirit TLS modes with custom certificates that support VERIFY_IDENTITY

set -e

echo "🔐 Testing Spirit TLS Modes with Custom Certificates"
echo "===================================================="
echo ""
echo "This test uses custom certificates with proper Subject Alternative Names:"
echo "  - DNS: localhost, mysql-tls-custom"
echo "  - IP: 127.0.0.1, ::1"
echo ""

# Build Spirit first (from project root)
echo "📦 Building Spirit..."
cd ../../
go build -o compose/tls/spirit ./cmd/spirit
cd compose/tls/

# Test connection configurations
MYSQL_HOST_IP="127.0.0.1:3308"          # IP address (should work with custom cert)
MYSQL_HOST_LOCALHOST="localhost:3308"    # Hostname (should work with custom cert)
MYSQL_USER="spirit"
MYSQL_PASSWORD="spirit" 
MYSQL_DATABASE="spirit_test"

echo ""
echo "🔍 Testing TLS Mode: REQUIRED (baseline - should work)"
echo "----------------------------------------------------"
./spirit \
  --host="$MYSQL_HOST_IP" \
  --username="$MYSQL_USER" \
  --password="$MYSQL_PASSWORD" \
  --database="$MYSQL_DATABASE" \
  --tls-mode="REQUIRED" \
  --table="test_table" \
  --alter="ADD COLUMN test_col_required VARCHAR(50)"

echo ""
echo "🔍 Testing TLS Mode: VERIFY_CA (should work with custom CA)"
echo "----------------------------------------------------------"
./spirit \
  --host="$MYSQL_HOST_IP" \
  --username="$MYSQL_USER" \
  --password="$MYSQL_PASSWORD" \
  --database="$MYSQL_DATABASE" \
  --tls-mode="VERIFY_CA" \
  --tls-ca="mysql-certs/custom-ca.pem" \
  --table="test_table" \
  --alter="ADD COLUMN test_col_verify_ca VARCHAR(50)"

echo ""
echo "🔍 Testing TLS Mode: VERIFY_IDENTITY with IP (should work - custom cert has IP SANs)"
echo "----------------------------------------------------------------------------------"
./spirit \
  --host="$MYSQL_HOST_IP" \
  --username="$MYSQL_USER" \
  --password="$MYSQL_PASSWORD" \
  --database="$MYSQL_DATABASE" \
  --tls-mode="VERIFY_IDENTITY" \
  --tls-ca="mysql-certs/custom-ca.pem" \
  --table="test_table" \
  --alter="ADD COLUMN test_col_verify_identity_ip VARCHAR(50)"

echo ""
echo "🔍 Testing TLS Mode: VERIFY_IDENTITY with hostname (should work - custom cert has DNS SANs)"
echo "----------------------------------------------------------------------------------------"
./spirit \
  --host="$MYSQL_HOST_LOCALHOST" \
  --username="$MYSQL_USER" \
  --password="$MYSQL_PASSWORD" \
  --database="$MYSQL_DATABASE" \
  --tls-mode="VERIFY_IDENTITY" \
  --tls-ca="mysql-certs/custom-ca.pem" \
  --table="test_table" \
  --alter="ADD COLUMN test_col_verify_identity_hostname VARCHAR(50)"

echo ""
echo "🔍 Testing TLS Mode: PREFERRED (should use TLS when available)"
echo "-------------------------------------------------------------"
./spirit \
  --host="$MYSQL_HOST_IP" \
  --username="$MYSQL_USER" \
  --password="$MYSQL_PASSWORD" \
  --database="$MYSQL_DATABASE" \
  --tls-mode="PREFERRED" \
  --table="test_table" \
  --alter="ADD COLUMN test_col_preferred_tls VARCHAR(50)"

echo ""
echo "✅ Custom Certificate TLS Testing Complete!"
echo ""
echo "📋 Expected Results:"
echo "   - REQUIRED: Should work (TLS connection established)"
echo "   - VERIFY_CA: Should work (certificate verification against custom CA)"
echo "   - VERIFY_IDENTITY with IP: Should work (custom cert has IP SANs for 127.0.0.1)"
echo "   - VERIFY_IDENTITY with hostname: Should work (custom cert has DNS SANs for localhost)"
echo "   - PREFERRED: Should work (TLS connection preferred and available)"

echo ""
echo "🔍 Verifying Final Table Structure"
echo "----------------------------------"
echo "Table structure after custom certificate TLS testing:"

# Connect to MySQL and show the table structure
docker-compose -f 8.0.43-tls-custom.yml exec -T mysql-tls-custom mysql \
  -h localhost \
  -u "$MYSQL_USER" \
  -p"$MYSQL_PASSWORD" \
  -D "$MYSQL_DATABASE" \
  -e "SHOW CREATE TABLE test_table\G" 2>/dev/null || echo "⚠️  Could not retrieve table structure"

echo ""
echo "📊 Final table columns:"
docker-compose -f 8.0.43-tls-custom.yml exec -T mysql-tls-custom mysql \
  -h localhost \
  -u "$MYSQL_USER" \
  -p"$MYSQL_PASSWORD" \
  -D "$MYSQL_DATABASE" \
  -e "DESCRIBE test_table;" 2>/dev/null || echo "⚠️  Could not describe table"

echo ""
echo "🎯 Expected Results Analysis:"
echo "✅ All TLS modes should have succeeded with custom certificates"
echo "✅ This proves VERIFY_IDENTITY works when certificates are properly configured"
echo ""
echo "Expected columns: test_col_required, test_col_verify_ca, test_col_verify_identity_ip, test_col_verify_identity_hostname, test_col_preferred_tls"
echo ""
echo "🔑 Key Difference from Default MySQL Certificates:"
echo "   - Custom certificates include proper Subject Alternative Names"
echo "   - IP SANs: 127.0.0.1, ::1 (enables IP-based VERIFY_IDENTITY)"
echo "   - DNS SANs: localhost, mysql-tls-custom (enables hostname-based VERIFY_IDENTITY)"
echo "   - This demonstrates what's required for production VERIFY_IDENTITY usage"
echo ""
echo "If you see all expected columns above, VERIFY_IDENTITY is working correctly! 🎉"
