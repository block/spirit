#!/bin/bash

# Script to generate custom MySQL certificates for replication TLS testing
# Supports both main and replica servers with proper hostnames

set -e

echo "🔐 Generating Custom MySQL Certificates for Replication TLS Testing"
echo "=================================================================="

CERT_DIR="mysql-certs"
mkdir -p "$CERT_DIR"

# Certificate configuration
DAYS=365
KEY_SIZE=2048
COUNTRY="US"
STATE="CA"
CITY="San Francisco"
ORG="Spirit Replication Test"
OU="TLS Testing"

echo "📋 Certificate Configuration:"
echo "   - Validity: $DAYS days"
echo "   - Key Size: $KEY_SIZE bits"
echo "   - Hostnames: localhost, mysql-main, mysql-replica, 127.0.0.1"
echo "   - Organization: $ORG"

# 1. Generate CA private key
echo ""
echo "🔑 Step 1: Generating CA private key..."
openssl genrsa -out "$CERT_DIR/custom-ca-key.pem" $KEY_SIZE

# 2. Generate CA certificate
echo "🏛️  Step 2: Generating CA certificate..."
openssl req -new -x509 -days $DAYS -key "$CERT_DIR/custom-ca-key.pem" \
    -out "$CERT_DIR/custom-ca.pem" \
    -subj "/C=$COUNTRY/ST=$STATE/L=$CITY/O=$ORG/OU=$OU/CN=Spirit-Replication-Test-CA"

# 3. Generate server private key
echo "🔑 Step 3: Generating server private key..."
openssl genrsa -out "$CERT_DIR/custom-server-key.pem" $KEY_SIZE

# 4. Generate server certificate signing request
echo "📝 Step 4: Generating server certificate signing request..."
openssl req -new -key "$CERT_DIR/custom-server-key.pem" \
    -out "$CERT_DIR/custom-server.csr" \
    -subj "/C=$COUNTRY/ST=$STATE/L=$CITY/O=$ORG/OU=$OU/CN=localhost"

# 5. Create extensions file for Subject Alternative Names (includes replication hosts)
echo "📋 Step 5: Creating certificate extensions for replication..."
cat > "$CERT_DIR/server-extensions.conf" << EOF
[req_ext]
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = mysql-main
DNS.3 = mysql-replica
DNS.4 = spirit-mysql-repl-tls-main
DNS.5 = spirit-mysql-repl-tls-replica
DNS.6 = spirit-mysql-repl-custom-main
DNS.7 = spirit-mysql-repl-custom-replica
DNS.8 = spirit-mysql-repl-mixed-main
DNS.9 = spirit-mysql-repl-mixed-replica
IP.1 = 127.0.0.1
IP.2 = ::1
EOF

# 6. Generate server certificate signed by our CA with proper SANs
echo "📜 Step 6: Generating server certificate with replication hostnames..."
openssl x509 -req -in "$CERT_DIR/custom-server.csr" \
    -CA "$CERT_DIR/custom-ca.pem" \
    -CAkey "$CERT_DIR/custom-ca-key.pem" \
    -CAcreateserial \
    -out "$CERT_DIR/custom-server-cert.pem" \
    -days $DAYS \
    -extensions req_ext \
    -extfile "$CERT_DIR/server-extensions.conf"

# 7. Verify the generated certificates
echo ""
echo "✅ Certificate Generation Complete!"
echo ""
echo "📋 Verification:"
echo "🏛️  CA Certificate:"
openssl x509 -in "$CERT_DIR/custom-ca.pem" -noout -subject -dates

echo ""
echo "🖥️  Server Certificate:"
openssl x509 -in "$CERT_DIR/custom-server-cert.pem" -noout -subject -dates

echo ""
echo "🌐 Server Certificate Subject Alternative Names:"
openssl x509 -in "$CERT_DIR/custom-server-cert.pem" -noout -text | grep -A 10 "Subject Alternative Name" || echo "❌ No SANs found"

echo ""
echo "📁 Generated Files:"
ls -la "$CERT_DIR/custom-"*

echo ""
echo "🎯 Next Steps:"
echo "   1. Update MySQL containers to use custom certificates"
echo "   2. Test VERIFY_IDENTITY mode with proper hostname verification"
echo "   3. Test replication TLS with both main and replica servers"

echo ""
echo "✅ Custom certificate generation for replication complete!"