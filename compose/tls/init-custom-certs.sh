#!/bin/bash

# Script to copy custom certificates during MySQL initialization
# This runs as part of the MySQL entrypoint process

set -e

echo "🔐 Copying custom TLS certificates to MySQL data directory..."

# Copy certificates from temporary location to MySQL data directory
cp /tmp/certs/ca.pem /var/lib/mysql/ca.pem
cp /tmp/certs/server-cert.pem /var/lib/mysql/server-cert.pem
cp /tmp/certs/server-key.pem /var/lib/mysql/server-key.pem

# Set proper ownership and permissions
chown mysql:mysql /var/lib/mysql/ca.pem
chown mysql:mysql /var/lib/mysql/server-cert.pem
chown mysql:mysql /var/lib/mysql/server-key.pem

chmod 644 /var/lib/mysql/ca.pem
chmod 644 /var/lib/mysql/server-cert.pem
chmod 600 /var/lib/mysql/server-key.pem

echo "✅ Custom TLS certificates copied and configured successfully"
echo "📜 Certificate details:"
echo "   CA: /var/lib/mysql/ca.pem"
echo "   Server Cert: /var/lib/mysql/server-cert.pem"
echo "   Server Key: /var/lib/mysql/server-key.pem"
