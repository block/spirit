#!/bin/bash

# Initialize custom certificates within MySQL container for replication TLS testing

set -e

echo "🔐 Initializing Custom TLS Certificates for MySQL Replication"
echo "============================================================="

# Create certificates directory if it doesn't exist
mkdir -p /var/lib/mysql-certs

# Check if custom certificates exist
if [ -f "/var/lib/mysql-certs/custom-ca.pem" ] && [ -f "/var/lib/mysql-certs/custom-server-cert.pem" ] && [ -f "/var/lib/mysql-certs/custom-server-key.pem" ]; then
    echo "✅ Custom certificates found, configuring MySQL to use them..."
    
    # Set proper permissions for MySQL to read the certificates
    chown mysql:mysql /var/lib/mysql-certs/custom-*.pem
    chmod 644 /var/lib/mysql-certs/custom-ca.pem
    chmod 644 /var/lib/mysql-certs/custom-server-cert.pem
    chmod 600 /var/lib/mysql-certs/custom-server-key.pem
    
    echo "📜 Certificate configuration:"
    echo "   CA: /var/lib/mysql-certs/custom-ca.pem"
    echo "   Cert: /var/lib/mysql-certs/custom-server-cert.pem"
    echo "   Key: /var/lib/mysql-certs/custom-server-key.pem"
    
    echo "✅ Custom certificate initialization complete!"
else
    echo "⚠️  Custom certificates not found in /var/lib/mysql-certs/"
    echo "   Make sure to run ./generate-custom-certs.sh first"
    echo "   MySQL will use auto-generated certificates"
fi