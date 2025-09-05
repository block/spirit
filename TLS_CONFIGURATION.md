# TLS Configuration Guide

This guide explains how to configure TLS connections in Spirit using MySQL-compatible SSL modes.

## Overview

Spirit uses the same TLS/SSL mode options as the MySQL client, making it familiar and intuitive for users. 

The system also automatically detects Amazon RDS hostname's and applies appropriate TLS settings.

## TLS Mode Options

| Mode | Description | Encryption | CA Verification | Hostname Verification | --tls-ca Required? |
|------|-------------|------------|-----------------|----------------------|-------------------|
| `DISABLED` | No TLS encryption | ❌ No | ❌ No | ❌ No | ❌ Never needed |
| `PREFERRED` | TLS if server supports it (default) | ✅ If available | ❌ No | ❌ No | ❌ Never needed |
| `REQUIRED` | TLS required, connection fails if unavailable | ✅ Required | ❌ No | ❌ No | ❌ Never needed |
| `VERIFY_CA` | TLS required + verify server certificate | ✅ Required | ✅ Yes | ❌ No | ⚠️ Optional* |
| `VERIFY_IDENTITY` | Full verification including hostname | ✅ Required | ✅ Yes | ✅ Yes | ⚠️ Optional* |

**\* Optional but recommended**: These modes can use the embedded RDS certificate bundle as a fallback, but providing `--tls-ca` gives you full control over which Certificate Authorities are trusted.

### Key Difference: VERIFY_CA vs VERIFY_IDENTITY

**VERIFY_CA**: Verifies the certificate is signed by a trusted CA **from your `--tls-ca` bundle**, but **allows hostname mismatches**.
```bash
# ✅ This works - certificate is valid but hostname doesn't match
spirit --tls-mode VERIFY_CA --tls-ca /path/to/ca.pem --host 192.168.1.100:3306
# Server cert is for "mysql.company.com" but connecting to IP - allowed
# Certificate MUST be signed by a CA in your /path/to/ca.pem bundle
```

**VERIFY_IDENTITY**: Full verification including **exact hostname matching**.
```bash
# ❌ This fails - hostname mismatch rejected
spirit --tls-mode VERIFY_IDENTITY --tls-ca /path/to/ca.pem --host 192.168.1.100:3306
# Error: "certificate is valid for mysql.company.com not 192.168.1.100"

# ✅ This works - hostname matches certificate
spirit --tls-mode VERIFY_IDENTITY --tls-ca /path/to/ca.pem --host mysql.company.com:3306
```

**Use VERIFY_CA when**: Connecting via IP addresses, load balancers, or when hostname differs from certificate.  
**Use VERIFY_IDENTITY when**: Maximum security is required and hostname exactly matches the certificate.

### VERIFY_CA Certificate Trust Logic

When using `VERIFY_CA` mode, Spirit implements custom certificate verification that provides security without hostname restrictions:

1. **Technical Implementation**
   - Uses `InsecureSkipVerify=true` to bypass Go's default TLS verification
   - Implements custom `VerifyPeerCertificate` function for selective validation
   - Your `--tls-ca` bundle (or embedded RDS bundle) defines trusted Certificate Authorities

2. **What gets verified:**
   - ✅ **Certificate chain validation**: Full cryptographic verification against your CA bundle
   - ✅ **Expiration dates**: Certificate must be valid and not expired  
   - ✅ **CA signature**: Must be signed by a CA in your certificate bundle
   - ✅ **Intermediate certificates**: Properly handles certificate chains
   - ❌ **Hostname matching**: Intentionally skipped (allows IP addresses, load balancers, etc.)

3. **Certificate Authority Trust**
   - Only Certificate Authorities in your specified bundle are trusted
   - If server's certificate is signed by a CA **not** in your bundle → **Connection fails**
   - If server's certificate is signed by a CA **in** your bundle → **Connection succeeds** (regardless of hostname)

4. **Example scenarios:**
   ```bash
   # Your bundle contains "CompanyCA"
   spirit --tls-mode VERIFY_CA --tls-ca /path/to/company-ca.pem --host 192.168.1.100:3306
   
   # ✅ Server cert signed by "CompanyCA" → Works (even with IP address)
   # ❌ Server cert signed by "SomeOtherCA" → Fails (not in your bundle)
   # ✅ Server cert expired but signed by "CompanyCA" → Fails (expiration checked)
   ```

This makes `VERIFY_CA` perfect for company-internal environments where you have your own Certificate Authority but need flexibility with hostnames/IPs.

## When Do You Need --tls-ca?

### **Never Need --tls-ca:**

**DISABLED, PREFERRED, REQUIRED modes** - These modes either don't use TLS or don't verify certificates:

```bash
# These work without --tls-ca flag
spirit --tls-mode DISABLED --host mysql.com:3306 ...     # No TLS at all
spirit --tls-mode PREFERRED --host mysql.com:3306 ...    # Auto TLS detection  
spirit --tls-mode REQUIRED --host mysql.com:3306 ...     # TLS encryption only
```

### **Optional for --tls-ca:**

**VERIFY_CA and VERIFY_IDENTITY modes** - These modes verify certificates but have fallback behavior:

```bash
# Works WITHOUT --tls-ca (uses embedded RDS bundle for all hosts)
spirit --tls-mode VERIFY_CA --host mysql.company.com:3306 ...
spirit --tls-mode VERIFY_IDENTITY --host mysql.company.com:3306 ...

# Works WITH --tls-ca (uses your custom certificate authority)
spirit --tls-mode VERIFY_CA --tls-ca /path/to/custom-ca.pem --host mysql.company.com:3306 ...
spirit --tls-mode VERIFY_IDENTITY --tls-ca /path/to/custom-ca.pem --host mysql.company.com:3306 ...
```

### **When to Use --tls-ca:**

1. **Company-internal Certificate Authority**: Your MySQL server uses certificates signed by your company's CA
2. **Custom Certificate Setup**: You want to control exactly which CAs are trusted
3. **Newer RDS Certificates**: You want to use a newer AWS RDS certificate bundle than the embedded one
4. **Security Policy Requirements**: Your organization requires explicit certificate authority specification

### **When NOT to Use --tls-ca:**

1. **Default RDS Connections**: The embedded certificate works fine for most RDS instances
2. **Testing/Development**: When you just want basic TLS without certificate management
3. **Self-signed Certificates**: Use `--tls-mode REQUIRED` instead (no certificate verification)

## Configuration Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--tls-mode` | TLS connection mode (see table above) | `PREFERRED` |
| `--tls-ca` | Path to custom TLS CA certificate file | `""` |

### tls-ca
When you provide `--tls-ca`, Spirit uses your custom certificate exclusively for all certificate validation. When no `--tls-ca` is provided, Spirit falls back to the embedded RDS certificate bundle for all hosts, providing reasonable certificate validation even for non-RDS MySQL servers.

**Key behavior:**
- **With `--tls-ca`**: Your certificate becomes the sole authority
- **Without `--tls-ca`**: Embedded RDS bundle used for all verification modes
- **RDS auto-detection**: RDS hosts automatically get appropriate TLS settings regardless of `--tls-ca`

This approach gives you full control while providing sensible defaults when no custom certificate is specified.

### Precedence for CA selection:
1. **Custom certificate via `--tls-ca` has HIGHEST priority**
   - If you provide `--tls-ca /path/to/custom-ca.pem`, Spirit uses your custom certificate exclusively
   - Your custom certificate becomes the sole authority for all certificate validation

2. **RDS auto-detection for RDS hosts**
   - For RDS hostnames (ending in `.rds.amazonaws.com`), Spirit automatically uses the `tls=rds` configuration
   - Uses the embedded RDS certificate bundle for verification

3. **Embedded RDS bundle as fallback for all hosts**
   - For non-RDS hosts when no custom certificate is provided, Spirit still uses the embedded RDS bundle
   - This provides reasonable certificate validation even for non-RDS MySQL servers
   - Uses mode-specific TLS configurations: `tls=required`, `tls=verify_ca`, `tls=verify_identity`

## Usage Examples

### 1. Default Behavior (PREFERRED Mode)

**Scenario**: Let Spirit automatically handle TLS based on the server type.

```bash
# For RDS hosts - automatically uses TLS with embedded certificate
spirit --host mydb.us-west-2.rds.amazonaws.com:3306 \
       --username myuser \
       --password mypass \
       --database mydb \
       --table mytable \
       --alter "ADD INDEX idx_column (column)"

# For non-RDS hosts - no TLS unless server advertises support
spirit --host myserver.internal:3306 \
       --username myuser \
       --password mypass \
       --database mydb \
       --table mytable \
       --alter "ADD INDEX idx_column (column)"
```

### 2. Disable TLS Completely

**Scenario**: Explicitly disable TLS encryption.

```bash
spirit --tls-mode DISABLED \
       --host myserver.com:3306 \
       --username myuser \
       --password mypass \
       --database mydb \
       --table mytable \
       --alter "ADD INDEX idx_column (column)"
```

**Use case**: Testing, legacy systems, or when connecting through secure tunnels.

### 3. Force TLS Encryption

**Scenario**: Require TLS but don't verify certificates (useful for self-signed certificates).

```bash
spirit --tls-mode REQUIRED \
       --host myserver.internal:3306 \
       --username myuser \
       --password mypass \
       --database mydb \
       --table mytable \
       --alter "ADD INDEX idx_column (column)"
```

**Use case**: Connecting to servers with self-signed certificates or when you want encryption but can't verify the certificate.

### 4. Custom Certificate Authority

**Scenario**: Use a custom CA certificate for verification.

```bash
spirit --tls-mode VERIFY_CA \
       --tls-ca /path/to/custom-ca.pem \
       --host mysql.company.com:3306 \
       --username myuser \
       --password mypass \
       --database mydb \
       --table mytable \
       --alter "ADD INDEX idx_column (column)"
```

**Use case**: Company-internal certificate authorities or custom-signed certificates.

### 5. Full Certificate Verification

**Scenario**: Maximum security with full certificate and hostname verification.

```bash
spirit --tls-mode VERIFY_IDENTITY \
       --tls-ca /path/to/ca-cert.pem \
       --host mysql.example.com:3306 \
       --username myuser \
       --password mypass \
       --database mydb \
       --table mytable \
       --alter "ADD INDEX idx_column (column)"
```

**Use case**: Production environments where security is paramount.

### 6. RDS with Custom Certificate

**Scenario**: Use a newer RDS certificate instead of the embedded one.

```bash
spirit --tls-mode VERIFY_IDENTITY \
       --tls-ca /path/to/rds-ca-2023-root.pem \
       --host mydb.us-west-2.rds.amazonaws.com:3306 \
       --username myuser \
       --password mypass \
       --database mydb \
       --table mytable \
       --alter "ADD INDEX idx_column (column)"
```

**Use case**: Using newer RDS certificate bundles or custom RDS configurations.

## Complete TLS Mode Examples with Column Addition

Here are comprehensive examples showing Spirit commands for each TLS mode, adding a column to demonstrate real-world usage:

### 1. DISABLED Mode - No TLS Encryption

```bash
# Add a new column with TLS completely disabled
spirit --tls-mode DISABLED \
       --host mysql.internal.company.com:3306 \
       --username migration_user \
       --password secretpassword \
       --database ecommerce \
       --table orders \
       --alter "ADD COLUMN order_notes TEXT AFTER order_status" \
       --threads 4 \
       --chunk-size 1000
```

**Result**: No TLS encryption used, fastest performance but no security.

### 2. PREFERRED Mode - Default Behavior

```bash
# Add a column with automatic TLS detection (default mode)
spirit --tls-mode PREFERRED \
       --host mydb.us-west-2.rds.amazonaws.com:3306 \
       --username admin \
       --password mypassword \
       --database production \
       --table users \
       --alter "ADD COLUMN last_login_ip VARCHAR(45) AFTER last_login" \
       --threads 8 \
       --chunk-size 2000
```

**Result**: Automatically uses TLS for RDS hosts with embedded certificates, optional for others.

### 3. REQUIRED Mode - Force TLS Without Certificate Verification

```bash
# Add a column requiring TLS but not verifying certificates
spirit --tls-mode REQUIRED \
       --host mysql.staging.company.com:3306 \
       --username staging_user \
       --password staging_pass \
       --database inventory \
       --table products \
       --alter "ADD COLUMN supplier_notes JSON AFTER supplier_id" \
       --threads 6 \
       --chunk-size 1500
```

**Result**: TLS encryption required, but accepts self-signed or invalid certificates.

### 4. VERIFY_CA Mode - Certificate Verification Without Hostname Check

```bash
# Add a column with CA verification using custom certificate
spirit --tls-mode VERIFY_CA \
       --tls-ca /etc/ssl/certs/company-ca-bundle.pem \
       --host 192.168.1.100:3306 \
       --username app_user \
       --password app_password \
       --database analytics \
       --table events \
       --alter "ADD COLUMN event_metadata JSON AFTER event_type" \
       --threads 4 \
       --chunk-size 1000
```

**Result**: Verifies certificate against custom CA bundle but allows IP addresses/hostname mismatches.

### 5. VERIFY_CA Mode - Using Embedded RDS Certificate for Non-RDS Host

```bash
# Add a column using embedded RDS certificate for non-RDS MySQL server
spirit --tls-mode VERIFY_CA \
       --host mysql.internal.corp:3306 \
       --username internal_user \
       --password internal_pass \
       --database hr_system \
       --table employees \
       --alter "ADD COLUMN emergency_contact VARCHAR(255) AFTER phone_number" \
       --threads 2 \
       --chunk-size 500
```

**Result**: Uses embedded RDS certificate bundle as fallback for certificate verification.

### 6. VERIFY_IDENTITY Mode - Full Certificate and Hostname Verification

```bash
# Add a column with maximum security verification
spirit --tls-mode VERIFY_IDENTITY \
       --tls-ca /opt/certificates/production-ca.pem \
       --host mysql.secure.company.com:3306 \
       --username secure_user \
       --password very_secure_password \
       --database financial \
       --table transactions \
       --alter "ADD COLUMN fraud_score DECIMAL(5,4) AFTER amount" \
       --threads 8 \
       --chunk-size 2000
```

**Result**: Full TLS verification including hostname matching - maximum security.

### 7. VERIFY_IDENTITY Mode - RDS with Automatic Certificate

```bash
# Add a column to RDS with full verification using auto-detected certificate
spirit --tls-mode VERIFY_IDENTITY \
       --host prod-db.cluster-xyz.us-east-1.rds.amazonaws.com:3306 \
       --username rds_admin \
       --password rds_password \
       --database customer_data \
       --table profiles \
       --alter "ADD COLUMN gdpr_consent_date DATETIME AFTER created_at" \
       --threads 10 \
       --chunk-size 3000
```

**Result**: Uses embedded RDS certificate with full verification for RDS hostname.

### 8. Custom Certificate Override for RDS

```bash
# Add a column to RDS using newer custom certificate instead of embedded one
spirit --tls-mode VERIFY_IDENTITY \
       --tls-ca /home/user/aws-rds-ca-cert-2024.pem \
       --host legacy-db.us-west-2.rds.amazonaws.com:3306 \
       --username legacy_user \
       --password legacy_password \
       --database legacy_system \
       --table audit_log \
       --alter "ADD COLUMN compliance_notes TEXT AFTER audit_type" \
       --threads 4 \
       --chunk-size 1000
```

**Result**: Custom certificate takes precedence over RDS auto-detection.

### 9. Large Table Migration with TLS

```bash
# Add a column to a large table with optimized settings and TLS
spirit --tls-mode VERIFY_CA \
       --tls-ca /etc/ssl/company-root-ca.pem \
       --host mysql-primary.datacenter.com:3306 \
       --username migration_admin \
       --password migration_password \
       --database bigdata \
       --table user_activities \
       --alter "ADD COLUMN session_metadata JSON AFTER session_id" \
       --threads 16 \
       --chunk-size 5000 \
       --max-lag-millis 1000 \
       --nice-ratio 0.1
```

**Result**: Efficient migration of large table with certificate verification but flexible hostname handling.

### 10. Development Environment Example

```bash
# Add a column in development with minimal TLS requirements
spirit --tls-mode REQUIRED \
       --host localhost:3306 \
       --username dev_user \
       --password dev_password \
       --database test_app \
       --table feature_flags \
       --alter "ADD COLUMN feature_description TEXT AFTER feature_name" \
       --threads 2 \
       --chunk-size 100
```

**Result**: TLS encryption for development but no certificate verification needed.

## TLS Configuration Verification

To verify your TLS configuration is working correctly, check the Spirit logs for TLS configuration registration messages:

```bash
# Run with verbose logging to see TLS config registration
spirit --tls-mode VERIFY_CA \
       --tls-ca /path/to/ca.pem \
       --host mysql.company.com:3306 \
       --username user \
       --password pass \
       --database testdb \
       --table testtable \
       --alter "ADD COLUMN test_col INT" \
       --verbose
```

Look for log messages indicating:
- `tls=verify_ca` configuration registered successfully
- Certificate bundle loaded from specified path
- TLS handshake completion

## Certificate File Examples

### Company CA Certificate Bundle
```bash
# /etc/ssl/certs/company-ca-bundle.pem
-----BEGIN CERTIFICATE-----
MIIDXTCCAkWgAwIBAgIJAKoK/heBjcOuMA0GCSqGSIb3DQEBBQUAMEUxCzAJBgNV
[... certificate content ...]
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
[... additional CA certificates if needed ...]
-----END CERTIFICATE-----
```

### AWS RDS Certificate Bundle
```bash
# Download latest RDS certificate bundle
wget https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
# Use with Spirit
spirit --tls-mode VERIFY_IDENTITY --tls-ca ./global-bundle.pem --host mydb.rds.amazonaws.com:3306 ...
```

## RDS Auto-Detection

Spirit automatically detects Amazon RDS hostnames (ending in `.rds.amazonaws.com`) and applies TLS appropriately:

- **DISABLED**: No TLS even for RDS hosts
- **PREFERRED**: Automatic TLS for RDS hosts, optional for others
- **REQUIRED/VERIFY_CA/VERIFY_IDENTITY**: TLS required for all hosts, RDS gets automatic certificate selection

## Security Recommendations

### ✅ Secure Configurations

- **Production**: Use `VERIFY_IDENTITY` with proper CA certificates
- **Staging**: Use `VERIFY_CA` for certificate validation without hostname checks
- **RDS**: Default `PREFERRED` mode is secure (auto-detects and uses embedded certificates)

### ⚠️ Less Secure Configurations

- **REQUIRED**: Encrypts data but doesn't verify certificate authenticity
- **PREFERRED**: May fall back to unencrypted connections for non-RDS hosts

### ❌ Insecure Configurations

- **DISABLED**: No encryption at all

## Troubleshooting

### Common Error Messages

1. **"x509: certificate signed by unknown authority"**
   - **Solution**: Use `--tls-ca` with the correct certificate or change to `REQUIRED` mode
   - **Cause**: Server certificate not trusted by default CA bundle

2. **"x509: certificate is valid for [hostname] not [your-hostname]"**
   - **Solution**: Use `VERIFY_CA` instead of `VERIFY_IDENTITY` or fix DNS/certificate
   - **Cause**: Hostname in certificate doesn't match connection hostname

3. **"tls: first record does not look like a TLS handshake"**
   - **Solution**: Server doesn't support TLS. Use `DISABLED` or check server configuration
   - **Cause**: Trying to use TLS with a server that doesn't support it

### Debug Steps

1. **Start with PREFERRED mode** to see if the connection works
2. **Check server TLS support**:
   ```sql
   SHOW VARIABLES LIKE 'have_ssl';
   ```
3. **Test certificate with openssl**:
   ```bash
   openssl s_client -connect hostname:3306 -servername hostname
   ```
4. **Gradually increase security**: PREFERRED → REQUIRED → VERIFY_CA → VERIFY_IDENTITY
5. **Verify TLS configuration registration**:
   - Check logs for TLS config registration messages
   - Look for mode-specific names: `tls=required`, `tls=verify_ca`, `tls=verify_identity`, `tls=rds`
6. **Certificate bundle verification**:
   ```bash
   # Check certificate validity
   openssl x509 -in /path/to/cert.pem -text -noout
   
   # Verify certificate chain
   openssl verify -CAfile /path/to/ca.pem /path/to/server-cert.pem
   ```

## Implementation Details

The TLS mode system works as follows:

### TLS Configuration Registration
Spirit registers mode-specific TLS configurations with the MySQL driver:
- `REQUIRED` mode → `tls=required` 
- `VERIFY_CA` mode → `tls=verify_ca`
- `VERIFY_IDENTITY` mode → `tls=verify_identity`
- RDS hosts → `tls=rds` (uses embedded certificate bundle)
- Default/PREFERRED → `tls=custom`

### Certificate Loading and Validation
1. **Certificate Selection**: 
   - Custom certificate via `--tls-ca` if provided
   - Embedded RDS certificate bundle as fallback for all modes requiring verification
2. **Mode-Specific Behavior**:
   - **REQUIRED**: Encryption only with `InsecureSkipVerify=true` (no certificate validation)
   - **VERIFY_CA**: Uses `InsecureSkipVerify=true` with custom `VerifyPeerCertificate` function that validates certificate chain but skips hostname verification
   - **VERIFY_IDENTITY**: Full standard TLS verification including hostname matching
3. **Connection Establishment**: MySQL driver uses the registered TLS configuration

### VERIFY_CA Technical Implementation
The `VERIFY_CA` mode uses a sophisticated approach:
- Sets `InsecureSkipVerify=true` to bypass Go's default verification
- Implements custom `VerifyPeerCertificate` function that:
  - Validates the full certificate chain against the CA bundle
  - Checks certificate expiration and validity
  - **Skips hostname verification** (allowing IP addresses, load balancers, etc.)
  - Handles intermediate certificates properly

This approach provides maximum compatibility with MySQL tooling while offering secure defaults and clear upgrade paths.
