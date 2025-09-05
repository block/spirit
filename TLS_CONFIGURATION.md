# TLS Configuration Guide

This guide explains how to configure TLS connections in Spirit using MySQL-compatible SSL modes.

## Overview

Spirit uses the same TLS/SSL mode options as the MySQL client, making it familiar and intuitive for users. 

The system also automatically detects Amazon RDS hostname's and applies appropriate TLS settings.

## TLS Mode Options

| Mode | Description | Encryption | CA Verification | Hostname Verification |
|------|-------------|------------|-----------------|----------------------|
| `DISABLED` | No TLS encryption | ❌ No | ❌ No | ❌ No |
| `PREFERRED` | TLS if server supports it (default) | ✅ If available | ❌ No | ❌ No |
| `REQUIRED` | TLS required, connection fails if unavailable | ✅ Required | ❌ No | ❌ No |
| `VERIFY_CA` | TLS required + verify server certificate | ✅ Required | ✅ Yes | ❌ No |
| `VERIFY_IDENTITY` | Full verification including hostname | ✅ Required | ✅ Yes | ✅ Yes |

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

When using `VERIFY_CA` mode, your `--tls-ca` bundle defines what Certificate Authorities are considered "trusted":

1. **Your `--tls-ca` bundle is the sole authority**
   - Only Certificate Authorities in your specified bundle are trusted
   - If server's certificate is signed by a CA **not** in your bundle → **Connection fails**
   - If server's certificate is signed by a CA **in** your bundle → **Connection succeeds** (regardless of hostname)

2. **What gets verified:**
   - ✅ **Certificate chain validation**: Full cryptographic verification against your CA bundle
   - ✅ **Expiration dates**: Certificate must be valid and not expired  
   - ✅ **CA signature**: Must be signed by a CA in your `--tls-ca` bundle
   - ❌ **Hostname matching**: Ignored (allows IP addresses, load balancers, etc.)

3. **Example scenarios:**
   ```bash
   # Your bundle contains "CompanyCA"
   spirit --tls-mode VERIFY_CA --tls-ca /path/to/company-ca.pem --host 192.168.1.100:3306
   
   # ✅ Server cert signed by "CompanyCA" → Works (even with IP address)
   # ❌ Server cert signed by "SomeOtherCA" → Fails (not in your bundle)
   ```

This makes `VERIFY_CA` perfect for company-internal environments where you have your own Certificate Authority but need flexibility with hostnames/IPs.

## Configuration Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--tls-mode` | TLS connection mode (see table above) | `PREFERRED` |
| `--tls-ca` | Path to custom TLS CA certificate file | `""` |

### tls-ca
When you provide `--tls-ca`, Spirit completely disregards the embedded RDS certificate bundle, even for RDS hosts. Your custom certificate becomes the sole authority for certificate validation.

This approach gives you full control while providing sensible defaults when no custom certificate is specified.

### Precedence for CA selection:
1. Custom certificate via `--tls-ca` has HIGHEST priority
- If you provide `--tls-ca /path/to/custom-ca.pem`, Spirit will completely disregard the embedded RDS bundle
- It loads your custom certificate and uses only that for verification

2. RDS auto-detection is SECONDARY
- Only used if no `--tls-ca` is provided AND the host is an RDS hostname
- Uses the embedded RDS certificate bundle

3. Embedded RDS bundle as FALLBACK
- For non-RDS hosts when no custom certificate is provided
- Uses the embedded RDS bundle as a "reasonable default"

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

## Migration from Previous Versions

If you were using the previous TLS flags, here's how to migrate:

| Old Flags | New Equivalent |
|-----------|----------------|
| `--tls-skip-verify` | `--tls-mode REQUIRED` |
| `--tls-ca /path/cert.pem` | `--tls-mode VERIFY_CA --tls-ca /path/cert.pem` |
| `--tls-disable-auto-detect` | `--tls-mode DISABLED` |
| Default (no flags) | `--tls-mode PREFERRED` (same behavior) |

## Implementation Details

The TLS mode system works as follows:

1. **Mode Processing**: The specified `--tls-mode` determines the verification level
2. **Certificate Selection**: 
   - Custom certificate via `--tls-ca` if provided
   - Embedded RDS certificate for RDS hosts
   - Embedded RDS certificate as fallback for non-RDS hosts in verification modes
3. **Connection Establishment**: MySQL driver uses the registered TLS configuration

This approach provides maximum compatibility with MySQL tooling while offering secure defaults and clear upgrade paths.
