# Spirit TLS Testing Matrix

## Complete Test Matrix

This document outlines the comprehensive TLS testing scenarios for Spirit's MySQL-compatible TLS modes, including replica TLS inheritance behavior.

## Main Database TLS Modes

### TLS-Disabled Server (Port 3306) 🔓

| Mode | Expected | Reason |
|------|----------|---------|
| **DISABLED** | ✅ Success | Plain connection allowed |
| **PREFERRED** | ✅ Success | Fallback to plain when TLS fails |
| **REQUIRED** | ❌ Failure | TLS required but not available |

**Test Command:** `make cleanup && make start && make verify && make cleanup`

**Key Test:** PREFERRED mode demonstrates MySQL-compatible fallback behavior:
1. Attempts TLS connection
2. TLS fails (server has SSL disabled)
3. Falls back to plain connection
4. Migration succeeds

### TLS-Enabled Server (Port 3307) 🔐
*MySQL Default Auto-Generated Certificates*

| Mode | Expected | Reason |
|------|----------|---------|
| **DISABLED** | ❌ Failure | Server requires secure transport |
| **REQUIRED** | ✅ Success | TLS required and available |
| **VERIFY_CA** | ✅ Success | Certificate verification against CA works |
| **VERIFY_IDENTITY** | ❌ Failure | MySQL auto-generated certs lack proper hostnames |
| **PREFERRED** | ✅ Success | TLS preferred and available |

**Test Command:** `make cleanup-tls && make start-tls && make verify-tls && make cleanup-tls`

**Key Tests:** 
1. **DISABLED mode failure** proves security enforcement
2. **VERIFY_IDENTITY failure** demonstrates need for proper certificates

**VERIFY_IDENTITY Failure Details:**
- MySQL's auto-generated certificate: `CN=MySQL_Server_8.0.43_Auto_Generated_Server_Certificate`
- No Subject Alternative Names for `localhost` or IP addresses
- Fails with both `127.0.0.1` and `localhost` (expected behavior)

### Custom Certificate Server (Port 3308) 📜
*Properly Configured Certificates with Subject Alternative Names*

| Mode | Expected | Reason |
|------|----------|---------|
| **DISABLED** | ❌ Failure | Server requires secure transport |
| **REQUIRED** | ✅ Success | TLS required and available |
| **VERIFY_CA** | ✅ Success | Certificate verification against custom CA |
| **VERIFY_IDENTITY** | ✅ Success | Full verification with proper hostnames |
| **PREFERRED** | ✅ Success | TLS preferred and available |

**Test Command:** `make cleanup-custom && make start-custom && make verify-custom && make cleanup-custom`

**Key Achievement:** VERIFY_IDENTITY success demonstrates proper certificate configuration:

**Custom Certificate Details:**
- **Subject**: `CN=localhost` (matches hostname)
- **Subject Alternative Names**:
  - DNS: `localhost`, `mysql-tls-custom`
  - IP: `127.0.0.1`, `::1`
- **CA**: Custom Spirit-Test-CA (not auto-generated)

**VERIFY_IDENTITY Success Cases:**
- ✅ **IP-based connection** (`127.0.0.1:3308`) - works because cert has IP SANs
- ✅ **Hostname-based connection** (`localhost:3308`) - works because cert has DNS SANs

**Production Insight:** This demonstrates what's required for VERIFY_IDENTITY in production:
1. Generate certificates with proper Subject Alternative Names
2. Include all hostnames/IPs that clients will use to connect
3. Use a proper Certificate Authority (not auto-generated)