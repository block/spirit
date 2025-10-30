# Spirit Replication TLS Testing Matrix

Docker Compose tests for Spirit's replica TLS support, validating both replica throttler and binary log replication TLS inheritance.

## Quick Start

```bash
# Test standard TLS inheritance (default MySQL certificates)
make cleanup && make start && make verify && make cleanup

# Test full VERIFY_IDENTITY support (custom certificates)
make cleanup-custom && make generate-certs && make start-custom && make verify-custom && make cleanup-custom

# Test mixed TLS environments (main TLS enabled, replica TLS disabled)
make cleanup-mixed && make start-mixed && make verify-mixed && make cleanup-mixed
```

## Test Environments

- **Standard TLS** (ports 3400-3401): Both servers with TLS enabled, default certificates
- **Custom TLS** (ports 3410-3411): Both servers with custom certificates supporting VERIFY_IDENTITY  
- **Mixed TLS** (ports 3420-3421): Main server TLS enabled, replica server TLS disabled

## Complete TLS Configuration Matrix

This comprehensive matrix shows all possible combinations of main DB TLS modes and replica DSN configurations, with exact end results for both connections.

### Matrix Legend
- 🟢 **TLS Enabled** - Encrypted connection established
- 🔴 **TLS Disabled** - Plain text connection  
- 🟡 **TLS Conditional** - Depends on server capabilities
- 🔒 **Preserved** - Existing DSN setting maintained
- 🏷️ **RDS Auto** - Automatic RDS certificate detection

### Complete TLS Behavior Matrix

This matrix shows how Spirit's TLS inheritance works with **valid MySQL driver parameters** and **actual named TLS configurations**.

#### Key Implementation Details:
- **Inheritance**: Only when replica DSN has no `tls=` parameter and main TLS ≠ `DISABLED`
- **Named Configs**: Spirit uses registered TLS configurations (`custom`, `rds`, `required`, `verify_ca`, `verify_identity`)
- **RDS Detection**: Automatic in all TLS modes for `*.rds.amazonaws.com` hosts
- **Parameter Preservation**: Existing `tls=` parameters in replica DSN are always preserved (no inheritance)
- **Host-Specific Certificates**: Even with the same `--tls-mode` command, each connection gets host-appropriate certificates (RDS hosts use RDS certs, non-RDS hosts use custom certs)

| Main DB CLI Flags | Main TLS Config | Replica DSN | Replica TLS Config | Final Behavior | Use Case |
|-------------------|-----------------|-------------|-------------------|----------------|----------|
| **Default Behavior (PREFERRED)** |
| *(no --tls-mode)* | `tls=custom` | `user:pass@tcp(replica:3306)/db` | `tls=custom` (inherited) | Both: Conditional TLS | Standard setup |
| *(no --tls-mode)* | `tls=custom` | `user:pass@tcp(replica:3306)/db?tls=false` | No TLS | Main: Conditional, Replica: Disabled | Performance optimization |
| *(no --tls-mode)* | `tls=custom` | `user:pass@tcp(replica:3306)/db?tls=true` | Force TLS | Main: Conditional, Replica: Required | Explicit replica security |
| *(no --tls-mode)* | `tls=custom` | `user:pass@tcp(replica:3306)/db?tls=preferred` | Conditional TLS | Both: Conditional TLS | Explicit conditional |
| **DISABLED Mode** |
| `--tls-mode DISABLED` | No TLS | `user:pass@tcp(replica:3306)/db` | No TLS (no inheritance) | Both: Plain text | No encryption anywhere |
| `--tls-mode DISABLED` | No TLS | `user:pass@tcp(replica:3306)/db?tls=false` | No TLS | Both: Plain text | Explicit confirmation |
| `--tls-mode DISABLED` | No TLS | `user:pass@tcp(replica:3306)/db?tls=preferred` | Conditional TLS | Main: Plain, Replica: Conditional | Override for replica |
| `--tls-mode DISABLED` | No TLS | `user:pass@tcp(replica:3306)/db?tls=true` | Force TLS | Main: Plain, Replica: Required | Security override |
| **REQUIRED Mode** |
| `--tls-mode REQUIRED` | `tls=required` | `user:pass@tcp(replica:3306)/db` | `tls=required` (inherited) | Both: Required TLS | Uniform encryption |
| `--tls-mode REQUIRED` | `tls=required` | `user:pass@tcp(replica:3306)/db?tls=false` | No TLS | Main: Required, Replica: Disabled | Performance exception |
| `--tls-mode REQUIRED` | `tls=required` | `user:pass@tcp(replica:3306)/db?tls=preferred` | Conditional TLS | Main: Required, Replica: Conditional | Downgrade replica |
| **VERIFY_CA Mode** |
| `--tls-mode VERIFY_CA` | `tls=verify_ca` | `user:pass@tcp(replica:3306)/db` | `tls=verify_ca` (inherited) | Both: CA verification | Certificate validation |
| `--tls-mode VERIFY_CA` | `tls=verify_ca` | `user:pass@tcp(replica:3306)/db?tls=false` | No TLS | Main: CA verify, Replica: Disabled | Security/performance split |
| `--tls-mode VERIFY_CA` | `tls=verify_ca` | `user:pass@tcp(replica:3306)/db?tls=true` | Force TLS | Main: CA verify, Replica: Basic TLS | Different security levels |
| **VERIFY_IDENTITY Mode** |
| `--tls-mode VERIFY_IDENTITY` | `tls=verify_identity` | `user:pass@tcp(replica:3306)/db` | `tls=verify_identity` (inherited) | Both: Full verification | Maximum security |
| `--tls-mode VERIFY_IDENTITY` | `tls=verify_identity` | `user:pass@tcp(replica:3306)/db?tls=false` | No TLS | Main: Full verify, Replica: Disabled | High security/fast replica |
| `--tls-mode VERIFY_IDENTITY` | `tls=verify_identity` | `user:pass@tcp(replica:3306)/db?tls=preferred` | Conditional TLS | Main: Full verify, Replica: Conditional | Flexible replica |
| **Custom Certificate Scenarios** |
| `--tls-mode REQUIRED --tls-ca /path/ca.pem` | `tls=required` + custom CA | `user:pass@tcp(replica:3306)/db` | `tls=required` + custom CA | Both: Custom PKI | Corporate certificates |
| `--tls-mode VERIFY_CA --tls-ca /path/ca.pem` | `tls=verify_ca` + custom CA | `user:pass@tcp(replica:3306)/db?tls=true` | Force TLS | Main: Custom CA verify, Replica: Basic TLS | Mixed certificate validation |
| **RDS Auto-Detection Scenarios** |
| `--tls-mode REQUIRED` + RDS host | `tls=rds` (auto-detected) | `user:pass@tcp(replica.rds.amazonaws.com:3306)/db` | `tls=rds` (inherited) | Both: RDS certificates | AWS RDS deployment |
| `--tls-mode REQUIRED` + RDS host | `tls=rds` (auto-detected) | `user:pass@tcp(local-replica:3306)/db` | `tls=required` (non-RDS) | Main: RDS certs, Replica: Custom certs | Mixed environments |

#### Valid MySQL Driver TLS Parameters:
- `tls=false` - Disable TLS completely
- `tls=true` - Enable TLS with basic verification  
- `tls=preferred` - Use TLS if available, fallback to plain text
- `tls=skip-verify` - Enable TLS but skip certificate verification
- `tls=<config-name>` - Use named TLS configuration (Spirit's approach)

#### Spirit's Named TLS Configurations:
- `custom` - PREFERRED mode configuration
- `required` - REQUIRED mode configuration  
- `verify_ca` - VERIFY_CA mode configuration
- `verify_identity` - VERIFY_IDENTITY mode configuration
- `rds` - Amazon RDS certificate bundle