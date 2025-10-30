# Spirit Replication TLS Testing Makefile
# Provides commands to test TLS for replica throttler and binary log replication

.PHONY: start verify cleanup start-custom verify-custom cleanup-custom start-mixed verify-mixed cleanup-mixed generate-certs help

# Default target
all: help

# Generate custom certificates for VERIFY_IDENTITY testing
generate-certs:
	@echo "🔐 Generating custom certificates for replication TLS testing"
	@echo "=============================================================="
	@chmod +x generate-custom-certs.sh
	@./generate-custom-certs.sh

# Start MySQL replication containers with TLS enabled (default MySQL certificates)
start:
	@echo "🚀 Starting MySQL Replication with TLS ENABLED (Default Certificates)"
	@echo "====================================================================="
	@docker stop spirit-mysql-repl-tls-main spirit-mysql-repl-tls-replica 2>/dev/null || true
	@docker container prune -f
	@docker-compose -f replication-tls-enabled.yml up -d
	@echo ""
	@echo "⏳ Waiting for MySQL containers to be ready..."
	@sleep 10
	@docker-compose -f replication-tls-enabled.yml exec mysql-main mysqladmin ping -h localhost -u root -prootpassword --wait=30 --count=5
	@docker-compose -f replication-tls-enabled.yml exec mysql-replica mysqladmin ping -h localhost -u root -prootpassword --wait=30 --count=5
	@echo ""
	@echo "📋 Extracting MySQL CA certificates for testing..."
	@mkdir -p mysql-certs
	@docker-compose -f replication-tls-enabled.yml exec mysql-main cat /var/lib/mysql/ca.pem > mysql-certs/main-ca.pem 2>/dev/null || echo "Using default MySQL certs"
	@docker-compose -f replication-tls-enabled.yml exec mysql-replica cat /var/lib/mysql/ca.pem > mysql-certs/replica-ca.pem 2>/dev/null || echo "Using default MySQL certs"
	@cat mysql-certs/main-ca.pem mysql-certs/replica-ca.pem > mysql-certs/combined-ca.pem 2>/dev/null || echo "Creating combined CA file"
	@echo ""
	@echo "✅ MySQL Replication containers are ready!"
	@echo "📊 Main DB SSL Status:"
	@docker-compose -f replication-tls-enabled.yml exec mysql-main mysql -u root -prootpassword -e "SHOW VARIABLES LIKE 'have_ssl';"
	@echo "📊 Main DB Secure Transport Requirement:"
	@docker-compose -f replication-tls-enabled.yml exec mysql-main mysql -u root -prootpassword -e "SHOW VARIABLES LIKE 'require_secure_transport';"
	@echo "📊 Replica DB SSL Status:"
	@docker-compose -f replication-tls-enabled.yml exec mysql-replica mysql -u root -prootpassword -e "SHOW VARIABLES LIKE 'have_ssl';"
	@echo "📊 Replica DB Secure Transport Requirement:"
	@docker-compose -f replication-tls-enabled.yml exec mysql-replica mysql -u root -prootpassword -e "SHOW VARIABLES LIKE 'require_secure_transport';"
	@echo ""
	@echo "🎯 Ready to test replica TLS inheritance! Use 'make verify' to run tests."

# Test TLS modes with replication (default MySQL certificates)
verify:
	@echo "🔐 Testing Replica TLS Inheritance with Default MySQL Certificates"
	@echo "=================================================================="
	@echo ""
	@echo "This will test:"
	@echo "  🔄 Replica throttler TLS inheritance from main database"
	@echo "  🔄 Binary log replication TLS support"
	@echo "  ❌ DISABLED mode - should fail (servers require TLS but Spirit won't use it)"
	@echo "  ✅ REQUIRED mode - should work (TLS available on both servers)"
	@echo "  ❌ VERIFY_CA mode - expected to fail (independent CA certificates)"
	@echo "  ❌ VERIFY_IDENTITY mode - expected to fail (hostname mismatch with default certs)"
	@echo "  ✅ PREFERRED mode - should work (TLS available and preferred)"
	@echo ""
	@echo "Starting replication TLS tests..."
	@echo ""
	@chmod +x test-replication-tls.sh
	@./test-replication-tls.sh

# Clean up TLS-enabled replication containers
cleanup:
	@echo "🧹 Cleaning up TLS-enabled replication test environment"
	@echo "======================================================"
	@docker-compose -f replication-tls-enabled.yml down -v --remove-orphans
	@docker container prune -f
	@docker network prune -f
	@rm -f spirit
	@echo ""
	@echo "✅ TLS-enabled replication cleanup complete!"

# Start MySQL replication containers with custom TLS certificates
start-custom:
	@echo "🔐 Starting MySQL Replication with CUSTOM TLS Certificates"
	@echo "=========================================================="
	@docker stop spirit-mysql-repl-custom-main spirit-mysql-repl-custom-replica 2>/dev/null || true
	@docker container prune -f
	@echo "📜 Ensuring custom certificates exist..."
	@if [ ! -f mysql-certs/custom-ca.pem ]; then \
		echo "⚠️  Custom certificates not found. Generating them..."; \
		./generate-custom-certs.sh; \
	else \
		echo "✅ Custom certificates found"; \
	fi
	@chmod +x init-custom-certs.sh
	@docker-compose -f replication-tls-custom.yml up -d
	@echo ""
	@echo "⏳ Waiting for custom-cert MySQL containers to be ready..."
	@sleep 10
	@docker-compose -f replication-tls-custom.yml exec mysql-main mysqladmin ping -h localhost -u root -prootpassword --wait=30 --count=5
	@docker-compose -f replication-tls-custom.yml exec mysql-replica mysqladmin ping -h localhost -u root -prootpassword --wait=30 --count=5
	@echo ""
	@echo "✅ Custom-certificate MySQL Replication containers are ready!"
	@echo "📊 Main DB SSL Status:"
	@docker-compose -f replication-tls-custom.yml exec mysql-main mysql -u root -prootpassword -e "SHOW VARIABLES LIKE 'have_ssl';"
	@echo "📊 Main DB Secure Transport Requirement:"
	@docker-compose -f replication-tls-custom.yml exec mysql-main mysql -u root -prootpassword -e "SHOW VARIABLES LIKE 'require_secure_transport';"
	@echo "📊 Replica DB SSL Status:"
	@docker-compose -f replication-tls-custom.yml exec mysql-replica mysql -u root -prootpassword -e "SHOW VARIABLES LIKE 'have_ssl';"
	@echo "📊 Replica DB Secure Transport Requirement:"
	@docker-compose -f replication-tls-custom.yml exec mysql-replica mysql -u root -prootpassword -e "SHOW VARIABLES LIKE 'require_secure_transport';"
	@echo ""
	@echo "📜 Certificate verification:"
	@echo "Custom CA Subject:"
	@openssl x509 -in mysql-certs/custom-ca.pem -noout -subject
	@echo "Server Certificate Subject:"
	@openssl x509 -in mysql-certs/custom-server-cert.pem -noout -subject
	@echo "Server Certificate SANs:"
	@openssl x509 -in mysql-certs/custom-server-cert.pem -noout -text | grep -A 5 "Subject Alternative Name" || echo "No SANs found"
	@echo ""
	@echo "🎯 Ready for full VERIFY_IDENTITY replication testing! Use 'make verify-custom' to test."

# Test TLS modes with custom certificates (including working VERIFY_IDENTITY)
verify-custom:
	@echo "🔐 Testing Replica TLS with Custom Certificates"
	@echo "=============================================="
	@echo ""
	@echo "This tests VERIFY_IDENTITY in working scenarios:"
	@echo "  ✅ All TLS modes should work with properly configured certificates"
	@echo "  ✅ VERIFY_IDENTITY should work with both IP addresses and hostnames"
	@echo "  🔄 Replica throttler should inherit TLS from main database"
	@echo "  🔄 Binary log replication should use same TLS as main database"
	@echo "  📜 Demonstrates proper certificate configuration for production use"
	@echo ""
	@echo "Starting custom certificate replication tests..."
	@echo ""
	@chmod +x test-replication-tls-custom.sh
	@./test-replication-tls-custom.sh

# Clean up custom certificate replication containers
cleanup-custom:
	@echo "🧹 Cleaning up custom certificate replication test environment"
	@echo "=============================================================="
	@docker-compose -f replication-tls-custom.yml down -v --remove-orphans
	@docker container prune -f
	@docker network prune -f
	@rm -f spirit
	@echo ""
	@echo "✅ Custom certificate replication cleanup complete!"

# Start mixed TLS environment (main TLS enabled, replica TLS disabled)
start-mixed:
	@echo "🔄 Starting Mixed TLS Environment (Main: TLS ON, Replica: TLS OFF)"
	@echo "================================================================="
	@docker stop spirit-mysql-repl-mixed-main spirit-mysql-repl-mixed-replica 2>/dev/null || true
	@docker container prune -f
	@docker-compose -f replication-tls-mixed.yml up -d
	@echo ""
	@echo "⏳ Waiting for mixed TLS containers to be ready..."
	@sleep 10
	@docker-compose -f replication-tls-mixed.yml exec mysql-main mysqladmin ping -h localhost -u root -prootpassword --wait=30 --count=5
	@docker-compose -f replication-tls-mixed.yml exec mysql-replica mysqladmin ping -h localhost -u root -prootpassword --wait=30 --count=5
	@echo ""
	@echo "✅ Mixed TLS environment is ready!"
	@echo "📊 Main DB SSL Status (should be ON):"
	@docker-compose -f replication-tls-mixed.yml exec mysql-main mysql -u root -prootpassword -e "SHOW VARIABLES LIKE 'have_ssl';"
	@echo "📊 Main DB Secure Transport Requirement:"
	@docker-compose -f replication-tls-mixed.yml exec mysql-main mysql -u root -prootpassword -e "SHOW VARIABLES LIKE 'require_secure_transport';"
	@echo "📊 Replica DB SSL Status (should be OFF):"
	@docker-compose -f replication-tls-mixed.yml exec mysql-replica mysql -u root -prootpassword -e "SHOW VARIABLES LIKE 'have_ssl';"
	@echo "📊 Replica DB Secure Transport Requirement:"
	@docker-compose -f replication-tls-mixed.yml exec mysql-replica mysql -u root -prootpassword -e "SHOW VARIABLES LIKE 'require_secure_transport';"
	@echo ""
	@echo "🎯 Ready to test mixed TLS scenarios! Use 'make verify-mixed' to test."

# Test mixed TLS scenarios (main TLS enabled, replica TLS disabled)
verify-mixed:
	@echo "🔄 Testing Mixed TLS Environment (Main TLS + Replica Non-TLS)"
	@echo "============================================================="
	@echo ""
	@echo "This tests replica TLS behavior when main and replica have different TLS configs:"
	@echo "  ✅ Main DB requires TLS (REQUIRED mode should work)"
	@echo "  🔄 Replica doesn't support TLS (should not inherit TLS from main)"
	@echo "  🎯 Tests smart DSN enhancement logic"
	@echo ""
	@echo "Starting mixed TLS scenario tests..."
	@echo ""
	@chmod +x test-replication-tls-mixed.sh
	@./test-replication-tls-mixed.sh

# Clean up mixed TLS environment
cleanup-mixed:
	@echo "🧹 Cleaning up mixed TLS test environment"
	@echo "=========================================="
	@docker-compose -f replication-tls-mixed.yml down -v --remove-orphans
	@docker container prune -f
	@docker network prune -f
	@rm -f spirit
	@echo ""
	@echo "✅ Mixed TLS cleanup complete!"

# Show help
help:
	@echo "Spirit Replication TLS Testing Makefile"
	@echo "======================================"
	@echo ""
	@echo "STANDARD TLS REPLICATION Tests (Ports 3400-3401):"
	@echo "  make start      - Start replication with TLS enabled (default certificates)"
	@echo "  make verify     - Test replica TLS inheritance with default certificates"
	@echo "  make cleanup    - Clean up standard TLS replication environment"
	@echo ""
	@echo "CUSTOM CERTIFICATE Tests (Ports 3410-3411):"
	@echo "  make generate-certs - Generate custom certificates with proper hostnames"
	@echo "  make start-custom   - Start replication with custom certificates"
	@echo "  make verify-custom  - Test all TLS modes including WORKING VERIFY_IDENTITY"
	@echo "  make cleanup-custom - Clean up custom certificate environment"
	@echo ""
	@echo "MIXED TLS Tests (Ports 3420-3421):"
	@echo "  make start-mixed  - Start mixed environment (main TLS ON, replica TLS OFF)"
	@echo "  make verify-mixed - Test TLS behavior with mixed configurations"
	@echo "  make cleanup-mixed- Clean up mixed TLS environment"
	@echo ""
	@echo "General:"
	@echo "  make help       - Show this help message"
	@echo ""
	@echo "Usage Examples:"
	@echo ""
	@echo "Test replica TLS inheritance with default MySQL certificates:"
	@echo "  1. make start    # Start TLS-enabled replication"
	@echo "  2. make verify   # Test replica TLS inheritance"
	@echo "  3. make cleanup  # Clean up"
	@echo ""
	@echo "Test full VERIFY_IDENTITY functionality with custom certificates:"
	@echo "  1. make start-custom  # Start replication with proper certificates"
	@echo "  2. make verify-custom # Test all modes work including VERIFY_IDENTITY"
	@echo "  3. make cleanup-custom# Clean up"
	@echo ""
	@echo "Test mixed TLS scenarios:"
	@echo "  1. make start-mixed  # Start mixed TLS environment"
	@echo "  2. make verify-mixed # Test smart TLS handling"
	@echo "  3. make cleanup-mixed# Clean up"
	@echo ""
	@echo "Key Test Scenarios:"
	@echo "  🔐 Standard TLS: Tests replica TLS inheritance with MySQL default certificates"
	@echo "  📜 Custom Certs: Tests full VERIFY_IDENTITY with proper certificate setup"
	@echo "  🔄 Mixed TLS: Tests behavior when main and replica have different TLS configs"
	@echo ""
	@echo "What Gets Tested:"
	@echo "  ✅ Replica throttler TLS inheritance from main database"
	@echo "  ✅ Binary log replication TLS configuration"
	@echo "  ✅ Smart DSN enhancement (only add TLS when not already present)"
	@echo "  ✅ All TLS modes: DISABLED, PREFERRED, REQUIRED, VERIFY_CA, VERIFY_IDENTITY"
	@echo "  ✅ Error handling for mismatched TLS configurations"