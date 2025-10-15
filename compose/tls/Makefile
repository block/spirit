# Spirit TLS Testing Makefile
# Provides simple commands to test TLS modes with local MySQL container

.PHONY: start verify cleanup start-tls verify-tls cleanup-tls generate-certs start-custom verify-custom cleanup-custom help

# Default target
all: help

# Start MySQL container for TLS testing
start:
	@echo "🚀 Starting MySQL 8.0.43 test container (SSL disabled for fallback testing)"
	@echo "=================================================================="
	@docker stop spirit-mysql-tls-test 2>/dev/null || true
	@docker container prune -f
	@docker-compose -f 8.0.43-tls-disabled.yml up -d
	@echo ""
	@echo "⏳ Waiting for MySQL to be ready..."
	@sleep 5
	@docker-compose -f 8.0.43-tls-disabled.yml exec mysql-tls-test mysqladmin ping -h localhost -u root -prootpassword --wait=30 --count=5
	@echo ""
	@echo "✅ MySQL container is ready!"
	@echo "📊 SSL Status (should show 'DISABLED'):"
	@docker-compose -f 8.0.43-tls-disabled.yml exec mysql-tls-test mysql -u root -prootpassword -e "SHOW VARIABLES LIKE 'have_ssl';"
	@echo ""
	@echo "🎯 Ready to run TLS tests! Use 'make verify' to run the tests."

# Verify TLS modes by running the test script
verify:
	@echo "🔍 Running TLS Mode Tests"
	@echo "========================="
	@echo ""
	@echo "This will test:"
	@echo "  ✅ DISABLED mode - should work with plain connection"
	@echo "  ✅ PREFERRED mode - should fallback from TLS to plain connection"
	@echo "  ❌ REQUIRED mode - should fail (TLS required but not available)"
	@echo ""
	@echo "Starting tests..."
	@echo ""
	@./test-tls-disabled.sh

# Clean up containers and resources
cleanup:
	@echo "🧹 Cleaning up TLS-disabled test environment"
	@echo "============================================"
	@docker-compose -f 8.0.43-tls-disabled.yml down -v --remove-orphans
	@docker container prune -f
	@docker network prune -f
	@rm -f spirit
	@echo ""
	@echo "✅ Cleanup complete!"

# Start MySQL container with TLS enabled
start-tls:
	@echo "🔐 Starting MySQL 8.0.43 with TLS ENABLED"
	@echo "=========================================="
	@docker stop spirit-mysql-tls-enabled 2>/dev/null || true
	@docker container prune -f
	@docker-compose -f 8.0.43-tls-enabled.yml up -d
	@echo ""
	@echo "⏳ Waiting for TLS-enabled MySQL to be ready..."
	@sleep 5
	@docker-compose -f 8.0.43-tls-enabled.yml exec mysql-tls-enabled mysqladmin ping -h localhost -u root -prootpassword --wait=30 --count=5
	@echo ""
	@echo "📋 Extracting MySQL CA certificate for VERIFY_CA testing..."
	@mkdir -p mysql-certs
	@docker-compose -f 8.0.43-tls-enabled.yml exec mysql-tls-enabled cat /var/lib/mysql/ca.pem > mysql-certs/ca.pem 2>/dev/null || echo "Using default MySQL certs"
	@echo ""
	@echo "✅ TLS-enabled MySQL container is ready!"
	@echo "📊 SSL Status (should show 'YES'):"
	@docker-compose -f 8.0.43-tls-enabled.yml exec mysql-tls-enabled mysql -u root -prootpassword -e "SHOW VARIABLES LIKE 'have_ssl';"
	@echo "🔐 TLS Requirements:"
	@docker-compose -f 8.0.43-tls-enabled.yml exec mysql-tls-enabled mysql -u root -prootpassword -e "SHOW VARIABLES LIKE 'require_secure_transport';"
	@echo ""
	@echo "📜 Certificate info:"
	@ls -la mysql-certs/ 2>/dev/null || echo "No certificates extracted"
	@echo ""
	@echo "🎯 Ready to run TLS-enabled tests! Use 'make verify-tls' to test REQUIRED, VERIFY_CA, VERIFY_IDENTITY modes."

# Verify TLS modes with TLS-enabled server
verify-tls:
	@echo "🔐 Running TLS Mode Tests (TLS-Enabled Server)"
	@echo "=============================================="
	@echo ""
	@echo "This will test:"
	@echo "  ❌ DISABLED mode - should FAIL (server requires secure transport)"
	@echo "  ✅ REQUIRED mode - should work (TLS connection required and available)"
	@echo "  ✅ VERIFY_CA mode - should work (certificate verification)"
	@echo "  ✅ VERIFY_IDENTITY mode - expected to fail (hostname mismatch, default MySQL certs)"
	@echo "  ✅ PREFERRED mode - should work (TLS available and preferred)"
	@echo ""
	@echo "Starting TLS-enabled tests..."
	@echo ""
	@./test-tls-enabled.sh

# Clean up TLS-enabled containers and resources
cleanup-tls:
	@echo "🧹 Cleaning up TLS-enabled test environment"
	@echo "============================================"
	@docker-compose -f 8.0.43-tls-enabled.yml down -v --remove-orphans
	@docker container prune -f
	@docker network prune -f
	@rm -f spirit
	@echo ""
	@echo "✅ TLS-enabled cleanup complete!"

# Generate custom certificates with proper hostnames for VERIFY_IDENTITY testing
generate-certs:
	@echo "🔐 Generating custom certificates with proper hostnames"
	@echo "======================================================"
	@./generate-custom-certs.sh

# Start MySQL container with custom certificates (enables full VERIFY_IDENTITY testing)
start-custom:
	@echo "🔐 Starting MySQL 8.0.43 with CUSTOM TLS Certificates"
	@echo "====================================================="
	@docker stop spirit-mysql-tls-custom 2>/dev/null || true
	@docker container prune -f
	@echo "📜 Ensuring custom certificates exist..."
	@if [ ! -f mysql-certs/custom-ca.pem ]; then \
		echo "⚠️  Custom certificates not found. Generating them..."; \
		./generate-custom-certs.sh; \
	else \
		echo "✅ Custom certificates found"; \
	fi
	@docker-compose -f 8.0.43-tls-custom.yml up -d
	@echo ""
	@echo "⏳ Waiting for custom-cert MySQL to be ready..."
	@sleep 5
	@docker-compose -f 8.0.43-tls-custom.yml exec mysql-tls-custom mysqladmin ping -h localhost -u root -prootpassword --wait=30 --count=5
	@echo ""
	@echo "✅ Custom-certificate MySQL container is ready!"
	@echo "📊 SSL Status (should show 'YES'):"
	@docker-compose -f 8.0.43-tls-custom.yml exec mysql-tls-custom mysql -u root -prootpassword -e "SHOW VARIABLES LIKE 'have_ssl';"
	@echo "🔐 TLS Requirements:"
	@docker-compose -f 8.0.43-tls-custom.yml exec mysql-tls-custom mysql -u root -prootpassword -e "SHOW VARIABLES LIKE 'require_secure_transport';"
	@echo ""
	@echo "📜 Certificate verification:"
	@echo "Custom CA Subject:"
	@openssl x509 -in mysql-certs/custom-ca.pem -noout -subject
	@echo "Server Certificate Subject:"
	@openssl x509 -in mysql-certs/custom-server-cert.pem -noout -subject
	@echo "Server Certificate SANs:"
	@openssl x509 -in mysql-certs/custom-server-cert.pem -noout -text | grep -A 3 "Subject Alternative Name" || echo "No SANs found"
	@echo ""
	@echo "🎯 Ready for full VERIFY_IDENTITY testing! Use 'make verify-custom' to test all modes including working VERIFY_IDENTITY."

# Test TLS modes with custom certificates (including working VERIFY_IDENTITY)
verify-custom:
	@echo "🔐 Running TLS Tests with Custom Certificates"
	@echo "============================================="
	@echo ""
	@echo "This tests VERIFY_IDENTITY in both failing and working scenarios:"
	@echo "  ✅ All modes should work with properly configured certificates"
	@echo "  ✅ VERIFY_IDENTITY should work with both IP addresses and hostnames"
	@echo "  📜 Demonstrates proper certificate configuration for production use"
	@echo ""
	@echo "Starting custom certificate tests..."
	@echo ""
	@./test-tls-custom-certs.sh

# Clean up custom certificate containers and resources
cleanup-custom:
	@echo "🧹 Cleaning up custom certificate test environment"
	@echo "=================================================="
	@docker-compose -f 8.0.43-tls-custom.yml down -v --remove-orphans
	@docker container prune -f
	@docker network prune -f
	@rm -f spirit
	@echo ""
	@echo "✅ Custom certificate cleanup complete!"

# Show help
help:
	@echo "Spirit TLS Testing Makefile"
	@echo "=========================="
	@echo ""
	@echo "TLS-DISABLED Server Tests (Port 3306):"
	@echo "  make start      - Start MySQL with SSL disabled"
	@echo "  make verify     - Test DISABLED, PREFERRED fallback, REQUIRED failure"
	@echo "  make cleanup    - Clean up TLS-disabled environment"
	@echo ""
	@echo "TLS-ENABLED Server Tests (Port 3307):"
	@echo "  make start-tls  - Start MySQL with SSL enabled (default certificates)"
	@echo "  make verify-tls - Test DISABLED failure, REQUIRED, VERIFY_CA, VERIFY_IDENTITY failure"
	@echo "  make cleanup-tls- Clean up TLS-enabled environment"
	@echo ""
	@echo "CUSTOM CERTIFICATE Tests (Port 3308):"
	@echo "  make generate-certs - Generate custom certificates with proper hostnames"
	@echo "  make start-custom   - Start MySQL with custom certificates"
	@echo "  make verify-custom  - Test all modes including WORKING VERIFY_IDENTITY"
	@echo "  make cleanup-custom - Clean up custom certificate environment"
	@echo ""
	@echo "General:"
	@echo "  make help       - Show this help message"
	@echo ""
	@echo "Usage Examples:"
	@echo ""
	@echo "Test TLS fallback behavior:"
	@echo "  1. make start    # Start TLS-disabled server"
	@echo "  2. make verify   # Test PREFERRED fallback"
	@echo "  3. make cleanup  # Clean up"
	@echo ""
	@echo "Test TLS security modes (with default MySQL certificates):"
	@echo "  1. make start-tls  # Start TLS-enabled server"
	@echo "  2. make verify-tls # Test REQUIRED, VERIFY_CA work; VERIFY_IDENTITY fails"
	@echo "  3. make cleanup-tls# Clean up"
	@echo ""
	@echo "Test full VERIFY_IDENTITY functionality (with custom certificates):"
	@echo "  1. make start-custom  # Start server with proper certificates"
	@echo "  2. make verify-custom # Test all modes work including VERIFY_IDENTITY"
	@echo "  3. make cleanup-custom# Clean up"
	@echo ""
	@echo "Key Tests:"
	@echo "  🔓 TLS-disabled: Tests PREFERRED mode fallback behavior"
	@echo "  🔐 TLS-enabled:  Tests TLS modes with MySQL default certificates"
	@echo "  📜 Custom-certs: Tests full VERIFY_IDENTITY with proper certificate setup"
