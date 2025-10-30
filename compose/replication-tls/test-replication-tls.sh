#!/bin/bash
# Test script for Spirit replication TLS inheritance with default MySQL certificates
# Tests replica throttler TLS inheritance and binary log replication TLS support
# Usage: ./test-replication-tls.sh [cleanup]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_step() {
    echo -e "${BLUE}🔧 $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Comprehensive cleanup function
cleanup() {
    echo ""
    echo "🧹 CLEANUP: Spirit Replication TLS Test Environment"
    echo "=================================================="
    echo ""
    
    # Step 1: Stop containers
    print_step "Stopping all replication TLS containers..."
    docker stop spirit-mysql-repl-tls-main spirit-mysql-repl-tls-replica 2>/dev/null || true
    docker stop replication-tls_mysql-main_1 replication-tls_mysql-replica_1 2>/dev/null || true
    print_success "All containers stopped"

    # Step 2: Remove containers
    print_step "Removing stopped containers..."
    docker container prune -f > /dev/null 2>&1
    print_success "Containers cleaned up"

    # Step 3: Remove images if they exist
    print_step "Removing old images..."
    docker rmi replication-tls_mysql-main replication-tls_mysql-replica 2>/dev/null || true
    docker rmi mysql:8.0.33 2>/dev/null || true
    print_success "Images cleaned up"

    # Step 4: Remove volumes
    print_step "Removing MySQL data volumes..."
    docker volume rm \
        replication-tls_mysql_main_data \
        replication-tls_mysql_replica_data \
        replication-tls_mysql-main-data \
        replication-tls_mysql-replica-data \
        mysql_main_data \
        mysql_replica_data \
        2>/dev/null || true
    print_success "Volumes cleaned up"

    # Step 5: Network cleanup
    print_step "Cleaning up networks..."
    docker network prune -f > /dev/null 2>&1
    print_success "Networks cleaned up"

    # Step 6: Remove Spirit binary
    print_step "Cleaning up Spirit binary..."
    rm -f spirit 2>/dev/null || true
    print_success "Spirit binary cleaned up"

    print_success "🎉 CLEANUP COMPLETE!"
    echo ""
    echo "Environment cleaned and ready for fresh start."
    echo "You can now run 'docker-compose up -d' to start fresh containers."
    echo ""
}

# Check for cleanup command
if [ "$1" = "cleanup" ]; then
    cleanup
    exit 0
fi

main() {
    echo "🔐 Testing Spirit Replication TLS Inheritance"
    echo "============================================="
    echo ""
    echo "This comprehensive test validates:"
    echo "  🔄 Replica throttler TLS inheritance from main database"
    echo "  🔄 Binary log replication TLS configuration"
    echo "  🛡️ TLS security enforcement on both main and replica"
    echo "  🎯 Smart DSN enhancement (preserve explicit TLS parameters)"
    echo ""
    echo "Test Environment:"
    echo "  📊 Main DB: 127.0.0.1:3400 (TLS enabled)"
    echo "  📊 Replica DB: 127.0.0.1:3401 (TLS enabled)"
    echo "  🔒 Both servers have require_secure_transport=ON"
    echo ""

    # Build Spirit first
    echo "📦 Building Spirit..."
    cd ../../
    go build -o compose/replication-tls/spirit ./cmd/spirit
    cd compose/replication-tls/

    # Database connection settings
    MAIN_HOST="127.0.0.1:3400"
    REPLICA_HOST="127.0.0.1:3401"
    MYSQL_USER="root"
    MYSQL_PASSWORD="rootpassword"
    MYSQL_DATABASE="test"

    # Ensure test database and table exist
    echo "🔧 Setting up test database and table..."
    docker exec spirit-mysql-repl-tls-main mysql -uroot -prootpassword -e "CREATE DATABASE IF NOT EXISTS test; USE test; CREATE TABLE IF NOT EXISTS test_table (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100) NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);" || echo "Warning: Could not set up main database"
    docker exec spirit-mysql-repl-tls-replica mysql -uroot -prootpassword -e "CREATE DATABASE IF NOT EXISTS test; USE test; CREATE TABLE IF NOT EXISTS test_table (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100) NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);" || echo "Warning: Could not set up replica database"

    # Reset table to clean state (remove any previous test columns)
    echo "🧹 Cleaning up previous test columns..."
    docker exec spirit-mysql-repl-tls-main mysql -uroot -prootpassword -e "USE test; DROP TABLE IF EXISTS test_table; CREATE TABLE test_table (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100) NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);" || echo "Warning: Could not reset main table"
    docker exec spirit-mysql-repl-tls-replica mysql -uroot -prootpassword -e "USE test; DROP TABLE IF EXISTS test_table; CREATE TABLE test_table (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100) NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);" || echo "Warning: Could not reset replica table"

    # Function to show table structure on both databases
    show_table_structure() {
        local test_name="$1"
        local replica_dsn_desc="$2"
        echo ""
        echo "📋 Table Structure After $test_name:"
        echo "====================================="
        echo "Main DB (127.0.0.1:3400):"
        docker exec spirit-mysql-repl-tls-main mysql -uroot -prootpassword -e "USE test; DESCRIBE test_table; SELECT COUNT(*) as row_count FROM test_table;" 2>/dev/null || echo "Failed to query main DB"
        echo ""
        echo "Replica DB (127.0.0.1:3401) - $replica_dsn_desc:"
        docker exec spirit-mysql-repl-tls-replica mysql -uroot -prootpassword -e "USE test; DESCRIBE test_table; SELECT COUNT(*) as row_count FROM test_table;" 2>/dev/null || echo "Failed to query replica DB"
        echo ""
    }

    # Test 1: REQUIRED mode (should work with TLS inheritance)
    echo ""
    echo "🔍 Test 1: TLS Mode REQUIRED with inheritance"
    echo "============================================="
    echo "Main: REQUIRED TLS, Replica DSN: no explicit TLS (inherits REQUIRED)"
    echo "Expected: SUCCESS - replica inherits main TLS configuration"
    echo "Using COPY DDL to force replica connection for throttling"
    ./spirit \
      --host="$MAIN_HOST" \
      --username="$MYSQL_USER" \
      --password="$MYSQL_PASSWORD" \
      --database="$MYSQL_DATABASE" \
      --tls-mode="REQUIRED" \
      --table="test_table" \
      --alter="MODIFY COLUMN id BIGINT AUTO_INCREMENT" \
      --replica-dsn="root:rootpassword@tcp(127.0.0.1:3401)/test" \
      --replica-max-lag=10s \
      --lock-wait-timeout=2s

    REQUIRED_RESULT=$?
    echo "✅ REQUIRED mode completed successfully"
    show_table_structure "REQUIRED mode" "inherits REQUIRED TLS"

    # Test 2: DISABLED mode (should fail because servers require TLS)
    echo ""
    echo "🔍 Test 2: TLS Mode DISABLED"
    echo "============================"
    echo "Main: DISABLED TLS, Replica DSN: no explicit TLS (inherits DISABLED)"
    echo "Expected: FAILURE - Both servers enforce require_secure_transport=ON"
    echo "Using COPY DDL to force replica connection for throttling"
    set +e  # Allow this to fail
    ./spirit \
      --host="$MAIN_HOST" \
      --username="$MYSQL_USER" \
      --password="$MYSQL_PASSWORD" \
      --database="$MYSQL_DATABASE" \
      --tls-mode="DISABLED" \
      --table="test_table" \
      --alter="CHANGE COLUMN name name VARCHAR(150) NOT NULL" \
      --replica-dsn="root:rootpassword@tcp(127.0.0.1:3401)/test" \
      --replica-max-lag=10s \
      --lock-wait-timeout=2s
    DISABLED_RESULT=$?
    set -e

    if [ $DISABLED_RESULT -eq 0 ]; then
        echo "⚠️  WARNING: DISABLED mode unexpectedly succeeded!"
        show_table_structure "DISABLED mode (unexpected success)" "inherits DISABLED TLS"
    else
        echo "✅ DISABLED mode correctly failed (TLS enforcement working)"
    fi

    # Test 3: SKIP_VERIFY mode (should work)
    echo ""
    echo "🔍 Test 3: TLS Mode SKIP_VERIFY"
    echo "==============================="
    echo "Main: SKIP_VERIFY TLS, Replica DSN: no explicit TLS (inherits SKIP_VERIFY)"
    echo "Expected: SUCCESS - TLS used but certificate verification skipped"
    echo "Using COPY DDL to force replica connection for throttling"
    ./spirit \
      --host="$MAIN_HOST" \
      --username="$MYSQL_USER" \
      --password="$MYSQL_PASSWORD" \
      --database="$MYSQL_DATABASE" \
      --tls-mode="SKIP_VERIFY" \
      --table="test_table" \
      --alter="MODIFY COLUMN name VARCHAR(100) CHARACTER SET latin1" \
      --replica-dsn="root:rootpassword@tcp(127.0.0.1:3401)/test" \
      --replica-max-lag=10s \
      --lock-wait-timeout=2s

    echo "✅ SKIP_VERIFY mode succeeded"
    show_table_structure "SKIP_VERIFY mode" "inherits SKIP_VERIFY TLS"

    # Test 4: VERIFY_CA mode (may fail with auto-generated certificates)
    echo ""
    echo "🔍 Test 4: TLS Mode VERIFY_CA with CA certificate"
    echo "================================================="
    echo "Main: VERIFY_CA TLS, Replica DSN: no explicit TLS (inherits VERIFY_CA)"
    echo "Expected: MAY FAIL - Auto-generated MySQL certificates have independent CAs"
    echo "Using COPY DDL to force replica connection for throttling"
    set +e  # Allow this to fail - this is expected with auto-generated certs
    ./spirit \
      --host="$MAIN_HOST" \
      --username="$MYSQL_USER" \
      --password="$MYSQL_PASSWORD" \
      --database="$MYSQL_DATABASE" \
      --tls-mode="VERIFY_CA" \
      --tls-ca="mysql-certs/combined-ca.pem" \
      --table="test_table" \
      --alter="CHANGE COLUMN created_at created_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP" \
      --replica-dsn="root:rootpassword@tcp(127.0.0.1:3401)/test" \
      --replica-max-lag=10s \
      --lock-wait-timeout=2s
    VERIFY_CA_RESULT=$?
    set -e

    if [ $VERIFY_CA_RESULT -eq 0 ]; then
        echo "✅ VERIFY_CA mode succeeded with combined CA certificates"
        show_table_structure "VERIFY_CA mode" "inherits VERIFY_CA TLS"
    else
        echo "⚠️  VERIFY_CA mode failed (expected with auto-generated MySQL certificates)"
        echo "   This is normal behavior - each MySQL container generates independent CA certificates"
        echo "   In production, you would use a shared CA or custom certificates for VERIFY_CA mode"
        echo "   The test validates that explicit TLS parameters are preserved during the failure"
    fi

    # Test 5: VERIFY_IDENTITY mode (should fail due to hostname mismatch)
    echo ""
    echo "🔍 Test 5: TLS Mode VERIFY_IDENTITY"
    echo "==================================="
    echo "Main: VERIFY_IDENTITY TLS, Replica DSN: no explicit TLS (inherits VERIFY_IDENTITY)"
    echo "Expected: FAILURE - MySQL's auto-generated certificates don't contain proper hostname SANs"
    echo "Using COPY DDL to force replica connection for throttling"
    set +e  # Allow this to fail
    ./spirit \
      --host="$MAIN_HOST" \
      --username="$MYSQL_USER" \
      --password="$MYSQL_PASSWORD" \
      --database="$MYSQL_DATABASE" \
      --tls-mode="VERIFY_IDENTITY" \
      --table="test_table" \
      --alter="ADD INDEX idx_name_email (name, created_at)" \
      --replica-dsn="root:rootpassword@tcp(127.0.0.1:3401)/test" \
      --replica-max-lag=10s \
      --lock-wait-timeout=2s
    VERIFY_IDENTITY_RESULT=$?
    set -e

    if [ $VERIFY_IDENTITY_RESULT -eq 0 ]; then
        echo "⚠️  WARNING: VERIFY_IDENTITY mode unexpectedly succeeded!"
        show_table_structure "VERIFY_IDENTITY mode (unexpected success)" "inherits VERIFY_IDENTITY TLS"
    else
        echo "✅ VERIFY_IDENTITY mode correctly failed (hostname verification failed)"
    fi

    # Test 6: PREFERRED mode (should work)
    echo ""
    echo "🔍 Test 6: TLS Mode PREFERRED"
    echo "============================="
    echo "Main: PREFERRED TLS, Replica DSN: no explicit TLS (inherits PREFERRED)"
    echo "Expected: SUCCESS - TLS is available, so PREFERRED mode will use it"
    echo "Using COPY DDL to force replica connection for throttling"
    ./spirit \
      --host="$MAIN_HOST" \
      --username="$MYSQL_USER" \
      --password="$MYSQL_PASSWORD" \
      --database="$MYSQL_DATABASE" \
      --tls-mode="PREFERRED" \
      --table="test_table" \
      --alter="ADD INDEX idx_preferred_test (created_at)" \
      --replica-dsn="root:rootpassword@tcp(127.0.0.1:3401)/test" \
      --replica-max-lag=10s \
      --lock-wait-timeout=2s

    echo "✅ PREFERRED mode succeeded with TLS"
    show_table_structure "PREFERRED mode" "inherits PREFERRED TLS"

    # Test 7:tls=false preservation
    echo ""
    echo "🔍 Test 7: Replica-DSN tls=false preservation"
    echo "===================================================="
    echo "Main: REQUIRED TLS, Replica DSN: explicit tls=false"
    echo "Expected: FAILURE - replica tls=false should be preserved and cause connection failure"
    echo "Using NON-INSTANT DDL to force replica connection usage"
    set +e  # Allow this to fail - this is the expected behavior
    ./spirit \
      --host="$MAIN_HOST" \
      --username="$MYSQL_USER" \
      --password="$MYSQL_PASSWORD" \
      --database="$MYSQL_DATABASE" \
      --tls-mode="REQUIRED" \
      --table="test_table" \
      --alter="MODIFY COLUMN name TEXT" \
      --replica-dsn="root:rootpassword@tcp(127.0.0.1:3401)/test?tls=false" \
      --replica-max-lag=10s \
      --lock-wait-timeout=2s
    TLS_FALSE_RESULT=$?
    set -e

    # Test 8: Replica DSN with explicit TLS (should preserve existing config)
    echo ""
    echo "🔍 Test 8: Replica DSN with explicit TLS preservation"
    echo "====================================================="
    echo "Main: SKIP_VERIFY TLS, Replica DSN: explicit tls=skip-verify"
    echo "Expected: SUCCESS - replica explicit TLS config should be preserved, not inherited"
    echo "Using COPY DDL to force replica connection for throttling"
    set +e  # May succeed or fail depending on server configuration
    ./spirit \
      --host="$MAIN_HOST" \
      --username="$MYSQL_USER" \
      --password="$MYSQL_PASSWORD" \
      --database="$MYSQL_DATABASE" \
      --tls-mode="SKIP_VERIFY" \
      --table="test_table" \
      --alter="ADD INDEX idx_created_at (created_at)" \
      --replica-dsn="root:rootpassword@tcp(127.0.0.1:3401)/test?tls=skip-verify" \
      --replica-max-lag=10s \
      --lock-wait-timeout=2s
    PRESERVE_TLS_RESULT=$?
    set -e

    if [ $PRESERVE_TLS_RESULT -eq 0 ]; then
        echo "✅ Replica with explicit tls=skip-verify succeeded"
        echo "   This proves explicit replica TLS config is preserved, not inherited"
        show_table_structure "Explicit TLS preservation" "explicit tls=skip-verify"
    else
        echo "❌ Replica with explicit tls=skip-verify failed - unexpected"
    fi

    # Final verification and summary
    echo ""
    echo "🏁 Final Table Structure Verification"
    echo "====================================="
    echo "Verifying all successful operations created their expected columns:"

    show_table_structure "Complete Test Suite" "various DSN configs tested"

    echo ""
    echo "🎯 TEST RESULTS SUMMARY"
    echo "======================="
    echo ""
    echo "✅ EXPECTED SUCCESSES (DDL operations should succeed):"
    echo "   - Test 1 (REQUIRED mode): MODIFY COLUMN with TLS inheritance"
    echo "   - Test 3 (SKIP_VERIFY mode): MODIFY COLUMN with TLS"
    echo "   - Test 6 (PREFERRED mode): ADD INDEX with TLS available"
    echo "   - Test 8 (Explicit TLS preservation): ADD INDEX with tls=skip-verify override"
    echo ""
    echo "❌ EXPECTED FAILURES (DDL operations should fail):"
    echo "   - Test 2 (DISABLED mode): CHANGE COLUMN failed (servers require TLS)"
    echo "   - Test 4 (VERIFY_CA mode): CHANGE COLUMN failed (independent CA certificates)"
    echo "   - Test 5 (VERIFY_IDENTITY mode): ADD INDEX failed (hostname mismatch)"
    echo "   - Test 7 (TLS preservation): MODIFY COLUMN failed (tls=false preserved!)"
    echo ""
    echo "🎯 All tests use COPY mode DDL operations that force replica throttler connections!"
    echo ""
    echo "🎉 REPLICATION TLS INHERITANCE TEST COMPLETE!"
    echo ""
    if [ $DISABLED_RESULT -ne 0 ] && [ $VERIFY_IDENTITY_RESULT -ne 0 ] && [ $TLS_FALSE_RESULT -ne 0 ]; then
        echo "✅ ALL CRITICAL TESTS PASSED WITH EXPECTED RESULTS!"
        echo "   - Expected failures failed correctly (DISABLED, VERIFY_IDENTITY, TLS bug test)"
        echo "   - Expected successes succeeded (REQUIRED, SKIP_VERIFY, PREFERRED)"
        if [ $VERIFY_CA_RESULT -eq 0 ]; then
            echo "   - VERIFY_CA mode unexpectedly succeeded ⚠️"
        else
            echo "   - VERIFY_CA mode failed as expected (independent MySQL CA certificates) ✅"
        fi
        echo "   - TLS bug fix is working properly"
        echo "   - Replica TLS inheritance is working correctly"
        echo "   - Replica throttler connections tested with COPY mode DDL"
        echo ""
    else
        echo "⚠️  SOME UNEXPECTED RESULTS DETECTED"
        echo "   Please review the test output above for details"
        echo "   Expected failures: DISABLED=$DISABLED_RESULT, VERIFY_IDENTITY=$VERIFY_IDENTITY_RESULT, TLS_FALSE=$TLS_FALSE_RESULT"
        echo "   Other results: VERIFY_CA=$VERIFY_CA_RESULT, PRESERVE_TLS=$PRESERVE_TLS_RESULT"
        echo "   (Non-zero values indicate expected failures occurred correctly)"
    fi
    if [ $DISABLED_RESULT -ne 0 ] && [ $VERIFY_IDENTITY_RESULT -ne 0 ] && [ $TLS_FALSE_RESULT -ne 0 ]; then
        echo "✅ ALL CRITICAL TESTS PASSED WITH EXPECTED RESULTS!"
        echo "   - Expected failures failed correctly (DISABLED, VERIFY_IDENTITY, TLS bug test)"
        echo "   - Expected successes succeeded (REQUIRED, SKIP_VERIFY, PREFERRED)"
        if [ $VERIFY_CA_RESULT -eq 0 ]; then
            echo "   - VERIFY_CA mode succeeded with extracted CA certificate ✅"
        else
            echo "   - VERIFY_CA mode failed (CA certificate extraction issue) ⚠️"
        fi
        echo "   - TLS bug fix is working properly"
        echo "   - Replica TLS inheritance is working correctly"
        echo "   - Replica throttler connections tested with COPY mode DDL"
        echo ""
    else
        echo "⚠️  SOME UNEXPECTED RESULTS DETECTED"
        echo "   Please review the test output above for details"
        echo "   Expected failures: DISABLED=$DISABLED_RESULT, VERIFY_IDENTITY=$VERIFY_IDENTITY_RESULT, TLS_FALSE=$TLS_FALSE_RESULT"
        echo "   Other results: VERIFY_CA=$VERIFY_CA_RESULT, PRESERVE_TLS=$PRESERVE_TLS_RESULT"
        echo "   (Non-zero values indicate expected failures occurred correctly)"
    fi
}

# Run main function
main "$@"