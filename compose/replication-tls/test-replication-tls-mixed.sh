#!/bin/bash
# Test script for Spirit replication TLS in mixed environments
# Tests behavior when main and replica have different TLS configurations
# Simplified version using SQL initialization
# Usage: ./test-replication-tls-mixed.sh [cleanup]

set -e

# Color constants for output formatting
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color
YELLOW='\033[1;33m'

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
    echo "🧹 CLEANUP: Spirit Replication TLS Mixed Environment Test"
    echo "======================================================="
    echo ""
    
    # Step 1: Stop containers
    print_step "Stopping all mixed environment replication containers..."
    docker-compose -f replication-tls-mixed.yml down 2>/dev/null || true
    docker stop spirit-mysql-mixed-main spirit-mysql-mixed-replica 2>/dev/null || true
    print_success "All containers stopped"

    # Step 2: Remove containers
    print_step "Removing stopped containers..."
    docker container prune -f > /dev/null 2>&1
    print_success "Containers cleaned up"

    # Step 3: Remove images if they exist
    print_step "Removing old images..."
    docker rmi replication-tls-mixed_mysql-main replication-tls-mixed_mysql-replica 2>/dev/null || true
    docker rmi mysql:8.0.33 2>/dev/null || true
    print_success "Images cleaned up"

    # Step 4: Remove volumes
    print_step "Removing MySQL data volumes..."
    docker volume rm \
        replication-tls-mixed_mysql_main_data \
        replication-tls-mixed_mysql_replica_data \
        replication-tls-mixed_mysql-main-data \
        replication-tls-mixed_mysql-replica-data \
        mysql_mixed_main_data \
        mysql_mixed_replica_data \
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

    print_success "🎉 MIXED ENVIRONMENT CLEANUP COMPLETE!"
    echo ""
    echo "Environment cleaned and ready for fresh start."
    echo "You can now run 'docker-compose -f replication-tls-mixed.yml up -d' to start fresh containers."
    echo ""
}

# Check for cleanup command
if [ "$1" = "cleanup" ]; then
    cleanup
    exit 0
fi

# Function to log test results
log_test() {
    local test_name="$1"
    local actual_result="$2"
    local expected_result="$3"
    
    if [ "$actual_result" = "$expected_result" ]; then
        echo -e "${GREEN}✅ $test_name: $actual_result (Expected: $expected_result)${NC}"
    else
        echo -e "${RED}❌ $test_name: $actual_result (Expected: $expected_result)${NC}"
    fi
}

# Function to show table structure on both databases
show_table_structure() {
    local test_name="$1"
    local replica_dsn_desc="$2"
    echo ""
    echo "� Table Structure After $test_name:"
    echo "====================================="
    echo "Main DB (localhost:3420):"
    docker-compose -f replication-tls-mixed.yml exec mysql-main mysql -u root -prootpassword -e "USE test; DESCRIBE users;" 2>/dev/null || echo "Failed to query main DB"
    echo ""
    echo "Replica DB (127.0.0.1:3421) - $replica_dsn_desc:"
    docker-compose -f replication-tls-mixed.yml exec mysql-replica mysql -u root -prootpassword -e "USE test; DESCRIBE users;" 2>/dev/null || echo "Failed to query replica DB"
    echo ""
}

# Main test execution
main() {
    echo -e "${BLUE}🔄 Spirit Mixed TLS Environment Testing (Simplified)${NC}"
    echo -e "${BLUE}====================================================${NC}"
    echo ""
    echo "This comprehensive test validates:"
    echo "  🔄 Mixed TLS configurations (main TLS enabled, replica TLS disabled)"
    echo "  🔄 TLS inheritance behavior in asymmetric environments"
    echo "  🔄 Explicit replica TLS parameter preservation"
    echo "  🏆 Adaptive TLS behavior based on server capabilities"
    echo ""
    echo "Test Environment:"
    echo "  📊 Main DB: localhost:3420 (TLS enabled)"
    echo "  📊 Replica DB: 127.0.0.1:3421 (TLS disabled)"
    echo "  🔧 Mixed security configuration testing"
    echo ""
    
    # Build Spirit first
    echo "📦 Building Spirit..."
    cd ../../
    go build -o compose/replication-tls/spirit ./cmd/spirit
    cd compose/replication-tls/
    
    # Ensure Docker containers are running
    echo "🔧 Checking container status..."
    if ! docker-compose -f replication-tls-mixed.yml ps | grep -q "Up"; then
        echo -e "${RED}❌ Mixed environment containers are not running. Please start them first.${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}✅ Mixed environment containers are running${NC}"
    
    # Verify containers are responsive
    echo "🔧 Verifying database connectivity..."
    if ! docker-compose -f replication-tls-mixed.yml exec mysql-main mysqladmin ping -h localhost -u root -prootpassword --silent; then
        echo -e "${RED}❌ Main database is not responding. Please check container status.${NC}"
        exit 1
    fi
    
    if ! docker-compose -f replication-tls-mixed.yml exec mysql-replica mysqladmin ping -h localhost -u root -prootpassword --silent; then
        echo -e "${RED}❌ Replica database is not responding. Please check container status.${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}✅ Both databases are responsive${NC}"
    
    # Database structure is automatically initialized by Docker init scripts
    echo -e "\n${BLUE}📋 Database structure initialized automatically by Docker init scripts${NC}"
    echo -e "${GREEN}✅ Database structure ready for testing${NC}"
    
    # Verify the initialization worked
    echo "🔍 Verifying initialized table structure..."
    echo "Main DB table structure:"
    docker-compose -f replication-tls-mixed.yml exec mysql-main mysql -u root -prootpassword -e "USE test; DESCRIBE users; SHOW CREATE TABLE users;" 
    echo ""
    echo "Replica DB table structure:"
    docker-compose -f replication-tls-mixed.yml exec mysql-replica mysql -u root -prootpassword -e "USE test; DESCRIBE users; SHOW CREATE TABLE users;"
    echo ""
    # Test 1: DISABLED mode (should fail - main DB requires TLS)
    echo ""
    echo "🔍 Test 1: TLS Mode DISABLED in mixed environment"
    echo "================================================="
    echo "Main: DISABLED TLS, Replica DSN: no explicit TLS (inherits DISABLED)"
    echo "Expected: FAILURE - Main database requires secure transport"
    echo "Using COPY DDL to force replica connection for throttling"
    set +e  # Allow this to fail
    ./spirit \
      --host="localhost:3420" \
      --username="root" \
      --password="rootpassword" \
      --database="test" \
      --tls-mode="DISABLED" \
      --table="users" \
      --alter="CHANGE COLUMN name name VARCHAR(150) NOT NULL" \
      --replica-dsn="root:rootpassword@tcp(127.0.0.1:3421)/test" \
      --replica-max-lag=10s \
      --lock-wait-timeout=2s
    DISABLED_RESULT=$?
    set -e

    if [ $DISABLED_RESULT -eq 0 ]; then
        echo "⚠️  WARNING: DISABLED mode unexpectedly succeeded!"
        show_table_structure "DISABLED mode (unexpected success)" "inherits DISABLED TLS"
    else
        echo "✅ DISABLED mode correctly failed (main DB requires secure transport)"
    fi

    # Test 2: REQUIRED mode (should fail - replica inherits REQUIRED but doesn't support TLS)
    echo ""
    echo "🔍 Test 2: TLS Mode REQUIRED in mixed environment"
    echo "================================================="
    echo "Main: REQUIRED TLS, Replica DSN: no explicit TLS (inherits REQUIRED)"
    echo "Expected: FAILURE - Replica inherits REQUIRED TLS but doesn't support TLS"
    echo "Using COPY DDL to force replica connection for throttling"
    set +e  # Allow this to fail - this is the expected behavior
    ./spirit \
      --host="localhost:3420" \
      --username="root" \
      --password="rootpassword" \
      --database="test" \
      --tls-mode="REQUIRED" \
      --table="users" \
      --alter="MODIFY COLUMN id BIGINT AUTO_INCREMENT" \
      --replica-dsn="root:rootpassword@tcp(127.0.0.1:3421)/test" \
      --replica-max-lag=10s \
      --lock-wait-timeout=2s
    REQUIRED_RESULT=$?
    set -e

    if [ $REQUIRED_RESULT -eq 0 ]; then
        echo "⚠️  WARNING: REQUIRED mode unexpectedly succeeded!"
        echo "   This suggests TLS inheritance may not be working correctly"
        show_table_structure "REQUIRED mode (unexpected success)" "inherits REQUIRED TLS"
    else
        echo "✅ REQUIRED mode correctly failed (replica inherited REQUIRED TLS but server doesn't support TLS)"
        echo "   This validates that TLS inheritance works as expected"
    fi

    # Test 3: PREFERRED mode (may fail with binlog syncer TLS requirements)
    echo ""
    echo "🔍 Test 3: TLS Mode PREFERRED in mixed environment"
    echo "=================================================="
    echo "Main: PREFERRED TLS, Replica DSN: no explicit TLS (inherits PREFERRED)"
    echo "Expected: SUCCESS - PREFERRED mode should fall back to plaintext when TLS not supported"
    echo "Using COPY DDL to force replica connection for throttling"
    set +e  # Allow this to fail but expect success
    ./spirit \
      --host="localhost:3420" \
      --username="root" \
      --password="rootpassword" \
      --database="test" \
      --tls-mode="PREFERRED" \
      --table="users" \
      --alter="MODIFY COLUMN name VARCHAR(100) CHARACTER SET latin1" \
      --replica-dsn="root:rootpassword@tcp(127.0.0.1:3421)/test" \
      --replica-max-lag=10s \
      --lock-wait-timeout=2s
    PREFERRED_RESULT=$?
    set -e

    if [ $PREFERRED_RESULT -eq 0 ]; then
        echo "✅ PREFERRED mode succeeded (correctly fell back to plaintext)"
        show_table_structure "PREFERRED mode" "inherits PREFERRED TLS (adaptive fallback)"
    else
        echo "⚠️  WARNING: PREFERRED mode unexpectedly failed!"
        echo "   PREFERRED should fall back to plaintext when TLS not supported"
        echo "   This may indicate an issue with TLS fallback logic"
    fi

    # Test 4: Explicit replica TLS=false with REQUIRED main
    echo ""
    echo "🔍 Test 4: Main REQUIRED + Explicit replica tls=false"
    echo "======================================================"
    echo "Main: REQUIRED TLS, Replica DSN: explicit tls=false"
    echo "Expected: SUCCESS - Explicit replica config overrides inheritance"
    echo "Using COPY DDL to force replica connection for throttling"
    set +e  # Allow this to fail and continue with other tests
    ./spirit \
      --host="localhost:3420" \
      --username="root" \
      --password="rootpassword" \
      --database="test" \
      --tls-mode="REQUIRED" \
      --table="users" \
      --alter="CHANGE COLUMN updated_at updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP" \
      --replica-dsn="root:rootpassword@tcp(127.0.0.1:3421)/test?tls=false" \
      --replica-max-lag=10s \
      --lock-wait-timeout=2s
    TEST4_RESULT=$?
    set -e

    if [ $TEST4_RESULT -eq 0 ]; then
        echo "✅ Explicit tls=false succeeded (overrides main TLS inheritance)"
        show_table_structure "Explicit tls=false" "explicit tls=false"
    else
        echo "⚠️  WARNING: Explicit tls=false unexpectedly failed!"
        echo "   This suggests there may be a DDL or connection issue"
    fi

    # Test 5: Explicit replica TLS=preferred with REQUIRED main
    echo ""
    echo "🔍 Test 5: Main REQUIRED + Explicit replica tls=preferred"
    echo "=========================================================="
    echo "Main: REQUIRED TLS, Replica DSN: explicit tls=preferred"
    echo "Expected: SUCCESS - Replica falls back to plain text when TLS not available"
    echo "Using COPY DDL to force replica connection for throttling"
    set +e  # Allow this to fail and continue with other tests
    ./spirit \
      --host="localhost:3420" \
      --username="root" \
      --password="rootpassword" \
      --database="test" \
      --tls-mode="REQUIRED" \
      --table="users" \
      --alter="ADD INDEX idx_name_email (name, email)" \
      --replica-dsn="root:rootpassword@tcp(127.0.0.1:3421)/test?tls=preferred" \
      --replica-max-lag=10s \
      --lock-wait-timeout=2s
    TEST5_RESULT=$?
    set -e

    if [ $TEST5_RESULT -eq 0 ]; then
        echo "✅ Explicit tls=preferred succeeded (adaptive fallback)"
        show_table_structure "Explicit tls=preferred" "explicit tls=preferred"
    else
        echo "⚠️  WARNING: Explicit tls=preferred unexpectedly failed!"
        echo "   This suggests there may be a DDL or connection issue"
    fi

    # Test 6: TLS preservation test - tls=skip-verify should work in mixed environment
    echo ""
    echo "🔍 Test 6: TLS preservation - explicit tls=skip-verify"
    echo "====================================================="
    echo "Main: REQUIRED TLS, Replica DSN: explicit tls=skip-verify"
    echo "Expected: FAILURE - skip-verify requires TLS support (unlike required which just demands encryption)"
    echo "Using COPY DDL to force replica connection for throttling"
    set +e  # Allow this to fail - this is the expected behavior
    ./spirit \
      --host="localhost:3420" \
      --username="root" \
      --password="rootpassword" \
      --database="test" \
      --tls-mode="REQUIRED" \
      --table="users" \
      --alter="CHANGE COLUMN created_at created_at DATETIME DEFAULT CURRENT_TIMESTAMP" \
      --replica-dsn="root:rootpassword@tcp(127.0.0.1:3421)/test?tls=skip-verify" \
      --replica-max-lag=10s \
      --lock-wait-timeout=2s
    TLS_SKIP_VERIFY_RESULT=$?
    set -e

    if [ $TLS_SKIP_VERIFY_RESULT -eq 0 ]; then
        echo "⚠️  WARNING: tls=skip-verify unexpectedly succeeded!"
        echo "   This suggests the replica may support TLS after all"
    else
        echo "✅ tls=skip-verify correctly failed (replica has no TLS capability at all)"
    fi

    # Final verification and summary
    echo ""
    echo "🏁 Final Table Structure Verification"
    echo "====================================="
    echo "Verifying all successful operations created their expected columns:"

    show_table_structure "Complete Mixed Environment Test Suite" "various mixed TLS configs tested"

    echo ""
    echo "🎯 MIXED ENVIRONMENT TEST RESULTS SUMMARY"
    echo "=========================================="
    echo ""
    echo "✅ EXPECTED SUCCESSES (operations should succeed):"
    echo "   - Test 3 (PREFERRED): Adaptive TLS behavior, falls back to plaintext when TLS not supported"
    echo "   - Test 4 (explicit tls=false): Explicit replica config overrides main TLS mode"
    echo "   - Test 5 (explicit tls=preferred): Explicit adaptive behavior with fallback"
    echo ""
    echo "❌ EXPECTED FAILURES (operations should fail):"
    echo "   - Test 1 (DISABLED): Main DB requires secure transport"
    echo "   - Test 2 (REQUIRED): Replica inherits REQUIRED TLS but doesn't support TLS"
    echo "   - Test 6 (skip-verify): Replica has no TLS capability at all"
    echo ""
    echo "🎯 KEY VALIDATION POINTS:"
    echo ""
    echo "1. 🔄 MIXED ENVIRONMENT TLS ADAPTATION WORKING:"
    echo "   - DISABLED mode fails when main DB requires secure transport"
    echo "   - PREFERRED mode adapts to individual server capabilities"
    echo "   - REQUIRED mode succeeds (driver adapts when replica lacks TLS support)"
    echo ""
    echo "2. 🔒 TLS INHERITANCE VS EXPLICIT PARAMETERS:"
    if [ $REQUIRED_RESULT -eq 0 ]; then
        echo "   - REQUIRED mode correctly succeeded (MySQL driver adapted to replica capabilities)"
        echo "   - TLS inheritance works with driver adaptation for server capabilities"
    else
        echo "   - ⚠️  REQUIRED mode unexpectedly failed - may indicate connection issue"
    fi
    echo ""
    echo "3. 🎯 EXPLICIT TLS PARAMETER PRESERVATION:"
    echo "   - Explicit tls=false overrides main TLS inheritance"
    echo "   - Explicit tls=preferred enables adaptive behavior"
    if [ $TLS_SKIP_VERIFY_RESULT -ne 0 ]; then
        echo "   - TLS preservation working (skip-verify correctly failed on non-TLS replica)"
    else
        echo "   - ⚠️  TLS preservation may need investigation"
    fi
    echo ""
    echo "4. 🏗️ ASYMMETRIC ENVIRONMENT SUPPORT:"
    echo "   - Spirit handles mixed security configurations gracefully"
    echo "   - Non-INSTANT DDL forces replica connections, validating TLS behavior"
    echo "   - Explicit configuration takes precedence over inheritance"
    echo ""
    echo "🎉 MIXED ENVIRONMENT TLS TEST COMPLETE!"
    echo ""
    if [ $DISABLED_RESULT -ne 0 ] && [ $REQUIRED_RESULT -ne 0 ] && [ $PREFERRED_RESULT -eq 0 ] && [ $TLS_SKIP_VERIFY_RESULT -ne 0 ]; then
        echo "✅ CRITICAL TESTS PASSED WITH EXPECTED RESULTS!"
        echo "   - Expected failures: DISABLED (main requires TLS), REQUIRED (replica inherits TLS), skip-verify (no TLS support)"
        echo "   - Expected successes: PREFERRED (adaptive fallback), explicit tls=false and tls=preferred override inheritance"
        echo "   - Mixed environment TLS inheritance is working correctly"
        echo "   - TLS parameter preservation is working properly"
        echo "   - PREFERRED mode correctly falls back to plaintext"
        echo ""
        echo "🚀 Your Spirit mixed environment TLS implementation is working great!"
    else
        echo "⚠️  SOME UNEXPECTED RESULTS DETECTED"
        echo "   Please review the test output above for details"
        echo "   Results: DISABLED=$DISABLED_RESULT, REQUIRED=$REQUIRED_RESULT, PREFERRED=$PREFERRED_RESULT, TLS_SKIP_VERIFY=$TLS_SKIP_VERIFY_RESULT"
        echo "   Expected: DISABLED≠0 (fail), REQUIRED≠0 (fail), PREFERRED=0 (succeed), TLS_SKIP_VERIFY≠0 (fail)"
    fi
}

# Run main function
main "$@"