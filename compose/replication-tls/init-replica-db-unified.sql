-- Unified Replica Database Initialization for Spirit TLS Testing
-- This script sets up the REPLICA database for all TLS test scenarios:
-- - Basic TLS tests (test-replication-tls.sh)
-- - Mixed environment tests (test-replication-tls-mixed.sh) 
-- - Custom certificate tests (test-replication-tls-custom.sh)
--
-- This replica will replicate from the main database and provide throttling capabilities

-- =============================================================================
-- DATABASE AND TABLE SETUP (will be replicated from main)
-- =============================================================================

-- Create test database (will be populated via replication)
CREATE DATABASE IF NOT EXISTS test;
USE test;

-- Note: Tables will be replicated from main database, but we create them here
-- as a fallback in case replication hasn't started yet

-- Create users table structure (will be replaced by replication)
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_name (name),
    INDEX idx_email (email)
);

-- Create test_table structure (will be replaced by replication)  
CREATE TABLE IF NOT EXISTS test_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- REPLICA DATABASE USER SETUP
-- =============================================================================

-- Create user for replica lag monitoring (used by Spirit for replica status checks)
CREATE USER IF NOT EXISTS 'replica_monitor'@'%' IDENTIFIED BY 'monitor_password';
GRANT SELECT ON *.* TO 'replica_monitor'@'%';
GRANT PROCESS ON *.* TO 'replica_monitor'@'%';
GRANT REPLICATION CLIENT ON *.* TO 'replica_monitor'@'%';

-- Create throttle user for replica throttling checks (used by Spirit throttler)
CREATE USER IF NOT EXISTS 'throttle_user'@'%' IDENTIFIED BY 'throttle_password';
GRANT SELECT ON *.* TO 'throttle_user'@'%';
GRANT PROCESS ON *.* TO 'throttle_user'@'%';

-- Flush privileges to ensure all changes take effect
FLUSH PRIVILEGES;

-- =============================================================================
-- REPLICA REPLICATION SETUP
-- =============================================================================

-- Wait for main server to be ready
SELECT SLEEP(10);

-- Configure replication to main server
CHANGE REPLICATION SOURCE TO
    SOURCE_HOST = 'mysql-main',
    SOURCE_PORT = 3306,
    SOURCE_USER = 'repl_user', 
    SOURCE_PASSWORD = 'repl_password',
    SOURCE_AUTO_POSITION = 1,
    SOURCE_SSL = 1;

-- Start replication
START REPLICA;

-- =============================================================================
-- VERIFICATION AND DIAGNOSTICS
-- =============================================================================

-- Show replica status for verification
SHOW REPLICA STATUS\G

-- Display final database structure for verification
SHOW TABLES;

-- Display user accounts for verification
SELECT User, Host FROM mysql.user WHERE User IN ('spirit', 'replica_monitor', 'throttle_user');

-- Display current database status
SELECT DATABASE() as current_database;

-- Verify replication is working (may show 0 if replication just started)
SELECT COUNT(*) as users_count FROM users;
SELECT COUNT(*) as test_table_count FROM test_table;

-- End of replica database initialization script