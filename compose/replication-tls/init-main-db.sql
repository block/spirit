-- Unified Main Database Initialization for Spirit TLS Testing
-- This script sets up the MAIN database for all TLS test scenarios:
-- - Basic TLS tests (test-replication-tls.sh)
-- - Mixed environment tests (test-replication-tls-mixed.sh) 
-- - Custom certificate tests (test-replication-tls-custom.sh)
--
-- Spirit requires tables to have primary keys for safe online schema migrations

-- =============================================================================
-- DATABASE AND TABLE SETUP
-- =============================================================================

-- Create test database
CREATE DATABASE IF NOT EXISTS test;
USE test;

-- Drop existing tables to ensure clean state
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS test_table;

-- Create users table with proper primary key for Spirit migrations
-- This is the main table used across all TLS test scenarios
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_name (name),
    INDEX idx_email (email)
);

-- Create test_table with proper primary key (used in replication tests)
-- Ensures compatibility with legacy test scripts
CREATE TABLE test_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- INITIAL TEST DATA
-- =============================================================================

-- Insert test data for users table (used by all TLS tests)
INSERT INTO users (name, email) VALUES 
    ('user1', 'user1@example.com'),
    ('user2', 'user2@example.com'),
    ('user3', 'user3@example.com');

-- Insert test data for test_table (used by replication tests)
INSERT INTO test_table (name) VALUES 
    ('Initial Record 1'),
    ('Initial Record 2'),
    ('Initial Record 3'),
    ('test_record_1'),
    ('test_record_2'),
    ('test_record_3');

-- =============================================================================
-- MAIN DATABASE USER SETUP
-- =============================================================================

-- Grant replication privileges to the main spirit user (needed for binary log reading)
-- This allows Spirit to read binary logs for replication monitoring
GRANT REPLICATION CLIENT ON *.* TO 'spirit'@'%';
GRANT REPLICATION SLAVE ON *.* TO 'spirit'@'%';
GRANT RELOAD ON *.* TO 'spirit'@'%';
GRANT SUPER ON *.* TO 'spirit'@'%';

-- Create replication user for binary log streaming to replica
CREATE USER IF NOT EXISTS 'repl_user'@'%' IDENTIFIED BY 'repl_password';
GRANT REPLICATION SLAVE ON *.* TO 'repl_user'@'%';

-- Create user for replica throttling checks (used by Spirit throttler)
CREATE USER IF NOT EXISTS 'throttle_user'@'%' IDENTIFIED BY 'throttle_password';
GRANT SELECT ON *.* TO 'throttle_user'@'%';
GRANT PROCESS ON *.* TO 'throttle_user'@'%';

-- Flush privileges to ensure all changes take effect
FLUSH PRIVILEGES;

-- =============================================================================
-- MAIN DATABASE REPLICATION SETUP
-- =============================================================================

-- Show master status for replication setup verification
SHOW MASTER STATUS;

-- =============================================================================
-- VERIFICATION AND DIAGNOSTICS
-- =============================================================================

-- Display final database structure for verification
SHOW TABLES;
DESCRIBE users;
DESCRIBE test_table;

-- Display user accounts for verification
SELECT User, Host FROM mysql.user WHERE User IN ('spirit', 'repl_user', 'throttle_user');

-- Display current database status
SELECT DATABASE() as current_database;
SELECT COUNT(*) as users_count FROM users;
SELECT COUNT(*) as test_table_count FROM test_table;

-- End of main database initialization script