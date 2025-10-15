-- Initialize test database for Spirit TLS testing

-- Create test table for Spirit to work with
CREATE TABLE IF NOT EXISTS test_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_name (name),
    INDEX idx_email (email)
);

-- Insert some test data
INSERT INTO test_table (name, email) VALUES 
    ('usr_a1x', 'a1x@test.local'),
    ('usr_b2y', 'b2y@test.local'),
    ('usr_c3z', 'c3z@test.local'),
    ('usr_d4w', 'd4w@test.local'),
    ('usr_e5v', 'e5v@test.local');

-- Grant necessary privileges to spirit user for migrations
-- Spirit needs these global privileges for migrations
GRANT SUPER, REPLICATION CLIENT, RELOAD, REPLICATION SLAVE ON *.* TO 'spirit'@'%';
-- Grant all privileges on the test database
GRANT ALL PRIVILEGES ON spirit_test.* TO 'spirit'@'%';
-- For MySQL 8.0+, also grant BINLOG_ADMIN if available
-- Note: SUPER privilege includes most of what's needed for migrations
FLUSH PRIVILEGES;

-- Show SSL status (should be OFF)
SHOW VARIABLES LIKE 'have_ssl';
SHOW VARIABLES LIKE 'ssl%';
