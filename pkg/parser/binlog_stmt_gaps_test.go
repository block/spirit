// Copyright 2026 Block, Inc.

package parser_test

import (
	"testing"
)

// These statements are all valid MySQL that can appear in the binary log as
// Query events; spirit's pkg/change must be able to parse them.

func TestRepairTable(t *testing.T) {
	table := []testCase{
		{"REPAIR TABLE t1", true, "REPAIR TABLE `t1`"},
		{"repair tables t1, t2", true, "REPAIR TABLE `t1`, `t2`"},
		{"REPAIR NO_WRITE_TO_BINLOG TABLE t1 QUICK EXTENDED USE_FRM", true, "REPAIR NO_WRITE_TO_BINLOG TABLE `t1` QUICK EXTENDED USE_FRM"},
		{"REPAIR LOCAL TABLE t1 USE_FRM", true, "REPAIR NO_WRITE_TO_BINLOG TABLE `t1` USE_FRM"},
		{"REPAIR TABLE t1 EXTENDED QUICK", true, "REPAIR TABLE `t1` QUICK EXTENDED"},
		{"REPAIR TABLE", false, ""},
	}
	RunTest(t, table, false)
}

func TestRenameTables(t *testing.T) {
	table := []testCase{
		{"RENAME TABLES t1 TO t2", true, "RENAME TABLE `t1` TO `t2`"},
		{"RENAME TABLES t1 TO t2, t3 TO t4", true, "RENAME TABLE `t1` TO `t2`, `t3` TO `t4`"},
	}
	RunTest(t, table, false)
}

func TestAnalyzeTableHistograms(t *testing.T) {
	table := []testCase{
		{"ANALYZE TABLES t1, t2", true, "ANALYZE TABLE `t1`,`t2`"},
		{"ANALYZE TABLE t UPDATE HISTOGRAM ON c1, c2 WITH 8 BUCKETS", true, "ANALYZE TABLE `t` UPDATE HISTOGRAM ON `c1`,`c2` WITH 8 BUCKETS"},
		{"ANALYZE TABLE t UPDATE HISTOGRAM ON c WITH 4 BUCKETS AUTO UPDATE", true, "ANALYZE TABLE `t` UPDATE HISTOGRAM ON `c` WITH 4 BUCKETS AUTO UPDATE"},
		{"ANALYZE TABLE t UPDATE HISTOGRAM ON c AUTO UPDATE", true, "ANALYZE TABLE `t` UPDATE HISTOGRAM ON `c` AUTO UPDATE"},
		{"ANALYZE TABLE t UPDATE HISTOGRAM ON c MANUAL UPDATE", true, "ANALYZE TABLE `t` UPDATE HISTOGRAM ON `c` MANUAL UPDATE"},
		{`ANALYZE TABLE t UPDATE HISTOGRAM ON c USING DATA '{"histogram": {}}'`, true, "ANALYZE TABLE `t` UPDATE HISTOGRAM ON `c` USING DATA '{\"histogram\": {}}'"},
		{"ANALYZE TABLE t DROP HISTOGRAM ON c", true, "ANALYZE TABLE `t` DROP HISTOGRAM ON `c`"},
	}
	RunTest(t, table, false)
}

func TestTransactionSpellings(t *testing.T) {
	table := []testCase{
		{"BEGIN WORK", true, "START TRANSACTION"},
		{"COMMIT WORK", true, "COMMIT"},
		{"ROLLBACK WORK", true, "ROLLBACK"},
		{"COMMIT WORK AND CHAIN", true, "COMMIT AND CHAIN"},
		{"ROLLBACK WORK AND NO CHAIN NO RELEASE", true, "ROLLBACK"},
		{"ROLLBACK WORK TO SAVEPOINT sp1", true, "ROLLBACK TO sp1"},
		{"ROLLBACK WORK TO sp1", true, "ROLLBACK TO sp1"},
		{"START TRANSACTION READ ONLY, WITH CONSISTENT SNAPSHOT", true, "START TRANSACTION READ ONLY"},
		{"START TRANSACTION WITH CONSISTENT SNAPSHOT, READ WRITE", true, "START TRANSACTION"},
		{"START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY", true, "START TRANSACTION READ ONLY"},
	}
	RunTest(t, table, false)
}

func TestFlushOptions(t *testing.T) {
	table := []testCase{
		{"FLUSH USER_RESOURCES", true, "FLUSH USER_RESOURCES"},
		{"FLUSH OPTIMIZER_COSTS", true, "FLUSH OPTIMIZER_COSTS"},
		{"FLUSH LOCAL OPTIMIZER_COSTS", true, "FLUSH NO_WRITE_TO_BINLOG OPTIMIZER_COSTS"},
		{"FLUSH RELAY LOGS", true, "FLUSH RELAY LOGS"},
		{"FLUSH RELAY LOGS FOR CHANNEL 'group_replication_applier'", true, "FLUSH RELAY LOGS FOR CHANNEL 'group_replication_applier'"},
		{"FLUSH TABLES t1 FOR EXPORT", true, "FLUSH TABLES `t1` FOR EXPORT"},
		{"FLUSH TABLES t1, t2 WITH READ LOCK", true, "FLUSH TABLES `t1`, `t2` WITH READ LOCK"},
	}
	RunTest(t, table, false)
}

func TestAlterDatabaseReadOnly(t *testing.T) {
	table := []testCase{
		{"ALTER DATABASE d READ ONLY = 1", true, "ALTER DATABASE `d` READ ONLY = 1"},
		{"ALTER DATABASE d READ ONLY 0", true, "ALTER DATABASE `d` READ ONLY = 0"},
		{"ALTER DATABASE d READ ONLY = DEFAULT", true, "ALTER DATABASE `d` READ ONLY = DEFAULT"},
		{"ALTER DATABASE READ ONLY = 1", true, "ALTER DATABASE READ ONLY = 1"},
		{"ALTER SCHEMA d READ ONLY = 1 CHARACTER SET = utf8mb4", true, "ALTER DATABASE `d` READ ONLY = 1 CHARACTER SET = utf8mb4"},
	}
	RunTest(t, table, false)
}

func TestAlterView(t *testing.T) {
	table := []testCase{
		{"ALTER VIEW v AS SELECT * FROM t", true, "ALTER ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS SELECT * FROM `t`"},
		{"ALTER ALGORITHM = MERGE DEFINER = u@h SQL SECURITY INVOKER VIEW v (a, b) AS SELECT a, b FROM t WITH LOCAL CHECK OPTION", true, "ALTER ALGORITHM = MERGE DEFINER = `u`@`h` SQL SECURITY INVOKER VIEW `v` (`a`,`b`) AS SELECT `a`,`b` FROM `t` WITH LOCAL CHECK OPTION"},
		{"ALTER VIEW v AS", false, ""},
	}
	RunTest(t, table, false)
}

func TestXAStatements(t *testing.T) {
	table := []testCase{
		{"XA START 'gtrid'", true, "XA START 'gtrid'"},
		{"XA BEGIN 'gtrid'", true, "XA START 'gtrid'"},
		{"XA START 'g', 'b'", true, "XA START 'g', 'b'"},
		{"XA START 'g', 'b', 12", true, "XA START 'g', 'b', 12"},
		{"XA START X'41', X'42', 1", true, "XA START 'A', 'B', 1"},
		{"XA START 0x7465737462, 0x2030405060, 0xb", true, "XA START 'testb', ' 0@P`', 11"},
		{"XA START 'g' JOIN", true, "XA START 'g' JOIN"},
		{"XA START 'g' RESUME", true, "XA START 'g' RESUME"},
		{"XA END 'g'", true, "XA END 'g'"},
		{"XA END 'g' SUSPEND", true, "XA END 'g' SUSPEND"},
		{"XA END 'g' SUSPEND FOR MIGRATE", true, "XA END 'g' SUSPEND FOR MIGRATE"},
		{"XA PREPARE 'g'", true, "XA PREPARE 'g'"},
		{"XA COMMIT 'g'", true, "XA COMMIT 'g'"},
		{"XA COMMIT 'g' ONE PHASE", true, "XA COMMIT 'g' ONE PHASE"},
		{"XA ROLLBACK 'g'", true, "XA ROLLBACK 'g'"},
		{"XA RECOVER", true, "XA RECOVER"},
		{"XA RECOVER CONVERT XID", true, "XA RECOVER CONVERT XID"},
		{"XA START", false, ""},
	}
	RunTest(t, table, false)
}

func TestSpatialReferenceSystem(t *testing.T) {
	table := []testCase{
		{`CREATE SPATIAL REFERENCE SYSTEM 4120 NAME 'Bessel' DEFINITION 'GEOGCS[]'`, true, "CREATE SPATIAL REFERENCE SYSTEM 4120 NAME 'Bessel' DEFINITION 'GEOGCS[]'"},
		{`CREATE OR REPLACE SPATIAL REFERENCE SYSTEM 4120 ORGANIZATION 'EPSG' IDENTIFIED BY 4120 DESCRIPTION 'd' NAME 'n' DEFINITION 'def'`, true, "CREATE OR REPLACE SPATIAL REFERENCE SYSTEM 4120 ORGANIZATION 'EPSG' IDENTIFIED BY 4120 DESCRIPTION 'd' NAME 'n' DEFINITION 'def'"},
		{`CREATE SPATIAL REFERENCE SYSTEM IF NOT EXISTS 4120 NAME 'n' DEFINITION 'def'`, true, "CREATE SPATIAL REFERENCE SYSTEM IF NOT EXISTS 4120 NAME 'n' DEFINITION 'def'"},
		{"DROP SPATIAL REFERENCE SYSTEM 4120", true, "DROP SPATIAL REFERENCE SYSTEM 4120"},
		{"DROP SPATIAL REFERENCE SYSTEM IF EXISTS 4120", true, "DROP SPATIAL REFERENCE SYSTEM IF EXISTS 4120"},
		// unrelated statements starting with the same tokens still parse
		{"CREATE SPATIAL INDEX idx ON t (g)", true, "CREATE SPATIAL INDEX `idx` ON `t` (`g`)"},
	}
	RunTest(t, table, false)
}

func TestTablespaceDDL(t *testing.T) {
	table := []testCase{
		{"CREATE TABLESPACE ts1 ADD DATAFILE 'ts1.ibd'", true, "CREATE TABLESPACE `ts1` ADD DATAFILE 'ts1.ibd'"},
		{"CREATE TABLESPACE ts1", true, "CREATE TABLESPACE `ts1`"},
		{"CREATE TABLESPACE ts1 ADD DATAFILE 'f' ENGINE = InnoDB", true, "CREATE TABLESPACE `ts1` ADD DATAFILE 'f' ENGINE = `InnoDB`"},
		{"CREATE TABLESPACE ts1 ADD DATAFILE 'f' FILE_BLOCK_SIZE = 8192 ENCRYPTION = 'Y'", true, "CREATE TABLESPACE `ts1` ADD DATAFILE 'f' FILE_BLOCK_SIZE = 8192 ENCRYPTION = 'Y'"},
		{"CREATE TABLESPACE ts1 ADD DATAFILE 'f' USE LOGFILE GROUP lg1 EXTENT_SIZE 4M INITIAL_SIZE 12M ENGINE NDB", true, "CREATE TABLESPACE `ts1` ADD DATAFILE 'f' USE LOGFILE GROUP `lg1` EXTENT_SIZE = 4M INITIAL_SIZE = 12M ENGINE = `NDB`"},
		{"CREATE UNDO TABLESPACE u1 ADD DATAFILE 'u1.ibu'", true, "CREATE UNDO TABLESPACE `u1` ADD DATAFILE 'u1.ibu'"},
		{"ALTER TABLESPACE ts1 ADD DATAFILE 'f2' INITIAL_SIZE = 1G WAIT", true, "ALTER TABLESPACE `ts1` ADD DATAFILE 'f2' INITIAL_SIZE = 1G WAIT"},
		{"ALTER TABLESPACE ts1 DROP DATAFILE 'f2' ENGINE NDB", true, "ALTER TABLESPACE `ts1` DROP DATAFILE 'f2' ENGINE = `NDB`"},
		{"ALTER TABLESPACE ts1 RENAME TO ts2", true, "ALTER TABLESPACE `ts1` RENAME TO `ts2`"},
		{"ALTER TABLESPACE ts1 AUTOEXTEND_SIZE = 64M", true, "ALTER TABLESPACE `ts1` AUTOEXTEND_SIZE = 64M"},
		{"ALTER UNDO TABLESPACE u1 SET INACTIVE", true, "ALTER UNDO TABLESPACE `u1` SET INACTIVE"},
		{"ALTER UNDO TABLESPACE u1 SET ACTIVE ENGINE = InnoDB", true, "ALTER UNDO TABLESPACE `u1` SET ACTIVE ENGINE = `InnoDB`"},
		{"DROP TABLESPACE ts1", true, "DROP TABLESPACE `ts1`"},
		{"DROP TABLESPACE ts1 ENGINE = NDB", true, "DROP TABLESPACE `ts1` ENGINE = `NDB`"},
		{"DROP UNDO TABLESPACE u1", true, "DROP UNDO TABLESPACE `u1`"},
		{"CREATE LOGFILE GROUP lg1 ADD UNDOFILE 'undo.dat' UNDO_BUFFER_SIZE = 8M ENGINE = NDB", true, "CREATE LOGFILE GROUP `lg1` ADD UNDOFILE 'undo.dat' UNDO_BUFFER_SIZE = 8M ENGINE = `NDB`"},
		{"ALTER LOGFILE GROUP lg1 ADD UNDOFILE 'undo2.dat' INITIAL_SIZE 4M WAIT ENGINE NDB", true, "ALTER LOGFILE GROUP `lg1` ADD UNDOFILE 'undo2.dat' INITIAL_SIZE = 4M WAIT ENGINE = `NDB`"},
		{"DROP LOGFILE GROUP lg1 ENGINE = NDB", true, "DROP LOGFILE GROUP `lg1` ENGINE = `NDB`"},
		{"CREATE TABLESPACE ts1 ADD DATAFILE 'f' INITIAL_SIZE = 12M, MAX_SIZE = 100M, NODEGROUP = 3, NO_WAIT", true, "CREATE TABLESPACE `ts1` ADD DATAFILE 'f' INITIAL_SIZE = 12M MAX_SIZE = 100M NODEGROUP = 3 NO_WAIT"},
	}
	RunTest(t, table, false)
}

func TestAlterInstance(t *testing.T) {
	table := []testCase{
		{"ALTER INSTANCE RELOAD TLS", true, "ALTER INSTANCE RELOAD TLS"},
		{"ALTER INSTANCE RELOAD TLS NO ROLLBACK ON ERROR", true, "ALTER INSTANCE RELOAD TLS NO ROLLBACK ON ERROR"},
		{"ALTER INSTANCE RELOAD TLS FOR CHANNEL mysql_admin", true, "ALTER INSTANCE RELOAD TLS FOR CHANNEL `mysql_admin`"},
		{"ALTER INSTANCE RELOAD TLS FOR CHANNEL mysql_main NO ROLLBACK ON ERROR", true, "ALTER INSTANCE RELOAD TLS FOR CHANNEL `mysql_main` NO ROLLBACK ON ERROR"},
		{"ALTER INSTANCE ROTATE INNODB MASTER KEY", true, "ALTER INSTANCE ROTATE INNODB MASTER KEY"},
		{"ALTER INSTANCE ROTATE BINLOG MASTER KEY", true, "ALTER INSTANCE ROTATE BINLOG MASTER KEY"},
		{"ALTER INSTANCE ROTATE innodb master key", true, "ALTER INSTANCE ROTATE INNODB MASTER KEY"},
		{"ALTER INSTANCE ENABLE INNODB REDO_LOG", true, "ALTER INSTANCE ENABLE INNODB REDO_LOG"},
		{"ALTER INSTANCE DISABLE INNODB REDO_LOG", true, "ALTER INSTANCE DISABLE INNODB REDO_LOG"},
		{"ALTER INSTANCE RELOAD KEYRING", true, "ALTER INSTANCE RELOAD KEYRING"},
		{"ALTER INSTANCE ROTATE FOO MASTER KEY", false, ""},
		{"ALTER INSTANCE ENABLE FOO REDO_LOG", false, ""},
	}
	RunTest(t, table, false)
}

func TestColumnVisibility(t *testing.T) {
	table := []testCase{
		{"CREATE TABLE t (a INT, b INT INVISIBLE)", true, "CREATE TABLE `t` (`a` INT,`b` INT INVISIBLE)"},
		{"CREATE TABLE t (a INT, b INT VISIBLE)", true, "CREATE TABLE `t` (`a` INT,`b` INT VISIBLE)"},
		{"CREATE TABLE t (a INT, b INT INVISIBLE NOT NULL DEFAULT 5)", true, "CREATE TABLE `t` (`a` INT,`b` INT INVISIBLE NOT NULL DEFAULT 5)"},
		// SHOW CREATE TABLE emits the attribute inside a versioned comment.
		{"CREATE TABLE t (a INT, b INT /*!80023 INVISIBLE */ DEFAULT NULL)", true, "CREATE TABLE `t` (`a` INT,`b` INT INVISIBLE DEFAULT NULL)"},
		{"CREATE TABLE t (a INT, b INT GENERATED ALWAYS AS (a + 1) VIRTUAL INVISIBLE)", true, "CREATE TABLE `t` (`a` INT,`b` INT GENERATED ALWAYS AS(`a`+1) VIRTUAL INVISIBLE)"},
		{"CREATE TABLE t (a INT, b INT GENERATED ALWAYS AS (a + 1) STORED INVISIBLE)", true, "CREATE TABLE `t` (`a` INT,`b` INT GENERATED ALWAYS AS(`a`+1) STORED INVISIBLE)"},
		{"ALTER TABLE t1 ADD COLUMN x INT INVISIBLE", true, "ALTER TABLE `t1` ADD COLUMN `x` INT INVISIBLE"},
		{"ALTER TABLE t1 MODIFY a INT INVISIBLE", true, "ALTER TABLE `t1` MODIFY COLUMN `a` INT INVISIBLE"},
		{"ALTER TABLE t1 MODIFY a INT VISIBLE", true, "ALTER TABLE `t1` MODIFY COLUMN `a` INT VISIBLE"},
		{"ALTER TABLE t1 ALTER COLUMN a SET VISIBLE", true, "ALTER TABLE `t1` ALTER COLUMN `a` SET VISIBLE"},
		{"ALTER TABLE t1 ALTER COLUMN a SET INVISIBLE", true, "ALTER TABLE `t1` ALTER COLUMN `a` SET INVISIBLE"},
		{"ALTER TABLE t1 ALTER a SET INVISIBLE", true, "ALTER TABLE `t1` ALTER COLUMN `a` SET INVISIBLE"},
	}
	RunTest(t, table, false)
}

func TestEngineAttribute(t *testing.T) {
	table := []testCase{
		{`CREATE TABLE t (c INT ENGINE_ATTRIBUTE '{"x":1}')`, true, "CREATE TABLE `t` (`c` INT ENGINE_ATTRIBUTE = '{\"x\":1}')"},
		{`CREATE TABLE t (c INT ENGINE_ATTRIBUTE = '{"x":1}' SECONDARY_ENGINE_ATTRIBUTE = '{"y":2}')`, true, "CREATE TABLE `t` (`c` INT ENGINE_ATTRIBUTE = '{\"x\":1}' SECONDARY_ENGINE_ATTRIBUTE = '{\"y\":2}')"},
		{`CREATE TABLE t (a VARCHAR(200), UNIQUE KEY k (a) VISIBLE ENGINE_ATTRIBUTE '{"a":1}')`, true, "CREATE TABLE `t` (`a` VARCHAR(200),UNIQUE `k`(`a`) VISIBLE ENGINE_ATTRIBUTE = '{\"a\":1}')"},
		{`CREATE INDEX i ON t1 ((a + b)) INVISIBLE ENGINE_ATTRIBUTE '{"x":1}'`, true, "CREATE INDEX `i` ON `t1` ((`a`+`b`)) INVISIBLE ENGINE_ATTRIBUTE = '{\"x\":1}'"},
		{`ALTER TABLE t1 ADD INDEX i (a) ENGINE_ATTRIBUTE '{"x":1}'`, true, "ALTER TABLE `t1` ADD INDEX `i`(`a`) ENGINE_ATTRIBUTE = '{\"x\":1}'"},
	}
	RunTest(t, table, false)
}

func TestVectorType(t *testing.T) {
	table := []testCase{
		{"CREATE TABLE t (v VECTOR)", true, "CREATE TABLE `t` (`v` VECTOR)"},
		{"CREATE TABLE t (v VECTOR(4))", true, "CREATE TABLE `t` (`v` VECTOR(4))"},
		{"CREATE TABLE t (v VECTOR(16383) NOT NULL)", true, "CREATE TABLE `t` (`v` VECTOR(16383) NOT NULL)"},
		{"ALTER TABLE t ADD COLUMN v VECTOR(4)", true, "ALTER TABLE `t` ADD COLUMN `v` VECTOR(4)"},
		{"SELECT CAST(x AS VECTOR) FROM t", true, "SELECT CAST(`x` AS VECTOR) FROM `t`"},
		{"SELECT CAST(x AS VECTOR(8)) FROM t", true, "SELECT CAST(`x` AS VECTOR(8)) FROM `t`"},
		// VECTOR stays usable as an identifier.
		{"CREATE TABLE vector (vector INT)", true, "CREATE TABLE `vector` (`vector` INT)"},
		{"SELECT vector FROM vector", true, "SELECT `vector` FROM `vector`"},
	}
	RunTest(t, table, false)
}

func TestACLCreateUser(t *testing.T) {
	table := []testCase{
		// IDENTIFIED BY RANDOM PASSWORD (MySQL 8.0.18+).
		{"CREATE USER pu4@localhost IDENTIFIED BY RANDOM PASSWORD", true, "CREATE USER `pu4`@`localhost` IDENTIFIED BY RANDOM PASSWORD"},
		{"CREATE USER u1@localhost IDENTIFIED WITH caching_sha2_password BY RANDOM PASSWORD", true, "CREATE USER `u1`@`localhost` IDENTIFIED WITH 'caching_sha2_password' BY RANDOM PASSWORD"},
		// Multi-factor authentication chains (MySQL 8.0.27+).
		{"CREATE USER u2@localhost IDENTIFIED BY 'pw1' AND IDENTIFIED WITH authentication_ldap_simple", true, "CREATE USER `u2`@`localhost` IDENTIFIED BY 'pw1' AND IDENTIFIED WITH 'authentication_ldap_simple'"},
		{"CREATE USER u3@localhost IDENTIFIED BY 'pw1' AND IDENTIFIED WITH authentication_ldap_simple AS 'cn=u3' AND IDENTIFIED WITH authentication_fido", true, "CREATE USER `u3`@`localhost` IDENTIFIED BY 'pw1' AND IDENTIFIED WITH 'authentication_ldap_simple' AS 'cn=u3' AND IDENTIFIED WITH 'authentication_fido'"},
		// IDENTIFIED WITH plugin AS 'hash' must not lose to GRANT's AS clause.
		{"CREATE USER x@l IDENTIFIED WITH 'p' AS 'h'", true, "CREATE USER `x`@`l` IDENTIFIED WITH 'p' AS 'h'"},
		// DEFAULT ROLE clause.
		{"CREATE USER u4 DEFAULT ROLE r1, r2", true, "CREATE USER `u4`@`%` DEFAULT ROLE `r1`@`%`, `r2`@`%`"},
		{"CREATE USER u10 IDENTIFIED BY 'x' DEFAULT ROLE r1 REQUIRE SSL PASSWORD EXPIRE NEVER", true, "CREATE USER `u10`@`%` IDENTIFIED BY 'x' DEFAULT ROLE `r1`@`%` REQUIRE SSL PASSWORD EXPIRE NEVER"},
		// INITIAL AUTHENTICATION (MySQL 8.0.27+ passwordless setup).
		{"CREATE USER u5@localhost IDENTIFIED WITH authentication_webauthn INITIAL AUTHENTICATION IDENTIFIED BY 'boot'", true, "CREATE USER `u5`@`localhost` IDENTIFIED WITH 'authentication_webauthn' INITIAL AUTHENTICATION IDENTIFIED BY 'boot'"},
		{"CREATE USER u6@localhost IDENTIFIED WITH authentication_webauthn INITIAL AUTHENTICATION IDENTIFIED BY RANDOM PASSWORD", true, "CREATE USER `u6`@`localhost` IDENTIFIED WITH 'authentication_webauthn' INITIAL AUTHENTICATION IDENTIFIED BY RANDOM PASSWORD"},
		// PASSWORD REQUIRE CURRENT [OPTIONAL | DEFAULT] (MySQL 8.0.13+).
		{"CREATE USER u7@localhost IDENTIFIED BY 'p' PASSWORD REQUIRE CURRENT", true, "CREATE USER `u7`@`localhost` IDENTIFIED BY 'p' PASSWORD REQUIRE CURRENT"},
		{"CREATE USER u8@localhost IDENTIFIED BY 'p' PASSWORD REQUIRE CURRENT OPTIONAL", true, "CREATE USER `u8`@`localhost` IDENTIFIED BY 'p' PASSWORD REQUIRE CURRENT OPTIONAL"},
		{"CREATE USER u9@localhost IDENTIFIED BY 'p' PASSWORD REQUIRE CURRENT DEFAULT", true, "CREATE USER `u9`@`localhost` IDENTIFIED BY 'p' PASSWORD REQUIRE CURRENT DEFAULT"},
	}
	RunTest(t, table, false)
}

func TestACLAlterUser(t *testing.T) {
	table := []testCase{
		// REPLACE 'current' password verification (MySQL 8.0.14+).
		{"ALTER USER u1@localhost IDENTIFIED BY 'new' REPLACE 'old' RETAIN CURRENT PASSWORD", true, "ALTER USER `u1`@`localhost` IDENTIFIED BY 'new' REPLACE 'old' RETAIN CURRENT PASSWORD"},
		{"ALTER USER u2@localhost IDENTIFIED BY RANDOM PASSWORD RETAIN CURRENT PASSWORD", true, "ALTER USER `u2`@`localhost` IDENTIFIED BY RANDOM PASSWORD RETAIN CURRENT PASSWORD"},
		{"ALTER USER u3@localhost IDENTIFIED BY 'new' REPLACE 'old'", true, "ALTER USER `u3`@`localhost` IDENTIFIED BY 'new' REPLACE 'old'"},
		// DEFAULT ROLE management.
		{"ALTER USER u4@localhost DEFAULT ROLE ALL", true, "ALTER USER `u4`@`localhost` DEFAULT ROLE ALL"},
		{"ALTER USER u5@localhost DEFAULT ROLE NONE", true, "ALTER USER `u5`@`localhost` DEFAULT ROLE NONE"},
		{"ALTER USER IF EXISTS u6@localhost DEFAULT ROLE r1, r2", true, "ALTER USER IF EXISTS `u6`@`localhost` DEFAULT ROLE `r1`@`%`, `r2`@`%`"},
		// Multi-factor management (MySQL 8.0.27+).
		{"ALTER USER u7@localhost ADD 2 FACTOR IDENTIFIED WITH authentication_ldap_simple", true, "ALTER USER `u7`@`localhost` ADD 2 FACTOR IDENTIFIED WITH 'authentication_ldap_simple'"},
		{"ALTER USER u8@localhost MODIFY 3 FACTOR IDENTIFIED WITH authentication_fido", true, "ALTER USER `u8`@`localhost` MODIFY 3 FACTOR IDENTIFIED WITH 'authentication_fido'"},
		{"ALTER USER u9@localhost DROP 2 FACTOR", true, "ALTER USER `u9`@`localhost` DROP 2 FACTOR"},
		{"ALTER USER u13@localhost IDENTIFIED BY 'a' AND IDENTIFIED WITH authentication_ldap_simple", true, "ALTER USER `u13`@`localhost` IDENTIFIED BY 'a' AND IDENTIFIED WITH 'authentication_ldap_simple'"},
		// WebAuthn device registration (MySQL 8.0.27+).
		{"ALTER USER u10@localhost 2 FACTOR INITIATE REGISTRATION", true, "ALTER USER `u10`@`localhost` 2 FACTOR INITIATE REGISTRATION"},
		{"ALTER USER u11@localhost 2 FACTOR FINISH REGISTRATION SET CHALLENGE_RESPONSE AS 'blob'", true, "ALTER USER `u11`@`localhost` 2 FACTOR FINISH REGISTRATION SET CHALLENGE_RESPONSE AS 'blob'"},
		{"ALTER USER u12@localhost 2 FACTOR FINISH REGISTRATION", true, "ALTER USER `u12`@`localhost` 2 FACTOR FINISH REGISTRATION"},
		{"ALTER USER u14@localhost 2 FACTOR UNREGISTER", true, "ALTER USER `u14`@`localhost` 2 FACTOR UNREGISTER"},
	}
	RunTest(t, table, false)
}

func TestACLSetPassword(t *testing.T) {
	table := []testCase{
		{"SET PASSWORD TO RANDOM", true, "SET PASSWORD TO RANDOM"},
		{"SET PASSWORD = 'new' REPLACE 'old'", true, "SET PASSWORD='new' REPLACE 'old'"},
		{"SET PASSWORD TO RANDOM REPLACE 'old'", true, "SET PASSWORD TO RANDOM REPLACE 'old'"},
		{"SET PASSWORD FOR u1@localhost TO RANDOM RETAIN CURRENT PASSWORD", true, "SET PASSWORD FOR `u1`@`localhost` TO RANDOM RETAIN CURRENT PASSWORD"},
		{"SET PASSWORD FOR u1@localhost = 'new' REPLACE 'old' RETAIN CURRENT PASSWORD", true, "SET PASSWORD FOR `u1`@`localhost`='new' REPLACE 'old' RETAIN CURRENT PASSWORD"},
		{"SET PASSWORD = 'new'", true, "SET PASSWORD='new'"},
	}
	RunTest(t, table, false)
}

func TestACLGrantRevoke(t *testing.T) {
	table := []testCase{
		// GRANT role ... WITH ADMIN OPTION (MySQL 8.0+).
		{"GRANT r1, r2 TO u1@localhost WITH ADMIN OPTION", true, "GRANT `r1`@`%`, `r2`@`%` TO `u1`@`localhost` WITH ADMIN OPTION"},
		// GRANT ... AS user [WITH ROLE ...] (MySQL 8.0.16+).
		{"GRANT SELECT ON db1.* TO u1@localhost AS u2@localhost", true, "GRANT SELECT ON `db1`.* TO `u1`@`localhost` AS `u2`@`localhost`"},
		{"GRANT SELECT ON db1.* TO u1@localhost AS u2@localhost WITH ROLE ALL", true, "GRANT SELECT ON `db1`.* TO `u1`@`localhost` AS `u2`@`localhost` WITH ROLE ALL"},
		{"GRANT SELECT ON db1.* TO u1@localhost AS u2@localhost WITH ROLE NONE", true, "GRANT SELECT ON `db1`.* TO `u1`@`localhost` AS `u2`@`localhost` WITH ROLE NONE"},
		{"GRANT SELECT ON db1.* TO u1@localhost AS u2@localhost WITH ROLE DEFAULT", true, "GRANT SELECT ON `db1`.* TO `u1`@`localhost` AS `u2`@`localhost` WITH ROLE DEFAULT"},
		{"GRANT SELECT ON db1.* TO u1@localhost AS u2@localhost WITH ROLE ALL EXCEPT r1", true, "GRANT SELECT ON `db1`.* TO `u1`@`localhost` AS `u2`@`localhost` WITH ROLE ALL EXCEPT `r1`@`%`"},
		{"GRANT SELECT ON db1.* TO u1@localhost AS u2@localhost WITH ROLE r1, r2", true, "GRANT SELECT ON `db1`.* TO `u1`@`localhost` AS `u2`@`localhost` WITH ROLE `r1`@`%`, `r2`@`%`"},
		{"GRANT SELECT ON db1.* TO u1@localhost WITH GRANT OPTION AS u2@localhost WITH ROLE ALL", true, "GRANT SELECT ON `db1`.* TO `u1`@`localhost` WITH GRANT OPTION AS `u2`@`localhost` WITH ROLE ALL"},
		// The 5.7-era GRANT auth-hash form must keep AS bound to the plugin.
		{"GRANT SELECT ON db1.* TO u1 IDENTIFIED WITH 'p' AS 'hash57'", true, "GRANT SELECT ON `db1`.* TO `u1`@`%` IDENTIFIED WITH 'p' AS 'hash57'"},
		// REVOKE [IF EXISTS] ... [IGNORE UNKNOWN USER] (MySQL 8.0.30+).
		{"REVOKE IF EXISTS SELECT ON db1.t1 FROM u1@localhost", true, "REVOKE IF EXISTS SELECT ON `db1`.`t1` FROM `u1`@`localhost`"},
		{"REVOKE IF EXISTS SELECT ON db1.t1 FROM u1@localhost IGNORE UNKNOWN USER", true, "REVOKE IF EXISTS SELECT ON `db1`.`t1` FROM `u1`@`localhost` IGNORE UNKNOWN USER"},
		{"REVOKE SELECT ON db1.t1 FROM u1@localhost IGNORE UNKNOWN USER", true, "REVOKE SELECT ON `db1`.`t1` FROM `u1`@`localhost` IGNORE UNKNOWN USER"},
		{"REVOKE IF EXISTS r1 FROM u1@localhost IGNORE UNKNOWN USER", true, "REVOKE IF EXISTS `r1`@`%` FROM `u1`@`localhost` IGNORE UNKNOWN USER"},
		{"REVOKE ALL PRIVILEGES, GRANT OPTION FROM u1@localhost", true, "REVOKE ALL, GRANT OPTION ON *.* FROM `u1`@`localhost`"},
		{"REVOKE IF EXISTS ALL PRIVILEGES, GRANT OPTION FROM u1@localhost IGNORE UNKNOWN USER", true, "REVOKE IF EXISTS ALL, GRANT OPTION ON *.* FROM `u1`@`localhost` IGNORE UNKNOWN USER"},
		// REVOKE PROXY mirrors GRANT PROXY.
		{"REVOKE PROXY ON u1@localhost FROM u2@localhost", true, "REVOKE PROXY ON `u1`@`localhost` FROM `u2`@`localhost`"},
		{"REVOKE IF EXISTS PROXY ON u1@localhost FROM u2@localhost, u3@localhost IGNORE UNKNOWN USER", true, "REVOKE IF EXISTS PROXY ON `u1`@`localhost` FROM `u2`@`localhost`, `u3`@`localhost` IGNORE UNKNOWN USER"},
	}
	RunTest(t, table, false)
}

func TestDefaultExpression(t *testing.T) {
	// MySQL 8.0.13+ DEFAULT (expr) takes a full expression; the parentheses
	// are part of the statement's meaning and must survive the round trip.
	table := []testCase{
		{"CREATE TABLE ct06 (a INT, b INT DEFAULT (a + 1))", true, "CREATE TABLE `ct06` (`a` INT,`b` INT DEFAULT (`a`+1))"},
		{"CREATE TABLE ct07 (a INT, b INT DEFAULT (a * a))", true, "CREATE TABLE `ct07` (`a` INT,`b` INT DEFAULT (`a`*`a`))"},
		{`CREATE TABLE ct09 (j JSON DEFAULT (CAST('["x"]' AS JSON)))`, true, "CREATE TABLE `ct09` (`j` JSON DEFAULT (CAST(_UTF8MB4'[\"x\"]' AS JSON)))"},
		{"CREATE TABLE ct10 (d DATE DEFAULT (CURRENT_DATE + INTERVAL 1 YEAR))", true, "CREATE TABLE `ct10` (`d` DATE DEFAULT (DATE_ADD(CURRENT_DATE(), INTERVAL 1 YEAR)))"},
		{"ALTER TABLE t1 ADD COLUMN y INT DEFAULT (a + 1)", true, "ALTER TABLE `t1` ADD COLUMN `y` INT DEFAULT (`a`+1)"},
		{"CREATE TABLE lx07 (a DATE DEFAULT (DATE '2020-01-01'))", true, "CREATE TABLE `lx07` (`a` DATE DEFAULT (DATE '2020-01-01'))"},
		{"CREATE TABLE lx08 (a TIMESTAMP DEFAULT (TIMESTAMP '2020-01-01 00:00:00'))", true, "CREATE TABLE `lx08` (`a` TIMESTAMP DEFAULT (TIMESTAMP '2020-01-01 00:00:00'))"},
		// Nested parentheses stay nested: MySQL prints DEFAULT ((1 + 2)).
		{"CREATE TABLE t (b INT DEFAULT ((1+2)))", true, "CREATE TABLE `t` (`b` INT DEFAULT ((1+2)))"},
		{"CREATE TABLE t (u VARCHAR(36) DEFAULT (UUID()))", true, "CREATE TABLE `t` (`u` VARCHAR(36) DEFAULT (UUID()))"},
		{"CREATE TABLE t (j JSON DEFAULT ('{}'))", true, "CREATE TABLE `t` (`j` JSON DEFAULT (_UTF8MB4'{}'))"},
		{"CREATE TABLE t (a INT, b INT DEFAULT (a))", true, "CREATE TABLE `t` (`a` INT,`b` INT DEFAULT (`a`))"},
		// Parenthesized NOW is an expression default and keeps its shape;
		// the bare timestamp-default form still folds to CURRENT_TIMESTAMP.
		{"CREATE TABLE t (ts DATETIME DEFAULT (NOW()))", true, "CREATE TABLE `t` (`ts` DATETIME DEFAULT (NOW()))"},
		{"CREATE TABLE t (ts DATETIME DEFAULT NOW())", true, "CREATE TABLE `t` (`ts` DATETIME DEFAULT CURRENT_TIMESTAMP())"},
		{"CREATE TABLE t (g POINT DEFAULT (POINT(0, 0)))", true, "CREATE TABLE `t` (`g` POINT DEFAULT (POINT(0, 0)))"},
		// KILL shares the builtin-function production; keep it working.
		{"KILL CONNECTION_ID()", true, "KILL CONNECTION_ID()"},
		// A bare SELECT is a syntax error in MySQL's expr too; a doubly
		// parenthesized subquery parses and is rejected semantically.
		{"CREATE TABLE t (b INT DEFAULT (SELECT 1))", false, ""},
		{"CREATE TABLE t (b INT DEFAULT ((SELECT 1)))", true, "CREATE TABLE `t` (`b` INT DEFAULT ((SELECT 1)))"},
	}
	RunTest(t, table, false)
}

// TestCharsetRegistry covers parse-level acceptance of every character set
// MySQL knows (not just the handful TiDB could store), the utf8mb3_* collation
// spellings, the utf8mb4 locale collations (IDs 310-323), and the
// BYTE/ASCII/UNICODE column-attribute shorthands from
// opt_charset_with_opt_binary. Restore shapes were verified against MySQL
// 8.0.46 SHOW CREATE TABLE.
func TestCharsetRegistry(t *testing.T) {
	table := []testCase{
		// BYTE/ASCII/UNICODE shorthands: BYTE means the binary charset (MySQL
		// prints binary(10)), ASCII means latin1, UNICODE means ucs2.
		{"CREATE TABLE t (a CHAR(10) BYTE)", true, "CREATE TABLE `t` (`a` BINARY(10))"},
		{"CREATE TABLE t (a CHAR(10) ASCII)", true, "CREATE TABLE `t` (`a` CHAR(10) CHARACTER SET LATIN1)"},
		{"CREATE TABLE t (a CHAR(10) UNICODE)", true, "CREATE TABLE `t` (`a` CHAR(10) CHARACTER SET UCS2)"},
		{"CREATE TABLE t (a CHAR ASCII)", true, "CREATE TABLE `t` (`a` CHAR CHARACTER SET LATIN1)"},
		{"CREATE TABLE t (a VARCHAR(10) ASCII)", true, "CREATE TABLE `t` (`a` VARCHAR(10) CHARACTER SET LATIN1)"},
		{"CREATE TABLE t (a VARCHAR(10) BYTE)", true, "CREATE TABLE `t` (`a` VARBINARY(10))"},
		{"SELECT CAST('a' AS CHAR(5) UNICODE)", true, "SELECT CAST(_UTF8MB4'a' AS CHAR(5) CHARSET UCS2)"},
		{"SELECT CAST('a' AS CHAR(5) BYTE)", true, "SELECT CAST(_UTF8MB4'a' AS BINARY(5))"},
		// Charset name clauses accept the full registry.
		{"CREATE TABLE t (a TEXT CHARACTER SET ucs2)", true, "CREATE TABLE `t` (`a` TEXT CHARACTER SET UCS2)"},
		{"CREATE TABLE t (a CHAR(10) CHARACTER SET latin2 COLLATE latin2_general_ci)", true, "CREATE TABLE `t` (`a` CHAR(10) CHARACTER SET LATIN2 COLLATE latin2_general_ci)"},
		{"CREATE TABLE t (a CHAR(10)) DEFAULT CHARSET=koi8r", true, "CREATE TABLE `t` (`a` CHAR(10)) DEFAULT CHARACTER SET = KOI8R"},
		{"CREATE DATABASE d CHARACTER SET ucs2", true, "CREATE DATABASE `d` CHARACTER SET = ucs2"},
		{"ALTER TABLE t CONVERT TO CHARACTER SET ucs2", true, "ALTER TABLE `t` CONVERT TO CHARACTER SET UCS2"},
		{"SELECT CONVERT('a' USING ucs2)", true, "SELECT CONVERT(_UTF8MB4'a' USING 'ucs2')"},
		{"SET NAMES ucs2", true, "SET NAMES 'ucs2'"},
		// String-literal introducers work for any known charset.
		{"SELECT _ucs2 X'0078'", true, "SELECT _UCS2 x'0078'"},
		{"SELECT _latin2'abc'", true, "SELECT _LATIN2'abc'"},
		{"SELECT _utf16 B'01111000'", true, "SELECT _UTF16 b'1111000'"},
		// utf8mb3_* spellings alias to the registry's utf8_* names, matching
		// the existing utf8mb3 -> utf8 charset canonicalization.
		{"CREATE TABLE t (a TEXT COLLATE utf8mb3_danish_ci)", true, "CREATE TABLE `t` (`a` TEXT COLLATE utf8_danish_ci)"},
		{"SELECT 'a' COLLATE utf8mb3_czech_ci", true, "SELECT _UTF8MB4'a' COLLATE utf8_czech_ci"},
		{"CREATE TABLE t (a TEXT CHARACTER SET utf8mb3)", true, "CREATE TABLE `t` (`a` TEXT CHARACTER SET UTF8)"},
		// utf8mb4 locale collations.
		{"CREATE TABLE t (a TEXT COLLATE utf8mb4_nb_0900_ai_ci)", true, "CREATE TABLE `t` (`a` TEXT COLLATE utf8mb4_nb_0900_ai_ci)"},
		{"CREATE TABLE t (a TEXT COLLATE utf8mb4_mn_cyrl_0900_as_cs)", true, "CREATE TABLE `t` (`a` TEXT COLLATE utf8mb4_mn_cyrl_0900_as_cs)"},
		{"SET NAMES utf8mb4 COLLATE utf8mb4_sr_latn_0900_ai_ci", true, "SET NAMES 'utf8mb4' COLLATE 'utf8mb4_sr_latn_0900_ai_ci'"},
		// Names MySQL does not know still parse: unknown character sets
		// and collations are execution-time errors in MySQL (1115/1273,
		// not 1064), e.g. user-defined LDML collations.
		{"CREATE TABLE t (a CHAR(10) CHARACTER SET nosuch)", true, "CREATE TABLE `t` (`a` CHAR(10) CHARACTER SET NOSUCH)"},
		{"CREATE TABLE t (a TEXT COLLATE nosuch_ci)", true, "CREATE TABLE `t` (`a` TEXT COLLATE nosuch_ci)"},
	}
	RunTest(t, table, false)
}

// TestLibraryDDL covers CREATE/ALTER/DROP LIBRARY and GRANT ... ON LIBRARY
// (MySQL 9.x), including dollar-quoted string bodies ($tag$ ... $tag$), which
// the lexer accepts and restore normalizes to ordinary quoted strings.
// Grammar shapes were validated against MySQL 9.7.0.
func TestLibraryDDL(t *testing.T) {
	table := []testCase{
		{"CREATE LIBRARY lib1 LANGUAGE JAVASCRIPT AS 'export function f(n) {return n}'", true, "CREATE LIBRARY `lib1` LANGUAGE JAVASCRIPT AS 'export function f(n) {return n}'"},
		{"CREATE LIBRARY IF NOT EXISTS lib2 COMMENT 'x' LANGUAGE JAVASCRIPT AS $$ export function g() {} $$", true, "CREATE LIBRARY IF NOT EXISTS `lib2` COMMENT 'x' LANGUAGE JAVASCRIPT AS ' export function g() {} '"},
		{"CREATE LIBRARY lib3 LANGUAGE JAVASCRIPT AS $mark$ export function h() {} $mark$", true, "CREATE LIBRARY `lib3` LANGUAGE JAVASCRIPT AS ' export function h() {} '"},
		{"CREATE LIBRARY probedb.libq LANGUAGE javascript AS 'x'", true, "CREATE LIBRARY `probedb`.`libq` LANGUAGE JAVASCRIPT AS 'x'"},
		{"CREATE LIBRARY q LANGUAGE JAVASCRIPT AS $$it's$$", true, "CREATE LIBRARY `q` LANGUAGE JAVASCRIPT AS 'it''s'"},
		{"ALTER LIBRARY lib1 COMMENT 'y'", true, "ALTER LIBRARY `lib1` COMMENT 'y'"},
		{"ALTER LIBRARY probedb.lib1 COMMENT 'y'", true, "ALTER LIBRARY `probedb`.`lib1` COMMENT 'y'"},
		{"DROP LIBRARY IF EXISTS lib1", true, "DROP LIBRARY IF EXISTS `lib1`"},
		{"DROP LIBRARY probedb.lib2", true, "DROP LIBRARY `probedb`.`lib2`"},
		{"GRANT EXECUTE ON LIBRARY probedb.lib1 TO u1@localhost", true, "GRANT EXECUTE ON LIBRARY `probedb`.`lib1` TO `u1`@`localhost`"},
		{"GRANT ALTER ROUTINE ON LIBRARY probedb.lib1 TO u1@localhost", true, "GRANT ALTER ROUTINE ON LIBRARY `probedb`.`lib1` TO `u1`@`localhost`"},
		{"REVOKE EXECUTE ON LIBRARY probedb.lib1 FROM u1@localhost", true, "REVOKE EXECUTE ON LIBRARY `probedb`.`lib1` FROM `u1`@`localhost`"},
		// A database that happens to be named library still works: the
		// following token ('.' vs identifier) picks the interpretation.
		{"GRANT SELECT ON library.t TO u1@localhost", true, "GRANT SELECT ON `library`.`t` TO `u1`@`localhost`"},
		// Identifiers starting with or containing $ are unaffected by
		// dollar-quote lexing.
		{"SELECT $x", true, "SELECT `$x`"},
		{"SELECT 1 AS $y", true, "SELECT 1 AS `$y`"},
		{"SELECT a$b FROM t", true, "SELECT `a$b` FROM `t`"},
		// Same rejections as MySQL 9.7.
		{"CREATE LIBRARY lib9 LANGUAGE JAVASCRIPT AS $$ unterminated", false, ""},
		{"ALTER LIBRARY lib1", false, ""},
		{"CREATE OR REPLACE LIBRARY lib4 LANGUAGE JAVASCRIPT AS 'x'", false, ""},
		{"CREATE LIBRARY lib5 AS 'x' LANGUAGE JAVASCRIPT", false, ""},
	}
	RunTest(t, table, false)
}

// TestFlushTargetList covers FLUSH with a comma-separated option list; the
// table form stays exclusive as in MySQL's grammar.
func TestFlushTargetList(t *testing.T) {
	table := []testCase{
		{"FLUSH STATUS, USER_RESOURCES", true, "FLUSH STATUS, USER_RESOURCES"},
		{"FLUSH LOCAL STATUS, USER_RESOURCES, PRIVILEGES", true, "FLUSH NO_WRITE_TO_BINLOG STATUS, USER_RESOURCES, PRIVILEGES"},
		{"FLUSH NO_WRITE_TO_BINLOG ERROR LOGS, ENGINE LOGS, OPTIMIZER_COSTS", true, "FLUSH NO_WRITE_TO_BINLOG ERROR LOGS, ENGINE LOGS, OPTIMIZER_COSTS"},
		{"FLUSH RELAY LOGS FOR CHANNEL 'ch1', STATUS", true, "FLUSH RELAY LOGS FOR CHANNEL 'ch1', STATUS"},
		{"FLUSH STATUS", true, "FLUSH STATUS"},
		{"FLUSH TABLES t1, t2 WITH READ LOCK", true, "FLUSH TABLES `t1`, `t2` WITH READ LOCK"},
	}
	RunTest(t, table, false)
}

func TestNotSecondaryColumn(t *testing.T) {
	table := []testCase{
		{"CREATE TABLE t (c INT NOT SECONDARY)", true, "CREATE TABLE `t` (`c` INT NOT SECONDARY)"},
		{"CREATE TABLE t (c INT NOT NULL NOT SECONDARY DEFAULT 5)", true, "CREATE TABLE `t` (`c` INT NOT NULL NOT SECONDARY DEFAULT 5)"},
		{"ALTER TABLE t1 MODIFY c INT NOT SECONDARY", true, "ALTER TABLE `t1` MODIFY COLUMN `c` INT NOT SECONDARY"},
		// SECONDARY stays usable as an identifier.
		{"CREATE TABLE secondary (secondary INT)", true, "CREATE TABLE `secondary` (`secondary` INT)"},
		{"CREATE TABLE t (c INT SECONDARY)", false, ""},
	}
	RunTest(t, table, false)
}

func TestCreateTableStartTransaction(t *testing.T) {
	// MySQL 8.0.21+ logs CREATE TABLE ... SELECT as CREATE TABLE ... START
	// TRANSACTION followed by row events, so the binlog form must parse.
	table := []testCase{
		{"CREATE TABLE t (a INT) START TRANSACTION", true, "CREATE TABLE `t` (`a` INT) START TRANSACTION"},
		{"CREATE TABLE IF NOT EXISTS t (a INT) ENGINE = InnoDB START TRANSACTION", true, "CREATE TABLE IF NOT EXISTS `t` (`a` INT) ENGINE = InnoDB START TRANSACTION"},
		{"CREATE TABLE t (a INT) START TRANSACTION AS SELECT 1", false, ""},
		{"CREATE TABLE t (a INT) START", false, ""},
	}
	RunTest(t, table, false)
}

func TestViewWithCheckOption(t *testing.T) {
	table := []testCase{
		// Plain WITH CHECK OPTION means CASCADED; restore normalizes the
		// default away, like it already does for an explicit CASCADED.
		{"CREATE VIEW v AS SELECT a FROM t WITH CHECK OPTION", true, "CREATE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS SELECT `a` FROM `t`"},
		{"CREATE VIEW v AS SELECT a FROM t WITH CASCADED CHECK OPTION", true, "CREATE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS SELECT `a` FROM `t`"},
		{"CREATE VIEW v AS SELECT a FROM t WITH LOCAL CHECK OPTION", true, "CREATE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS SELECT `a` FROM `t` WITH LOCAL CHECK OPTION"},
		{"CREATE VIEW v AS SELECT a FROM t WITH CHECK", false, ""},
	}
	RunTest(t, table, false)
}

func TestDerivedTableColumnList(t *testing.T) {
	table := []testCase{
		{"SELECT * FROM (SELECT 1, 2) dt (foo, bar)", true, "SELECT * FROM (SELECT 1,2) AS `dt`(`foo`, `bar`)"},
		{"SELECT * FROM (SELECT 1) AS dt (foo)", true, "SELECT * FROM (SELECT 1) AS `dt`(`foo`)"},
		{"CREATE VIEW pv8 AS SELECT * FROM (SELECT 1, 2) dt (foo, bar)", true, "CREATE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `pv8` AS SELECT * FROM (SELECT 1,2) AS `dt`(`foo`, `bar`)"},
		// A column list requires an alias.
		{"SELECT * FROM (SELECT 1) (foo)", false, ""},
	}
	RunTest(t, table, false)
}

func TestTableMaintenanceStatements(t *testing.T) {
	table := []testCase{
		{"CHECK TABLE t1", true, "CHECK TABLE `t1`"},
		{"CHECK TABLES t1, t2 FOR UPGRADE", true, "CHECK TABLE `t1`, `t2` FOR UPGRADE"},
		{"CHECK TABLE t1 QUICK FAST MEDIUM EXTENDED CHANGED", true, "CHECK TABLE `t1` QUICK FAST MEDIUM EXTENDED CHANGED"},
		{"CHECK TABLE t1 UPGRADE", false, ""},
		{"CHECKSUM TABLE t1, t2", true, "CHECKSUM TABLE `t1`, `t2`"},
		{"CHECKSUM TABLE t1 QUICK", true, "CHECKSUM TABLE `t1` QUICK"},
		{"CHECKSUM TABLE t1 EXTENDED", true, "CHECKSUM TABLE `t1` EXTENDED"},
	}
	RunTest(t, table, false)
}

func TestHandlerStatements(t *testing.T) {
	table := []testCase{
		{"HANDLER t1 OPEN", true, "HANDLER `t1` OPEN"},
		{"HANDLER test.t1 OPEN AS h1", true, "HANDLER `test`.`t1` OPEN AS `h1`"},
		{"HANDLER t1 OPEN h1", true, "HANDLER `t1` OPEN AS `h1`"},
		{"HANDLER h1 CLOSE", true, "HANDLER `h1` CLOSE"},
		{"HANDLER t1 READ FIRST", true, "HANDLER `t1` READ FIRST"},
		{"HANDLER t1 READ NEXT LIMIT 5", true, "HANDLER `t1` READ NEXT LIMIT 5"},
		{"HANDLER t1 READ idx PREV WHERE a > 1", true, "HANDLER `t1` READ `idx` PREV WHERE `a`>1"},
		{"HANDLER t1 READ idx LAST", true, "HANDLER `t1` READ `idx` LAST"},
		{"HANDLER t1 READ `PRIMARY` = (1, 'x')", true, "HANDLER `t1` READ `PRIMARY` = (1, _UTF8MB4'x')"},
		{"HANDLER t1 READ idx <= (10) WHERE b = 2 LIMIT 3", true, "HANDLER `t1` READ `idx` <= (10) WHERE `b`=2 LIMIT 3"},
		// Natural-order scans only go forward.
		{"HANDLER t1 READ PREV", false, ""},
		{"HANDLER t1 READ idx != (1)", false, ""},
	}
	RunTest(t, table, false)
}

func TestPurgeBinaryLogs(t *testing.T) {
	table := []testCase{
		{"PURGE BINARY LOGS TO 'binlog.000001'", true, "PURGE BINARY LOGS TO 'binlog.000001'"},
		{"PURGE MASTER LOGS TO 'binlog.000001'", true, "PURGE BINARY LOGS TO 'binlog.000001'"},
		{"PURGE BINARY LOGS BEFORE '2026-08-01 00:00:00'", true, "PURGE BINARY LOGS BEFORE _UTF8MB4'2026-08-01 00:00:00'"},
		{"PURGE BINARY LOGS BEFORE NOW() - INTERVAL 3 DAY", true, "PURGE BINARY LOGS BEFORE DATE_SUB(NOW(), INTERVAL 3 DAY)"},
		{"PURGE BINARY LOGS", false, ""},
	}
	RunTest(t, table, false)
}

func TestImportTable(t *testing.T) {
	table := []testCase{
		{"IMPORT TABLE FROM 't1.sdi'", true, "IMPORT TABLE FROM 't1.sdi'"},
		{"IMPORT TABLE FROM 'sdi_dir/*.sdi', 't2.sdi'", true, "IMPORT TABLE FROM 'sdi_dir/*.sdi', 't2.sdi'"},
		{"IMPORT TABLE FROM", false, ""},
	}
	RunTest(t, table, false)
}

func TestKeyCacheStatements(t *testing.T) {
	table := []testCase{
		{"CACHE INDEX t1 IN hot_cache", true, "CACHE INDEX `t1` IN `hot_cache`"},
		{"CACHE INDEX t1, t2 IN DEFAULT", true, "CACHE INDEX `t1`, `t2` IN `default`"},
		{"CACHE INDEX t1 INDEX (i1, i2) IN hot_cache", true, "CACHE INDEX `t1` INDEX (`i1`, `i2`) IN `hot_cache`"},
		{"CACHE INDEX t1 KEY (i1) IN hot_cache", true, "CACHE INDEX `t1` INDEX (`i1`) IN `hot_cache`"},
		{"CACHE INDEX t1 PARTITION (p0, p1) IN hot_cache", true, "CACHE INDEX `t1` PARTITION (`p0`, `p1`) IN `hot_cache`"},
		{"CACHE INDEX t1 PARTITION (ALL) INDEX (i1) IN hot_cache", true, "CACHE INDEX `t1` PARTITION (ALL) INDEX (`i1`) IN `hot_cache`"},
		{"CACHE INDEX t1", false, ""},
		{"LOAD INDEX INTO CACHE t1", true, "LOAD INDEX INTO CACHE `t1`"},
		{"LOAD INDEX INTO CACHE t1 IGNORE LEAVES, t2", true, "LOAD INDEX INTO CACHE `t1` IGNORE LEAVES, `t2`"},
		{"LOAD INDEX INTO CACHE t1 PARTITION (p0) INDEX (i1) IGNORE LEAVES", true, "LOAD INDEX INTO CACHE `t1` PARTITION (`p0`) INDEX (`i1`) IGNORE LEAVES"},
	}
	RunTest(t, table, false)
}

func TestPluginAndComponentStatements(t *testing.T) {
	table := []testCase{
		{"INSTALL PLUGIN clone SONAME 'mysql_clone.so'", true, "INSTALL PLUGIN `clone` SONAME 'mysql_clone.so'"},
		{"UNINSTALL PLUGIN clone", true, "UNINSTALL PLUGIN `clone`"},
		{"INSTALL COMPONENT 'file://component_keyring_file'", true, "INSTALL COMPONENT 'file://component_keyring_file'"},
		{"INSTALL COMPONENT 'file://a', 'file://b'", true, "INSTALL COMPONENT 'file://a', 'file://b'"},
		{"INSTALL COMPONENT 'file://a' SET GLOBAL x.y = 1", true, "INSTALL COMPONENT 'file://a' SET @@GLOBAL.`x.y`=1"},
		{"UNINSTALL COMPONENT 'file://a', 'file://b'", true, "UNINSTALL COMPONENT 'file://a', 'file://b'"},
		{"INSTALL PLUGIN clone", false, ""},
	}
	RunTest(t, table, false)
}

func TestServerStatements(t *testing.T) {
	table := []testCase{
		{"CREATE SERVER s FOREIGN DATA WRAPPER mysql OPTIONS (USER 'u', HOST '127.0.0.1', DATABASE 'db')", true, "CREATE SERVER `s` FOREIGN DATA WRAPPER `mysql` OPTIONS (USER 'u', HOST '127.0.0.1', DATABASE 'db')"},
		{"CREATE SERVER 's1' FOREIGN DATA WRAPPER 'mysql' OPTIONS (PORT 3306)", true, "CREATE SERVER `s1` FOREIGN DATA WRAPPER 'mysql' OPTIONS (PORT 3306)"},
		{"ALTER SERVER s OPTIONS (PASSWORD 'p', SOCKET '/tmp/x.sock', OWNER 'o')", true, "ALTER SERVER `s` OPTIONS (PASSWORD 'p', SOCKET '/tmp/x.sock', OWNER 'o')"},
		{"DROP SERVER IF EXISTS s", true, "DROP SERVER IF EXISTS `s`"},
		{"DROP SERVER s", true, "DROP SERVER `s`"},
		{"CREATE SERVER s FOREIGN DATA WRAPPER mysql", false, ""},
	}
	RunTest(t, table, false)
}

func TestResourceGroupStatements(t *testing.T) {
	table := []testCase{
		{"CREATE RESOURCE GROUP rg TYPE = SYSTEM", true, "CREATE RESOURCE GROUP `rg` TYPE = SYSTEM"},
		{"CREATE RESOURCE GROUP rg TYPE USER VCPU = 0-3, 8 THREAD_PRIORITY = -5 DISABLE", true, "CREATE RESOURCE GROUP `rg` TYPE = USER VCPU = 0-3, 8 THREAD_PRIORITY = -5 DISABLE"},
		{"ALTER RESOURCE GROUP rg VCPU 2 ENABLE", true, "ALTER RESOURCE GROUP `rg` VCPU = 2 ENABLE"},
		{"ALTER RESOURCE GROUP rg DISABLE FORCE", true, "ALTER RESOURCE GROUP `rg` DISABLE FORCE"},
		{"DROP RESOURCE GROUP rg FORCE", true, "DROP RESOURCE GROUP `rg` FORCE"},
		{"SET RESOURCE GROUP rg", true, "SET RESOURCE GROUP `rg`"},
		{"SET RESOURCE GROUP rg FOR 14, 78", true, "SET RESOURCE GROUP `rg` FOR 14, 78"},
		{"CREATE RESOURCE GROUP rg", false, ""},
		// SET of a variable that happens to be named resource still works.
		{"SET resource = 1", true, "SET @@SESSION.`resource`=1"},
	}
	RunTest(t, table, false)
}

func TestCloneStatements(t *testing.T) {
	table := []testCase{
		{"CLONE LOCAL DATA DIRECTORY = '/tmp/clone_dir'", true, "CLONE LOCAL DATA DIRECTORY = '/tmp/clone_dir'"},
		{"CLONE LOCAL DATA DIRECTORY '/tmp/clone_dir'", true, "CLONE LOCAL DATA DIRECTORY = '/tmp/clone_dir'"},
		{"CLONE INSTANCE FROM 'u'@'donor' : 3306 IDENTIFIED BY 'pw'", true, "CLONE INSTANCE FROM `u`@`donor`:3306 IDENTIFIED BY 'pw'"},
		{"CLONE INSTANCE FROM u@h:3306 IDENTIFIED BY 'pw' DATA DIRECTORY '/d' REQUIRE SSL", true, "CLONE INSTANCE FROM `u`@`h`:3306 IDENTIFIED BY 'pw' DATA DIRECTORY = '/d' REQUIRE SSL"},
		{"CLONE INSTANCE FROM u@h:3306 IDENTIFIED BY 'pw' REQUIRE NO SSL", true, "CLONE INSTANCE FROM `u`@`h`:3306 IDENTIFIED BY 'pw' REQUIRE NO SSL"},
		{"CLONE INSTANCE FROM u@h IDENTIFIED BY 'pw'", false, ""},
	}
	RunTest(t, table, false)
}

func TestInstanceLockStatements(t *testing.T) {
	table := []testCase{
		{"LOCK INSTANCE FOR BACKUP", true, "LOCK INSTANCE FOR BACKUP"},
		{"UNLOCK INSTANCE", true, "UNLOCK INSTANCE"},
		{"LOCK INSTANCE", false, ""},
		// LOCK TABLES gained aliases and LOW_PRIORITY WRITE.
		{"LOCK TABLES t1 AS a1 READ, t2 LOW_PRIORITY WRITE", true, "LOCK TABLES `t1` AS `a1` READ, `t2` LOW_PRIORITY WRITE"},
		{"LOCK TABLE t1 a1 WRITE", true, "LOCK TABLES `t1` AS `a1` WRITE"},
	}
	RunTest(t, table, false)
}

func TestChangeReplicationStatements(t *testing.T) {
	table := []testCase{
		{"CHANGE REPLICATION SOURCE TO SOURCE_HOST='127.0.0.1', SOURCE_PORT=3306, SOURCE_USER='root'", true, "CHANGE REPLICATION SOURCE TO SOURCE_HOST = _UTF8MB4'127.0.0.1', SOURCE_PORT = 3306, SOURCE_USER = _UTF8MB4'root'"},
		// The deprecated spelling parses to the same node and restores modern.
		{"CHANGE MASTER TO MASTER_HOST='h', MASTER_AUTO_POSITION=1 FOR CHANNEL 'ch'", true, "CHANGE REPLICATION SOURCE TO MASTER_HOST = _UTF8MB4'h', MASTER_AUTO_POSITION = 1 FOR CHANNEL 'ch'"},
		{"CHANGE REPLICATION SOURCE TO SOURCE_SSL_VERIFY_SERVER_CERT=0, SOURCE_DELAY=3600", true, "CHANGE REPLICATION SOURCE TO SOURCE_SSL_VERIFY_SERVER_CERT = 0, SOURCE_DELAY = 3600"},
		{"CHANGE REPLICATION FILTER REPLICATE_DO_DB=(db1,db2), REPLICATE_IGNORE_TABLE=(db1.t1)", true, "CHANGE REPLICATION FILTER REPLICATE_DO_DB = (`db1`, `db2`), REPLICATE_IGNORE_TABLE = (`db1`.`t1`)"},
		{"CHANGE REPLICATION FILTER REPLICATE_WILD_DO_TABLE=('db1.new%'), REPLICATE_REWRITE_DB=((db1,db2)) FOR CHANNEL 'ch'", true, "CHANGE REPLICATION FILTER REPLICATE_WILD_DO_TABLE = ('db1.new%'), REPLICATE_REWRITE_DB = ((`db1`, `db2`)) FOR CHANNEL 'ch'"},
		{"CHANGE REPLICATION FILTER REPLICATE_DO_DB=()", true, "CHANGE REPLICATION FILTER REPLICATE_DO_DB = ()"},
		{"CHANGE REPLICATION SOURCE TO", false, ""},
	}
	RunTest(t, table, false)
}

func TestStartStopReplica(t *testing.T) {
	table := []testCase{
		{"START REPLICA", true, "START REPLICA"},
		{"START SLAVE", true, "START REPLICA"},
		{"START REPLICA IO_THREAD", true, "START REPLICA IO_THREAD"},
		{"START REPLICA IO_THREAD, SQL_THREAD", true, "START REPLICA IO_THREAD, SQL_THREAD"},
		{"START REPLICA SQL_THREAD UNTIL SQL_AFTER_GTIDS = '3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5'", true, "START REPLICA SQL_THREAD UNTIL SQL_AFTER_GTIDS = _UTF8MB4'3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5'"},
		{"START SLAVE UNTIL MASTER_LOG_FILE='master1-bin.000001', MASTER_LOG_POS=4", true, "START REPLICA UNTIL MASTER_LOG_FILE = _UTF8MB4'master1-bin.000001', MASTER_LOG_POS = 4"},
		{"START REPLICA USER='root' PASSWORD='' DEFAULT_AUTH='mysql_native_password'", true, "START REPLICA USER = _UTF8MB4'root' PASSWORD = _UTF8MB4'' DEFAULT_AUTH = _UTF8MB4'mysql_native_password'"},
		{"START REPLICA IO_THREAD UNTIL SQL_AFTER_MTS_GAPS USER='u' PASSWORD='p' FOR CHANNEL 'ch1'", true, "START REPLICA IO_THREAD UNTIL SQL_AFTER_MTS_GAPS USER = _UTF8MB4'u' PASSWORD = _UTF8MB4'p' FOR CHANNEL 'ch1'"},
		{"START GROUP_REPLICATION", true, "START GROUP_REPLICATION"},
		{"START GROUP_REPLICATION USER='rpl_user', PASSWORD='pw'", true, "START GROUP_REPLICATION USER = _UTF8MB4'rpl_user', PASSWORD = _UTF8MB4'pw'"},
		{"STOP REPLICA", true, "STOP REPLICA"},
		{"STOP SLAVE", true, "STOP REPLICA"},
		{"STOP REPLICA IO_THREAD FOR CHANNEL ''", true, "STOP REPLICA IO_THREAD FOR CHANNEL ''"},
		{"STOP GROUP_REPLICATION", true, "STOP GROUP_REPLICATION"},
		{"START REPLICA UNTIL", false, ""},
		{"START REPLICA IO_THREAD SQL_THREAD", false, ""},
		{"STOP REPLICA UNTIL SOURCE_LOG_POS=4", false, ""},
	}
	RunTest(t, table, false)
}

func TestResetStatements(t *testing.T) {
	table := []testCase{
		{"RESET REPLICA", true, "RESET REPLICA"},
		{"RESET SLAVE ALL FOR CHANNEL 'ch'", true, "RESET REPLICA ALL FOR CHANNEL 'ch'"},
		{"RESET MASTER", true, "RESET BINARY LOGS AND GTIDS"},
		{"RESET MASTER TO 1234", true, "RESET BINARY LOGS AND GTIDS TO 1234"},
		{"RESET BINARY LOGS AND GTIDS", true, "RESET BINARY LOGS AND GTIDS"},
		{"RESET PERSIST", true, "RESET PERSIST"},
		{"RESET PERSIST system_var", true, "RESET PERSIST `system_var`"},
		{"RESET PERSIST IF EXISTS innodb_buffer_pool_size", true, "RESET PERSIST IF EXISTS `innodb_buffer_pool_size`"},
		{"RESET", false, ""},
	}
	RunTest(t, table, false)
}

func TestSetPersist(t *testing.T) {
	table := []testCase{
		{"SET PERSIST max_connections = 1000", true, "SET @@PERSIST.`max_connections`=1000"},
		{"SET PERSIST_ONLY back_log = 100", true, "SET @@PERSIST_ONLY.`back_log`=100"},
		{"SET PERSIST innodb_buffer_pool_size = DEFAULT", true, "SET @@PERSIST.`innodb_buffer_pool_size`=DEFAULT"},
		{"SET @@PERSIST.max_connections = 500", true, "SET @@PERSIST.`max_connections`=500"},
		{"SET @@PERSIST_ONLY.back_log = 99", true, "SET @@PERSIST_ONLY.`back_log`=99"},
		// New keywords must keep working as plain identifiers.
		{"SELECT filter, gtids, io_thread, sql_thread, stop, default_auth, plugin_dir FROM t", true, "SELECT `filter`,`gtids`,`io_thread`,`sql_thread`,`stop`,`default_auth`,`plugin_dir` FROM `t`"},
	}
	RunTest(t, table, false)
}

func TestExplainVariants(t *testing.T) {
	table := []testCase{
		{"EXPLAIN FORMAT=TREE SELECT * FROM t1", true, "EXPLAIN FORMAT = 'TREE' SELECT * FROM `t1`"},
		{"EXPLAIN FORMAT = TREE INSERT INTO t1 VALUES (1)", true, "EXPLAIN FORMAT = 'TREE' INSERT INTO `t1` VALUES (1)"},
		{"EXPLAIN ANALYZE FORMAT=TREE SELECT 1", true, "EXPLAIN ANALYZE FORMAT = 'TREE' SELECT 1"},
		{"EXPLAIN ANALYZE FORMAT='TREE' SELECT 1", true, "EXPLAIN ANALYZE FORMAT = 'TREE' SELECT 1"},
		{"EXPLAIN FORMAT=JSON INTO @e SELECT 1", true, "EXPLAIN FORMAT = 'JSON' INTO @e SELECT 1"},
		{"EXPLAIN ANALYZE FORMAT=JSON INTO @e SELECT a FROM t", true, "EXPLAIN ANALYZE FORMAT = 'JSON' INTO @e SELECT `a` FROM `t`"},
		{"EXPLAIN INTO @e SELECT 1", false, ""},
	}
	RunTest(t, table, false)
}

func TestShowRoutineStatements(t *testing.T) {
	table := []testCase{
		{"SHOW CREATE PROCEDURE db1.p1", true, "SHOW CREATE PROCEDURE `db1`.`p1`"},
		{"SHOW CREATE FUNCTION f1", true, "SHOW CREATE FUNCTION `f1`"},
		{"SHOW CREATE TRIGGER trg1", true, "SHOW CREATE TRIGGER `trg1`"},
		{"SHOW CREATE EVENT db1.ev1", true, "SHOW CREATE EVENT `db1`.`ev1`"},
		{"SHOW CREATE LIBRARY db1.lib1", true, "SHOW CREATE LIBRARY `db1`.`lib1`"},
		{"SHOW PROCEDURE STATUS LIKE 'p%'", true, "SHOW PROCEDURE STATUS LIKE _UTF8MB4'p%'"},
		{"SHOW FUNCTION STATUS WHERE Db='test'", true, "SHOW FUNCTION STATUS WHERE `Db`=_UTF8MB4'test'"},
		{"SHOW LIBRARY STATUS", true, "SHOW LIBRARY STATUS"},
		{"SHOW PROCEDURE CODE p1", true, "SHOW PROCEDURE CODE `p1`"},
		{"SHOW FUNCTION CODE db1.f1", true, "SHOW FUNCTION CODE `db1`.`f1`"},
		{"SHOW PARSE_TREE SELECT 1", true, "SHOW PARSE_TREE SELECT 1"},
		{"SHOW CREATE PROCEDURE", false, ""},
	}
	RunTest(t, table, false)
}

func TestShowBinlogAndReplicaStatements(t *testing.T) {
	table := []testCase{
		{"SHOW BINARY LOGS", true, "SHOW BINARY LOGS"},
		{"SHOW MASTER LOGS", true, "SHOW BINARY LOGS"},
		{"SHOW BINLOG EVENTS", true, "SHOW BINLOG EVENTS"},
		{"SHOW BINLOG EVENTS IN 'master-bin.000001'", true, "SHOW BINLOG EVENTS IN 'master-bin.000001'"},
		{"SHOW BINLOG EVENTS IN 'x' FROM 4 LIMIT 2,1", true, "SHOW BINLOG EVENTS IN 'x' FROM 4 LIMIT 2,1"},
		{"SHOW REPLICAS", true, "SHOW REPLICAS"},
		{"SHOW SLAVE HOSTS", true, "SHOW REPLICAS"},
		{"SHOW REPLICA STATUS FOR CHANNEL 'ch'", true, "SHOW REPLICA STATUS FOR CHANNEL 'ch'"},
		{"SHOW SLAVE STATUS", true, "SHOW REPLICA STATUS"},
	}
	RunTest(t, table, false)
}

func TestShowFilterVariants(t *testing.T) {
	table := []testCase{
		{"SHOW WARNINGS LIMIT 1", true, "SHOW WARNINGS LIMIT 1"},
		{"SHOW WARNINGS LIMIT 2, 1", true, "SHOW WARNINGS LIMIT 2,1"},
		{"SHOW ERRORS LIMIT 0, 10", true, "SHOW ERRORS LIMIT 0,10"},
		{"SHOW COUNT(*) WARNINGS", true, "SHOW COUNT(*) WARNINGS"},
		{"SHOW COUNT(*) ERRORS", true, "SHOW COUNT(*) ERRORS"},
		{"SHOW LOCAL VARIABLES LIKE 'sql_mode'", true, "SHOW SESSION VARIABLES LIKE _UTF8MB4'sql_mode'"},
		{"SHOW STORAGE ENGINES", true, "SHOW ENGINES"},
		// MySQL does not allow LIKE/WHERE on SHOW WARNINGS.
		{"SHOW WARNINGS LIKE 'x'", false, ""},
		// New keywords must keep working as identifiers.
		{"SELECT tree, code, replicas, parse_tree FROM t", true, "SELECT `tree`,`code`,`replicas`,`parse_tree` FROM `t`"},
	}
	RunTest(t, table, false)
}

func TestSelectIntoAndLocking(t *testing.T) {
	table := []testCase{
		// Trailing INTO user variables.
		{"SELECT 1 INTO @a", true, "SELECT 1 INTO @`a`"},
		{"SELECT 1, 2 INTO @a, @b", true, "SELECT 1,2 INTO @`a`, @`b`"},
		{"SELECT a FROM t INTO @v", true, "SELECT `a` FROM `t` INTO @`v`"},
		{"SELECT a FROM t WHERE b = 1 INTO @v", true, "SELECT `a` FROM `t` WHERE `b`=1 INTO @`v`"},
		{"SELECT a FROM t ORDER BY a LIMIT 1 INTO @v", true, "SELECT `a` FROM `t` ORDER BY `a` LIMIT 1 INTO @`v`"},
		{"SELECT 1 WHERE 1 INTO @a", true, "SELECT 1 FROM DUAL WHERE 1 INTO @`a`"},
		{"SELECT 1 LIMIT 1 INTO @a", true, "SELECT 1 LIMIT 1 INTO @`a`"},
		{"SELECT 1 UNION SELECT 2 INTO @a", true, "SELECT 1 UNION SELECT 2 INTO @`a`"},
		// INTO directly after the select item list (before FROM).
		{"SELECT a INTO @v FROM t", true, "SELECT `a` FROM `t` INTO @`v`"},
		{"SELECT a, b INTO @x, @y FROM t WHERE c = 1", true, "SELECT `a`,`b` FROM `t` WHERE `c`=1 INTO @`x`, @`y`"},
		{"SELECT 1 INTO @a FROM DUAL", true, "SELECT 1 INTO @`a`"},
		{"SELECT * INTO OUTFILE '/tmp/x' FIELDS TERMINATED BY ',' FROM t", true, "SELECT * FROM `t` INTO OUTFILE '/tmp/x' FIELDS TERMINATED BY ','"},
		// INTO DUMPFILE, and DUMPFILE stays usable as an identifier.
		{"SELECT * FROM t INTO DUMPFILE '/tmp/x'", true, "SELECT * FROM `t` INTO DUMPFILE '/tmp/x'"},
		{"SELECT dumpfile FROM t", true, "SELECT `dumpfile` FROM `t`"},
		// INTO combined with locking clauses, both orders.
		{"SELECT a FROM t INTO @v FOR UPDATE", true, "SELECT `a` FROM `t` FOR UPDATE INTO @`v`"},
		{"SELECT a FROM t FOR UPDATE INTO @v", true, "SELECT `a` FROM `t` FOR UPDATE INTO @`v`"},
		{"SELECT 1 INTO @a FOR UPDATE", true, "SELECT 1 FOR UPDATE INTO @`a`"},
		// Multiple locking clauses in one query block.
		{"SELECT a FROM t FOR UPDATE OF t FOR SHARE OF t", true, "SELECT `a` FROM `t` FOR UPDATE OF `t` FOR SHARE OF `t`"},
		{"SELECT a FROM t FOR UPDATE FOR SHARE NOWAIT", true, "SELECT `a` FROM `t` FOR UPDATE FOR SHARE NOWAIT"},
		{"SELECT 1 LOCK IN SHARE MODE FOR UPDATE", true, "SELECT 1 FOR SHARE FOR UPDATE"},
		// TABLE/VALUES statements take the same tail.
		{"TABLE t INTO @a", true, "TABLE `t` INTO @`a`"},
		{"VALUES ROW(1,2) INTO @a, @b", true, "VALUES ROW(1,2) INTO @`a`, @`b`"},
		// MySQL rejects more than one INTO clause per query block.
		{"SELECT a INTO @v FROM t INTO @w", false, ""},
		{"SELECT 1 INTO @a INTO @b", false, ""},
	}
	RunTest(t, table, false)
}

func TestJSONValueFunction(t *testing.T) {
	table := []testCase{
		{"SELECT JSON_VALUE(doc, '$.x') FROM t", true, "SELECT JSON_VALUE(`doc`, _UTF8MB4'$.x') FROM `t`"},
		{"SELECT JSON_VALUE(doc, '$.x' RETURNING SIGNED) FROM t", true, "SELECT JSON_VALUE(`doc`, _UTF8MB4'$.x' RETURNING SIGNED) FROM `t`"},
		{"SELECT JSON_VALUE(doc, '$.x' RETURNING DECIMAL(6,4)) FROM t", true, "SELECT JSON_VALUE(`doc`, _UTF8MB4'$.x' RETURNING DECIMAL(6, 4)) FROM `t`"},
		{"SELECT JSON_VALUE(doc, '$.x' RETURNING CHAR(4) CHARSET ascii) FROM t", true, "SELECT JSON_VALUE(`doc`, _UTF8MB4'$.x' RETURNING CHAR(4) CHARSET ASCII) FROM `t`"},
		{"SELECT JSON_VALUE(doc, '$.x' RETURNING DATETIME NULL ON EMPTY) FROM t", true, "SELECT JSON_VALUE(`doc`, _UTF8MB4'$.x' RETURNING DATETIME NULL ON EMPTY) FROM `t`"},
		{"SELECT JSON_VALUE(doc, '$.x' DEFAULT 456 ON ERROR) FROM t", true, "SELECT JSON_VALUE(`doc`, _UTF8MB4'$.x' DEFAULT 456 ON ERROR) FROM `t`"},
		{"SELECT JSON_VALUE(doc, '$.x' NULL ON EMPTY ERROR ON ERROR) FROM t", true, "SELECT JSON_VALUE(`doc`, _UTF8MB4'$.x' NULL ON EMPTY ERROR ON ERROR) FROM `t`"},
		// MySQL requires ON EMPTY before ON ERROR.
		{"SELECT JSON_VALUE(doc, '$.x' ERROR ON ERROR NULL ON EMPTY) FROM t", false, ""},
		// JSON_VALUE stays usable as an identifier.
		{"SELECT json_value FROM t", true, "SELECT `json_value` FROM `t`"},
	}
	RunTest(t, table, false)
}

func TestExpressionGaps(t *testing.T) {
	table := []testCase{
		// WEIGHT_STRING debug form.
		{"SELECT WEIGHT_STRING(a, 1, 2, 0xC0) FROM t", true, "SELECT WEIGHT_STRING(`a`, 1, 2, x'c0') FROM `t`"},
		// MATCH as a simple expression (comparison operand) and without parens.
		{"SELECT * FROM t WHERE MATCH(a) AGAINST('q') > 0.5", true, "SELECT * FROM `t` WHERE MATCH (`a`) AGAINST (_UTF8MB4'q')>0.5"},
		{"SELECT MATCH a AGAINST('q') FROM t", true, "SELECT MATCH (`a`) AGAINST (_UTF8MB4'q') FROM `t`"},
		{"SELECT MATCH a, b AGAINST('q' IN BOOLEAN MODE) FROM t", true, "SELECT MATCH (`a`,`b`) AGAINST (_UTF8MB4'q' IN BOOLEAN MODE) FROM `t`"},
		// Spatial cast targets, including the GEOMCOLLECTION synonym.
		{"SELECT CAST(x AS POINT) FROM t", true, "SELECT CAST(`x` AS POINT) FROM `t`"},
		{"SELECT CAST(x AS GEOMCOLLECTION) FROM t", true, "SELECT CAST(`x` AS GEOMETRYCOLLECTION) FROM `t`"},
		{"SELECT CONVERT(x, MULTIPOLYGON) FROM t", true, "SELECT CONVERT(`x`, MULTIPOLYGON) FROM `t`"},
		{"CREATE TABLE g (a GEOMCOLLECTION)", true, "CREATE TABLE `g` (`a` GEOMETRYCOLLECTION)"},
		// GEOMCOLLECTION also names the spatial constructor function.
		{"SELECT GEOMCOLLECTION(POINT(0, 0))", true, "SELECT GEOMCOLLECTION(POINT(0, 0))"},
		// New keywords stay usable as identifiers; EMPTY is reserved in MySQL 8.0.4+.
		{"SELECT returning, geomcollection FROM t", true, "SELECT `returning`,`geomcollection` FROM `t`"},
		{"CREATE TABLE empty (a INT)", false, ""},
	}
	RunTest(t, table, false)
}

func TestKillUserVariable(t *testing.T) {
	table := []testCase{
		{"KILL @id", true, "KILL @`id`"},
		{"KILL QUERY @thread_id", true, "KILL QUERY @`thread_id`"},
		{"KILL CONNECTION @id", true, "KILL @`id`"},
		{"KILL QUERY 42", true, "KILL QUERY 42"},
	}
	RunTest(t, table, false)
}

func TestHelpStatement(t *testing.T) {
	table := []testCase{
		{"HELP 'contents'", true, "HELP 'contents'"},
		{"help '%function_1'", true, "HELP '%function_1'"},
		{"HELP contents", true, "HELP 'contents'"},
	}
	RunTest(t, table, false)
}

func TestGetDiagnostics(t *testing.T) {
	table := []testCase{
		{"GET DIAGNOSTICS @var = NUMBER", true, "GET DIAGNOSTICS @`var` = NUMBER"},
		{"GET DIAGNOSTICS @var1 = NUMBER, @var2 = ROW_COUNT", true, "GET DIAGNOSTICS @`var1` = NUMBER, @`var2` = ROW_COUNT"},
		{"GET DIAGNOSTICS @v = row_count", true, "GET DIAGNOSTICS @`v` = ROW_COUNT"},
		{"GET CURRENT DIAGNOSTICS @v = ROW_COUNT", true, "GET CURRENT DIAGNOSTICS @`v` = ROW_COUNT"},
		{"GET STACKED DIAGNOSTICS CONDITION 1 @e = MYSQL_ERRNO, @m = MESSAGE_TEXT", true, "GET STACKED DIAGNOSTICS CONDITION 1 @`e` = MYSQL_ERRNO, @`m` = MESSAGE_TEXT"},
		{"GET DIAGNOSTICS CONDITION NULL @var = CLASS_ORIGIN", true, "GET DIAGNOSTICS CONDITION NULL @`var` = CLASS_ORIGIN"},
		{"GET DIAGNOSTICS CONDITION @c @var = RETURNED_SQLSTATE", true, "GET DIAGNOSTICS CONDITION @`c` @`var` = RETURNED_SQLSTATE"},
		// MySQL keeps statement and condition information items disjoint.
		{"GET DIAGNOSTICS @v = MESSAGE_TEXT", false, ""},
		{"GET DIAGNOSTICS CONDITION 1 @v = NUMBER", false, ""},
	}
	RunTest(t, table, false)
}

func TestSetValueKeywords(t *testing.T) {
	table := []testCase{
		{"SET GLOBAL delay_key_write = ALL", true, "SET @@GLOBAL.`delay_key_write`=_UTF8MB4'ALL'"},
		{"SET @@SESSION.binlog_format = ROW", true, "SET @@SESSION.`binlog_format`=_UTF8MB4'ROW'"},
		{"SET GLOBAL log_timestamps = SYSTEM", true, "SET @@GLOBAL.`log_timestamps`=_UTF8MB4'SYSTEM'"},
		{"SET PERSIST innodb_monitor_enable = all", true, "SET @@PERSIST.`innodb_monitor_enable`=_UTF8MB4'ALL'"},
	}
	RunTest(t, table, false)
}

func TestShowEngineAndExtended(t *testing.T) {
	table := []testCase{
		{"SHOW ENGINE csv STATUS", true, "SHOW ENGINE `csv` STATUS"},
		{"SHOW ENGINE csv LOGS", true, "SHOW ENGINE `csv` LOGS"},
		{"SHOW ENGINE MYISAM MUTEX", true, "SHOW ENGINE `MYISAM` MUTEX"},
		{"SHOW ENGINE InnoDB STATUS", true, "SHOW ENGINE `InnoDB` STATUS"},
		{"SHOW EXTENDED TABLES FROM test", true, "SHOW EXTENDED TABLES IN `test`"},
		{"SHOW EXTENDED FULL TABLES FROM test", true, "SHOW EXTENDED FULL TABLES IN `test`"},
		{"SHOW EXTENDED INDEX FROM t1", true, "SHOW EXTENDED INDEX IN `t1`"},
		{"SHOW EXTENDED FULL COLUMNS FROM t1", true, "SHOW EXTENDED FULL COLUMNS IN `t1`"},
	}
	RunTest(t, table, false)
}

func TestExplainForSchema(t *testing.T) {
	table := []testCase{
		{"EXPLAIN FORMAT=JSON INTO @x FOR SCHEMA s1 SELECT 1", true, "EXPLAIN FORMAT = 'JSON' INTO @x FOR SCHEMA `s1` SELECT 1"},
		{"EXPLAIN FORMAT=TREE FOR SCHEMA s1 SELECT * FROM t1", true, "EXPLAIN FORMAT = 'TREE' FOR SCHEMA `s1` SELECT * FROM `t1`"},
		{"EXPLAIN FORMAT=JSON FOR DATABASE s1 SELECT 1", true, "EXPLAIN FORMAT = 'JSON' FOR SCHEMA `s1` SELECT 1"},
		{"EXPLAIN FOR SCHEMA s1 SELECT 1", true, "EXPLAIN FORMAT = 'row' FOR SCHEMA `s1` SELECT 1"},
		{"EXPLAIN FORMAT=JSON INTO @x FOR SCHEMA s1 UPDATE t1 SET c1 = 1", true, "EXPLAIN FORMAT = 'JSON' INTO @x FOR SCHEMA `s1` UPDATE `t1` SET `c1`=1"},
	}
	RunTest(t, table, false)
}

func TestCreateViewIfNotExists(t *testing.T) {
	table := []testCase{
		{"CREATE VIEW IF NOT EXISTS v1 AS SELECT 1", true, "CREATE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW IF NOT EXISTS `v1` AS SELECT 1"},
		{"CREATE VIEW IF NOT EXISTS v1 (v1_a) AS SELECT a FROM t1", true, "CREATE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW IF NOT EXISTS `v1` (`v1_a`) AS SELECT `a` FROM `t1`"},
	}
	RunTest(t, table, false)
}

func TestKeywordRoleNames(t *testing.T) {
	table := []testCase{
		{"CREATE ROLE skip", true, "CREATE ROLE `skip`@`%`"},
		{"CREATE ROLE skip, locked, nowait", true, "CREATE ROLE `skip`@`%`, `locked`@`%`, `nowait`@`%`"},
		{"CREATE ROLE binlog", true, "CREATE ROLE `binlog`@`%`"},
		{"DROP ROLE role", true, "DROP ROLE `role`@`%`"},
		{"DROP ROLE skip, locked, nowait", true, "DROP ROLE `skip`@`%`, `locked`@`%`, `nowait`@`%`"},
	}
	RunTest(t, table, false)
}

func TestLoadXML(t *testing.T) {
	table := []testCase{
		{"LOAD XML INFILE 'x.dat' INTO TABLE t1", true, "LOAD XML INFILE 'x.dat' INTO TABLE `t1`"},
		{"LOAD XML INFILE 'x.dat' INTO TABLE t1 ROWS IDENTIFIED BY '<row>'", true, "LOAD XML INFILE 'x.dat' INTO TABLE `t1` ROWS IDENTIFIED BY '<row>'"},
		{"LOAD XML LOCAL INFILE 'x.dat' INTO TABLE t1 ROWS IDENTIFIED BY '<row>' IGNORE 4 ROWS", true, "LOAD XML LOCAL INFILE 'x.dat' IGNORE INTO TABLE `t1` ROWS IDENTIFIED BY '<row>' IGNORE 4 LINES"},
		{"LOAD XML INFILE 'x.dat' INTO TABLE t1 ROWS IDENTIFIED BY '<row>' (a, @b) SET b = concat('!', @b)", true, "LOAD XML INFILE 'x.dat' INTO TABLE `t1` ROWS IDENTIFIED BY '<row>' (`a`,@`b`) SET `b`=CONCAT(_UTF8MB4'!', @`b`)"},
		{"LOAD DATA INFILE 'x.dat' INTO TABLE t1 IGNORE 2 ROWS", true, "LOAD DATA INFILE 'x.dat' INTO TABLE `t1` IGNORE 2 LINES"},
		// LOAD XML takes no FIELDS/LINES clauses.
		{"LOAD XML INFILE 'x.dat' INTO TABLE t1 FIELDS TERMINATED BY ','", false, ""},
	}
	RunTest(t, table, false)
}

func TestDoStatementAlias(t *testing.T) {
	table := []testCase{
		{"DO 1", true, "DO 1"},
		// MySQL accepts (and discards) aliases on DO expressions.
		{"DO 1 AS x", true, "DO 1"},
		{"DO 1 x", true, "DO 1"},
		{"DO 1 AS 'xyz'", true, "DO 1"},
		{"DO 1 + 2, SLEEP(0) AS s", true, "DO 1+2, SLEEP(0)"},
	}
	RunTest(t, table, false)
}

func TestLikeEscapeExpression(t *testing.T) {
	table := []testCase{
		// Single-character string literals keep the legacy byte representation.
		{"SELECT 'a' LIKE 'b' ESCAPE '|'", true, "SELECT _UTF8MB4'a' LIKE _UTF8MB4'b' ESCAPE '|'"},
		{"SELECT 'a' LIKE 'b' ESCAPE ''", true, "SELECT _UTF8MB4'a' LIKE _UTF8MB4'b' ESCAPE ''"},
		// Everything else is kept as an expression; MySQL validates the
		// one-character requirement at execution time.
		{"SELECT a LIKE b ESCAPE 0x5C FROM t", true, "SELECT `a` LIKE `b` ESCAPE x'5c' FROM `t`"},
		{"SELECT 'x' LIKE 'y' ESCAPE 'ñ'", true, "SELECT _UTF8MB4'x' LIKE _UTF8MB4'y' ESCAPE _UTF8MB4'ñ'"},
		{"SELECT a LIKE b ESCAPE EXPORT_SET(1,'a','b') FROM t", true, "SELECT `a` LIKE `b` ESCAPE EXPORT_SET(1, _UTF8MB4'a', _UTF8MB4'b') FROM `t`"},
		{"SELECT a LIKE b ESCAPE (SELECT c FROM u LIMIT 1) FROM t", true, "SELECT `a` LIKE `b` ESCAPE (SELECT `c` FROM `u` LIMIT 1) FROM `t`"},
		{"SELECT 'a' NOT LIKE 'b' ESCAPE @e", true, "SELECT _UTF8MB4'a' NOT LIKE _UTF8MB4'b' ESCAPE @`e`"},
	}
	RunTest(t, table, false)
}

func TestSelectNoFromHaving(t *testing.T) {
	table := []testCase{
		{"SELECT 1 HAVING 1", true, "SELECT 1 HAVING 1"},
		{"SELECT 1 WHERE 1 HAVING 1", true, "SELECT 1 FROM DUAL WHERE 1 HAVING 1"},
		{"SELECT 1 WHERE 1 GROUP BY 1 HAVING 1", true, "SELECT 1 FROM DUAL WHERE 1 GROUP BY 1 HAVING 1"},
		{"SELECT 1 GROUP BY 1 HAVING 1 ORDER BY 1 LIMIT 1", true, "SELECT 1 GROUP BY 1 HAVING 1 ORDER BY 1 LIMIT 1"},
		{"SELECT 1 FROM DUAL HAVING 1", true, "SELECT 1 HAVING 1"},
		{"SELECT 1 FROM DUAL WHERE 1 GROUP BY 1 HAVING 1 ORDER BY 1 FOR UPDATE", true, "SELECT 1 FROM DUAL WHERE 1 GROUP BY 1 HAVING 1 ORDER BY 1 FOR UPDATE"},
	}
	RunTest(t, table, false)
}

func TestQualifyClause(t *testing.T) {
	table := []testCase{
		{"SELECT c FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) = 1", true, "SELECT `c` FROM `t` QUALIFY ROW_NUMBER() OVER (PARTITION BY `p` ORDER BY `o`)=1"},
		{"SELECT c, ROW_NUMBER() OVER w AS rn FROM t WINDOW w AS (ORDER BY c) QUALIFY rn = 1", true, "SELECT `c`,ROW_NUMBER() OVER `w` AS `rn` FROM `t` WINDOW `w` AS (ORDER BY `c`) QUALIFY `rn`=1"},
		{"SELECT c FROM t WHERE a = 1 GROUP BY c HAVING COUNT(*) > 1 QUALIFY RANK() OVER (ORDER BY c) < 3 ORDER BY c LIMIT 5", true, "SELECT `c` FROM `t` WHERE `a`=1 GROUP BY `c` HAVING COUNT(1)>1 QUALIFY RANK() OVER (ORDER BY `c`)<3 ORDER BY `c` LIMIT 5"},
		{"SELECT * FROM t QUALIFY ROW_NUMBER() OVER () = 1 INTO OUTFILE '/tmp/x'", true, "SELECT * FROM `t` QUALIFY ROW_NUMBER() OVER ()=1 INTO OUTFILE '/tmp/x'"},
		// QUALIFY is a reserved word in MySQL 9.7.
		{"SELECT qualify FROM t", false, ""},
	}
	RunTest(t, table, true)
}

func TestGroupingSets(t *testing.T) {
	table := []testCase{
		{"SELECT a, b, SUM(c) FROM t GROUP BY GROUPING SETS ((a), (b), (a, b), ())", true, "SELECT `a`,`b`,SUM(`c`) FROM `t` GROUP BY GROUPING SETS ((`a`),(`b`),(`a`,`b`),())"},
		{"SELECT a FROM t GROUP BY GROUPING SETS ((a))", true, "SELECT `a` FROM `t` GROUP BY GROUPING SETS ((`a`))"},
		// GROUPING remains callable as a function and usable as an identifier.
		{"SELECT GROUPING(a) FROM t GROUP BY a WITH ROLLUP", true, "SELECT GROUPING(`a`) FROM `t` GROUP BY `a` WITH ROLLUP"},
		{"SELECT grouping FROM t", true, "SELECT `grouping` FROM `t`"},
		{"SELECT sets FROM t", true, "SELECT `sets` FROM `t`"},
	}
	RunTest(t, table, false)
}

func TestStCollectAggregate(t *testing.T) {
	table := []testCase{
		{"SELECT ST_COLLECT(g) FROM t", true, "SELECT ST_COLLECT(`g`) FROM `t`"},
		{"SELECT ST_COLLECT(DISTINCT g) FROM t", true, "SELECT ST_COLLECT(DISTINCT `g`) FROM `t`"},
		{"SELECT ST_COLLECT(g) OVER (PARTITION BY p) FROM t", true, "SELECT ST_COLLECT(`g`) OVER (PARTITION BY `p`) FROM `t`"},
		{"SELECT st_collect FROM t", true, "SELECT `st_collect` FROM `t`"},
	}
	RunTest(t, table, true)
}

func TestCastAtTimeZone(t *testing.T) {
	table := []testCase{
		{"SELECT CAST(NULL AT TIME ZONE 'UTC' AS DATETIME)", true, "SELECT CAST(NULL AT TIME ZONE 'UTC' AS DATETIME)"},
		{"SELECT CAST(TIMESTAMP'2019-10-10 10:11:12' AT TIME ZONE '+00:00' AS DATETIME)", true, "SELECT CAST(TIMESTAMP '2019-10-10 10:11:12' AT TIME ZONE '+00:00' AS DATETIME)"},
		{"SELECT CAST(a AT TIME ZONE '+00:00' AS DATETIME) FROM t1", true, "SELECT CAST(`a` AT TIME ZONE '+00:00' AS DATETIME) FROM `t1`"},
		// AT and ZONE remain usable as identifiers.
		{"SELECT at FROM t", true, "SELECT `at` FROM `t`"},
		{"SELECT zone FROM t", true, "SELECT `zone` FROM `t`"},
	}
	RunTest(t, table, false)
}

func TestJSONValueTemporalDefault(t *testing.T) {
	table := []testCase{
		{"SELECT JSON_VALUE(j, '$.a' RETURNING DATE DEFAULT DATE'2020-01-01' ON ERROR) FROM t", true, "SELECT JSON_VALUE(`j`, _UTF8MB4'$.a' RETURNING DATE DEFAULT DATE '2020-01-01' ON ERROR) FROM `t`"},
		{"SELECT JSON_VALUE(j, '$.a' RETURNING TIME DEFAULT TIME'10:00:00' ON EMPTY) FROM t", true, "SELECT JSON_VALUE(`j`, _UTF8MB4'$.a' RETURNING TIME DEFAULT TIME '10:00:00' ON EMPTY) FROM `t`"},
		{"SELECT JSON_VALUE(j, '$.a' RETURNING DATETIME DEFAULT TIMESTAMP'2020-01-01 10:00:00' ON ERROR) FROM t", true, "SELECT JSON_VALUE(`j`, _UTF8MB4'$.a' RETURNING DATETIME DEFAULT TIMESTAMP '2020-01-01 10:00:00' ON ERROR) FROM `t`"},
	}
	RunTest(t, table, false)
}

func TestSoundsLike(t *testing.T) {
	// MySQL defines `a SOUNDS LIKE b` as `SOUNDEX(a) = SOUNDEX(b)`; the parser
	// desugars it the same way.
	table := []testCase{
		{"SELECT a SOUNDS LIKE b FROM t", true, "SELECT SOUNDEX(`a`)=SOUNDEX(`b`) FROM `t`"},
		{"SELECT 'x' SOUNDS LIKE 'y'", true, "SELECT SOUNDEX(_UTF8MB4'x')=SOUNDEX(_UTF8MB4'y')"},
	}
	RunTest(t, table, false)
}

func TestJSONTable(t *testing.T) {
	table := []testCase{
		{"SELECT attrs.* FROM t_json, JSON_TABLE(json_col, '$[*]' COLUMNS (nickname JSON PATH '$.nickname')) as attrs", true, "SELECT `attrs`.* FROM (`t_json`) JOIN JSON_TABLE(`json_col`, '$[*]' COLUMNS (`nickname` JSON PATH '$.nickname')) AS `attrs`"},
		{"SELECT * FROM json_table('[]', '$[*]' COLUMNS (p CHAR(1) CHARACTER SET utf8mb3 PATH '$.a')) AS t", true, "SELECT * FROM JSON_TABLE(_UTF8MB4'[]', '$[*]' COLUMNS (`p` CHAR(1) CHARACTER SET UTF8 PATH '$.a')) AS `t`"},
		{"SELECT description FROM JSON_TABLE(plan, '$**.operation' COLUMNS (o FOR ORDINALITY, description TEXT PATH '$')) AS jt", true, "SELECT `description` FROM JSON_TABLE(`plan`, '$**.operation' COLUMNS (`o` FOR ORDINALITY, `description` TEXT PATH '$')) AS `jt`"},
		{"SELECT * FROM JSON_TABLE('[1]', '$[*]' COLUMNS (i INT PATH '$[0]' NULL ON EMPTY DEFAULT '9' ON ERROR)) AS t3", true, "SELECT * FROM JSON_TABLE(_UTF8MB4'[1]', '$[*]' COLUMNS (`i` INT PATH '$[0]' NULL ON EMPTY DEFAULT _UTF8MB4'9' ON ERROR)) AS `t3`"},
		{"SELECT * FROM JSON_TABLE('[1]', '$[*]' COLUMNS (i INT EXISTS PATH '$[0]')) AS t4", true, "SELECT * FROM JSON_TABLE(_UTF8MB4'[1]', '$[*]' COLUMNS (`i` INT EXISTS PATH '$[0]')) AS `t4`"},
		// NESTED with and without the optional PATH keyword; both restore with it.
		{"SELECT * FROM JSON_TABLE('[]', '$[*]' COLUMNS (a INT PATH '$.a', NESTED PATH '$.b[*]' COLUMNS (b INT PATH '$'), NESTED '$.c[*]' COLUMNS (c TEXT PATH '$'))) tt", true, "SELECT * FROM JSON_TABLE(_UTF8MB4'[]', '$[*]' COLUMNS (`a` INT PATH '$.a', NESTED PATH '$.b[*]' COLUMNS (`b` INT PATH '$'), NESTED PATH '$.c[*]' COLUMNS (`c` TEXT PATH '$'))) AS `tt`"},
		{"SELECT id FROM JSON_TABLE(IF(x<>NOW(), '[{\"a\":1}]', '[]'), '$[*]' COLUMNS (id INT PATH '$.a')) AS jt", true, "SELECT `id` FROM JSON_TABLE(IF(`x`!=NOW(), _UTF8MB4'[{\"a\":1}]', _UTF8MB4'[]'), '$[*]' COLUMNS (`id` INT PATH '$.a')) AS `jt`"},
		// PATH, NESTED and ORDINALITY remain usable as identifiers, and
		// JSON_TABLE is only a keyword when followed by a parenthesis.
		{"SELECT path, nested, ordinality FROM t", true, "SELECT `path`,`nested`,`ordinality` FROM `t`"},
		// JSON_TABLE is a reserved word, so it cannot name a column.
		{"SELECT * FROM t WHERE json_table = 5", false, ""},
		// JSON_TABLE is not a scalar function.
		{"SELECT JSON_TABLE('[]', '$' COLUMNS (i INT PATH '$'))", false, ""},
	}
	RunTest(t, table, false)
}

func TestAlterTablePartitionMaintenance(t *testing.T) {
	table := []testCase{
		// ANALYZE PARTITION is a regular alter spec, so it can carry
		// NO_WRITE_TO_BINLOG/ALL and combine with other specs by comma.
		{"ALTER TABLE t1 ANALYZE PARTITION p0", true, "ALTER TABLE `t1` ANALYZE PARTITION `p0`"},
		{"ALTER TABLE t1 ANALYZE PARTITION NO_WRITE_TO_BINLOG p0, p1", true, "ALTER TABLE `t1` ANALYZE PARTITION NO_WRITE_TO_BINLOG `p0`,`p1`"},
		{"ALTER TABLE t1 ANALYZE PARTITION ALL", true, "ALTER TABLE `t1` ANALYZE PARTITION ALL"},
		{"ALTER TABLE t1 ANALYZE PARTITION ALL, CHECK PARTITION ALL", true, "ALTER TABLE `t1` ANALYZE PARTITION ALL, CHECK PARTITION ALL"},
		{"ALTER TABLE t1 ANALYZE PARTITION", false, ""},
		// REORGANIZE PARTITION combines with modifier specs by comma.
		{"ALTER TABLE t1 ALGORITHM=INPLACE, REORGANIZE PARTITION p0 INTO (PARTITION p1 VALUES LESS THAN (10))", true, "ALTER TABLE `t1` ALGORITHM = INPLACE, REORGANIZE PARTITION `p0` INTO (PARTITION `p1` VALUES LESS THAN (10))"},
		{"ALTER TABLE t1 REORGANIZE PARTITION p0,p1 INTO (PARTITION p2 VALUES LESS THAN MAXVALUE)", true, "ALTER TABLE `t1` REORGANIZE PARTITION `p0`,`p1` INTO (PARTITION `p2` VALUES LESS THAN (MAXVALUE))"},
		{"ALTER TABLE t1 REORGANIZE PARTITION", true, "ALTER TABLE `t1` REORGANIZE PARTITION"},
		// SECONDARY_LOAD/UNLOAD accept a partition list in MySQL 9.x.
		{"ALTER TABLE t1 SECONDARY_LOAD", true, "ALTER TABLE `t1` SECONDARY_LOAD"},
		{"ALTER TABLE t1 SECONDARY_LOAD PARTITION (p0)", true, "ALTER TABLE `t1` SECONDARY_LOAD PARTITION (`p0`)"},
		{"ALTER TABLE t1 SECONDARY_UNLOAD PARTITION (p0, p1)", true, "ALTER TABLE `t1` SECONDARY_UNLOAD PARTITION (`p0`,`p1`)"},
		// A trailing repartition clause still parses after SECONDARY_LOAD's
		// PARTITION-vs-PARTITION BY shift preference.
		{"ALTER TABLE t1 ADD COLUMN b INT PARTITION BY HASH (a) PARTITIONS 4", true, "ALTER TABLE `t1` ADD COLUMN `b` INT PARTITION BY HASH (`a`) PARTITIONS 4"},
	}
	RunTest(t, table, false)
}

func TestCreateTableParenQuery(t *testing.T) {
	table := []testCase{
		// CREATE TABLE t (query) with no AS keyword and no column list.
		{"CREATE TABLE t4 (SELECT 1 AS x)", true, "CREATE TABLE `t4` AS (SELECT 1 AS `x`)"},
		{"CREATE TABLE IF NOT EXISTS t4 (SELECT 1)", true, "CREATE TABLE IF NOT EXISTS `t4` AS (SELECT 1)"},
		{"CREATE TEMPORARY TABLE t4 (SELECT 1)", true, "CREATE TEMPORARY TABLE `t4` AS (SELECT 1)"},
		{"CREATE TABLE t4 (SELECT * FROM t1) ORDER BY 1", true, "CREATE TABLE `t4` AS (SELECT * FROM `t1`) ORDER BY 1"},
		{"CREATE TABLE t4 (SELECT 1) LIMIT 5", true, "CREATE TABLE `t4` AS (SELECT 1) LIMIT 5"},
		{"CREATE TABLE t4 (SELECT 1 UNION SELECT 2) ORDER BY 1 LIMIT 1", true, "CREATE TABLE `t4` AS (SELECT 1 UNION SELECT 2) ORDER BY 1 LIMIT 1"},
		{"create table t4 (select a,b from t1) union (select a,b from t2) limit 2", true, "CREATE TABLE `t4` AS (SELECT `a`,`b` FROM `t1`) UNION (SELECT `a`,`b` FROM `t2`) LIMIT 2"},
		{"CREATE TABLE t5 (SELECT 1) UNION SELECT 2", true, "CREATE TABLE `t5` AS (SELECT 1) UNION SELECT 2"},
		{"CREATE TABLE t5 (SELECT 1) UNION (SELECT 2) ORDER BY 1 LIMIT 2", true, "CREATE TABLE `t5` AS (SELECT 1) UNION (SELECT 2) ORDER BY 1 LIMIT 2"},
		// The unparenthesized query forms keep flowing through
		// CreateTableSelectOpt (TABLE/VALUES operands included).
		{"CREATE TABLE t6 TABLE t1", true, "CREATE TABLE `t6` AS TABLE `t1`"},
		{"CREATE TABLE t6 VALUES ROW(1,2)", true, "CREATE TABLE `t6` AS VALUES ROW(1,2)"},
		{"CREATE TABLE t6 SELECT 1 UNION SELECT 2", true, "CREATE TABLE `t6` AS SELECT 1 UNION SELECT 2"},
		{"CREATE TABLE t4 (LIKE t1)", true, "CREATE TABLE `t4` LIKE `t1`"},
	}
	RunTest(t, table, false)
}

func TestLoadDataExtensions(t *testing.T) {
	table := []testCase{
		// CONCURRENT priority modifier; FROM is optional noise MySQL accepts
		// before INFILE and drops from the canonical form.
		{"LOAD DATA CONCURRENT INFILE '/tmp/x' INTO TABLE t1", true, "LOAD DATA CONCURRENT INFILE '/tmp/x' INTO TABLE `t1`"},
		{"LOAD DATA LOW_PRIORITY FROM INFILE '/tmp/x' INTO TABLE t1", true, "LOAD DATA LOW_PRIORITY INFILE '/tmp/x' INTO TABLE `t1`"},
		{"LOAD DATA CONCURRENT LOCAL INFILE '/tmp/x' IGNORE INTO TABLE t1", true, "LOAD DATA CONCURRENT LOCAL INFILE '/tmp/x' IGNORE INTO TABLE `t1`"},
		{"LOAD DATA CONCURRENT LOW_PRIORITY INFILE '/tmp/x' INTO TABLE t1", false, ""},
		// NDB's IN PRIMARY KEY ORDER hint and target partition list.
		{"LOAD DATA FROM INFILE '/tmp/x' IN PRIMARY KEY ORDER INTO TABLE t1", true, "LOAD DATA INFILE '/tmp/x' IN PRIMARY KEY ORDER INTO TABLE `t1`"},
		{"LOAD DATA INFILE '/tmp/x' INTO TABLE t1 PARTITION (p0, p1)", true, "LOAD DATA INFILE '/tmp/x' INTO TABLE `t1` PARTITION(`p0`, `p1`)"},
		// SET accepts := and the full modifier stack together.
		{"LOAD DATA INFILE '/tmp/x' REPLACE INTO TABLE t1 PARTITION (p0) CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' IGNORE 1 LINES (a, @v) SET b := @v + 1", true, "LOAD DATA INFILE '/tmp/x' REPLACE INTO TABLE `t1` PARTITION(`p0`) CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' IGNORE 1 LINES (`a`,@`v`) SET `b`=@`v`+1"},
		// CONCURRENT stays usable as an identifier.
		{"SELECT concurrent FROM t1", true, "SELECT `concurrent` FROM `t1`"},
	}
	RunTest(t, table, false)
}

func TestTableOptionValueForms(t *testing.T) {
	table := []testCase{
		// AUTOEXTEND_SIZE takes a plain byte count as well as '4M'-style
		// strings ("4M" unquoted lexes as an identifier).
		{"CREATE TABLE t2 (a INT) AUTOEXTEND_SIZE = 4194304", true, "CREATE TABLE `t2` (`a` INT) AUTOEXTEND_SIZE = 4194304"},
		{"CREATE TABLE t2 (a INT) AUTOEXTEND_SIZE = 4M", true, "CREATE TABLE `t2` (`a` INT) AUTOEXTEND_SIZE = 4M"},
		// ENCRYPTION values are validated at execution time, not parse time.
		{"CREATE TABLE t2 (a INT) ENCRYPTION 'foo'", true, "CREATE TABLE `t2` (`a` INT) ENCRYPTION = 'foo'"},
		{"CREATE TABLE t2 (a INT) ENCRYPTION = 'N'", true, "CREATE TABLE `t2` (`a` INT) ENCRYPTION = 'N'"},
		// FLOAT/DOUBLE accept (and ignore) a decimal precision.
		{"CREATE TABLE t3 (f FLOAT(10.3))", true, "CREATE TABLE `t3` (`f` FLOAT)"},
		{"CREATE TABLE t3 (d DOUBLE(10.3))", true, "CREATE TABLE `t3` (`d` DOUBLE)"},
		{"CREATE TABLE t3 (f FLOAT(10,3))", true, "CREATE TABLE `t3` (`f` FLOAT(10,3))"},
	}
	RunTest(t, table, false)
}

func TestSelectAliasCurrentRole(t *testing.T) {
	table := []testCase{
		// MySQL allows CURRENT_ROLE as a column alias even though it is also
		// a braces-optional function name.
		{"SELECT 1 AS CURRENT_ROLE", true, "SELECT 1 AS `CURRENT_ROLE`"},
		{"SELECT 1 CURRENT_ROLE", true, "SELECT 1 AS `CURRENT_ROLE`"},
		{"SELECT CURRENT_ROLE", true, "SELECT CURRENT_ROLE()"},
	}
	RunTest(t, table, false)
}

// TestGroupByRollupFunc covers GROUP BY ROLLUP(...), the function-style
// spelling of WITH ROLLUP added in MySQL 8.0.1, which restores to the
// WITH ROLLUP form. A generic function named rollup stays a syntax error,
// matching MySQL (1064).
func TestGroupByRollupFunc(t *testing.T) {
	table := []testCase{
		{"SELECT a, SUM(b) FROM t GROUP BY ROLLUP(a)", true, "SELECT `a`,SUM(`b`) FROM `t` GROUP BY `a` WITH ROLLUP"},
		{"SELECT a FROM t GROUP BY ROLLUP(a, b, c) HAVING a > 1", true, "SELECT `a` FROM `t` GROUP BY `a`,`b`,`c` WITH ROLLUP HAVING `a`>1"},
		{"SELECT rollup(1)", false, ""},
		{"CREATE TABLE rollup (a INT)", true, "CREATE TABLE `rollup` (`a` INT)"},
	}
	RunTest(t, table, false)
}

// TestQualifyWithoutFrom covers QUALIFY directly after the field list
// (MySQL 9.7 allows it with no FROM clause).
func TestQualifyWithoutFrom(t *testing.T) {
	table := []testCase{
		{"SELECT 1 AS res QUALIFY ROW_NUMBER() OVER () > 10", true, "SELECT 1 AS `res` QUALIFY ROW_NUMBER() OVER ()>10"},
		{"SELECT (SELECT 1 QUALIFY ROW_NUMBER() OVER () < 10) AS res", true, "SELECT (SELECT 1 QUALIFY ROW_NUMBER() OVER ()<10) AS `res`"},
		{"SELECT 1 QUALIFY SUM(1) OVER () > 0 ORDER BY 1 LIMIT 2", true, "SELECT 1 QUALIFY SUM(1) OVER ()>0 ORDER BY 1 LIMIT 2"},
	}
	RunTest(t, table, true)
}

// TestUserVariableAssignExpr covers @var := val as a simple expression
// (MySQL sql_yacc.yy variable_aux), usable as any operand. The assigned
// value is a BitExpr: arithmetic binds greedily, while comparison and
// logical operators bind outside the assignment.
func TestUserVariableAssignExpr(t *testing.T) {
	table := []testCase{
		{"SELECT (@t2:=1)+@t3:=4, @t2, @t3", true, "SELECT (@`t2`:=1)+@`t3`:=4,@`t2`,@`t3`"},
		{"SELECT @t1:=(@t2:=1)+@t3:=4, @t1", true, "SELECT @`t1`:=(@`t2`:=1)+@`t3`:=4,@`t1`"},
		{"SELECT 1 = @v := 2 AND 0", true, "SELECT 1=@`v`:=2 AND 0"},
		{"SELECT @a := b COLLATE utf8mb4_bin FROM t", true, "SELECT @`a`:=`b` COLLATE utf8mb4_bin FROM `t`"},
		{"SELECT hex(@a:=1), hex(@a)", true, "SELECT HEX(@`a`:=1),HEX(@`a`)"},
	}
	RunTest(t, table, false)
}

// TestLeadLagUserVariableOffset covers user variables as LAG/LEAD offsets.
func TestLeadLagUserVariableOffset(t *testing.T) {
	table := []testCase{
		{"DO LAG(1, @v) OVER()", true, "DO LAG(1, @`v`) OVER ()"},
		{"DO LEAD(1, @n, 3) OVER()", true, "DO LEAD(1, @`n`, 3) OVER ()"},
	}
	RunTest(t, table, true)
}

// TestCastNChar covers CAST(... AS NCHAR/NATIONAL CHAR): the national
// character set is always utf8mb3, registered under its "utf8" alias.
func TestCastNChar(t *testing.T) {
	table := []testCase{
		{"SELECT CAST('abc' AS NCHAR(2))", true, "SELECT CAST(_UTF8MB4'abc' AS CHAR(2) CHARSET UTF8)"},
		{"SELECT CAST('abc' AS NATIONAL CHAR)", true, "SELECT CAST(_UTF8MB4'abc' AS CHAR CHARSET UTF8)"},
		{"SELECT CAST('abc' AS NCHAR)", true, "SELECT CAST(_UTF8MB4'abc' AS CHAR CHARSET UTF8)"},
	}
	RunTest(t, table, false)
}

// TestStructuredSystemVariable covers @@scope.instance.component system
// variables whose component after the dot is quoted separately; the fully
// unquoted spelling lexes as a single token and restores identically.
func TestStructuredSystemVariable(t *testing.T) {
	table := []testCase{
		{"SELECT @@global.`default`.`key_buffer_size`", true, "SELECT @@GLOBAL.`default.key_buffer_size`"},
		{"SELECT @@GLOBAL.default.key_buffer_size", true, "SELECT @@GLOBAL.`default.key_buffer_size`"},
	}
	RunTest(t, table, false)
}

// TestJSONArrayaggNullOnNull covers JSON_ARRAYAGG(expr NULL ON NULL), which
// spells out the default null handling (the AST is unchanged). JSON_OBJECTAGG
// does not accept the clause, matching MySQL (1064).
func TestJSONArrayaggNullOnNull(t *testing.T) {
	table := []testCase{
		{"SELECT JSON_ARRAYAGG(a NULL ON NULL) FROM t1", true, "SELECT JSON_ARRAYAGG(`a`) FROM `t1`"},
		{"SELECT JSON_OBJECTAGG('a', 1 NULL ON NULL)", false, ""},
	}
	RunTest(t, table, false)
	windowed := []testCase{
		{"SELECT JSON_ARRAYAGG(a NULL ON NULL) OVER () FROM t1", true, "SELECT JSON_ARRAYAGG(`a`) OVER () FROM `t1`"},
	}
	RunTest(t, windowed, true)
}

// TestVersionedComments covers /*!NNNNN ... */ comments gated on the MySQL
// version this parser mimics (90700): content at or below it is parsed,
// content above it is skipped, and a bare /*! is always parsed. mysqldump
// 9.x emits /*!999999 for sandbox mode.
func TestVersionedComments(t *testing.T) {
	table := []testCase{
		{"select 1 /*!999999 +1 */", true, "SELECT 1"},
		{"SELECT 1 /*!080100 +1*/ AS r", true, "SELECT 1+1 AS `r`"},
		{"select 1 + /*!00000 2 */ + 3 /*!99999 noise*/ + 4", true, "SELECT 1+2+3+4"},
		{"/*!99999 --- */INSERT /*!INTO*/ /*!10000 t1 */ VALUES(10) /*!99999 ,(11)*/", true, "INSERT INTO `t1` VALUES (10)"},
		// While discarding, one nested /* ... */ level is honored per level
		// (recursively, so siblings each nest), quoted strings are not
		// special, and deeper nesting or an unclosed comment is a syntax
		// error -- all matching MySQL 9.7 behavior exactly.
		{"SELECT 1 /*!99999 /* */ */", true, "SELECT 1"},
		{"SELECT 1 /*!99999 /* */ /* */ */", true, "SELECT 1"},
		{"SELECT 1 /*!99999 ' */", true, "SELECT 1"},
		{"SELECT 1 /*!99999 /* /* */ */ */", false, ""},
		{"SELECT 1 /*!99999 /* */", false, ""},
	}
	RunTest(t, table, false)
}

// TestHintOutsideHintSlot: a /*+ ... */ where MySQL accepts no hint is
// warned about and skipped like a comment instead of failing the parse.
func TestHintOutsideHintSlot(t *testing.T) {
	table := []testCase{
		{"CREATE /*+ x */ TABLE t (a INT)", true, "CREATE TABLE `t` (`a` INT)"},
	}
	RunTest(t, table, false)
}

// TestSelectIntoOutfileCharset covers INTO OUTFILE ... CHARACTER SET.
func TestSelectIntoOutfileCharset(t *testing.T) {
	table := []testCase{
		{"select * from t1 into outfile 'tmp1.txt' character set binary", true, "SELECT * FROM `t1` INTO OUTFILE 'tmp1.txt' CHARACTER SET BINARY"},
		{"SELECT '00' UNION SELECT '10' INTO OUTFILE 'tmpp2.txt' CHARACTER SET ucs2", true, "SELECT _UTF8MB4'00' UNION SELECT _UTF8MB4'10' INTO OUTFILE 'tmpp2.txt' CHARACTER SET UCS2"},
	}
	RunTest(t, table, false)
}

// TestJSONTableColumnCollate covers COLLATE on JSON_TABLE column types;
// MySQL validates the collation against the charset at execution time.
func TestJSONTableColumnCollate(t *testing.T) {
	table := []testCase{
		{"SELECT * FROM json_table('[]', '$[*]' COLUMNS (p CHAR(1) CHARACTER SET ucs2 COLLATE ucs2_persian_ci PATH '$.a')) AS t", true, "SELECT * FROM JSON_TABLE(_UTF8MB4'[]', '$[*]' COLUMNS (`p` CHAR(1) CHARACTER SET UCS2 COLLATE ucs2_persian_ci PATH '$.a')) AS `t`"},
		{"SELECT * FROM json_table('[]', '$[*]' COLUMNS (p CHAR(1) CHARACTER SET ucs2 COLLATE ucs2_persian_ci EXISTS PATH '$.a')) AS t", true, "SELECT * FROM JSON_TABLE(_UTF8MB4'[]', '$[*]' COLUMNS (`p` CHAR(1) CHARACTER SET UCS2 COLLATE ucs2_persian_ci EXISTS PATH '$.a')) AS `t`"},
	}
	RunTest(t, table, false)
}

// TestSetLocalTransaction covers the LOCAL synonym for SESSION.
func TestSetLocalTransaction(t *testing.T) {
	table := []testCase{
		{"SET LOCAL TRANSACTION ISOLATION LEVEL READ COMMITTED", true, "SET @@SESSION.`tx_isolation`=_UTF8MB4'READ-COMMITTED'"},
		{"SET LOCAL TRANSACTION READ ONLY", true, "SET @@SESSION.`tx_read_only`=_UTF8MB4'1'"},
	}
	RunTest(t, table, false)
}

// TestShowRelaylogEvents covers SHOW RELAYLOG EVENTS.
func TestShowRelaylogEvents(t *testing.T) {
	table := []testCase{
		{"SHOW RELAYLOG EVENTS", true, "SHOW RELAYLOG EVENTS"},
		{"SHOW RELAYLOG EVENTS IN 'relay-bin.000002' FROM 4 LIMIT 2", true, "SHOW RELAYLOG EVENTS IN 'relay-bin.000002' FROM 4 LIMIT 2"},
		{"SELECT relaylog FROM t", true, "SELECT `relaylog` FROM `t`"},
	}
	RunTest(t, table, false)
}

// TestHelpIdentifier covers unquoted HELP topics.
func TestHelpIdentifier(t *testing.T) {
	table := []testCase{
		{"HELP data_types", true, "HELP 'data_types'"},
		{"HELP contents", true, "HELP 'contents'"},
	}
	RunTest(t, table, false)
}

// TestResetBinaryLogsToHex covers RESET BINARY LOGS AND GTIDS TO 0xF, which
// restores using the decimal spelling of the index.
func TestResetBinaryLogsToHex(t *testing.T) {
	table := []testCase{
		{"RESET BINARY LOGS AND GTIDS TO 0xF", true, "RESET BINARY LOGS AND GTIDS TO 15"},
		{"RESET BINARY LOGS AND GTIDS TO 15", true, "RESET BINARY LOGS AND GTIDS TO 15"},
	}
	RunTest(t, table, false)
}

// TestCacheIndexPrimary covers PRIMARY as an index name in CACHE INDEX and
// LOAD INDEX INTO CACHE key lists.
func TestCacheIndexPrimary(t *testing.T) {
	table := []testCase{
		{"load index into cache t1, t2 key (primary,b) ignore leaves", true, "LOAD INDEX INTO CACHE `t1`, `t2` INDEX (`PRIMARY`, `b`) IGNORE LEAVES"},
		{"CACHE INDEX t1 KEY (PRIMARY) IN default", true, "CACHE INDEX `t1` INDEX (`PRIMARY`) IN `default`"},
	}
	RunTest(t, table, false)
}

// TestAlterUserFuncReplacePassword covers REPLACE 'current' on the
// ALTER USER USER() form.
func TestAlterUserFuncReplacePassword(t *testing.T) {
	table := []testCase{
		{"ALTER USER user() IDENTIFIED BY 'ahaha' REPLACE 'hehe'", true, "ALTER USER USER() IDENTIFIED BY 'ahaha' REPLACE 'hehe'"},
		{"ALTER USER IF EXISTS user() IDENTIFIED BY 'x' REPLACE 'y' RETAIN CURRENT PASSWORD", true, "ALTER USER IF EXISTS USER() IDENTIFIED BY 'x' REPLACE 'y' RETAIN CURRENT PASSWORD"},
	}
	RunTest(t, table, false)
}

// TestGrantQuotedDynamicPriv covers dynamic privileges written as quoted
// strings; the privilege position (an ON clause) resolves them, while plain
// GRANT ... TO keeps reading roles.
func TestGrantQuotedDynamicPriv(t *testing.T) {
	table := []testCase{
		{"GRANT 'SYSTEM_VARIABLES_ADMIN' ON *.* TO 'var_admin_user'@'%'", true, "GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO `var_admin_user`@`%`"},
		{"GRANT 'r1' TO 'u'@'%'", true, "GRANT `r1`@`%` TO `u`@`%`"},
	}
	RunTest(t, table, false)
}

// TestCreateLibraryBodyForms covers COMMENT placed after LANGUAGE and binary
// library bodies, which decode to their byte content.
func TestCreateLibraryBodyForms(t *testing.T) {
	table := []testCase{
		{"CREATE LIBRARY lib1 LANGUAGE JAVASCRIPT COMMENT 'hi' AS 'export function f(n) {return n}'", true, "CREATE LIBRARY `lib1` COMMENT 'hi' LANGUAGE JAVASCRIPT AS 'export function f(n) {return n}'"},
		{"CREATE LIBRARY lib2 LANGUAGE JAVASCRIPT AS 0x2F2A20636F6D6D656E74202A2F", true, "CREATE LIBRARY `lib2` LANGUAGE JAVASCRIPT AS '/* comment */'"},
		{"CREATE LIBRARY lib4 COMMENT 'a' LANGUAGE JAVASCRIPT COMMENT 'b' AS X'414243'", true, "CREATE LIBRARY `lib4` COMMENT 'b' LANGUAGE JAVASCRIPT AS 'ABC'"},
	}
	RunTest(t, table, false)
}

// TestCreateJSONDualityView covers CREATE JSON [RELATIONAL] DUALITY VIEW and
// the JSON_DUALITY_OBJECT constructor.
func TestCreateJSONDualityView(t *testing.T) {
	table := []testCase{
		{"CREATE OR REPLACE JSON RELATIONAL DUALITY VIEW dv AS SELECT JSON_DUALITY_OBJECT(WITH (INSERT, UPDATE, DELETE) '_id' : f1) FROM t", true, "CREATE OR REPLACE JSON RELATIONAL DUALITY VIEW `dv` AS SELECT JSON_DUALITY_OBJECT(WITH (INSERT, UPDATE, DELETE) '_id' : `f1`) FROM `t`"},
		{"CREATE JSON DUALITY VIEW IF NOT EXISTS dv2 AS SELECT JSON_DUALITY_OBJECT(\"_id\" : C1, \"C2\" : C2) FROM T1", true, "CREATE JSON DUALITY VIEW IF NOT EXISTS `dv2` AS SELECT JSON_DUALITY_OBJECT('_id' : `C1`, 'C2' : `C2`) FROM `T1`"},
		{"CREATE OR REPLACE ALGORITHM = MERGE DEFINER = current_user() SQL SECURITY INVOKER JSON RELATIONAL DUALITY VIEW dv1 AS SELECT 1", true, "CREATE OR REPLACE ALGORITHM = MERGE SQL SECURITY INVOKER JSON RELATIONAL DUALITY VIEW `dv1` AS SELECT 1"},
		{"CREATE ALGORITHM = TEMPTABLE DEFINER = u@h JSON DUALITY VIEW dv3 AS SELECT 1", true, "CREATE ALGORITHM = TEMPTABLE DEFINER = `u`@`h` JSON DUALITY VIEW `dv3` AS SELECT 1"},
		// DUALITY and RELATIONAL stay usable as identifiers.
		{"SELECT duality, relational FROM t", true, "SELECT `duality`,`relational` FROM `t`"},
		{"CREATE TABLE duality (relational INT)", true, "CREATE TABLE `duality` (`relational` INT)"},
	}
	RunTest(t, table, false)
}

// TestJSONDualityObject covers the JSON_DUALITY_OBJECT member forms; the
// comma spelling is a syntax error, members use 'key' : value.
func TestJSONDualityObject(t *testing.T) {
	table := []testCase{
		{"SELECT JSON_DUALITY_OBJECT(WITH(UPDATE, DELETE, INSERT) '_id' : id, 'x' : x) FROM t1", true, "SELECT JSON_DUALITY_OBJECT(WITH (UPDATE, DELETE, INSERT) '_id' : `id`, 'x' : `x`) FROM `t1`"},
		{"SELECT JSON_DUALITY_OBJECT('a', 1)", false, ""},
		// Nested constructors inside aggregates and subqueries.
		{"SELECT JSON_ARRAYAGG(JSON_DUALITY_OBJECT('c' : (SELECT JSON_DUALITY_OBJECT('d' : d) FROM u))) FROM t", true, "SELECT JSON_ARRAYAGG(JSON_DUALITY_OBJECT('c' : (SELECT JSON_DUALITY_OBJECT('d' : `d`) FROM `u`))) FROM `t`"},
	}
	RunTest(t, table, false)
}

// TestDropAlterRoutine covers DROP and ALTER for all four routine kinds.
func TestDropAlterRoutine(t *testing.T) {
	table := []testCase{
		{"DROP PROCEDURE bulk_insert_dest", true, "DROP PROCEDURE `bulk_insert_dest`"},
		{"DROP PROCEDURE IF EXISTS p1", true, "DROP PROCEDURE IF EXISTS `p1`"},
		{"DROP FUNCTION IF EXISTS test.f1", true, "DROP FUNCTION IF EXISTS `test`.`f1`"},
		{"DROP TRIGGER IF EXISTS t1_bi", true, "DROP TRIGGER IF EXISTS `t1_bi`"},
		{"DROP EVENT IF EXISTS e1", true, "DROP EVENT IF EXISTS `e1`"},
		{"alter procedure bar comment 'foo'", true, "ALTER PROCEDURE `bar` COMMENT 'foo'"},
		{"ALTER PROCEDURE p1", true, "ALTER PROCEDURE `p1`"},
		{"ALTER FUNCTION sf1 SQL SECURITY INVOKER CONTAINS SQL", true, "ALTER FUNCTION `sf1` SQL SECURITY INVOKER CONTAINS SQL"},
		{"ALTER FUNCTION f1 USING (lib1, lib2 AS l2)", true, "ALTER FUNCTION `f1` USING (`lib1`, `lib2` AS `l2`)"},
		{"ALTER FUNCTION f1 DROP COMMENT", true, "ALTER FUNCTION `f1` DROP COMMENT"},
		{"ALTER EVENT e1 ON SCHEDULE EVERY 5 MINUTE", true, "ALTER EVENT `e1` ON SCHEDULE EVERY 5 MINUTE"},
		{"ALTER EVENT e1 ON COMPLETION NOT PRESERVE", true, "ALTER EVENT `e1` ON COMPLETION NOT PRESERVE"},
		{"ALTER EVENT e1 RENAME TO e2 ENABLE", true, "ALTER EVENT `e1` RENAME TO `e2` ENABLE"},
		{"ALTER EVENT e1 DO SELECT 2", true, "ALTER EVENT `e1` DO SELECT 2"},
		// A CURRENT_USER definer matches what an absent clause parses to, so
		// it is not written back.
		{"ALTER DEFINER = CURRENT_USER EVENT e1 COMMENT 'x'", true, "ALTER EVENT `e1` COMMENT 'x'"},
		{"ALTER DEFINER = u@h EVENT e1 DISABLE ON SLAVE", true, "ALTER DEFINER = `u`@`h` EVENT `e1` DISABLE ON REPLICA"},
	}
	RunTest(t, table, false)
}

// TestCreateProcedureFunction covers stored and loadable routine creation.
func TestCreateProcedureFunction(t *testing.T) {
	table := []testCase{
		{"CREATE PROCEDURE p1() SELECT 1", true, "CREATE PROCEDURE `p1`() SELECT 1"},
		{"CREATE PROCEDURE p2(IN a INT, OUT b VARCHAR(10), INOUT c DECIMAL(10,2)) BEGIN SELECT a; END", true, "CREATE PROCEDURE `p2`(IN `a` INT, OUT `b` VARCHAR(10), INOUT `c` DECIMAL(10,2)) BEGIN SELECT `a`; END"},
		{"CREATE DEFINER=root@localhost PROCEDURE p3() COMMENT 'hi' DETERMINISTIC READS SQL DATA SQL SECURITY INVOKER BEGIN END", true, "CREATE DEFINER = `root`@`localhost` PROCEDURE `p3`() COMMENT 'hi' DETERMINISTIC READS SQL DATA SQL SECURITY INVOKER BEGIN END"},
		{"CREATE PROCEDURE IF NOT EXISTS p4() NOT DETERMINISTIC NO SQL MODIFIES SQL DATA LANGUAGE SQL SELECT 1", true, "CREATE PROCEDURE IF NOT EXISTS `p4`() NOT DETERMINISTIC NO SQL MODIFIES SQL DATA LANGUAGE SQL SELECT 1"},
		{"CREATE FUNCTION f1(a INT) RETURNS INT RETURN a + 1", true, "CREATE FUNCTION `f1`(`a` INT) RETURNS INT RETURN `a`+1"},
		{"CREATE FUNCTION f2() RETURNS VARCHAR(20) CHARSET utf8mb4 BEGIN RETURN 'x'; END", true, "CREATE FUNCTION `f2`() RETURNS VARCHAR(20) CHARACTER SET UTF8MB4 BEGIN RETURN _UTF8MB4'x'; END"},
		// Loadable (UDF) functions: no parameter list, keyword return type.
		{"CREATE AGGREGATE FUNCTION udf_sum RETURNS REAL SONAME 'udf.so'", true, "CREATE AGGREGATE FUNCTION `udf_sum` RETURNS REAL SONAME 'udf.so'"},
		{"CREATE FUNCTION metaphon RETURNS STRING SONAME 'udf_example.so'", true, "CREATE FUNCTION `metaphon` RETURNS STRING SONAME 'udf_example.so'"},
		{"CREATE FUNCTION udf_i RETURNS INT SONAME 'u.so'", true, "CREATE FUNCTION `udf_i` RETURNS INTEGER SONAME 'u.so'"},
		// External language bodies: AS 'string' and AS $tag$...$tag$.
		{"CREATE FUNCTION js_f(n INT) RETURNS INT LANGUAGE JAVASCRIPT AS $$ return n * 2 $$", true, "CREATE FUNCTION `js_f`(`n` INT) RETURNS INT LANGUAGE JAVASCRIPT AS ' return n * 2 '"},
		{"CREATE PROCEDURE js_p() LANGUAGE JAVASCRIPT AS 'console.log(1)'", true, "CREATE PROCEDURE `js_p`() LANGUAGE JAVASCRIPT AS 'console.log(1)'"},
		// The routine keywords stay usable where MySQL keeps them unreserved.
		{"SELECT returns, contains, schedule, every FROM t", true, "SELECT `returns`,`contains`,`schedule`,`every` FROM `t`"},
		{"CALL work(10000)", true, "CALL `work`(10000)"},
		{"call avg ()", true, "CALL `avg`()"},
		// DECLARE is reserved in MySQL.
		{"CREATE TABLE declare (a INT)", false, ""},
	}
	RunTest(t, table, false)
}

// TestCreateTriggerEvent covers trigger and event creation.
func TestCreateTriggerEvent(t *testing.T) {
	table := []testCase{
		// new.a is written lowercase: the SET restore goes through the
		// @@SESSION spelling, which case-folds variable names on reparse.
		{"CREATE TRIGGER t1_bi BEFORE INSERT ON t1 FOR EACH ROW SET new.a = 1", true, "CREATE TRIGGER `t1_bi` BEFORE INSERT ON `t1` FOR EACH ROW SET @@SESSION.`new.a`=1"},
		{"CREATE TRIGGER t1_au AFTER UPDATE ON t1 FOR EACH ROW FOLLOWS t1_au0 BEGIN INSERT INTO log VALUES (OLD.a, NEW.a); END", true, "CREATE TRIGGER `t1_au` AFTER UPDATE ON `t1` FOR EACH ROW FOLLOWS `t1_au0` BEGIN INSERT INTO `log` VALUES (`OLD`.`a`,`NEW`.`a`); END"},
		{"CREATE DEFINER=u@h TRIGGER IF NOT EXISTS t1_bd BEFORE DELETE ON t1 FOR EACH ROW PRECEDES o BEGIN END", true, "CREATE DEFINER = `u`@`h` TRIGGER IF NOT EXISTS `t1_bd` BEFORE DELETE ON `t1` FOR EACH ROW PRECEDES `o` BEGIN END"},
		{"CREATE EVENT e1 ON SCHEDULE AT '2026-01-01 00:00:00' DO UPDATE t SET a = a + 1", true, "CREATE EVENT `e1` ON SCHEDULE AT _UTF8MB4'2026-01-01 00:00:00' DO UPDATE `t` SET `a`=`a`+1"},
		{"CREATE EVENT IF NOT EXISTS e2 ON SCHEDULE EVERY 1 DAY STARTS '2026-01-01' ENDS '2027-01-01' ON COMPLETION PRESERVE DISABLE ON SLAVE COMMENT 'nightly' DO BEGIN CALL p1(); END", true, "CREATE EVENT IF NOT EXISTS `e2` ON SCHEDULE EVERY 1 DAY STARTS _UTF8MB4'2026-01-01' ENDS _UTF8MB4'2027-01-01' ON COMPLETION PRESERVE DISABLE ON REPLICA COMMENT 'nightly' DO BEGIN CALL `p1`(); END"},
		{"CREATE EVENT e3 ON SCHEDULE EVERY 5 MINUTE ENABLE DO SELECT 1", true, "CREATE EVENT `e3` ON SCHEDULE EVERY 5 MINUTE ENABLE DO SELECT 1"},
	}
	RunTest(t, table, false)
}

// TestSignalResignal covers SIGNAL and RESIGNAL, valid both at top level and
// in stored program bodies.
func TestSignalResignal(t *testing.T) {
	table := []testCase{
		{"SIGNAL SQLSTATE '01000'", true, "SIGNAL SQLSTATE '01000'"},
		{"SIGNAL SQLSTATE VALUE '45000'", true, "SIGNAL SQLSTATE '45000'"},
		{"SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'oops', MYSQL_ERRNO = 1000", true, "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = _UTF8MB4'oops', MYSQL_ERRNO = 1000"},
		{"SIGNAL my_cond", true, "SIGNAL `my_cond`"},
		{"RESIGNAL", true, "RESIGNAL"},
		{"RESIGNAL SET MYSQL_ERRNO = 5", true, "RESIGNAL SET MYSQL_ERRNO = 5"},
		{"RESIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'e'", true, "RESIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = _UTF8MB4'e'"},
	}
	RunTest(t, table, false)
}

// TestStoredProgramBody covers the compound statement language.
func TestStoredProgramBody(t *testing.T) {
	table := []testCase{
		{"CREATE PROCEDURE wh() BEGIN DECLARE i INT DEFAULT 0; WHILE i < 10 DO SET i = i + 1; END WHILE; END", true, "CREATE PROCEDURE `wh`() BEGIN DECLARE `i` INT DEFAULT 0; WHILE `i`<10 DO SET @@SESSION.`i`=`i`+1; END WHILE; END"},
		{"CREATE PROCEDURE rp() BEGIN REPEAT SELECT 1; UNTIL done END REPEAT; END", true, "CREATE PROCEDURE `rp`() BEGIN REPEAT SELECT 1; UNTIL `done` END REPEAT; END"},
		{"CREATE PROCEDURE ifp(x INT) BEGIN IF x = 1 THEN SELECT 1; ELSEIF x = 2 THEN SELECT 2; ELSE SELECT 0; END IF; END", true, "CREATE PROCEDURE `ifp`(IN `x` INT) BEGIN IF `x`=1 THEN SELECT 1; ELSEIF `x`=2 THEN SELECT 2; ELSE SELECT 0; END IF; END"},
		{"CREATE PROCEDURE lbl() lab: BEGIN ITERATE lab; LEAVE lab; END lab", true, "CREATE PROCEDURE `lbl`() `lab`: BEGIN ITERATE `lab`; LEAVE `lab`; END `lab`"},
		{"CREATE PROCEDURE dcl() BEGIN DECLARE a, b BIGINT UNSIGNED DEFAULT 0; DECLARE my_cond CONDITION FOR SQLSTATE '45000'; DECLARE c2 CONDITION FOR 1062; SIGNAL my_cond; END", true, "CREATE PROCEDURE `dcl`() BEGIN DECLARE `a`, `b` BIGINT UNSIGNED DEFAULT 0; DECLARE `my_cond` CONDITION FOR SQLSTATE '45000'; DECLARE `c2` CONDITION FOR 1062; SIGNAL `my_cond`; END"},
		{"CREATE PROCEDURE hnd() BEGIN DECLARE CONTINUE HANDLER FOR SQLEXCEPTION, SQLWARNING, 1062, SQLSTATE '23000', my_cond BEGIN END; DECLARE EXIT HANDLER FOR NOT FOUND SELECT 'gone'; END", true, "CREATE PROCEDURE `hnd`() BEGIN DECLARE CONTINUE HANDLER FOR SQLEXCEPTION, SQLWARNING, 1062, SQLSTATE '23000', `my_cond` BEGIN END; DECLARE EXIT HANDLER FOR NOT FOUND SELECT _UTF8MB4'gone'; END"},
		{"CREATE PROCEDURE cur() BEGIN DECLARE cur CURSOR FOR SELECT a FROM t1; OPEN cur; FETCH NEXT FROM cur INTO a, @b; FETCH FROM cur INTO a; FETCH cur INTO a; CLOSE cur; END", true, "CREATE PROCEDURE `cur`() BEGIN DECLARE `cur` CURSOR FOR SELECT `a` FROM `t1`; OPEN `cur`; FETCH `cur` INTO `a`, @`b`; FETCH `cur` INTO `a`; FETCH `cur` INTO `a`; CLOSE `cur`; END"},
		// A cursor may be named next; FETCH NEXT FROM still parses.
		{"CREATE PROCEDURE fnx() BEGIN FETCH next INTO a; FETCH NEXT FROM next INTO a; END", true, "CREATE PROCEDURE `fnx`() BEGIN FETCH `next` INTO `a`; FETCH `next` INTO `a`; END"},
		{"CREATE PROCEDURE nst() BEGIN BEGIN SELECT 1; END; lp: WHILE 1 = 1 DO LEAVE lp; END WHILE lp; END", true, "CREATE PROCEDURE `nst`() BEGIN BEGIN SELECT 1; END; `lp`: WHILE 1=1 DO LEAVE `lp`; END WHILE `lp`; END"},
		{"CREATE FUNCTION rf() RETURNS INT BEGIN RETURN 1; END", true, "CREATE FUNCTION `rf`() RETURNS INT BEGIN RETURN 1; END"},
	}
	RunTest(t, table, false)
}

// TestStoredProgramCase pins the CASE statement forms separately because the
// restore spacing around WHEN arms is load-bearing for the reparse.
func TestStoredProgramCase(t *testing.T) {
	table := []testCase{
		{"CREATE PROCEDURE cs(x INT) BEGIN CASE x WHEN 1 THEN SELECT 'one'; ELSE SELECT 'many'; END CASE; END", true, "CREATE PROCEDURE `cs`(IN `x` INT) BEGIN CASE `x` WHEN 1 THEN SELECT _UTF8MB4'one';  ELSE SELECT _UTF8MB4'many';  END CASE; END"},
		{"CREATE PROCEDURE cw(x INT) BEGIN CASE WHEN x > 0 THEN SELECT '+'; END CASE; END", true, "CREATE PROCEDURE `cw`(IN `x` INT) BEGIN CASE WHEN `x`>0 THEN SELECT _UTF8MB4'+';  END CASE; END"},
	}
	RunTest(t, table, false)
}

// TestSelectIntoVariables covers SELECT ... INTO, which names routine
// variables as bare identifiers, and the trailing INTO of a parenthesised
// query expression.
func TestSelectIntoVariables(t *testing.T) {
	table := []testCase{
		{"SELECT a, b INTO x, y FROM t1", true, "SELECT `a`,`b` FROM `t1` INTO `x`, `y`"},
		{"SELECT a INTO @v FROM t1", true, "SELECT `a` FROM `t1` INTO @`v`"},
		{"(SELECT a FROM t1 ORDER BY a LIMIT 1) INTO x", true, "(SELECT `a` FROM `t1` ORDER BY `a` LIMIT 1) INTO `x`"},
		{"(SELECT a FROM t1) INTO @v", true, "(SELECT `a` FROM `t1`) INTO @`v`"},
	}
	RunTest(t, table, false)
}

// TestRoutineDeclarations covers the routine-signature clauses: a trailing
// COLLATE on a parameter, RETURNS or DECLARE type, library aliases without
// AS, GET DIAGNOSTICS naming routine variables, and function-argument
// attribute aliases (which MySQL accepts syntactically for any function).
func TestRoutineDeclarations(t *testing.T) {
	table := []testCase{
		{"CREATE PROCEDURE p(x CHAR(3) COLLATE utf8mb4_bin) BEGIN DECLARE y VARCHAR(5) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT 'a'; END", true, "CREATE PROCEDURE `p`(IN `x` CHAR(3) COLLATE utf8mb4_bin) BEGIN DECLARE `y` VARCHAR(5) CHARACTER SET LATIN1 COLLATE latin1_bin DEFAULT _UTF8MB4'a'; END"},
		{"CREATE FUNCTION f() RETURNS CHAR(3) COLLATE utf8mb4_bin RETURN 'a'", true, "CREATE FUNCTION `f`() RETURNS CHAR(3) COLLATE utf8mb4_bin RETURN _UTF8MB4'a'"},
		{"CREATE FUNCTION jf(n INT) RETURNS INT LANGUAGE JAVASCRIPT USING (mylib ml, other AS o) AS $$ return 1 $$", true, "CREATE FUNCTION `jf`(`n` INT) RETURNS INT LANGUAGE JAVASCRIPT USING (`mylib` AS `ml`, `other` AS `o`) AS ' return 1 '"},
		{"CREATE PROCEDURE gd() BEGIN GET DIAGNOSTICS CONDITION n @x = MESSAGE_TEXT; GET DIAGNOSTICS CONDITION 1 v = RETURNED_SQLSTATE; END", true, "CREATE PROCEDURE `gd`() BEGIN GET DIAGNOSTICS CONDITION `n` @`x` = MESSAGE_TEXT; GET DIAGNOSTICS CONDITION 1 `v` = RETURNED_SQLSTATE; END"},
		{"SELECT myudf(a AS x, b y) FROM t1", true, "SELECT MYUDF(`a` AS `x`, `b` AS `y`) FROM `t1`"},
	}
	RunTest(t, table, false)
}

// TestJoinAndOperatorSpellings covers alternate spellings MySQL accepts in
// joins, casts and the MEMBER OF predicate, plus the STRAIGHT_JOIN nesting
// that lets a following join keep its own ON clause.
func TestJoinAndOperatorSpellings(t *testing.T) {
	table := []testCase{
		{"SELECT * FROM t1 NATURAL INNER JOIN t2", true, "SELECT * FROM `t1` NATURAL JOIN `t2`"},
		{"SELECT CAST(1 AS DOUBLE PRECISION)", true, "SELECT CAST(1 AS DOUBLE)"},
		{"SELECT 1 MEMBER ('[1]')", true, "SELECT 1 MEMBER OF (_UTF8MB4'[1]')"},
		{"SELECT * FROM t1 a STRAIGHT_JOIN t2 b INNER JOIN t3 c ON b.x = c.x ON a.y = b.y", true, "SELECT * FROM `t1` AS `a` STRAIGHT_JOIN (`t2` AS `b` JOIN `t3` AS `c` ON `b`.`x`=`c`.`x`) ON `a`.`y`=`b`.`y`"},
		// MEMBER binds as the predicate, so a trailing bare MEMBER is a
		// syntax error rather than a column alias -- as in MySQL.
		{"SELECT 1 MEMBER", false, ""},
	}
	RunTest(t, table, false)
}

// TestJSONTableClauses covers the JSON_TABLE-only relaxations: either
// ON EMPTY/ON ERROR order, and a character-set introducer on a path literal.
func TestJSONTableClauses(t *testing.T) {
	table := []testCase{
		{"SELECT * FROM JSON_TABLE ('[1]', '$[*]' COLUMNS (a INT PATH '$')) jt", true, "SELECT * FROM JSON_TABLE(_UTF8MB4'[1]', '$[*]' COLUMNS (`a` INT PATH '$')) AS `jt`"},
		{"SELECT * FROM JSON_TABLE('{}', '$' COLUMNS (x VARCHAR(10) PATH '$.x' NULL ON ERROR NULL ON EMPTY)) jt", true, "SELECT * FROM JSON_TABLE(_UTF8MB4'{}', '$' COLUMNS (`x` VARCHAR(10) PATH '$.x' NULL ON EMPTY NULL ON ERROR)) AS `jt`"},
		{"SELECT * FROM JSON_TABLE(JSON_OBJECT(), _utf8mb4'$' COLUMNS (NESTED PATH _utf8mb4'$.x' COLUMNS (y INT PATH _utf8mb4'$.y'))) jt", true, "SELECT * FROM JSON_TABLE(JSON_OBJECT(), '$' COLUMNS (NESTED PATH '$.x' COLUMNS (`y` INT PATH '$.y'))) AS `jt`"},
		// JSON_VALUE keeps MySQL's stricter ordering.
		{"SELECT JSON_VALUE('{}', '$.x' NULL ON ERROR NULL ON EMPTY)", false, ""},
	}
	RunTest(t, table, false)
}

// TestCharsetBinaryCombos covers ASCII/UNICODE combined with BINARY in either
// order, which MySQL folds into a charset plus the binary flag.
func TestCharsetBinaryCombos(t *testing.T) {
	table := []testCase{
		{"CREATE TABLE t1 (a VARCHAR(10) BINARY ASCII, b VARCHAR(10) ASCII BINARY, c VARCHAR(10) BINARY UNICODE, d VARCHAR(10) UNICODE BINARY)", true, "CREATE TABLE `t1` (`a` VARCHAR(10) BINARY CHARACTER SET LATIN1,`b` VARCHAR(10) BINARY CHARACTER SET LATIN1,`c` VARCHAR(10) BINARY CHARACTER SET UCS2,`d` VARCHAR(10) BINARY CHARACTER SET UCS2)"},
		{"SELECT CONVERT('a', CHAR(2) ASCII)", true, "SELECT CONVERT(_UTF8MB4'a', CHAR(2) CHARSET LATIN1)"},
	}
	RunTest(t, table, false)
}

// TestRoutineVariableOperands covers the places a routine parameter or local
// may stand in for a literal: a LEAD/LAG offset, LIMIT, and KILL.
func TestRoutineVariableOperands(t *testing.T) {
	table := []testCase{
		{"CREATE PROCEDURE p1(n INTEGER) DO LAG('x', n) OVER ()", true, "CREATE PROCEDURE `p1`(IN `n` INT) DO LAG(_UTF8MB4'x', `n`) OVER ()"},
		{"CREATE PROCEDURE p2(p1 INT, p2 INT) SELECT * FROM t1 LIMIT p1, p2", true, "CREATE PROCEDURE `p2`(IN `p1` INT, IN `p2` INT) SELECT * FROM `t1` LIMIT `p1`,`p2`"},
		{"CREATE PROCEDURE p6(a INT) KILL a", true, "CREATE PROCEDURE `p6`(IN `a` INT) KILL `a`"},
		{"CREATE PROCEDURE p7(a INT) KILL QUERY a", true, "CREATE PROCEDURE `p7`(IN `a` INT) KILL QUERY `a`"},
	}
	RunTest(t, table, true)
}

// TestRoutineBodyControl covers START TRANSACTION as a routine body statement
// (a bare BEGIN there opens a compound statement instead) and the loop forms,
// whose bodies MySQL requires to be non-empty.
func TestRoutineBodyControl(t *testing.T) {
	table := []testCase{
		{"CREATE PROCEDURE p3() START TRANSACTION", true, "CREATE PROCEDURE `p3`() START TRANSACTION"},
		{"CREATE PROCEDURE p4() BEGIN START TRANSACTION; COMMIT; END", true, "CREATE PROCEDURE `p4`() BEGIN START TRANSACTION; COMMIT; END"},
		{"CREATE PROCEDURE p() BEGIN END", true, "CREATE PROCEDURE `p`() BEGIN END"},
		{"CREATE PROCEDURE p() BEGIN LOOP END LOOP; END", false, ""},
		{"CREATE PROCEDURE p() BEGIN WHILE 0 DO END WHILE; END", false, ""},
		{"CREATE PROCEDURE p() BEGIN REPEAT UNTIL 1 END REPEAT; END", false, ""},
	}
	RunTest(t, table, false)
}

// TestKeywordLabels covers labels named after unreserved keywords. MySQL
// excludes the keywords that open a statement, and so does the fork.
func TestKeywordLabels(t *testing.T) {
	table := []testCase{
		{"CREATE PROCEDURE p5() BEGIN skip: LOOP LEAVE skip; END LOOP skip; END", true, "CREATE PROCEDURE `p5`() BEGIN `skip`: LOOP LEAVE `skip`; END LOOP `skip`; END"},
		{"CREATE FUNCTION f2() RETURNS INT BEGIN format: LOOP RETURN 1; END LOOP; RETURN 0; END", true, "CREATE FUNCTION `f2`() RETURNS INT BEGIN `format`: LOOP RETURN 1; END LOOP; RETURN 0; END"},
		{"CREATE PROCEDURE p() BEGIN until: LOOP LEAVE until; END LOOP until; END", true, "CREATE PROCEDURE `p`() BEGIN `until`: LOOP LEAVE `until`; END LOOP `until`; END"},
		{"CREATE PROCEDURE p() BEGIN start: LOOP LEAVE start; END LOOP start; END", false, ""},
		{"CREATE PROCEDURE p() BEGIN commit: LOOP LEAVE commit; END LOOP commit; END", false, ""},
	}
	RunTest(t, table, false)
}

// TestIntervalArity pins the INTERVAL split: the function form takes two or
// more arguments, so a single parenthesised argument is a temporal interval.
func TestIntervalArity(t *testing.T) {
	table := []testCase{
		{"SELECT '1985-10-19' - INTERVAL(1) DAY_MICROSECOND", true, "SELECT DATE_SUB(_UTF8MB4'1985-10-19', INTERVAL (1) DAY_MICROSECOND)"},
		{"SELECT INTERVAL(1, 2, 3)", true, "SELECT INTERVAL(1, 2, 3)"},
		{"SELECT NOW() + INTERVAL 1 DAY", true, "SELECT DATE_ADD(NOW(), INTERVAL 1 DAY)"},
		{"SELECT INTERVAL(1)", false, ""},
	}
	RunTest(t, table, false)
}

// TestRegexpPattern pins REGEXP's pattern as a full bit_expr. LIKE's pattern is
// a simple_expr, so the same shape is a syntax error there — on a live server
// too.
func TestRegexpPattern(t *testing.T) {
	table := []testCase{
		{"SELECT 'a' REGEXP 'a' + 'b'", true, "SELECT _UTF8MB4'a' REGEXP _UTF8MB4'a'+_UTF8MB4'b'"},
		{"SELECT a REGEXP b | c", true, "SELECT `a` REGEXP `b`|`c`"},
		{"SELECT a RLIKE b + c", true, "SELECT `a` REGEXP `b`+`c`"},
		{"SELECT a NOT REGEXP b + c", true, "SELECT `a` NOT REGEXP `b`+`c`"},
		{"SELECT 'a' LIKE 'a' + 'b'", false, ""},
	}
	RunTest(t, table, false)
}
