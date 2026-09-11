package dbconn

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/block/mysql"
)

// DriverName is the database/sql driver spirit opens every connection with.
//
// It is exported because it is part of this package's contract, not an
// implementation detail: EnhanceDSNWithTLS returns a DSN whose tls= name
// refers to an entry this package registered, and a driver's TLS registry is
// a package-level global. A consumer that opens such a DSN with a different
// driver gets "invalid value / unknown config name: rds" at connect time —
// an error that says nothing about drivers. Open it with sql.Open(
// dbconn.DriverName, ...) and that stays correct through any future move.
const DriverName = "block-mysql"

const (
	rdsTLSConfigName      = "rds"
	customTLSConfigName   = "custom"
	requiredTLSConfigName = "required"
	verifyCATLSConfigName = "verify_ca"
	verifyIDTLSConfigName = "verify_identity"

	// tlsDisabledConfigName is the DSN's explicit "no TLS" value. Unlike the
	// names above it is not something this package registers — the driver
	// understands it directly — and unlike an empty tls= it survives the
	// driver's RDS auto-TLS. See the DISABLED branch of newDSN.
	tlsDisabledConfigName = "false"
)

// maxConnLifetime is the default maximum lifetime for pooled connections.
// It is a var (not a const) only so tests can shorten it; production code
// must not modify it. Note that pools holding session-scoped state (such as
// the advisory lock's GET_LOCK) are deliberately exempted from this limit —
// see NewAdvisoryLock.
var maxConnLifetime = time.Minute * 3

// SetPoolSize sets a pool's connection limit, keeping the idle limit equal to
// it. Both must move together: database/sql closes a connection returned to a
// pool whose free list already holds MaxIdleConns entries, so an idle limit
// below the open limit turns every release past that point into a close and
// every subsequent acquire into a fresh dial, TLS handshake and MySQL auth.
// The copy phase is exactly that workload — up to a few hundred write and read
// workers cycling connections continuously — and the churn is invisible in the
// status block, which does not report pool internals at all.
//
// Holding the connections idle instead costs nothing the caller has not
// already reserved: SetMaxOpenConns is the budget, and this only stops the pool
// from throwing away what it is entitled to keep. Note that connections are
// still recycled on maxConnLifetime, which this does not change.
//
// n <= 0 means unlimited to SetMaxOpenConns; pass it through unchanged (and
// leave the idle limit alone, since "unlimited idle" is not expressible) rather
// than silently reinterpreting it.
func SetPoolSize(db *sql.DB, n int) {
	db.SetMaxOpenConns(n)
	if n > 0 {
		db.SetMaxIdleConns(n)
	}
}

var once sync.Once

// IsRDSHost reports whether host is an Amazon RDS or Aurora endpoint.
//
// [DriverName] applies verified TLS to such a host by itself, so the
// database/sql paths in this package no longer need to ask. It stays exported
// and in use for two reasons: [GetTLSConfigForBinlog] serves the go-mysql
// binlog client, which is not a database/sql connection and so is not covered
// by the driver; and block/schemabot calls it to decide a TLS mode.
//
// Two things changed by delegating. The match is now case-insensitive, which is
// a fix — DNS is case-insensitive, nothing normalizes the host, and a
// hostname that arrives uppercased is the same endpoint. And GovCloud and
// China endpoints now report false, because the bundle behind
// [NewTLSConfig] contains no roots for either partition, so verifying against
// it could only ever fail. Reach those with --tls-certificate-path and that
// partition's own bundle.
func IsRDSHost(host string) bool {
	return mysql.IsRDSAddr(host)
}

// NewTLSConfig returns a TLS config that verifies an Amazon RDS or Aurora
// server against the RDS root bundle.
//
// The bundle and the pool behind this used to be spirit's own — an embedded
// copy of global-bundle.pem plus an x509.CertPool built from it. Both now come
// from [DriverName], which carries the same bundle for its own auto-TLS, so
// there is one copy to refresh instead of two that can drift apart.
//
// Each call returns a config with its own RootCAs, so callers may modify the
// result (GetTLSConfigForBinlog sets ServerName on it). It also pins
// MinVersion to TLS 1.2, which spirit's version did not.
func NewTLSConfig() *tls.Config {
	return mysql.RDSTLSConfig()
}

// NewCustomTLSConfig creates a TLS config based on SSL mode and certificate data.
//
// An empty certData means "use the RDS roots", which is the fallback for a
// host that is not recognizably RDS and for which no --tls-certificate-path was
// given. That fallback is inherited behaviour and is rarely what anyone wants:
// a non-RDS server will not present an RDS-issued certificate, so VERIFY_CA
// and VERIFY_IDENTITY against it fail by construction. It is preserved here
// rather than changed, because tightening it is a behaviour change that
// belongs in its own commit.
func NewCustomTLSConfig(certData []byte, sslMode string) *tls.Config {
	var caCertPool *x509.CertPool
	if len(certData) == 0 {
		// The driver's RDS roots, and a private copy of the pool: the switch
		// below hands it to callers who may append to it, and x509.CertPool has
		// no copy-on-write.
		caCertPool = mysql.RDSTLSConfig().RootCAs
	} else {
		caCertPool = x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(certData)
	}

	switch strings.ToUpper(sslMode) {
	case "DISABLED":
		// This shouldn't be called for DISABLED mode, but handle gracefully
		return nil
	case "PREFERRED":
		// Encryption only - no certificate verification at all
		return &tls.Config{
			InsecureSkipVerify: true,
		}
	case "REQUIRED":
		// Encryption only - no certificate verification but could use RootCAs for fallback
		return &tls.Config{
			RootCAs:            caCertPool,
			InsecureSkipVerify: true,
		}
	case "VERIFY_CA":
		// Verify certificate against CA, but allow hostname mismatches
		return &tls.Config{
			RootCAs:            caCertPool,
			InsecureSkipVerify: true, // Skip all default verification
			VerifyPeerCertificate: func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
				// Custom verification that validates certificate chain but skips hostname
				if len(rawCerts) == 0 {
					return errors.New("no certificates provided")
				}

				// Parse all certificates in the chain
				var certs []*x509.Certificate
				for _, rawCert := range rawCerts {
					cert, err := x509.ParseCertificate(rawCert)
					if err != nil {
						return fmt.Errorf("failed to parse certificate: %w", err)
					}
					certs = append(certs, cert)
				}

				// Create intermediate pool from the chain (excluding leaf)
				intermediates := x509.NewCertPool()
				for _, cert := range certs[1:] {
					intermediates.AddCert(cert)
				}

				// Verify the certificate chain against our CA pool
				opts := x509.VerifyOptions{
					Roots:         caCertPool,
					Intermediates: intermediates,
					// Don't set DNSName to skip hostname verification
				}

				_, err := certs[0].Verify(opts)
				if err != nil {
					return fmt.Errorf("certificate verification failed: %w", err)
				}

				return nil // Certificate is valid
			},
		}
	case "VERIFY_IDENTITY":
		// Full verification including hostname
		return &tls.Config{
			RootCAs:            caCertPool,
			InsecureSkipVerify: false,
		}
	default:
		// Default to PREFERRED behavior - encryption only, no certificate verification
		return &tls.Config{
			InsecureSkipVerify: true,
		}
	}
}

// LoadCertificateFromFile loads certificate data from a file
func LoadCertificateFromFile(filePath string) ([]byte, error) {
	return os.ReadFile(filePath)
}

// initRDSTLS registers the RDS trust store under rdsTLSConfigName.
//
// The registration survives the driver's own auto-TLS because the name is part
// of this package's contract, not an internal detail: EnhanceDSNWithTLS returns
// DSNs carrying tls=rds, so a consumer must open them with [DriverName].
//
// That is a requirement, not a reassurance. A TLS registry is a package global
// of whichever driver package registered it, so a consumer that opens a
// tls=rds DSN with upstream go-sql-driver fails in ParseDSN with "invalid value
// / unknown config name: rds" — before any dial, on every RDS host. Since this
// package moved to github.com/block/mysql, "the same driver" means block/mysql.
// block/schemabot is the consumer that does this; it is switching to
// block/mysql alongside this change, which is what keeps it working.
func initRDSTLS() error {
	var err error
	once.Do(func() {
		err = mysql.RegisterTLSConfig(rdsTLSConfigName, NewTLSConfig())
	})
	return err
}

// initCustomTLS initializes a custom TLS configuration based on SSL mode
func initCustomTLS(config *DBConfig) error {
	var certData []byte
	var err error

	if config.TLSCertificatePath != "" {
		certData, err = LoadCertificateFromFile(config.TLSCertificatePath)
		if err != nil {
			return err
		}
		// An empty file is an error, not a fallback. NewCustomTLSConfig reads
		// no bytes as "use the RDS roots", which is right for a caller that
		// named no path but wrong for one that named a path to a truncated or
		// not-yet-populated private CA: that operator asked to verify against
		// their own root and would silently get Amazon's instead, with a
		// connection that succeeds. Only the path is distinguishable from the
		// bytes, so this has to be caught here.
		if len(certData) == 0 {
			return fmt.Errorf("TLS certificate file %q is empty; expected PEM-encoded CA certificates", config.TLSCertificatePath)
		}
	}
	// Otherwise certData stays nil, which NewCustomTLSConfig reads as "use the
	// RDS roots" — the same fallback as before, now sourced from the driver.

	tlsConfig := NewCustomTLSConfig(certData, config.TLSMode)
	if tlsConfig != nil {
		// Use mode-specific config names to avoid conflicts
		configName := getTLSConfigName(config.TLSMode)
		err = mysql.RegisterTLSConfig(configName, tlsConfig)
		// Ignore "TLS config already registered" errors for tests
		if err != nil && strings.Contains(err.Error(), "already registered") {
			err = nil
		}
	}
	return err
}

// getTLSConfigName returns the appropriate TLS config name for the mode
func getTLSConfigName(mode string) string {
	switch strings.ToUpper(mode) {
	case "DISABLED":
		// This should never be called for DISABLED mode, but handle gracefully
		return ""
	case "PREFERRED":
		return customTLSConfigName
	case "REQUIRED":
		return requiredTLSConfigName
	case "VERIFY_CA":
		return verifyCATLSConfigName
	case "VERIFY_IDENTITY":
		return verifyIDTLSConfigName
	default:
		// Unknown modes default to custom behavior
		return customTLSConfigName
	}
}

// newDSN returns a new DSN to be used to connect to MySQL.
// It accepts a DSN as input and appends TLS configuration
// based on the provided configuration and host detection.
func newDSN(dsn string, config *DBConfig) (string, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", err
	}

	// Determine TLS configuration strategy based on SSL mode,
	// but only if the DSN doesn't already have explicit TLS configuration.
	if cfg.TLSConfig == "" {
		switch strings.ToUpper(config.TLSMode) {
		case "DISABLED":
			// No TLS — and it has to be said out loud rather than left blank.
			//
			// [DriverName] applies verified TLS to an RDS address when the DSN
			// asks for nothing, so leaving TLSConfig empty here would hand
			// --tls-mode=DISABLED users on RDS a TLS connection: exactly the
			// opposite of what they asked for, from a driver upgrade, with no
			// error to notice. "false" is the DSN's explicit off switch, which
			// the driver honours and its auto-TLS declines to override.
			cfg.TLSConfig = tlsDisabledConfigName

		case "REQUIRED", "VERIFY_CA", "VERIFY_IDENTITY":
			// TLS with certificate selection - determine which certificate to use
			switch {
			case config.TLSCertificatePath != "":
				// Use custom certificate
				if err = initCustomTLS(config); err != nil {
					return "", err
				}
				cfg.TLSConfig = getTLSConfigName(config.TLSMode)
			case IsRDSHost(cfg.Addr):
				// Use RDS certificate for RDS hosts
				if err = initRDSTLS(); err != nil {
					return "", err
				}
				cfg.TLSConfig = rdsTLSConfigName
			default:
				// Use the RDS roots as fallback for non-RDS hosts
				if err = initCustomTLS(config); err != nil {
					return "", err
				}
				cfg.TLSConfig = getTLSConfigName(config.TLSMode)
			}

		case "PREFERRED":
			fallthrough // Use same logic as default case

		default:
			// PREFERRED and unknown modes - use permissive TLS behavior
			// For RDS hosts, use RDS certificate. For others, use the RDS roots as fallback
			if IsRDSHost(cfg.Addr) {
				if err = initRDSTLS(); err != nil {
					return "", err
				}
				cfg.TLSConfig = rdsTLSConfigName
			} else {
				// Use the RDS roots as fallback for non-RDS hosts
				if err = initCustomTLS(config); err != nil {
					return "", err
				}
				cfg.TLSConfig = getTLSConfigName(config.TLSMode)
			}
		}
	} // end if cfg.TLSConfig == ""

	// Set session variables via Params map.
	//
	// Spirit overrides sql_mode on every connection. This looks ill-advised
	// at first glance — but the user may have inserted data with a more
	// permissive mode than the server's current default, and we need to be
	// able to reproduce that data exactly when copying. Standard tools take
	// the same approach: WordPress / Drupal change sql_mode, mysqldump
	// overrides it, etc. Historically Spirit set it to the empty string for
	// the most permissive copy possible.
	//
	// We now set it to exactly one mode: NO_AUTO_VALUE_ON_ZERO. Without
	// this mode, MySQL rewrites a literal 0 in an AUTO_INCREMENT column to
	// the next sequence value on INSERT/REPLACE. That silently corrupts a
	// MODIFY ... AUTO_INCREMENT migration of a column whose source data
	// already contains 0: the row at pk=0 ends up at a different pk in the
	// new table, and neither the copier's INSERT IGNORE SELECT nor the
	// checksum recopy can undo it (each retry re-allocates a fresh
	// auto-inc value). NO_AUTO_VALUE_ON_ZERO is otherwise a no-op (it
	// only affects literal-0 inserts into AUTO_INCREMENT columns), so it
	// is safe to enable on every connection — copier, checksum recopy,
	// binlog applier.
	if cfg.Params == nil {
		cfg.Params = make(map[string]string)
	}
	cfg.Params["sql_mode"] = `"NO_AUTO_VALUE_ON_ZERO"`
	cfg.Params["time_zone"] = `"+00:00"`
	cfg.Params["innodb_lock_wait_timeout"] = strconv.Itoa(config.InnodbLockWaitTimeout)
	cfg.Params["lock_wait_timeout"] = strconv.Itoa(config.LockWaitTimeout)
	cfg.Params["range_optimizer_max_mem_size"] = strconv.FormatInt(config.RangeOptimizerMaxMemSize, 10)
	cfg.Params["transaction_isolation"] = `"read-committed"`
	// go driver charset option, sets:
	// character_set_client, character_set_connection, character_set_results
	cfg.Params["charset"] = "utf8mb4"

	// Set driver options directly on the config struct.
	cfg.Collation = "utf8mb4_bin"
	// Note: there is no rejectReadOnly to set. [DriverName] recycles a
	// connection that reports a read-only error unconditionally — the option
	// and its default-off are gone — so the blue/green and Aurora-failover
	// protection spirit used to opt into is now simply how the driver behaves.
	cfg.InterpolateParams = config.InterpolateParams
	// Report tinyint(1) as an integer rather than a bool. The (1) is a display
	// width, not a range — the column holds the whole signed tinyint range —
	// but the driver maps every signed tinyint(1) to a Go bool by default,
	// collapsing any non-zero value to true. The copier reads rows back into
	// Go to build its INSERT, so that collapse would write 1 in place of a
	// stored 2, and the information is gone by the time spirit sees the value:
	// no amount of care further up can recover it.
	if err := cfg.Apply(mysql.TinyInt1IsBool(false)); err != nil {
		return "", fmt.Errorf("could not disable tinyInt1IsBool: %w", err)
	}
	// Allow cleartext password authentication only when the connection is
	// actually encrypted (required for AWS RDS IAM auth, safe because the
	// connection uses TLS).
	//
	// Checking against tlsDisabledConfigName as well as "" is load-bearing:
	// DISABLED now writes tls=false rather than leaving the field empty, and a
	// bare `!= ""` would read that as "TLS is on" and start sending passwords
	// in the clear over a plaintext connection.
	cfg.AllowCleartextPasswords = cfg.TLSConfig != "" && cfg.TLSConfig != tlsDisabledConfigName
	cfg.AllowNativePasswords = true

	return cfg.FormatDSN(), nil
}

// isTLSUnsupportedByServer reports whether err indicates that the MySQL server
// does not support TLS at all — i.e. the server's handshake advertised no
// CLIENT_SSL capability while the client requested TLS. go-sql-driver returns
// the sentinel mysql.ErrNoTLS ("TLS requested but server does not support TLS")
// in exactly this case (see packets.go handleAuthResult / readHandshakePacket).
//
// This is the ONLY condition under which PREFERRED mode is allowed to silently
// downgrade the connection to plaintext. Every other failure — a network
// timeout, max_connections exhaustion, bad credentials (1045), or a
// certificate-verification failure (which is how PREFERRED behaves against RDS
// hosts, where it uses the fully-verifying rds config) — is a genuine error
// that must propagate, not be papered over with an unencrypted connection.
func isTLSUnsupportedByServer(err error) bool {
	return errors.Is(err, mysql.ErrNoTLS)
}

// New is similar to sql.Open except we take the inputDSN and
// append additional options to it to standardize the connection.
// It will also ping the connection to ensure it is valid.
func New(inputDSN string, config *DBConfig) (db *sql.DB, err error) {
	return NewWithConnectionType(inputDSN, config, "main database")
}

// NewWithConnectionType is like New but includes context about the connection type for better error messages
func NewWithConnectionType(inputDSN string, config *DBConfig, connectionType string) (db *sql.DB, err error) {
	// Normalize the TLS mode once, up front, so every comparison and switch
	// below (and in newDSN) is case-insensitive. The CLI documents --tls-mode
	// as case-insensitive, so e.g. "preferred" must behave exactly like
	// "PREFERRED" rather than falling through to a default branch.
	if config != nil {
		configNorm := *config
		configNorm.TLSMode = strings.ToUpper(config.TLSMode)
		config = &configNorm
	}
	dsn, err := newDSN(inputDSN, config)
	if err != nil {
		return nil, err
	}
	defer func() {
		if db != nil && err == nil { // successful connection
			// There are many different ways we create a DB connection.
			// Ensure we change conn settings in all code paths.
			SetPoolSize(db, config.MaxOpenConnections)
			db.SetConnMaxLifetime(maxConnLifetime)
		}
	}()
	// For PREFERRED mode, implement fallback behavior
	if config.TLSMode == "PREFERRED" {
		// First try with TLS
		db, err := sql.Open(DriverName, dsn)
		if err == nil {
			//nolint: noctx // requires too much refactoring
			if pingErr := db.Ping(); pingErr == nil {
				// TLS connection successful
				return db, nil
			} else {
				_ = db.Close()
				// Only fall back to plaintext when the server genuinely does
				// not support TLS. Any other ping failure (timeout, auth,
				// max_connections, certificate verification) must propagate —
				// silently downgrading the whole pool to plaintext on those
				// would be a security regression and would mask real errors.
				if !isTLSUnsupportedByServer(pingErr) {
					return nil, fmt.Errorf("[%s-CONNECTION] ping failed: %w", strings.ToUpper(strings.ReplaceAll(connectionType, " ", "-")), pingErr)
				}
				slog.Warn("server does not support TLS; PREFERRED mode is downgrading this connection to plaintext",
					"connection_type", connectionType)
			}
		} else {
			// sql.Open only validates the DSN; a non-nil error here means the
			// DSN itself is bad, so there is nothing to ping or fall back to.
			return nil, fmt.Errorf("failed to open %s connection: %w", connectionType, err)
		}

		// TLS is unsupported by the server, try without TLS by rebuilding the
		// DSN with TLS disabled.
		// We must use newDSN (not createFallbackDSN on the raw inputDSN) so that
		// all critical session variables (sql_mode, time_zone, charset, rejectReadOnly, etc.)
		// are included in the fallback connection.
		configCopy := *config
		configCopy.TLSMode = "DISABLED"

		fallbackDSN, err := newDSN(inputDSN, &configCopy)
		if err != nil {
			return nil, fmt.Errorf("failed to create fallback DSN for %s connection: %w", connectionType, err)
		}

		db, err = sql.Open(DriverName, fallbackDSN)
		if err != nil {
			return nil, fmt.Errorf("failed to open fallback %s connection: %w", connectionType, err)
		}
		//nolint: noctx // requires too much refactoring
		if err := db.Ping(); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("[%s-CONNECTION-FALLBACK] ping failed: %w", strings.ToUpper(strings.ReplaceAll(connectionType, " ", "-")), err)
		}
		return db, nil
	}

	// For all other modes, use standard connection
	db, err = sql.Open(DriverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open %s connection: %w", connectionType, err)
	}
	//nolint: noctx // requires too much refactoring
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("[%s-CONNECTION] ping failed: %w", strings.ToUpper(strings.ReplaceAll(connectionType, " ", "-")), err)
	}
	return db, nil
}

// EnhanceDSNWithTLS enhances a DSN with TLS settings from the provided config
// if the DSN doesn't already contain TLS parameters.
// This allows replica connections to inherit TLS settings from the main connection
// while still respecting explicit TLS configuration in the DSN.
//
// The returned DSN names a TLS configuration registered in [DriverName]'s
// registry. Open it with that driver: a caller that hands the result to a
// different one fails at connect time with "invalid value / unknown config
// name", because TLS registries are per-driver package globals rather than
// anything the DSN carries.
func EnhanceDSNWithTLS(inputDSN string, config *DBConfig) (string, error) {
	// A nil config is "the caller said nothing about TLS", which is not the
	// same as asking for none: leave the DSN alone and let whatever opens it
	// decide. DISABLED is an explicit request and is handled below, because on
	// an RDS host it now takes a positive `tls=false` to be honored.
	if config == nil {
		return inputDSN, nil
	}

	// Handle empty DSN
	if inputDSN == "" {
		return inputDSN, nil
	}

	cfg, err := mysql.ParseDSN(inputDSN)
	if err != nil {
		// Return original DSN for graceful degradation when parsing fails
		return inputDSN, nil //nolint:nilerr // Intentional graceful degradation
	}

	// If DSN already has TLS configuration, respect it. This outranks
	// DISABLED, matching addTLSParametersToDSN: the DSN is the more specific
	// statement of intent.
	if cfg.TLSConfig != "" {
		return inputDSN, nil
	}

	// TLSMode is documented as case-insensitive; compare on the upper-cased
	// value so a lowercase "disabled" is honored here too.
	//
	// DISABLED used to return inputDSN untouched. That is no longer the same
	// thing as "no TLS": the driver applies TLS to an RDS address when the DSN
	// asked for nothing, so returning a DSN with no `tls=` at all handed
	// DISABLED callers the opposite of what they requested. Say it positively.
	// See newDSN, which carries the same fix for the other DSN producer.
	if strings.ToUpper(config.TLSMode) == "DISABLED" {
		cfg.TLSConfig = tlsDisabledConfigName
		return cfg.FormatDSN(), nil
	}

	// Enhance DSN with TLS settings from main config
	return addTLSParametersToDSN(inputDSN, config)
}

// addTLSParametersToDSN adds TLS parameters to a DSN based on the provided config
func addTLSParametersToDSN(dsn string, config *DBConfig) (string, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return dsn, err // Return original DSN with error if parsing fails
	}

	// If DSN already has explicit TLS configuration, preserve it
	if cfg.TLSConfig != "" {
		return cfg.FormatDSN(), nil
	}

	// Initialize TLS configurations if needed
	var tlsParam string
	switch strings.ToUpper(config.TLSMode) {
	case "DISABLED":
		// tls=false, not an untouched DSN: the driver reads "no tls= at all"
		// as permission to apply RDS auto-TLS, so silence here would enable
		// TLS on exactly the hosts DISABLED matters for.
		cfg.TLSConfig = tlsDisabledConfigName
		return cfg.FormatDSN(), nil
	case "PREFERRED":
		// For PREFERRED mode, we need to setup custom TLS config
		if err := initCustomTLS(config); err != nil {
			return dsn, err
		}
		tlsParam = customTLSConfigName
	case "REQUIRED":
		if IsRDSHost(cfg.Addr) {
			if err := initRDSTLS(); err != nil {
				return dsn, err
			}
			tlsParam = rdsTLSConfigName
		} else {
			if err := initCustomTLS(config); err != nil {
				return dsn, err
			}
			tlsParam = requiredTLSConfigName
		}
	case "VERIFY_CA":
		if err := initCustomTLS(config); err != nil {
			return dsn, err
		}
		tlsParam = verifyCATLSConfigName
	case "VERIFY_IDENTITY":
		if err := initCustomTLS(config); err != nil {
			return dsn, err
		}
		tlsParam = verifyIDTLSConfigName
	default:
		// For unknown modes, use PREFERRED logic
		if err := initCustomTLS(config); err != nil {
			return dsn, err
		}
		tlsParam = customTLSConfigName
	}

	// Add TLS parameter to DSN via parsed config to avoid issues with
	// special characters (e.g. ? or &) in the password
	cfg.TLSConfig = tlsParam
	return cfg.FormatDSN(), nil
}

// GetTLSConfigForBinlog creates a TLS config for binary log connections
// using the same logic as main database connections
func GetTLSConfigForBinlog(config *DBConfig, host string) (*tls.Config, error) {
	// TLSMode is documented as case-insensitive, so compare on the
	// upper-cased value. A lowercase "disabled" previously fell through the
	// exact "DISABLED" check below and into the default branch, which built a
	// PREFERRED-style TLS config for the binlog connection — that then failed
	// confusingly against a TLS-less server while the main pool worked.
	if config == nil || strings.ToUpper(config.TLSMode) == "DISABLED" {
		return nil, nil
	}

	var tlsConfig *tls.Config

	switch strings.ToUpper(config.TLSMode) {
	case "DISABLED":
		// No TLS for the binlog connection. (Unreachable in practice because
		// the early return above handles DISABLED, but kept explicit so the
		// switch covers every documented mode rather than treating DISABLED as
		// an unknown mode that falls into the PREFERRED-style default.)
		return nil, nil

	case "PREFERRED":
		// For PREFERRED mode, we need to setup custom TLS config
		if err := initCustomTLS(config); err != nil {
			return nil, err
		}
		var certData []byte
		if config.TLSCertificatePath != "" {
			var err error
			certData, err = LoadCertificateFromFile(config.TLSCertificatePath)
			if err != nil {
				return nil, fmt.Errorf("failed to load TLS certificate: %w", err)
			}
		}
		tlsConfig = NewCustomTLSConfig(certData, config.TLSMode)

	case "REQUIRED":
		if IsRDSHost(host) {
			if err := initRDSTLS(); err != nil {
				return nil, err
			}
			tlsConfig = NewTLSConfig()
		} else {
			if err := initCustomTLS(config); err != nil {
				return nil, err
			}
			var certData []byte
			if config.TLSCertificatePath != "" {
				var err error
				certData, err = LoadCertificateFromFile(config.TLSCertificatePath)
				if err != nil {
					return nil, fmt.Errorf("failed to load TLS certificate: %w", err)
				}
			}
			tlsConfig = NewCustomTLSConfig(certData, config.TLSMode)
		}

	case "VERIFY_CA":
		if err := initCustomTLS(config); err != nil {
			return nil, err
		}
		var certData []byte
		if config.TLSCertificatePath != "" {
			var err error
			certData, err = LoadCertificateFromFile(config.TLSCertificatePath)
			if err != nil {
				return nil, fmt.Errorf("failed to load TLS certificate: %w", err)
			}
		}
		tlsConfig = NewCustomTLSConfig(certData, config.TLSMode)

	case "VERIFY_IDENTITY":
		if err := initCustomTLS(config); err != nil {
			return nil, err
		}
		var certData []byte
		if config.TLSCertificatePath != "" {
			var err error
			certData, err = LoadCertificateFromFile(config.TLSCertificatePath)
			if err != nil {
				return nil, fmt.Errorf("failed to load TLS certificate: %w", err)
			}
		}
		tlsConfig = NewCustomTLSConfig(certData, config.TLSMode)

	default:
		// For unknown modes, use PREFERRED logic
		if err := initCustomTLS(config); err != nil {
			return nil, err
		}
		var certData []byte
		if config.TLSCertificatePath != "" {
			var err error
			certData, err = LoadCertificateFromFile(config.TLSCertificatePath)
			if err != nil {
				return nil, fmt.Errorf("failed to load TLS certificate: %w", err)
			}
		}
		tlsConfig = NewCustomTLSConfig(certData, config.TLSMode)
	}

	// Special handling for RDS hosts when TLS config is disabled or nil
	if tlsConfig == nil && IsRDSHost(host) {
		tlsConfig = NewTLSConfig()
	}

	// Set ServerName for certificate verification if we have a TLS config
	if tlsConfig != nil {
		tlsConfig.ServerName = host
	}

	return tlsConfig, nil
}

// SplitDSNs splits a comma-separated list of DSNs into a slice, trimming
// surrounding whitespace and dropping empty entries. An empty input returns
// nil. (Used for the replica DSN list, but takes no position on what the
// individual DSNs mean.)
func SplitDSNs(dsnList string) []string {
	if dsnList == "" {
		return nil
	}
	parts := strings.Split(dsnList, ",")
	dsns := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			dsns = append(dsns, trimmed)
		}
	}
	return dsns
}

// RedactDSN returns dsn with the password masked, safe for logging. It keeps
// the username, host and parameters so logs stay useful, masking only the
// password and only when one was actually present. If the driver can't parse
// the DSN it still never echoes a password: it redacts the credentials section
// before '@', or — lacking '@' — masks from the first ':' (a malformed
// "user:password" still has its password hidden).
func RedactDSN(dsn string) string {
	if dsn == "" {
		return dsn
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		// Unparseable — never risk echoing a password. If there's a credentials
		// separator ('@'), redact everything up to it. Otherwise a ':' may still
		// separate a "user:password" pair whose tail is malformed (no host), so
		// mask from the first ':'. Only a string with neither '@' nor ':' has
		// nothing credential-shaped to leak.
		if i := strings.LastIndex(dsn, "@"); i >= 0 {
			return "<redacted>" + dsn[i:]
		}
		if user, _, found := strings.Cut(dsn, ":"); found {
			return user + ":***"
		}
		return dsn
	}
	// Mask only when the DSN actually carried a password field (user:pass@ or
	// user:@), so a password-less "user@host" DSN is left untouched. The first
	// colon must precede the first '@' (a colon inside host:port comes after).
	at := strings.Index(dsn, "@")
	colon := strings.Index(dsn, ":")
	if colon != -1 && at != -1 && colon < at {
		cfg.Passwd = "***"
	}
	return cfg.FormatDSN()
}
