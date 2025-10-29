package dbconn

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
)

const (
	rdsTLSConfigName      = "rds"
	customTLSConfigName   = "custom"
	requiredTLSConfigName = "required"
	verifyCATLSConfigName = "verify_ca"
	verifyIDTLSConfigName = "verify_identity"
	maxConnLifetime       = time.Minute * 3
	maxIdleConns          = 10
)

// rdsAddr matches Amazon RDS hostnames with optional :port suffix.
// It's used to automatically load the Amazon RDS CA and enable TLS.
// The leading \. ensures only legitimate *.rds.amazonaws.com subdomains match,
// preventing subdomain spoofing attacks (e.g., fake-rds.amazonaws.com).
var (
	rdsAddr = regexp.MustCompile(`\.rds\.amazonaws\.com(:\d+)?$`)
	once    sync.Once
	// https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
	//go:embed rdsGlobalBundle.pem
	rdsGlobalBundle []byte
)

func IsRDSHost(host string) bool {
	return rdsAddr.MatchString(host)
}

// NewTLSConfig creates a TLS config using the embedded RDS global bundle
func NewTLSConfig() *tls.Config {
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(rdsGlobalBundle)
	return &tls.Config{RootCAs: caCertPool}
}

// NewCustomTLSConfig creates a TLS config based on SSL mode and certificate data
func NewCustomTLSConfig(certData []byte, sslMode string) *tls.Config {
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(certData)

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

// GetEmbeddedRDSBundle returns the embedded RDS certificate bundle
func GetEmbeddedRDSBundle() []byte {
	return rdsGlobalBundle
}

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
	} else {
		// Use embedded RDS bundle as fallback
		certData = rdsGlobalBundle
	}

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
	var ops []string
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", err
	}

	// If DSN already has TLS configuration, respect it and don't override
	if cfg.TLSConfig != "" {
		// DSN already has explicit TLS configuration - use it as-is
		return dsn, nil
	}

	// Check if DSN already has tls parameter in query string (case insensitive)
	lowerDSN := strings.ToLower(dsn)
	if strings.Contains(lowerDSN, "tls=") {
		// DSN already has explicit TLS parameter - use it as-is
		return dsn, nil
	}

	// Determine TLS configuration strategy based on SSL mode
	switch strings.ToUpper(config.TLSMode) {
	case "DISABLED":
		// No TLS - don't add any TLS parameters

	case "REQUIRED", "VERIFY_CA", "VERIFY_IDENTITY":
		// TLS with certificate selection - determine which certificate to use
		if config.TLSCertificatePath != "" {
			// Use custom certificate
			if err = initCustomTLS(config); err != nil {
				return "", err
			}
			configName := getTLSConfigName(config.TLSMode)
			ops = append(ops, fmt.Sprintf("%s=%s", "tls", url.QueryEscape(configName)))
		} else if IsRDSHost(cfg.Addr) {
			// Use RDS certificate for RDS hosts
			if err = initRDSTLS(); err != nil {
				return "", err
			}
			ops = append(ops, fmt.Sprintf("%s=%s", "tls", url.QueryEscape(rdsTLSConfigName)))
		} else {
			// Use embedded RDS bundle as fallback for non-RDS hosts
			if err = initCustomTLS(config); err != nil {
				return "", err
			}
			configName := getTLSConfigName(config.TLSMode)
			ops = append(ops, fmt.Sprintf("%s=%s", "tls", url.QueryEscape(configName)))
		}

	case "PREFERRED":
		fallthrough // Use same logic as default case

	default:
		// PREFERRED and unknown modes - use permissive TLS behavior
		// For RDS hosts, use RDS certificate. For others, use embedded RDS bundle as fallback
		if IsRDSHost(cfg.Addr) {
			if err = initRDSTLS(); err != nil {
				return "", err
			}
			ops = append(ops, fmt.Sprintf("%s=%s", "tls", url.QueryEscape(rdsTLSConfigName)))
		} else {
			// Use embedded RDS bundle as fallback for non-RDS hosts
			if err = initCustomTLS(config); err != nil {
				return "", err
			}
			configName := getTLSConfigName(config.TLSMode)
			ops = append(ops, fmt.Sprintf("%s=%s", "tls", url.QueryEscape(configName)))
		}
	}

	// Setting sql_mode looks ill-advised, but unfortunately it's required.
	// A user might have set their SQL mode to empty even if the
	// server has it enabled. After they've inserted data,
	// we need to be able to produce the same when copying.
	// If you look at standard packages like wordpress, drupal etc.
	// they all change the SQL mode. If you look at mysqldump, etc.
	// they all unset the SQL mode just like this.
	ops = append(ops, fmt.Sprintf("%s=%s", "sql_mode", url.QueryEscape(`""`)))
	ops = append(ops, fmt.Sprintf("%s=%s", "time_zone", url.QueryEscape(`"+00:00"`)))
	ops = append(ops, fmt.Sprintf("%s=%s", "innodb_lock_wait_timeout", url.QueryEscape(strconv.Itoa(config.InnodbLockWaitTimeout))))
	ops = append(ops, fmt.Sprintf("%s=%s", "lock_wait_timeout", url.QueryEscape(strconv.Itoa(config.LockWaitTimeout))))
	ops = append(ops, fmt.Sprintf("%s=%s", "range_optimizer_max_mem_size", url.QueryEscape(strconv.FormatInt(config.RangeOptimizerMaxMemSize, 10))))
	ops = append(ops, fmt.Sprintf("%s=%s", "transaction_isolation", url.QueryEscape(`"read-committed"`)))
	// go driver options, should set:
	// character_set_client, character_set_connection, character_set_results
	ops = append(ops, fmt.Sprintf("%s=%s", "charset", "binary"))
	ops = append(ops, fmt.Sprintf("%s=%s", "collation", "binary"))
	// So that we recycle the connection if we inadvertently connect to an old primary which is now a read only replica.
	// This behaviour has been observed during blue/green upgrades and failover on AWS Aurora.
	// See also: https://github.com/go-sql-driver/mysql?tab=readme-ov-file#rejectreadonly
	ops = append(ops, fmt.Sprintf("%s=%s", "rejectReadOnly", "true"))
	// Set interpolateParams
	ops = append(ops, fmt.Sprintf("%s=%t", "interpolateParams", config.InterpolateParams))
	// Allow mysql_native_password authentication
	ops = append(ops, fmt.Sprintf("%s=%s", "allowNativePasswords", "true"))

	// Check if DSN already has query parameters
	separator := "?"
	if strings.Contains(dsn, "?") {
		separator = "&"
	}
	dsn = fmt.Sprintf("%s%s%s", dsn, separator, strings.Join(ops, "&"))
	return dsn, nil
}

// New is similar to sql.Open except we take the inputDSN and
// append additional options to it to standardize the connection.
// It will also ping the connection to ensure it is valid.
func New(inputDSN string, config *DBConfig) (db *sql.DB, err error) {
	return NewWithContext(inputDSN, config, "main database")
}

// NewWithContext is like New but includes context about the connection type for better error messages
func NewWithContext(inputDSN string, config *DBConfig, connectionType string) (db *sql.DB, err error) {
	dsn, err := newDSN(inputDSN, config)
	if err != nil {
		return nil, err
	}
	defer func() {
		if db != nil && err == nil { // successful connection
			// There are many different ways we create a DB connection.
			// Ensure we change conn settings in all code paths.
			db.SetMaxOpenConns(config.MaxOpenConnections)
			db.SetConnMaxLifetime(maxConnLifetime)
			db.SetMaxIdleConns(maxIdleConns)
		}
	}()
	// For PREFERRED mode, implement fallback behavior
	if config.TLSMode == "PREFERRED" {
		// First try with TLS
		db, err := sql.Open("mysql", dsn)
		if err == nil {
			if err := db.Ping(); err == nil {
				// TLS connection successful
				return db, nil
			}
			_ = db.Close()
		}

		// TLS failed, try without TLS by explicitly removing TLS parameters
		configCopy := *config
		configCopy.TLSMode = "DISABLED"

		// For fallback, we need to strip any existing TLS parameters from the DSN
		// because the TLS parameter preservation logic will otherwise keep them
		fallbackDSN, err := createFallbackDSN(inputDSN)
		if err != nil {
			return nil, fmt.Errorf("failed to create fallback DSN for %s connection: %w", connectionType, err)
		}

		db, err = sql.Open("mysql", fallbackDSN)
		if err != nil {
			return nil, fmt.Errorf("failed to open fallback %s connection: %w", connectionType, err)
		}
		if err := db.Ping(); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("[%s-CONNECTION-FALLBACK] ping failed: %w", strings.ToUpper(strings.ReplaceAll(connectionType, " ", "-")), err)
		}
		return db, nil
	}

	// For all other modes, use standard connection
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open %s connection: %w", connectionType, err)
	}
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
func EnhanceDSNWithTLS(inputDSN string, config *DBConfig) (string, error) {
	if config == nil || config.TLSMode == "DISABLED" {
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

	// If DSN already has TLS configuration, respect it
	if cfg.TLSConfig != "" {
		return inputDSN, nil
	}

	// Check if DSN already has tls parameter in query string (case insensitive)
	lowerDSN := strings.ToLower(inputDSN)
	if strings.Contains(lowerDSN, "tls=") {
		return inputDSN, nil
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

	// Initialize TLS configurations if needed
	var tlsParam string
	switch strings.ToUpper(config.TLSMode) {
	case "DISABLED":
		return dsn, nil // No TLS needed
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

	// Add TLS parameter to DSN
	separator := "?"
	if strings.Contains(dsn, "?") {
		separator = "&"
	}
	return fmt.Sprintf("%s%stls=%s", dsn, separator, url.QueryEscape(tlsParam)), nil
}

// createFallbackDSN creates a DSN with all TLS parameters removed for PREFERRED mode fallback
func createFallbackDSN(inputDSN string) (string, error) {
	cfg, err := mysql.ParseDSN(inputDSN)
	if err != nil {
		// Return original DSN for graceful degradation when parsing fails
		return inputDSN, nil //nolint:nilerr // Intentional graceful degradation
	}

	// Remove TLS configuration - both from TLSConfig field and params
	cfg.TLSConfig = ""

	// Remove tls parameter from params if present (case-insensitive)
	if cfg.Params != nil {
		// Remove 'tls' parameter in any case variation
		for key := range cfg.Params {
			if strings.ToLower(key) == "tls" {
				delete(cfg.Params, key)
			}
		}
	}

	// Format the DSN back without TLS parameters
	return cfg.FormatDSN(), nil
}
