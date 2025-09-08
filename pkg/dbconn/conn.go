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

	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
)

const (
	rdsTLSConfigName      = "rds"
	customTLSConfigName   = "custom"
	requiredTLSConfigName = "required"
	verifyCATLSConfigName = "verify_ca"
	verifyIDTLSConfigName = "verify_identity"
	maxConnLifetime       = time.Minute * 3
)

// rdsAddr matches Amazon RDS hostnames with optional :port suffix.
// It's used to automatically load the Amazon RDS CA and enable TLS
var (
	rdsAddr = regexp.MustCompile(`rds\.amazonaws\.com(:\d+)?$`)
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

	switch sslMode {
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
	switch mode {
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

	// Determine TLS configuration strategy based on SSL mode
	switch config.TLSMode {
	case "DISABLED":
		// No TLS - don't add any TLS parameters

	case "PREFERRED":
		// Try to establish TLS if server supports it
		// For RDS hosts, always use TLS. For others, attempt TLS gracefully
		if IsRDSHost(cfg.Addr) {
			if err = initRDSTLS(); err != nil {
				return "", err
			}
			ops = append(ops, fmt.Sprintf("%s=%s", "tls", url.QueryEscape(rdsTLSConfigName)))
		} else if config.TLSCertificatePath != "" {
			// Custom certificate provided - use it
			if err = initCustomTLS(config); err != nil {
				return "", err
			}
			configName := getTLSConfigName(config.TLSMode)
			ops = append(ops, fmt.Sprintf("%s=%s", "tls", url.QueryEscape(configName)))
		} else {
			// For non-RDS hosts without custom cert, use permissive TLS (encryption only)
			if err = initCustomTLS(config); err != nil {
				return "", err
			}
			configName := getTLSConfigName(config.TLSMode)
			ops = append(ops, fmt.Sprintf("%s=%s", "tls", url.QueryEscape(configName)))
		}
		// PREFERRED mode always attempts TLS but doesn't fail if unavailable

	case "REQUIRED", "VERIFY_CA", "VERIFY_IDENTITY":
		// TLS is required - determine which certificate to use
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

	default:
		// Unknown mode - default to PREFERRED behavior
		if IsRDSHost(cfg.Addr) {
			if err = initRDSTLS(); err != nil {
				return "", err
			}
			ops = append(ops, fmt.Sprintf("%s=%s", "tls", url.QueryEscape(rdsTLSConfigName)))
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
func New(inputDSN string, config *DBConfig) (*sql.DB, error) {
	dsn, err := newDSN(inputDSN, config)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		utils.ErrInErr(db.Close())
		return nil, err
	}
	db.SetMaxOpenConns(config.MaxOpenConnections)
	db.SetConnMaxLifetime(maxConnLifetime)
	return db, nil
}
