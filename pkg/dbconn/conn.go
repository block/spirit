package dbconn

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	_ "embed"
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
	rdsTLSConfigName    = "rds"
	customTLSConfigName = "custom"
	maxConnLifetime     = time.Minute * 3
)

// rdsAddr matches Amazon RDS hostnames with optional :port suffix.
// It's used to automatically load the Amazon RDS CA and enable TLS
var (
	rdsAddr    = regexp.MustCompile(`rds\.amazonaws\.com(:\d+)?$`)
	once       sync.Once
	customOnce sync.Once
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
	case "PREFERRED", "REQUIRED":
		// Encryption only - no certificate verification
		return &tls.Config{
			RootCAs:            caCertPool,
			InsecureSkipVerify: true,
		}
	case "VERIFY_CA":
		// Verify certificate against CA, but allow hostname mismatches
		return &tls.Config{
			RootCAs:            caCertPool,
			InsecureSkipVerify: false,
			VerifyPeerCertificate: func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
				// Custom verification that skips hostname checking
				// but validates the certificate chain
				if len(verifiedChains) > 0 {
					return nil // Certificate chain is valid
				}
				return fmt.Errorf("certificate verification failed")
			},
			ServerName: "", // Don't set ServerName to skip hostname verification
		}
	case "VERIFY_IDENTITY":
		// Full verification including hostname
		return &tls.Config{
			RootCAs:            caCertPool,
			InsecureSkipVerify: false,
		}
	default:
		// Default to full verification for unknown modes
		return &tls.Config{
			RootCAs:            caCertPool,
			InsecureSkipVerify: false,
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
	var err error
	customOnce.Do(func() {
		var certData []byte

		if config.TLSCertificatePath != "" {
			certData, err = LoadCertificateFromFile(config.TLSCertificatePath)
			if err != nil {
				return
			}
		} else {
			// Use embedded RDS bundle as fallback
			certData = rdsGlobalBundle
		}

		tlsConfig := NewCustomTLSConfig(certData, config.TLSMode)
		if tlsConfig != nil {
			err = mysql.RegisterTLSConfig(customTLSConfigName, tlsConfig)
		}
	})
	return err
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
			ops = append(ops, fmt.Sprintf("%s=%s", "tls", url.QueryEscape(customTLSConfigName)))
		}
		// For PREFERRED mode, if no custom cert and not RDS, we don't force TLS

	case "REQUIRED", "VERIFY_CA", "VERIFY_IDENTITY":
		// TLS is required - determine which certificate to use
		if config.TLSCertificatePath != "" {
			// Use custom certificate
			if err = initCustomTLS(config); err != nil {
				return "", err
			}
			ops = append(ops, fmt.Sprintf("%s=%s", "tls", url.QueryEscape(customTLSConfigName)))
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
			ops = append(ops, fmt.Sprintf("%s=%s", "tls", url.QueryEscape(customTLSConfigName)))
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
	dsn = fmt.Sprintf("%s?%s", dsn, strings.Join(ops, "&"))
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
