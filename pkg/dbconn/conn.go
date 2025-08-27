package dbconn

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	_ "embed"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
)

const (
	rdsTLSConfigName = "rds"
	maxConnLifetime  = time.Minute * 3
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

func NewTLSConfig() *tls.Config {
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(rdsGlobalBundle)
	return &tls.Config{RootCAs: caCertPool}
}

func initRDSTLS() error {
	var err error
	once.Do(func() {
		err = mysql.RegisterTLSConfig(rdsTLSConfigName, NewTLSConfig())
	})
	return err
}

// newDSN returns a new DSN to be used to connect to MySQL.
// It accepts a DSN as input and appends TLS configuration
// if the host is an Amazon RDS hostname.
func newDSN(dsn string, config *DBConfig) (string, error) {
	var ops []string
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", err
	}
	if IsRDSHost(cfg.Addr) {
		if err = initRDSTLS(); err != nil {
			return "", err
		}
		ops = append(ops, fmt.Sprintf("%s=%s", "tls", url.QueryEscape(rdsTLSConfigName)))
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
