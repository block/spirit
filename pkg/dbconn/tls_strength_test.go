package dbconn

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/pem"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/stretchr/testify/require"
)

// The tests in this file assert how strongly a TLS configuration verifies the
// server, rather than that it has the expected fields set. The distinction
// matters because every weakening in this package is shape-preserving: a
// VERIFY_CA config that verifies nothing has the same struct layout as one that
// verifies correctly, and a skip-verify config registered under the
// "verify_identity" name still produces a DSN that connects. Shape assertions
// pass through all of it.

// newTestCA returns a self-signed CA and a leaf certificate it issued for
// commonName, plus the CA's PEM encoding. The leaf's DNS name is deliberately
// something no test connects to: VERIFY_CA's whole purpose is accepting a valid
// chain whose hostname does not match, so the hostname must be wrong for the
// test to mean anything.
func newTestCA(t *testing.T, commonName string) (caPEM []byte, leafDER []byte) {
	t.Helper()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: commonName + " Test Root CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	caCert, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: commonName + " leaf"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"not-the-host-under-test.example"},
	}
	leafDER, err = x509.CreateCertificate(rand.Reader, leafTmpl, caCert, &leafKey.PublicKey, caKey)
	require.NoError(t, err)

	caPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	return caPEM, leafDER
}

// TestVerifyCAActuallyVerifiesTheChain exercises VERIFY_CA's
// VerifyPeerCertificate callback against real certificates.
//
// VERIFY_CA is implemented as InsecureSkipVerify plus that callback, so the
// callback is the only thing verifying anything. Replacing its body with
// `return nil` leaves the struct identical and degrades the mode to no
// verification at all — strictly weaker than PREFERRED, under a name that
// promises the opposite. Asserting RootCAs != nil cannot see that; calling the
// callback can.
func TestVerifyCAActuallyVerifiesTheChain(t *testing.T) {
	trustedCAPEM, trustedLeaf := newTestCA(t, "Trusted")
	_, untrustedLeaf := newTestCA(t, "Untrusted")

	cfg := NewCustomTLSConfig(trustedCAPEM, "VERIFY_CA")
	require.NotNil(t, cfg)
	require.NotNil(t, cfg.VerifyPeerCertificate,
		"VERIFY_CA has no verification callback, so it verifies nothing")

	t.Run("accepts a chain from the configured CA", func(t *testing.T) {
		// The leaf's DNS name does not match anything: VERIFY_CA is defined as
		// chain verification without hostname verification, so this must pass.
		require.NoError(t, cfg.VerifyPeerCertificate([][]byte{trustedLeaf}, nil),
			"a certificate issued by the configured CA was rejected")
	})

	t.Run("rejects a chain from an unconfigured CA", func(t *testing.T) {
		err := cfg.VerifyPeerCertificate([][]byte{untrustedLeaf}, nil)
		require.Error(t, err,
			"a certificate from an unknown CA was accepted; VERIFY_CA is verifying nothing")
		require.Contains(t, err.Error(), "certificate verification failed")
	})

	t.Run("rejects an empty chain", func(t *testing.T) {
		require.Error(t, cfg.VerifyPeerCertificate(nil, nil),
			"a server presenting no certificate was accepted")
	})

	t.Run("rejects an unparseable certificate", func(t *testing.T) {
		require.Error(t, cfg.VerifyPeerCertificate([][]byte{[]byte("not a certificate")}, nil))
	})
}

// TestVerifyCARejectsRDSChainWhenPrivateCAConfigured is the cross-check on the
// empty-certData fallback. A caller who names their own CA must not also
// inherit Amazon's roots, or "verify against my CA" would silently accept any
// RDS-issued certificate.
func TestVerifyCARejectsRDSChainWhenPrivateCAConfigured(t *testing.T) {
	privateCAPEM, _ := newTestCA(t, "Private")
	_, otherLeaf := newTestCA(t, "Other")

	cfg := NewCustomTLSConfig(privateCAPEM, "VERIFY_CA")
	require.NotNil(t, cfg)

	// The private pool must not be the driver's RDS pool.
	rdsRoots := mysql.RDSTLSConfig().RootCAs
	require.NotSame(t, rdsRoots, cfg.RootCAs,
		"a configured private CA shares the pool object with the RDS roots")
	require.Error(t, cfg.VerifyPeerCertificate([][]byte{otherLeaf}, nil))
}

// TestStrictModesVerifyServerCertificate pins the verification strength of the
// modes whose names promise it. Deleting ServerName is already caught
// elsewhere; flipping InsecureSkipVerify was not, and it is the field that
// decides whether verification happens at all.
func TestStrictModesVerifyServerCertificate(t *testing.T) {
	t.Run("NewCustomTLSConfig VERIFY_IDENTITY", func(t *testing.T) {
		cfg := NewCustomTLSConfig(nil, "VERIFY_IDENTITY")
		require.NotNil(t, cfg)
		require.False(t, cfg.InsecureSkipVerify,
			"VERIFY_IDENTITY skips certificate verification")
		require.NotNil(t, cfg.RootCAs, "VERIFY_IDENTITY has no roots to verify against")
	})

	t.Run("NewTLSConfig", func(t *testing.T) {
		cfg := NewTLSConfig()
		require.NotNil(t, cfg)
		require.False(t, cfg.InsecureSkipVerify,
			"the RDS TLS config skips certificate verification")
		require.NotNil(t, cfg.RootCAs)
	})

	// The binlog path is the stated reason NewTLSConfig survives the
	// retirement, and it is a separate code path from the database/sql pool: a
	// weakening here drops verification on the replication stream while the
	// pool stays correct, so nothing looks wrong.
	t.Run("GetTLSConfigForBinlog", func(t *testing.T) {
		const rdsHost = "db.cluster-abc.us-east-1.rds.amazonaws.com"
		for _, mode := range []string{"REQUIRED", "VERIFY_IDENTITY"} {
			t.Run(mode, func(t *testing.T) {
				cfg, err := GetTLSConfigForBinlog(&DBConfig{TLSMode: mode}, rdsHost)
				require.NoError(t, err)
				require.NotNil(t, cfg, "%s produced no TLS config for an RDS host", mode)
				require.NotNil(t, cfg.RootCAs, "%s has no roots", mode)
				require.Equal(t, rdsHost, cfg.ServerName)
				if mode == "VERIFY_IDENTITY" {
					require.False(t, cfg.InsecureSkipVerify,
						"VERIFY_IDENTITY skips verification on the binlog stream")
				}
			})
		}
	})
}

// TestRegisteredTLSConfigStrengthMatchesItsName closes the gap where
// initCustomTLS registers a config under a mode's name without that config
// actually implementing the mode. The registry is a driver global with no
// exported getter, so the config is read back the way a connection would: by
// parsing a DSN that names it, which is exactly what resolves the name to a
// *tls.Config.
func TestRegisteredTLSConfigStrengthMatchesItsName(t *testing.T) {
	require.NoError(t, initCustomTLS(&DBConfig{TLSMode: "VERIFY_IDENTITY"}))

	cfg, err := mysql.ParseDSN("u:p@tcp(127.0.0.1:3306)/db?tls=" + verifyIDTLSConfigName)
	require.NoError(t, err, "the config registered under %q is not resolvable", verifyIDTLSConfigName)
	require.NotNil(t, cfg.TLS)
	require.False(t, cfg.TLS.InsecureSkipVerify,
		"the config registered as %q skips verification", verifyIDTLSConfigName)
	require.NotNil(t, cfg.TLS.RootCAs)

	// VERIFY_CA is registered under its own name and must carry the callback
	// that does its verifying — a skip-verify config with no callback would
	// resolve here just as happily.
	require.NoError(t, initCustomTLS(&DBConfig{TLSMode: "VERIFY_CA"}))
	caCfg, err := mysql.ParseDSN("u:p@tcp(127.0.0.1:3306)/db?tls=" + verifyCATLSConfigName)
	require.NoError(t, err)
	require.NotNil(t, caCfg.TLS)
	require.NotNil(t, caCfg.TLS.VerifyPeerCertificate,
		"the config registered as %q has no verification callback, so it verifies nothing",
		verifyCATLSConfigName)
}

// TestEmptyCertificateFileIsAnError separates "named no CA file" from "named a
// CA file that is empty". The first is a deliberate request for the RDS roots;
// the second is an operator pointing at a truncated or unpopulated private CA,
// who would otherwise silently verify against Amazon's roots on a connection
// that succeeds.
func TestEmptyCertificateFileIsAnError(t *testing.T) {
	emptyPath := t.TempDir() + "/empty.pem"
	require.NoError(t, os.WriteFile(emptyPath, nil, 0o600))

	err := initCustomTLS(&DBConfig{TLSMode: "VERIFY_IDENTITY", TLSCertificatePath: emptyPath})
	require.Error(t, err, "an empty CA file fell back to the RDS roots silently")
	require.Contains(t, err.Error(), "is empty")

	// The no-path case still gets the RDS roots, so the check above narrowed
	// nothing it should not have.
	require.NoError(t, initCustomTLS(&DBConfig{TLSMode: "VERIFY_IDENTITY"}))
	require.NotNil(t, NewCustomTLSConfig(nil, "VERIFY_IDENTITY").RootCAs)
}

// TestRDSRootPoolCarriesAKnownAmazonRoot replaces the per-certificate
// validation the deleted bundle's test used to do. Spirit no longer owns the
// bundle, so what matters now is that a future block/mysql bump cannot hand
// over a pool that is non-empty but not the RDS trust store — which would
// surface in production as an "unknown authority" error naming nothing in this
// repository.
func TestRDSRootPoolCarriesAKnownAmazonRoot(t *testing.T) {
	pool := NewTLSConfig().RootCAs
	require.NotNil(t, pool)

	subjects := pool.Subjects() //nolint:staticcheck // SA1019: no other way to enumerate a pool we did not build
	require.NotEmpty(t, subjects, "the RDS root pool is empty")

	var found bool
	for _, rawSubject := range subjects {
		var rdn pkix.RDNSequence
		if _, err := asn1.Unmarshal(rawSubject, &rdn); err != nil {
			continue
		}
		var name pkix.Name
		name.FillFromRDNSequence(&rdn)
		if name.CommonName == "Amazon RDS Root 2019 CA" {
			found = true
			break
		}
	}
	require.True(t, found,
		"the RDS root pool does not contain \"Amazon RDS Root 2019 CA\"; the bundle "+
			"the driver supplies is not the RDS trust store")
}

// TestDisabledModeProducesNoTLSFromEitherDSNProducer pins that both DSN
// producers agree. newDSN was fixed first; EnhanceDSNWithTLS is the exported
// one, and it is the one whose result a consumer opens themselves.
func TestDisabledModeProducesNoTLSFromEitherDSNProducer(t *testing.T) {
	const rdsDSN = "u:p@tcp(db.cluster-abc.us-east-1.rds.amazonaws.com:3306)/app"

	enhanced, err := EnhanceDSNWithTLS(rdsDSN, &DBConfig{TLSMode: "DISABLED"})
	require.NoError(t, err)
	requireNoEffectiveTLS(t, enhanced, "EnhanceDSNWithTLS with TLSMode=DISABLED on an RDS host")

	// Lowercase too, since the mode is documented as case-insensitive.
	enhanced, err = EnhanceDSNWithTLS(rdsDSN, &DBConfig{TLSMode: "disabled"})
	require.NoError(t, err)
	requireNoEffectiveTLS(t, enhanced, "EnhanceDSNWithTLS with TLSMode=disabled on an RDS host")

	// An explicit tls= in the DSN still outranks the mode: DISABLED fills in a
	// missing setting, it does not overwrite a stated one.
	explicit := rdsDSN + "?tls=skip-verify"
	out, err := EnhanceDSNWithTLS(explicit, &DBConfig{TLSMode: "DISABLED"})
	require.NoError(t, err)
	cfg, err := mysql.ParseDSN(out)
	require.NoError(t, err)
	require.NotNil(t, cfg.TLS, "DISABLED overwrote the DSN's explicit tls= setting")

	// A nil config means "nothing was said about TLS" and must stay a no-op,
	// so the change above did not quietly become a global default.
	out, err = EnhanceDSNWithTLS(rdsDSN, nil)
	require.NoError(t, err)
	require.Equal(t, rdsDSN, out, "a nil config modified the DSN")
}
