package statement

import "strings"

func init() { registerNormalizer(encryptionNormalizer{}) }

// encryptionNormalizer rewrites the table ENCRYPTION option to the form SHOW
// CREATE TABLE reports. MySQL keeps the case the CREATE TABLE used
// (ENCRYPTION='n' is stored and printed as 'n') but reads 'Y' and 'N' in either
// case, so the value is upper-cased to compare equal.
//
// ENCRYPTION='N' is then dropped. On a server whose default_table_encryption
// is OFF (the default), in a schema without DEFAULT ENCRYPTION='Y', SHOW CREATE
// TABLE prints no ENCRYPTION for an unencrypted table, whether it was created
// with ENCRYPTION='N', ENCRYPTION='n' or no option at all. Without the rule a
// schema that spells out ENCRYPTION='N' differs from its own live table on
// every run. After it, a parsed table carries Encryption only when it is
// encrypted ('Y'), and an absent option means unencrypted.
//
// That reading does not hold where encryption is the default (a schema with
// DEFAULT ENCRYPTION='Y', or default_table_encryption=ON). There a table that
// omits the option is encrypted, and SHOW CREATE TABLE prints ENCRYPTION='N'
// for one that opts out. A parsed CREATE TABLE knows neither default, so it
// cannot tell those two apart: both read as unencrypted.
//
// A value other than Y or N is left as written: MySQL rejects it (error 3184).
type encryptionNormalizer struct{}

func (encryptionNormalizer) Name() string { return "encryption" }

func (encryptionNormalizer) Normalize(ct *CreateTable) *CreateTable {
	if ct.TableOptions == nil || ct.TableOptions.Encryption == nil {
		return ct
	}
	switch strings.ToUpper(*ct.TableOptions.Encryption) {
	case "Y":
		ct.TableOptions.Encryption = new("Y")
	case "N":
		ct.TableOptions.Encryption = nil
	}
	if *ct.TableOptions == (TableOptions{}) {
		ct.TableOptions = nil
	}
	return ct
}
