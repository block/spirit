package statement

import "strings"

func init() { registerNormalizer(encryptionNormalizer{}) }

// encryptionNormalizer canonicalizes the case of the table ENCRYPTION option.
// MySQL keeps the case the CREATE TABLE used (ENCRYPTION='n' is stored and
// printed as 'n') but reads 'Y' and 'N' in either case, so the value is
// upper-cased to compare equal.
//
// An explicit ENCRYPTION='N' is kept distinct from an absent option. What an
// omitted option means depends on the server (default_table_encryption) and on
// the schema (DEFAULT ENCRYPTION), neither of which a parsed CREATE TABLE
// knows: with encryption off by default SHOW CREATE TABLE prints no ENCRYPTION
// for an unencrypted table, but with it on by default a table that omits the
// option is encrypted and one that opts out is printed as ENCRYPTION='N'.
// Erasing 'N' here would make a schema that omits the option indistinguishable
// from one that asks for decryption, so diffTableOptions decides what to emit
// from the declared value instead: an omitted option is never changed, 'Y'
// encrypts, and 'N' decrypts a table that is encrypted. A schema that wants a
// table decrypted must say ENCRYPTION='N'.
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
		ct.TableOptions.Encryption = new("N")
	}
	if *ct.TableOptions == (TableOptions{}) {
		ct.TableOptions = nil
	}
	return ct
}
