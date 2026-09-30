package statement

import (
	"unicode/utf8"

	"github.com/block/spirit/pkg/parser/charset"
	"github.com/block/spirit/pkg/utils"
)

func init() { registerNormalizer(charBinaryLiteralDefaultNormalizer{}) }

// charBinaryLiteralDefaultNormalizer rewrites a hex or bit literal DEFAULT on
// a char or varchar column to the string MySQL stores: the literal's bytes
// read as characters in the column's charset. SHOW CREATE TABLE reports that
// string, so `b varchar(4) DEFAULT 0x61` comes back as
// `b varchar(4) DEFAULT 'a'`. Without the rule the literal diffs against the
// live column and emits a MODIFY COLUMN that MySQL stores as the string again,
// so the next diff emits it again. Verified against MySQL 8.0.43:
//
//	char(4) DEFAULT x'61' / 0x61 / b'01100001'      -> 'a'
//	varchar(4) DEFAULT x'' / b''                     -> ''
//	char(4) DEFAULT x'c3a9'                          -> 'é'
//	char(4) DEFAULT x'27'                            -> ''''  (a quote)
//	char(4) DEFAULT x'f09f9880'                      -> 0xF09F9880  (not utf8mb3)
//	char(4) DEFAULT x'ff'                            -> error 1067  (not valid utf8mb4)
//	char(1) DEFAULT x'6162'                          -> error 1067  (longer than the width)
//	char(4) CHARACTER SET ascii DEFAULT x'80'        -> error 1067
//	char(4) CHARACTER SET latin1 DEFAULT x'e9'       -> 'é'  (the latin1 byte, transcoded)
//	char(4) CHARACTER SET utf16 DEFAULT x'61'        -> 'a'  (left-padded to a whole character)
//
// The bytes are only read where the rule can reproduce what MySQL reports:
//
//   - utf8mb4 and utf8mb3, when the bytes are valid utf8mb3. SHOW CREATE TABLE
//     reports a default that is not valid utf8mb3 as a hex literal, as it does
//     on binary (see [binaryDefaultBytesNormalizer]), and the written literal
//     already matches that. Bytes that are not valid in the charset at all
//     are rejected by MySQL.
//   - the single-byte and multi-byte charsets in [asciiCompatibleCharsets],
//     when every byte is ASCII. Those bytes are the same characters in each of
//     them. A byte above 0x7F is reported transcoded to the connection's
//     charset (latin1 x'e9' comes back as 'é'), which this rule does not
//     reproduce.
//
// Every other charset is left alone. utf16 and the other wide charsets pad and
// decode the bytes their own way, and swe7 maps some ASCII bytes to other
// characters (x'5b' is 'Ä'). So is a column whose charset the statement does
// not determine: it takes the database's default charset, which can be any of
// those (on a utf16 database, char(2) DEFAULT x'6162' stores the one character
// U+6162). A value longer than the column's width, which MySQL rejects, and an
// expression default, which MySQL stores as written, are left alone too.
//
// The rule reads the column's type through [storedColumnType], so a char or
// varchar column whose charset resolves to binary is left to
// [binaryDefaultBytesNormalizer], whether or not binaryCharsetNormalizer has
// rewritten its type yet.
type charBinaryLiteralDefaultNormalizer struct{}

func (charBinaryLiteralDefaultNormalizer) Name() string { return "char-binary-literal-default" }

func (charBinaryLiteralDefaultNormalizer) Normalize(ct *CreateTable) *CreateTable {
	for i := range ct.Columns {
		c := &ct.Columns[i]
		if c.Default == nil || c.DefaultIsExpr || c.Length == nil {
			continue
		}
		switch storedColumnType(c, ct) {
		case "char", "varchar":
		default:
			continue
		}
		value, ok := binaryLiteralBytes(c)
		if !ok || !readableInCharset(value, c, ct) || utf8.RuneCountInString(value) > *c.Length {
			continue
		}
		c.Default, c.DefaultKind = &value, DefaultKindString
	}
	return ct
}

// readableInCharset reports whether bytes form a string that SHOW CREATE TABLE
// reports byte for byte as a string default on this column. See
// [charBinaryLiteralDefaultNormalizer] for the charsets it accepts and why.
func readableInCharset(value string, c *Column, ct *CreateTable) bool {
	cs, _ := resolvedCharsetCollation(c, ct)
	cs = NormalizeCharsetName(cs)
	if cs == charset.CharsetUTF8MB4 || cs == charset.CharsetUTF8MB3 {
		return utils.ValidUTF8MB3(value)
	}
	if !asciiCompatibleCharsets[cs] {
		return false // including "", a charset the statement does not determine
	}
	for i := range len(value) {
		if value[i] >= utf8.RuneSelf {
			return false
		}
	}
	return true
}

// asciiCompatibleCharsets are the charsets in which each byte from 0x00 to 0x7F
// is the ASCII character of the same code, so SHOW CREATE TABLE reports a
// default made of those bytes unchanged. Each was measured against MySQL
// 8.0.43 with every byte in that range. A charset missing from the list
// is one the bytes decode differently in (swe7, and the wide charsets ucs2,
// utf16, utf16le and utf32), or one handled separately (utf8mb4, utf8mb3,
// binary).
var asciiCompatibleCharsets = map[string]bool{
	"armscii8": true, "ascii": true, "big5": true, "cp1250": true, "cp1251": true,
	"cp1256": true, "cp1257": true, "cp850": true, "cp852": true, "cp866": true,
	"cp932": true, "dec8": true, "eucjpms": true, "euckr": true, "gb18030": true,
	"gb2312": true, "gbk": true, "geostd8": true, "greek": true, "hebrew": true,
	"hp8": true, "keybcs2": true, "koi8r": true, "koi8u": true, "latin1": true,
	"latin2": true, "latin5": true, "latin7": true, "macce": true, "macroman": true,
	"sjis": true, "tis620": true, "ujis": true,
}
