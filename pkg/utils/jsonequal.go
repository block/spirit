package utils

import (
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"strings"
)

// JSONEqual reports whether a and b are the same JSON document, and whether
// both are valid JSON at all. Object keys compare in any order, whitespace is
// ignored, and a number compares by the exact value MySQL stores for it in a
// JSON value: an integer literal that fits in an int64 or a uint64 is that
// integer, every other number is the nearest double (an integer literal too
// large for a uint64 included: 12345678901234567890123 is stored as
// 1.2345678901234568e22). So 9007199254740992 and 9007199254740993 are
// different documents, 1e2 and 100.0 are the same, as are 1 and 1.0, and the
// integer 9007199254740993 differs from the double 9007199254740992.0 — and
// from 9007199254740993.0, which is that same double. Decoding everything to
// float64 (encoding/json's default), or comparing an integer against a double
// as float64, folds integers past 2^53 together, which hides a change MySQL
// stores and reports.
func JSONEqual(a, b string) (equal, valid bool) {
	docA, okA := decodeJSON(a)
	docB, okB := decodeJSON(b)
	if !okA || !okB {
		return false, false
	}
	return jsonValueEqual(docA, docB), true
}

// decodeJSON decodes one complete JSON document, keeping numbers as their
// literal text. Trailing content makes the text invalid, as it does for
// json.Unmarshal.
func decodeJSON(text string) (any, bool) {
	dec := json.NewDecoder(strings.NewReader(text))
	dec.UseNumber()
	var doc any
	if err := dec.Decode(&doc); err != nil {
		return nil, false
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, false
	}
	return doc, true
}

func jsonValueEqual(a, b any) bool {
	switch x := a.(type) {
	case map[string]any:
		y, ok := b.(map[string]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for k, xv := range x {
			yv, ok := y[k]
			if !ok || !jsonValueEqual(xv, yv) {
				return false
			}
		}
		return true
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for i := range x {
			if !jsonValueEqual(x[i], y[i]) {
				return false
			}
		}
		return true
	case json.Number:
		y, ok := b.(json.Number)
		return ok && jsonNumberEqual(x, y)
	default:
		// string, bool, nil
		return a == b
	}
}

// jsonNumberEqual compares two JSON numbers by the exact values MySQL stores
// for them. Two numbers that cannot be read compare as text.
func jsonNumberEqual(x, y json.Number) bool {
	xv, okX := jsonNumberValue(x)
	yv, okY := jsonNumberValue(y)
	if !okX || !okY {
		return x == y
	}
	return xv.Cmp(yv) == 0
}

// jsonNumberValue returns the value MySQL stores for a JSON number, exactly:
// the integer itself for an integer literal within the int64 or uint64 range,
// the nearest double for every other number. Both fit a big.Float without
// loss, so an integer and a double compare at full precision rather than
// through a float64 that cannot hold every integer past 2^53.
func jsonNumberValue(n json.Number) (*big.Float, bool) {
	if i, ok := jsonInteger(n); ok {
		return new(big.Float).SetInt(i), true
	}
	f, err := n.Float64()
	if err != nil {
		return nil, false
	}
	return new(big.Float).SetFloat64(f), true
}

// jsonInteger returns the value of a JSON number that is an integer literal
// within the int64 or uint64 range, the numbers MySQL keeps exact.
func jsonInteger(n json.Number) (*big.Int, bool) {
	s := string(n)
	if strings.ContainsAny(s, ".eE") {
		return nil, false
	}
	i, ok := new(big.Int).SetString(s, 10)
	if !ok || (!i.IsInt64() && !i.IsUint64()) {
		return nil, false
	}
	return i, true
}
