package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJSONEqual(t *testing.T) {
	cases := []struct {
		name  string
		a, b  string
		equal bool
		valid bool
	}{
		{"identical", `{"x":1}`, `{"x":1}`, true, true},
		{"whitespace and key order", `{"x":1,"y":[1,2]}`, `{"y": [1, 2], "x": 1}`, true, true},
		{"a value differs", `{"x":1}`, `{"x":2}`, false, true},
		{"a key differs", `{"x":1}`, `{"y":1}`, false, true},
		{"an extra key", `{"x":1}`, `{"x":1,"y":1}`, false, true},
		{"array order matters", `[1,2]`, `[2,1]`, false, true},
		{"nested", `{"a":{"b":[{"c":null}]}}`, `{"a": {"b": [{"c": null}]}}`, true, true},
		{"types differ", `{"x":1}`, `{"x":"1"}`, false, true},
		{"bool against number", `true`, `1`, false, true},
		// Numbers, as MySQL stores them in a JSON value.
		{"integers past 2^53 compare exactly", `{"x":9007199254740992}`, `{"x":9007199254740993}`, false, true},
		{"equal large integers", `{"x":9007199254740993}`, `{"x": 9007199254740993}`, true, true},
		{"uint64 range is exact", `18446744073709551615`, `18446744073709551614`, false, true},
		{"negative integers", `-9223372036854775808`, `-9223372036854775807`, false, true},
		{"exponent against decimal", `{"z":1e2}`, `{"z": 100.0}`, true, true},
		{"integer against the same double", `{"y":1}`, `{"y": 1.0}`, true, true},
		{"integer past uint64 is a double", `{"w":12345678901234567890123}`, `{"w": 1.2345678901234568e22}`, true, true},
		{"doubles that differ", `1.5`, `1.25`, false, true},
		// An integer and a double compare exactly, not through a float64:
		// MySQL stores 9007199254740993 as an integer and 9007199254740992.0
		// as a double, and compares them unequal.
		{"integer against a nearby double", `9007199254740993`, `9007199254740992.0`, false, true},
		{"integer against the double it rounds to", `9007199254740993`, `9007199254740993.0`, false, true},
		{"integer against its exact double", `9007199254740992`, `9007199254740992.0`, true, true},
		{"integer against an exponent", `100`, `1e2`, true, true},
		{"negative zero against zero", `-0.0`, `0`, true, true},
		// Validity.
		{"invalid left", `{`, `{}`, false, false},
		{"invalid right", `{}`, `{"x":}`, false, false},
		{"trailing content", `{} x`, `{}`, false, false},
		{"both invalid", `nope`, `nope`, false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			equal, valid := JSONEqual(c.a, c.b)
			require.Equal(t, c.valid, valid, "valid")
			require.Equal(t, c.equal, equal, "equal")
			// Symmetric.
			equal, valid = JSONEqual(c.b, c.a)
			require.Equal(t, c.valid, valid, "valid (reversed)")
			require.Equal(t, c.equal, equal, "equal (reversed)")
		})
	}
}
