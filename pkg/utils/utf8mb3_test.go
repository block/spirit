package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidUTF8MB3(t *testing.T) {
	assert.True(t, ValidUTF8MB3(""))
	assert.True(t, ValidUTF8MB3("a\x00\x00"), "NUL is a valid character")
	assert.True(t, ValidUTF8MB3("é"), "a 2-byte character")
	assert.True(t, ValidUTF8MB3("￿"), "the last character in the Basic Multilingual Plane")
	assert.False(t, ValidUTF8MB3("😀"), "a 4-byte character is utf8mb4 only")
	assert.False(t, ValidUTF8MB3("\xff\x00\x00"), "not UTF-8")
	assert.False(t, ValidUTF8MB3("\xed\xa0\x80"), "an encoded surrogate is not valid UTF-8")
}
