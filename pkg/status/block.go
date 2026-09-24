package status

import (
	"fmt"
	"strings"
)

// blockIndent prefixes every row of a Block. The header keeps the log
// handler's own prefix (timestamp, level); the rows are indented under it so
// that a block reads as one report rather than as several unrelated lines.
const blockIndent = "  "

// Block is the multi-line report a runner returns from Status(): a header line
// followed by one indented row per subsystem (copier, applier, change feed,
// checkpoint).
//
// The single-line form it replaces had grown to twenty-odd space-separated
// fields, which is dense but not readable: a value that changes width shifts
// every field after it, so the eye cannot follow one number down a scrollback.
// Grouping by subsystem and padding the labels to a common width is what makes
// a spike in one field visible across ticks. See
// github.com/block/spirit/issues/329.
//
// Note that the whole block is one log record with newlines in it. Under the
// default slog handler (what the CLI uses) that prints as written. A handler
// that quotes the message — slog's TextHandler, or a JSON handler — will render
// the newlines escaped instead.
//
// The zero value is not useful; construct with NewBlock.
type Block struct {
	header string
	rows   []blockRow
}

type blockRow struct {
	label string
	text  string
}

// NewBlock starts a block with the given header line.
func NewBlock(format string, args ...any) *Block {
	return &Block{header: fmt.Sprintf(format, args...)}
}

// Row appends a labelled row of fields. A row whose text is empty is dropped,
// so a caller can pass a helper that reports nothing — a nil applier, a change
// feed that does not publish stats — without having to test for it first.
// Trailing spaces are trimmed for the same reason: they are what is left when
// such a helper contributes nothing to the end of a row.
func (b *Block) Row(label, format string, args ...any) *Block {
	text := strings.TrimRight(fmt.Sprintf(format, args...), " ")
	if text == "" {
		return b
	}
	b.rows = append(b.rows, blockRow{label: label, text: text})
	return b
}

// String renders the block, with the labels padded to a common width so every
// row's fields start in the same column.
func (b *Block) String() string {
	if len(b.rows) == 0 {
		return b.header
	}
	width := 0
	for _, r := range b.rows {
		width = max(width, len(r.label))
	}
	var sb strings.Builder
	sb.WriteString(b.header)
	for _, r := range b.rows {
		sb.WriteString("\n")
		sb.WriteString(blockIndent)
		sb.WriteString(r.label)
		sb.WriteString(strings.Repeat(" ", width-len(r.label)+1))
		sb.WriteString(r.text)
	}
	return sb.String()
}
