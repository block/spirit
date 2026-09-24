package change

import (
	"errors"
	"fmt"

	"github.com/go-mysql-org/go-mysql/replication"
)

type eventType int

const (
	eventTypeUnknown eventType = iota
	eventTypeDelete
	eventTypeInsert
	eventTypeUpdate
)

// parseEventType converts a replication.EventType to a custom eventType.
// This follows the logic of canal/sync.go:
// https://github.com/go-mysql-org/go-mysql/blob/ee9447d96b48783abb05ab76a12501e5f1161e47/canal/sync.go#L282-L294
// Except we no longer need to use canal directly.
func parseEventType(eventType replication.EventType) eventType {
	switch eventType { //nolint:exhaustive
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2, replication.MARIADB_WRITE_ROWS_COMPRESSED_EVENT_V1:
		return eventTypeInsert
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2, replication.MARIADB_DELETE_ROWS_COMPRESSED_EVENT_V1:
		return eventTypeDelete
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2, replication.MARIADB_UPDATE_ROWS_COMPRESSED_EVENT_V1:
		return eventTypeUpdate
	default:
		return eventTypeUnknown
	}
}

// errUnsupportedRowsEvent is the sentinel behind every
// unsupportedRowsEventError, so callers can match the class with
// errors.Is without parsing the message.
var errUnsupportedRowsEvent = errors.New("unsupported rows event type")

// unsupportedRowsEventError builds the fatal error for a rows event whose
// subtype parseEventType does not recognize. go-mysql parses more
// rows-event subtypes into *replication.RowsEvent than this package knows
// how to apply — PARTIAL_UPDATE_ROWS_EVENT (binlog_row_value_options=
// PARTIAL_JSON) is the one a running server can start emitting after
// preflight has already passed. Such an event carries real row changes, so
// both clients fail the stream on it rather than dropping the rows; see
// binlogClient.processRowsEvent.
//
// The message names the subtype both ways — go-mysql's string form and the
// raw header byte — because an event type new enough that go-mysql has no
// name for it renders as "Unknown", and the byte is then the only way to
// identify it from a log line.
func unsupportedRowsEventError(t replication.EventType) error {
	return fmt.Errorf("%w %s (0x%02x): its row changes cannot be applied", errUnsupportedRowsEvent, t, uint8(t))
}
