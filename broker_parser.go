package pgxrepl

import (
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

// Parser is a struct that represents
type Parser struct {
	relations map[uint32]*pglogrepl.RelationMessageV2
	types     *pgtype.Map
	handler   Handler
	stream    bool
}

// Parse processes the data
func (x *Parser) Parse(data []byte) error {
	payload, err := pglogrepl.ParseV2(data, x.stream)
	if err != nil {
		return err
	}

	switch message := payload.(type) {
	case *pglogrepl.RelationMessageV2:
		x.relations[message.RelationID] = message
	case *pglogrepl.BeginMessage:
		// begin transaction
	case *pglogrepl.CommitMessage:
		// commit transaction
	case *pglogrepl.InsertMessageV2:
		// Create a map to store the column current
		current, err := x.record(message.RelationID, message.Tuple)
		if err != nil {
			return err
		}

		// arguments
		args := InsertEventArgs{
			NewRow: current,
			Table:  "",
		}
		// handle the event
		if err := x.handler.Handle(args); err != nil {
			return err
		}
	case *pglogrepl.UpdateMessageV2:
		// create a map to store the column values
		previous, err := x.record(message.RelationID, message.OldTuple)
		if err != nil {
			return err
		}

		// create a map to store the column values
		current, err := x.record(message.RelationID, message.NewTuple)
		if err != nil {
			return err
		}

		// arguments
		args := UpdateEventArgs{
			NewRow: current,
			OldRow: previous,
			Table:  "",
		}
		// handle the event
		if err := x.handler.Handle(args); err != nil {
			return err
		}
	case *pglogrepl.DeleteMessageV2:
		// create a map to store the column values
		previous, err := x.record(message.RelationID, message.OldTuple)
		if err != nil {
			return err
		}
		// arguments
		args := DeleteEventArgs{
			OldRow: previous,
			Table:  "",
		}
		// handle the event
		if err := x.handler.Handle(args); err != nil {
			return err
		}
	case *pglogrepl.TruncateMessageV2:
		// not handled
	case *pglogrepl.TypeMessageV2:
		// not handled
	case *pglogrepl.OriginMessage:
		// not handled
	case *pglogrepl.LogicalDecodingMessageV2:
		// not handled
	case *pglogrepl.StreamStartMessageV2:
		x.stream = true
	case *pglogrepl.StreamStopMessageV2:
		x.stream = false
	case *pglogrepl.StreamCommitMessageV2:
		// not handled
	case *pglogrepl.StreamAbortMessageV2:
		// not handled
	default:
		return fmt.Errorf("unknown message type in pgoutput stream: %T", message)
	}

	return nil
}

func (x *Parser) record(id uint32, tuple *pglogrepl.TupleData) (map[string]any, error) {
	// get the relation from the relation ID
	relation, ok := x.relations[id]
	if !ok {
		return nil, fmt.Errorf("relation %d not found", id)
	}

	// Create a map to store the column data
	data := make(map[string]any)

	for index, column := range tuple.Columns {
		name := relation.Columns[index].Name
		switch column.DataType {
		case 'n': // null
			data[name] = nil
		case 'u': // unchanged toast
			// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
		case 't': // text
			value, err := x.decode(x.types, column.Data, relation.Columns[index].DataType)
			if err != nil {
				return nil, err
			}
			data[name] = value
		}
	}

	return data, nil
}

func (x *Parser) decode(mi *pgtype.Map, data []byte, dataType uint32) (any, error) {
	if dt, ok := mi.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(mi, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}
