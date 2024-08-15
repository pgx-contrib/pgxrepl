package pgxrepl

import (
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// Collector is a struct that represents
type Collector struct {
	relations map[uint32]*pglogrepl.RelationMessageV2
	registry  *pgtype.Map
	handler   Handler
	stream    bool
}

// Collect processes the data
func (x *Collector) Collect(data []byte) error {
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
		// Create a map to store the column row
		row, err := x.row(message.RelationID, message.Tuple)
		if err != nil {
			return err
		}

		// arguments
		args := InsertEventArgs{
			Table:  row.replation.RelationName,
			NewRow: row,
		}
		// handle the event
		if err := x.handler.Handle(args); err != nil {
			return err
		}
	case *pglogrepl.UpdateMessageV2:
		// create a map to store the column values
		oldRow, err := x.row(message.RelationID, message.OldTuple)
		if err != nil {
			return err
		}

		// create a map to store the column values
		newRow, err := x.row(message.RelationID, message.NewTuple)
		if err != nil {
			return err
		}

		// arguments
		args := UpdateEventArgs{
			Table:  newRow.replation.RelationName,
			NewRow: newRow,
			OldRow: oldRow,
		}
		// handle the event
		if err := x.handler.Handle(args); err != nil {
			return err
		}
	case *pglogrepl.DeleteMessageV2:
		// create a map to store the column values
		row, err := x.row(message.RelationID, message.OldTuple)
		if err != nil {
			return err
		}
		// arguments
		args := DeleteEventArgs{
			Table:  row.replation.RelationName,
			OldRow: row,
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

func (x *Collector) row(id uint32, data *pglogrepl.TupleData) (*Row, error) {
	// get the relation from the relation ID
	relation, ok := x.relations[id]
	if !ok {
		return nil, fmt.Errorf("relation %d not found", id)
	}

	row := &Row{
		replation: relation,
		registry:  x.registry,
		data:      data,
	}

	return row, nil
}

// Handler is an interface that represents the handler
type Handler interface {
	Handle(any) error
}

// InsertHandler is a struct that represents the insert handler
type InsertEventArgs struct {
	// NewRow is a map that represents the row
	NewRow pgx.CollectableRow
	// Table is a string that represents the table
	Table string
}

// UpdateHandler is a struct that represents the update handler
type UpdateEventArgs struct {
	// NewRow is a map that represents the new row
	NewRow pgx.CollectableRow
	// OldRow is a map that represents the old row
	OldRow pgx.CollectableRow
	// Table is a string that represents the table
	Table string
}

// DeleteHandler is a struct that represents the delete handler
type DeleteEventArgs struct {
	// OldRow is a map that represents the old row
	OldRow pgx.CollectableRow
	// Table is a string that represents the table
	Table string
}

var _ pgx.CollectableRow = &Row{}

// Row is a struct that represents the row
type Row struct {
	data      *pglogrepl.TupleData
	replation *pglogrepl.RelationMessageV2
	registry  *pgtype.Map
}

// FieldDescriptions implements pgx.CollectableRow.
func (r *Row) FieldDescriptions() []pgconn.FieldDescription {
	collection := []pgconn.FieldDescription{}

	for index := range r.data.Columns {
		column := r.replation.Columns[index]

		item := pgconn.FieldDescription{
			TableOID:     r.replation.RelationID,
			Name:         column.Name,
			DataTypeOID:  column.DataType,
			TypeModifier: column.TypeModifier,
		}

		collection = append(collection, item)
	}

	return collection
}

// RawValues implements pgx.CollectableRow.
func (r *Row) RawValues() [][]byte {
	collection := make([][]byte, 0)

	for _, column := range r.data.Columns {
		collection = append(collection, column.Data)
	}

	return collection
}

// Scan implements pgx.CollectableRow.
func (r *Row) Scan(dest ...any) error {
	panic("unimplemented")
}

// Values implements pgx.CollectableRow.
func (r *Row) Values() ([]any, error) {
	// Create a map to store the column collection
	collection := make([]any, 0)

	// decode is a function that decodes the data
	decode := func(mi *pgtype.Map, data []byte, dataType uint32) (any, error) {
		if dt, ok := mi.TypeForOID(dataType); ok {
			return dt.Codec.DecodeValue(mi, dataType, pgtype.TextFormatCode, data)
		}
		return string(data), nil
	}

	for index, column := range r.data.Columns {
		switch column.DataType {
		case 'n': // null
			collection = append(collection, nil)
		case 'u': // unchanged toast
			// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
		case 't': // text
			item, err := decode(r.registry, column.Data, r.replation.Columns[index].DataType)
			if err != nil {
				return nil, err
			}
			collection = append(collection, item)
		}
	}

	return collection, nil
}
