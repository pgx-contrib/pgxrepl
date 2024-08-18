package pgxrepl

import (
	"fmt"
	"reflect"

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

	switch m := payload.(type) {
	case *pglogrepl.RelationMessageV2:
		x.relations[m.RelationID] = m
	case *pglogrepl.BeginMessage:
		// begin transaction
	case *pglogrepl.CommitMessage:
		// commit transaction
	case *pglogrepl.InsertMessageV2:
		// create a relation
		relation, err := x.relation(m.RelationID)
		if err != nil {
			return err
		}
		// arguments
		operation := InsertOperation{
			// set the table name
			TableName: pgx.Identifier{
				relation.Namespace,
				relation.RelationName,
			},
			// set the new row
			NewRow: &Row{
				metadata: m.Tuple,
				relation: relation,
				registry: x.registry,
			},
		}
		// handle the event
		if err := x.handler.Handle(operation); err != nil {
			return err
		}
	case *pglogrepl.UpdateMessageV2:
		// create a relation
		relation, err := x.relation(m.RelationID)
		if err != nil {
			return err
		}
		// arguments
		operation := UpdateOperation{
			// set the table name
			TableName: pgx.Identifier{
				relation.Namespace,
				relation.RelationName,
			},
			// set the new row
			NewRow: &Row{
				relation: relation,
				metadata: m.NewTuple,
				registry: x.registry,
			},
			// set the old row
			OldRow: &Row{
				relation: relation,
				metadata: m.OldTuple,
				registry: x.registry,
			},
		}
		// handle the event
		if err := x.handler.Handle(operation); err != nil {
			return err
		}
	case *pglogrepl.DeleteMessageV2:
		// create a relation
		relation, err := x.relation(m.RelationID)
		if err != nil {
			return err
		}
		// arguments
		operation := DeleteOperation{
			// set the table name
			TableName: pgx.Identifier{
				relation.Namespace,
				relation.RelationName,
			},
			// set the old row
			OldRow: &Row{
				relation: relation,
				metadata: m.OldTuple,
				registry: x.registry,
			},
		}
		// handle the event
		if err := x.handler.Handle(operation); err != nil {
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
		return fmt.Errorf("unknown message type in pgoutput stream: %T", m)
	}

	return nil
}

func (x *Collector) relation(id uint32) (*pglogrepl.RelationMessageV2, error) {
	// get the relation from the relation ID
	relation, ok := x.relations[id]
	if !ok {
		return nil, fmt.Errorf("relation %d not found", id)
	}

	return relation, nil
}

// Handler is an interface that represents the handler
type Handler interface {
	// Handle handles the operation
	Handle(any) error
}

// InsertOperation is a struct that represents the insert operation
type InsertOperation struct {
	// NewRow is a the actual collecatable row
	NewRow *Row
	// TableName is a string that represents the table
	TableName pgx.Identifier
}

// UpdateOperation is a struct that represents the update operation
type UpdateOperation struct {
	// NewRow is a the actual collecatable row
	NewRow *Row
	// OldRow is a the actual collecatable row
	OldRow *Row
	// Table is a string that represents the table
	TableName pgx.Identifier
}

// DeleteOperation is a struct that represents the delete operation
type DeleteOperation struct {
	// OldRow is a the actual collecatable row
	OldRow *Row
	// TableName is a string that represents the table
	TableName pgx.Identifier
}

var _ pgx.CollectableRow = &Row{}

// Row is a struct that represents the row
type Row struct {
	metadata *pglogrepl.TupleData
	relation *pglogrepl.RelationMessageV2
	registry *pgtype.Map
}

// FieldDescriptions implements pgx.CollectableRow.
func (r *Row) FieldDescriptions() []pgconn.FieldDescription {
	collection := []pgconn.FieldDescription{}

	for index := range r.metadata.Columns {
		column := r.relation.Columns[index]

		description := pgconn.FieldDescription{
			Name:         column.Name,
			DataTypeOID:  column.DataType,
			TypeModifier: column.TypeModifier,
			TableOID:     r.relation.RelationID,
		}

		collection = append(collection, description)
	}

	return collection
}

// RawValues implements pgx.CollectableRow.
func (r *Row) RawValues() [][]byte {
	collection := make([][]byte, 0)

	for _, column := range r.metadata.Columns {
		collection = append(collection, column.Data)
	}

	return collection
}

// Scan implements pgx.CollectableRow.
func (r *Row) Scan(values ...any) error {
	row, err := r.Values()
	if err != nil {
		return err
	}

	vcount := len(values)
	rcount := len(row)
	// check the number of columns
	if vcount != rcount {
		return fmt.Errorf("number of field descriptions must equal number of values, got %v and %v", vcount, rcount)
	}
	// copy the values
	for index := range values {
		source := reflect.ValueOf(row[index]).Elem()
		// set the values
		target := reflect.ValueOf(values[index]).Elem()
		target.Set(source)
	}

	return nil
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

	for index, column := range r.metadata.Columns {
		switch column.DataType {
		case 'n': // null
			collection = append(collection, nil)
		case 'u': // unchanged toast
			// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
		case 't': // text
			item, err := decode(r.registry, column.Data, r.relation.Columns[index].DataType)
			if err != nil {
				return nil, err
			}
			collection = append(collection, item)
		}
	}

	return collection, nil
}
