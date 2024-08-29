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
		operation := &InsertOperation{
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
		operation := &UpdateOperation{
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
		operation := &DeleteOperation{
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
	NewRow pgx.CollectableRow
	// TableName is a string that represents the table
	TableName pgx.Identifier
}

// UpdateOperation is a struct that represents the update operation
type UpdateOperation struct {
	// NewRow is a the actual collecatable row
	NewRow pgx.CollectableRow
	// OldRow is a the actual collecatable row
	OldRow pgx.CollectableRow
	// Table is a string that represents the table
	TableName pgx.Identifier
}

// DeleteOperation is a struct that represents the delete operation
type DeleteOperation struct {
	// OldRow is a the actual collecatable row
	OldRow pgx.CollectableRow
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

// Values implements pgx.CollectableRow.
func (r *Row) Values() ([]any, error) {
	values := make([]any, r.metadata.ColumnNum)
	// prepare the values
	if err := r.Scan(values...); err != nil {
		return nil, err
	}
	// done!
	return values, nil
}

// RawValues implements pgx.CollectableRow.
func (r *Row) RawValues() [][]byte {
	values := make([][]byte, r.metadata.ColumnNum)

	for index, column := range r.metadata.Columns {
		values[index] = column.Data
	}
	// done!
	return values
}

// FieldDescriptions implements pgx.CollectableRow.
func (r *Row) FieldDescriptions() []pgconn.FieldDescription {
	fields := []pgconn.FieldDescription{}

	for index := range r.metadata.Columns {
		column := r.relation.Columns[index]

		description := pgconn.FieldDescription{
			Name:         column.Name,
			DataTypeOID:  column.DataType,
			TypeModifier: column.TypeModifier,
			TableOID:     r.relation.RelationID,
			Format:       pgtype.TextFormatCode,
		}

		fields = append(fields, description)
	}

	return fields
}

// Scan implements pgx.CollectableRow.
func (r *Row) Scan(values ...any) error {
	return pgx.ScanRow(
		r.registry,
		r.FieldDescriptions(),
		r.RawValues(),
		values...,
	)
}
