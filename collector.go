package pgxrepl

import (
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// Handler receives decoded replication events. Every method is called in
// WAL order on a single goroutine. Returning a non-nil error aborts the
// broker loop; the slot's confirmed LSN is only advanced after HandleCommit
// returns nil, so a failed event replays on restart.
type Handler interface {
	HandleBegin(*BeginOperation) error
	HandleCommit(*CommitOperation) error
	HandleInsert(*InsertOperation) error
	HandleUpdate(*UpdateOperation) error
	HandleDelete(*DeleteOperation) error
	HandleTruncate(*TruncateOperation) error
}

// BeginOperation marks the start of a transaction.
type BeginOperation struct {
	XID        uint32
	FinalLSN   pglogrepl.LSN
	CommitTime time.Time
}

// CommitOperation marks the end of a transaction. The broker advances the
// replication slot to TransactionEndLSN after the handler returns nil.
type CommitOperation struct {
	CommitLSN         pglogrepl.LSN
	TransactionEndLSN pglogrepl.LSN
	CommitTime        time.Time
}

// InsertOperation represents a single row insert.
type InsertOperation struct {
	TableName pgx.Identifier
	NewRow    pgx.CollectableRow
}

// UpdateOperation represents a single row update. OldRow is nil unless the
// table has REPLICA IDENTITY FULL or an explicit replica identity index.
type UpdateOperation struct {
	TableName pgx.Identifier
	NewRow    pgx.CollectableRow
	OldRow    pgx.CollectableRow
}

// DeleteOperation represents a single row delete. OldRow contains the key
// columns (or all columns under REPLICA IDENTITY FULL).
type DeleteOperation struct {
	TableName pgx.Identifier
	OldRow    pgx.CollectableRow
}

// TruncateOperation represents a TRUNCATE affecting one or more tables.
type TruncateOperation struct {
	TableNames      []pgx.Identifier
	Cascade         bool
	RestartIdentity bool
}

// Collector decodes pgoutput WAL payloads into handler calls.
type Collector struct {
	relations map[uint32]*pglogrepl.RelationMessage
	registry  *pgtype.Map
	handler   Handler
}

// Collect parses a WAL payload and dispatches to the handler.
func (x *Collector) Collect(data []byte) error {
	payload, err := pglogrepl.Parse(data)
	if err != nil {
		return err
	}

	switch m := payload.(type) {
	case *pglogrepl.RelationMessage:
		x.relations[m.RelationID] = m
	case *pglogrepl.BeginMessage:
		return x.handler.HandleBegin(&BeginOperation{
			XID:        m.Xid,
			FinalLSN:   m.FinalLSN,
			CommitTime: m.CommitTime,
		})
	case *pglogrepl.CommitMessage:
		return x.handler.HandleCommit(&CommitOperation{
			CommitLSN:         m.CommitLSN,
			TransactionEndLSN: m.TransactionEndLSN,
			CommitTime:        m.CommitTime,
		})
	case *pglogrepl.InsertMessage:
		relation, err := x.relation(m.RelationID)
		if err != nil {
			return err
		}
		return x.handler.HandleInsert(&InsertOperation{
			TableName: pgx.Identifier{relation.Namespace, relation.RelationName},
			NewRow:    x.row(relation, m.Tuple),
		})
	case *pglogrepl.UpdateMessage:
		relation, err := x.relation(m.RelationID)
		if err != nil {
			return err
		}
		return x.handler.HandleUpdate(&UpdateOperation{
			TableName: pgx.Identifier{relation.Namespace, relation.RelationName},
			NewRow:    x.row(relation, m.NewTuple),
			OldRow:    x.row(relation, m.OldTuple),
		})
	case *pglogrepl.DeleteMessage:
		relation, err := x.relation(m.RelationID)
		if err != nil {
			return err
		}
		return x.handler.HandleDelete(&DeleteOperation{
			TableName: pgx.Identifier{relation.Namespace, relation.RelationName},
			OldRow:    x.row(relation, m.OldTuple),
		})
	case *pglogrepl.TruncateMessage:
		names := make([]pgx.Identifier, 0, len(m.RelationIDs))
		for _, id := range m.RelationIDs {
			relation, err := x.relation(id)
			if err != nil {
				return err
			}
			names = append(names, pgx.Identifier{relation.Namespace, relation.RelationName})
		}
		return x.handler.HandleTruncate(&TruncateOperation{
			TableNames:      names,
			Cascade:         m.Option&pglogrepl.TruncateOptionCascade != 0,
			RestartIdentity: m.Option&pglogrepl.TruncateOptionRestartIdentity != 0,
		})
	case *pglogrepl.TypeMessage, *pglogrepl.OriginMessage:
		// Not exposed to the handler; see README non-goals.
	default:
		return fmt.Errorf("pgxrepl: unsupported pgoutput message: %T", m)
	}

	return nil
}

func (x *Collector) relation(id uint32) (*pglogrepl.RelationMessage, error) {
	relation, ok := x.relations[id]
	if !ok {
		return nil, fmt.Errorf("pgxrepl: relation %d not found", id)
	}
	return relation, nil
}

func (x *Collector) row(relation *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) *Row {
	if tuple == nil {
		return nil
	}
	return &Row{metadata: tuple, relation: relation, registry: x.registry}
}

var _ pgx.CollectableRow = &Row{}

// Row adapts a pgoutput TupleData to pgx.CollectableRow, so handlers can
// decode replicated values with pgx.RowTo / pgx.RowToStructByName.
type Row struct {
	metadata *pglogrepl.TupleData
	relation *pglogrepl.RelationMessage
	registry *pgtype.Map
}

// Values implements pgx.CollectableRow.
func (r *Row) Values() ([]any, error) {
	values := make([]any, r.metadata.ColumnNum)
	if err := r.Scan(values...); err != nil {
		return nil, err
	}
	return values, nil
}

// RawValues implements pgx.CollectableRow.
func (r *Row) RawValues() [][]byte {
	values := make([][]byte, r.metadata.ColumnNum)
	for index, column := range r.metadata.Columns {
		values[index] = column.Data
	}
	return values
}

// FieldDescriptions implements pgx.CollectableRow.
func (r *Row) FieldDescriptions() []pgconn.FieldDescription {
	fields := make([]pgconn.FieldDescription, 0, len(r.metadata.Columns))
	for index := range r.metadata.Columns {
		column := r.relation.Columns[index]
		fields = append(fields, pgconn.FieldDescription{
			Name:         column.Name,
			DataTypeOID:  column.DataType,
			TypeModifier: column.TypeModifier,
			TableOID:     r.relation.RelationID,
			Format:       pgtype.TextFormatCode,
		})
	}
	return fields
}

// Scan implements pgx.CollectableRow.
func (r *Row) Scan(values ...any) error {
	return pgx.ScanRow(r.registry, r.FieldDescriptions(), r.RawValues(), values...)
}
