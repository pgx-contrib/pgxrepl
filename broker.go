package pgxrepl

import (
	"context"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

// Broker runs the replication loop. It starts streaming from Slot using
// Publication, decodes WAL payloads, and dispatches them to Handler.
//
// The slot's confirmed flush LSN advances only after Handler.HandleCommit
// returns nil, so a failed transaction replays on restart.
type Broker struct {
	// Conn is an open replication connection (opened with
	// "?replication=database" in the connection string).
	Conn *pgconn.PgConn
	// Handler receives decoded operations.
	Handler Handler
	// Slot is the logical replication slot name (see CreateSlot).
	Slot string
	// Publication is the publication name (see CreatePublication).
	Publication string
}

// Run blocks until ctx is canceled or a handler/connection error occurs.
func (x *Broker) Run(ctx context.Context) error {
	ack := &ackingHandler{Handler: x.Handler}

	collector := &Collector{
		handler:   ack,
		relations: map[uint32]*pglogrepl.RelationMessage{},
		registry:  pgtype.NewMap(),
	}

	connector := &Connector{conn: x.Conn}
	ack.connector = connector

	if err := connector.Start(ctx, x.Slot, x.Publication); err != nil {
		return err
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		if err := connector.Status(ctx); err != nil {
			return err
		}

		message, err := connector.Receive(ctx)
		switch {
		case ctx.Err() != nil:
			return ctx.Err()
		case pgconn.Timeout(err):
			continue
		case err != nil:
			return err
		}

		payload, ok := message.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		switch payload.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			info, err := pglogrepl.ParsePrimaryKeepaliveMessage(payload.Data[1:])
			if err != nil {
				return err
			}
			if info.ReplyRequested {
				connector.deadline = time.Time{}
			}
		case pglogrepl.XLogDataByteID:
			data, err := pglogrepl.ParseXLogData(payload.Data[1:])
			if err != nil {
				return err
			}
			if err = collector.Collect(data.WALData); err != nil {
				return err
			}
		}
	}
}

// ackingHandler wraps the user's Handler and advances the connector's
// confirmed LSN after each successful HandleCommit.
type ackingHandler struct {
	Handler
	connector *Connector
}

func (h *ackingHandler) HandleCommit(op *CommitOperation) error {
	if err := h.Handler.HandleCommit(op); err != nil {
		return err
	}
	h.connector.Ack(op.TransactionEndLSN)
	return nil
}
