package pgxrepl

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Connector wraps a replication connection: starts replication for a slot,
// sends periodic standby status updates, and receives backend messages.
type Connector struct {
	conn     *pgconn.PgConn
	deadline time.Time
	position pglogrepl.LSN
}

// Start begins streaming from the current server XLog position for the
// given slot, with pgoutput proto v1 and the given publication.
func (x *Connector) Start(ctx context.Context, slot, publication string) error {
	options := pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			"proto_version '1'",
			fmt.Sprintf("publication_names '%s'", publication),
		},
	}

	// Pass LSN 0 so Postgres streams from the slot's own restart_lsn.
	// For logical slots the server ignores any LSN before the slot's
	// confirmed flush position, and starting at 0 ensures we don't skip
	// past pending WAL after the slot was created.
	if err := pglogrepl.StartReplication(ctx, x.conn, slot, 0, options); err != nil {
		return err
	}

	return nil
}

// Status sends a StandbyStatusUpdate when the keepalive deadline passes.
func (x *Connector) Status(ctx context.Context) error {
	if time.Now().After(x.deadline) {
		output := pglogrepl.StandbyStatusUpdate{WALWritePosition: x.position}

		if err := pglogrepl.SendStandbyStatusUpdate(ctx, x.conn, output); err != nil {
			return err
		}

		x.deadline = time.Now().Add(10 * time.Second)
	}

	return nil
}

// Ack advances the position reported back to the primary. The next Status
// call will send it as the confirmed flush LSN.
func (x *Connector) Ack(lsn pglogrepl.LSN) {
	if lsn > x.position {
		x.position = lsn
	}
}

// Receive reads the next backend message, respecting the keepalive deadline.
func (x *Connector) Receive(ctx context.Context) (pgproto3.BackendMessage, error) {
	ctx, cancel := context.WithDeadline(ctx, x.deadline)
	defer cancel()

	message, err := x.conn.ReceiveMessage(ctx)
	if err != nil {
		return nil, err
	}

	if response, ok := message.(*pgproto3.ErrorResponse); ok {
		return nil, pgconn.ErrorResponseToPgError(response)
	}

	return message, nil
}
