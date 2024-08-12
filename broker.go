package pgxrepl

import (
	"context"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

// Broker is a struct that represents the replication
type Broker struct {
	// Conn is the connection to the database
	Conn *pgconn.PgConn
	// Name is the name of the replication
	Name string
}

// Serve serves for changes on the replication connection
func (x *Broker) Serve(ctx context.Context) error {
	// create the parser
	parser := &Parser{
		relations: map[uint32]*pglogrepl.RelationMessageV2{},
		types:     &pgtype.Map{},
		stream:    false,
	}

	// create the iterator
	iterator := &Iterator{
		conn: x.Conn,
	}

	// start the iterator
	if err := iterator.Start(ctx, x.Name); err != nil {
		return err
	}

	for {
		// report the status
		if err := iterator.Status(ctx); err != nil {
			return err
		}

		// receive the message
		message, err := iterator.Receive(ctx)
		// handle the error
		if err != nil {
			return err
		}

		// check if the message is a copy data
		payload, ok := message.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		switch payload.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			// parse the primary keepalive message
			info, err := pglogrepl.ParsePrimaryKeepaliveMessage(payload.Data[1:])
			if err != nil {
				return err
			}

			// update the state position
			if info.ServerWALEnd > iterator.position {
				iterator.position = info.ServerWALEnd
			}

			// reset the deadline
			if info.ReplyRequested {
				iterator.deadline = time.Time{}
			}
		case pglogrepl.XLogDataByteID:
			// parse the log data
			data, err := pglogrepl.ParseXLogData(payload.Data[1:])
			if err != nil {
				return err
			}

			// parse the data
			if err = parser.Parse(data.WALData); err != nil {
				return err
			}

			// update the state position
			if data.WALStart > iterator.position {
				iterator.position = data.WALStart
			}
		}
	}
}

// Close closes the replication
func (x *Broker) Close() error {
	return nil
}
