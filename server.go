package pgxrepl

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Server is a struct that represents the replication
type Server struct {
	// State is the state of the replication
	State *State
	// Conn is the connection to the database
	Conn *pgconn.PgConn
	// Name is the name of the replication
	Name string
}

// Options returns the Options for the replication
func (x *Server) Options() pglogrepl.StartReplicationOptions {
	return pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			fmt.Sprintf("proto_version '%v'", 2),
			fmt.Sprintf("publication_names '%v'", x.Name),
			fmt.Sprintf("messages '%v'", true),
			fmt.Sprintf("streaming '%v'", true),
		},
	}
}

// KeepAlive reports the KeepAlive of the replication
func (x *Server) KeepAlive(ctx context.Context) error {
	if time.Now().After(x.State.Deadline) {
		output := pglogrepl.StandbyStatusUpdate{WALWritePosition: x.State.Position}

		if err := pglogrepl.SendStandbyStatusUpdate(ctx, x.Conn, output); err != nil {
			return err
		}

		x.State.Deadline = time.Now().Add(duration)
	}

	return nil
}

// Listen starts the replication
func (x *Server) Listen(ctx context.Context) error {
	sysident, err := pglogrepl.IdentifySystem(ctx, x.Conn)
	if err != nil {
		return err
	}

	// start the replication
	if err = pglogrepl.StartReplication(ctx, x.Conn, x.Name, sysident.XLogPos, x.Options()); err != nil {
		return err
	}

	// state
	x.State = &State{
		Position: sysident.XLogPos,
		Deadline: time.Now().Add(duration),
	}

	return nil
}

// Serve serves for changes on the replication connection
func (x *Server) Serve(ctx context.Context) error {
	parser := &Parser{}

	for {
		// report the status
		if err := x.KeepAlive(ctx); err != nil {
			return err
		}

		// create a new context with a deadline
		ctx, cancel := context.WithDeadline(ctx, x.State.Deadline)
		// receive the message
		message, err := x.Conn.ReceiveMessage(ctx)
		// cancel the context
		cancel()

		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}

			return err
		}

		// check if the message is an error response
		if response, ok := message.(*pgproto3.ErrorResponse); ok {
			return &Error{response: response}
		}

		// check if the message is a copy data
		envelope, ok := message.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		switch envelope.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			// parse the primary keepalive message
			info, err := pglogrepl.ParsePrimaryKeepaliveMessage(envelope.Data[1:])
			if err != nil {
				return err
			}

			// update the state position
			if info.ServerWALEnd > x.State.Position {
				x.State.Position = info.ServerWALEnd
			}

			// reset the deadline
			if info.ReplyRequested {
				x.State.Deadline = time.Time{}
			}
		case pglogrepl.XLogDataByteID:
			// parse the log data
			data, err := pglogrepl.ParseXLogData(envelope.Data[1:])
			if err != nil {
				return err
			}

			// parse the data
			if err = parser.Parse(data.WALData); err != nil {
				return err
			}

			// update the state position
			if data.WALStart > x.State.Position {
				x.State.Position = data.WALStart
			}
		}
	}
}

// ListenAndServe listens and serves the replication
func (x *Server) ListenAndServe(ctx context.Context) error {
	if err := x.Listen(ctx); err != nil {
		return err
	}

	return x.Serve(ctx)
}

// Close closes the replication
func (x *Server) Close() error {
	return nil
}
