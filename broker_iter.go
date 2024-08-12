package pgxrepl

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Iterator is a struct that represents the state
type Iterator struct {
	conn     *pgconn.PgConn
	deadline time.Time
	position pglogrepl.LSN
}

// Start starts the iterator
func (x *Iterator) Start(ctx context.Context, name string) error {
	options := pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			fmt.Sprintf("proto_version '%v'", 2),
			fmt.Sprintf("publication_names '%v'", name),
			fmt.Sprintf("messages '%v'", true),
			fmt.Sprintf("streaming '%v'", true),
		},
	}

	sysident, err := pglogrepl.IdentifySystem(ctx, x.conn)
	if err != nil {
		return err
	}

	x.position = sysident.XLogPos
	// start the replication
	if err = pglogrepl.StartReplication(ctx, x.conn, name, sysident.XLogPos, options); err != nil {
		return err
	}

	return nil
}

// Status reports the Status of the replication
func (x *Iterator) Status(ctx context.Context) error {
	if time.Now().After(x.deadline) {
		output := pglogrepl.StandbyStatusUpdate{WALWritePosition: x.position}

		if err := pglogrepl.SendStandbyStatusUpdate(ctx, x.conn, output); err != nil {
			return err
		}

		x.deadline = time.Now().Add(10 * time.Second)
	}

	return nil
}

// Receive receives the message
func (x *Iterator) Receive(ctx context.Context) (pgproto3.BackendMessage, error) {
	// create a new context with a deadline
	ctx, cancel := context.WithDeadline(ctx, x.deadline)
	// receive the message
	message, err := x.conn.ReceiveMessage(ctx)
	// cancel the context
	cancel()

	// handle the error
	if err != nil {
		if pgconn.Timeout(err) {
			return nil, nil
		}

		return nil, err
	}

	// check if the message is an error response
	if response, ok := message.(*pgproto3.ErrorResponse); ok {
		return nil, &Error{response: response}
	}

	return message, nil
}

// Error is a struct that represents the error
type Error struct {
	response *pgproto3.ErrorResponse
}

// Error returns the error
func (x *Error) Error() string {
	return fmt.Sprintf(`code: %v, message: %v, detail: %v, table: %v`,
		x.response.Code, x.response.Message, x.response.Detail, x.response.TableName)
}
