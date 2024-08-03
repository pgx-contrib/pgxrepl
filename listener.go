package pgxrepl

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

// Listener is a struct that represents the replication
type Listener struct {
	// Conn is the connection to the database
	Conn *pgconn.PgConn
	// Name is the name of the replication
	Name string
}

// Listen listens for changes on the replication connection
func (x *Listener) Listen(ctx context.Context) error {
	param := func(key string, value any) string {
		return fmt.Sprintf("%v '%v'", key, value)
	}

	options := pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			param("proto_version", 2),
			param("publication_names", x.Name),
			param("messages", true),
			param("streaming", true),
		},
	}

	sysident, err := pglogrepl.IdentifySystem(ctx, x.Conn)
	if err != nil {
		return err
	}

	if err = pglogrepl.StartReplication(ctx, x.Conn, x.Name, sysident.XLogPos, options); err != nil {
		return err
	}

	position := sysident.XLogPos
	duration := 10 * time.Second
	deadline := time.Now().Add(duration)
	// message processor
	processor := &Processor{
		stream:    false,
		types:     pgtype.NewMap(),
		relations: map[uint32]*pglogrepl.RelationMessageV2{},
	}

	for {
		if time.Now().After(deadline) {
			output := pglogrepl.StandbyStatusUpdate{WALWritePosition: position}

			if err = pglogrepl.SendStandbyStatusUpdate(ctx, x.Conn, output); err != nil {
				return err
			}

			deadline = time.Now().Add(duration)
		}

		ctx, cancel := context.WithDeadline(ctx, deadline)
		message, err := x.Conn.ReceiveMessage(ctx)
		// cancel the context
		cancel()

		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}

			return err
		}

		if response, ok := message.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("code: %v, message: %v, detail: %v, table: %v",
				response.Code, response.Message, response.Detail, response.TableName)
		}

		envelope, ok := message.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		switch envelope.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			info, err := pglogrepl.ParsePrimaryKeepaliveMessage(envelope.Data[1:])
			if err != nil {
				return err
			}

			if info.ServerWALEnd > position {
				position = info.ServerWALEnd
			}

			if info.ReplyRequested {
				deadline = time.Time{}
			}
		case pglogrepl.XLogDataByteID:
			data, err := pglogrepl.ParseXLogData(envelope.Data[1:])
			if err != nil {
				return err
			}

			if err = processor.Process(data.WALData); err != nil {
				return err
			}

			if data.WALStart > position {
				position = data.WALStart
			}
		}
	}
}

// Close closes the replication
func (x *Listener) Close() error {
	return nil
}
