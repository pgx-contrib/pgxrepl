package pgxrepl

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// CreateSlot creates a logical replication slot with the pgoutput plugin.
// It is safe to call only against a connection opened in replication mode.
func CreateSlot(ctx context.Context, conn *pgconn.PgConn, name string) error {
	_, err := pglogrepl.CreateReplicationSlot(
		ctx,
		conn,
		name,
		"pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication},
	)
	return err
}

// DropSlot drops a replication slot.
func DropSlot(ctx context.Context, conn *pgconn.PgConn, name string) error {
	return pglogrepl.DropReplicationSlot(
		ctx,
		conn,
		name,
		pglogrepl.DropReplicationSlotOptions{Wait: true},
	)
}

// CreatePublication creates a PUBLICATION for the given tables. If tables
// is empty, the publication is created FOR ALL TABLES. Use a regular (not
// replication-mode) connection.
func CreatePublication(ctx context.Context, conn *pgconn.PgConn, name string, tables []pgx.Identifier) error {
	var sql string
	if len(tables) == 0 {
		sql = fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", pgx.Identifier{name}.Sanitize())
	} else {
		parts := make([]string, 0, len(tables))
		for _, t := range tables {
			parts = append(parts, t.Sanitize())
		}
		sql = fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pgx.Identifier{name}.Sanitize(), strings.Join(parts, ", "))
	}
	return conn.Exec(ctx, sql).Close()
}

// DropPublication drops a publication if it exists.
func DropPublication(ctx context.Context, conn *pgconn.PgConn, name string) error {
	sql := fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pgx.Identifier{name}.Sanitize())
	return conn.Exec(ctx, sql).Close()
}
