# pgxrepl

[![CI](https://github.com/pgx-contrib/pgxrepl/actions/workflows/ci.yml/badge.svg)](https://github.com/pgx-contrib/pgxrepl/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/pgx-contrib/pgxrepl)](https://github.com/pgx-contrib/pgxrepl/releases)
[![Go Reference](https://pkg.go.dev/badge/github.com/pgx-contrib/pgxrepl.svg)](https://pkg.go.dev/github.com/pgx-contrib/pgxrepl)
[![License](https://img.shields.io/github/license/pgx-contrib/pgxrepl)](LICENSE)
[![Go Version](https://img.shields.io/github/go-mod/go-version/pgx-contrib/pgxrepl)](go.mod)
[![pgx Version](https://img.shields.io/badge/pgx-v5-blue)](https://github.com/jackc/pgx)

Logical replication consumer for [pgx v5](https://github.com/jackc/pgx).

## Features

- Typed `Insert`, `Update`, `Delete`, and `Truncate` events with `BEGIN` / `COMMIT` framing
- Replicated rows expose `pgx.CollectableRow` — decode with `pgx.RowTo` / `pgx.RowToStructByName`
- Slot and publication helpers (`CreateSlot`, `DropSlot`, `CreatePublication`, `DropPublication`)
- At-least-once delivery — the slot advances only after `HandleCommit` returns nil, so failed transactions replay on restart

## Installation

```bash
go get github.com/pgx-contrib/pgxrepl
```

Your Postgres server must be configured with `wal_level=logical`.

## Usage

```go
package main

import (
    "context"
    "os"

    "github.com/jackc/pgx/v5"
    "github.com/jackc/pgx/v5/pgconn"
    "github.com/pgx-contrib/pgxrepl"
)

type Logger struct{}

func (Logger) HandleBegin(op *pgxrepl.BeginOperation) error    { return nil }
func (Logger) HandleCommit(op *pgxrepl.CommitOperation) error  { return nil }
func (Logger) HandleInsert(op *pgxrepl.InsertOperation) error {
    var id int
    var name string
    if err := op.NewRow.Scan(&id, &name); err != nil {
        return err
    }
    // ... forward to your sink
    return nil
}
func (Logger) HandleUpdate(op *pgxrepl.UpdateOperation) error     { return nil }
func (Logger) HandleDelete(op *pgxrepl.DeleteOperation) error     { return nil }
func (Logger) HandleTruncate(op *pgxrepl.TruncateOperation) error { return nil }

func main() {
    ctx := context.Background()
    dsn := os.Getenv("PGX_DATABASE_URL")

    // 1. Create a publication on a normal connection.
    normal, err := pgconn.Connect(ctx, dsn)
    must(err)
    must(pgxrepl.CreatePublication(ctx, normal, "users_pub", []pgx.Identifier{{"public", "users"}}))
    must(normal.Close(ctx))

    // 2. Create a slot on a replication connection.
    cfg, err := pgconn.ParseConfig(dsn)
    must(err)
    cfg.RuntimeParams["replication"] = "database"
    repl, err := pgconn.ConnectConfig(ctx, cfg)
    must(err)
    must(pgxrepl.CreateSlot(ctx, repl, "users_slot"))

    // 3. Run the broker.
    broker := &pgxrepl.Broker{
        Conn:        repl,
        Handler:     Logger{},
        Slot:        "users_slot",
        Publication: "users_pub",
    }
    must(broker.Run(ctx))
}

func must(err error) { if err != nil { panic(err) } }
```

## Non-goals

pgxrepl deliberately stays narrow. The following pgoutput features are not handled, and there are no plans to add them:

- Streaming in-progress transactions (`StreamStart` / `StreamStop` / `StreamCommit` / `StreamAbort`)
- `Origin`, `Type`, and `LogicalDecodingMessage` events
- Physical replication

If you need any of these, drive [pglogrepl](https://github.com/jackc/pglogrepl) directly.

## Development

### DevContainer

Open in VS Code with the Dev Containers extension. The environment provides Go,
Nix, and a PostgreSQL 18 instance started with `wal_level=logical`.

```
PGX_DATABASE_URL=postgres://vscode@postgres:5432/pgxrepl?sslmode=disable
```

### Nix

```bash
nix develop          # enter shell with Go
go tool ginkgo run -r
```

### Run tests

```bash
# Unit tests only (no database required — integration specs skip)
go tool ginkgo run -r

# With integration tests
export PGX_DATABASE_URL="postgres://localhost/pgxrepl?sslmode=disable"
go tool ginkgo run -r
```

## License

[MIT](LICENSE)
