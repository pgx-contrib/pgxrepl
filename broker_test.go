package pgxrepl_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/pgx-contrib/pgxrepl"
)

var _ = Describe("Broker", Ordered, func() {
	var (
		ctx   context.Context
		pool  *pgxpool.Pool
		dsn   string
		suite string
	)

	BeforeAll(func() {
		dsn = os.Getenv("PGX_DATABASE_URL")
		if dsn == "" {
			Skip("PGX_DATABASE_URL not set")
		}
		ctx = context.Background()

		var err error
		pool, err = pgxpool.New(ctx, dsn)
		Expect(err).To(Succeed())

		var wal string
		Expect(pool.QueryRow(ctx, "SHOW wal_level").Scan(&wal)).To(Succeed())
		if wal != "logical" {
			Skip("wal_level must be 'logical'; got " + wal)
		}
	})

	AfterAll(func() {
		if pool != nil {
			pool.Close()
		}
	})

	BeforeEach(func() {
		suite = fmt.Sprintf("t%d", time.Now().UnixNano())

		_, err := pool.Exec(ctx, fmt.Sprintf(
			`CREATE TABLE %s (id INT PRIMARY KEY, name TEXT)`, suite))
		Expect(err).To(Succeed())
		_, err = pool.Exec(ctx, fmt.Sprintf(
			`ALTER TABLE %s REPLICA IDENTITY FULL`, suite))
		Expect(err).To(Succeed())

		withPgConn(ctx, pool, func(c *pgconn.PgConn) {
			Expect(pgxrepl.CreatePublication(ctx, c, suite, []pgx.Identifier{{suite}})).To(Succeed())
		})

		repl := mustReplConn(ctx, dsn)
		Expect(pgxrepl.CreateSlot(ctx, repl, suite)).To(Succeed())
		Expect(repl.Close(ctx)).To(Succeed())
	})

	AfterEach(func() {
		withPgConn(ctx, pool, func(c *pgconn.PgConn) {
			_ = pgxrepl.DropPublication(ctx, c, suite)
		})
		repl := mustReplConn(ctx, dsn)
		_ = pgxrepl.DropSlot(ctx, repl, suite)
		_ = repl.Close(ctx)
		_, _ = pool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, suite))
	})

	It("streams BEGIN, typed DML ops, and COMMIT in order", func() {
		h := newRecorder(nil)
		stop := startBroker(ctx, dsn, suite, h)
		defer stop()

		mustExec(ctx, pool, fmt.Sprintf(`INSERT INTO %s VALUES (1, 'a')`, suite))
		mustExec(ctx, pool, fmt.Sprintf(`UPDATE %s SET name = 'b' WHERE id = 1`, suite))
		mustExec(ctx, pool, fmt.Sprintf(`DELETE FROM %s WHERE id = 1`, suite))
		mustExec(ctx, pool, fmt.Sprintf(`TRUNCATE %s`, suite))

		Eventually(h.kinds, "10s", "50ms").Should(Equal([]string{
			"begin", "insert", "commit",
			"begin", "update", "commit",
			"begin", "delete", "commit",
			"begin", "truncate", "commit",
		}))
	})

	It("decodes row values via CollectableRow", func() {
		h := newRecorder(nil)
		stop := startBroker(ctx, dsn, suite, h)
		defer stop()

		mustExec(ctx, pool, fmt.Sprintf(`INSERT INTO %s VALUES (42, 'hello')`, suite))

		Eventually(func() int { return len(h.inserts()) }, "10s", "50ms").Should(Equal(1))

		op := h.inserts()[0]
		Expect(op.TableName).To(Equal(pgx.Identifier{"public", suite}))

		var id int
		var name string
		Expect(op.NewRow.Scan(&id, &name)).To(Succeed())
		Expect(id).To(Equal(42))
		Expect(name).To(Equal("hello"))
	})

	It("does not advance the slot when a handler returns an error", func() {
		mustExec(ctx, pool, fmt.Sprintf(
			`INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')`, suite))

		boom := errors.New("boom")
		var seen int
		failHandler := newRecorder(func(op any) error {
			if _, ok := op.(*pgxrepl.InsertOperation); ok {
				seen++
				if seen == 2 {
					return boom
				}
			}
			return nil
		})
		Expect(runBrokerFor(ctx, dsn, suite, failHandler, 5*time.Second)).To(MatchError(boom))

		// Re-run with a fresh broker — because the transaction never acked,
		// all 3 inserts replay inside a new BEGIN/COMMIT frame.
		h := newRecorder(nil)
		stop := startBroker(ctx, dsn, suite, h)
		defer stop()

		Eventually(func() int { return len(h.inserts()) }, "10s", "50ms").Should(Equal(3))
		Expect(h.kinds()).To(ContainElements("begin", "insert", "insert", "insert", "commit"))
	})
})

// --- helpers ---

type recorder struct {
	mu     sync.Mutex
	events []any
	before func(any) error
}

func newRecorder(before func(any) error) *recorder {
	return &recorder{before: before}
}

func (r *recorder) record(op any) error {
	if r.before != nil {
		if err := r.before(op); err != nil {
			return err
		}
	}
	r.mu.Lock()
	r.events = append(r.events, op)
	r.mu.Unlock()
	return nil
}

func (r *recorder) kinds() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.events))
	for _, e := range r.events {
		switch e.(type) {
		case *pgxrepl.BeginOperation:
			out = append(out, "begin")
		case *pgxrepl.CommitOperation:
			out = append(out, "commit")
		case *pgxrepl.InsertOperation:
			out = append(out, "insert")
		case *pgxrepl.UpdateOperation:
			out = append(out, "update")
		case *pgxrepl.DeleteOperation:
			out = append(out, "delete")
		case *pgxrepl.TruncateOperation:
			out = append(out, "truncate")
		}
	}
	return out
}

func (r *recorder) inserts() []*pgxrepl.InsertOperation {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*pgxrepl.InsertOperation
	for _, e := range r.events {
		if op, ok := e.(*pgxrepl.InsertOperation); ok {
			out = append(out, op)
		}
	}
	return out
}

func (r *recorder) HandleBegin(op *pgxrepl.BeginOperation) error       { return r.record(op) }
func (r *recorder) HandleCommit(op *pgxrepl.CommitOperation) error     { return r.record(op) }
func (r *recorder) HandleInsert(op *pgxrepl.InsertOperation) error     { return r.record(op) }
func (r *recorder) HandleUpdate(op *pgxrepl.UpdateOperation) error     { return r.record(op) }
func (r *recorder) HandleDelete(op *pgxrepl.DeleteOperation) error     { return r.record(op) }
func (r *recorder) HandleTruncate(op *pgxrepl.TruncateOperation) error { return r.record(op) }

func replConfig(dsn string) *pgconn.Config {
	cfg, err := pgconn.ParseConfig(dsn)
	Expect(err).To(Succeed())
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = map[string]string{}
	}
	cfg.RuntimeParams["replication"] = "database"
	return cfg
}

func mustReplConn(ctx context.Context, dsn string) *pgconn.PgConn {
	conn, err := pgconn.ConnectConfig(ctx, replConfig(dsn))
	Expect(err).To(Succeed())
	return conn
}

func withPgConn(ctx context.Context, pool *pgxpool.Pool, fn func(*pgconn.PgConn)) {
	c, err := pool.Acquire(ctx)
	Expect(err).To(Succeed())
	defer c.Release()
	fn(c.Conn().PgConn())
}

func mustExec(ctx context.Context, pool *pgxpool.Pool, sql string) {
	_, err := pool.Exec(ctx, sql)
	Expect(err).To(Succeed())
}

// startBroker runs a Broker in a goroutine until stop() is called.
func startBroker(parent context.Context, dsn, name string, h pgxrepl.Handler) (stop func()) {
	ctx, cancel := context.WithCancel(parent)
	conn := mustReplConn(ctx, dsn)
	b := &pgxrepl.Broker{Conn: conn, Handler: h, Slot: name, Publication: name}
	done := make(chan struct{})
	go func() {
		_ = b.Run(ctx)
		close(done)
	}()
	return func() {
		cancel()
		<-done
		_ = conn.Close(context.Background())
	}
}

// runBrokerFor runs a Broker until timeout, returning the first error.
func runBrokerFor(parent context.Context, dsn, name string, h pgxrepl.Handler, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	conn, err := pgconn.ConnectConfig(ctx, replConfig(dsn))
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	b := &pgxrepl.Broker{Conn: conn, Handler: h, Slot: name, Publication: name}
	return b.Run(ctx)
}
