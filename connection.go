package sqltrace

import (
	"context"
	"database/sql/driver"
	"fmt"
)

type conn struct {
	wrapped driver.Conn

	// This is only non-empty if an active trasaction is using this connection.
	//
	// Note that the way database/sql calls are implemented in terms of
	// database/sql/driver interfaces is a bit unusual.
	//
	// When methods are called on a database/sql.TX object, it does not
	// result in parallel method calls on the driver.TX object. Instead,
	// these calls (Prepare, Exec, Query, etc.) go straight to a driver.Conn
	// object.
	//
	// This works because it is assumed that when a transaction starts, it
	// takes exclusive ownership of a connection. All further calls on that
	// connection before a rollback or commit are implicitly part of the
	// transaction.
	//
	// So we can safely store transaction-wide spans in the connection
	// itself, because we know that the connection will not be used
	// for non-transaction methods. On commit or rollback we remove the
	// span from the connection.
	tx_span sqlspan

	// TODO do we need to make conn thread safe? database/sql does not
	// require it (it locks always when using a connection).
}

type pingerConn struct {
	conn
}

func newConn(wrapped driver.Conn) driver.Conn {
	return &conn{wrapped, sqlspan{nil}}
}

func newPingerConn(wrapped driver.Conn) driver.Conn {
	return &pingerConn{conn{wrapped, sqlspan{nil}}}
}

// Conn interface

func (c *conn) Prepare(sql string) (driver.Stmt, error) {
	span := beginSpan(getParentSpan(nil, &c.tx_span), "PrepareStatement")
	defer span.end()

	wrappedStmt, err := c.wrapped.Prepare(sql)
	span.setSql(sql).setError(err)
	return wrapStmt(wrappedStmt, sql, emptySpan()), err
}

func (c *conn) Close() error {
	return c.wrapped.Close()
}

func (c *conn) Begin() (driver.Tx, error) {
	if c.tx_span.span != nil {
		panic("Nested transactions should not be possible")
	}

	wrappedTx, err := c.wrapped.Begin()
	return wrapTx(wrappedTx, c), err
}

// ConnBeginTx interface

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	// It's safe to store the span in the connection because the transaction
	// gets exclusive use of the connection until commit/rollback.
	c.tx_span = getParentSpan(ctx, nil)

	span := beginSpan(c.tx_span, "BeginTransaction")
	defer span.end()

	var wrappedTx driver.Tx
	var err error

	if wrapped, ok := c.wrapped.(driver.ConnBeginTx); ok {
		wrappedTx, err = wrapped.BeginTx(ctx, opts)
	} else {
		if opts.Isolation != 0 || opts.ReadOnly {
			wrappedTx = nil
			err = fmt.Errorf("Wrapped driver can't handle non-default transaction options")
		} else {
			select {
			case <-ctx.Done():
				wrappedTx, err = nil, ctx.Err()
			default:
				wrappedTx, err = c.wrapped.Begin()
			}
		}
	}

	span.setError(err)
	if err != nil {
		c.tx_span = emptySpan()
	}
	return wrapTx(wrappedTx, c), err
}

// ConnPrepareContext interface

func (c *conn) PrepareContext(ctx context.Context, sql string) (driver.Stmt, error) {
	parentSpan := getParentSpan(ctx, &c.tx_span)
	span := beginSpan(parentSpan, "PrepareStatement")
	defer span.end()

	var wrappedStmt driver.Stmt
	var err error

	if wrapped, ok := c.wrapped.(driver.ConnPrepareContext); ok {
		wrappedStmt, err = wrapped.PrepareContext(ctx, sql)
	} else {
		select {
		case <-ctx.Done():
			wrappedStmt, err = nil, ctx.Err()
		default:
			wrappedStmt, err = c.wrapped.Prepare(sql)
		}
	}

	span.setSql(sql).setError(err)
	return wrapStmt(wrappedStmt, sql, parentSpan), err
}

// Execer interface

func (c *conn) Exec(sql string, args []driver.Value) (driver.Result, error) {
	execer, ok := c.wrapped.(driver.Execer)
	if !ok {
		// If we return driver.ErrSkip, database/sql will prepare,
		// exec, and close a stmt, all with provided ctx. When that
		// happens, we'll generate our spans, if needed.
		return nil, driver.ErrSkip
	}

	span := beginSpan(getParentSpan(nil, &c.tx_span), "Exec")
	defer span.end()

	result, err := execer.Exec(sql, args)
	span.setSql(sql).setResult(result).setError(err)

	return result, err
}

// ExecerContext interface

func (c *conn) ExecContext(ctx context.Context, sql string, args []driver.NamedValue) (driver.Result, error) {
	execer, ok := c.wrapped.(driver.ExecerContext)
	if !ok {
		// If we return driver.ErrSkip, database/sql will prepare,
		// exec, and close a stmt, all with provided ctx. When that
		// happens, we'll generate our spans, if needed.
		return nil, driver.ErrSkip
	}

	span := beginSpan(getParentSpan(ctx, &c.tx_span), "Exec")
	defer span.end()

	result, err := execer.ExecContext(ctx, sql, args)
	span.setSql(sql).setResult(result).setError(err)

	return result, err
}

// Pinger interface

func (c *pingerConn) Ping(ctx context.Context) error {
	// We only use a pingerConn if the wrapped conn implements driver.Pinger
	pinger := c.wrapped.(driver.Pinger)

	span := beginSpan(getParentSpan(ctx, &c.conn.tx_span), "Ping")
	defer span.end()

	err := pinger.Ping(ctx)
	span.setError(err)
	return err
}

// Queryer interface

func (c *conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	queryer, ok := c.wrapped.(driver.Queryer)
	if !ok {
		// If we return driver.ErrSkip, database/sql will prepare,
		// query, and close a stmt, all with provided ctx. When that
		// happens, we'll generate our spans, if needed.
		return nil, driver.ErrSkip
	}

	parentSpan := getParentSpan(nil, &c.tx_span)
	span := beginSpan(parentSpan, "Query")
	defer span.end()

	rows, err := queryer.Query(query, args)
	span.setSql(query).setError(err)

	return wrapRows(rows, parentSpan), err
}

// QueryerContext interface

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	qc, ok := c.wrapped.(driver.QueryerContext)
	if !ok {
		// If we return driver.ErrSkip, database/sql will prepare,
		// query, and close a stmt, all with provided ctx. When that
		// happens, we'll generate our spans, if needed.
		return nil, driver.ErrSkip
	}

	parentSpan := getParentSpan(ctx, &c.tx_span)
	span := beginSpan(parentSpan, "Query")
	defer span.end()

	rows, err := qc.QueryContext(ctx, query, args)
	span.setSql(query).setError(err)

	return wrapRows(rows, parentSpan), err
}
