package sqltrace

import (
	"database/sql/driver"
)

type tx struct {
	wrapped    driver.Tx
	connection *conn
}

func wrapTx(wrapped driver.Tx, c *conn) driver.Tx {
	if wrapped == nil {
		return nil
	}
	return tx{wrapped, c}
}

// Tx interface

func (t tx) Commit() error {
	span := beginSpan(t.connection.tx_span, "Commit")
	defer span.end()

	err := t.wrapped.Commit()
	span.setError(err)

	t.connection.tx_span = emptySpan()
	return err
}

func (t tx) Rollback() error {
	span := beginSpan(t.connection.tx_span, "Rollback")
	defer span.end()

	err := t.wrapped.Rollback()
	span.setError(err)

	t.connection.tx_span = emptySpan()
	return err
}
