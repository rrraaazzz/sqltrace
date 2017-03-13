// sqltrace is a database/sql/driver wrapper. When driver methods are called
// with a context containing an OpenTracing span, sub-spans are created for all
// database operations.
//
// Furthermore, transactions and prepared statements save the OpenTracing span
// passed to them through the context. If such a span exists, all operations on
// that transaction or prepared statement are traced even if a context is not
// passed to them explicitly.
package sqltrace

import "database/sql/driver"

// OpenFunc is a function that opens a database connection, given a data source
// name.
type OpenFunc func(dsn string) (driver.Conn, error)

// New returns a sqltrace driver that wraps connections returned by the given
// OpenFunc. When given an OpenTracing span in a context, it emits sub-spans
// for database operations.
func New(open OpenFunc) driver.Driver {
	return tracingDriver{
		openWrapped: open,
	}
}
