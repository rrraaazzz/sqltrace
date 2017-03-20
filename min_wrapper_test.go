package sqltrace

import "database/sql/driver"

// Wraps a Driver but does not implement any of the optional interfaces.
// Useful for testing, since most existing drivers do implement many of
// the optional interfaces.
func hideOptionalInterfaces(d driver.Driver) driver.Driver {
	return minDriver{d}
}

type minDriver struct {
	wrapped driver.Driver
}

type minConn struct {
	wrapped driver.Conn
}

type minStmt struct {
	wrapped driver.Stmt
}

type minRows struct {
	wrapped driver.Rows
}

// Driver

func (m minDriver) Open(name string) (driver.Conn, error) {
	c, err := m.wrapped.Open(name)
	if err != nil {
		return nil, err
	}
	return minConn{c}, nil
}

// Connection

func (m minConn) Prepare(query string) (driver.Stmt, error) {
	s, err := m.wrapped.Prepare(query)
	if err != nil {
		return nil, err
	}
	return minStmt{s}, nil
}

func (m minConn) Close() error {
	return m.wrapped.Close()
}

func (m minConn) Begin() (driver.Tx, error) {
	return m.wrapped.Begin()
}

// Statement

func (m minStmt) Close() error {
	return m.wrapped.Close()
}

func (m minStmt) NumInput() int {
	return m.wrapped.NumInput()
}

func (m minStmt) Exec(args []driver.Value) (driver.Result, error) {
	return m.wrapped.Exec(args)
}

func (m minStmt) Query(args []driver.Value) (driver.Rows, error) {
	r, err := m.wrapped.Query(args)
	if err != nil {
		return nil, err
	}
	return minRows{r}, nil
}

// Rows

func (m minRows) Columns() []string {
	return m.wrapped.Columns()
}

func (m minRows) Close() error {
	return m.wrapped.Close()
}

func (m minRows) Next(dest []driver.Value) error {
	return m.wrapped.Next(dest)
}
