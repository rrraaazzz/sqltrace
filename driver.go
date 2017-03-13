package sqltrace

import "database/sql/driver"

type tracingDriver struct {
	openWrapped OpenFunc
}

// Driver interface

func (d tracingDriver) Open(name string) (driver.Conn, error) {
	wrapped, err := d.openWrapped(name)
	if err != nil {
		return nil, err
	}

	// We implement driver.Pinger if and only if wrapped does.
	if _, ok := wrapped.(driver.Pinger); ok {
		return newPingerConn(wrapped), nil
	} else {
		return newConn(wrapped), nil
	}
}
