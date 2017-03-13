package sqltrace

import (
	"database/sql/driver"
	"reflect"
)

type rows struct {
	wrapped driver.Rows
	span    sqlspan
}

type rowsNextResultSet struct {
	rows
}

func wrapRows(wrapped driver.Rows, span sqlspan) driver.Rows {
	if wrapped == nil {
		return nil
	}
	rows := rows{wrapped, span}

	// Only implement driver.RowsNextResultSet if the wrapped rows do
	if _, ok := wrapped.(driver.RowsNextResultSet); ok {
		return rowsNextResultSet{rows}
	} else {
		return rows
	}
}

// Rows interface

func (r rows) Columns() []string {
	return r.wrapped.Columns()
}

func (r rows) Close() error {
	span := beginSpan(r.span, "CloseRows")

	err := r.wrapped.Close()
	span.setError(err)

	span.end()
	return err
}

func (r rows) Next(dest []driver.Value) error {
	span := beginSpan(r.span, "NextRow")
	defer span.end()

	err := r.wrapped.Next(dest)
	span.setError(err)
	return err
}

// RowsNextResultSet interface

func (r rowsNextResultSet) HasNextResultSet() bool {
	// Safe to cast since we only wrap it if it implements the interface
	return r.rows.wrapped.(driver.RowsNextResultSet).HasNextResultSet()
}

func (r rowsNextResultSet) NextResultSet() error {
	span := beginSpan(r.span, "NextResultSet")
	defer span.end()

	// Safe to cast since we only wrap it if it implements the interface
	err := r.rows.wrapped.(driver.RowsNextResultSet).NextResultSet()
	span.setError(err)
	return err
}

// It would be nice if we could implement these column info interfaces only
// if the wrapped driver does. But since there are 5 of them, it would result
// in 32 combinations. So instead we just implement all of them. If the
// wrapped driver is missing one of the implementations, we use the same
// defaults that database.sql uses.

// RowsColumnTypeDatabaseTypeName interface

func (r rows) ColumnTypeDatabaseTypeName(index int) string {
	if col, ok := r.wrapped.(driver.RowsColumnTypeDatabaseTypeName); ok {
		return col.ColumnTypeDatabaseTypeName(index)
	}
	return ""
}

// RowsColumnTypeLength interface

func (r rows) ColumnTypeLength(index int) (length int64, ok bool) {
	if col, ok := r.wrapped.(driver.RowsColumnTypeLength); ok {
		return col.ColumnTypeLength(index)
	}
	return 0, false
}

// RowsColumnTypeNullable interface

func (r rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	if col, ok := r.wrapped.(driver.RowsColumnTypeNullable); ok {
		return col.ColumnTypeNullable(index)
	}
	return false, false
}

// RowsColumnTypePrecisionScale interface

func (r rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	if col, ok := r.wrapped.(driver.RowsColumnTypePrecisionScale); ok {
		return col.ColumnTypePrecisionScale(index)
	}
	return 0, 0, false
}

// RowsColumnTypeScanType interface

func (r rows) ColumnTypeScanType(index int) reflect.Type {
	if col, ok := r.wrapped.(driver.RowsColumnTypeScanType); ok {
		return col.ColumnTypeScanType(index)
	}
	// This is what database.sql defaults to
	return reflect.TypeOf(new(interface{})).Elem()
}
