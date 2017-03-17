package sqltrace

import (
	"context"
	"database/sql/driver"
	"fmt"
)

type stmt struct {
	wrapped driver.Stmt
	sql     string
	span    sqlspan
}

func wrapStmt(wrapped driver.Stmt, sql string, span sqlspan) driver.Stmt {
	if wrapped == nil {
		return nil
	}
	return stmt{wrapped, sql, span}
}

// Stmt interface

func (s stmt) Close() error {
	span := beginSpan(s.span, "CloseStatement")
	defer span.end()

	err := s.wrapped.Close()
	span.setSql(s.sql).setError(err)
	return err
}

func (s stmt) NumInput() int {
	return s.wrapped.NumInput()
}

func (s stmt) Exec(args []driver.Value) (driver.Result, error) {
	span := beginSpan(s.span, "Exec")
	defer span.end()

	result, err := s.wrapped.Exec(args)
	span.setSql(s.sql).setResult(result).setError(err)

	return result, err
}

func (s stmt) Query(args []driver.Value) (driver.Rows, error) {
	span := beginSpan(s.span, "Query")
	defer span.end()

	rows, err := s.wrapped.Query(args)
	span.setSql(s.sql).setError(err)
	return wrapRows(rows, s.span), err
}

// StmtExecContext interface

func (s stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	span := beginSpan(getParentSpan(ctx, &s.span), "Exec")
	defer span.end()

	var result driver.Result
	var err error

	if sec, ok := s.wrapped.(driver.StmtExecContext); ok {
		result, err = sec.ExecContext(ctx, args)
	} else {
		var positionalArgs []driver.Value
		positionalArgs, err = namedToPositionalArgs(args, "Stmt.ExecContext")

		if err == nil {
			select {
			case <-ctx.Done():
				result, err = nil, ctx.Err()
			default:
				result, err = s.wrapped.Exec(positionalArgs)
			}
		}
	}

	span.setSql(s.sql).setResult(result).setError(err)
	return result, err
}

// StmtQueryContext interface

func (s stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	parentSpan := getParentSpan(ctx, &s.span)
	span := beginSpan(parentSpan, "Query")
	defer span.end()

	var rows driver.Rows
	var err error

	if sqc, ok := s.wrapped.(driver.StmtQueryContext); ok {
		rows, err = sqc.QueryContext(ctx, args)
	} else {
		var positionalArgs []driver.Value
		positionalArgs, err = namedToPositionalArgs(args, "Stmt.QueryContext")

		if err == nil {
			select {
			case <-ctx.Done():
				rows, err = nil, ctx.Err()
			default:
				rows, err = s.wrapped.Query(positionalArgs)
			}
		}
	}

	span.setSql(s.sql).setError(err)
	return wrapRows(rows, parentSpan), err
}

// Helpers

func namedToPositionalArgs(args []driver.NamedValue, opName string) ([]driver.Value, error) {
	positionalArgs := make([]driver.Value, len(args))
	for i, arg := range args {
		if len(arg.Name) > 0 {
			return nil, fmt.Errorf("wrapped sql driver does not support named parameters in %v", opName)
		}
		positionalArgs[i] = arg.Value
	}
	return positionalArgs, nil
}
