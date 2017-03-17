package sqltrace

import (
	"context"
	"database/sql/driver"

	ot "github.com/opentracing/opentracing-go"
	otext "github.com/opentracing/opentracing-go/ext"
	otlog "github.com/opentracing/opentracing-go/log"
)

type sqlspan struct {
	span ot.Span
}

func emptySpan() sqlspan {
	return sqlspan{}
}

func getParentSpan(ctx context.Context, alt_parent *sqlspan) sqlspan {
	var parentSpan ot.Span
	if ctx != nil {
		parentSpan = ot.SpanFromContext(ctx)
	}
	if parentSpan == nil && alt_parent != nil {
		parentSpan = alt_parent.span
	}
	return sqlspan{parentSpan}
}

func beginSpan(parent sqlspan, name string) sqlspan {
	if parent.isEmpty() {
		return sqlspan{nil}
	}
	span := ot.StartSpan(name, ot.ChildOf(parent.span.Context()))
	// set some standard tags that apply to all our spans
	otext.Component.Set(span, "sqltrace")
	otext.SpanKindRPCClient.Set(span)
	span.SetTag("db.type", "sql")
	return sqlspan{span}
}

func (s sqlspan) isEmpty() bool {
	return s.span == nil
}

func (s sqlspan) end() {
	if !s.isEmpty() {
		s.span.Finish()
		s.span = nil
	}
}

func (s sqlspan) setSql(sql string) sqlspan {
	if s.isEmpty() || sql == "" {
		return s
	}
	s.span.SetTag("db.statement", sql)
	return s
}

func (s sqlspan) setError(err error) sqlspan {
	if s.isEmpty() || err == nil {
		return s
	}
	s.span.SetTag(string(otext.Error), true)
	s.span.LogKV(otlog.String("event", "error"), otlog.Error(err))
	return s
}

func (s sqlspan) setResult(result driver.Result) sqlspan {
	if s.isEmpty() || result == nil {
		return s
	}

	lastInsertId, err := result.LastInsertId()
	if err == nil {
		s.span.SetTag("db.last_insert_id", lastInsertId)
	} else {
		s.setError(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err == nil {
		s.span.SetTag("db.rows_affected", rowsAffected)
	} else {
		s.setError(err)
	}

	return s
}
