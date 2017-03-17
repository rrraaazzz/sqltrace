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
	return sqlspan{ot.StartSpan(name, ot.ChildOf(parent.span.Context()))}
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
	s.span.LogFields(otlog.String("sql", sql))
	return s
}

func (s sqlspan) setError(err error) sqlspan {
	if s.isEmpty() || err == nil {
		return s
	}
	s.span.SetTag(string(otext.Error), true)
	s.span.LogKV(otlog.Error(err))
	return s
}

func (s sqlspan) setResult(result driver.Result) sqlspan {
	if s.isEmpty() || result == nil {
		return s
	}
	fields := make([]otlog.Field, 0, 2)

	// Note that we store a string on errors. The opentracing/log package also
	// lets us store errors directly, but the otlog.Errors helper doesn't take
	// a key value, it hardcodes it to "error", which we don't want.
	//
	// But, when the log fields actually get serialized, the error simply
	// turns into a string value by calling err.Error(). So we do this directly.

	lastInsertId, err := result.LastInsertId()
	if err == nil {
		fields = append(fields, otlog.Int64("lastInsertId", lastInsertId))
	} else {
		fields = append(fields, otlog.String("lastInsertIdError", err.Error()))
	}

	rowsAffected, err := result.RowsAffected()
	if err == nil {
		fields = append(fields, otlog.Int64("rowsAffected", rowsAffected))
	} else {
		fields = append(fields, otlog.String("rowsAffectedError", err.Error()))
	}

	s.span.LogFields(fields...)
	return s
}
