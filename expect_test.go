// This file contains utilities for generating expectations when
// testing sqltrace. It generates both sqlmock expectations, as
// well as OpenTracing span expectations.

// Due to spans being ordered, expectations must be constructed
// in the order they are expected to occur. This is slightly more
// restrictive than sqlmock would allow.

package sqltrace

import (
	"database/sql/driver"
	"regexp"
	"strings"
	"testing"

	otmock "github.com/opentracing/opentracing-go/mocktracer"
	sqlmock "gopkg.in/DATA-DOG/go-sqlmock.v1"
)

type expectedResult struct {
	lastInsertId int64
	rowsAffected int64
}

type expectedSpan struct {
	name     string
	sql      string
	result   *expectedResult
	hasError bool
}

func (e *expectedSpan) setSql(s string) *expectedSpan {
	if e == nil {
		return nil
	}
	e.sql = s
	return e
}

func (e *expectedSpan) setResult(lastInsertId, rowsAffected int64) *expectedSpan {
	if e == nil {
		return nil
	}
	e.result = &expectedResult{lastInsertId, rowsAffected}
	return e
}

func (e *expectedSpan) setErr() *expectedSpan {
	if e == nil {
		return nil
	}
	e.hasError = true
	return e
}

func getSpanSql(s *otmock.MockSpan) string {
	value := s.Tag("db.statement")
	if str, ok := value.(string); ok {
		return str
	}
	return ""
}

type expectations struct {
	mock  sqlmock.Sqlmock
	spans []*expectedSpan
}

type expectedBegin struct {
	mock *sqlmock.ExpectedBegin
	span *expectedSpan
}

type expectedCommit struct {
	mock *sqlmock.ExpectedCommit
	span *expectedSpan
}

type expectedRollback struct {
	mock *sqlmock.ExpectedRollback
	span *expectedSpan
}

type expectedPrepare struct {
	e    *expectations
	sql  string
	mock *sqlmock.ExpectedPrepare
	span *expectedSpan
}

type expectedQuery struct {
	mock *sqlmock.ExpectedQuery
	span *expectedSpan
}

type expectedExec struct {
	mock *sqlmock.ExpectedExec
	span *expectedSpan
}

type expectedRows struct {
	e        *expectations
	mock     *sqlmock.Rows
	rowCount int
	useSpans bool
	spans    []*expectedSpan
}

func expect(mock sqlmock.Sqlmock) *expectations {
	return &expectations{
		mock:  mock,
		spans: nil}
}

func (e *expectations) addSpan(forRealz bool, name string) *expectedSpan {
	if !forRealz {
		return nil
	}
	e.spans = append(e.spans, &expectedSpan{name: name})
	return e.spans[len(e.spans)-1]
}

func (e *expectations) begin(withSpan bool) *expectedBegin {
	return &expectedBegin{
		mock: e.mock.ExpectBegin(),
		span: e.addSpan(withSpan, "begin_transaction")}
}

func (e *expectedBegin) err(beginError error) *expectedBegin {
	e.mock.WillReturnError(beginError)
	e.span.setErr()
	return e
}

func (e *expectations) commit(withSpan bool) *expectedCommit {
	return &expectedCommit{
		mock: e.mock.ExpectCommit(),
		span: e.addSpan(withSpan, "commit")}
}

func (e *expectedCommit) err(commitError error) *expectedCommit {
	e.mock.WillReturnError(commitError)
	e.span.setErr()
	return e
}

func (e *expectations) rollback(withSpan bool) *expectedRollback {
	return &expectedRollback{
		mock: e.mock.ExpectRollback(),
		span: e.addSpan(withSpan, "rollback")}
}

func (e *expectedRollback) err(rollbackError error) *expectedRollback {
	e.mock.WillReturnError(rollbackError)
	e.span.setErr()
	return e
}

func (e *expectations) prepare(withSpan bool, sql string) *expectedPrepare {
	return &expectedPrepare{
		e:    e,
		sql:  sql,
		mock: e.mock.ExpectPrepare(sql),
		span: e.addSpan(withSpan, "prepare_statement").setSql(sql)}
}

func (e *expectedPrepare) err(prepareError error) *expectedPrepare {
	e.mock.WillReturnError(prepareError)
	e.span.setErr()
	return e
}

func (e *expectedPrepare) query() *expectedQuery {
	return &expectedQuery{
		mock: e.mock.ExpectQuery(),
		span: e.e.addSpan(e.span != nil, "query").setSql(e.sql)}
}

func (e *expectedPrepare) exec() *expectedExec {
	return &expectedExec{
		mock: e.mock.ExpectExec(),
		span: e.e.addSpan(e.span != nil, "exec").setSql(e.sql)}
}

func (e *expectedPrepare) close(err error) *expectedPrepare {
	closeSpan := e.e.addSpan(e.span != nil, "close_statement").setSql(e.sql)
	if err != nil {
		e.mock.WillReturnCloseError(err)
		closeSpan.setErr()
	}
	return e
}

func (e *expectations) query(withSpan bool, query string) *expectedQuery {
	return &expectedQuery{
		mock: e.mock.ExpectQuery(query),
		span: e.addSpan(withSpan, "query").setSql(query)}
}

func (e *expectedQuery) err(queryError error) *expectedQuery {
	e.mock.WillReturnError(queryError)
	e.span.setErr()
	return e
}

func (e *expectedQuery) rows(r *expectedRows) *expectedQuery {
	e.mock.WillReturnRows(r.mock)
	return e
}

func (e *expectations) exec(withSpan bool, sql string) *expectedExec {
	return &expectedExec{
		mock: e.mock.ExpectExec(sql),
		span: e.addSpan(withSpan, "exec").setSql(sql)}
}

func (e *expectedExec) err(execError error) *expectedExec {
	e.mock.WillReturnError(execError)
	e.span.setErr()
	return e
}

func (e *expectedExec) result(lastInsertId, rowsAffected int64, resultErr error) *expectedExec {
	var result driver.Result
	if resultErr == nil {
		e.span.setResult(lastInsertId, rowsAffected)
		result = sqlmock.NewResult(lastInsertId, rowsAffected)
	} else {
		result = sqlmock.NewErrorResult(resultErr)
	}
	e.mock.WillReturnResult(result)
	return e
}

func (e *expectations) rows(withSpan bool, columns ...string) *expectedRows {
	return &expectedRows{
		e:        e,
		mock:     sqlmock.NewRows(columns),
		rowCount: 0,
		useSpans: withSpan,
		spans:    nil}
}

func (e *expectedRows) row(values ...driver.Value) *expectedRows {
	e.mock.AddRow(values...)
	e.rowCount++
	e.spans = append(e.spans, e.e.addSpan(e.useSpans, "next_row"))
	return e
}

func (e *expectedRows) rowError(re error, values ...driver.Value) *expectedRows {
	e.mock.AddRow(values...)
	e.mock.RowError(e.rowCount, re)
	e.rowCount++
	e.spans = append(e.spans, e.e.addSpan(e.useSpans, "next_row").setErr())
	return e
}

func (e *expectedRows) close(err error) *expectedRows {
	closeSpan := e.e.addSpan(e.useSpans, "close_rows")
	if err != nil {
		e.mock.CloseError(err)
		closeSpan.setErr()
	}
	return e
}

func spanHasResult(s *otmock.MockSpan, res *expectedResult) bool {
	last_insert_id := s.Tag("db.last_insert_id")
	if value, ok := last_insert_id.(int64); !ok || value != res.lastInsertId {
		return false
	}

	rows_affected := s.Tag("db.rows_affected")
	if value, ok := rows_affected.(int64); !ok || value != res.rowsAffected {
		return false
	}

	return true
}

func spanHasError(s *otmock.MockSpan) bool {
	return s.Tag("error") != nil
}

func (e *expectations) check(t *testing.T, spans []*otmock.MockSpan) {
	mockErr := e.mock.ExpectationsWereMet()
	if mockErr != nil {
		t.Errorf("Database call expectations were not met: %v", mockErr)
	}

	if len(spans) != len(e.spans) {
		var expectedNames []string
		var haveNames []string
		for _, s := range e.spans {
			expectedNames = append(expectedNames, s.name)
		}
		for _, s := range spans {
			haveNames = append(haveNames, s.OperationName)
		}
		t.Errorf("Expecting spans: %v. Have spans: %v.",
			strings.Join(expectedNames, ", "),
			strings.Join(haveNames, ", "))
		return
	}

	for i, se := range e.spans {
		span := spans[i]
		errors := []string{}

		// check name
		nameRex := regexp.MustCompile(se.name)
		if !nameRex.MatchString(span.OperationName) {
			errors = append(errors, "name")
		}

		// check sql
		s := getSpanSql(span)
		if se.sql != "" && se.sql != s {
			errors = append(errors, "sql")
		}

		// check result
		if se.result != nil && !spanHasResult(span, se.result) {
			errors = append(errors, "result")
		}

		// check error
		if spanHasError(span) != se.hasError {
			errors = append(errors, "error status")
		}

		if len(errors) > 0 {
			t.Errorf("Expecting span: %#v. Found: %#v. Have unexpected %v", se, span,
				strings.Join(errors, ", "))
		}
	}
}
