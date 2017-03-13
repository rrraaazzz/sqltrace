package sqltrace

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"testing"

	ot "github.com/opentracing/opentracing-go"
	otmock "github.com/opentracing/opentracing-go/mocktracer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sqlmock "gopkg.in/DATA-DOG/go-sqlmock.v1"
)

var dsnCounter int
var isInitTracedMock bool

type testData struct {
	db        *sql.DB
	dsn       string
	mock      sqlmock.Sqlmock
	tracer    *otmock.MockTracer
	ctx       context.Context
	sqlmockDB *sql.DB
}

func setup(t *testing.T, hideOptionalIfaces bool) testData {
	var result testData
	var err error

	result.dsn = fmt.Sprintf("sqlmock_connection_%v", dsnCounter)
	dsnCounter++

	// sqlmock.Open returns an error if an existing connection
	// was not created beforehand using New or NewWithDSN.
	// So we create a dummy connection that we keep alive for
	// the whole duration of the test.
	result.sqlmockDB, result.mock, err = sqlmock.NewWithDSN(result.dsn)
	require.Nil(t, err)

	if !isInitTracedMock {
		// This is a global object in the sqlmock package.
		// Since there is no nicer way to get hold of it, this hack
		// will do.
		//
		// TODO fix/fork sqlmock and remove this hack
		mockDriver := result.sqlmockDB.Driver()

		sql.Register("traced_mock", New(func(name string) (driver.Conn, error) {
			return mockDriver.Open(name)
		}))

		sql.Register("traced_mock_min", New(func(name string) (driver.Conn, error) {
			return hideOptionalInterfaces(mockDriver).Open(name)
		}))

		isInitTracedMock = true
	}

	var driverName string
	if hideOptionalIfaces {
		driverName = "traced_mock_min"
	} else {
		driverName = "traced_mock"
	}
	result.db, err = sql.Open(driverName, result.dsn)
	require.Nil(t, err)

	result.tracer = otmock.New()
	ot.SetGlobalTracer(result.tracer)

	span := result.tracer.StartSpan("ParentSpan")
	result.ctx = ot.ContextWithSpan(context.Background(), span)

	return result
}

func (d testData) close() {
	if d.db != nil {
		d.db.Close()
	}
	if d.sqlmockDB != nil {
		d.sqlmockDB.Close()
	}
	if d.tracer != nil {
		ot.SetGlobalTracer(nil)
	}
}

// represents a flag that can be turned on or off in a test
type testParam struct {
	value *bool

	// If a param depends on other params, pointers to them
	// will be stored here. A param can only be enabled if
	// all params it depends on are enabled.
	enabled []*bool
}

// adds a param to the list and returns a pointer to its value
func addParam(params *[]testParam, parents ...*bool) *bool {
	value := false
	p := testParam{&value, parents}
	*params = append(*params, p)
	return (*params)[len(*params)-1].value
}

// Generates a new set of params. Returns true when all possible
// param combinations have been generated. (Think of each param
// as a bit in a binary number. This increments that number by 1
// and returns true when it would overflow).
func incParams(params []testParam) bool {
	done := true
	for i, _ := range params {
		p := &params[i]
		allParentsEnabled := true
		for _, parent := range p.enabled {
			if !*parent {
				allParentsEnabled = false
				break
			}
		}
		if !allParentsEnabled {
			*p.value = false
			continue
		}
		if !*p.value {
			*p.value = true
			done = false
			break
		}
		*p.value = false
	}
	return done
}

func beginTx(db *sql.DB, ctx context.Context) (*sql.Tx, error) {
	if ctx != nil {
		return db.BeginTx(ctx, &sql.TxOptions{})
	} else {
		return db.Begin()
	}
}

func prepareStmt(db *sql.DB, tx *sql.Tx, ctx context.Context, query string) (*sql.Stmt, error) {
	if ctx != nil {
		if tx != nil {
			return tx.PrepareContext(ctx, query)
		} else {
			return db.PrepareContext(ctx, query)
		}
	} else {
		if tx != nil {
			return tx.Prepare(query)
		} else {
			return db.Prepare(query)
		}
	}
}

func query(db *sql.DB, tx *sql.Tx, stmt *sql.Stmt, ctx context.Context,
	query string, args ...interface{}) (*sql.Rows, error) {
	if stmt != nil {
		if ctx != nil {
			return stmt.QueryContext(ctx, args...)
		} else {
			return stmt.Query(args...)
		}
	} else if tx != nil {
		if ctx != nil {
			return tx.QueryContext(ctx, query, args...)
		} else {
			return tx.Query(query, args...)
		}
	} else {
		if ctx != nil {
			return db.QueryContext(ctx, query, args...)
		} else {
			return db.Query(query, args...)
		}
	}
}

func exec(db *sql.DB, tx *sql.Tx, stmt *sql.Stmt, ctx context.Context,
	query string, args ...interface{}) (sql.Result, error) {
	if stmt != nil {
		if ctx != nil {
			return stmt.ExecContext(ctx, args...)
		} else {
			return stmt.Exec(args...)
		}
	} else if tx != nil {
		if ctx != nil {
			return tx.ExecContext(ctx, query, args...)
		} else {
			return tx.Exec(query, args...)
		}
	} else {
		if ctx != nil {
			return db.ExecContext(ctx, query, args...)
		} else {
			return db.Exec(query, args...)
		}
	}
}

func TestOpenOk(t *testing.T) {
	d1 := setup(t, false)
	defer d1.close()

	d2 := setup(t, true)
	defer d2.close()
}

func TestPrepare(t *testing.T) {
	var params []testParam
	useMinimalInterfaces := addParam(&params)
	useCtx := addParam(&params)

	for {
		t.Logf("Prepare test with minimal interfaces: %v, context %v",
			*useMinimalInterfaces, *useCtx)

		d := setup(t, false)
		defer d.close()

		e := expect(d.mock)
		var ctx context.Context
		if *useCtx {
			ctx = d.ctx
		}

		// simple prepare and close
		e.prepare(*useCtx, "test1").close(nil)
		stmt, err := prepareStmt(d.db, nil, ctx, "test1")
		assert.Nil(t, err)
		require.NotNil(t, stmt)
		assert.Nil(t, stmt.Close())

		// prepare with error and no close
		prepareErr := fmt.Errorf("prepare error")
		e.prepare(*useCtx, "test2").err(prepareErr)
		stmt, err = prepareStmt(d.db, nil, ctx, "test2")
		assert.Nil(t, stmt)
		assert.Equal(t, prepareErr, err)

		e.check(t, d.tracer.FinishedSpans())
		done := incParams(params)
		if done {
			break
		}
	}
}

func TestTx(t *testing.T) {
	var params []testParam
	useMinimalInterfaces := addParam(&params)
	useCtx := addParam(&params)

	for {
		t.Logf("Tx test with minimal interfaces: %v, context %v",
			useMinimalInterfaces, useCtx)

		d := setup(t, false)
		defer d.close()

		e := expect(d.mock)

		var ctx context.Context
		if *useCtx {
			ctx = d.ctx
		}

		// successful begin and commit
		e.begin(*useCtx)
		e.commit(*useCtx)
		tx, err := beginTx(d.db, ctx)
		assert.Nil(t, err)
		require.NotNil(t, tx)
		assert.Nil(t, tx.Commit())

		// successful begin and rollback
		e.begin(*useCtx)
		e.rollback(*useCtx)
		tx, err = beginTx(d.db, ctx)
		assert.Nil(t, err)
		require.NotNil(t, tx)
		assert.Nil(t, tx.Rollback())

		// begin error
		beginErr := fmt.Errorf("begin error")
		e.begin(*useCtx).err(beginErr)
		tx, err = beginTx(d.db, ctx)
		assert.Nil(t, tx)
		require.NotNil(t, err)
		assert.Equal(t, beginErr, err)

		// commit error
		commitErr := fmt.Errorf("commit error")
		e.begin(*useCtx)
		e.commit(*useCtx).err(commitErr)
		tx, err = beginTx(d.db, ctx)
		require.NotNil(t, tx)
		err = tx.Commit()
		require.NotNil(t, err)
		assert.Equal(t, commitErr, err)

		// rollback error
		rollbackErr := fmt.Errorf("rollback error")
		e.begin(*useCtx)
		e.rollback(*useCtx).err(rollbackErr)
		tx, err = beginTx(d.db, ctx)
		require.NotNil(t, tx)
		err = tx.Rollback()
		require.NotNil(t, err)
		assert.Equal(t, rollbackErr, err)

		e.check(t, d.tracer.FinishedSpans())
		done := incParams(params)
		if done {
			break
		}
	}
}

func TestQueryExec(t *testing.T) {
	var params []testParam
	useMinimalInterfaces := addParam(&params)
	useTx := addParam(&params)
	txHasCtx := addParam(&params, useTx)
	useStmt := addParam(&params)
	stmtHasCtx := addParam(&params, useStmt)
	queryExecHaveCtx := addParam(&params)
	queryExecHaveErr := addParam(&params)

	for {
		t.Logf("Running query/exec test with minimalInterfaces: %v, useTx: %v, "+
			"txHasCtx: %v, useStmt: %v, stmtHasCtx: %v, queryExecHaveCtx: %v, "+
			"queryExecHaveErr: %v", *useMinimalInterfaces, *useTx, *txHasCtx, *useStmt,
			*stmtHasCtx, *queryExecHaveCtx, *queryExecHaveErr)

		d := setup(t, *useMinimalInterfaces)
		defer d.close()

		var txCtx, stmtCtx, queryExecCtx context.Context
		if *txHasCtx {
			txCtx = d.ctx
		}
		if *stmtHasCtx {
			stmtCtx = d.ctx
		}
		if *queryExecHaveCtx {
			queryExecCtx = d.ctx
		}

		e := expect(d.mock)

		var tx *sql.Tx
		var err error
		if *useTx {
			e.begin(*txHasCtx)
			tx, err = beginTx(d.db, txCtx)
			require.NotNil(t, tx)
			require.Nil(t, err)
		}

		var stmt *sql.Stmt
		var stmtExpectation *expectedPrepare
		stmtHasSpan := *stmtHasCtx || (*useTx && *txHasCtx)
		if *useStmt {
			stmtExpectation = e.prepare(stmtHasSpan, "q")
			stmt, err = prepareStmt(d.db, tx, stmtCtx, "q")
			require.NotNil(t, stmt)
			require.Nil(t, err)
		}

		queryExecError := fmt.Errorf("query exec error")
		rowError := fmt.Errorf("row error")

		arg1 := expectedArg{"arg1", 1, 13}
		arg2 := expectedArg{"arg2", 2, "arg"}

		qeHasSpan := *queryExecHaveCtx || (*useStmt && *stmtHasCtx) || (*useTx && *txHasCtx)

		// If the driver we are wrapping only exposes the minimal required
		// interfaces, it's not possible to exec or query without first
		// generating a prepared statement.
		internalStmt := *useMinimalInterfaces && !*useStmt

		var expectedInternalStmt *expectedPrepare
		if internalStmt {
			expectedInternalStmt = e.prepare(qeHasSpan, "q")
		}

		queryExpectation := e.query(qeHasSpan, "q").args(arg1, arg2)
		if *queryExecHaveErr {
			queryExpectation.err(queryExecError)
		} else {
			r := e.rows(qeHasSpan, "col1", "col2")
			r.row(1, "r1").row(2, "r2").rowError(rowError, 3, "r3")
			r.close(nil)

			queryExpectation.rows(r)
		}

		if internalStmt {
			expectedInternalStmt.close(nil)
		}

		rows, err := query(d.db, tx, stmt, queryExecCtx, "q", 13, "arg")

		if *queryExecHaveErr {
			assert.Nil(t, rows)
			require.NotNil(t, err)
			assert.Equal(t, err, queryExecError)
		} else {
			require.Nil(t, err)
			require.NotNil(t, rows)

			cols, err := rows.Columns()
			require.Nil(t, err)
			require.Equal(t, 2, len(cols))
			require.Equal(t, "col1", cols[0])
			require.Equal(t, "col2", cols[1])

			require.True(t, rows.Next())
			require.Nil(t, rows.Err())

			var c1 int
			var c2 string
			require.Nil(t, rows.Scan(&c1, &c2))
			assert.Equal(t, c1, 1)
			assert.Equal(t, c2, "r1")

			require.True(t, rows.Next())
			require.Nil(t, rows.Err())
			require.Nil(t, rows.Scan(&c1, &c2))
			assert.Equal(t, c1, 2)
			assert.Equal(t, c2, "r2")

			require.False(t, rows.Next())
			require.Equal(t, rowError, rows.Err())

			require.Nil(t, rows.Close())
		}

		if internalStmt {
			expectedInternalStmt = e.prepare(qeHasSpan, "q")
		}

		execExpectation := e.exec(qeHasSpan, "q").args(arg1, arg2)
		if *queryExecHaveErr {
			execExpectation.err(queryExecError)
		} else {
			execExpectation.result(37, 42, nil)
		}

		result, err := exec(d.db, tx, stmt, queryExecCtx, "q", 13, "arg")

		if internalStmt {
			expectedInternalStmt.close(nil)
		}

		if *queryExecHaveErr {
			assert.Nil(t, result)
			require.NotNil(t, err)
			assert.Equal(t, err, queryExecError)
		} else {
			require.Nil(t, err)
			require.NotNil(t, result)

			lastInsertId, err := result.LastInsertId()
			assert.Equal(t, lastInsertId, int64(37))
			assert.Nil(t, err)

			rowsAffected, err := result.RowsAffected()
			assert.Equal(t, rowsAffected, int64(42))
			assert.Nil(t, err)
		}

		if stmt != nil {
			stmtExpectation.close(nil)
			stmt.Close()
		}

		if tx != nil {
			e.commit(*txHasCtx)
			assert.Nil(t, tx.Commit())
		}

		e.check(t, d.tracer.FinishedSpans())
		done := incParams(params)
		if done {
			break
		}
	}
}

func TestPing(t *testing.T) {
	for _, minimalIf := range []bool{true, false} {
		d := setup(t, minimalIf)
		defer d.close()

		err := d.db.Ping()
		assert.Nil(t, err)

		err = d.db.PingContext(d.ctx)
		assert.Nil(t, err)

		spans := d.tracer.FinishedSpans()

		if minimalIf {
			require.Equal(t, 0, len(spans))
		} else {
			require.Equal(t, 1, len(spans))
			assert.Equal(t, "Ping", spans[0].OperationName)
		}
	}
}

func TestDeprecated(t *testing.T) {
	// A few methods in the sql driver can't be called through database/sql
	d := setup(t, false)
	defer d.close()

	drv := d.db.Driver()
	conn, err := drv.Open(d.dsn)

	require.Nil(t, err)
	require.NotNil(t, conn)

	e := expect(d.mock)
	e.begin(false)

	tx, err := conn.Begin()
	require.Nil(t, err)
	require.NotNil(t, tx)

	estmt := e.prepare(false, "sql")
	stmt, err := conn.Prepare("sql")
	require.Nil(t, err)
	require.NotNil(t, stmt)

	arg := expectedArg{name: "n", index: 0, value: 10}
	erows := e.rows(false, "col").row(1).row(2)
	e.query(false, "sql").args(arg).rows(erows)
	erows.close(nil)

	rows, err := stmt.Query([]driver.Value{int64(10)})
	require.Nil(t, err)
	require.NotNil(t, rows)

	drvValues := []driver.Value{0}
	require.Nil(t, rows.Next(drvValues))
	require.Equal(t, 1, drvValues[0])

	require.Nil(t, rows.Next(drvValues))
	require.Equal(t, 2, drvValues[0])

	require.NotNil(t, rows.Next(drvValues))
	require.Nil(t, rows.Close())

	e.exec(false, "sql").args(arg).result(11, 12, nil)
	result, err := stmt.Exec([]driver.Value{int64(10)})
	require.Nil(t, err)
	require.NotNil(t, result)

	lastId, err := result.LastInsertId()
	require.Nil(t, err)
	require.Equal(t, int64(11), lastId)
	rowsAffected, err := result.RowsAffected()
	require.Nil(t, err)
	require.Equal(t, int64(12), rowsAffected)

	estmt.close(nil)
	require.Nil(t, stmt.Close())

	e.commit(false)
	tx.Commit()

	erows = e.rows(false, "col")
	e.query(false, "sql").args(arg).rows(erows)
	erows.close(nil)

	rows, err = conn.(driver.Queryer).Query("sql", []driver.Value{int64(10)})
	require.Nil(t, err)
	require.NotNil(t, rows)

	drvValues = []driver.Value{0}
	require.NotNil(t, rows.Next(drvValues))
	require.Nil(t, rows.Close())

	e.exec(false, "sql").args(arg).result(13, 14, nil)
	result, err = conn.(driver.Execer).Exec("sql", []driver.Value{int64(10)})
	require.Nil(t, err)
	require.NotNil(t, result)

	lastId, err = result.LastInsertId()
	require.Nil(t, err)
	require.Equal(t, int64(13), lastId)
	rowsAffected, err = result.RowsAffected()
	require.Nil(t, err)
	require.Equal(t, int64(14), rowsAffected)

	require.Nil(t, d.mock.ExpectationsWereMet())
}
