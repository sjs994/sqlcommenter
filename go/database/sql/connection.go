package sql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"runtime/debug"
	"strings"

	"github.com/google/sqlcommenter/go/core"
)

var attemptedToAutosetApplication = false

type sqlCommenterConn struct {
	driver.Conn
	options core.CommenterOptions
}

func newSQLCommenterConn(conn driver.Conn, options core.CommenterOptions) *sqlCommenterConn {
	return &sqlCommenterConn{
		Conn:    conn,
		options: options,
	}
}

/*
TODO: Check whether to implement or not ?
type Conn interface {
	// Prepare returns a prepared statement, bound to this connection.
	Prepare(query string) (Stmt, error)

	// Close invalidates and potentially stops any current
	// prepared statements and transactions, marking this
	// connection as no longer in use.
	//
	// Because the sql package maintains a free pool of
	// connections and only calls Close when there's a surplus of
	// idle connections, it shouldn't be necessary for drivers to
	// do their own connection caching.
	//
	// Drivers must ensure all network calls made by Close
	// do not block indefinitely (e.g. apply a timeout).
	Close() error

	// Begin starts and returns a new transaction.
	//
	// Deprecated: Drivers should implement ConnBeginTx instead (or additionally).
	Begin() (Tx, error)
}

// ConnPrepareContext enhances the Conn interface with context.
type ConnPrepareContext interface {
	// PrepareContext returns a prepared statement, bound to this connection.
	// context is for the preparation of the statement,
	// it must not store the context within the statement itself.
	PrepareContext(ctx context.Context, query string) (Stmt, error)
}

// ConnBeginTx enhances the Conn interface with context and TxOptions.
type ConnBeginTx interface {
	// BeginTx starts and returns a new transaction.
	// If the context is canceled by the user the sql package will
	// call Tx.Rollback before discarding and closing the connection.
	//
	// This must check opts.Isolation to determine if there is a set
	// isolation level. If the driver does not support a non-default
	// level and one is set or if there is a non-default isolation level
	// that is not supported, an error must be returned.
	//
	// This must also check opts.ReadOnly to determine if the read-only
	// value is true to either set the read-only transaction property if supported
	// or return an error if it is not supported.
	BeginTx(ctx context.Context, opts TxOptions) (Tx, error)
}

// SessionResetter may be implemented by Conn to allow drivers to reset the
// session state associated with the connection and to signal a bad connection.
type SessionResetter interface {
	// ResetSession is called prior to executing a query on the connection
	// if the connection has been used before. If the driver returns ErrBadConn
	// the connection is discarded.
	ResetSession(ctx context.Context) error
}
*/

func (s *sqlCommenterConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	queryer, ok := s.Conn.(driver.Queryer)
	if !ok {
		return nil, driver.ErrSkip
	}
	ctx := context.Background()
	extendedQuery := s.withComment(ctx, query)
	return queryer.Query(extendedQuery, args)
}

func (s *sqlCommenterConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	queryer, ok := s.Conn.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	extendedQuery := s.withComment(ctx, query)
	return queryer.QueryContext(ctx, extendedQuery, args)
}

func (s *sqlCommenterConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	execor, ok := s.Conn.(driver.Execer)
	if !ok {
		return nil, driver.ErrSkip
	}
	ctx := context.Background()
	extendedQuery := s.withComment(ctx, query)
	return execor.Exec(extendedQuery, args)
}

func (s *sqlCommenterConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	execor, ok := s.Conn.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	extendedQuery := s.withComment(ctx, query)
	return execor.ExecContext(ctx, extendedQuery, args)
}

func (s *sqlCommenterConn) Raw() driver.Conn {
	return s.Conn
}

// ***** Commenter Functions *****

func (conn *sqlCommenterConn) withComment(ctx context.Context, query string) string {
	var commentsMap = map[string]string{}
	query = strings.TrimSpace(query)
	config := conn.options.Config

	// Sorted alphabetically
	if config.EnableAction && (ctx.Value(core.Action) != nil) {
		commentsMap[core.Action] = ctx.Value(core.Action).(string)
	}

	// `driver` information should not be coming from framework.
	// So, explicitly adding that here.
	if config.EnableDBDriver {
		commentsMap[core.Driver] = fmt.Sprintf("database/sql:%s", conn.options.Tags.DriverName)
	}

	if config.EnableFramework && (ctx.Value(core.Framework) != nil) {
		commentsMap[core.Framework] = ctx.Value(core.Framework).(string)
	}

	if config.EnableRoute && (ctx.Value(core.Route) != nil) {
		commentsMap[core.Route] = ctx.Value(core.Route).(string)
	}

	if config.EnableTraceparent {
		carrier := core.ExtractTraceparent(ctx)
		if val, ok := carrier["traceparent"]; ok {
			commentsMap[core.Traceparent] = val
		}
	}

	if config.EnableApplication {
		if !attemptedToAutosetApplication && conn.options.Tags.Application == "" {
			attemptedToAutosetApplication = true
			bi, ok := debug.ReadBuildInfo()
			if ok {
				conn.options.Tags.Application = bi.Path
			}
		}
		commentsMap[core.Application] = conn.options.Tags.Application
	}

	var commentsString string = ""
	if len(commentsMap) > 0 { // Converts comments map to string and appends it to query
		commentsString = fmt.Sprintf("/*%s*/", core.ConvertMapToComment(commentsMap))
	}

	// A semicolon at the end of the SQL statement means the query ends there.
	// We need to insert the comment before that to be considered as part of the SQL statemtent.
	if query[len(query)-1:] == ";" {
		return fmt.Sprintf("%s%s;", strings.TrimSuffix(query, ";"), commentsString)
	}
	return fmt.Sprintf("%s%s", query, commentsString)
}

// ***** Commenter Functions *****
