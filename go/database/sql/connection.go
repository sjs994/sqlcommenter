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

type Tags struct {
	DriverName  string
	Application string
}

type sqlCommenterConn struct {
	driver.Conn
	options core.CommenterOptions
	tags    Tags
}

func newSQLCommenterConn(conn driver.Conn, options core.CommenterOptions) *sqlCommenterConn {
	return &sqlCommenterConn{
		Conn:    conn,
		options: options,
	}
}

func (s *sqlCommenterConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	queryer, ok := s.Conn.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	extendedQuery := s.withComment(ctx, query)
	return queryer.QueryContext(ctx, extendedQuery, args)
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

func appendSQLComment(query string, commentMap map[string]string) string {
	comment := core.ConvertMapToComment(commentMap)
	return fmt.Sprintf(`%s /*%s*/`, query, comment)
}

func getComments(ctx context.Context) map[string]string {
	m := map[string]string{}
	fmt.Printf("Comments: %#v", ctx)
	// Sorted alphabetically
	if ctx.Value(core.Action) != nil {
		m[core.Action] = ctx.Value(core.Action).(string)
	}
	if ctx.Value(core.Framework) != nil {
		m[core.Framework] = ctx.Value(core.Framework).(string)
	}
	if ctx.Value(core.Route) != nil {
		m[core.Route] = ctx.Value(core.Route).(string)
	}
	return m
}

// ***** Query Functions *****

// ***** Commenter Functions *****

func (conn *sqlCommenterConn) withComment(ctx context.Context, query string) string {
	var commentsMap = map[string]string{}
	query = strings.TrimSpace(query)

	// Sorted alphabetically
	if conn.options.EnableAction && (ctx.Value(core.Action) != nil) {
		commentsMap[core.Action] = ctx.Value(core.Action).(string)
	}

	// `driver` information should not be coming from framework.
	// So, explicitly adding that here.
	if conn.options.EnableDBDriver {
		commentsMap[core.Driver] = fmt.Sprintf("database/sql:%s", conn.tags.DriverName)
	}

	if conn.options.EnableFramework && (ctx.Value(core.Framework) != nil) {
		commentsMap[core.Framework] = ctx.Value(core.Framework).(string)
	}

	if conn.options.EnableRoute && (ctx.Value(core.Route) != nil) {
		commentsMap[core.Route] = ctx.Value(core.Route).(string)
	}

	if conn.options.EnableTraceparent {
		carrier := core.ExtractTraceparent(ctx)
		if val, ok := carrier["traceparent"]; ok {
			commentsMap[core.Traceparent] = val
		}
	}

	if conn.options.EnableApplication {
		if !attemptedToAutosetApplication && conn.tags.Application == "" {
			attemptedToAutosetApplication = true
			bi, ok := debug.ReadBuildInfo()
			if ok {
				conn.tags.Application = bi.Path
			}
		}
		commentsMap[core.Application] = conn.tags.Application
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
