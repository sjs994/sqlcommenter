package sql

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/google/sqlcommenter/go/core"
)

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

func (s *sqlCommenterConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	queryer, ok := s.Conn.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	commentMap := getComments(ctx)
	query = appendSQLComment(query, commentMap)
	return queryer.QueryContext(ctx, query, args)
}

func (s *sqlCommenterConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	execor, ok := s.Conn.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	commentMap := getComments(ctx)
	query = appendSQLComment(query, commentMap)
	return execor.ExecContext(ctx, query, args)
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
