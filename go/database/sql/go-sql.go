// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/google/sqlcommenter/go/core"
)

var (
	_ driver.Driver        = (*sqlCommenterDriver)(nil)
	_ driver.DriverContext = (*sqlCommenterDriver)(nil)
)

// SQLCommenterDriver returns a driver object that contains SQLCommenter drivers.
type sqlCommenterDriver struct {
	driver driver.Driver
}

type sqlCommenterConn struct {
	driver.Conn
}

func newConn(conn driver.Conn) *sqlCommenterConn {
	return &sqlCommenterConn{
		Conn: conn,
	}
}

func newDriver(dri driver.Driver) driver.Driver {
	if _, ok := dri.(driver.DriverContext); ok {
		return newSQLCommenterDriver(dri)
	}
	// Only implements driver.Driver
	return struct{ driver.Driver }{newSQLCommenterDriver(dri)}
}

func newSQLCommenterDriver(dri driver.Driver) *sqlCommenterDriver {
	return &sqlCommenterDriver{driver: dri}
}

func (d *sqlCommenterDriver) Open(name string) (driver.Conn, error) {
	rawConn, err := d.driver.Open(name)
	if err != nil {
		return nil, err
	}
	return newConn(rawConn), nil
}

func (d *sqlCommenterDriver) OpenConnector(name string) (driver.Connector, error) {
	rawConnector, err := d.driver.(driver.DriverContext).OpenConnector(name)
	if err != nil {
		return nil, err
	}
	return newConnector(rawConnector, d), err
}

var _ driver.Connector = (*sqlCommenterConnector)(nil)

type sqlCommenterConnector struct {
	driver.Connector
	otDriver *sqlCommenterDriver
}

func newConnector(connector driver.Connector, otDriver *sqlCommenterDriver) *sqlCommenterConnector {
	return &sqlCommenterConnector{
		Connector: connector,
		otDriver:  otDriver,
	}
}

func (c *sqlCommenterConnector) Connect(ctx context.Context) (connection driver.Conn, err error) {
	connection, err = c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return newConn(connection), nil
}

func (c *sqlCommenterConnector) Driver() driver.Driver {
	return c.otDriver
}

// dsnConnector is copied from sql.dsnConnector
type dsnConnector struct {
	dsn    string
	driver driver.Driver
}

func (t dsnConnector) Connect(_ context.Context) (driver.Conn, error) {
	return t.driver.Open(t.dsn)
}

func (t dsnConnector) Driver() driver.Driver {
	return t.driver
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

func (s *sqlCommenterConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	queryer, ok := s.Conn.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	commentMap := getComments(ctx)
	query = appendSQLComment(query, commentMap)
	return queryer.QueryContext(ctx, query, args)
}

// Raw returns the underlying driver connection
// Issue: https://github.com/XSAM/otelsql/issues/98
func (s *sqlCommenterConn) Raw() driver.Conn {
	return s.Conn
}

// Open is a wrapper over sql.Open with OTel instrumentation.
func Open(driverName, dataSourceName string) (*sql.DB, error) {
	// Retrieve the driver implementation we need to wrap with instrumentation
	db, err := sql.Open(driverName, "")
	if err != nil {
		return nil, err
	}
	d := db.Driver()
	if err = db.Close(); err != nil {
		return nil, err
	}

	otDriver := newSQLCommenterDriver(d)

	if _, ok := d.(driver.DriverContext); ok {
		connector, err := otDriver.OpenConnector(dataSourceName)
		if err != nil {
			return nil, err
		}
		return sql.OpenDB(connector), nil
	}

	return sql.OpenDB(dsnConnector{dsn: dataSourceName, driver: otDriver}), nil
}
