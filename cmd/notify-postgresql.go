/*
 * Minio Cloud Storage, (C) 2014-2016 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// PostgreSQL Notifier implementation. A table with a specific
// structure (column names, column types, and primary key/uniqueness
// constraint) is used. The user may set the table name in the
// configuration. A sample SQL command that creates a command with the
// required structure is:
//
//     CREATE TABLE myminio (
//         key VARCHAR PRIMARY KEY,
//         value JSONB
//     );
//
// PostgreSQL's "INSERT ... ON CONFLICT ... DO UPDATE ..." feature
// (UPSERT) is used here, so the minimum version of PostgreSQL
// required is 9.5.
//
// On each create or update object event in Minio Object storage
// server, a row is created or updated in the table in Postgres. On
// each object removal, the corresponding row is deleted from the
// table.

package cmd

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/Sirupsen/logrus"

	// libpq db driver is usually imported blank - see examples in
	// https://godoc.org/github.com/lib/pq
	_ "github.com/lib/pq"
)

const (
	upsertRow = `INSERT INTO %s (key, value)
VALUES ($1, $2)
ON CONFLICT (key)
DO UPDATE SET value = EXCLUDED.value;`
	deleteRow = ` DELETE FROM %s
WHERE key = $1;`
	createTable = `CREATE TABLE %s (
    key VARCHAR PRIMARY KEY,
    value JSONB
);`
	tableExists = `SELECT 1 FROM %s;`
)

type postgreSQLNotify struct {
	Enable bool `json:"enable"`

	// pass connection string in config directly. This string is
	// formatted according to
	// https://godoc.org/github.com/lib/pq#hdr-Connection_String_Parameters
	ConnectionString string `json:"connectionString"`
	// specifying a table name is required.
	Table string `json:"table"`

	// uses the values below if no connection string is specified
	// - however the connection string method offers more
	// flexibility.
	Host     string `json:"host"`
	Port     string `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"database"`
}

type pgConn struct {
	connStr       string
	table         string
	preparedStmts map[string]*sql.Stmt
	*sql.DB
}

func dialPostgreSQL(pgN postgreSQLNotify) (pgConn, error) {
	if !pgN.Enable {
		return pgConn{}, errNotifyNotEnabled
	}

	// check that table is specified
	if pgN.Table == "" {
		return pgConn{}, fmt.Errorf(
			"PostgreSQL Notifier Error: Table was not specified in configuration")
	}

	connStr := pgN.ConnectionString
	// check if connection string is specified
	if connStr == "" {
		// build from other parameters
		params := []string{}
		if pgN.Host != "" {
			params = append(params, "host="+pgN.Host)
		}
		if pgN.Port != "" {
			params = append(params, "port="+pgN.Port)
		}
		if pgN.User != "" {
			params = append(params, "user="+pgN.User)
		}
		if pgN.Password != "" {
			params = append(params, "password="+pgN.Password)
		}
		if pgN.Database != "" {
			params = append(params, "dbname="+pgN.Database)
		}
		connStr = strings.Join(params, " ")
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return pgConn{}, fmt.Errorf(
			"PostgreSQL Notifier Error: Connection opening failure (connectionString=%s): %v",
			connStr, err,
		)
	}

	// ping to check that server is actually reachable.
	err = db.Ping()
	if err != nil {
		return pgConn{}, fmt.Errorf(
			"PostgreSQL Notifier Error: Ping to server failed with: %v",
			err,
		)
	}

	// check that table exists - if not, create it.
	_, err = db.Exec(fmt.Sprintf(tableExists, pgN.Table))
	if err != nil {
		// most likely, table does not exist. try to create it:
		_, errCreate := db.Exec(fmt.Sprintf(createTable, pgN.Table))
		if errCreate != nil {
			// failed to create the table. error out.
			return pgConn{}, fmt.Errorf(
				"PostgreSQL Notifier Error: 'Select' failed with %v, then 'Create Table' failed with %v",
				err, errCreate,
			)
		}
	}

	// create prepared statements
	stmts := make(map[string]*sql.Stmt)
	// insert or update statement
	stmts["upsertRow"], err = db.Prepare(fmt.Sprintf(upsertRow, pgN.Table))
	if err != nil {
		return pgConn{},
			fmt.Errorf("PostgreSQL Notifier Error: create UPSERT prepared statement failed with: %v", err)
	}
	stmts["deleteRow"], err = db.Prepare(fmt.Sprintf(deleteRow, pgN.Table))
	if err != nil {
		return pgConn{},
			fmt.Errorf("PostgreSQL Notifier Error: create DELETE prepared statement failed with: %v", err)
	}

	return pgConn{connStr, pgN.Table, stmts, db}, nil
}

func newPostgreSQLNotify(accountID string) (*logrus.Logger, error) {
	pgNotify := serverConfig.GetPostgreSQLNotifyByID(accountID)

	// Dial postgres
	pgC, err := dialPostgreSQL(pgNotify)
	if err != nil {
		return nil, err
	}

	pgLog := logrus.New()

	pgLog.Out = ioutil.Discard

	pgLog.Formatter = new(logrus.JSONFormatter)

	pgLog.Hooks.Add(pgC)

	return pgLog, nil
}

func (pgC pgConn) Close() {
	// first close all prepared statements
	for _, v := range pgC.preparedStmts {
		_ = v.Close()
	}
	// close db connection
	_ = pgC.DB.Close()
}

func (pgC pgConn) Fire(entry *logrus.Entry) error {
	// get event type by trying to convert to string
	entryEventType, ok := entry.Data["EventType"].(string)
	if !ok {
		// ignore event if converting EventType to string
		// fails.
		return nil
	}

	// Check for event delete
	if eventMatch(entryEventType, []string{"s3:ObjectRemoved:*"}) {
		// delete row from the table
		_, err := pgC.preparedStmts["deleteRow"].Exec(entry.Data["Key"])
		if err != nil {
			return fmt.Errorf(
				"Error deleting event with key = %v - got postgres error - %v",
				entry.Data["Key"], err,
			)
		}
	} else {
		// json encode the value for the row
		value, err := json.Marshal(map[string]interface{}{
			"Records": entry.Data["Records"],
		})
		if err != nil {
			return fmt.Errorf(
				"Unable to encode event %v to JSON - got error - %v",
				entry.Data["Records"], err,
			)
		}

		// upsert row into the table
		_, err = pgC.preparedStmts["upsertRow"].Exec(entry.Data["Key"], value)
		if err != nil {
			return fmt.Errorf(
				"Unable to upsert event with Key=%v and Value=%v - got postgres error - %v",
				entry.Data["Key"], entry.Data["Records"], err,
			)
		}
	}

	return nil
}

func (pgC pgConn) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.InfoLevel,
	}
}
