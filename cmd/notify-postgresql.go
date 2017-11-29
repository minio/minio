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

// PostgreSQL Notifier implementation. Two formats, "namespace" and
// "access" are supported.
//
// * Namespace format
//
// On each create or update object event in Minio Object storage
// server, a row is created or updated in the table in Postgres. On
// each object removal, the corresponding row is deleted from the
// table.
//
// A table with a specific structure (column names, column types, and
// primary key/uniqueness constraint) is used. The user may set the
// table name in the configuration. A sample SQL command that creates
// a table with the required structure is:
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
// * Access format
//
// On each event, a row is appended to the configured table. There is
// no deletion or modification of existing rows.
//
// A different table schema is used for this format. A sample SQL
// commant that creates a table with the required structure is:
//
// CREATE TABLE myminio (
//     event_time TIMESTAMP WITH TIME ZONE NOT NULL,
//     event_data JSONB
// );

package cmd

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"

	// Register postgres driver
	_ "github.com/lib/pq"
)

const (
	// Queries for format=namespace mode. Here the `key` column is
	// the bucket and object of the event. When objects are
	// deleted, the corresponding row is deleted in the
	// table. When objects are created or over-written, rows are
	// inserted or updated respectively in the table.
	upsertRowForNS = `INSERT INTO %s (key, value)
VALUES ($1, $2)
ON CONFLICT (key)
DO UPDATE SET value = EXCLUDED.value;`
	deleteRowForNS = ` DELETE FROM %s
WHERE key = $1;`
	createTableForNS = `CREATE TABLE %s (
    key VARCHAR PRIMARY KEY,
    value JSONB
);`

	// Queries for format=access mode. Here the `event_time`
	// column of the table, stores the time at which the event
	// occurred in the Minio server.
	insertRowForAccess = `INSERT INTO %s (event_time, event_data)
VALUES ($1, $2);`
	createTableForAccess = `CREATE TABLE %s (
    event_time TIMESTAMP WITH TIME ZONE NOT NULL,
    event_data JSONB
);`

	// Query to check if a table already exists.
	tableExists = `SELECT 1 FROM %s;`
)

var (
	pgErrFunc = newNotificationErrorFactory("PostgreSQL")

	errPGFormatError = pgErrFunc(`"format" value is invalid - it must be one of "%s" or "%s".`, formatNamespace, formatAccess)
	errPGTableError  = pgErrFunc("Table was not specified in the configuration.")
)

type postgreSQLNotify struct {
	Enable bool `json:"enable"`

	Format string `json:"format"`

	// Pass connection string in config directly. This string is
	// formatted according to
	// https://godoc.org/github.com/lib/pq#hdr-Connection_String_Parameters
	ConnectionString string `json:"connectionString"`
	// specifying a table name is required.
	Table string `json:"table"`

	// The values below, if non-empty are appended to
	// ConnectionString above. Default values are shown in
	// comments below (implicitly used by the library).
	Host     string `json:"host"`     // default: localhost
	Port     string `json:"port"`     // default: 5432
	User     string `json:"user"`     // default: user running minio
	Password string `json:"password"` // default: no password
	Database string `json:"database"` // default: same as user
}

func (p *postgreSQLNotify) Validate() error {
	if !p.Enable {
		return nil
	}
	if p.Format != formatNamespace && p.Format != formatAccess {
		return errPGFormatError
	}
	if p.ConnectionString == "" {
		if _, err := checkURL(p.Host); err != nil {
			return err
		}
	}
	if p.Table == "" {
		return errPGTableError
	}
	return nil
}

type pgConn struct {
	connStr       string
	table         string
	format        string
	preparedStmts map[string]*sql.Stmt
	*sql.DB
}

func dialPostgreSQL(pgN postgreSQLNotify) (pc pgConn, e error) {
	if !pgN.Enable {
		return pc, errNotifyNotEnabled
	}

	// collect connection params
	params := []string{pgN.ConnectionString}
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
	connStr := strings.Join(params, " ")

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return pc, pgErrFunc(
			"Connection opening failure (connectionString=%s): %v",
			connStr, err)
	}

	// ping to check that server is actually reachable.
	err = db.Ping()
	if err != nil {
		return pc, pgErrFunc("Ping to server failed with: %v",
			err)
	}

	// check that table exists - if not, create it.
	_, err = db.Exec(fmt.Sprintf(tableExists, pgN.Table))
	if err != nil {
		createStmt := createTableForNS
		if pgN.Format == formatAccess {
			createStmt = createTableForAccess
		}

		// most likely, table does not exist. try to create it:
		_, errCreate := db.Exec(fmt.Sprintf(createStmt, pgN.Table))
		if errCreate != nil {
			// failed to create the table. error out.
			return pc, pgErrFunc(
				"'Select' failed with %v, then 'Create Table' failed with %v",
				err, errCreate,
			)
		}
	}

	// create prepared statements
	stmts := make(map[string]*sql.Stmt)
	switch pgN.Format {
	case formatNamespace:
		// insert or update statement
		stmts["upsertRow"], err = db.Prepare(fmt.Sprintf(upsertRowForNS,
			pgN.Table))
		if err != nil {
			return pc, pgErrFunc(
				"create UPSERT prepared statement failed with: %v", err)
		}
		// delete statement
		stmts["deleteRow"], err = db.Prepare(fmt.Sprintf(deleteRowForNS,
			pgN.Table))
		if err != nil {
			return pc, pgErrFunc(
				"create DELETE prepared statement failed with: %v", err)
		}
	case formatAccess:
		// insert statement
		stmts["insertRow"], err = db.Prepare(fmt.Sprintf(insertRowForAccess,
			pgN.Table))
		if err != nil {
			return pc, pgErrFunc(
				"create INSERT prepared statement failed with: %v", err)
		}
	}

	return pgConn{connStr, pgN.Table, pgN.Format, stmts, db}, nil
}

func newPostgreSQLNotify(accountID string) (*logrus.Logger, error) {
	pgNotify := globalServerConfig.Notify.GetPostgreSQLByID(accountID)

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

func jsonEncodeEventData(d interface{}) ([]byte, error) {
	// json encode the value for the row
	value, err := json.Marshal(map[string]interface{}{
		"Records": d,
	})
	if err != nil {
		return nil, pgErrFunc(
			"Unable to encode event %v to JSON: %v", d, err)
	}
	return value, nil
}

func (pgC pgConn) Fire(entry *logrus.Entry) error {
	// get event type by trying to convert to string
	entryEventType, ok := entry.Data["EventType"].(string)
	if !ok {
		// ignore event if converting EventType to string
		// fails.
		return nil
	}

	switch pgC.format {
	case formatNamespace:
		// Check for event delete
		if eventMatch(entryEventType, []string{"s3:ObjectRemoved:*"}) {
			// delete row from the table
			_, err := pgC.preparedStmts["deleteRow"].Exec(entry.Data["Key"])
			if err != nil {
				return pgErrFunc(
					"Error deleting event with key=%v: %v",
					entry.Data["Key"], err,
				)
			}
		} else {
			value, err := jsonEncodeEventData(entry.Data["Records"])
			if err != nil {
				return err
			}

			// upsert row into the table
			_, err = pgC.preparedStmts["upsertRow"].Exec(entry.Data["Key"], value)
			if err != nil {
				return pgErrFunc(
					"Unable to upsert event with key=%v and value=%v: %v",
					entry.Data["Key"], entry.Data["Records"], err,
				)
			}
		}
	case formatAccess:
		// eventTime is taken from the first entry in the
		// records.
		events, ok := entry.Data["Records"].([]NotificationEvent)
		if !ok {
			return pgErrFunc("unable to extract event time due to conversion error of entry.Data[\"Records\"]=%v", entry.Data["Records"])
		}
		eventTime, err := time.Parse(timeFormatAMZ, events[0].EventTime)
		if err != nil {
			return pgErrFunc("unable to parse event time \"%s\": %v",
				events[0].EventTime, err)
		}

		value, err := jsonEncodeEventData(entry.Data["Records"])
		if err != nil {
			return err
		}

		_, err = pgC.preparedStmts["insertRow"].Exec(eventTime, value)
		if err != nil {
			return pgErrFunc("Unable to insert event with value=%v: %v",
				value, err)
		}
	}

	return nil
}

func (pgC pgConn) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.InfoLevel,
	}
}
