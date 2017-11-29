/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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

// MySQL Notifier implementation. Two formats, "namespace" and
// "access" are supported.
//
// * Namespace format
//
// On each create or update object event in Minio Object storage
// server, a row is created or updated in the table in MySQL. On each
// object removal, the corresponding row is deleted from the table.
//
// A table with a specific structure (column names, column types, and
// primary key/uniqueness constraint) is used. The user may set the
// table name in the configuration. A sample SQL command that creates
// a command with the required structure is:
//
//     CREATE TABLE myminio (
//         key_name VARCHAR(2048),
//         value JSONB,
//         PRIMARY KEY (key_name),
//     );
//
// MySQL's "INSERT ... ON DUPLICATE ..." feature (UPSERT) is used
// here. The implementation has been tested with MySQL Ver 14.14
// Distrib 5.7.17.
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
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/go-sql-driver/mysql"
)

const (
	// Queries for format=namespace mode.
	upsertRowForNSMySQL = `INSERT INTO %s (key_name, value)
VALUES (?, ?)
ON DUPLICATE KEY UPDATE value=VALUES(value);
`
	deleteRowForNSMySQL = ` DELETE FROM %s
WHERE key_name = ?;`
	createTableForNSMySQL = `CREATE TABLE %s (
    key_name VARCHAR(2048),
    value JSON,
    PRIMARY KEY (key_name)
);`

	// Queries for format=access mode.
	insertRowForAccessMySQL = `INSERT INTO %s (event_time, event_data)
VALUES (?, ?);`
	createTableForAccessMySQL = `CREATE TABLE %s (
    event_time DATETIME NOT NULL,
    event_data JSON
);`

	// Query to check if a table already exists.
	tableExistsMySQL = `SELECT 1 FROM %s;`
)

var (
	mysqlErrFunc = newNotificationErrorFactory("MySQL")

	errMysqlFormat = mysqlErrFunc(`"format" value is invalid - it must be one of "%s" or "%s".`, formatNamespace, formatAccess)
	errMysqlTable  = mysqlErrFunc("Table was not specified in the configuration.")
)

type mySQLNotify struct {
	Enable bool `json:"enable"`

	Format string `json:"format"`

	// pass data-source-name connection string in config
	// directly. This string is formatted according to
	// https://github.com/go-sql-driver/mysql#dsn-data-source-name
	DsnString string `json:"dsnString"`
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

func (m *mySQLNotify) Validate() error {
	if !m.Enable {
		return nil
	}
	if m.Format != formatNamespace && m.Format != formatAccess {
		return errMysqlFormat
	}
	if m.DsnString == "" {
		if _, err := checkURL(m.Host); err != nil {
			return err
		}
	}
	if m.Table == "" {
		return errMysqlTable
	}
	return nil
}

type mySQLConn struct {
	dsnStr        string
	table         string
	format        string
	preparedStmts map[string]*sql.Stmt
	*sql.DB
}

func dialMySQL(msql mySQLNotify) (mc mySQLConn, e error) {
	if !msql.Enable {
		return mc, errNotifyNotEnabled
	}

	dsnStr := msql.DsnString
	// check if connection string is specified
	if dsnStr == "" {
		// build from other parameters
		config := mysql.Config{
			User:   msql.User,
			Passwd: msql.Password,
			Net:    "tcp",
			Addr:   msql.Host + ":" + msql.Port,
			DBName: msql.Database,
		}
		dsnStr = config.FormatDSN()
	}

	db, err := sql.Open("mysql", dsnStr)
	if err != nil {
		return mc, mysqlErrFunc(
			"Connection opening failure (dsnStr=%s): %v",
			dsnStr, err)
	}

	// ping to check that server is actually reachable.
	err = db.Ping()
	if err != nil {
		return mc, mysqlErrFunc(
			"Ping to server failed with: %v", err)
	}

	// check that table exists - if not, create it.
	_, err = db.Exec(fmt.Sprintf(tableExistsMySQL, msql.Table))
	if err != nil {
		createStmt := createTableForNSMySQL
		if msql.Format == formatAccess {
			createStmt = createTableForAccessMySQL
		}

		// most likely, table does not exist. try to create it:
		_, errCreate := db.Exec(fmt.Sprintf(createStmt, msql.Table))
		if errCreate != nil {
			// failed to create the table. error out.
			return mc, mysqlErrFunc(
				"'Select' failed with %v, then 'Create Table' failed with %v",
				err, errCreate,
			)
		}
	}

	// create prepared statements
	stmts := make(map[string]*sql.Stmt)
	switch msql.Format {
	case formatNamespace:
		// insert or update statement
		stmts["upsertRow"], err = db.Prepare(fmt.Sprintf(upsertRowForNSMySQL,
			msql.Table))
		if err != nil {
			return mc, mysqlErrFunc("create UPSERT prepared statement failed with: %v", err)
		}
		// delete statement
		stmts["deleteRow"], err = db.Prepare(fmt.Sprintf(deleteRowForNSMySQL,
			msql.Table))
		if err != nil {
			return mc, mysqlErrFunc("create DELETE prepared statement failed with: %v", err)
		}
	case formatAccess:
		// insert statement
		stmts["insertRow"], err = db.Prepare(fmt.Sprintf(insertRowForAccessMySQL,
			msql.Table))
		if err != nil {
			return mc, mysqlErrFunc(
				"create INSERT prepared statement failed with: %v", err)
		}

	}
	return mySQLConn{dsnStr, msql.Table, msql.Format, stmts, db}, nil
}

func newMySQLNotify(accountID string) (*logrus.Logger, error) {
	mysqlNotify := globalServerConfig.Notify.GetMySQLByID(accountID)

	// Dial mysql
	myC, err := dialMySQL(mysqlNotify)
	if err != nil {
		return nil, err
	}

	mySQLLog := logrus.New()

	mySQLLog.Out = ioutil.Discard

	mySQLLog.Formatter = new(logrus.JSONFormatter)

	mySQLLog.Hooks.Add(myC)

	return mySQLLog, nil
}

func (myC mySQLConn) Close() {
	// first close all prepared statements
	for _, v := range myC.preparedStmts {
		_ = v.Close()
	}
	// close db connection
	_ = myC.DB.Close()
}

func (myC mySQLConn) Fire(entry *logrus.Entry) error {
	// get event type by trying to convert to string
	entryEventType, ok := entry.Data["EventType"].(string)
	if !ok {
		// ignore event if converting EventType to string
		// fails.
		return nil
	}

	jsonEncoder := func(d interface{}) ([]byte, error) {
		value, err := json.Marshal(map[string]interface{}{
			"Records": d,
		})
		if err != nil {
			return nil, mysqlErrFunc(
				"Unable to encode event %v to JSON: %v", d, err)
		}
		return value, nil
	}

	switch myC.format {
	case formatNamespace:
		// Check for event delete
		if eventMatch(entryEventType, []string{"s3:ObjectRemoved:*"}) {
			// delete row from the table
			_, err := myC.preparedStmts["deleteRow"].Exec(entry.Data["Key"])
			if err != nil {
				return mysqlErrFunc(
					"Error deleting event with key = %v - got mysql error - %v",
					entry.Data["Key"], err,
				)
			}
		} else {
			value, err := jsonEncoder(entry.Data["Records"])
			if err != nil {
				return err
			}

			// upsert row into the table
			_, err = myC.preparedStmts["upsertRow"].Exec(entry.Data["Key"], value)
			if err != nil {
				return mysqlErrFunc(
					"Unable to upsert event with Key=%v and Value=%v - got mysql error - %v",
					entry.Data["Key"], entry.Data["Records"], err,
				)
			}
		}
	case formatAccess:
		// eventTime is taken from the first entry in the
		// records.
		events, ok := entry.Data["Records"].([]NotificationEvent)
		if !ok {
			return mysqlErrFunc("unable to extract event time due to conversion error of entry.Data[\"Records\"]=%v", entry.Data["Records"])
		}
		eventTime, err := time.Parse(timeFormatAMZ, events[0].EventTime)
		if err != nil {
			return mysqlErrFunc("unable to parse event time \"%s\": %v",
				events[0].EventTime, err)
		}

		value, err := jsonEncodeEventData(entry.Data["Records"])
		if err != nil {
			return err
		}

		_, err = myC.preparedStmts["insertRow"].Exec(eventTime, value)
		if err != nil {
			return mysqlErrFunc("Unable to insert event with value=%v: %v",
				value, err)
		}
	}

	return nil
}

func (myC mySQLConn) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.InfoLevel,
	}
}
