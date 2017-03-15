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

// MySQL Notifier implementation. A table with a specific
// structure (column names, column types, and primary key/uniqueness
// constraint) is used. The user may set the table name in the
// configuration. A sample SQL command that creates a command with the
// required structure is:
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
// On each create or update object event in Minio Object storage
// server, a row is created or updated in the table in MySQL. On
// each object removal, the corresponding row is deleted from the
// table.

package cmd

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/Sirupsen/logrus"
	"github.com/go-sql-driver/mysql"
)

const (
	upsertRowMySQL = `INSERT INTO %s (key_name, value)
VALUES (?, ?)
ON DUPLICATE KEY UPDATE value=VALUES(value);
`
	deleteRowMySQL = ` DELETE FROM %s
WHERE key_name = ?;`
	createTableMySQL = `CREATE TABLE %s (
    key_name VARCHAR(2048),
    value JSON,
    PRIMARY KEY (key_name)
);`
	tableExistsMySQL = `SELECT 1 FROM %s;`
)

type mySQLNotify struct {
	Enable bool `json:"enable"`

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
	if m.DsnString == "" {
		if _, err := checkURL(m.Host); err != nil {
			return err
		}
	}
	if m.Table == "" {
		return fmt.Errorf(
			"MySQL Notifier Error: Table was not specified in configuration")
	}
	return nil
}

type mySQLConn struct {
	dsnStr        string
	table         string
	preparedStmts map[string]*sql.Stmt
	*sql.DB
}

func dialMySQL(msql mySQLNotify) (mySQLConn, error) {
	if !msql.Enable {
		return mySQLConn{}, errNotifyNotEnabled
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
		return mySQLConn{}, fmt.Errorf(
			"MySQL Notifier Error: Connection opening failure (dsnStr=%s): %v",
			dsnStr, err,
		)
	}

	// ping to check that server is actually reachable.
	err = db.Ping()
	if err != nil {
		return mySQLConn{}, fmt.Errorf(
			"MySQL Notifier Error: Ping to server failed with: %v",
			err,
		)
	}

	// check that table exists - if not, create it.
	_, err = db.Exec(fmt.Sprintf(tableExistsMySQL, msql.Table))
	if err != nil {
		// most likely, table does not exist. try to create it:
		_, errCreate := db.Exec(fmt.Sprintf(createTableMySQL, msql.Table))
		if errCreate != nil {
			// failed to create the table. error out.
			return mySQLConn{}, fmt.Errorf(
				"MySQL Notifier Error: 'Select' failed with %v, then 'Create Table' failed with %v",
				err, errCreate,
			)
		}
	}

	// create prepared statements
	stmts := make(map[string]*sql.Stmt)
	// insert or update statement
	stmts["upsertRow"], err = db.Prepare(fmt.Sprintf(upsertRowMySQL, msql.Table))
	if err != nil {
		return mySQLConn{},
			fmt.Errorf("MySQL Notifier Error: create UPSERT prepared statement failed with: %v", err)
	}
	stmts["deleteRow"], err = db.Prepare(fmt.Sprintf(deleteRowMySQL, msql.Table))
	if err != nil {
		return mySQLConn{},
			fmt.Errorf("MySQL Notifier Error: create DELETE prepared statement failed with: %v", err)
	}

	return mySQLConn{dsnStr, msql.Table, stmts, db}, nil
}

func newMySQLNotify(accountID string) (*logrus.Logger, error) {
	mysqlNotify := serverConfig.Notify.GetMySQLByID(accountID)

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

	// Check for event delete
	if eventMatch(entryEventType, []string{"s3:ObjectRemoved:*"}) {
		// delete row from the table
		_, err := myC.preparedStmts["deleteRow"].Exec(entry.Data["Key"])
		if err != nil {
			return fmt.Errorf(
				"Error deleting event with key = %v - got mysql error - %v",
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
		_, err = myC.preparedStmts["upsertRow"].Exec(entry.Data["Key"], value)
		if err != nil {
			return fmt.Errorf(
				"Unable to upsert event with Key=%v and Value=%v - got mysql error - %v",
				entry.Data["Key"], entry.Data["Records"], err,
			)
		}
	}

	return nil
}

func (myC mySQLConn) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.InfoLevel,
	}
}
