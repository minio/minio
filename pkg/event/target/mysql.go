/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

package target

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/minio/minio/pkg/event"
	xnet "github.com/minio/minio/pkg/net"
)

const (
	mysqlTableExists          = `SELECT 1 FROM %s;`
	mysqlCreateNamespaceTable = `CREATE TABLE %s (key_name VARCHAR(2048), value JSON, PRIMARY KEY (key_name));`
	mysqlCreateAccessTable    = `CREATE TABLE %s (event_time DATETIME NOT NULL, event_data JSON);`

	mysqlUpdateRow = `INSERT INTO %s (key_name, value) VALUES (?, ?) ON DUPLICATE KEY UPDATE value=VALUES(value);`
	mysqlDeleteRow = `DELETE FROM %s WHERE key_name = ?;`
	mysqlInsertRow = `INSERT INTO %s (event_time, event_data) VALUES (?, ?);`
)

// MySQLArgs - MySQL target arguments.
type MySQLArgs struct {
	Enable   bool     `json:"enable"`
	Format   string   `json:"format"`
	DSN      string   `json:"dsnString"`
	Table    string   `json:"table"`
	Host     xnet.URL `json:"host"`
	Port     string   `json:"port"`
	User     string   `json:"user"`
	Password string   `json:"password"`
	Database string   `json:"database"`
}

// MySQLTarget - MySQL target.
type MySQLTarget struct {
	id         event.TargetID
	args       MySQLArgs
	updateStmt *sql.Stmt
	deleteStmt *sql.Stmt
	insertStmt *sql.Stmt
	db         *sql.DB
}

// ID - returns target ID.
func (target *MySQLTarget) ID() event.TargetID {
	return target.id
}

// Send - sends event to MySQL.
func (target *MySQLTarget) Send(eventData event.Event) error {
	if target.args.Format == event.NamespaceFormat {
		objectName, err := url.QueryUnescape(eventData.S3.Object.Key)
		if err != nil {
			return err
		}
		key := eventData.S3.Bucket.Name + "/" + objectName

		if eventData.EventName == event.ObjectRemovedDelete {
			_, err = target.deleteStmt.Exec(key)
		} else {
			var data []byte
			if data, err = json.Marshal(struct{ Records []event.Event }{[]event.Event{eventData}}); err != nil {
				return err
			}

			_, err = target.updateStmt.Exec(key, data)
		}
		return err
	}

	if target.args.Format == event.AccessFormat {
		eventTime, err := time.Parse(event.AMZTimeFormat, eventData.EventTime)
		if err != nil {
			return err
		}

		data, err := json.Marshal(struct{ Records []event.Event }{[]event.Event{eventData}})
		if err != nil {
			return err
		}

		_, err = target.insertStmt.Exec(eventTime, data)
		return err
	}

	return nil
}

// Close - closes underneath connections to MySQL database.
func (target *MySQLTarget) Close() error {
	if target.updateStmt != nil {
		// FIXME: log returned error. ignore time being.
		_ = target.updateStmt.Close()
	}

	if target.deleteStmt != nil {
		// FIXME: log returned error. ignore time being.
		_ = target.deleteStmt.Close()
	}

	if target.insertStmt != nil {
		// FIXME: log returned error. ignore time being.
		_ = target.insertStmt.Close()
	}

	return target.db.Close()
}

// NewMySQLTarget - creates new MySQL target.
func NewMySQLTarget(id string, args MySQLArgs) (*MySQLTarget, error) {
	if args.DSN == "" {
		config := mysql.Config{
			User:   args.User,
			Passwd: args.Password,
			Net:    "tcp",
			Addr:   args.Host.String() + ":" + args.Port,
			DBName: args.Database,
		}

		args.DSN = config.FormatDSN()
	}

	db, err := sql.Open("mysql", args.DSN)
	if err != nil {
		return nil, err
	}

	if err = db.Ping(); err != nil {
		return nil, err
	}

	if _, err = db.Exec(fmt.Sprintf(mysqlTableExists, args.Table)); err != nil {
		createStmt := mysqlCreateNamespaceTable
		if args.Format == event.AccessFormat {
			createStmt = mysqlCreateAccessTable
		}

		if _, err = db.Exec(fmt.Sprintf(createStmt, args.Table)); err != nil {
			return nil, err
		}
	}

	var updateStmt, deleteStmt, insertStmt *sql.Stmt
	switch args.Format {
	case event.NamespaceFormat:
		// insert or update statement
		if updateStmt, err = db.Prepare(fmt.Sprintf(mysqlUpdateRow, args.Table)); err != nil {
			return nil, err
		}
		// delete statement
		if deleteStmt, err = db.Prepare(fmt.Sprintf(mysqlDeleteRow, args.Table)); err != nil {
			return nil, err
		}
	case event.AccessFormat:
		// insert statement
		if insertStmt, err = db.Prepare(fmt.Sprintf(mysqlInsertRow, args.Table)); err != nil {
			return nil, err
		}
	}

	return &MySQLTarget{
		id:         event.TargetID{id, "mysql"},
		args:       args,
		updateStmt: updateStmt,
		deleteStmt: deleteStmt,
		insertStmt: insertStmt,
		db:         db,
	}, nil
}
