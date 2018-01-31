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

package target

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	_ "github.com/lib/pq" // Register postgres driver
	"github.com/minio/minio/pkg/event"
	xnet "github.com/minio/minio/pkg/net"
)

const (
	psqlTableExists          = `SELECT 1 FROM %s;`
	psqlCreateNamespaceTable = `CREATE TABLE %s (key VARCHAR PRIMARY KEY, value JSONB);`
	psqlCreateAccessTable    = `CREATE TABLE %s (event_time TIMESTAMP WITH TIME ZONE NOT NULL, event_data JSONB);`

	psqlUpdateRow = `INSERT INTO %s (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;`
	psqlDeleteRow = `DELETE FROM %s WHERE key = $1;`
	psqlInsertRow = `INSERT INTO %s (event_time, event_data) VALUES ($1, $2);`
)

// PostgreSQLArgs - PostgreSQL target arguments.
type PostgreSQLArgs struct {
	Enable           bool     `json:"enable"`
	Format           string   `json:"format"`
	ConnectionString string   `json:"connectionString"`
	Table            string   `json:"table"`
	Host             xnet.URL `json:"host"`     // default: localhost
	Port             string   `json:"port"`     // default: 5432
	User             string   `json:"user"`     // default: user running minio
	Password         string   `json:"password"` // default: no password
	Database         string   `json:"database"` // default: same as user
}

// PostgreSQLTarget - PostgreSQL target.
type PostgreSQLTarget struct {
	id         event.TargetID
	args       PostgreSQLArgs
	updateStmt *sql.Stmt
	deleteStmt *sql.Stmt
	insertStmt *sql.Stmt
	db         *sql.DB
}

// ID - returns target ID.
func (target *PostgreSQLTarget) ID() event.TargetID {
	return target.id
}

// Send - sends event to PostgreSQL.
func (target *PostgreSQLTarget) Send(eventData event.Event) error {
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

// Close - closes underneath connections to PostgreSQL database.
func (target *PostgreSQLTarget) Close() error {
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

// NewPostgreSQLTarget - creates new PostgreSQL target.
func NewPostgreSQLTarget(id string, args PostgreSQLArgs) (*PostgreSQLTarget, error) {
	params := []string{args.ConnectionString}
	if !args.Host.IsEmpty() {
		params = append(params, "host="+args.Host.String())
	}
	if args.Port != "" {
		params = append(params, "port="+args.Port)
	}
	if args.User != "" {
		params = append(params, "user="+args.User)
	}
	if args.Password != "" {
		params = append(params, "password="+args.Password)
	}
	if args.Database != "" {
		params = append(params, "dbname="+args.Database)
	}
	connStr := strings.Join(params, " ")

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	if err = db.Ping(); err != nil {
		return nil, err
	}

	if _, err = db.Exec(fmt.Sprintf(psqlTableExists, args.Table)); err != nil {
		createStmt := psqlCreateNamespaceTable
		if args.Format == event.AccessFormat {
			createStmt = psqlCreateAccessTable
		}

		if _, err = db.Exec(fmt.Sprintf(createStmt, args.Table)); err != nil {
			return nil, err
		}
	}

	var updateStmt, deleteStmt, insertStmt *sql.Stmt
	switch args.Format {
	case event.NamespaceFormat:
		// insert or update statement
		if updateStmt, err = db.Prepare(fmt.Sprintf(psqlUpdateRow, args.Table)); err != nil {
			return nil, err
		}
		// delete statement
		if deleteStmt, err = db.Prepare(fmt.Sprintf(psqlDeleteRow, args.Table)); err != nil {
			return nil, err
		}
	case event.AccessFormat:
		// insert statement
		if insertStmt, err = db.Prepare(fmt.Sprintf(psqlInsertRow, args.Table)); err != nil {
			return nil, err
		}
	}

	return &PostgreSQLTarget{
		id:         event.TargetID{id, "postgresql"},
		args:       args,
		updateStmt: updateStmt,
		deleteStmt: deleteStmt,
		insertStmt: insertStmt,
		db:         db,
	}, nil
}
