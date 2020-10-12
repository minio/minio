/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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
// On each create or update object event in MinIO Object storage
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
// command that creates a table with the required structure is:
//
// CREATE TABLE myminio (
//     event_time TIMESTAMP WITH TIME ZONE NOT NULL,
//     event_data JSONB
// );

package target

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq" // Register postgres driver

	"github.com/minio/minio/pkg/event"
	xnet "github.com/minio/minio/pkg/net"
)

const (
	pgCreateAccessTable = `CREATE TABLE %s (event_time TIMESTAMP WITH TIME ZONE NOT NULL, event_data JSONB);`
	pgInsertRow         = `INSERT INTO %s (event_time, event_data) VALUES ($1, $2);`

	pgCreateNamespaceTable = `CREATE TABLE %s (
                                      bucket           VARCHAR NOT NULL,
                                      object           VARCHAR NOT NULL,
                                      version_id       VARCHAR DEFAULT 'NULL_VERSION' NOT NULL,
                                      is_delete_marker BOOL DEFAULT false NOT NULL,
                                      size             INT8,
                                      etag             VARCHAR,
                                      last_modified    TIMESTAMPTZ NOT NULL,
                                      event_data       JSONB NOT NULL,
                                      PRIMARY KEY (bucket, object, version_id)
                                  );`
	pgNamespaceTableSchemaV0 = `SELECT key, value FROM %s WHERE false;`

	pgNamespaceTableSchemaV1 = `SELECT bucket, object, version_id, is_delete_marker, size, etag, last_modified, event_data
                                      FROM %s
                                     WHERE false;`

	pgNullVersion = "NULL_VERSION" // implies NULL version id in the namespace table.

	// For PUT objects, replace the row if it already exists.
	pgNsInsertForPuts = `INSERT INTO %s (bucket, object, version_id, is_delete_marker, size, etag, last_modified, event_data)
                                     VALUES ($1,     $2,     $3,         $4,               $5,   $6,   $7,            $8)
                                     ON CONFLICT (bucket, object, version_id)
                                        DO UPDATE SET is_delete_marker = EXCLUDED.is_delete_marker,
                                                      size = EXCLUDED.size,
                                                      etag = EXCLUDED.etag,
                                                      last_modified = EXCLUDED.last_modified,
                                                      event_data = EXCLUDED.event_data;`
	// The ON CONFLICT DO NOTHING clause, allows GetObject/HeadObject events
	// to add rows to the table if a row matching the object and version is
	// not present.
	pgNsInsertForGetsOrHeads = `INSERT INTO %s (bucket, object, version_id, is_delete_marker, size, etag, last_modified, event_data)
                                            VALUES ($1,     $2,     $3,         $4,               $5,   $6,   $7,            $8)
                                       ON CONFLICT (bucket, object, version_id)
                                          DO NOTHING;`
	pgNsDeleteRow = `DELETE FROM %s WHERE bucket = $1 AND object = $2 AND version_id = $3;`
)

// Postgres constants
const (
	PostgresFormat             = "format"
	PostgresConnectionString   = "connection_string"
	PostgresTable              = "table"
	PostgresHost               = "host"
	PostgresPort               = "port"
	PostgresUsername           = "username"
	PostgresPassword           = "password"
	PostgresDatabase           = "database"
	PostgresQueueDir           = "queue_dir"
	PostgresQueueLimit         = "queue_limit"
	PostgresMaxOpenConnections = "max_open_connections"

	EnvPostgresEnable             = "MINIO_NOTIFY_POSTGRES_ENABLE"
	EnvPostgresFormat             = "MINIO_NOTIFY_POSTGRES_FORMAT"
	EnvPostgresConnectionString   = "MINIO_NOTIFY_POSTGRES_CONNECTION_STRING"
	EnvPostgresTable              = "MINIO_NOTIFY_POSTGRES_TABLE"
	EnvPostgresHost               = "MINIO_NOTIFY_POSTGRES_HOST"
	EnvPostgresPort               = "MINIO_NOTIFY_POSTGRES_PORT"
	EnvPostgresUsername           = "MINIO_NOTIFY_POSTGRES_USERNAME"
	EnvPostgresPassword           = "MINIO_NOTIFY_POSTGRES_PASSWORD"
	EnvPostgresDatabase           = "MINIO_NOTIFY_POSTGRES_DATABASE"
	EnvPostgresQueueDir           = "MINIO_NOTIFY_POSTGRES_QUEUE_DIR"
	EnvPostgresQueueLimit         = "MINIO_NOTIFY_POSTGRES_QUEUE_LIMIT"
	EnvPostgresMaxOpenConnections = "MINIO_NOTIFY_POSTGRES_MAX_OPEN_CONNECTIONS"
)

// PostgreSQLArgs - PostgreSQL target arguments.
type PostgreSQLArgs struct {
	Enable             bool      `json:"enable"`
	Format             string    `json:"format"`
	ConnectionString   string    `json:"connectionString"`
	Table              string    `json:"table"`
	Host               xnet.Host `json:"host"`     // default: localhost
	Port               string    `json:"port"`     // default: 5432
	Username           string    `json:"username"` // default: user running minio
	Password           string    `json:"password"` // default: no password
	Database           string    `json:"database"` // default: same as user
	QueueDir           string    `json:"queueDir"`
	QueueLimit         uint64    `json:"queueLimit"`
	MaxOpenConnections int       `json:"maxOpenConnections"`
}

// Validate PostgreSQLArgs fields
func (p PostgreSQLArgs) Validate() error {
	if !p.Enable {
		return nil
	}
	if p.Table == "" {
		return fmt.Errorf("empty table name")
	}
	if p.Format != "" {
		f := strings.ToLower(p.Format)
		if f != event.NamespaceFormat && f != event.AccessFormat {
			return fmt.Errorf("unrecognized format value")
		}
	}

	if p.ConnectionString != "" {
		// No pq API doesn't help to validate connection string
		// prior connection, so no validation for now.
	} else {
		// Some fields need to be specified when ConnectionString is unspecified
		if p.Port == "" {
			return fmt.Errorf("unspecified port")
		}
		if _, err := strconv.Atoi(p.Port); err != nil {
			return fmt.Errorf("invalid port")
		}
		if p.Database == "" {
			return fmt.Errorf("database unspecified")
		}
	}

	if p.QueueDir != "" {
		if !filepath.IsAbs(p.QueueDir) {
			return errors.New("queueDir path should be absolute")
		}
	}

	if p.MaxOpenConnections < 0 {
		return errors.New("maxOpenConnections cannot be less than zero")
	}

	return nil
}

// PostgreSQLTarget - PostgreSQL target.
type PostgreSQLTarget struct {
	id                  event.TargetID
	args                PostgreSQLArgs
	nsInsertStmtForPuts *sql.Stmt
	nsInsertStmtForGets *sql.Stmt
	nsDeleteStmt        *sql.Stmt
	accInsertStmt       *sql.Stmt
	db                  *sql.DB
	store               Store
	firstPing           bool
	connString          string
	loggerOnce          func(ctx context.Context, err error, id interface{}, errKind ...interface{})
}

// ID - returns target ID.
func (target *PostgreSQLTarget) ID() event.TargetID {
	return target.id
}

// HasQueueStore - Checks if the queueStore has been configured for the target
func (target *PostgreSQLTarget) HasQueueStore() bool {
	return target.store != nil
}

// IsActive - Return true if target is up and active
func (target *PostgreSQLTarget) IsActive() (bool, error) {
	if target.db == nil {
		db, err := sql.Open("postgres", target.connString)
		if err != nil {
			return false, err
		}
		target.db = db
		if target.args.MaxOpenConnections > 0 {
			// Set the maximum connections limit
			target.db.SetMaxOpenConns(target.args.MaxOpenConnections)
		}
	}
	if err := target.db.Ping(); err != nil {
		if IsConnErr(err) {
			return false, errNotConnected
		}
		return false, err
	}
	return true, nil
}

// Save - saves the events to the store if questore is configured, which will be replayed when the PostgreSQL connection is active.
func (target *PostgreSQLTarget) Save(eventData event.Event) error {
	if target.store != nil {
		return target.store.Put(eventData)
	}
	_, err := target.IsActive()
	if err != nil {
		return err
	}
	return target.send(eventData)
}

// IsConnErr - To detect a connection error.
func IsConnErr(err error) bool {
	if err == nil {
		return false
	}
	return IsConnRefusedErr(err) || err.Error() == "sql: database is closed" || err.Error() == "sql: statement is closed" || err.Error() == "invalid connection"
}

func (target *PostgreSQLTarget) doesTableExist(tableName string) (bool, error) {
	const pgTableExists = `SELECT 1 FROM %s WHERE false;`
	_, err := target.db.Exec(fmt.Sprintf(pgTableExists, tableName))
	if IsConnErr(err) {
		return false, err
	}
	return err == nil, nil
}

// send - sends an event to the PostgreSQL.
func (target *PostgreSQLTarget) send(eventData event.Event) error {
	if target.args.Format == event.NamespaceFormat {
		objectName, err := url.QueryUnescape(eventData.S3.Object.Key)
		if err != nil {
			return err
		}
		versionID := eventData.S3.Object.VersionID
		if versionID == "" {
			versionID = pgNullVersion
		}

		switch {
		case eventData.EventName == event.ObjectRemovedDelete && !eventData.S3.Object.DeleteMarker:
			args := []interface{}{
				eventData.S3.Bucket.Name,
				objectName,
				versionID,
			}
			_, err = target.nsDeleteStmt.Exec(args...)
		case eventData.EventName == event.ObjectCreatedPut ||
			(eventData.EventName == event.ObjectRemovedDelete &&
				eventData.S3.Object.DeleteMarker):
			var data []byte
			if data, err = json.Marshal(eventData); err != nil {
				return err
			}

			args := []interface{}{
				eventData.S3.Bucket.Name,
				objectName,
				versionID,
				eventData.S3.Object.DeleteMarker,
				eventData.S3.Object.Size,
				eventData.S3.Object.ETag,
				eventData.S3.Object.LastModified,
				data,
			}
			if eventData.S3.Object.DeleteMarker {
				// Set size nad etag as NULL in the table
				args[4] = nil
				args[5] = nil
			}
			_, err = target.nsInsertStmtForPuts.Exec(args...)
		case eventData.EventName == event.ObjectAccessedGet || eventData.EventName == event.ObjectAccessedHead:
			var data []byte
			if data, err = json.Marshal(eventData); err != nil {
				return err
			}

			args := []interface{}{
				eventData.S3.Bucket.Name,
				objectName,
				versionID,
				eventData.S3.Object.DeleteMarker,
				eventData.S3.Object.Size,
				eventData.S3.Object.ETag,
				eventData.S3.Object.LastModified,
				data,
			}
			if eventData.S3.Object.DeleteMarker {
				// Set size nad etag as NULL in the table
				args[4] = nil
				args[5] = nil
			}
			_, err = target.nsInsertStmtForGets.Exec(args...)
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

		if _, err = target.accInsertStmt.Exec(eventTime, data); err != nil {
			return err
		}
	}

	return nil
}

// Send - reads an event from store and sends it to PostgreSQL.
func (target *PostgreSQLTarget) Send(eventKey string) error {
	_, err := target.IsActive()
	if err != nil {
		return err
	}
	if !target.firstPing {
		if err := target.initPGTable(); err != nil {
			if IsConnErr(err) {
				return errNotConnected
			}
			return err
		}
	}

	eventData, eErr := target.store.Get(eventKey)
	if eErr != nil {
		// The last event key in a successful batch will be sent in the channel atmost once by the replayEvents()
		// Such events will not exist and wouldve been already been sent successfully.
		if os.IsNotExist(eErr) {
			return nil
		}
		return eErr
	}

	if err := target.send(eventData); err != nil {
		if IsConnErr(err) {
			return errNotConnected
		}
		return err
	}

	// Delete the event from store.
	return target.store.Del(eventKey)
}

// Close - closes underneath connections to PostgreSQL database.
func (target *PostgreSQLTarget) Close() error {
	if target.nsInsertStmtForPuts != nil {
		// FIXME: log returned error. ignore time being.
		_ = target.nsInsertStmtForPuts.Close()
	}
	if target.nsInsertStmtForGets != nil {
		// FIXME: log returned error. ignore time being.
		_ = target.nsInsertStmtForGets.Close()
	}

	if target.nsDeleteStmt != nil {
		// FIXME: log returned error. ignore time being.
		_ = target.nsDeleteStmt.Close()
	}

	if target.accInsertStmt != nil {
		// FIXME: log returned error. ignore time being.
		_ = target.accInsertStmt.Close()
	}

	return target.db.Close()
}

func (target *PostgreSQLTarget) createTableAndMigrateData() error {
	if exists, err := target.doesTableExist(target.args.Table); err != nil {
		return err
	} else if !exists {
		// Table does not already exist, create it:
		createStmt := pgCreateNamespaceTable
		if target.args.Format == event.AccessFormat {
			createStmt = pgCreateAccessTable
		}

		if _, dbErr := target.db.Exec(fmt.Sprintf(createStmt, target.args.Table)); dbErr != nil {
			return dbErr
		}
		return nil
	}

	// For access format, as table already exists, nothing to migrate.
	if target.args.Format == event.AccessFormat {
		return nil
	}

	// For namespace table format, we may have to migrate to new version.
	renamedTable := fmt.Sprintf("%s_v0_tmp", target.args.Table)

	// Check if existing table is V0
	if _, err := target.db.Exec(fmt.Sprintf(pgNamespaceTableSchemaV0, target.args.Table)); err != nil {
		if IsConnErr(err) {
			return err
		}
	} else {
		// Table schema is V0. Migrate to V1.
		const pgMigrateV0RenameTable = `ALTER TABLE %s RENAME TO %s;`

		// Rename existing table
		_, err = target.db.Exec(fmt.Sprintf(pgMigrateV0RenameTable, target.args.Table, renamedTable))
		if err != nil {
			return fmt.Errorf("Failed to rename existing table with %v", err)
		}

		// Create new table
		if _, dbErr := target.db.Exec(fmt.Sprintf(pgCreateNamespaceTable, target.args.Table)); dbErr != nil {
			// Attempt to rollback table rename.
			_, err = target.db.Exec(fmt.Sprintf(pgMigrateV0RenameTable, renamedTable, target.args.Table))
			if err != nil {
				return fmt.Errorf("Rollback of renamed table failed with %v; creation of new table failed with %v", err, dbErr)
			}

			return fmt.Errorf("Failed to create new table after renaming old table with %v", dbErr)
		}

		// Start data migration thread
		go target.migrateRowsFromV0ToV1(renamedTable, target.args.Table)
		return nil
	}

	// Check if existing table is V1
	if _, err := target.db.Exec(fmt.Sprintf(pgNamespaceTableSchemaV1, target.args.Table)); err != nil {
		if IsConnErr(err) {
			return err
		}
	} else {
		// Table schema is V1 and current, however a migration may be
		// incomplete - check and resume migration.
		if oldTableExists, err := target.doesTableExist(renamedTable); err != nil {
			return err
		} else if oldTableExists {
			// Old renamed table exists. Continue data migration
			go target.migrateRowsFromV0ToV1(renamedTable, target.args.Table)
		}

		// Old table does not exist, nothing to do.
		return nil
	}

	return errors.New("table exists and has an invalid schema")
}

func (target *PostgreSQLTarget) migrateRowsFromV0ToV1(oldTable, newTable string) {
	const (
		pgMigrateV0InsertRows = `INSERT INTO %s (bucket, object, version_id, is_delete_marker, size, etag, last_modified, event_data)
                                              SELECT left(key, strpos(key, '/')-1),
                                                     right(key, -strpos(key, '/')),
                                                     COALESCE(value->'Records'->0->'s3'->'object'->>'versionId', 'NULL_VERSION'),
                                                     COALESCE((value->'Records'->0->'s3'->'object'->>'deleteMarker')::bool, false),
                                                     (value->'Records'->0->'s3'->'object'->>'size')::int8,
                                                     value->'Records'->0->'s3'->'object'->>'eTag',
                                                     COALESCE((value->'Records'->0->'s3'->'object'->>'lastModified')::timestamptz, '-infinity'),
                                                     value
                                                FROM %s
                                                ORDER BY key ASC
                                                LIMIT 1000
                                         ON CONFLICT DO NOTHING;`
		pgMigrateV0RemoveRows = `DELETE FROM %s WHERE key IN (SELECT key FROM %s ORDER BY key ASC LIMIT 1000);`
		pgMigrateV0HasRows    = `SELECT CASE
                                               WHEN EXISTS (SELECT * FROM %s LIMIT 1) THEN true
                                               ELSE false
                                             END;`
		pgMigrateDropTable = `DROP TABLE %s;`
	)
	hasRows := func() (bool, error) {
		rows, err := target.db.Query(fmt.Sprintf(pgMigrateV0HasRows, oldTable))
		if err != nil {
			return false, err
		}

		defer rows.Close()
		var ret bool
		for rows.Next() {
			if e := rows.Scan(&ret); e != nil {
				return false, e
			}
		}
		return ret, nil
	}
	copyRowSet := func() error {
		tx, err := target.db.Begin()
		if err != nil {
			return err
		}
		defer tx.Rollback()

		if _, err := tx.Exec(fmt.Sprintf(pgMigrateV0InsertRows, newTable, oldTable)); err != nil {
			return err
		}

		if _, err := tx.Exec(fmt.Sprintf(pgMigrateV0RemoveRows, oldTable, oldTable)); err != nil {
			return err
		}

		return tx.Commit()
	}

	for {
		if yes, err := hasRows(); err != nil {
			oErr := fmt.Errorf("Error checking if old notifications table has rows (%v) - data migration thread will quit and reattempt on server restart.", err)
			target.loggerOnce(context.Background(), oErr, target.ID())
			return
		} else if !yes {
			break
		}

		if err := copyRowSet(); err != nil {
			oErr := fmt.Errorf("Error copying notification rows from old table to new table (%v) - data migration thread will quit and reattempt on server restart.", err)
			target.loggerOnce(context.Background(), oErr, target.ID())
			return
		}
	}

	// Done migration, drop old table.
	_, err := target.db.Exec(fmt.Sprintf(pgMigrateDropTable, oldTable))
	if err != nil {
		oErr := fmt.Errorf("Error removing old table (%v) - data migration thread will quit and reattempt on server restart.", err)
		target.loggerOnce(context.Background(), oErr, target.ID())
	}
}

func (target *PostgreSQLTarget) prepareStatements() error {
	var err error
	switch target.args.Format {
	case event.NamespaceFormat:
		// insert statements
		if target.nsInsertStmtForPuts, err = target.db.Prepare(fmt.Sprintf(pgNsInsertForPuts, target.args.Table)); err != nil {
			return err
		}
		if target.nsInsertStmtForGets, err = target.db.Prepare(fmt.Sprintf(pgNsInsertForGetsOrHeads, target.args.Table)); err != nil {
			return err
		}
		// delete statement
		if target.nsDeleteStmt, err = target.db.Prepare(fmt.Sprintf(pgNsDeleteRow, target.args.Table)); err != nil {
			return err
		}
	case event.AccessFormat:
		// insert statement
		if target.accInsertStmt, err = target.db.Prepare(fmt.Sprintf(pgInsertRow, target.args.Table)); err != nil {
			return err
		}
	}

	return nil
}

func (target *PostgreSQLTarget) initPGTable() error {
	var err error
	if err = target.createTableAndMigrateData(); err != nil {
		return err
	}
	if err = target.prepareStatements(); err != nil {
		return err
	}
	target.firstPing = true
	return nil
}

// NewPostgreSQLTarget - creates new PostgreSQL target.
func NewPostgreSQLTarget(id string, args PostgreSQLArgs, doneCh <-chan struct{}, loggerOnce func(ctx context.Context, err error, id interface{}, kind ...interface{}), test bool) (*PostgreSQLTarget, error) {
	params := []string{args.ConnectionString}
	if args.ConnectionString == "" {
		params = []string{}
		if !args.Host.IsEmpty() {
			params = append(params, "host="+args.Host.String())
		}
		if args.Port != "" {
			params = append(params, "port="+args.Port)
		}
		if args.Username != "" {
			params = append(params, "username="+args.Username)
		}
		if args.Password != "" {
			params = append(params, "password="+args.Password)
		}
		if args.Database != "" {
			params = append(params, "dbname="+args.Database)
		}
	}
	connStr := strings.Join(params, " ")

	target := &PostgreSQLTarget{
		id:         event.TargetID{ID: id, Name: "postgresql"},
		args:       args,
		firstPing:  false,
		connString: connStr,
		loggerOnce: loggerOnce,
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return target, err
	}
	target.db = db

	if args.MaxOpenConnections > 0 {
		// Set the maximum connections limit
		target.db.SetMaxOpenConns(args.MaxOpenConnections)
	}

	var store Store

	if args.QueueDir != "" {
		queueDir := filepath.Join(args.QueueDir, storePrefix+"-postgresql-"+id)
		store = NewQueueStore(queueDir, args.QueueLimit)
		if oErr := store.Open(); oErr != nil {
			target.loggerOnce(context.Background(), oErr, target.ID())
			return target, oErr
		}
		target.store = store
	}

	err = target.db.Ping()
	if err != nil {
		if target.store == nil || !(IsConnRefusedErr(err) || IsConnResetErr(err)) {
			target.loggerOnce(context.Background(), err, target.ID())
			return target, err
		}
	} else {
		if err = target.initPGTable(); err != nil {
			target.loggerOnce(context.Background(), err, target.ID())
			return target, err
		}
	}

	if target.store != nil && !test {
		// Replays the events from the store.
		eventKeyCh := replayEvents(target.store, doneCh, target.loggerOnce, target.ID())
		// Start replaying events from the store.
		go sendEvents(target, eventKeyCh, doneCh, target.loggerOnce)
	}

	return target, nil
}
