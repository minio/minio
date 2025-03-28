// Copyright (c) 2015-2023 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

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
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	_ "github.com/lib/pq" // Register postgres driver

	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/once"
	"github.com/minio/minio/internal/store"
	xnet "github.com/minio/pkg/v3/net"
)

const (
	psqlTableExists          = `SELECT 1 FROM %s;`
	psqlCreateNamespaceTable = `CREATE TABLE %s (key VARCHAR PRIMARY KEY, value JSONB);`
	psqlCreateAccessTable    = `CREATE TABLE %s (event_time TIMESTAMP WITH TIME ZONE NOT NULL, event_data JSONB);`

	psqlUpdateRow = `INSERT INTO %s (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;`
	psqlDeleteRow = `DELETE FROM %s WHERE key = $1;`
	psqlInsertRow = `INSERT INTO %s (event_time, event_data) VALUES ($1, $2);`
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
	if err := validatePsqlTableName(p.Table); err != nil {
		return err
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
	initOnce once.Init

	id         event.TargetID
	args       PostgreSQLArgs
	updateStmt *sql.Stmt
	deleteStmt *sql.Stmt
	insertStmt *sql.Stmt
	db         *sql.DB
	store      store.Store[event.Event]
	firstPing  bool
	connString string
	loggerOnce logger.LogOnce
	quitCh     chan struct{}
}

// ID - returns target ID.
func (target *PostgreSQLTarget) ID() event.TargetID {
	return target.id
}

// Name - returns the Name of the target.
func (target *PostgreSQLTarget) Name() string {
	return target.ID().String()
}

// Store returns any underlying store if set.
func (target *PostgreSQLTarget) Store() event.TargetStore {
	return target.store
}

// IsActive - Return true if target is up and active
func (target *PostgreSQLTarget) IsActive() (bool, error) {
	if err := target.init(); err != nil {
		return false, err
	}
	return target.isActive()
}

func (target *PostgreSQLTarget) isActive() (bool, error) {
	if err := target.db.Ping(); err != nil {
		if IsConnErr(err) {
			return false, store.ErrNotConnected
		}
		return false, err
	}
	return true, nil
}

// Save - saves the events to the store if questore is configured, which will be replayed when the PostgreSQL connection is active.
func (target *PostgreSQLTarget) Save(eventData event.Event) error {
	if target.store != nil {
		_, err := target.store.Put(eventData)
		return err
	}

	if err := target.init(); err != nil {
		return err
	}

	_, err := target.isActive()
	if err != nil {
		return err
	}
	return target.send(eventData)
}

// IsConnErr - To detect a connection error.
func IsConnErr(err error) bool {
	return xnet.IsConnRefusedErr(err) || err.Error() == "sql: database is closed" || err.Error() == "sql: statement is closed" || err.Error() == "invalid connection"
}

// send - sends an event to the PostgreSQL.
func (target *PostgreSQLTarget) send(eventData event.Event) error {
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

		if _, err = target.insertStmt.Exec(eventTime, data); err != nil {
			return err
		}
	}

	return nil
}

// SendFromStore - reads an event from store and sends it to PostgreSQL.
func (target *PostgreSQLTarget) SendFromStore(key store.Key) error {
	if err := target.init(); err != nil {
		return err
	}

	_, err := target.isActive()
	if err != nil {
		return err
	}
	if !target.firstPing {
		if err := target.executeStmts(); err != nil {
			if IsConnErr(err) {
				return store.ErrNotConnected
			}
			return err
		}
	}

	eventData, eErr := target.store.Get(key)
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
			return store.ErrNotConnected
		}
		return err
	}

	// Delete the event from store.
	return target.store.Del(key)
}

// Close - closes underneath connections to PostgreSQL database.
func (target *PostgreSQLTarget) Close() error {
	close(target.quitCh)
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

	if target.db != nil {
		target.db.Close()
	}

	return nil
}

// Executes the table creation statements.
func (target *PostgreSQLTarget) executeStmts() error {
	_, err := target.db.Exec(fmt.Sprintf(psqlTableExists, target.args.Table))
	if err != nil {
		createStmt := psqlCreateNamespaceTable
		if target.args.Format == event.AccessFormat {
			createStmt = psqlCreateAccessTable
		}

		if _, dbErr := target.db.Exec(fmt.Sprintf(createStmt, target.args.Table)); dbErr != nil {
			return dbErr
		}
	}

	switch target.args.Format {
	case event.NamespaceFormat:
		// insert or update statement
		if target.updateStmt, err = target.db.Prepare(fmt.Sprintf(psqlUpdateRow, target.args.Table)); err != nil {
			return err
		}
		// delete statement
		if target.deleteStmt, err = target.db.Prepare(fmt.Sprintf(psqlDeleteRow, target.args.Table)); err != nil {
			return err
		}
	case event.AccessFormat:
		// insert statement
		if target.insertStmt, err = target.db.Prepare(fmt.Sprintf(psqlInsertRow, target.args.Table)); err != nil {
			return err
		}
	}

	return nil
}

func (target *PostgreSQLTarget) init() error {
	return target.initOnce.Do(target.initPostgreSQL)
}

func (target *PostgreSQLTarget) initPostgreSQL() error {
	args := target.args

	db, err := sql.Open("postgres", target.connString)
	if err != nil {
		return err
	}
	target.db = db

	if args.MaxOpenConnections > 0 {
		// Set the maximum connections limit
		target.db.SetMaxOpenConns(args.MaxOpenConnections)
	}

	err = target.db.Ping()
	if err != nil {
		if !xnet.IsConnRefusedErr(err) && !xnet.IsConnResetErr(err) {
			target.loggerOnce(context.Background(), err, target.ID().String())
		}
	} else {
		if err = target.executeStmts(); err != nil {
			target.loggerOnce(context.Background(), err, target.ID().String())
		} else {
			target.firstPing = true
		}
	}

	if err != nil {
		target.db.Close()
		return err
	}

	yes, err := target.isActive()
	if err != nil {
		return err
	}
	if !yes {
		return store.ErrNotConnected
	}

	return nil
}

// NewPostgreSQLTarget - creates new PostgreSQL target.
func NewPostgreSQLTarget(id string, args PostgreSQLArgs, loggerOnce logger.LogOnce) (*PostgreSQLTarget, error) {
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

	var queueStore store.Store[event.Event]
	if args.QueueDir != "" {
		queueDir := filepath.Join(args.QueueDir, storePrefix+"-postgresql-"+id)
		queueStore = store.NewQueueStore[event.Event](queueDir, args.QueueLimit, event.StoreExtension)
		if err := queueStore.Open(); err != nil {
			return nil, fmt.Errorf("unable to initialize the queue store of PostgreSQL `%s`: %w", id, err)
		}
	}

	target := &PostgreSQLTarget{
		id:         event.TargetID{ID: id, Name: "postgresql"},
		args:       args,
		firstPing:  false,
		store:      queueStore,
		connString: connStr,
		loggerOnce: loggerOnce,
		quitCh:     make(chan struct{}),
	}

	if target.store != nil {
		store.StreamItems(target.store, target, target.quitCh, target.loggerOnce)
	}

	return target, nil
}

var errInvalidPsqlTablename = errors.New("invalid PostgreSQL table")

func validatePsqlTableName(name string) error {
	// check for quoted string (string may not contain a quote)
	if match, err := regexp.MatchString("^\"[^\"]+\"$", name); err != nil {
		return err
	} else if match {
		return nil
	}

	// normalize the name to letters, digits, _ or $
	valid := true
	cleaned := strings.Map(func(r rune) rune {
		switch {
		case unicode.IsLetter(r):
			return 'a'
		case unicode.IsDigit(r):
			return '0'
		case r == '_', r == '$':
			return r
		default:
			valid = false
			return -1
		}
	}, name)

	if valid {
		// check for simple name or quoted name
		// - letter/underscore followed by one or more letter/digit/underscore
		// - any text between quotes (text cannot contain a quote itself)
		if match, err := regexp.MatchString("^[a_][a0_$]*$", cleaned); err != nil {
			return err
		} else if match {
			return nil
		}
	}

	return errInvalidPsqlTablename
}
