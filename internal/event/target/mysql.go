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
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/once"
	"github.com/minio/minio/internal/store"
	xnet "github.com/minio/pkg/v3/net"
)

const (
	mysqlTableExists = `SELECT 1 FROM %s;`
	// Some MySQL has a 3072 byte limit on key sizes.
	mysqlCreateNamespaceTable = `CREATE TABLE %s (
             key_name VARCHAR(3072) NOT NULL,
             key_hash CHAR(64) GENERATED ALWAYS AS (SHA2(key_name, 256)) STORED NOT NULL PRIMARY KEY,
             value JSON)
           CHARACTER SET = utf8mb4 COLLATE = utf8mb4_bin ROW_FORMAT = Dynamic;`
	mysqlCreateAccessTable = `CREATE TABLE %s (event_time DATETIME NOT NULL, event_data JSON)
                                    ROW_FORMAT = Dynamic;`

	mysqlUpdateRow = `INSERT INTO %s (key_name, value) VALUES (?, ?) ON DUPLICATE KEY UPDATE value=VALUES(value);`
	mysqlDeleteRow = `DELETE FROM %s WHERE key_hash = SHA2(?, 256);`
	mysqlInsertRow = `INSERT INTO %s (event_time, event_data) VALUES (?, ?);`
)

// MySQL related constants
const (
	MySQLFormat             = "format"
	MySQLDSNString          = "dsn_string"
	MySQLTable              = "table"
	MySQLHost               = "host"
	MySQLPort               = "port"
	MySQLUsername           = "username"
	MySQLPassword           = "password"
	MySQLDatabase           = "database"
	MySQLQueueLimit         = "queue_limit"
	MySQLQueueDir           = "queue_dir"
	MySQLMaxOpenConnections = "max_open_connections"

	EnvMySQLEnable             = "MINIO_NOTIFY_MYSQL_ENABLE"
	EnvMySQLFormat             = "MINIO_NOTIFY_MYSQL_FORMAT"
	EnvMySQLDSNString          = "MINIO_NOTIFY_MYSQL_DSN_STRING"
	EnvMySQLTable              = "MINIO_NOTIFY_MYSQL_TABLE"
	EnvMySQLHost               = "MINIO_NOTIFY_MYSQL_HOST"
	EnvMySQLPort               = "MINIO_NOTIFY_MYSQL_PORT"
	EnvMySQLUsername           = "MINIO_NOTIFY_MYSQL_USERNAME"
	EnvMySQLPassword           = "MINIO_NOTIFY_MYSQL_PASSWORD"
	EnvMySQLDatabase           = "MINIO_NOTIFY_MYSQL_DATABASE"
	EnvMySQLQueueLimit         = "MINIO_NOTIFY_MYSQL_QUEUE_LIMIT"
	EnvMySQLQueueDir           = "MINIO_NOTIFY_MYSQL_QUEUE_DIR"
	EnvMySQLMaxOpenConnections = "MINIO_NOTIFY_MYSQL_MAX_OPEN_CONNECTIONS"
)

// MySQLArgs - MySQL target arguments.
type MySQLArgs struct {
	Enable             bool     `json:"enable"`
	Format             string   `json:"format"`
	DSN                string   `json:"dsnString"`
	Table              string   `json:"table"`
	Host               xnet.URL `json:"host"`
	Port               string   `json:"port"`
	User               string   `json:"user"`
	Password           string   `json:"password"`
	Database           string   `json:"database"`
	QueueDir           string   `json:"queueDir"`
	QueueLimit         uint64   `json:"queueLimit"`
	MaxOpenConnections int      `json:"maxOpenConnections"`
}

// Validate MySQLArgs fields
func (m MySQLArgs) Validate() error {
	if !m.Enable {
		return nil
	}

	if m.Format != "" {
		f := strings.ToLower(m.Format)
		if f != event.NamespaceFormat && f != event.AccessFormat {
			return fmt.Errorf("unrecognized format")
		}
	}

	if m.Table == "" {
		return fmt.Errorf("table unspecified")
	}

	if m.DSN != "" {
		if _, err := mysql.ParseDSN(m.DSN); err != nil {
			return err
		}
	} else {
		// Some fields need to be specified when DSN is unspecified
		if m.Port == "" {
			return fmt.Errorf("unspecified port")
		}
		if _, err := strconv.Atoi(m.Port); err != nil {
			return fmt.Errorf("invalid port")
		}
		if m.Database == "" {
			return fmt.Errorf("database unspecified")
		}
	}

	if m.QueueDir != "" {
		if !filepath.IsAbs(m.QueueDir) {
			return errors.New("queueDir path should be absolute")
		}
	}

	if m.MaxOpenConnections < 0 {
		return errors.New("maxOpenConnections cannot be less than zero")
	}

	return nil
}

// MySQLTarget - MySQL target.
type MySQLTarget struct {
	initOnce once.Init

	id         event.TargetID
	args       MySQLArgs
	updateStmt *sql.Stmt
	deleteStmt *sql.Stmt
	insertStmt *sql.Stmt
	db         *sql.DB
	store      store.Store[event.Event]
	firstPing  bool
	loggerOnce logger.LogOnce

	quitCh chan struct{}
}

// ID - returns target ID.
func (target *MySQLTarget) ID() event.TargetID {
	return target.id
}

// Name - returns the Name of the target.
func (target *MySQLTarget) Name() string {
	return target.ID().String()
}

// Store returns any underlying store if set.
func (target *MySQLTarget) Store() event.TargetStore {
	return target.store
}

// IsActive - Return true if target is up and active
func (target *MySQLTarget) IsActive() (bool, error) {
	if err := target.init(); err != nil {
		return false, err
	}
	return target.isActive()
}

func (target *MySQLTarget) isActive() (bool, error) {
	if err := target.db.Ping(); err != nil {
		if IsConnErr(err) {
			return false, store.ErrNotConnected
		}
		return false, err
	}
	return true, nil
}

// Save - saves the events to the store which will be replayed when the SQL connection is active.
func (target *MySQLTarget) Save(eventData event.Event) error {
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

// send - sends an event to the mysql.
func (target *MySQLTarget) send(eventData event.Event) error {
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

// SendFromStore - reads an event from store and sends it to MySQL.
func (target *MySQLTarget) SendFromStore(key store.Key) error {
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

// Close - closes underneath connections to MySQL database.
func (target *MySQLTarget) Close() error {
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
		return target.db.Close()
	}

	return nil
}

// Executes the table creation statements.
func (target *MySQLTarget) executeStmts() error {
	_, err := target.db.Exec(fmt.Sprintf(mysqlTableExists, target.args.Table))
	if err != nil {
		createStmt := mysqlCreateNamespaceTable
		if target.args.Format == event.AccessFormat {
			createStmt = mysqlCreateAccessTable
		}

		if _, dbErr := target.db.Exec(fmt.Sprintf(createStmt, target.args.Table)); dbErr != nil {
			return dbErr
		}
	}

	switch target.args.Format {
	case event.NamespaceFormat:
		// insert or update statement
		if target.updateStmt, err = target.db.Prepare(fmt.Sprintf(mysqlUpdateRow, target.args.Table)); err != nil {
			return err
		}
		// delete statement
		if target.deleteStmt, err = target.db.Prepare(fmt.Sprintf(mysqlDeleteRow, target.args.Table)); err != nil {
			return err
		}
	case event.AccessFormat:
		// insert statement
		if target.insertStmt, err = target.db.Prepare(fmt.Sprintf(mysqlInsertRow, target.args.Table)); err != nil {
			return err
		}
	}

	return nil
}

func (target *MySQLTarget) init() error {
	return target.initOnce.Do(target.initMySQL)
}

func (target *MySQLTarget) initMySQL() error {
	args := target.args

	db, err := sql.Open("mysql", args.DSN)
	if err != nil {
		target.loggerOnce(context.Background(), err, target.ID().String())
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

// NewMySQLTarget - creates new MySQL target.
func NewMySQLTarget(id string, args MySQLArgs, loggerOnce logger.LogOnce) (*MySQLTarget, error) {
	var queueStore store.Store[event.Event]
	if args.QueueDir != "" {
		queueDir := filepath.Join(args.QueueDir, storePrefix+"-mysql-"+id)
		queueStore = store.NewQueueStore[event.Event](queueDir, args.QueueLimit, event.StoreExtension)
		if err := queueStore.Open(); err != nil {
			return nil, fmt.Errorf("unable to initialize the queue store of MySQL `%s`: %w", id, err)
		}
	}

	if args.DSN == "" {
		config := mysql.Config{
			User:                 args.User,
			Passwd:               args.Password,
			Net:                  "tcp",
			Addr:                 args.Host.String() + ":" + args.Port,
			DBName:               args.Database,
			AllowNativePasswords: true,
			CheckConnLiveness:    true,
		}

		args.DSN = config.FormatDSN()
	}

	target := &MySQLTarget{
		id:         event.TargetID{ID: id, Name: "mysql"},
		args:       args,
		firstPing:  false,
		store:      queueStore,
		loggerOnce: loggerOnce,
		quitCh:     make(chan struct{}),
	}

	if target.store != nil {
		store.StreamItems(target.store, target, target.quitCh, target.loggerOnce)
	}

	return target, nil
}
