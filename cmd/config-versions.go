// Copyright (c) 2015-2021 MinIO, Inc.
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

package cmd

import (
	"sync"

	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/config/cache"
	"github.com/minio/minio/internal/config/compress"
	xldap "github.com/minio/minio/internal/config/identity/ldap"
	"github.com/minio/minio/internal/config/identity/openid"
	"github.com/minio/minio/internal/config/notify"
	"github.com/minio/minio/internal/config/policy/opa"
	"github.com/minio/minio/internal/config/storageclass"
	"github.com/minio/minio/internal/event/target"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/quick"
)

type configV1 struct {
	Version   string `json:"version"`
	AccessKey string `json:"accessKeyId"`
	SecretKey string `json:"secretAccessKey"`
}

type configV2 struct {
	Version     string `json:"version"`
	Credentials struct {
		AccessKey string `json:"accessKeyId"`
		SecretKey string `json:"secretAccessKey"`
		Region    string `json:"region"`
	} `json:"credentials"`
	MongoLogger struct {
		Addr       string `json:"addr"`
		DB         string `json:"db"`
		Collection string `json:"collection"`
	} `json:"mongoLogger"`
	SyslogLogger struct {
		Network string `json:"network"`
		Addr    string `json:"addr"`
	} `json:"syslogLogger"`
	FileLogger struct {
		Filename string `json:"filename"`
	} `json:"fileLogger"`
}

// backendV3 type.
type backendV3 struct {
	Type  string   `json:"type"`
	Disk  string   `json:"disk,omitempty"`
	Disks []string `json:"disks,omitempty"`
}

// syslogLogger v3
type syslogLoggerV3 struct {
	Addr   string `json:"address"`
	Level  string `json:"level"`
	Enable bool   `json:"enable"`
}

// loggerV3 type.
type loggerV3 struct {
	Console struct {
		Level  string `json:"level"`
		Enable bool   `json:"enable"`
	}
	File struct {
		Filename string `json:"fileName"`
		Level    string `json:"level"`
		Enable   bool   `json:"enable"`
	}
	Syslog struct {
		Addr   string `json:"address"`
		Level  string `json:"level"`
		Enable bool   `json:"enable"`
	} `json:"syslog"`
	// Add new loggers here.
}

// configV3 server configuration version '3'.
type configV3 struct {

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`

	// Additional error logging configuration.
	Logger  loggerV3 `json:"logger"`
	Version string   `json:"version"`

	// http Server configuration.
	Addr string `json:"address"`

	Region string `json:"region"`

	// Backend configuration.
	Backend backendV3 `json:"backend"`
}

// logger type representing version '4' logger config.
type loggerV4 struct {
	Console struct {
		Level  string `json:"level"`
		Enable bool   `json:"enable"`
	} `json:"console"`
	File struct {
		Filename string `json:"fileName"`
		Level    string `json:"level"`
		Enable   bool   `json:"enable"`
	} `json:"file"`
	Syslog struct {
		Addr   string `json:"address"`
		Level  string `json:"level"`
		Enable bool   `json:"enable"`
	} `json:"syslog"`
}

// configV4 server configuration version '4'.
type configV4 struct {
	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`

	// Additional error logging configuration.
	Logger loggerV4 `json:"logger"`
}

// logger type representing version '5' logger config.
type loggerV5 struct {
	Redis struct {
		Level    string `json:"level"`
		Addr     string `json:"address"`
		Password string `json:"password"`
		Key      string `json:"key"`
		Enable   bool   `json:"enable"`
	} `json:"redis"`
	ElasticSearch struct {
		Level  string `json:"level"`
		URL    string `json:"url"`
		Index  string `json:"index"`
		Enable bool   `json:"enable"`
	} `json:"elasticsearch"`
	File struct {
		Filename string `json:"fileName"`
		Level    string `json:"level"`
		Enable   bool   `json:"enable"`
	} `json:"file"`
	Syslog struct {
		Addr   string `json:"address"`
		Level  string `json:"level"`
		Enable bool   `json:"enable"`
	} `json:"syslog"`
	Console struct {
		Level  string `json:"level"`
		Enable bool   `json:"enable"`
	} `json:"console"`
	AMQP struct {
		Level        string `json:"level"`
		URL          string `json:"url"`
		Exchange     string `json:"exchange"`
		RoutingKey   string `json:"routingKey"`
		ExchangeType string `json:"exchangeType"`
		Enable       bool   `json:"enable"`
		Mandatory    bool   `json:"mandatory"`
		Immediate    bool   `json:"immediate"`
		Durable      bool   `json:"durable"`
		Internal     bool   `json:"internal"`
		NoWait       bool   `json:"noWait"`
		AutoDeleted  bool   `json:"autoDeleted"`
	} `json:"amqp"`
}

// configV5 server configuration version '5'.
type configV5 struct {
	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`

	// Additional error logging configuration.
	Logger loggerV5 `json:"logger"`
}

// consoleLogger - default logger if not other logging is enabled.
type consoleLoggerV1 struct {
	Level  string `json:"level"`
	Enable bool   `json:"enable"`
}

type fileLoggerV1 struct {
	Filename string `json:"fileName"`
	Level    string `json:"level"`
	Enable   bool   `json:"enable"`
}

type loggerV6 struct {
	Console consoleLoggerV1 `json:"console"`
	File    fileLoggerV1    `json:"file"`
	Syslog  syslogLoggerV3  `json:"syslog"`
}

// configV6 server configuration version '6'.
type configV6 struct {

	// Notification queue configuration.
	Notify notifierV1 `json:"notify"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`

	// Additional error logging configuration.
	Logger loggerV6 `json:"logger"`

	Version string `json:"version"`

	Region string `json:"region"`
}

// Notifier represents collection of supported notification queues in version
// 1 without NATS streaming.
type notifierV1 struct {
	AMQP          map[string]target.AMQPArgs          `json:"amqp"`
	NATS          map[string]natsNotifyV1             `json:"nats"`
	ElasticSearch map[string]target.ElasticsearchArgs `json:"elasticsearch"`
	Redis         map[string]target.RedisArgs         `json:"redis"`
	PostgreSQL    map[string]target.PostgreSQLArgs    `json:"postgresql"`
	Kafka         map[string]target.KafkaArgs         `json:"kafka"`
}

// Notifier represents collection of supported notification queues in version 2
// with NATS streaming but without webhook.
type notifierV2 struct {
	AMQP          map[string]target.AMQPArgs          `json:"amqp"`
	NATS          map[string]target.NATSArgs          `json:"nats"`
	ElasticSearch map[string]target.ElasticsearchArgs `json:"elasticsearch"`
	Redis         map[string]target.RedisArgs         `json:"redis"`
	PostgreSQL    map[string]target.PostgreSQLArgs    `json:"postgresql"`
	Kafka         map[string]target.KafkaArgs         `json:"kafka"`
}

// configV7 server configuration version '7'.
type serverConfigV7 struct {

	// Notification queue configuration.
	Notify notifierV1 `json:"notify"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`

	// Additional error logging configuration.
	Logger loggerV6 `json:"logger"`

	Version string `json:"version"`

	Region string `json:"region"`
}

// serverConfigV8 server configuration version '8'. Adds NATS notify.Config
// configuration.
type serverConfigV8 struct {

	// Notification queue configuration.
	Notify notifierV1 `json:"notify"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`

	// Additional error logging configuration.
	Logger loggerV6 `json:"logger"`

	Version string `json:"version"`

	Region string `json:"region"`
}

// serverConfigV9 server configuration version '9'. Adds PostgreSQL
// notify.Config configuration.
type serverConfigV9 struct {

	// Notification queue configuration.
	Notify notifierV1 `json:"notify"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`

	// Additional error logging configuration.
	Logger loggerV6 `json:"logger"`

	Version string `json:"version"`

	Region string `json:"region"`
}

type loggerV7 struct {
	File    fileLoggerV1    `json:"file"`
	Console consoleLoggerV1 `json:"console"`
	sync.RWMutex
}

// serverConfigV10 server configuration version '10' which is like
// version '9' except it drops support of syslog config, and makes the
// RWMutex global (so it does not exist in this struct).
type serverConfigV10 struct {

	// Notification queue configuration.
	Notify notifierV1 `json:"notify"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`

	// Additional error logging configuration.
	Logger loggerV7 `json:"logger"`

	Version string `json:"version"`

	Region string `json:"region"`
}

// natsNotifyV1 - structure was valid until config V 11
type natsNotifyV1 struct {
	Address      string `json:"address"`
	Subject      string `json:"subject"`
	Username     string `json:"username"`
	Password     string `json:"password"`
	Token        string `json:"token"`
	PingInterval int64  `json:"pingInterval"`
	Enable       bool   `json:"enable"`
	Secure       bool   `json:"secure"`
}

// serverConfigV11 server configuration version '11' which is like
// version '10' except it adds support for Kafka notifications.
type serverConfigV11 struct {

	// Notification queue configuration.
	Notify notifierV1 `json:"notify"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`

	// Additional error logging configuration.
	Logger loggerV7 `json:"logger"`

	Version string `json:"version"`

	Region string `json:"region"`
}

// serverConfigV12 server configuration version '12' which is like
// version '11' except it adds support for NATS streaming notifications.
type serverConfigV12 struct {

	// Notification queue configuration.
	Notify notifierV2 `json:"notify"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`

	// Additional error logging configuration.
	Logger loggerV7 `json:"logger"`

	Version string `json:"version"`

	Region string `json:"region"`
}

type notifierV3 struct {
	AMQP          map[string]target.AMQPArgs          `json:"amqp"`
	Elasticsearch map[string]target.ElasticsearchArgs `json:"elasticsearch"`
	Kafka         map[string]target.KafkaArgs         `json:"kafka"`
	MQTT          map[string]target.MQTTArgs          `json:"mqtt"`
	MySQL         map[string]target.MySQLArgs         `json:"mysql"`
	NATS          map[string]target.NATSArgs          `json:"nats"`
	PostgreSQL    map[string]target.PostgreSQLArgs    `json:"postgresql"`
	Redis         map[string]target.RedisArgs         `json:"redis"`
	Webhook       map[string]target.WebhookArgs       `json:"webhook"`
}

// serverConfigV13 server configuration version '13' which is like
// version '12' except it adds support for webhook notification.
type serverConfigV13 struct {

	// Additional error logging configuration.
	Logger *loggerV7 `json:"logger"`

	// Notification queue configuration.
	Notify *notifierV3 `json:"notify"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Version    string           `json:"version"`

	Region string `json:"region"`
}

// serverConfigV14 server configuration version '14' which is like
// version '13' except it adds support of browser param.
type serverConfigV14 struct {

	// Additional error logging configuration.
	Logger *loggerV7 `json:"logger"`

	// Notification queue configuration.
	Notify *notifierV3 `json:"notify"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Version    string           `json:"version"`

	Region  string          `json:"region"`
	Browser config.BoolFlag `json:"browser"`
}

// serverConfigV15 server configuration version '15' which is like
// version '14' except it adds mysql support
type serverConfigV15 struct {

	// Additional error logging configuration.
	Logger *loggerV7 `json:"logger"`

	// Notification queue configuration.
	Notify *notifierV3 `json:"notify"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Version    string           `json:"version"`

	Region  string          `json:"region"`
	Browser config.BoolFlag `json:"browser"`
}

// FileLogger is introduced to workaround the dependency about logrus
type FileLogger struct {
	Filename string `json:"filename"`
	Enable   bool   `json:"enable"`
}

// ConsoleLogger is introduced to workaround the dependency about logrus
type ConsoleLogger struct {
	Enable bool `json:"enable"`
}

// Loggers struct is defined with FileLogger and ConsoleLogger
// although they are removed from logging logic. They are
// kept here just to workaround the dependency migration
// code/logic has on them.
type loggers struct {
	File FileLogger `json:"file"`
	sync.RWMutex
	Console ConsoleLogger `json:"console"`
}

// serverConfigV16 server configuration version '16' which is like
// version '15' except it makes a change to logging configuration.
type serverConfigV16 struct {

	// Additional error logging configuration.
	Logger *loggers `json:"logger"`

	// Notification queue configuration.
	Notify *notifierV3 `json:"notify"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Version    string           `json:"version"`

	Region  string          `json:"region"`
	Browser config.BoolFlag `json:"browser"`
}

// serverConfigV17 server configuration version '17' which is like
// version '16' except it adds support for "format" parameter in
// database event notification targets: PostgreSQL, MySQL, Redis and
// Elasticsearch.
type serverConfigV17 struct {

	// Additional error logging configuration.
	Logger *loggers `json:"logger"`

	// Notification queue configuration.
	Notify *notifierV3 `json:"notify"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Version    string           `json:"version"`

	Region  string          `json:"region"`
	Browser config.BoolFlag `json:"browser"`
}

// serverConfigV18 server configuration version '18' which is like
// version '17' except it adds support for "deliveryMode" parameter in
// the AMQP notification target.
type serverConfigV18 struct {

	// Additional error logging configuration.
	Logger *loggers `json:"logger"`

	// Notification queue configuration.
	Notify *notifierV3 `json:"notify"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Version    string           `json:"version"`

	Region string `json:"region"`
	sync.RWMutex
	Browser config.BoolFlag `json:"browser"`
}

// serverConfigV19 server configuration version '19' which is like
// version '18' except it adds support for MQTT notifications.
type serverConfigV19 struct {

	// Additional error logging configuration.
	Logger *loggers `json:"logger"`

	// Notification queue configuration.
	Notify *notifierV3 `json:"notify"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Version    string           `json:"version"`

	Region string `json:"region"`
	sync.RWMutex
	Browser config.BoolFlag `json:"browser"`
}

// serverConfigV20 server configuration version '20' which is like
// version '19' except it adds support for VirtualHostDomain
type serverConfigV20 struct {

	// Additional error logging configuration.
	Logger *loggers `json:"logger"`

	// Notification queue configuration.
	Notify *notifierV3 `json:"notify"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Version    string           `json:"version"`

	Region string `json:"region"`
	Domain string `json:"domain"`

	sync.RWMutex
	Browser config.BoolFlag `json:"browser"`
}

// serverConfigV21 is just like version '20' without logger field
type serverConfigV21 struct {

	// Notification queue configuration.
	Notify *notifierV3 `json:"notify"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Version    string           `json:"version"`

	Region string `json:"region"`
	Domain string `json:"domain"`

	sync.RWMutex
	Browser config.BoolFlag `json:"browser"`
}

// serverConfigV22 is just like version '21' with added support
// for StorageClass.
type serverConfigV22 struct {

	// Notification queue configuration.
	Notify notifierV3 `json:"notify"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Version    string           `json:"version"`

	Region string `json:"region"`
	Domain string `json:"domain"`

	// Storage class configuration
	StorageClass storageclass.Config `json:"storageclass"`

	Browser config.BoolFlag `json:"browser"`
}

// serverConfigV23 is just like version '22' with addition of cache field.
type serverConfigV23 struct {

	// Notification queue configuration.
	Notify notifierV3 `json:"notify"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`

	// Cache configuration
	Cache cache.Config `json:"cache"`

	Version string `json:"version"`

	Region string `json:"region"`
	Domain string `json:"domain"`

	// Storage class configuration
	StorageClass storageclass.Config `json:"storageclass"`

	Browser config.BoolFlag `json:"browser"`
}

// serverConfigV24 is just like version '23', we had to revert
// the changes which were made in 6fb06045028b7a57c37c60a612c8e50735279ab4
type serverConfigV24 struct {

	// Notification queue configuration.
	Notify notifierV3 `json:"notify"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`

	// Cache configuration
	Cache cache.Config `json:"cache"`

	Version string `json:"version"`

	Region string `json:"region"`
	Domain string `json:"domain"`

	// Storage class configuration
	StorageClass storageclass.Config `json:"storageclass"`

	Browser config.BoolFlag `json:"browser"`
}

// serverConfigV25 is just like version '24', stores additionally
// worm variable.
type serverConfigV25 struct {

	// Notification queue configuration.
	Notify       notifierV3 `json:"notify"`
	quick.Config `json:"-"` // ignore interfaces

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`

	// Cache configuration
	Cache cache.Config `json:"cache"`

	Version string `json:"version"`

	Region string `json:"region"`
	Domain string `json:"domain"`

	// Storage class configuration
	StorageClass storageclass.Config `json:"storageclass"`

	Browser config.BoolFlag `json:"browser"`
	Worm    config.BoolFlag `json:"worm"`
}

// serverConfigV26 is just like version '25', stores additionally
// cache max use value in 'cache.Config'.
type serverConfigV26 struct {

	// Notification queue configuration.
	Notify       notifierV3 `json:"notify"`
	quick.Config `json:"-"` // ignore interfaces

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`

	// Cache configuration
	Cache cache.Config `json:"cache"`

	Version string `json:"version"`

	Region string `json:"region"`
	Domain string `json:"domain"`

	// Storage class configuration
	StorageClass storageclass.Config `json:"storageclass"`

	Browser config.BoolFlag `json:"browser"`
	Worm    config.BoolFlag `json:"worm"`
}

// serverConfigV27 is just like version '26', stores additionally
// the logger field
type serverConfigV27 struct {

	// Notification queue configuration.
	Notify notifierV3 `json:"notify"`

	// Logger configuration
	Logger       logger.Config `json:"logger"`
	quick.Config `json:"-"`    // ignore interfaces

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`

	// Cache configuration
	Cache cache.Config `json:"cache"`

	Version string `json:"version"`

	Region string `json:"region"`
	Domain string `json:"domain"`

	// Storage class configuration
	StorageClass storageclass.Config `json:"storageclass"`

	Browser config.BoolFlag `json:"browser"`
	Worm    config.BoolFlag `json:"worm"`
}

// serverConfigV28 is just like version '27', additionally
// storing KMS config
type serverConfigV28 struct {

	// Notification queue configuration.
	Notify notifierV3 `json:"notify"`

	// Logger configuration
	Logger       logger.Config `json:"logger"`
	quick.Config `json:"-"`    // ignore interfaces

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`

	// Cache configuration
	Cache cache.Config `json:"cache"`

	Version string `json:"version"`

	Region string `json:"region"`

	// Storage class configuration
	StorageClass storageclass.Config `json:"storageclass"`

	Worm config.BoolFlag `json:"worm"`
}

// serverConfigV33 is just like version '32', removes clientID from NATS and MQTT, and adds queueDir, queueLimit in all notification targets.
type serverConfigV33 struct {

	// Notification queue configuration.
	Notify notify.Config `json:"notify"`

	// OpenID configuration
	OpenID openid.Config `json:"openid"`

	// External policy enforcements.
	Policy struct {
		// OPA configuration.
		OPA opa.Args `json:"opa"`

		// Add new external policy enforcements here.
	} `json:"policy"`

	// Logger configuration
	Logger logger.Config `json:"logger"`

	quick.Config `json:"-"` // ignore interfaces

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`

	LDAPServerConfig xldap.LegacyConfig `json:"ldapserverconfig"`

	// Cache configuration
	Cache cache.Config `json:"cache"`

	Region string `json:"region"`

	Version string `json:"version"`

	// Compression configuration
	Compression compress.Config `json:"compress"`

	// Storage class configuration
	StorageClass storageclass.Config `json:"storageclass"`

	Worm config.BoolFlag `json:"worm"`
}
