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

/////////////////// Config V1 ///////////////////
type configV1 struct {
	Version   string `json:"version"`
	AccessKey string `json:"accessKeyId"`
	SecretKey string `json:"secretAccessKey"`
}

/////////////////// Config V2 ///////////////////
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

/////////////////// Config V3 ///////////////////
// backendV3 type.
type backendV3 struct {
	Type  string   `json:"type"`
	Disk  string   `json:"disk,omitempty"`
	Disks []string `json:"disks,omitempty"`
}

// syslogLogger v3
type syslogLoggerV3 struct {
	Enable bool   `json:"enable"`
	Addr   string `json:"address"`
	Level  string `json:"level"`
}

// loggerV3 type.
type loggerV3 struct {
	Console struct {
		Enable bool   `json:"enable"`
		Level  string `json:"level"`
	}
	File struct {
		Enable   bool   `json:"enable"`
		Filename string `json:"fileName"`
		Level    string `json:"level"`
	}
	Syslog struct {
		Enable bool   `json:"enable"`
		Addr   string `json:"address"`
		Level  string `json:"level"`
	} `json:"syslog"`
	// Add new loggers here.
}

// configV3 server configuration version '3'.
type configV3 struct {
	Version string `json:"version"`

	// Backend configuration.
	Backend backendV3 `json:"backend"`

	// http Server configuration.
	Addr string `json:"address"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`

	// Additional error logging configuration.
	Logger loggerV3 `json:"logger"`
}

// logger type representing version '4' logger config.
type loggerV4 struct {
	Console struct {
		Enable bool   `json:"enable"`
		Level  string `json:"level"`
	} `json:"console"`
	File struct {
		Enable   bool   `json:"enable"`
		Filename string `json:"fileName"`
		Level    string `json:"level"`
	} `json:"file"`
	Syslog struct {
		Enable bool   `json:"enable"`
		Addr   string `json:"address"`
		Level  string `json:"level"`
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
	Console struct {
		Enable bool   `json:"enable"`
		Level  string `json:"level"`
	} `json:"console"`
	File struct {
		Enable   bool   `json:"enable"`
		Filename string `json:"fileName"`
		Level    string `json:"level"`
	} `json:"file"`
	Syslog struct {
		Enable bool   `json:"enable"`
		Addr   string `json:"address"`
		Level  string `json:"level"`
	} `json:"syslog"`
	AMQP struct {
		Enable       bool   `json:"enable"`
		Level        string `json:"level"`
		URL          string `json:"url"`
		Exchange     string `json:"exchange"`
		RoutingKey   string `json:"routingKey"`
		ExchangeType string `json:"exchangeType"`
		Mandatory    bool   `json:"mandatory"`
		Immediate    bool   `json:"immediate"`
		Durable      bool   `json:"durable"`
		Internal     bool   `json:"internal"`
		NoWait       bool   `json:"noWait"`
		AutoDeleted  bool   `json:"autoDeleted"`
	} `json:"amqp"`
	ElasticSearch struct {
		Enable bool   `json:"enable"`
		Level  string `json:"level"`
		URL    string `json:"url"`
		Index  string `json:"index"`
	} `json:"elasticsearch"`
	Redis struct {
		Enable   bool   `json:"enable"`
		Level    string `json:"level"`
		Addr     string `json:"address"`
		Password string `json:"password"`
		Key      string `json:"key"`
	} `json:"redis"`
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
	Enable bool   `json:"enable"`
	Level  string `json:"level"`
}

type fileLoggerV1 struct {
	Enable   bool   `json:"enable"`
	Filename string `json:"fileName"`
	Level    string `json:"level"`
}

type loggerV6 struct {
	Console consoleLoggerV1 `json:"console"`
	File    fileLoggerV1    `json:"file"`
	Syslog  syslogLoggerV3  `json:"syslog"`
}

// configV6 server configuration version '6'.
type configV6 struct {
	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`

	// Additional error logging configuration.
	Logger loggerV6 `json:"logger"`

	// Notification queue configuration.
	Notify notifierV1 `json:"notify"`
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
	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`

	// Additional error logging configuration.
	Logger loggerV6 `json:"logger"`

	// Notification queue configuration.
	Notify notifierV1 `json:"notify"`
}

// serverConfigV8 server configuration version '8'. Adds NATS notify.Config
// configuration.
type serverConfigV8 struct {
	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`

	// Additional error logging configuration.
	Logger loggerV6 `json:"logger"`

	// Notification queue configuration.
	Notify notifierV1 `json:"notify"`
}

// serverConfigV9 server configuration version '9'. Adds PostgreSQL
// notify.Config configuration.
type serverConfigV9 struct {
	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`

	// Additional error logging configuration.
	Logger loggerV6 `json:"logger"`

	// Notification queue configuration.
	Notify notifierV1 `json:"notify"`
}

type loggerV7 struct {
	sync.RWMutex
	Console consoleLoggerV1 `json:"console"`
	File    fileLoggerV1    `json:"file"`
}

// serverConfigV10 server configuration version '10' which is like
// version '9' except it drops support of syslog config, and makes the
// RWMutex global (so it does not exist in this struct).
type serverConfigV10 struct {
	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`

	// Additional error logging configuration.
	Logger loggerV7 `json:"logger"`

	// Notification queue configuration.
	Notify notifierV1 `json:"notify"`
}

// natsNotifyV1 - structure was valid until config V 11
type natsNotifyV1 struct {
	Enable       bool   `json:"enable"`
	Address      string `json:"address"`
	Subject      string `json:"subject"`
	Username     string `json:"username"`
	Password     string `json:"password"`
	Token        string `json:"token"`
	Secure       bool   `json:"secure"`
	PingInterval int64  `json:"pingInterval"`
}

// serverConfigV11 server configuration version '11' which is like
// version '10' except it adds support for Kafka notifications.
type serverConfigV11 struct {
	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`

	// Additional error logging configuration.
	Logger loggerV7 `json:"logger"`

	// Notification queue configuration.
	Notify notifierV1 `json:"notify"`
}

// serverConfigV12 server configuration version '12' which is like
// version '11' except it adds support for NATS streaming notifications.
type serverConfigV12 struct {
	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`

	// Additional error logging configuration.
	Logger loggerV7 `json:"logger"`

	// Notification queue configuration.
	Notify notifierV2 `json:"notify"`
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
	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`

	// Additional error logging configuration.
	Logger *loggerV7 `json:"logger"`

	// Notification queue configuration.
	Notify *notifierV3 `json:"notify"`
}

// serverConfigV14 server configuration version '14' which is like
// version '13' except it adds support of browser param.
type serverConfigV14 struct {
	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`
	Browser    config.BoolFlag  `json:"browser"`

	// Additional error logging configuration.
	Logger *loggerV7 `json:"logger"`

	// Notification queue configuration.
	Notify *notifierV3 `json:"notify"`
}

// serverConfigV15 server configuration version '15' which is like
// version '14' except it adds mysql support
type serverConfigV15 struct {
	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`
	Browser    config.BoolFlag  `json:"browser"`

	// Additional error logging configuration.
	Logger *loggerV7 `json:"logger"`

	// Notification queue configuration.
	Notify *notifierV3 `json:"notify"`
}

// FileLogger is introduced to workaround the dependency about logrus
type FileLogger struct {
	Enable   bool   `json:"enable"`
	Filename string `json:"filename"`
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
	sync.RWMutex
	Console ConsoleLogger `json:"console"`
	File    FileLogger    `json:"file"`
}

// serverConfigV16 server configuration version '16' which is like
// version '15' except it makes a change to logging configuration.
type serverConfigV16 struct {
	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`
	Browser    config.BoolFlag  `json:"browser"`

	// Additional error logging configuration.
	Logger *loggers `json:"logger"`

	// Notification queue configuration.
	Notify *notifierV3 `json:"notify"`
}

// serverConfigV17 server configuration version '17' which is like
// version '16' except it adds support for "format" parameter in
// database event notification targets: PostgreSQL, MySQL, Redis and
// Elasticsearch.
type serverConfigV17 struct {
	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`
	Browser    config.BoolFlag  `json:"browser"`

	// Additional error logging configuration.
	Logger *loggers `json:"logger"`

	// Notification queue configuration.
	Notify *notifierV3 `json:"notify"`
}

// serverConfigV18 server configuration version '18' which is like
// version '17' except it adds support for "deliveryMode" parameter in
// the AMQP notification target.
type serverConfigV18 struct {
	sync.RWMutex
	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`
	Browser    config.BoolFlag  `json:"browser"`

	// Additional error logging configuration.
	Logger *loggers `json:"logger"`

	// Notification queue configuration.
	Notify *notifierV3 `json:"notify"`
}

// serverConfigV19 server configuration version '19' which is like
// version '18' except it adds support for MQTT notifications.
type serverConfigV19 struct {
	sync.RWMutex
	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`
	Browser    config.BoolFlag  `json:"browser"`

	// Additional error logging configuration.
	Logger *loggers `json:"logger"`

	// Notification queue configuration.
	Notify *notifierV3 `json:"notify"`
}

// serverConfigV20 server configuration version '20' which is like
// version '19' except it adds support for VirtualHostDomain
type serverConfigV20 struct {
	sync.RWMutex
	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`
	Browser    config.BoolFlag  `json:"browser"`
	Domain     string           `json:"domain"`

	// Additional error logging configuration.
	Logger *loggers `json:"logger"`

	// Notification queue configuration.
	Notify *notifierV3 `json:"notify"`
}

// serverConfigV21 is just like version '20' without logger field
type serverConfigV21 struct {
	sync.RWMutex
	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`
	Browser    config.BoolFlag  `json:"browser"`
	Domain     string           `json:"domain"`

	// Notification queue configuration.
	Notify *notifierV3 `json:"notify"`
}

// serverConfigV22 is just like version '21' with added support
// for StorageClass.
type serverConfigV22 struct {
	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`
	Browser    config.BoolFlag  `json:"browser"`
	Domain     string           `json:"domain"`

	// Storage class configuration
	StorageClass storageclass.Config `json:"storageclass"`

	// Notification queue configuration.
	Notify notifierV3 `json:"notify"`
}

// serverConfigV23 is just like version '22' with addition of cache field.
type serverConfigV23 struct {
	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`
	Browser    config.BoolFlag  `json:"browser"`
	Domain     string           `json:"domain"`

	// Storage class configuration
	StorageClass storageclass.Config `json:"storageclass"`

	// Cache configuration
	Cache cache.Config `json:"cache"`

	// Notification queue configuration.
	Notify notifierV3 `json:"notify"`
}

// serverConfigV24 is just like version '23', we had to revert
// the changes which were made in 6fb06045028b7a57c37c60a612c8e50735279ab4
type serverConfigV24 struct {
	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`
	Browser    config.BoolFlag  `json:"browser"`
	Domain     string           `json:"domain"`

	// Storage class configuration
	StorageClass storageclass.Config `json:"storageclass"`

	// Cache configuration
	Cache cache.Config `json:"cache"`

	// Notification queue configuration.
	Notify notifierV3 `json:"notify"`
}

// serverConfigV25 is just like version '24', stores additionally
// worm variable.
type serverConfigV25 struct {
	quick.Config `json:"-"` // ignore interfaces

	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`
	Browser    config.BoolFlag  `json:"browser"`
	Worm       config.BoolFlag  `json:"worm"`
	Domain     string           `json:"domain"`

	// Storage class configuration
	StorageClass storageclass.Config `json:"storageclass"`

	// Cache configuration
	Cache cache.Config `json:"cache"`

	// Notification queue configuration.
	Notify notifierV3 `json:"notify"`
}

// serverConfigV26 is just like version '25', stores additionally
// cache max use value in 'cache.Config'.
type serverConfigV26 struct {
	quick.Config `json:"-"` // ignore interfaces

	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`
	Browser    config.BoolFlag  `json:"browser"`
	Worm       config.BoolFlag  `json:"worm"`
	Domain     string           `json:"domain"`

	// Storage class configuration
	StorageClass storageclass.Config `json:"storageclass"`

	// Cache configuration
	Cache cache.Config `json:"cache"`

	// Notification queue configuration.
	Notify notifierV3 `json:"notify"`
}

// serverConfigV27 is just like version '26', stores additionally
// the logger field
type serverConfigV27 struct {
	quick.Config `json:"-"` // ignore interfaces

	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`
	Browser    config.BoolFlag  `json:"browser"`
	Worm       config.BoolFlag  `json:"worm"`
	Domain     string           `json:"domain"`

	// Storage class configuration
	StorageClass storageclass.Config `json:"storageclass"`

	// Cache configuration
	Cache cache.Config `json:"cache"`

	// Notification queue configuration.
	Notify notifierV3 `json:"notify"`

	// Logger configuration
	Logger logger.Config `json:"logger"`
}

// serverConfigV28 is just like version '27', additionally
// storing KMS config
type serverConfigV28 struct {
	quick.Config `json:"-"` // ignore interfaces

	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`
	Worm       config.BoolFlag  `json:"worm"`

	// Storage class configuration
	StorageClass storageclass.Config `json:"storageclass"`

	// Cache configuration
	Cache cache.Config `json:"cache"`

	// Notification queue configuration.
	Notify notifierV3 `json:"notify"`

	// Logger configuration
	Logger logger.Config `json:"logger"`
}

// serverConfigV29 is just like version '28'.
type serverConfigV29 serverConfigV28

// serverConfigV30 is just like version '29', stores additionally
// extensions and mimetypes fields for compression.
type serverConfigV30 struct {
	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`
	Worm       config.BoolFlag  `json:"worm"`

	// Storage class configuration
	StorageClass storageclass.Config `json:"storageclass"`

	// Cache configuration
	Cache cache.Config `json:"cache"`

	// Notification queue configuration.
	Notify notifierV3 `json:"notify"`

	// Logger configuration
	Logger logger.Config `json:"logger"`

	// Compression configuration
	Compression compress.Config `json:"compress"`
}

// serverConfigV31 is just like version '30', with OPA and OpenID configuration.
type serverConfigV31 struct {
	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`
	Worm       config.BoolFlag  `json:"worm"`

	// Storage class configuration
	StorageClass storageclass.Config `json:"storageclass"`

	// Cache configuration
	Cache cache.Config `json:"cache"`

	// Notification queue configuration.
	Notify notifierV3 `json:"notify"`

	// Logger configuration
	Logger logger.Config `json:"logger"`

	// Compression configuration
	Compression compress.Config `json:"compress"`

	// OpenID configuration
	OpenID openid.Config `json:"openid"`

	// External policy enforcements.
	Policy struct {
		// OPA configuration.
		OPA opa.Args `json:"opa"`

		// Add new external policy enforcements here.
	} `json:"policy"`
}

// serverConfigV32 is just like version '31' with added nsq notifer.
type serverConfigV32 struct {
	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`
	Worm       config.BoolFlag  `json:"worm"`

	// Storage class configuration
	StorageClass storageclass.Config `json:"storageclass"`

	// Cache configuration
	Cache cache.Config `json:"cache"`

	// Notification queue configuration.
	Notify notify.Config `json:"notify"`

	// Logger configuration
	Logger logger.Config `json:"logger"`

	// Compression configuration
	Compression compress.Config `json:"compress"`

	// OpenID configuration
	OpenID openid.Config `json:"openid"`

	// External policy enforcements.
	Policy struct {
		// OPA configuration.
		OPA opa.Args `json:"opa"`

		// Add new external policy enforcements here.
	} `json:"policy"`
}

// serverConfigV33 is just like version '32', removes clientID from NATS and MQTT, and adds queueDir, queueLimit in all notification targets.
type serverConfigV33 struct {
	quick.Config `json:"-"` // ignore interfaces

	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`
	Worm       config.BoolFlag  `json:"worm"`

	// Storage class configuration
	StorageClass storageclass.Config `json:"storageclass"`

	// Cache configuration
	Cache cache.Config `json:"cache"`

	// Notification queue configuration.
	Notify notify.Config `json:"notify"`

	// Logger configuration
	Logger logger.Config `json:"logger"`

	// Compression configuration
	Compression compress.Config `json:"compress"`

	// OpenID configuration
	OpenID openid.Config `json:"openid"`

	// External policy enforcements.
	Policy struct {
		// OPA configuration.
		OPA opa.Args `json:"opa"`

		// Add new external policy enforcements here.
	} `json:"policy"`

	LDAPServerConfig xldap.Config `json:"ldapserverconfig"`
}
