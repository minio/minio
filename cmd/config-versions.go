/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
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

package cmd

import (
	"sync"

	"github.com/minio/minio/pkg/auth"
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
	AMQP          map[string]amqpNotify          `json:"amqp"`
	NATS          map[string]natsNotifyV1        `json:"nats"`
	ElasticSearch map[string]elasticSearchNotify `json:"elasticsearch"`
	Redis         map[string]redisNotify         `json:"redis"`
	PostgreSQL    map[string]postgreSQLNotify    `json:"postgresql"`
	Kafka         map[string]kafkaNotify         `json:"kafka"`
}

// Notifier represents collection of supported notification queues in version 2
// with NATS streaming but without webhook.
type notifierV2 struct {
	AMQP          map[string]amqpNotify          `json:"amqp"`
	NATS          map[string]natsNotify          `json:"nats"`
	ElasticSearch map[string]elasticSearchNotify `json:"elasticsearch"`
	Redis         map[string]redisNotify         `json:"redis"`
	PostgreSQL    map[string]postgreSQLNotify    `json:"postgresql"`
	Kafka         map[string]kafkaNotify         `json:"kafka"`
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

	// Read Write mutex.
	rwMutex *sync.RWMutex
}

// serverConfigV8 server configuration version '8'. Adds NATS notifier
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

	// Read Write mutex.
	rwMutex *sync.RWMutex
}

// serverConfigV9 server configuration version '9'. Adds PostgreSQL
// notifier configuration.
type serverConfigV9 struct {
	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`

	// Additional error logging configuration.
	Logger loggerV6 `json:"logger"`

	// Notification queue configuration.
	Notify notifierV1 `json:"notify"`

	// Read Write mutex.
	rwMutex *sync.RWMutex
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
	Notify *notifier `json:"notify"`
}

// serverConfigV14 server configuration version '14' which is like
// version '13' except it adds support of browser param.
type serverConfigV14 struct {
	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`
	Browser    BrowserFlag      `json:"browser"`

	// Additional error logging configuration.
	Logger *loggerV7 `json:"logger"`

	// Notification queue configuration.
	Notify *notifier `json:"notify"`
}

// serverConfigV15 server configuration version '15' which is like
// version '14' except it adds mysql support
type serverConfigV15 struct {
	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`
	Browser    BrowserFlag      `json:"browser"`

	// Additional error logging configuration.
	Logger *loggerV7 `json:"logger"`

	// Notification queue configuration.
	Notify *notifier `json:"notify"`
}

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
	Browser    BrowserFlag      `json:"browser"`

	// Additional error logging configuration.
	Logger *loggers `json:"logger"`

	// Notification queue configuration.
	Notify *notifier `json:"notify"`
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
	Browser    BrowserFlag      `json:"browser"`

	// Additional error logging configuration.
	Logger *loggers `json:"logger"`

	// Notification queue configuration.
	Notify *notifier `json:"notify"`
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
	Browser    BrowserFlag      `json:"browser"`

	// Additional error logging configuration.
	Logger *loggers `json:"logger"`

	// Notification queue configuration.
	Notify *notifier `json:"notify"`
}

// serverConfigV19 server configuration version '19' which is like
// version '18' except it adds support for MQTT notifications.
type serverConfigV19 struct {
	sync.RWMutex
	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`
	Browser    BrowserFlag      `json:"browser"`

	// Additional error logging configuration.
	Logger *loggers `json:"logger"`

	// Notification queue configuration.
	Notify *notifier `json:"notify"`
}

// serverConfigV20 server configuration version '20' which is like
// version '19' except it adds support for VirtualHostDomain
type serverConfigV20 struct {
	sync.RWMutex
	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`
	Browser    BrowserFlag      `json:"browser"`
	Domain     string           `json:"domain"`

	// Additional error logging configuration.
	Logger *loggers `json:"logger"`

	// Notification queue configuration.
	Notify *notifier `json:"notify"`
}

// serverConfigV21 is just like version '20' without logger field
type serverConfigV21 struct {
	sync.RWMutex
	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`
	Browser    BrowserFlag      `json:"browser"`
	Domain     string           `json:"domain"`

	// Notification queue configuration.
	Notify *notifier `json:"notify"`
}

// serverConfigV22 is just like version '21' with added support
// for StorageClass
type serverConfigV22 struct {
	sync.RWMutex
	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`
	Browser    BrowserFlag      `json:"browser"`
	Domain     string           `json:"domain"`

	// Storage class configuration
	StorageClass storageClassConfig `json:"storageclass"`

	// Notification queue configuration.
	Notify *notifier `json:"notify"`
}
