# Minio Server `config.json` (v18) Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

Minio server stores all its configuration data in `${HOME}/.minio/config.json` file by default. Following sections provide detailed explanation of each fields and how to customize them. A complete example of `config.json` is available [here](https://raw.githubusercontent.com/minio/minio/master/docs/config/config.sample.json)

## Configuration Directory
The default configuration directory is `${HOME}/.minio`. You can override the default configuration directory using `--config-dir` command-line option. Minio server generates a new `config.json` with auto-generated access credentials when its started for the first time.

```sh
minio server --config-dir /etc/minio /data
```

### Certificate Directory
TLS certificates are stored under ``${HOME}/.minio/certs`` directory. You need to place certificates here to enable `HTTPS` based access. Read more about [How to secure access to Minio server with TLS](http://docs.minio.io/docs/how-to-secure-access-to-minio-server-with-tls).

Following is the directory structure for Minio server with TLS certificates.

```sh
$ tree ~/.minio
/home/user1/.minio
├── certs
│   ├── CAs
│   ├── private.key
│   └── public.crt
└── config.json
```

### Configuration Fields
#### Version
|Field|Type|Description|
|:---|:---|:---|
|``version``|_string_| `version` determines the configuration file format. Any older version will be automatically be migrated to the latest version upon startup. [DO NOT EDIT THIS FIELD MANUALLY]|

#### Credential
|Field|Type|Description|
|:---|:---|:---|
|``credential``| | Auth credential for object storage and web access.|
|``credential.accessKey`` | _string_ | Access key of minimum 5 characters in length. You may override this field with `MINIO_ACCESS_KEY` environment variable.|
|``credential.secretKey`` | _string_ | Secret key of minimum 8 characters in length. You may override this field with `MINIO_SECRET_KEY` environment variable.|

Example:

```sh
export MINIO_ACCESS_KEY=admin
export MINIO_SECRET_KEY=password
minio server /data
```

#### Region
|Field|Type|Description|
|:---|:---|:---|
|``region``| _string_ | `region` describes the physical location of the server. By default it is set to ``. You may override this field with `MINIO_REGION` environment variable. If you are unsure leave it unset.|

Example:

```sh
export MINIO_REGION="my_region"
minio server /data
```

#### Browser
|Field|Type|Description|
|:---|:---|:---|
|``browser``| _string_ | Enable or disable access to web UI. By default it is set to `on`. You may override this field with ``MINIO_BROWSER`` environment variable.|

Example:

```sh
export MINIO_BROWSER=off
minio server /data
```

#### Logger
|Field|Type|Description|
|:---|:---|:---|
|``logger ``| |Server logs errors and fatal messages via logger. You may enable one or more loggers at the same time.|
|``logger.console``| |Send log messages to console.|
|``logger.console.enable``| _bool_ | Enable or disable console logger. Default is set to _true_.|
|``logger.file``| |Send log message to a file.|
|``logger.file.enable``| _bool_ | Enable or disable file logger. Default is set to _false_.|
|``logger.file.filename``| _string_ | Path and name of the log file. Example: _/var/log/minio.log_ |

#### Notify
|Field|Type|Description|
|:---|:---|:---|
|``notify``| |Notify enables bucket notification events for lambda computing via the following targets.|
|``notify.amqp``| |[Configure to publish Minio events via AMQP target.](http://docs.minio.io/docs/minio-bucket-notification-guide#AMQP)|
|``notify.mqtt``| |[Configure to publish Minio events via MQTT target.](http://docs.minio.io/docs/minio-bucket-notification-guide#MQTT)|
|``notify.elasticsearch``| |[Configure to publish Minio events via Elasticsearch target.](http://docs.minio.io/docs/minio-bucket-notification-guide#Elasticsearch)|
|``notify.redis``| |[Configure to publish Minio events via Redis target.](http://docs.minio.io/docs/minio-bucket-notification-guide#Redis)|
|``notify.nats``| |[Configure to publish Minio events via NATS target.](http://docs.minio.io/docs/minio-bucket-notification-guide#NATS)|
|``notify.postgresql``| |[Configure to publish Minio events via PostgreSQL target.](http://docs.minio.io/docs/minio-bucket-notification-guide#PostgreSQL)|
|``notify.kafka``| |[Configure to publish Minio events via Apache Kafka target.](http://docs.minio.io/docs/minio-bucket-notification-guide#apache-kafka)|
|``notify.webhook``| |[Configure to publish Minio events via Webhooks target.](http://docs.minio.io/docs/minio-bucket-notification-guide#webhooks)|

## Explore Further
* [Minio Quickstart Guide](https://docs.minio.io/docs/minio-quickstart-guide)
