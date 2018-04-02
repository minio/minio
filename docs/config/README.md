# Minio Server `config.json` (v23) Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

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
|``credential.accessKey`` | _string_ | Access key of minimum 3 characters in length. You may override this field with `MINIO_ACCESS_KEY` environment variable.|
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

### Domain
|Field|Type|Description|
|:---|:---|:---|
|``domain``| _string_ | Enable virtual-host-style requests i.e http://bucket.mydomain.com/object|

By default, Minio supports path-style requests which look like http://mydomain.com/bucket/object. MINIO_DOMAIN environmental variable (or `domain` in config.json) can be used to enable virtual-host-style requests. If the request `Host` header matches with `(.+).mydomain.com` then the mattched pattern `$1` is used as bucket and the path is used as object. More information on path-style and virtual-host-style [here](http://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAPI.html)

Example:

```sh
export MINIO_DOMAIN=mydomain.com
minio server /data
```

### Storage Class
|Field|Type|Description|
|:---|:---|:---|
|``storageclass``| | Set storage class for configurable data and parity, as per object basis.|
|``storageclass.standard`` | _string_ | Value for standard storage class. It should be in the format `EC:Parity`, for example to set 4 disk parity for standard storage class objects, set this field to `EC:4`.|
|``storageclass.rrs`` | _string_ |  Value for reduced redundancy storage class. It should be in the format `EC:Parity`, for example to set 3 disk parity for reduced redundancy storage class objects, set this field to `EC:3`.|

By default, parity for objects with standard storage class is set to `N/2`, and parity for objects with reduced redundancy storage class objects is set to `2`. Read more about storage class support in Minio server [here](https://github.com/minio/minio/blob/master/docs/erasure/storage-class/README.md).

### Cache
|Field|Type|Description|
|:---|:---|:---|
|``drives``| _[]string_ | List of mounted file system drives with [`atime`](http://kerolasa.github.io/filetimes.html) support enabled|
|``exclude`` | _[]string_ | List of wildcard patterns for prefixes to exclude from cache |
|``expiry`` | _int_ | Days to cache expiry |

#### Notify
|Field|Type|Description|
|:---|:---|:---|
|``notify``| |Notify enables bucket notification events for lambda computing via the following targets.|
|``notify.amqp``| |[Configure to publish Minio events via AMQP target.](http://docs.minio.io/docs/minio-bucket-notification-guide#AMQP)|
|``notify.nats``| |[Configure to publish Minio events via NATS target.](http://docs.minio.io/docs/minio-bucket-notification-guide#NATS)|
|``notify.elasticsearch``| |[Configure to publish Minio events via Elasticsearch target.](http://docs.minio.io/docs/minio-bucket-notification-guide#Elasticsearch)|
|``notify.redis``| |[Configure to publish Minio events via Redis target.](http://docs.minio.io/docs/minio-bucket-notification-guide#Redis)|
|``notify.postgresql``| |[Configure to publish Minio events via PostgreSQL target.](http://docs.minio.io/docs/minio-bucket-notification-guide#PostgreSQL)|
|``notify.kafka``| |[Configure to publish Minio events via Apache Kafka target.](http://docs.minio.io/docs/minio-bucket-notification-guide#apache-kafka)|
|``notify.webhook``| |[Configure to publish Minio events via Webhooks target.](http://docs.minio.io/docs/minio-bucket-notification-guide#webhooks)|
|``notify.mysql``| |[Configure to publish Minio events via MySql target.](https://docs.minio.io/docs/minio-bucket-notification-guide#MySQL)|
|``notify.mqtt``| |[Configure to publish Minio events via MQTT target.](http://docs.minio.io/docs/minio-bucket-notification-guide#MQTT)|

## Explore Further
* [Minio Quickstart Guide](https://docs.minio.io/docs/minio-quickstart-guide)
