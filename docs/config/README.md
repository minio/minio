# MinIO Server Config Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

## Configuration Directory

Till MinIO release `RELEASE.2018-08-02T23-11-36Z`, MinIO server configuration file (`config.json`) was stored in the configuration directory specified by `--config-dir` or defaulted to `${HOME}/.minio`. However from releases after `RELEASE.2018-08-18T03-49-57Z`, the configuration file (only), has been migrated to the storage backend (storage backend is the directory passed to MinIO server while starting the server).

You can specify the location of your existing config using `--config-dir`, MinIO will migrate the `config.json` to your backend storage. Your current `config.json` will be renamed upon successful migration as `config.json.deprecated` in your current `--config-dir`. All your existing configurations are honored after this migration.

Additionally `--config-dir` is now a legacy option which will is scheduled for removal in future, so please update your local startup, ansible scripts accordingly.

```sh
minio server /data
```

MinIO also encrypts all the config, IAM and policies content with admin credentials.

### Certificate Directory

TLS certificates by default are stored under ``${HOME}/.minio/certs`` directory. You need to place certificates here to enable `HTTPS` based access. Read more about [How to secure access to MinIO server with TLS](https://docs.min.io/docs/how-to-secure-access-to-minio-server-with-tls).

Following is the directory structure for MinIO server with TLS certificates.

```sh
$ mc tree --files ~/.minio
/home/user1/.minio
└─ certs
   ├─ CAs
   ├─ private.key
   └─ public.crt
```

You can provide a custom certs directory using `--certs-dir` command line option.

#### Credentials
On MinIO admin credentials or root credentials are only allowed to be changed using ENVs namely `MINIO_ACCESS_KEY` and `MINIO_SECRET_KEY`. Using the combination of these two values MinIO encrypts the config stored at the backend.

```
export MINIO_ACCESS_KEY=minio
export MINIO_SECRET_KEY=minio13
minio server /data
```

##### Rotating encryption with new credentials

Additionally if you wish to change the admin credentials, then MinIO will automatically detect this and re-encrypt with new credentials as shown below. For one time only special ENVs as shown below needs to be set for rotating the encryption config.

> Old ENVs are never remembered in memory and are destroyed right after they are used to migrate your existing content with new credentials. You are safe to remove them after the server as successfully started, by restarting the services once again.

```
export MINIO_ACCESS_KEY=newminio
export MINIO_SECRET_KEY=newminio123
export MINIO_ACCESS_KEY_OLD=minio
export MINIO_SECRET_KEY_OLD=minio123
minio server /data
```

Once the migration is complete, server will automatically unset the `MINIO_ACCESS_KEY_OLD` and `MINIO_SECRET_KEY_OLD` with in the process namespace.

> **NOTE: Make sure to remove `MINIO_ACCESS_KEY_OLD` and `MINIO_SECRET_KEY_OLD` in scripts or service files before next service restarts of the server to avoid double encryption of your existing contents.**

#### Region
```
KEY:
region  label the location of the server

ARGS:
name     (string)    name of the location of the server e.g. "us-west-rack2"
comment  (sentence)  optionally add a comment to this setting
```

or environment variables
```
KEY:
region  label the location of the server

ARGS:
MINIO_REGION_NAME     (string)    name of the location of the server e.g. "us-west-rack2"
MINIO_REGION_COMMENT  (sentence)  optionally add a comment to this setting
```

Example:

```sh
export MINIO_REGION_NAME="my_region"
minio server /data
```

### Storage Class
By default, parity for objects with standard storage class is set to `N/2`, and parity for objects with reduced redundancy storage class objects is set to `2`. Read more about storage class support in MinIO server [here](https://github.com/minio/minio/blob/master/docs/erasure/storage-class/README.md).

```
KEY:
storage_class  define object level redundancy

ARGS:
standard  (string)    set the parity count for default standard storage class e.g. "EC:4"
rrs       (string)    set the parity count for reduced redundancy storage class e.g. "EC:2"
comment   (sentence)  optionally add a comment to this setting
```

or environment variables
```
KEY:
storage_class  define object level redundancy

ARGS:
MINIO_STORAGE_CLASS_STANDARD  (string)    set the parity count for default standard storage class e.g. "EC:4"
MINIO_STORAGE_CLASS_RRS       (string)    set the parity count for reduced redundancy storage class e.g. "EC:2"
MINIO_STORAGE_CLASS_COMMENT   (sentence)  optionally add a comment to this setting
```

### Cache
MinIO provides caching storage tier for primarily gateway deployments, allowing you to cache content for faster reads, cost savings on repeated downloads from the cloud.

```
KEY:
cache  add caching storage tier

ARGS:
drives*  (csv)       comma separated mountpoints e.g. "/optane1,/optane2"
expiry   (number)    cache expiry duration in days e.g. "90"
quota    (number)    limit cache drive usage in percentage e.g. "90"
exclude  (csv)       comma separated wildcard exclusion patterns e.g. "bucket/*.tmp,*.exe"
after    (number)    minimum number of access before caching an object
comment  (sentence)  optionally add a comment to this setting
```

or environment variables
```
KEY:
cache  add caching storage tier

ARGS:
MINIO_CACHE_DRIVES*  (csv)       comma separated mountpoints e.g. "/optane1,/optane2"
MINIO_CACHE_EXPIRY   (number)    cache expiry duration in days e.g. "90"
MINIO_CACHE_QUOTA    (number)    limit cache drive usage in percentage e.g. "90"
MINIO_CACHE_EXCLUDE  (csv)       comma separated wildcard exclusion patterns e.g. "bucket/*.tmp,*.exe"
MINIO_CACHE_AFTER    (number)    minimum number of access before caching an object
MINIO_CACHE_COMMENT  (sentence)  optionally add a comment to this setting
```

#### Etcd
MinIO supports storing encrypted IAM assets and bucket DNS records on etcd.

> NOTE: if *path_prefix* is set then MinIO will not federate your buckets, namespaced IAM assets are assumed as isolated tenants, only buckets are considered globally unique but performing a lookup with a *bucket* which belongs to a different tenant will fail unlike federated setups where MinIO would port-forward and route the request to relevant cluster accordingly. This is a special feature, federated deployments should not need to set *path_prefix*.

```
KEY:
etcd  federate multiple clusters for IAM and Bucket DNS

ARGS:
endpoints*       (csv)       comma separated list of etcd endpoints e.g. "http://localhost:2379"
path_prefix      (path)      namespace prefix to isolate tenants e.g. "customer1/"
coredns_path     (path)      shared bucket DNS records, default is "/skydns"
client_cert      (path)      client cert for mTLS authentication
client_cert_key  (path)      client cert key for mTLS authentication
comment          (sentence)  optionally add a comment to this setting
```

or environment variables
```
KEY:
etcd  federate multiple clusters for IAM and Bucket DNS

ARGS:
MINIO_ETCD_ENDPOINTS*       (csv)       comma separated list of etcd endpoints e.g. "http://localhost:2379"
MINIO_ETCD_PATH_PREFIX      (path)      namespace prefix to isolate tenants e.g. "customer1/"
MINIO_ETCD_COREDNS_PATH     (path)      shared bucket DNS records, default is "/skydns"
MINIO_ETCD_CLIENT_CERT      (path)      client cert for mTLS authentication
MINIO_ETCD_CLIENT_CERT_KEY  (path)      client cert key for mTLS authentication
MINIO_ETCD_COMMENT          (sentence)  optionally add a comment to this setting
```

### API
By default, there is no limitation on the number of concurrents requests that a server/cluster processes at the same time. However, it is possible to impose such limitation using the API subsystem. Read more about throttling limitation in MinIO server [here](https://github.com/minio/minio/blob/master/docs/throttle/README.md).

```
KEY:
api  manage global HTTP API call specific features, such as throttling, authentication types, etc.

ARGS:
requests_max       (number)    set the maximum number of concurrent requests, e.g. "1600"
requests_deadline  (duration)  set the deadline for API requests waiting to be processed e.g. "1m"
ready_deadline     (duration)  set the deadline for health check API /minio/health/ready e.g. "1m"
cors_allow_origin  (csv)       set comma separated list of origins allowed for CORS requests e.g. "https://example1.com,https://example2.com"
```

or environment variables

```
MINIO_API_REQUESTS_MAX       (number)    set the maximum number of concurrent requests, e.g. "1600"
MINIO_API_REQUESTS_DEADLINE  (duration)  set the deadline for API requests waiting to be processed e.g. "1m"
MINIO_API_READY_DEADLINE     (duration)  set the deadline for health check API /minio/health/ready e.g. "1m"
MINIO_API_CORS_ALLOW_ORIGIN  (csv)       set comma separated list of origins allowed for CORS requests e.g. "https://example1.com,https://example2.com"
```

#### Notifications
Notification targets supported by MinIO are in the following list. To configure individual targets please refer to more detailed documentation [here](https://docs.min.io/docs/minio-bucket-notification-guide.html)

```
notify_webhook        publish bucket notifications to webhook endpoints
notify_amqp           publish bucket notifications to AMQP endpoints
notify_kafka          publish bucket notifications to Kafka endpoints
notify_mqtt           publish bucket notifications to MQTT endpoints
notify_nats           publish bucket notifications to NATS endpoints
notify_nsq            publish bucket notifications to NSQ endpoints
notify_mysql          publish bucket notifications to MySQL databases
notify_postgres       publish bucket notifications to Postgres databases
notify_elasticsearch  publish bucket notifications to Elasticsearch endpoints
notify_redis          publish bucket notifications to Redis datastores
```

### Accessing configuration
All configuration changes can be made using [`mc admin config` get/set/reset/export/import commands](https://github.com/minio/mc/blob/master/docs/minio-admin-complete-guide.md).

#### List all config keys available
```
~ mc admin config set myminio/
```

#### Obtain help for each key
```
~ mc admin config set myminio/ <key>
```

e.g: `mc admin config set myminio/ etcd` returns available `etcd` config args

```
~ mc admin config set play/ etcd
KEY:
etcd  federate multiple clusters for IAM and Bucket DNS

ARGS:
endpoints*       (csv)       comma separated list of etcd endpoints e.g. "http://localhost:2379"
path_prefix      (path)      namespace prefix to isolate tenants e.g. "customer1/"
coredns_path     (path)      shared bucket DNS records, default is "/skydns"
client_cert      (path)      client cert for mTLS authentication
client_cert_key  (path)      client cert key for mTLS authentication
comment          (sentence)  optionally add a comment to this setting
```

To get ENV equivalent for each config args use `--env` flag
```
~ mc admin config set play/ etcd --env
KEY:
etcd  federate multiple clusters for IAM and Bucket DNS

ARGS:
MINIO_ETCD_ENDPOINTS*       (csv)       comma separated list of etcd endpoints e.g. "http://localhost:2379"
MINIO_ETCD_PATH_PREFIX      (path)      namespace prefix to isolate tenants e.g. "customer1/"
MINIO_ETCD_COREDNS_PATH     (path)      shared bucket DNS records, default is "/skydns"
MINIO_ETCD_CLIENT_CERT      (path)      client cert for mTLS authentication
MINIO_ETCD_CLIENT_CERT_KEY  (path)      client cert key for mTLS authentication
MINIO_ETCD_COMMENT          (sentence)  optionally add a comment to this setting
```

This behavior is consistent across all keys, each key self documents itself with valid examples.

## Environment only settings (not in config)

#### Usage crawler
> NOTE: Data usage crawler is not supported under Gateway deployments.

Data usage crawler is enabled by default, following ENVs allow for more staggered delay in terms of usage calculation.

The crawler adapts to the system speed and completely pauses when the system is under load. It is possible to adjust the speed of the crawler and thereby the latency of updates being reflected. The delays between each operation of the crawl can be adjusted by the `MINIO_DISK_USAGE_CRAWL_DELAY` environment variable. By default the value is `10`. This means the crawler will sleep *10x* the time each operation takes.

This will in most setups make the crawler slow enough to not impact overall system performance. Setting `MINIO_DISK_USAGE_CRAWL_DELAY` to a *lower* value will make the crawler faster and setting it to 0 will make the crawler run at full speed (not recommended). Setting it to a higher value will make the crawler slower, further consume less resources.

Example: Following setting will decrease the crawler speed by a factor of 3, reducing the system resource use, but increasing the latency of updates being reflected.

```sh
export MINIO_DISK_USAGE_CRAWL_DELAY=30
minio server /data
```

### Browser

Enable or disable access to web UI. By default it is set to `on`. You may override this field with `MINIO_BROWSER` environment variable.

Example:

```sh
export MINIO_BROWSER=off
minio server /data
```

### Domain

By default, MinIO supports path-style requests that are of the format http://mydomain.com/bucket/object. `MINIO_DOMAIN` environment variable is used to enable virtual-host-style requests. If the request `Host` header matches with `(.+).mydomain.com` then the matched pattern `$1` is used as bucket and the path is used as object. More information on path-style and virtual-host-style [here](http://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAPI.html)
Example:

```sh
export MINIO_DOMAIN=mydomain.com
minio server /data
```

For advanced use cases `MINIO_DOMAIN` environment variable supports multiple-domains with comma separated values.
```sh
export MINIO_DOMAIN=sub1.mydomain.com,sub2.mydomain.com
minio server /data
```

## Explore Further
* [MinIO Quickstart Guide](https://docs.min.io/docs/minio-quickstart-guide)
* [Configure MinIO Server with TLS](https://docs.min.io/docs/how-to-secure-access-to-minio-server-with-tls)
