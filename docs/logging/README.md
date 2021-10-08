# MinIO Logging Quickstart Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)
This document explains how to configure MinIO server to log to different logging targets.

## Log Targets
MinIO supports currently two target types

- console
- http

### Console Target
Console target is on always and cannot be disabled.

### HTTP Target
HTTP target logs to a generic HTTP endpoint in JSON format and is not enabled by default. To enable HTTP target logging you would have to update your MinIO server configuration using `mc admin config set` command.

Assuming `mc` is already [configured](https://docs.min.io/docs/minio-client-quickstart-guide.html)
```
mc admin config get myminio/ logger_webhook
logger_webhook:name1 auth_token="" endpoint=""
```

```
mc admin config set myminio logger_webhook:name1 auth_token="" endpoint="http://endpoint:port/path"
mc admin service restart myminio
```

NOTE: `http://endpoint:port/path` is a placeholder value to indicate the URL format, please change this accordingly as per your configuration.

MinIO also honors environment variable for HTTP target logging as shown below, this setting will override the endpoint settings in the MinIO server config.
```
export MINIO_LOGGER_WEBHOOK_ENABLE_target1="on"
export MINIO_LOGGER_WEBHOOK_AUTH_TOKEN_target1="token"
export MINIO_LOGGER_WEBHOOK_ENDPOINT_target1=http://localhost:8080/minio/logs
minio server /mnt/data
```

## Audit Targets
Assuming `mc` is already [configured](https://docs.min.io/docs/minio-client-quickstart-guide.html)

### HTTP Target
```
mc admin config get myminio/ audit_webhook
audit_webhook:name1 enable=off endpoint= auth_token= client_cert= client_key=
```

```
mc admin config set myminio audit_webhook:name1 auth_token="" endpoint="http://endpoint:port/path"
mc admin service restart myminio
```

NOTE: `http://endpoint:port/path` is a placeholder value to indicate the URL format, please change this accordingly as per your configuration.

MinIO also honors environment variable for HTTP target Audit logging as shown below, this setting will override the endpoint settings in the MinIO server config.
```
export MINIO_AUDIT_WEBHOOK_ENABLE_target1="on"
export MINIO_AUDIT_WEBHOOK_AUTH_TOKEN_target1="token"
export MINIO_AUDIT_WEBHOOK_ENDPOINT_target1=http://localhost:8080/minio/logs
export MINIO_AUDIT_WEBHOOK_CLIENT_CERT="/tmp/cert.pem"
export MINIO_AUDIT_WEBHOOK_CLIENT_KEY=="/tmp/key.pem"
minio server /mnt/data
```

Setting this environment variable automatically enables audit logging to the HTTP target. The audit logging is in JSON format as described below.

NOTE:
- `timeToFirstByte` and `timeToResponse` will be expressed in Nanoseconds.
- Additionally in the case of the erasure coded setup `tags.objectErasureMap` provides per object details about
   - Pool number the object operation was performed on.
   - Set number the object operation was performed on.
   - The list of disks participating in this operation belong to the set.

```json
{
  "version": "1",
  "deploymentid": "51bcc7b9-a447-4251-a940-d9d0aab9af69",
  "time": "2021-10-08T00:46:36.801714978Z",
  "trigger": "incoming",
  "api": {
    "name": "PutObject",
    "bucket": "testbucket",
    "object": "hosts",
    "status": "OK",
    "statusCode": 200,
    "rx": 380,
    "tx": 476,
    "timeToResponse": "257694819ns"
  },
  "remotehost": "127.0.0.1",
  "requestID": "16ABE7A785E7AC2C",
  "userAgent": "MinIO (linux; amd64) minio-go/v7.0.15 mc/DEVELOPMENT.2021-10-06T23-39-34Z",
  "requestHeader": {
    "Authorization": "AWS4-HMAC-SHA256 Credential=minio/20211008/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length,Signature=4c60a59e5eb3b0a68693c7fee9dbb5a8a509e0717668669194d37bf182fde031",
    "Content-Length": "380",
    "Content-Type": "application/octet-stream",
    "User-Agent": "MinIO (linux; amd64) minio-go/v7.0.15 mc/DEVELOPMENT.2021-10-06T23-39-34Z",
    "X-Amz-Content-Sha256": "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
    "X-Amz-Date": "20211008T004636Z",
    "X-Amz-Decoded-Content-Length": "207",
    "X-Amz-Server-Side-Encryption": "aws:kms"
  },
  "responseHeader": {
    "Accept-Ranges": "bytes",
    "Content-Length": "0",
    "Content-Security-Policy": "block-all-mixed-content",
    "ETag": "4939450d1beec11e10a91ee7700bb593",
    "Server": "MinIO",
    "Strict-Transport-Security": "max-age=31536000; includeSubDomains",
    "Vary": "Origin,Accept-Encoding",
    "X-Amz-Request-Id": "16ABE7A785E7AC2C",
    "X-Amz-Server-Side-Encryption": "aws:kms",
    "X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id": "my-minio-key",
    "X-Content-Type-Options": "nosniff",
    "X-Xss-Protection": "1; mode=block",
    "x-amz-version-id": "ac4639f6-c544-4f3f-af1e-b4c0736f67f9"
  },
  "tags": {
    "objectErasureMap": {
      "hosts": {
        "poolId": 1,
        "setId": 1,
        "disks": [
          "/mnt/data1",
          "/mnt/data2",
          "/mnt/data3",
          "/mnt/data4"
        ]
      }
    }
  }
}
```

### Kafka Target
Assuming that you already have Apache Kafka configured and running.
```
mc admin config set myminio/ audit_kafka
KEY:
audit_kafka[:name]  send audit logs to kafka endpoints

ARGS:
brokers*         (csv)       comma separated list of Kafka broker addresses
topic            (string)    Kafka topic used for bucket notifications
sasl_username    (string)    username for SASL/PLAIN or SASL/SCRAM authentication
sasl_password    (string)    password for SASL/PLAIN or SASL/SCRAM authentication
sasl_mechanism   (string)    sasl authentication mechanism, default 'plain'
tls_client_auth  (string)    clientAuth determines the Kafka server's policy for TLS client auth
sasl             (on|off)    set to 'on' to enable SASL authentication
tls              (on|off)    set to 'on' to enable TLS
tls_skip_verify  (on|off)    trust server TLS without verification, defaults to "on" (verify)
client_tls_cert  (path)      path to client certificate for mTLS auth
client_tls_key   (path)      path to client key for mTLS auth
version          (string)    specify the version of the Kafka cluster
comment          (sentence)  optionally add a comment to this setting
```

Configure MinIO to send audit logs to locally running Kafka brokers
```
mc admin config set myminio/ audit_kafka:target1 brokers=localhost:29092 topic=auditlog
mc admin service restart myminio/
```

On another terminal assuming you have `kafkacat` installed

```
kafkacat -b localhost:29092 -t auditlog  -C

{"version":"1","deploymentid":"8a1d8091-b874-45df-b9ea-e044eede6ace","time":"2021-07-13T02:00:47.020547414Z","trigger":"incoming","api":{"name":"ListBuckets","status":"OK","statusCode":200,"timeToFirstByte":"261795ns","timeToResponse":"312490ns"},"remotehost":"127.0.0.1","requestID":"16913736591C237F","userAgent":"MinIO (linux; amd64) minio-go/v7.0.11 mc/DEVELOPMENT.2021-07-09T02-22-26Z","requestHeader":{"Authorization":"AWS4-HMAC-SHA256 Credential=minio/20210713/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=7fe65c5467e05ca21de64094688da43f96f34fec82e8955612827079f4600527","User-Agent":"MinIO (linux; amd64) minio-go/v7.0.11 mc/DEVELOPMENT.2021-07-09T02-22-26Z","X-Amz-Content-Sha256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","X-Amz-Date":"20210713T020047Z"},"responseHeader":{"Accept-Ranges":"bytes","Content-Length":"547","Content-Security-Policy":"block-all-mixed-content","Content-Type":"application/xml","Server":"MinIO","Vary":"Origin,Accept-Encoding","X-Amz-Request-Id":"16913736591C237F","X-Xss-Protection":"1; mode=block"}}
```

MinIO also honors environment variable for Kafka target Audit logging as shown below, this setting will override the endpoint settings in the MinIO server config.

```
mc admin config set myminio/ audit_kafka --env
KEY:
audit_kafka[:name]  send audit logs to kafka endpoints

ARGS:
MINIO_AUDIT_KAFKA_ENABLE*          (on|off)    enable audit_kafka target, default is 'off'
MINIO_AUDIT_KAFKA_BROKERS*         (csv)       comma separated list of Kafka broker addresses
MINIO_AUDIT_KAFKA_TOPIC            (string)    Kafka topic used for bucket notifications
MINIO_AUDIT_KAFKA_SASL_USERNAME    (string)    username for SASL/PLAIN or SASL/SCRAM authentication
MINIO_AUDIT_KAFKA_SASL_PASSWORD    (string)    password for SASL/PLAIN or SASL/SCRAM authentication
MINIO_AUDIT_KAFKA_SASL_MECHANISM   (string)    sasl authentication mechanism, default 'plain'
MINIO_AUDIT_KAFKA_TLS_CLIENT_AUTH  (string)    clientAuth determines the Kafka server's policy for TLS client auth
MINIO_AUDIT_KAFKA_SASL             (on|off)    set to 'on' to enable SASL authentication
MINIO_AUDIT_KAFKA_TLS              (on|off)    set to 'on' to enable TLS
MINIO_AUDIT_KAFKA_TLS_SKIP_VERIFY  (on|off)    trust server TLS without verification, defaults to "on" (verify)
MINIO_AUDIT_KAFKA_CLIENT_TLS_CERT  (path)      path to client certificate for mTLS auth
MINIO_AUDIT_KAFKA_CLIENT_TLS_KEY   (path)      path to client key for mTLS auth
MINIO_AUDIT_KAFKA_VERSION          (string)    specify the version of the Kafka cluster
MINIO_AUDIT_KAFKA_COMMENT          (sentence)  optionally add a comment to this setting
```

```
export MINIO_AUDIT_KAFKA_ENABLE_target1="on"
export MINIO_AUDIT_KAFKA_BROKERS_target1="localhost:29092"
export MINIO_AUDIT_KAFKA_TOPIC_target1="auditlog"
minio server /mnt/data
```

Setting this environment variable automatically enables audit logging to the Kafka target. The audit logging is in JSON format as described below.

NOTE:
- `timeToFirstByte` and `timeToResponse` will be expressed in Nanoseconds.
- Additionally in the case of the erasure coded setup `tags.objectErasureMap` provides per object details about
   - Pool number the object operation was performed on.
   - Set number the object operation was performed on.
   - The list of disks participating in this operation belong to the set.

## Explore Further
* [MinIO Quickstart Guide](https://docs.min.io/docs/minio-quickstart-guide)
* [Configure MinIO Server with TLS](https://docs.min.io/docs/how-to-secure-access-to-minio-server-with-tls)
