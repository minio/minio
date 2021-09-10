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
  "deploymentid": "bc0e4d1e-bacc-42eb-91ad-2d7f3eacfa8d",
  "time": "2019-08-12T21:34:37.187817748Z",
  "api": {
    "name": "PutObject",
    "bucket": "testbucket",
    "object": "hosts",
    "status": "OK",
    "statusCode": 200,
    "timeToFirstByte": "366333ns",
    "timeToResponse": "16438202ns"
  },
  "remotehost": "127.0.0.1",
  "requestID": "15BA4A72C0C70AFC",
  "userAgent": "MinIO (linux; amd64) minio-go/v6.0.32 mc/2019-08-12T18:27:13Z",
  "requestHeader": {
    "Authorization": "AWS4-HMAC-SHA256 Credential=minio/20190812/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length,Signature=d3f02a6aeddeb29b06e1773b6a8422112890981269f2463a26f307b60423177c",
    "Content-Length": "686",
    "Content-Type": "application/octet-stream",
    "User-Agent": "MinIO (linux; amd64) minio-go/v6.0.32 mc/2019-08-12T18:27:13Z",
    "X-Amz-Content-Sha256": "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
    "X-Amz-Date": "20190812T213437Z",
    "X-Amz-Decoded-Content-Length": "512"
  },
  "responseHeader": {
    "Accept-Ranges": "bytes",
    "Content-Length": "0",
    "Content-Security-Policy": "block-all-mixed-content",
    "ETag": "a414c889dc276457bd7175f974332cb0-1",
    "Server": "MinIO/DEVELOPMENT.2019-08-12T21-28-07Z",
    "Vary": "Origin",
    "X-Amz-Request-Id": "15BA4A72C0C70AFC",
    "X-Xss-Protection": "1; mode=block"
  },
  "tags": {
    "objectErasureMap": {
      "object": {
        "poolId": 1,
        "setId": 10,
        "disks": [
          "http://server01/mnt/pool1/disk01",
          "http://server02/mnt/pool1/disk02",
          "http://server03/mnt/pool1/disk03",
          "http://server04/mnt/pool1/disk04"
        ]
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
