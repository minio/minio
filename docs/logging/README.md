# MinIO Logging Quickstart Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

This document explains how to configure MinIO server to log to different logging targets.

## Log Targets

MinIO supports currently two target types

- console
- http

### Logging Console Target

Console target is on always and cannot be disabled.

### Logging HTTP Target

HTTP target logs to a generic HTTP endpoint in JSON format and is not enabled by default. To enable HTTP target logging you would have to update your MinIO server configuration using `mc admin config set` command.

Assuming `mc` is already [configured](https://docs.min.io/community/minio-object-store/reference/minio-mc.html#quickstart)

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

Assuming `mc` is already [configured](https://docs.min.io/community/minio-object-store/reference/minio-mc.html#quickstart)

### Audit HTTP Target

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
- Additionally in the case of the erasure coded setup `tags.objectLocation` provides per object details about
  - Pool number the object operation was performed on.
  - Set number the object operation was performed on.
  - The list of drives participating in this operation belong to the set.

```json
{
  "version": "1",
  "deploymentid": "90e81272-45d9-4fe8-9c45-c9a7322bf4b5",
  "time": "2024-05-09T07:38:10.449688982Z",
  "event": "",
  "trigger": "incoming",
  "api": {
    "name": "PutObject",
    "bucket": "testbucket",
    "object": "hosts",
    "status": "OK",
    "statusCode": 200,
    "rx": 401,
    "tx": 0,
    "timeToResponse": "13309747ns",
    "timeToResponseInNS": "13309747"
  },
  "remotehost": "127.0.0.1",
  "requestID": "17CDC1F4D7E69123",
  "userAgent": "MinIO (linux; amd64) minio-go/v7.0.70 mc/RELEASE.2024-04-30T17-44-48Z",
  "requestPath": "/testbucket/hosts",
  "requestHost": "localhost:9000",
  "requestHeader": {
    "Accept-Encoding": "zstd,gzip",
    "Authorization": "AWS4-HMAC-SHA256 Credential=minioadmin/20240509/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length,Signature=d4d6862e6cc61011a61fa801da71048ece4f32a0562cad6bb88bdda50d7fcb95",
    "Content-Length": "401",
    "Content-Type": "application/octet-stream",
    "User-Agent": "MinIO (linux; amd64) minio-go/v7.0.70 mc/RELEASE.2024-04-30T17-44-48Z",
    "X-Amz-Content-Sha256": "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
    "X-Amz-Date": "20240509T073810Z",
    "X-Amz-Decoded-Content-Length": "228"
  },
  "responseHeader": {
    "Accept-Ranges": "bytes",
    "Content-Length": "0",
    "ETag": "9fe7a344ef4227d3e53751e9d88ce41e",
    "Server": "MinIO",
    "Strict-Transport-Security": "max-age=31536000; includeSubDomains",
    "Vary": "Origin,Accept-Encoding",
    "X-Amz-Id-2": "dd9025bab4ad464b049177c95eb6ebf374d3b3fd1af9251148b658df7ac2e3e8",
    "X-Amz-Request-Id": "17CDC1F4D7E69123",
    "X-Content-Type-Options": "nosniff",
    "X-Xss-Protection": "1; mode=block"
  },
  "tags": {
    "objectLocation": {
      "name": "hosts",
      "poolId": 1,
      "setId": 1,
      "drives": [
        "/mnt/data1",
        "/mnt/data2",
        "/mnt/data3",
        "/mnt/data4"
      ]
    }
  },
  "accessKey": "minioadmin"
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

{"version":"1","deploymentid":"90e81272-45d9-4fe8-9c45-c9a7322bf4b5","time":"2024-05-09T07:38:10.449688982Z","event":"","trigger":"incoming","api":{"name":"PutObject","bucket":"testbucket","object":"hosts","status":"OK","statusCode":200,"rx":401,"tx":0,"timeToResponse":"13309747ns","timeToResponseInNS":"13309747"},"remotehost":"127.0.0.1","requestID":"17CDC1F4D7E69123","userAgent":"MinIO (linux; amd64) minio-go/v7.0.70 mc/RELEASE.2024-04-30T17-44-48Z","requestPath":"/testbucket/hosts","requestHost":"localhost:9000","requestHeader":{"Accept-Encoding":"zstd,gzip","Authorization":"AWS4-HMAC-SHA256 Credential=minioadmin/20240509/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length,Signature=d4d6862e6cc61011a61fa801da71048ece4f32a0562cad6bb88bdda50d7fcb95","Content-Length":"401","Content-Type":"application/octet-stream","User-Agent":"MinIO (linux; amd64) minio-go/v7.0.70 mc/RELEASE.2024-04-30T17-44-48Z","X-Amz-Content-Sha256":"STREAMING-AWS4-HMAC-SHA256-PAYLOAD","X-Amz-Date":"20240509T073810Z","X-Amz-Decoded-Content-Length":"228"},"responseHeader":{"Accept-Ranges":"bytes","Content-Length":"0","ETag":"9fe7a344ef4227d3e53751e9d88ce41e","Server":"MinIO","Strict-Transport-Security":"max-age=31536000; includeSubDomains","Vary":"Origin,Accept-Encoding","X-Amz-Id-2":"dd9025bab4ad464b049177c95eb6ebf374d3b3fd1af9251148b658df7ac2e3e8","X-Amz-Request-Id":"17CDC1F4D7E69123","X-Content-Type-Options":"nosniff","X-Xss-Protection":"1; mode=block"},"tags":{"objectLocation":{"name":"hosts","poolId":1,"setId":1,"drives":["/mnt/data1","/mnt/data2","/mnt/data3","/mnt/data4"]}},"accessKey":"minioadmin"}
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
- Additionally in the case of the erasure coded setup `tags.objectLocation` provides per object details about
  - Pool number the object operation was performed on.
  - Set number the object operation was performed on.
  - The list of drives participating in this operation belong to the set.

## Explore Further

- [MinIO Quickstart Guide](https://docs.min.io/community/minio-object-store/operations/deployments/baremetal-deploy-minio-on-redhat-linux.html)
- [Configure MinIO Server with TLS](https://docs.min.io/community/minio-object-store/operations/network-encryption.html)
