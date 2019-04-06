# MinIO Logging Quickstart Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)
This document explains how to configure MinIO server to log to different logging targets.

## Log Targets
MinIO supports currently two target types

- console
- http

### Console Target
Console target logs to `/dev/stderr` and is enabled by default. To turn-off console logging you would have to update your MinIO server configuration using `mc admin config set` command.

Assuming `mc` is already [configured](https://docs.min.io/docs/minio-client-quickstart-guide.html)
```
mc admin config get myminio/ > /tmp/config
```

Edit the `/tmp/config` and toggle `console` field `enabled` from `true` to `false`.

```json
	"logger": {
		"console": {
			"enabled": false
		}
	},
```

Once changed, now you may set the changed config to server through following commands.
```
mc admin config set myminio/ < /tmp/config
mc admin restart myminio/
```

### HTTP Target
HTTP target logs to a generic HTTP endpoint in JSON format and is not enabled by default. To enable HTTP target logging you would have to update your MinIO server configuration using `mc admin config set` command.

Assuming `mc` is already [configured](https://docs.min.io/docs/minio-client-quickstart-guide.html)
```
mc admin config get myminio/ > /tmp/config
```

Edit the `/tmp/config` and toggle `http` field `enabled` from `false` to `true`.
```json
	"logger": {
		"console": {
			"enabled": false
		},
		"http": {
			"1": {
				"enabled": true,
			    "endpoint": "http://endpoint:port/path"
			}
		}
	},
```
NOTE: `http://endpoint:port/path` is a placeholder value to indicate the URL format, please change this accordingly as per your configuration.

Once changed, now you may set the changed config to server through following commands.
```
mc admin config set myminio/ < /tmp/config
mc admin restart myminio/
```

MinIO also honors environment variable for HTTP target logging as shown below, this setting will override the endpoint settings in the MinIO server config.
```
MINIO_LOGGER_HTTP_ENDPOINT=http://localhost:8080/minio/logs minio server /mnt/data
```

## Audit Targets
For audit logging MinIO supports only HTTP target type for now. Audit logging is currently only available through environment variable.
```
MINIO_AUDIT_LOGGER_HTTP_ENDPOINT=http://localhost:8080/minio/logs/audit minio server /mnt/data
```

Setting this environment variable automatically enables audit logging to the HTTP target. The audit logging is in JSON format as described below.
```json
{
  "version": "1",
  "deploymentid": "1b3002bf-5005-4d9b-853e-64a05008ebb2",
  "time": "2018-11-21T23:16:06.828154172Z",
  "api": {
    "name": "PutObject",
    "bucket": "my-bucketname",
    "object": "my-objectname",
    "status": "OK",
    "statusCode": 200
  },
  "remotehost": "127.0.0.1",
  "requestID": "156946C6C1E7842C",
  "userAgent": "MinIO (linux; amd64) minio-go/v6.0.6",
  "requestClaims": {
    "accessKey": "A1YABB5YPX3ZPL4227XJ",
    "aud": "PoEgXP6uVO45IsENRngDXj5Au5Ya",
    "azp": "PoEgXP6uVO45IsENRngDXj5Au5Ya",
    "exp": 1542845766,
    "iat": 1542842166,
    "iss": "https://localhost:9443/oauth2/token",
    "jti": "33527fcc-254f-43d2-a558-4942554b8ff8"
  },
  "requestHeader": {
    "Authorization": "AWS4-HMAC-SHA256 Credential=A1YABB5YPX3ZPL4227XJ/20181121/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-security-token,Signature=689d9b8f67b5625ea2f0b8cbb3f777d8839a91d50aa81e6a5555f5a6360c1714",
    "Content-Length": "184",
    "Content-Type": "application/octet-stream",
    "User-Agent": "MinIO (linux; amd64) minio-go/v6.0.6",
    "X-Amz-Content-Sha256": "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
    "X-Amz-Date": "20181121T231606Z",
    "X-Amz-Decoded-Content-Length": "12",
    "X-Amz-Security-Token": "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NLZXkiOiJBMVlBQkI1WVBYM1pQTDQyMjdYSiIsImF1ZCI6IlBvRWdYUDZ1Vk80NUlzRU5SbmdEWGo1QXU1WWEiLCJhenAiOiJQb0VnWFA2dVZPNDVJc0VOUm5nRFhqNUF1NVlhIiwiZXhwIjoxNTQyODQ1NzY2LCJpYXQiOjE1NDI4NDIxNjYsImlzcyI6Imh0dHBzOi8vbG9jYWxob3N0Ojk0NDMvb2F1dGgyL3Rva2VuIiwianRpIjoiMzM1MjdmY2MtMjU0Zi00M2QyLWE1NTgtNDk0MjU1NGI4ZmY4In0.KEuAq2cQ3H7dfIB5DVuvcgBXT38mr0gthrIbVRSZcA2OWo8QiH1-DWXj9xYbndgr1p2tiEUsQ49cuszQGEVGMQ"
  },
  "responseHeader": {
    "Accept-Ranges": "bytes",
    "Content-Security-Policy": "block-all-mixed-content",
    "Content-Type": "application/xml",
    "Etag": "",
    "Server": "MinIO/DEVELOPMENT.2018-11-21T23-15-06Z (linux; amd64)",
    "Vary": "Origin",
    "X-Amz-Request-Id": "156946C6C1E7842C",
    "X-Minio-Deployment-Id": "1b3002bf-5005-4d9b-853e-64a05008ebb2",
    "X-Xss-Protection": "1; mode=block"
  }
}
```

## Explore Further
* [MinIO Quickstart Guide](https://docs.min.io/docs/minio-quickstart-guide)
* [Configure MinIO Server with TLS](https://docs.min.io/docs/how-to-secure-access-to-minio-server-with-tls)
