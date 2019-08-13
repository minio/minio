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
  "deploymentid": "bc0e4d1e-bacc-42eb-91ad-2d7f3eacfa8d",
  "time": "2019-08-12T21:34:37.187817748Z",
  "api": {
    "name": "PutObject",
    "bucket": "testbucket",
    "object": "hosts",
    "status": "OK",
    "statusCode": 200,
    "timeToFirstByte": "0s",
    "timeToResponse": "2.143308ms"
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
  }
}
```

## Explore Further
* [MinIO Quickstart Guide](https://docs.min.io/docs/minio-quickstart-guide)
* [Configure MinIO Server with TLS](https://docs.min.io/docs/how-to-secure-access-to-minio-server-with-tls)
