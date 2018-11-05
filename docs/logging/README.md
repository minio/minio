# Minio Logging Quickstart Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)
This document explains how to configure Minio server to log to different logging targets.

## Log Targets
Minio supports currently two target types

- console
- http

### Console Target
Console target logs to `/dev/stderr` and is enabled by default. To turn-off console logging you would have to update your Minio server configuration using `mc admin config set` command.

Assuming `mc` is already [configured](https://docs.minio.io/docs/minio-client-quickstart-guide.html)
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
HTTP target logs to a generic HTTP endpoint in JSON format and is not enabled by default. To enable HTTP target logging you would have to update your Minio server configuration using `mc admin config set` command.

Assuming `mc` is already [configured](https://docs.minio.io/docs/minio-client-quickstart-guide.html)
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

Minio also honors environment variable for HTTP target logging as shown below, this setting will override the endpoint settings in the Minio server config.
```
MINIO_LOGGER_HTTP_ENDPOINT=http://localhost:8080/minio/logs minio server /mnt/data
```

## Audit Targets
For audit logging Minio supports only HTTP target type for now. Audit logging is currently only available through environment variable.
```
MINIO_AUDIT_LOGGER_HTTP_ENDPOINT=http://localhost:8080/minio/logs/audit minio server /mnt/data
```

Setting this environment variable automatically enables audit logging to the HTTP target. The audit logging is in JSON format as described below.
```json
{
  "version": "1",
  "deploymentid": "1b3002bf-5005-4d9b-853e-64a05008ebb2",
  "time": "2018-11-02T21:57:58.231480177Z",
  "api": {
    "name": "ListBuckets",
    "args": {}
  },
  "remotehost": "127.0.0.1",
  "requestID": "15636D7C53428FD4",
  "userAgent": "Minio (linux; amd64) minio-go/v6.0.8 mc/2018-11-02T21:13:30Z",
  "requestHeader": {
    "Authorization": "AWS4-HMAC-SHA256 Credential=minio/20181102/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=6db486b42a85b23bffba66d654ce60242a7e92fb27cd4a1756e68082c02cc204",
    "User-Agent": "Minio (linux; amd64) minio-go/v6.0.8 mc/2018-11-02T21:13:30Z",
    "X-Amz-Content-Sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    "X-Amz-Date": "20181102T215758Z"
  },
  "responseHeader": {
    "Accept-Ranges": "bytes",
    "Content-Security-Policy": "block-all-mixed-content",
    "Content-Type": "application/xml",
    "Server": "Minio/DEVELOPMENT.2018-11-02T21-57-15Z (linux; amd64)",
    "Vary": "Origin",
    "X-Amz-Request-Id": "15636D7C53428FD4",
    "X-Xss-Protection": "1; mode=block"
  }
}
```

## Explore Further
* [Minio Quickstart Guide](https://docs.minio.io/docs/minio-quickstart-guide)
* [Configure Minio Server with TLS](https://docs.minio.io/docs/how-to-secure-access-to-minio-server-with-tls)
