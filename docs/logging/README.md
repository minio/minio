# Minio Logging Quickstart Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)
This document explains how to configure Minio server to log to different targets.

## Targets
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

Additionally Minio also honors environment variable for HTTP target.
```
MINIO_LOGGER_HTTP_ENDPOINT=http://localhost:8080/ minio server /mnt/data
```

NOTE: In case of HTTP endpoint Minio automatically enables audit logging, i.e we log each incoming request to HTTP target. The audit log JSON format is as follows
```json
{
  "version": "1",
  "time": "2018-10-30T23:59:08.831972892Z",
  "api": {
    "name": "PutObject",
    "args": {
      "bucket": "ersan",
      "object": "hosts"
    }
  },
  "remotehost": "127.0.0.1",
  "requestID": "1562885B6A24D20F",
  "userAgent": "Minio (linux; amd64) minio-go/v6.0.8 mc/2018-10-29T19:07:05Z",
  "metadata": {
    "accessKey": "Q3AM3UQ867SPQQA43P2F",
    "etag": "6e5ad789aa1b14afcd57efc596408bb2",
    "region": "",
    "sourceIPAddress": "127.0.0.1"
  }
}
```

## Explore Further
* [Minio Quickstart Guide](https://docs.minio.io/docs/minio-quickstart-guide)
* [Configure Minio Server with TLS](https://docs.minio.io/docs/how-to-secure-access-to-minio-server-with-tls)
