# Access Management Plugin Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

MinIO now includes support for using an Access Management Plugin. This is to allow object storage access control to be managed externally via a webhook.

When configured, MinIO sends request and credential details for every API call to an external HTTP(S) endpoint and expects an allow/deny response. MinIO is thus able to delegate access management to an external system, and users are able to use a custom solution instead of S3 standard IAM policies.

Latency sensitive applications may notice an increased latency due to a request to the external plugin upon every authenticated request to MinIO. User are advised to provision their infrastructure such that latency and performance is acceptable.

## Quickstart

To easily try out the feature, run the included demo Access Management Plugin program in this directory:

```sh
go run access-manager-plugin.go
```

This program, lets the admin user perform any action and prevents all other users from performing `s3:Put*` operations.

In another terminal start MinIO:

```sh
export MINIO_CI_CD=1
export MINIO_ROOT_USER=minio
export MINIO_ROOT_PASSWORD=minio123
export MINIO_POLICY_PLUGIN_URL=http://localhost:8080/
minio server /tmp/disk{1...4}
```

Now, let's test it out with `mc`:

```sh
mc alias set myminio http://localhost:9000 minio minio123
mc ls myminio
mc mb myminio/test
mc cp /etc/issue myminio/test
mc admin user add myminio foo foobar123
export MC_HOST_foo=http://foo:foobar123@localhost:9000
mc ls foo
mc cp /etc/issue myminio/test/issue2
```

Only the last operation would fail with a permissions error.

## Configuration

Access Management Plugin can be configured with environment variables:

```sh
$ mc admin config set myminio policy_plugin --env
KEY:
policy_plugin  enable Access Management Plugin for policy enforcement

ARGS:
MINIO_POLICY_PLUGIN_URL*          (url)       plugin hook endpoint (HTTP(S)) e.g. "http://localhost:8181/v1/data/httpapi/authz/allow"
MINIO_POLICY_PLUGIN_AUTH_TOKEN    (string)    authorization header for plugin hook endpoint
MINIO_POLICY_PLUGIN_ENABLE_HTTP2  (bool)      Enable experimental HTTP2 support to connect to plugin service (default: 'off')
MINIO_POLICY_PLUGIN_COMMENT       (sentence)  optionally add a comment to this setting
```

By default this plugin uses HTTP 1.x. To enable HTTP2 use the `MINIO_POLICY_PLUGIN_ENABLE_HTTP2` environment variable.

## Request and Response

MinIO will make a `POST` request with a JSON body to the given plugin URL. If the auth token parameter is set, it will be sent as an authorization header.

The JSON body structure can be seen from this sample:

<details><summary>Request Body Sample</summary>

```json
{
  "input": {
    "account": "minio",
    "groups": null,
    "action": "s3:ListBucket",
    "bucket": "test",
    "conditions": {
      "Authorization": [
        "AWS4-HMAC-SHA256 Credential=minio/20220507/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=62012db6c47d697620cf6c68f0f45f6e34894589a53ab1faf6dc94338468c78a"
      ],
      "CurrentTime": [
        "2022-05-07T18:31:41Z"
      ],
      "Delimiter": [
        "/"
      ],
      "EpochTime": [
        "1651948301"
      ],
      "Prefix": [
        ""
      ],
      "Referer": [
        ""
      ],
      "SecureTransport": [
        "false"
      ],
      "SourceIp": [
        "127.0.0.1"
      ],
      "User-Agent": [
        "MinIO (linux; amd64) minio-go/v7.0.24 mc/DEVELOPMENT.2022-04-20T23-07-53Z"
      ],
      "UserAgent": [
        "MinIO (linux; amd64) minio-go/v7.0.24 mc/DEVELOPMENT.2022-04-20T23-07-53Z"
      ],
      "X-Amz-Content-Sha256": [
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
      ],
      "X-Amz-Date": [
        "20220507T183141Z"
      ],
      "authType": [
        "REST-HEADER"
      ],
      "principaltype": [
        "Account"
      ],
      "signatureversion": [
        "AWS4-HMAC-SHA256"
      ],
      "userid": [
        "minio"
      ],
      "username": [
        "minio"
      ],
      "versionid": [
        ""
      ]
    },
    "owner": true,
    "object": "",
    "claims": {},
    "denyOnly": false
  }
}
```

</details>

The response expected by MinIO, is a JSON body with a boolean:

```json
{
    "result": true
}
```

The following structure is also accepted:

```json
{
    "result": {
        "allow": true
    }
}
```

Any unmentioned JSON object keys in the above are ignored.
