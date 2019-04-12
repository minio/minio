# Kerberizing MinIO [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

MinIO can provide temporary access credentials for users authenticated by Kerberos, through a custom STS API implemented for Kerberos integration.

In this setup, the MinIO instance is a Kerberos service. Users authenticate with Kerberos and request a service ticket for the MinIO principal. This ticket is sent to the MinIO server; the server verifies this and responds with temporary access credentials.

## Setting up MinIO with Kerberos authentication

1. With a Kerberos KDC setup available, we first create the service principal for MinIO on the KDC server with:

``` shell
$ sudo kadmin.local -q "addprinc -randkey minio/myminio.com"
Authenticating as principal root/admin@MYREALM with password.
WARNING: no policy specified for minio/myminio.com@MYREALM; defaulting to no policy
Principal "minio/myminio.com@MYREALM" created.
```

2. Next create a keytab file for the MinIO principal:

``` shell
$ sudo kadmin.local -q "ktadd -k test.keytab minio/myminio.com"
Authenticating as principal root/admin@MYREALM with password.
Entry for principal minio/myminio.com with kvno 2, encryption type aes256-cts-hmac-sha1-96 added to keytab WRFILE:test.keytab.
Entry for principal minio/myminio.com with kvno 2, encryption type aes128-cts-hmac-sha1-96 added to keytab WRFILE:test.keytab.
```

3. Copy this keytab file over to the MinIO server(s).

4. Set the `MINIO_KERBEROS_KEYTAB` environment variable for the MinIO server to the path of the keytab file.

Now, the server will accept the Kerberos STS API.

## Kerberos STS API

To use the Kerberos STS API, the client generates a [KRB_AP_REQ message](https://tools.ietf.org/html/rfc4120#section-3.2.1), encodes it to Base64 and sends it to the server.

In the currently implemented version, the temporary credentials expire when the service ticket expires. New credentials can be requested with renewed service ticket. The temporary credentials also have read-write access to the MinIO server.

### Request Parameters

#### APReq

Contains Base64 encoded AP_REQ message to prove to the MinIO server that the user is indeed authenticated by Kerberos.


| Params | Value |
| :-- | :-- |
| *Type* | *String* |
| *Required* | *Yes* |

#### Version

Indicates STS API version information, the only supported value is '2011-06-15'.  This value is borrowed from AWS STS API documentation for compatibility reasons.

| Params | Value |
| :-- | :-- |
| *Type* | *String* |
| *Required* | *Yes* |

#### Response Elements

XML response for this API is similar to [AWS STS AssumeRoleWithWebIdentity](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html#API_AssumeRoleWithWebIdentity_ResponseElements) and is not included here for brevity.
