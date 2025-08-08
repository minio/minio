# AssumeRoleWithCertificate [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

## Introduction

MinIO provides a custom STS API that allows authentication with client X.509 / TLS certificates.

A major advantage of certificate-based authentication compared to other STS authentication methods, like OpenID Connect or LDAP/AD, is that client authentication works without any additional/external component that must be constantly available. Therefore, certificate-based authentication may provide better availability / lower operational complexity.

The MinIO TLS STS API can be configured via MinIO's standard configuration API (i.e. using `mc admin config set/get`). Further, it can be configured via the following environment variables:

```
mc admin config set myminio identity_tls --env
KEY:
identity_tls  enable X.509 TLS certificate SSO support

ARGS:
MINIO_IDENTITY_TLS_SKIP_VERIFY  (on|off)    trust client certificates without verification. Defaults to "off" (verify)
```

The MinIO TLS STS API is disabled by default. However, it can be *enabled* by setting environment variable:

```
export MINIO_IDENTITY_TLS_ENABLE=on
```

## Example

MinIO exposes a custom S3 STS API endpoint as `Action=AssumeRoleWithCertificate`. A client has to send an HTTP `POST` request to `https://<host>:<port>?Action=AssumeRoleWithCertificate&Version=2011-06-15`. Since the authentication and authorization happens via X.509 certificates the client has to send the request over **TLS** and has to provide
a client certificate.

The following curl example shows how to authenticate to a MinIO server with client certificate and obtain STS access credentials.

```curl
curl -X POST --key private.key --cert public.crt "https://minio:9000?Action=AssumeRoleWithCertificate&Version=2011-06-15&DurationSeconds=3600"
```

```xml
<?xml version="1.0" encoding="UTF-8"?>
<AssumeRoleWithCertificateResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
   <AssumeRoleWithCertificateResult>
      <Credentials>
         <AccessKeyId>YC12ZBHUVW588BQAE5BM</AccessKeyId>
         <SecretAccessKey>Zgl9+zdE0pZ88+hLqtfh0ocLN+WQTJixHouCkZkW</SecretAccessKey>
         <Expiration>2021-07-19T20:10:45Z</Expiration
         <SessionToken>eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NLZXkiOiJZQzEyWkJIVVZXNTg4QlFBRTVCTSIsImV4cCI6MTYyNjcyNTQ0NX0.wvMUf3w_x16qpVWgua8WxnV1Sgtv1jOnSu03vbrwOMzV3cI4q3_9WZD9LwlP-34DTsvbsg7gCBGh6YNriMMiQw</SessionToken>
      </Credentials>
   </AssumeRoleWithCertificateResult>
   <ResponseMetadata>
      <RequestId>169339CD8B3A6948</RequestId>
   </ResponseMetadata>
</AssumeRoleWithCertificateResponse>
```

## Authentication Flow

A client can request temp. S3 credentials via the STS API. It can authenticate via a client certificate and obtain a access/secret key pair as well as a session token. These credentials are associated to an S3 policy at the MinIO server.

In case of certificate-based authentication, MinIO has to map the client-provided certificate to an S3 policy. MinIO does this via the subject common name field of the X.509 certificate. So, MinIO will associate a certificate with a subject `CN = foobar` to a S3 policy named `foobar`.

The following self-signed certificate is issued for `consoleAdmin`. So, MinIO would associate it with the pre-defined `consoleAdmin` policy.

```
Certificate:
    Data:
        Version: 3 (0x2)
        Serial Number:
            35:ac:60:46:ad:8d:de:18:dc:0b:f6:98:14:ee:89:e8
        Signature Algorithm: ED25519
        Issuer: CN = consoleAdmin
        Validity
            Not Before: Jul 19 15:08:44 2021 GMT
            Not After : Aug 18 15:08:44 2021 GMT
        Subject: CN = consoleAdmin
        Subject Public Key Info:
            Public Key Algorithm: ED25519
                ED25519 Public-Key:
                pub:
                    5a:91:87:b8:77:fe:d4:af:d9:c7:c7:ce:55:ae:74:
                    aa:f3:f1:fe:04:63:9b:cb:20:97:61:97:90:94:fa:
                    12:8b
        X509v3 extensions:
            X509v3 Key Usage: critical
                Digital Signature
            X509v3 Extended Key Usage: 
                TLS Web Client Authentication
            X509v3 Basic Constraints: critical
                CA:FALSE
    Signature Algorithm: ED25519
         7e:aa:be:ed:47:4d:b9:2f:fc:ed:7f:5a:fc:6b:c0:05:5b:f5:
         a0:31:fe:86:e3:8e:3f:49:af:6d:d5:ac:c7:c4:57:47:ce:97:
         7d:ab:b8:e9:75:ec:b4:39:fb:c8:cf:53:16:5b:1f:15:b6:7f:
         5a:d1:35:2d:fc:31:3a:10:e7:0c
```

> Observe the `Subject: CN = consoleAdmin` field.

Also, note that the certificate has to contain the `Extended Key Usage: TLS Web Client Authentication`. Otherwise, MinIO would not accept the certificate as client certificate.

Now, the STS certificate-based authentication happens in 4 steps:

- Client sends HTTP `POST` request over a TLS connection hitting the MinIO TLS STS API.
- MinIO verifies that the client certificate is valid.
- MinIO tries to find a policy that matches the `CN` of the client certificate.
- MinIO returns temp. S3 credentials associated to the found policy.

The returned credentials expiry after a certain period of time that can be configured via `&DurationSeconds=3600`. By default, the STS credentials are valid for 1 hour. The minimum expiration allowed is 15 minutes.

Further, the temp. S3 credentials will never out-live the client certificate. For example, if the `MINIO_IDENTITY_TLS_STS_EXPIRY` is 7 days but the certificate itself is only valid for the next 3 days, then MinIO will return S3 credentials that are valid for 3 days only.

## Caveat

*Applications that use direct S3 API will work fine, however interactive users uploading content using (when POSTing to the presigned URL an app generates) a popup becomes visible on browser to provide client certs, you would have to manually cancel and continue. This may be annoying to use but there is no workaround for now.*

## Explore Further

- [MinIO Admin Complete Guide](https://docs.min.io/community/minio-object-store/reference/minio-mc-admin.html)
- [The MinIO documentation website](https://docs.min.io/community/minio-object-store/index.html)
