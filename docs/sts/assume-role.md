# AssumeRole [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

## Introduction

Returns a set of temporary security credentials that you can use to access MinIO resources. AssumeRole requires authorization credentials for an existing user on MinIO. The advantages of this API are

- To be able to reliably use S3 multipart APIs feature of the SDKs without re-inventing the wheel of pre-signing the each URL in multipart API. This is very tedious to implement with all the scenarios of fault tolerance that's already implemented by the client SDK. The general client SDKs don't support multipart with presigned URLs.
- To be able to easily get the temporary credentials to upload to a prefix. Make it possible for a client to upload a whole folder using the session. The server side applications need not create a presigned URL and serve to the client for each file. Since, the client would have the session it can do it by itself.

The temporary security credentials returned by this API consists of an access key, a secret key, and a security token. Applications can use these temporary security credentials to sign calls to MinIO API operations. The policy applied to these temporary credentials is inherited from the MinIO user credentials. By default, the temporary security credentials created by AssumeRole last for one hour. However, use the optional DurationSeconds parameter to specify the duration of the credentials. This value varies from 900 seconds (15 minutes) up to the maximum session duration of 365 days.

## API Request Parameters

### Version

Indicates STS API version information, the only supported value is '2011-06-15'. This value is borrowed from AWS STS API documentation for compatibility reasons.

| Params     | Value    |
| :--        | :--      |
| *Type*     | *String* |
| *Required* | *Yes*    |

### AUTHPARAMS

Indicates STS API Authorization information. If you are familiar with AWS Signature V4 Authorization header, this STS API supports signature V4 authorization as mentioned [here](https://docs.aws.amazon.com/general/latest/gr/signature-version-4.html)

### DurationSeconds

The duration, in seconds. The value can range from 900 seconds (15 minutes) up to 365 days. If value is higher than this setting, then operation fails. By default, the value is set to 3600 seconds.

| Params        | Value                                              |
| :--           | :--                                                |
| *Type*        | *Integer*                                          |
| *Valid Range* | *Minimum value of 900. Maximum value of 31536000.* |
| *Required*    | *No*                                               |

### Policy

An IAM policy in JSON format that you want to use as an inline session policy. This parameter is optional. Passing policies to this operation returns new temporary credentials. The resulting session's permissions are the intersection of the canned policy name and the policy set here. You cannot use this policy to grant more permissions than those allowed by the canned policy name being assumed.

| Params        | Value                                          |
| :--           | :--                                            |
| *Type*        | *String*                                       |
| *Valid Range* | *Minimum length of 1. Maximum length of 2048.* |
| *Required*    | *No*                                           |

### Response Elements

XML response for this API is similar to [AWS STS AssumeRole](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html#API_AssumeRole_ResponseElements)

### Errors

XML error response for this API is similar to [AWS STS AssumeRole](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html#API_AssumeRole_Errors)

## Sample `POST` Request

```
http://minio:9000/?Action=AssumeRole&DurationSeconds=3600&Version=2011-06-15&Policy={"Version":"2012-10-17","Statement":[{"Sid":"Stmt1","Effect":"Allow","Action":"s3:*","Resource":"arn:aws:s3:::*"}]}&AUTHPARAMS
```

## Sample Response

```
<?xml version="1.0" encoding="UTF-8"?>
<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleResult>
    <AssumedRoleUser>
      <Arn/>
      <AssumeRoleId/>
    </AssumedRoleUser>
    <Credentials>
      <AccessKeyId>Y4RJU1RNFGK48LGO9I2S</AccessKeyId>
      <SecretAccessKey>sYLRKS1Z7hSjluf6gEbb9066hnx315wHTiACPAjg</SecretAccessKey>
      <Expiration>2019-08-08T20:26:12Z</Expiration>
      <SessionToken>eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NLZXkiOiJZNFJKVTFSTkZHSzQ4TEdPOUkyUyIsImF1ZCI6IlBvRWdYUDZ1Vk80NUlzRU5SbmdEWGo1QXU1WWEiLCJhenAiOiJQb0VnWFA2dVZPNDVJc0VOUm5nRFhqNUF1NVlhIiwiZXhwIjoxNTQxODExMDcxLCJpYXQiOjE1NDE4MDc0NzEsImlzcyI6Imh0dHBzOi8vbG9jYWxob3N0Ojk0NDMvb2F1dGgyL3Rva2VuIiwianRpIjoiYTBiMjc2MjktZWUxYS00M2JmLTg3MzktZjMzNzRhNGNkYmMwIn0.ewHqKVFTaP-j_kgZrcOEKroNUjk10GEp8bqQjxBbYVovV0nHO985VnRESFbcT6XMDDKHZiWqN2vi_ETX_u3Q-w</SessionToken>
    </Credentials>
  </AssumeRoleResult>
  <ResponseMetadata>
    <RequestId>c6104cbe-af31-11e0-8154-cbc7ccf896c7</RequestId>
  </ResponseMetadata>
</AssumeRoleResponse>
```

## Using AssumeRole API

```
export MINIO_ROOT_USER=minio
export MINIO_ROOT_PASSWORD=minio123
minio server ~/test
```

Create new users following the multi-user guide [here](https://docs.min.io/community/minio-object-store/administration/identity-access-management.html)

### Testing an example with awscli tool

> Use the same username and password created in the previous steps.

```
[foobar]
region = us-east-1
aws_access_key_id = foobar
aws_secret_access_key = foo12345
```

> NOTE: In the following commands `--role-arn` and `--role-session-name` are not meaningful for MinIO and can be set to any value satisfying the command line requirements.

```
$ aws --profile foobar --endpoint-url http://localhost:9000 sts assume-role --policy '{"Version":"2012-10-17","Statement":[{"Sid":"Stmt1","Effect":"Allow","Action":"s3:*","Resource":"arn:aws:s3:::*"}]}' --role-arn arn:xxx:xxx:xxx:xxxx --role-session-name anything
{
    "AssumedRoleUser": {
        "Arn": ""
    },
    "Credentials": {
        "SecretAccessKey": "xbnWUoNKgFxi+uv3RI9UgqP3tULQMdI+Hj+4psd4",
        "SessionToken": "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NLZXkiOiJLOURUSU1VVlpYRVhKTDNBVFVPWSIsImV4cCI6MzYwMDAwMDAwMDAwMCwicG9saWN5IjoidGVzdCJ9.PetK5wWUcnCJkMYv6TEs7HqlA4x_vViykQ8b2T_6hapFGJTO34sfTwqBnHF6lAiWxRoZXco11B0R7y58WAsrQw",
        "Expiration": "2019-02-20T19:56:59-08:00",
        "AccessKeyId": "K9DTIMUVZXEXJL3ATUOY"
    }
}
```

### Testing an example with `assume-role.go`

The included program in this directory can also be used for testing:

``` shell
$ go run assume-role.go -u foobar -p foo12345 -d
Only displaying credentials:
AccessKeyID: 27YDRYEM0S9B44AJJX9X
SecretAccessKey: LHPdHeaLiYk+pDZ3hgN3sdwXpJC2qbhBfZ8ii9Z3
SessionToken: eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NLZXkiOiIyN1lEUllFTTBTOUI0NEFKSlg5WCIsImV4cCI6MzYwMDAwMDAwMDAwMCwicG9saWN5IjoiY29uc29sZUFkbWluIn0.2d9t0UOm1jQmwe31_5CyN63f6CL-fhqZSO-XhZIp-NH5QteWv9oSMjIrcNWzMgNDblrUfAZ0JSs8a1ciLQF9Ww

```

## Explore Further

- [MinIO Admin Complete Guide](https://docs.min.io/community/minio-object-store/reference/minio-mc-admin.html)
- [The MinIO documentation website](https://docs.min.io/community/minio-object-store/index.html)
