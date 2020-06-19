# AssumeRoleWithClientGrants [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

**Table of Contents**

- [Introduction](#introduction)
- [API Request Parameters](#api-request-parameters)
    - [Token](#token)
    - [Version](#version)
    - [DurationSeconds](#durationseconds)
    - [Policy](#policy)
    - [Response Elements](#response-elements)
    - [Errors](#errors)
- [Sample `POST` Request](#sample-post-request)
- [Sample Response](#sample-response)
- [Testing](#testing)

## Introduction

Returns a set of temporary security credentials for applications/clients who have been authenticated through client credential grants provided by identity provider. Example providers include KeyCloak, Okta etc.

Calling AssumeRoleWithClientGrants does not require the use of MinIO default credentials. Therefore, client application can be distributed that requests temporary security credentials without including MinIO default credentials. Instead, the identity of the caller is validated by using a JWT access token from the identity provider. The temporary security credentials returned by this API consists of an access key, a secret key, and a security token. Applications can use these temporary security credentials to sign calls to MinIO API operations.

By default, the temporary security credentials created by AssumeRoleWithClientGrants last for one hour. However, use the optional DurationSeconds parameter to specify the duration of the credentials. This value varies from 900 seconds (15 minutes) up to the maximum session duration to 12 hours.

## API Request Parameters
### Token
The OAuth 2.0 access token that is provided by the identity provider. Application must get this token by authenticating the application using client credential grants before the application makes an AssumeRoleWithClientGrants call.

| Params               | Value                                          |
| :--                  | :--                                            |
| *Type*               | *String*                                       |
| *Length Constraints* | *Minimum length of 4. Maximum length of 2048.* |
| *Required*           | *Yes*                                          |

### Version
Indicates STS API version information, the only supported value is '2011-06-15'.  This value is borrowed from AWS STS API documentation for compatibility reasons.

| Params     | Value    |
| :--        | :--      |
| *Type*     | *String* |
| *Required* | *Yes*    |

### DurationSeconds
The duration, in seconds. The value can range from 900 seconds (15 minutes) up to 12 hours. If value is higher than this setting, then operation fails. By default, the value is set to 3600 seconds.

| Params        | Value                                           |
| :--           | :--                                             |
| *Type*        | *Integer*                                       |
| *Valid Range* | *Minimum value of 900. Maximum value of 43200.* |
| *Required*    | *No*                                            |

### Policy
An IAM policy in JSON format that you want to use as an inline session policy. This parameter is optional. Passing policies to this operation returns new temporary credentials. The resulting session's permissions are the intersection of the canned policy name and the policy set here. You cannot use this policy to grant more permissions than those allowed by the canned policy name being assumed.

| Params        | Value                                          |
| :--           | :--                                            |
| *Type*        | *String*                                       |
| *Valid Range* | *Minimum length of 1. Maximum length of 2048.* |
| *Required*    | *No*                                           |

### Response Elements
XML response for this API is similar to [AWS STS AssumeRoleWithWebIdentity](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html#API_AssumeRoleWithWebIdentity_ResponseElements)

### Errors
XML error response for this API is similar to [AWS STS AssumeRoleWithWebIdentity](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html#API_AssumeRoleWithWebIdentity_Errors)

## Sample `POST` Request
```
http://minio.cluster:9000?Action=AssumeRoleWithClientGrants&DurationSeconds=3600&Token=eyJ4NXQiOiJOVEF4Wm1NeE5ETXlaRGczTVRVMVpHTTBNekV6T0RKaFpXSTRORE5sWkRVMU9HRmtOakZpTVEiLCJraWQiOiJOVEF4Wm1NeE5ETXlaRGczTVRVMVpHTTBNekV6T0RKaFpXSTRORE5sWkRVMU9HRmtOakZpTVEiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiJQb0VnWFA2dVZPNDVJc0VOUm5nRFhqNUF1NVlhIiwiYXpwIjoiUG9FZ1hQNnVWTzQ1SXNFTlJuZ0RYajVBdTVZYSIsImlzcyI6Imh0dHBzOlwvXC9sb2NhbGhvc3Q6OTQ0M1wvb2F1dGgyXC90b2tlbiIsImV4cCI6MTU0MTgwOTU4MiwiaWF0IjoxNTQxODA1OTgyLCJqdGkiOiI2Y2YyMGIwZS1lNGZmLTQzZmQtYTdiYS1kYTc3YTE3YzM2MzYifQ.Jm29jPliRvrK6Os34nSK3rhzIYLFjE__zdVGNng3uGKXGKzP3We_i6NPnhA0szJXMOKglXzUF1UgSz8MctbaxFS8XDusQPVe4LkB_45hwBm6TmBxzui911nt-1RbBLN_jZIlvl2lPrbTUH5hSn9kEkph6seWanTNQpz9tNEoVa6R_OX3kpJqxe8tLQUWw453A1JTwFNhdHa6-f1K8_Q_eEZ_4gOYINQ9t_fhTibdbkXZkJQFLop-Jwoybi9s4nwQU_dATocgcufq5eCeNItQeleT-23lGxIz0X7CiJrJynYLdd-ER0F77SumqEb5iCxhxuf4H7dovwd1kAmyKzLxpw&Version=2011-06-15
```

## Sample Response
```
<?xml version="1.0" encoding="UTF-8"?>
<AssumeRoleWithClientGrantsResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleWithClientGrantsResult>
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
  </AssumeRoleWithClientGrantsResult>
  <ResponseMetadata/>
</AssumeRoleWithClientGrantsResponse>
```

## Testing
```
export MINIO_ACCESS_KEY=minio
export MINIO_SECRET_KEY=minio123
export MINIO_IDENTITY_OPENID_CONFIG_URL=http://localhost:8080/auth/realms/demo/.well-known/openid-configuration
export MINIO_IDENTITY_OPENID_CLIENT_ID="843351d4-1080-11ea-aa20-271ecba3924a"
minio server /mnt/export
```

Testing with an example
> Obtaining client ID and secrets follow [Keycloak configuring documentation](https://github.com/minio/minio/blob/master/docs/sts/keycloak.md)

```
$ go run client-grants.go -cid PoEgXP6uVO45IsENRngDXj5Au5Ya -csec eKsw6z8CtOJVBtrOWvhRWL4TUCga

##### Credentials
{
	"accessKey": "NUIBORZYTV2HG2BMRSXR",
	"secretKey": "qQlP5O7CFPc5m5IXf1vYhuVTFj7BRVJqh0FqZ86S",
	"expiration": "2018-08-21T17:10:29-07:00",
	"sessionToken": "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NLZXkiOiJOVUlCT1JaWVRWMkhHMkJNUlNYUiIsImF1ZCI6IlBvRWdYUDZ1Vk80NUlzRU5SbmdEWGo1QXU1WWEiLCJhenAiOiJQb0VnWFA2dVZPNDVJc0VOUm5nRFhqNUF1NVlhIiwiZXhwIjoxNTM0ODk2NjI5LCJpYXQiOjE1MzQ4OTMwMjksImlzcyI6Imh0dHBzOi8vbG9jYWxob3N0Ojk0NDMvb2F1dGgyL3Rva2VuIiwianRpIjoiNjY2OTZjZTctN2U1Ny00ZjU5LWI0MWQtM2E1YTMzZGZiNjA4In0.eJONnVaSVHypiXKEARSMnSKgr-2mlC2Sr4fEGJitLcJF_at3LeNdTHv0_oHsv6ZZA3zueVGgFlVXMlREgr9LXA"
}
```
