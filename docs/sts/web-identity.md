# AssumeRoleWithWebIdentity [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

**Table of Contents**

- [Introduction](#introduction)
- [API Request Parameters](#api-request-parameters)
    - [WebIdentityToken](#webidentitytoken)
    - [Version](#version)
    - [DurationSeconds](#durationseconds)
    - [Policy](#policy)
    - [Response Elements](#response-elements)
    - [Errors](#errors)
- [Sample `POST` Request](#sample-post-request)
- [Sample Response](#sample-response)
- [Testing](#testing)
- [Authorization Flow](#authorization-flow)
- [MinIO Browser](#minio-browser)

## Introduction

Calling AssumeRoleWithWebIdentity does not require the use of MinIO default credentials. Therefore, you can distribute an application (for example, on mobile devices) that requests temporary security credentials without including MinIO default credentials in the application. Instead, the identity of the caller is validated by using a JWT access token from the web identity provider. The temporary security credentials returned by this API consists of an access key, a secret key, and a security token. Applications can use these temporary security credentials to sign calls to MinIO API operations.

By default, the temporary security credentials created by AssumeRoleWithWebIdentity last for one hour. However, use the optional DurationSeconds parameter to specify the duration of the credentials. This value varies from 900 seconds (15 minutes) up to the maximum session duration to 12 hours.

## API Request Parameters
### WebIdentityToken
The OAuth 2.0 access token that is provided by the web identity provider. Application must get this token by authenticating the user who is using your application with a web identity provider before the application makes an AssumeRoleWithWebIdentity call.

| Params               | Value                                          |
| :--                  | :--                                            |
| *Type*               | *String*                                       |
| *Length Constraints* | *Minimum length of 4. Maximum length of 2048.* |
| *Required*           | *Yes*                                          |

### Version
Indicates STS API version information, the only supported value is '2011-06-15'. This value is borrowed from AWS STS API documentation for compatibility reasons.

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
http://minio.cluster:9000?Action=AssumeRoleWithWebIdentity&DurationSeconds=3600&WebIdentityToken=eyJ4NXQiOiJOVEF4Wm1NeE5ETXlaRGczTVRVMVpHTTBNekV6T0RKaFpXSTRORE5sWkRVMU9HRmtOakZpTVEiLCJraWQiOiJOVEF4Wm1NeE5ETXlaRGczTVRVMVpHTTBNekV6T0RKaFpXSTRORE5sWkRVMU9HRmtOakZpTVEiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiJQb0VnWFA2dVZPNDVJc0VOUm5nRFhqNUF1NVlhIiwiYXpwIjoiUG9FZ1hQNnVWTzQ1SXNFTlJuZ0RYajVBdTVZYSIsImlzcyI6Imh0dHBzOlwvXC9sb2NhbGhvc3Q6OTQ0M1wvb2F1dGgyXC90b2tlbiIsImV4cCI6MTU0MTgwOTU4MiwiaWF0IjoxNTQxODA1OTgyLCJqdGkiOiI2Y2YyMGIwZS1lNGZmLTQzZmQtYTdiYS1kYTc3YTE3YzM2MzYifQ.Jm29jPliRvrK6Os34nSK3rhzIYLFjE__zdVGNng3uGKXGKzP3We_i6NPnhA0szJXMOKglXzUF1UgSz8MctbaxFS8XDusQPVe4LkB_45hwBm6TmBxzui911nt-1RbBLN_jZIlvl2lPrbTUH5hSn9kEkph6seWanTNQpz9tNEoVa6R_OX3kpJqxe8tLQUWw453A1JTwFNhdHa6-f1K8_Q_eEZ_4gOYINQ9t_fhTibdbkXZkJQFLop-Jwoybi9s4nwQU_dATocgcufq5eCeNItQeleT-23lGxIz0X7CiJrJynYLdd-ER0F77SumqEb5iCxhxuf4H7dovwd1kAmyKzLxpw&Version=2011-06-15
```

## Sample Response
```
<?xml version="1.0" encoding="UTF-8"?>
<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleWithWebIdentityResult>
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
  </AssumeRoleWithWebIdentityResult>
  <ResponseMetadata/>
</AssumeRoleWithWebIdentityResponse>
```

## Testing
```
export MINIO_ACCESS_KEY=minio
export MINIO_SECRET_KEY=minio123
export MINIO_IDENTITY_OPENID_CONFIG_URL=https://accounts.google.com/.well-known/openid-configuration
export MINIO_IDENTITY_OPENID_CLIENT_ID="843351d4-1080-11ea-aa20-271ecba3924a"
# Optional: Allow to specify the requested OpenID scopes (OpenID only requires the `openid` scope)
#export MINIO_IDENTITY_OPENID_SCOPES="openid,profile,email"
minio server /mnt/export
```

or using `mc`
```
mc admin config get myminio identity_openid
identity_openid config_url=https://accounts.google.com/.well-known/openid-configuration client_id=843351d4-1080-11ea-aa20-271ecba3924a
```

Testing with an example
> Visit [Google Developer Console](https://console.cloud.google.com) under Project, APIs, Credentials to get your OAuth2 client credentials. Add `http://localhost:8080/oauth2/callback` as a valid OAuth2 Redirect URL.

```
$ go run web-identity.go -cid 204367807228-ok7601k6gj1pgge7m09h7d79co8p35xx.apps.googleusercontent.com -csec XsT_PgPdT1nO9DD45rMLJw7G
2018/12/26 17:49:36 listening on http://localhost:8080/
```

Note: For a reasonable test outcome, make sure the assumed user has at least permission/policy to list all buckets. That policy would look like below:
```
{
  "version": "2012-10-17",
  "statement": [
    {
      "effect": "Allow",
      "action": [
        "s3:ListAllMyBuckets"
      ],
      "resource": [
        "arn:aws:s3:::*"
      ]
    }
  ]
}
```

## Authorization Flow

- Visit http://localhost:8080, login will direct the user to the Google OAuth2 Auth URL to obtain a permission grant.
- The redirection URI (callback handler) receives the OAuth2 callback, verifies the state parameter, and obtains a Token.
- Using the access token the callback handler further talks to Google OAuth2 Token URL to obtain an JWT id_token.
- Once obtained the JWT id_token is further sent to STS endpoint i.e MinIO to retrive temporary credentials.
- Temporary credentials are displayed on the browser upon successful retrieval.


## MinIO Browser
To support WebIdentity login on MinIO Browser

- Set openid configuration and restart MinIO

```
mc admin config set myminio identity_openid config_url="<CONFIG_URL>" client_id="<client_identifier>"
```

```
mc admin service restart myminio
```

Sample URLs for Keycloak are

`config_url` - `http://localhost:8080/auth/realms/demo/.well-known/openid-configuration`

JWT token returned by the Identity Provider should include a custom claim for the policy, this is required to create a STS user in MinIO. The name of the custom claim could be either `policy` or `<NAMESPACE_PREFIX>policy`.  If there is no namespace then `claim_prefix` can be ingored. For example if the custom claim name is `https://min.io/policy` then, `claim_prefix` should be set as `https://min.io/`.

- Open MinIO Browser and click `Log in with OpenID`
- Enter the `Client ID` obtained from Identity Provider and press ENTER, if not you can set a `client_id` on server to avoid this step.
- The user will be redirected to the Identity Provider login page
- Upon successful login on Identity Provider page the user will be automatically logged into MinIO Browser
