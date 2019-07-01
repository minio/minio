## AssumeRoleWithWebIdentity [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)
Calling AssumeRoleWithWebIdentity does not require the use of MinIO default credentials. Therefore, you can distribute an application (for example, on mobile devices) that requests temporary security credentials without including MinIO default credentials in the application. Instead, the identity of the caller is validated by using a JWT access token from the web identity provider. The temporary security credentials returned by this API consists of an access key, a secret key, and a security token. Applications can use these temporary security credentials to sign calls to MinIO API operations.

By default, the temporary security credentials created by AssumeRoleWithWebIdentity last for one hour. However, use the optional DurationSeconds parameter to specify the duration of the credentials. This value varies from 900 seconds (15 minutes) up to the maximum session duration to 12 hours.

### Request Parameters
#### DurationSeconds
The duration, in seconds. The value can range from 900 seconds (15 minutes) up to 12 hours. If value is higher than this setting, then operation fails. By default, the value is set to 3600 seconds.

| Params | Value |
| :-- | :-- |
| *Type* | *Integer* |
| *Valid Range* | *Minimum value of 900. Maximum value of 43200.* |
| *Required* | *No* |

#### WebIdentityToken
The OAuth 2.0 access token that is provided by the web identity provider. Application must get this token by authenticating the user who is using your application with a web identity provider before the application makes an AssumeRoleWithWebIdentity call.

| Params | Value |
| :-- | :-- |
| *Type* | *String* |
| *Length Constraints* | *Minimum length of 4. Maximum length of 2048.* |
| *Required* | *Yes* |

#### Version
Indicates STS API version information, the only supported value is '2011-06-15'. This value is borrowed from AWS STS API documentation for compatibility reasons.

| Params | Value |
| :-- | :-- |
| *Type* | *String* |
| *Required* | *Yes* |

#### Response Elements
XML response for this API is similar to [AWS STS AssumeRoleWithWebIdentity](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html#API_AssumeRoleWithWebIdentity_ResponseElements)

#### Errors
XML error response for this API is similar to [AWS STS AssumeRoleWithWebIdentity](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html#API_AssumeRoleWithWebIdentity_Errors)

#### Sample Request
```
http://minio.cluster:9000?Action=AssumeRoleWithWebIdentity&DurationSeconds=3600&WebIdentityToken=eyJ4NXQiOiJOVEF4Wm1NeE5ETXlaRGczTVRVMVpHTTBNekV6T0RKaFpXSTRORE5sWkRVMU9HRmtOakZpTVEiLCJraWQiOiJOVEF4Wm1NeE5ETXlaRGczTVRVMVpHTTBNekV6T0RKaFpXSTRORE5sWkRVMU9HRmtOakZpTVEiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiJQb0VnWFA2dVZPNDVJc0VOUm5nRFhqNUF1NVlhIiwiYXpwIjoiUG9FZ1hQNnVWTzQ1SXNFTlJuZ0RYajVBdTVZYSIsImlzcyI6Imh0dHBzOlwvXC9sb2NhbGhvc3Q6OTQ0M1wvb2F1dGgyXC90b2tlbiIsImV4cCI6MTU0MTgwOTU4MiwiaWF0IjoxNTQxODA1OTgyLCJqdGkiOiI2Y2YyMGIwZS1lNGZmLTQzZmQtYTdiYS1kYTc3YTE3YzM2MzYifQ.Jm29jPliRvrK6Os34nSK3rhzIYLFjE__zdVGNng3uGKXGKzP3We_i6NPnhA0szJXMOKglXzUF1UgSz8MctbaxFS8XDusQPVe4LkB_45hwBm6TmBxzui911nt-1RbBLN_jZIlvl2lPrbTUH5hSn9kEkph6seWanTNQpz9tNEoVa6R_OX3kpJqxe8tLQUWw453A1JTwFNhdHa6-f1K8_Q_eEZ_4gOYINQ9t_fhTibdbkXZkJQFLop-Jwoybi9s4nwQU_dATocgcufq5eCeNItQeleT-23lGxIz0X7CiJrJynYLdd-ER0F77SumqEb5iCxhxuf4H7dovwd1kAmyKzLxpw&Version=2011-06-15
```

#### Sample Response
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
      <Expiration>2018-11-09T16:51:11-08:00</Expiration>
      <SessionToken>eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NLZXkiOiJZNFJKVTFSTkZHSzQ4TEdPOUkyUyIsImF1ZCI6IlBvRWdYUDZ1Vk80NUlzRU5SbmdEWGo1QXU1WWEiLCJhenAiOiJQb0VnWFA2dVZPNDVJc0VOUm5nRFhqNUF1NVlhIiwiZXhwIjoxNTQxODExMDcxLCJpYXQiOjE1NDE4MDc0NzEsImlzcyI6Imh0dHBzOi8vbG9jYWxob3N0Ojk0NDMvb2F1dGgyL3Rva2VuIiwianRpIjoiYTBiMjc2MjktZWUxYS00M2JmLTg3MzktZjMzNzRhNGNkYmMwIn0.ewHqKVFTaP-j_kgZrcOEKroNUjk10GEp8bqQjxBbYVovV0nHO985VnRESFbcT6XMDDKHZiWqN2vi_ETX_u3Q-w</SessionToken>
    </Credentials>
  </AssumeRoleWithWebIdentityResult>
  <ResponseMetadata/>
</AssumeRoleWithWebIdentityResponse>
```

#### Testing
```
$ export MINIO_ACCESS_KEY=minio
$ export MINIO_SECRET_KEY=minio123
$ export MINIO_IAM_JWKS_URL=https://www.googleapis.com/oauth2/v3/certs
$ minio server /mnt/export

$ mc admin config get myminio
...
{
  "openid": {
    "jwks": {
      "url": "https://www.googleapis.com/oauth2/v3/certs"
     }
   }
}
```

Testing with an example
> Visit [Google Developer Console](https://console.cloud.google.com) under Project, APIs, Credentials to get your OAuth2 client credentials. Add `http://localhost:8080/oauth2/callback` as a valid OAuth2 Redirect URL.

```
$ go run web-identity.go -cid 204367807228-ok7601k6gj1pgge7m09h7d79co8p35xx.apps.googleusercontent.com -csec XsT_PgPdT1nO9DD45rMLJw7G
2018/12/26 17:49:36 listening on http://localhost:8080/
```

### Authorization Flow

- Visit http://localhost:8080, login will direct the user to the Google OAuth2 Auth URL to obtain a permission grant.
- The redirection URI (callback handler) receives the OAuth2 callback, verifies the state parameter, and obtains a Token.
- Using the access token the callback handler further talks to Google OAuth2 Token URL to obtain an JWT id_token.
- Once obtained the JWT id_token is further sent to STS endpoint i.e MinIO to retrive temporary credentials.
- Temporary credentials are displayed on the browser upon successful retrieval.
