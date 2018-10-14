## AssumeRoleWithClientGrants [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)
Returns a set of temporary security credentials for applications/clients who have been authenticated through client grants provided by identity provider. Example providers include WSO2, KeyCloak etc.

Calling AssumeRoleWithClientGrants does not require the use of Minio default credentials. Therefore, client application can be distributed that requests temporary security credentials without including Minio default credentials. Instead, the identity of the caller is validated by using a JWT access token from the identity provider. The temporary security credentials returned by this API consist of an access key, a secret key, and a security token. Applications can use these temporary security credentials to sign calls to Minio API operations.

By default, the temporary security credentials created by AssumeRoleWithClientGrants last for one hour. However, use the optional DurationSeconds parameter to specify the duration of the credentials. This value varies from 900 seconds (15 minutes) up to the maximum session duration to 12 hours.

### Request Parameters
#### DurationSeconds
The duration, in seconds. The value can range from 900 seconds (15 minutes) up to the 12 hours. If value is higher than this setting, then operation fails. By default, the value is set to 3600 seconds.

| Params | Value |
| :-- | :-- |
| *Type* | *Integer* |
| *Valid Range* | *Minimum value of 900. Maximum value of 43200.* |
| *Required* | *No* |

#### Token
The OAuth 2.0 access token that is provided by the identity provider. Application must get this token by authenticating the application using client grants before the application makes an AssumeRoleWithClientGrants call.

| Params | Value |
| :-- | :-- |
| *Type* | *String* |
| *Length Constraints* | *Minimum length of 4. Maximum length of 2048.* |
| *Required* | *Yes* |

#### Response Elements
XML response for this API is similar to [AWS STS AssumeRoleWithWebIdentity](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html#API_AssumeRoleWithWebIdentity_ResponseElements)

#### Errors
XML error response for this API is similar to [AWS STS AssumeRoleWithWebIdentity](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html#API_AssumeRoleWithWebIdentity_Errors)

#### Testing
```
$ export MINIO_ACCESS_KEY=minio
$ export MINIO_SECRET_KEY=minio123
$ export MINIO_IAM_JWKS_URL=https://localhost:9443/oauth2/jwks
$ export MINIO_IAM_OPA_URL=http://localhost:8181/v1/data/httpapi/authz
$ minio server /mnt/export

$ mc admin config get myminio
...
{
  "openid": {
    "jwks": {
      "url": "https://localhost:9443/oauth2/jwks"
     }
   }
  "policy": {
    "opa": {
      "url": "http://localhost:8181/v1/data/httpapi/authz",
      "authToken": ""
    }
  }
}
```

Testing with an example
> Obtaining client ID and secrets follow [WSO2 configuring documentation](./wso2.md)

```
go run full-example.go -cid PoEgXP6uVO45IsENRngDXj5Au5Ya -csec eKsw6z8CtOJVBtrOWvhRWL4TUCga

##### Credentials
{
	"accessKey": "NUIBORZYTV2HG2BMRSXR",
	"secretKey": "qQlP5O7CFPc5m5IXf1vYhuVTFj7BRVJqh0FqZ86S",
	"expiration": "2018-08-21T17:10:29-07:00",
	"sessionToken": "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NLZXkiOiJOVUlCT1JaWVRWMkhHMkJNUlNYUiIsImF1ZCI6IlBvRWdYUDZ1Vk80NUlzRU5SbmdEWGo1QXU1WWEiLCJhenAiOiJQb0VnWFA2dVZPNDVJc0VOUm5nRFhqNUF1NVlhIiwiZXhwIjoxNTM0ODk2NjI5LCJpYXQiOjE1MzQ4OTMwMjksImlzcyI6Imh0dHBzOi8vbG9jYWxob3N0Ojk0NDMvb2F1dGgyL3Rva2VuIiwianRpIjoiNjY2OTZjZTctN2U1Ny00ZjU5LWI0MWQtM2E1YTMzZGZiNjA4In0.eJONnVaSVHypiXKEARSMnSKgr-2mlC2Sr4fEGJitLcJF_at3LeNdTHv0_oHsv6ZZA3zueVGgFlVXMlREgr9LXA"
}
```
