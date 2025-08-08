# AssumeRoleWithWebIdentity [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

## Introduction

MinIO supports the standard AssumeRoleWithWebIdentity STS API to enable integration with OIDC/OpenID based identity provider environments. This allows the generation of temporary credentials with pre-defined access policies for applications/users to interact with MinIO object storage.

Calling AssumeRoleWithWebIdentity does not require the use of MinIO root or IAM credentials. Therefore, you can distribute an application (for example, on mobile devices) that requests temporary security credentials without including MinIO long lasting credentials in the application. Instead, the identity of the caller is validated by using a JWT id_token from the web identity provider. The temporary security credentials returned by this API consists of an access key, a secret key, and a security token. Applications can use these temporary security credentials to sign calls to MinIO API operations.

By default, the temporary security credentials created by AssumeRoleWithWebIdentity last for one hour. However, the optional DurationSeconds parameter can be used to specify the validity duration of the generated credentials. This value varies from 900 seconds (15 minutes) up to the maximum session duration of 365 days.

## Configuring OpenID Identity Provider on MinIO

Configuration can be performed via MinIO's standard configuration API (i.e. using `mc admin config set/get` commands) or equivalently via environment variables. For brevity we show only environment variables here:

```
$ mc admin config set myminio identity_openid --env
KEY:
identity_openid[:name]  enable OpenID SSO support

ARGS:
MINIO_IDENTITY_OPENID_ENABLE*               (on|off)    enable identity_openid target, default is 'off'
MINIO_IDENTITY_OPENID_DISPLAY_NAME          (string)    Friendly display name for this Provider/App
MINIO_IDENTITY_OPENID_CONFIG_URL*           (url)       openid discovery document e.g. "https://accounts.google.com/.well-known/openid-configuration"
MINIO_IDENTITY_OPENID_CLIENT_ID*            (string)    unique public identifier for apps e.g. "292085223830.apps.googleusercontent.com"
MINIO_IDENTITY_OPENID_CLIENT_SECRET*        (string)    secret for the unique public identifier for apps
MINIO_IDENTITY_OPENID_ROLE_POLICY           (string)    Set the IAM access policies applicable to this client application and IDP e.g. "app-bucket-write,app-bucket-list"
MINIO_IDENTITY_OPENID_CLAIM_NAME            (string)    JWT canned policy claim name (default: 'policy')
MINIO_IDENTITY_OPENID_SCOPES                (csv)       Comma separated list of OpenID scopes for server, defaults to advertised scopes from discovery document e.g. "email,admin"
MINIO_IDENTITY_OPENID_VENDOR                (string)    Specify vendor type for vendor specific behavior to checking validity of temporary credentials and service accounts on MinIO
MINIO_IDENTITY_OPENID_CLAIM_USERINFO        (on|off)    Enable fetching claims from UserInfo Endpoint for authenticated user
MINIO_IDENTITY_OPENID_KEYCLOAK_REALM        (string)    Specify Keycloak 'realm' name, only honored if vendor was set to 'keycloak' as value, if no realm is specified 'master' is default
MINIO_IDENTITY_OPENID_KEYCLOAK_ADMIN_URL    (string)    Specify Keycloak 'admin' REST API endpoint e.g. http://localhost:8080/auth/admin/
MINIO_IDENTITY_OPENID_REDIRECT_URI_DYNAMIC  (on|off)    Enable 'Host' header based dynamic redirect URI (default: 'off')
MINIO_IDENTITY_OPENID_COMMENT               (sentence)  optionally add a comment to this setting
```

### Access Control Configuration Variables

Either `MINIO_IDENTITY_OPENID_ROLE_POLICY` (recommended) or `MINIO_IDENTITY_OPENID_CLAIM_NAME` must be specified but not both. See the section Access Control Policies to understand the differences between the two.

**NOTE**: When configuring multiple OpenID based authentication providers on a MinIO cluster, any number of Role Policy based providers may be configured, and at most one JWT Claim based provider may be configured.

<details><summary>Example 1: Two role policy providers</summary>

Sample environment variables:

```
MINIO_IDENTITY_OPENID_DISPLAY_NAME="my first openid"
MINIO_IDENTITY_OPENID_CONFIG_URL=http://myopenid.com/.well-known/openid-configuration
MINIO_IDENTITY_OPENID_CLIENT_ID="minio-client-app"
MINIO_IDENTITY_OPENID_CLIENT_SECRET="minio-client-app-secret"
MINIO_IDENTITY_OPENID_SCOPES="openid,groups"
MINIO_IDENTITY_OPENID_REDIRECT_URI="http://127.0.0.1:10000/oauth_callback"
MINIO_IDENTITY_OPENID_ROLE_POLICY="consoleAdmin"

MINIO_IDENTITY_OPENID_DISPLAY_NAME_APP2="another oidc"
MINIO_IDENTITY_OPENID_CONFIG_URL_APP2="http://anotheroidc.com/.well-known/openid-configuration"
MINIO_IDENTITY_OPENID_CLIENT_ID_APP2="minio-client-app-2"
MINIO_IDENTITY_OPENID_CLIENT_SECRET_APP2="minio-client-app-secret-2"
MINIO_IDENTITY_OPENID_SCOPES_APP2="openid,groups"
MINIO_IDENTITY_OPENID_REDIRECT_URI_APP2="http://127.0.0.1:10000/oauth_callback"
MINIO_IDENTITY_OPENID_ROLE_POLICY_APP2="readwrite"

```
</details>

<details><summary>Example 2: Single claim based provider</summary>

Sample environment variables:

```
MINIO_IDENTITY_OPENID_DISPLAY_NAME="my openid"
MINIO_IDENTITY_OPENID_CONFIG_URL=http://myopenid.com/.well-known/openid-configuration
MINIO_IDENTITY_OPENID_CLIENT_ID="minio-client-app"
MINIO_IDENTITY_OPENID_CLIENT_SECRET="minio-client-app-secret"
MINIO_IDENTITY_OPENID_SCOPES="openid,groups"
MINIO_IDENTITY_OPENID_REDIRECT_URI="http://127.0.0.1:10000/oauth_callback"
MINIO_IDENTITY_OPENID_CLAIM_NAME="groups"
```
</details>

### Redirection from OpenID Provider

To login to MinIO, the user first loads the MinIO console on their browser, and selects the OpenID Provider they wish to use (the `MINIO_IDENTITY_OPENID_DISPLAY_NAME` value is shown here). The user is then redirected to the OpenID provider's login page and performs necessary login actions (e.g. entering credentials, responding to MFA authentication challenges, etc). After successful login, the user is redirected back to the MinIO console. This redirect URL is specified as a parameter by MinIO when the user is redirected to the OpenID Provider in the beginning. For some setups, extra configuration may be required for this step to work correctly.

For a simple setup where the user/client app accesses MinIO directly (i.e. with no intervening proxies/load-balancers), and each MinIO server (if there are more than one) has a unique domain name, this redirection should work automatically with no further configuration. For example, if the MinIO service is being accessed by the browser at the URL `https://minio-node-1.example.org`, the redirect URL will be `https://minio-node-1.example.org/oauth_callback` and all is well.

For deployments with a load-balancer (LB), it is required that the LB is configured to send requests from the same user/client-app to the same backend MinIO server (at least for the initial login request and subsequent redirection, as the OpenID auth flow's state parameter is currently local to the MinIO server). For this setup, set the `MINIO_BROWSER_REDIRECT_URL` parameter to the publicly/client-accessible endpoint for the MinIO Console. For example `MINIO_BROWSER_REDIRECT_URL=https://console.minio.example.org`. This will ensure that the redirect URL is set to `https://console.minio.example.org/oauth_callback` and the login process should work correctly.

For deployments employing DNS round-robin on a single domain to all the MinIO servers, it is possible that after redirection the browser may land on a different MinIO server. For example, the domain `console.minio.example.org` may resolve to `console-X.minio.example.org`, where `X` is `1`, `2`, `3` or `4`. For the login to work, if the user first landed on `console-1.minio.example.org`, they must be redirected back to the same place after logging in at the OpenID provider's web-page. To ensure this, set the `MINIO_IDENTITY_OPENID_REDIRECT_URI_DYNAMIC=on` parameter - this lets MinIO set the redirect URL based on the "Host" header of the (initial login) request.

The **deprecated** parameter `MINIO_IDENTITY_OPENID_REDIRECT_URI` works similar to the `MINIO_BROWSER_REDIRECT_URL` but needs to include the `/oauth_callback` suffix. Please do not use it, as it is sufficient to the set the `MINIO_BROWSER_REDIRECT_URL` parameter (which is required anyway for most load-balancer based setups to work correctly). This deprecated parameter **will be removed** in a future release. 

## Specifying Access Control with IAM Policies

The STS API authenticates the user by verifying the JWT provided in the request. However access to object storage resources are controlled via named IAM policies defined in the MinIO instance. Once authenticated via the STS API, the MinIO server applies one or more IAM policies to the generated credentials. MinIO's AssumeRoleWithWebIdentity implementation supports specifying IAM policies in two ways:

1. Role Policy (Recommended): When specified as part of the OpenID provider configuration, all users authenticating via this provider are authorized to (only) use the specified role policy. The policy to associate with such users is specified via the `role_policy` configuration parameter or the `MINIO_IDENTITY_OPENID_ROLE_POLICY` environment variable. The value is a comma-separated list of IAM access policy names already defined in the server. In this situation, the server prints a role ARN at startup that must be specified as a `RoleArn` API request parameter in the STS AssumeRoleWithWebIdentity API call. When using Role Policies, multiple OpenID providers and/or client applications (with unique client IDs) may be configured with independent role policies. Each configuration is assigned a unique RoleARN by the MinIO server and this is used to select the policies to apply to temporary credentials generated in the AssumeRoleWithWebIdentity call.

2. `id_token` claims: When the role policy is not configured, MinIO looks for a specific claim in the `id_token` (JWT) returned by the OpenID provider in the STS request. The default claim is `policy` and can be overridden by the `claim_name` configuration parameter or the `MINIO_IDENTITY_OPENID_CLAIM_NAME` environment variable. The claim value can be a string (comma-separated list) or an array of IAM access policy names defined in the server. A `RoleArn` API request parameter *must not* be specified in the STS AssumeRoleWithWebIdentity API call.

## API Request Parameters

### WebIdentityToken

The OAuth 2.0 id_token that is provided by the web identity provider. Application must get this token by authenticating the user who is using your application with a web identity provider before the application makes an AssumeRoleWithWebIdentity call.

| Params               | Value                                          |
| :--                  | :--                                            |
| *Type*               | *String*                                       |
| *Length Constraints* | *Minimum length of 4. Maximum length of 2048.* |
| *Required*           | *Yes*                                          |

### WebIdentityAccessToken (MinIO Extension)

There are situations when identity provider does not provide user claims in `id_token` instead it needs to be retrieved from UserInfo endpoint, this extension is only useful in this scenario. This is rare so use it accordingly depending on your Identity provider implementation. `access_token` is available as part of the OIDC authentication flow similar to `id_token`.

| Params     | Value    |
| :--        | :--      |
| *Type*     | *String* |
| *Required* | *No*     |

### RoleArn

The role ARN to use. This must be specified if and only if the web identity provider is configured with a role policy.

| Params     | Value    |
| :--        | :--      |
| *Type*     | *String* |
| *Required* | *No*     |

### Version

Indicates STS API version information, the only supported value is '2011-06-15'. This value is borrowed from AWS STS API documentation for compatibility reasons.

| Params     | Value    |
| :--        | :--      |
| *Type*     | *String* |
| *Required* | *Yes*    |

### DurationSeconds

The duration, in seconds. The value can range from 900 seconds (15 minutes) up to 365 days. If value is higher than this setting, then operation fails. By default, the value is set to 3600 seconds. If no *DurationSeconds* is specified expiry seconds is obtained from *WebIdentityToken*.

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

## Using WebIdentity API

```
export MINIO_ROOT_USER=minio
export MINIO_ROOT_PASSWORD=minio123
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

> NOTE: for a reasonable test outcome, make sure the assumed user has at least permission/policy to list all buckets. That policy would look like below:

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

- Visit <http://localhost:8080>, login will direct the user to the Google OAuth2 Auth URL to obtain a permission grant.
- The redirection URI (callback handler) receives the OAuth2 callback, verifies the state parameter, and obtains a Token.
- Using the id_token the callback handler further talks to Google OAuth2 Token URL to obtain an JWT id_token.
- Once obtained the JWT id_token is further sent to STS endpoint i.e MinIO to retrieve temporary credentials.
- Temporary credentials are displayed on the browser upon successful retrieval.

## Using MinIO Console

To support WebIdentity based login for MinIO Console, set openid configuration and restart MinIO

```
mc admin config set myminio identity_openid config_url="<CONFIG_URL>" client_id="<client_identifier>"
```

```
mc admin service restart myminio
```

Sample URLs for Keycloak are

`config_url` - `http://localhost:8080/auth/realms/demo/.well-known/openid-configuration`

JWT token returned by the Identity Provider should include a custom claim for the policy, this is required to create a STS user in MinIO. The name of the custom claim could be either `policy` or `<NAMESPACE_PREFIX>policy`.  If there is no namespace then `claim_prefix` can be ignored. For example if the custom claim name is `https://min.io/policy` then, `claim_prefix` should be set as `https://min.io/`.

- Open MinIO Console and click `Login with SSO`
- The user will be redirected to the Identity Provider login page
- Upon successful login on Identity Provider page the user will be automatically logged into MinIO Console.

## Explore Further

- [MinIO Admin Complete Guide](https://docs.min.io/community/minio-object-store/reference/minio-mc-admin.html)
- [The MinIO documentation website](https://docs.min.io/community/minio-object-store/index.html)
