# Casdoor Quickstart Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

Casdoor is a UI-first centralized authentication / Single-Sign-On (SSO) platform supporting OAuth 2.0, OIDC and SAML, integrated with Casbin RBAC and ABAC permission management. This document covers configuring Casdoor identity provider support with MinIO.

## Prerequisites

Configure and install casdoor server by following [Casdoor Server Installation](https://casdoor.org/docs/basic/server-installation).
For a quick installation, docker-compose reference configs are also available on the [Casdoor Try with Docker](https://casdoor.org/docs/basic/try-with-docker).

### Configure Casdoor

- Go to Applications
  - Create or use an existing Casdoor application
  - Edit the application
    - Copy `Client ID` and `Client secret`
    - Add your redirect url (callback url) to `Redirect URLs`
    - Save

- Go to Users
  - Edit the user
    - Add your MinIO policy (ex: `readwrite`) in `Tag`
    - Save

- Open your favorite browser and visit: **http://`CASDOOR_ENDPOINT`/.well-known/openid-configuration**, you will see the OIDC configure of Casdoor.

### Configure MinIO

```
export MINIO_ROOT_USER=minio
export MINIO_ROOT_PASSWORD=minio123
minio server /mnt/export
```

Here are all the available options to configure OpenID connect

```
mc admin config set myminio/ identity_openid

KEY:
identity_openid  enable OpenID SSO support

ARGS:
config_url*   (url)       openid discovery document e.g. "https://accounts.google.com/.well-known/openid-configuration"
client_id     (string)    unique public identifier for apps e.g. "292085223830.apps.googleusercontent.com"
claim_name    (string)    JWT canned policy claim name, defaults to "policy"
claim_prefix  (string)    JWT claim namespace prefix e.g. "customer1/"
scopes        (csv)       Comma separated list of OpenID scopes for server, defaults to advertised scopes from discovery document e.g. "email,admin"
comment       (sentence)  optionally add a comment to this setting
```

and ENV based options

```
mc admin config set myminio/ identity_openid --env

KEY:
identity_openid  enable OpenID SSO support

ARGS:
MINIO_IDENTITY_OPENID_CONFIG_URL*   (url)       openid discovery document e.g. "https://accounts.google.com/.well-known/openid-configuration"
MINIO_IDENTITY_OPENID_CLIENT_ID     (string)    unique public identifier for apps e.g. "292085223830.apps.googleusercontent.com"
MINIO_IDENTITY_OPENID_CLAIM_NAME    (string)    JWT canned policy claim name, defaults to "policy"
MINIO_IDENTITY_OPENID_CLAIM_PREFIX  (string)    JWT claim namespace prefix e.g. "customer1/"
MINIO_IDENTITY_OPENID_SCOPES        (csv)       Comma separated list of OpenID scopes for server, defaults to advertised scopes from discovery document e.g. "email,admin"
MINIO_IDENTITY_OPENID_COMMENT       (sentence)  optionally add a comment to this setting
```

Set `identity_openid` config with `config_url`, `client_id` and restart MinIO

```
~ mc admin config set myminio identity_openid config_url="http://CASDOOR_ENDPOINT/.well-known/openid-configuration" client_id=<client id> client_secret=<client secret> claim_name="tag"
```

> NOTE: As MinIO needs to use a claim attribute in JWT for its policy, you should configure it in casdoor as well. Currently, casdoor uses `tag` as a workaround for configuring MinIO's policy.

Once successfully set restart the MinIO instance.

```
mc admin service restart myminio
```

### Using WebIdentiy API

On another terminal run `web-identity.go` a sample client application which obtains JWT id_tokens from an identity provider, in our case its Keycloak. Uses the returned id_token response to get new temporary credentials from the MinIO server using the STS API call `AssumeRoleWithWebIdentity`.

```
$ go run docs/sts/web-identity.go -cid account -csec 072e7f00-4289-469c-9ab2-bbe843c7f5a8  -config-ep "http://CASDOOR_ENDPOINT/.well-known/openid-configuration" -port 8888
2018/12/26 17:49:36 listening on http://localhost:8888/
```

This will open the login page of Casdoor, upon successful login, STS credentials along with any buckets discovered using the credentials will be printed on the screen, for example:

```
{
  buckets: [ ],
  credentials: {
    AccessKeyID: "EJOLVY3K3G4BF37YD1A0",
    SecretAccessKey: "1b+w8LlDqMQOquKxIlZ2ggP+bgE51iwNG7SUVPJJ",
    SessionToken: "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NLZXkiOiJFSk9MVlkzSzNHNEJGMzdZRDFBMCIsImFkZHJlc3MiOltdLCJhZmZpbGlhdGlvbiI6IiIsImFwcGxlIjoiIiwiYXVkIjpbIjI0YTI1ZWEwNzE0ZDkyZTc4NTk1Il0sImF2YXRhciI6Imh0dHBzOi8vY2FzYmluLm9yZy9pbWcvY2FzYmluLnN2ZyIsImF6dXJlYWQiOiIiLCJiaW8iOiIiLCJiaXJ0aGRheSI6IiIsImNyZWF0ZWRJcCI6IiIsImNyZWF0ZWRUaW1lIjoiMjAyMS0xMi0wNlQyMzo1ODo0MyswODowMCIsImRpbmd0YWxrIjoiIiwiZGlzcGxheU5hbWUiOiJjYmMiLCJlZHVjYXRpb24iOiIiLCJlbWFpbCI6IjE5OTkwNjI2LmxvdmVAMTYzLmNvbSIsImV4cCI6MTY0MzIwMjIyMCwiZmFjZWJvb2siOiIiLCJnZW5kZXIiOiIiLCJnaXRlZSI6IiIsImdpdGh1YiI6IiIsImdpdGxhYiI6IiIsImdvb2dsZSI6IiIsImhhc2giOiIiLCJob21lcGFnZSI6IiIsImlhdCI6MTY0MzE5MjEwMSwiaWQiOiIxYzU1NTgxZS01ZmEyLTQ4NTEtOWM2NC04MjNhNjYyZDBkY2IiLCJpZENhcmQiOiIiLCJpZENhcmRUeXBlIjoiIiwiaXNBZG1pbiI6dHJ1ZSwiaXNEZWZhdWx0QXZhdGFyIjpmYWxzZSwiaXNEZWxldGVkIjpmYWxzZSwiaXNGb3JiaWRkZW4iOmZhbHNlLCJpc0dsb2JhbEFkbWluIjp0cnVlLCJpc09ubGluZSI6ZmFsc2UsImlzcyI6Imh0dHA6Ly9sb2NhbGhvc3Q6ODAwMCIsImxhbmd1YWdlIjoiIiwibGFyayI6IiIsImxhc3RTaWduaW5JcCI6IiIsImxhc3RTaWduaW5UaW1lIjoiIiwibGRhcCI6IiIsImxpbmtlZGluIjoiIiwibG9jYXRpb24iOiIiLCJuYW1lIjoiY2JjIiwibmJmIjoxNjQzMTkyMTAxLCJub25jZSI6Im51bGwiLCJvd25lciI6ImJ1aWx0LWluIiwicGFzc3dvcmQiOiIiLCJwYXNzd29yZFNhbHQiOiIiLCJwZXJtYW5lbnRBdmF0YXIiOiIiLCJwaG9uZSI6IjE4ODE3NTgzMjA3IiwicHJlSGFzaCI6IjAwY2JiNGEyOTBjZDBjZDgwZmZkZWMyZjBhOWJlM2E2IiwicHJvcGVydGllcyI6e30sInFxIjoiIiwicmFua2luZyI6MCwicmVnaW9uIjoiIiwic2NvcmUiOjIwMDAsInNpZ251cEFwcGxpY2F0aW9uIjoiYXBwLWJ1aWx0LWluIiwic2xhY2siOiIiLCJzdWIiOiIxYzU1NTgxZS01ZmEyLTQ4NTEtOWM2NC04MjNhNjYyZDBkY2IiLCJ0YWciOiJyZWFkd3JpdGUiLCJ0aXRsZSI6IiIsInR5cGUiOiJub3JtYWwtdXNlciIsInVwZGF0ZWRUaW1lIjoiIiwid2VjaGF0IjoiIiwid2Vjb20iOiIiLCJ3ZWlibyI6IiJ9.C5ZoJrojpRSePg_Ef9O-JTnc9BgoDNC5JX5AxlE9npd2tNl3ftudhny47pG6GgNDeiCMiaxueNyb_HPEPltJTw",
    SignerType: 1
  }
}
```

### Using MinIO Console

- Open MinIO URL on the browser, lets say <http://localhost:9000/>
- Click on `Login with SSO`
- User will be redirected to the Casdoor user login page, upon successful login the user will be redirected to MinIO page and logged in automatically,
  the user should see now the buckets and objects they have access to.

## Explore Further

- [Casdoor MinIO Integration](https://casdoor.org/docs/integration/minio)
- [MinIO STS Quickstart Guide](https://docs.min.io/community/minio-object-store/developers/security-token-service.html)
- [The MinIO documentation website](https://docs.min.io/community/minio-object-store/index.html)
