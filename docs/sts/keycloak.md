# Keycloak Quickstart Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

Keycloak is an open source Identity and Access Management solution aimed at modern applications and services, this document covers configuring Keycloak identity provider support with MinIO.

## Prerequisites

Configure and install keycloak server by following [Keycloak Installation Guide](https://www.keycloak.org/docs/latest/server_installation/#installing-the-software).
For a quick installation, docker-compose reference configs are also available on the [Keycloak GitHub](https://github.com/keycloak/keycloak-containers/tree/main/docker-compose-examples).

### Configure Keycloak Realm

- Go to Clients
  - Click on account
    - Settings
    - Change `Access Type` to `confidential`.
    - Save
  - Click on credentials tab
    - Copy the `Secret` to clipboard.
    - This value is needed for `MINIO_IDENTITY_OPENID_CLIENT_SECRET` for MinIO.

- Go to Users
  - Click on the user
  - Attribute, add a new attribute `Key` is `policy`, `Value` is name of the `policy` on MinIO (ex: `readwrite`)
  - Add and Save

- Go to Clients
  - Click on `account`
  - Settings, set `Valid Redirect URIs` to `*`, expand `Advanced Settings` and set `Access Token Lifespan` to `1 Hours`
  - Save

- Go to Clients
  - Click on `account`
  - Mappers
  - Create
    - `Name` with any text
    - `Mapper Type` is `User Attribute`
    - `User Attribute` is `policy`
    - `Token Claim Name` is `policy`
    - `Claim JSON Type` is `string`
  - Save

- Open <http://localhost:8080/auth/realms/{your-realm-name}/.well-known/openid-configuration> to verify OpenID discovery document, verify it has `authorization_endpoint` and `jwks_uri`

### Enable Keycloak Admin REST API support

Before being able to authenticate against the Admin REST API using a client_id and a client_secret you need to make sure the client is configured as it follows:

- `account` client_id is a confidential client that belongs to the realm `{realm}`
- `account` client_id is has **Service Accounts Enabled** option enabled.
- `account` client_id has a custom "Audience" mapper, in the Mappers section.
  - Included Client Audience: security-admin-console

#### Adding 'admin' Role

- Go to Roles
  - Add new Role `admin` with Description `${role_admin}`.
  - Add this Role into compositive role named `default-roles-{realm}` - `{realm}` should be replaced with whatever realm you created from `prerequisites` section. This role is automatically trusted in the 'Service Accounts' tab.

- Check that `account` client_id has the role 'admin' assigned in the "Service Account Roles" tab.

After that, you will be able to obtain an id_token for the Admin REST API using client_id and client_secret:

```
curl \
  -d "client_id=<YOUR_CLIENT_ID>" \
  -d "client_secret=<YOUR_CLIENT_SECRET>" \
  -d "grant_type=client_credentials" \
  "http://localhost:8080/auth/realms/{realm}/protocol/openid-connect/token"
```

The result will be a JSON document. To invoke the API you need to extract the value of the access_token property. You can then invoke the API by including the value in the Authorization header of requests to the API.

The following example shows how to get the details of the user with `{userid}` from `{realm}` realm:

```
curl \
  -H "Authorization: Bearer eyJhbGciOiJSUz..." \
  "http://localhost:8080/auth/admin/realms/{realm}/users/{userid}"
```

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
~ mc admin config set myminio identity_openid config_url="http://localhost:8080/auth/realms/{your-realm-name}/.well-known/openid-configuration" client_id="account"
```

> NOTE: You can configure the `scopes` parameter to restrict the OpenID scopes requested by minio to the IdP, for example, `"openid,policy_role_attribute"`, being `policy_role_attribute` a client_scope / client_mapper that maps a role attribute called policy to a `policy` claim returned by Keycloak

Once successfully set restart the MinIO instance.

```
mc admin service restart myminio
```

### Using WebIdentiy API

Client ID can be found by clicking any of the clients listed [here](http://localhost:8080/auth/admin/master/console/#/realms/minio/clients). If you have followed the above steps docs, the default Client ID will be `account`.

```
$ go run docs/sts/web-identity.go -cid account -csec 072e7f00-4289-469c-9ab2-bbe843c7f5a8  -config-ep "http://localhost:8080/auth/realms/minio/.well-known/openid-configuration" -port 8888
2018/12/26 17:49:36 listening on http://localhost:8888/
```

This will open the login page of keycloak, upon successful login, STS credentials along with any buckets discovered using the credentials will be printed on the screen, for example:

```
{
  "buckets": [
    "bucket-x"
  ],
  "credentials": {
    "AccessKeyID": "6N2BALX7ELO827DXS3GK",
    "SecretAccessKey": "23JKqAD+um8ObHqzfIh+bfqwG9V8qs9tFY6MqeFR+xxx",
    "SessionToken": "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NLZXkiOiI2TjJCQUxYN0VMTzgyN0RYUzNHSyIsImFjciI6IjAiLCJhdWQiOiJhY2NvdW50IiwiYXV0aF90aW1lIjoxNTY5OTEwNTUyLCJhenAiOiJhY2NvdW50IiwiZW1haWxfdmVyaWZpZWQiOmZhbHNlLCJleHAiOjE1Njk5MTQ1NTQsImlhdCI6MTU2OTkxMDk1NCwiaXNzIjoiaHR0cDovL2xvY2FsaG9zdDo4MDgxL2F1dGgvcmVhbG1zL2RlbW8iLCJqdGkiOiJkOTk4YTBlZS01NDk2LTQ4OWYtYWJlMi00ZWE5MjJiZDlhYWYiLCJuYmYiOjAsInBvbGljeSI6InJlYWR3cml0ZSIsInByZWZlcnJlZF91c2VybmFtZSI6Im5ld3VzZXIxIiwic2Vzc2lvbl9zdGF0ZSI6IjJiYTAyYTI2LWE5MTUtNDUxNC04M2M1LWE0YjgwYjc4ZTgxNyIsInN1YiI6IjY4ZmMzODVhLTA5MjItNGQyMS04N2U5LTZkZTdhYjA3Njc2NSIsInR5cCI6IklEIn0._UG_-ZHgwdRnsp0gFdwChb7VlbPs-Gr_RNUz9EV7TggCD59qjCFAKjNrVHfOSVkKvYEMe0PvwfRKjnJl3A_mBA"",
    "SignerType": 1
  }
}
```

> NOTE: You can use the `-cscopes` parameter to restrict the requested scopes, for example to `"openid,policy_role_attribute"`, being `policy_role_attribute` a client_scope / client_mapper that maps a role attribute called policy to a `policy` claim returned by Keycloak.

These credentials can now be used to perform MinIO API operations.

### Using MinIO Console

- Open MinIO URL on the browser, lets say <http://localhost:9000/>
- Click on `Login with SSO`
- User will be redirected to the Keycloak user login page, upon successful login the user will be redirected to MinIO page and logged in automatically,
  the user should see now the buckets and objects they have access to.

## Explore Further

- [MinIO STS Quickstart Guide](https://docs.min.io/community/minio-object-store/developers/security-token-service.html)
- [The MinIO documentation website](https://docs.min.io/community/minio-object-store/index.html)
