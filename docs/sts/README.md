# MinIO STS Quickstart Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

The MinIO Security Token Service (STS) is an endpoint service that enables clients to request temporary credentials for MinIO resources. Temporary credentials work almost identically to default admin credentials, with some differences:

- Temporary credentials are short-term, as the name implies. They can be configured to last for anywhere from a few minutes to several hours. After the credentials expire, MinIO no longer recognizes them or allows any kind of access from API requests made with them.
- Temporary credentials do not need to be stored with the application but are generated dynamically and provided to the application when requested. When (or even before) the temporary credentials expire, the application can request new credentials.

Following are advantages for using temporary credentials:

- Eliminates the need to embed long-term credentials with an application.
- Eliminates the need to provide access to buckets and objects without having to define static credentials.
- Temporary credentials have a limited lifetime, there is no need to rotate them or explicitly revoke them. Expired temporary credentials cannot be reused.

## Identity Federation

| AuthN                                                                                  | Description                                                                                                                                   |
| :----------------------                                                                | ------------------------------------------                                                                                                    |
| [**WebIdentity**](https://github.com/minio/minio/blob/master/docs/sts/web-identity.md) | Let users request temporary credentials using any OpenID(OIDC) compatible web identity providers such as KeyCloak, Dex, Facebook, Google etc. |
| [**AD/LDAP**](https://github.com/minio/minio/blob/master/docs/sts/ldap.md)             | Let AD/LDAP users request temporary credentials using AD/LDAP username and password.                                                          |
| [**AssumeRole**](https://github.com/minio/minio/blob/master/docs/sts/assume-role.md)   | Let MinIO users request temporary credentials using user access and secret keys.                                                              |

### Understanding JWT Claims

> NOTE: JWT claims are only meant for WebIdentity and ClientGrants.
> AssumeRole or LDAP users can skip the entire portion and directly visit one of the links below.
>
> - [**AssumeRole**](https://github.com/minio/minio/blob/master/docs/sts/assume-role.md)
> - [**AD/LDAP**](https://github.com/minio/minio/blob/master/docs/sts/ldap.md)

The id_token received is a signed JSON Web Token (JWT). Use a JWT decoder to decode the id_token to access the payload of the token that includes following JWT claims, `policy` claim is mandatory and should be present as part of your JWT claim. Without this claim the generated credentials will not have access to any resources on the server, using these credentials application would receive 'Access Denied' errors.

| Claim Name | Type                                              | Claim Value                                                                                                                                                                                                        |
|:----------:|:-------------------------------------------------:|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|
| policy     | _string_ or _[]string_ or _comma_separated_value_ | Canned policy name to be applied for STS credentials. (Mandatory) - This can be configured to any desired value such as `roles` or `groups` by setting the environment variable `MINIO_IDENTITY_OPENID_CLAIM_NAME` |

## Get started

In this document we will explain in detail on how to configure all the prerequisites.

> NOTE: If you are interested in AssumeRole API only, skip to [here](https://github.com/minio/minio/blob/master/docs/sts/assume-role.md)

### Prerequisites

- [Configuring keycloak](https://github.com/minio/minio/blob/master/docs/sts/keycloak.md) or [Configuring Casdoor](https://github.com/minio/minio/blob/master/docs/sts/casdoor.md)
- [Configuring etcd](https://github.com/minio/minio/blob/master/docs/sts/etcd.md)

### Setup MinIO with Identity Provider

Make sure we have followed the previous step and configured each software independently, once done we can now proceed to use MinIO STS API and MinIO server to use these credentials to perform object API operations.

#### KeyCloak

```
export MINIO_ROOT_USER=minio
export MINIO_ROOT_PASSWORD=minio123
export MINIO_IDENTITY_OPENID_CONFIG_URL=http://localhost:8080/auth/realms/demo/.well-known/openid-configuration
export MINIO_IDENTITY_OPENID_CLIENT_ID="843351d4-1080-11ea-aa20-271ecba3924a"
minio server /mnt/data
```

#### Casdoor

```
export MINIO_ROOT_USER=minio
export MINIO_ROOT_PASSWORD=minio123
export MINIO_IDENTITY_OPENID_CONFIG_URL=http://CASDOOR_ENDPOINT/.well-known/openid-configuration
export MINIO_IDENTITY_OPENID_CLIENT_ID="843351d4-1080-11ea-aa20-271ecba3924a"
minio server /mnt/data
```

### Using WebIdentiy API

On another terminal run `web-identity.go` a sample client application which obtains JWT id_tokens from an identity provider, in our case its Keycloak. Uses the returned id_token response to get new temporary credentials from the MinIO server using the STS API call `AssumeRoleWithWebIdentity`.

```
$ go run docs/sts/web-identity.go -cid account -csec 072e7f00-4289-469c-9ab2-bbe843c7f5a8  -config-ep "http://localhost:8080/auth/realms/demo/.well-known/openid-configuration" -port 8888
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

- [MinIO Admin Complete Guide](https://docs.min.io/community/minio-object-store/reference/minio-mc-admin.html)
- [The MinIO documentation website](https://docs.min.io/community/minio-object-store/index.html)
