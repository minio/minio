# MinIO STS Quickstart Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)
The MinIO Security Token Service (STS) is an endpoint service that enables clients to request temporary credentials for MinIO resources. Temporary credentials work almost identically to default admin credentials, with some differences:

- Temporary credentials are short-term, as the name implies. They can be configured to last for anywhere from a few minutes to several hours. After the credentials expire, MinIO no longer recognizes them or allows any kind of access from API requests made with them.
- Temporary credentials do not need to be stored with the application but are generated dynamically and provided to the application when requested. When (or even before) the temporary credentials expire, the application can request new credentials.

Following are advantages for using temporary credentials:

- Eliminates the need to embed long-term credentials with an application.
- Eliminates the need to provide access to buckets and objects without having to define static credentials.
- Temporary credentials have a limited lifetime, there is no need to rotate them or explicitly revoke them. Expired temporary credentials cannot be reused.

## Identity Federation
|AuthN | Description |
| :---------------------- | ------------------------------------------ |
| [**Client grants**](https://github.com/minio/minio/blob/master/docs/sts/client-grants.md) | Let applications request `client_grants` using any well-known third party identity provider such as KeyCloak, Okta. This is known as the client grants approach to temporary access. Using this approach helps clients keep MinIO credentials to be secured. MinIO STS supports client grants, tested against identity providers such as KeyCloak, Okta. |
| [**WebIdentity**](https://github.com/minio/minio/blob/master/docs/sts/web-identity.md) | Let users request temporary credentials using any OpenID(OIDC) compatible web identity providers such as KeyCloak, Facebook, Google etc. |
| [**AssumeRole**](https://github.com/minio/minio/blob/master/docs/sts/assume-role.md) | Let MinIO users request temporary credentials using user access and secret keys. |
| [**AD/LDAP**](https://github.com/minio/minio/blob/master/docs/sts/ldap.md) | Let AD/LDAP users request temporary credentials using AD/LDAP username and password. |

## Get started
In this document we will explain in detail on how to configure all the prerequisites.

> NOTE: If you are interested in AssumeRole API only, skip to [here](https://github.com/minio/minio/blob/master/docs/sts/assume-role.md)

### 1. Prerequisites
- [Configuring keycloak](https://github.com/minio/minio/blob/master/docs/sts/keycloak.md)
- [Configuring etcd (optional needed only in gateway or federation mode)](https://github.com/minio/minio/blob/master/docs/sts/etcd.md)

### 2. Setup MinIO with Keycloak
Make sure we have followed the previous step and configured each software independently, once done we can now proceed to use MinIO STS API and MinIO server to use these credentials to perform object API operations.

```
export MINIO_ACCESS_KEY=minio
export MINIO_SECRET_KEY=minio123
export MINIO_IDENTITY_OPENID_CONFIG_URL=http://localhost:8080/auth/realms/demo/.well-known/openid-configuration
export MINIO_IDENTITY_OPENID_CLIENT_ID="843351d4-1080-11ea-aa20-271ecba3924a"
minio server /mnt/data
```

### 3. Setup MinIO Gateway with Keycloak and Etcd
Make sure we have followed the previous step and configured each software independently, once done we can now proceed to use MinIO STS API and MinIO gateway to use these credentials to perform object API operations.

> NOTE: MinIO gateway requires etcd to be configured to use STS API.

```
export MINIO_ACCESS_KEY=aws_access_key
export MINIO_SECRET_KEY=aws_secret_key
export MINIO_IDENTITY_OPENID_CONFIG_URL=http://localhost:8080/auth/realms/demo/.well-known/openid-configuration
export MINIO_IDENTITY_OPENID_CLIENT_ID="843351d4-1080-11ea-aa20-271ecba3924a"
export MINIO_ETCD_ENDPOINTS=http://localhost:2379
minio gateway s3
```

### 4. Test using client-grants.go
On another terminal run `client-grants.go` a sample client application which obtains JWT access tokens from an identity provider, in our case its Keycloak. Uses the returned access token response to get new temporary credentials from the MinIO server using the STS API call `AssumeRoleWithClientGrants`.

```
go run client-grants.go -cid PoEgXP6uVO45IsENRngDXj5Au5Ya -csec eKsw6z8CtOJVBtrOWvhRWL4TUCga

##### Credentials
{
	"accessKey": "NUIBORZYTV2HG2BMRSXR",
	"secretKey": "qQlP5O7CFPc5m5IXf1vYhuVTFj7BRVJqh0FqZ86S",
	"expiration": "2018-08-21T17:10:29-07:00",
	"sessionToken": "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NLZXkiOiJOVUlCT1JaWVRWMkhHMkJNUlNYUiIsImF1ZCI6IlBvRWdYUDZ1Vk80NUlzRU5SbmdEWGo1QXU1WWEiLCJhenAiOiJQb0VnWFA2dVZPNDVJc0VOUm5nRFhqNUF1NVlhIiwiZXhwIjoxNTM0ODk2NjI5LCJpYXQiOjE1MzQ4OTMwMjksImlzcyI6Imh0dHBzOi8vbG9jYWxob3N0Ojk0NDMvb2F1dGgyL3Rva2VuIiwianRpIjoiNjY2OTZjZTctN2U1Ny00ZjU5LWI0MWQtM2E1YTMzZGZiNjA4In0.eJONnVaSVHypiXKEARSMnSKgr-2mlC2Sr4fEGJitLcJF_at3LeNdTHv0_oHsv6ZZA3zueVGgFlVXMlREgr9LXA"
}
```

## Explore Further
- [MinIO Admin Complete Guide](https://docs.min.io/docs/minio-admin-complete-guide.html)
- [The MinIO documentation website](https://docs.min.io)
