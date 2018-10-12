# Minio STS Quickstart Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)
The Minio Security Token Service (STS) is an endpoint service that enables clients to request temporary credentials for Minio resources. Temporary credentials work almost identically to default admin credentials, with some differences:

- Temporary credentials are short-term, as the name implies. They can be configured to last for anywhere from a few minutes to several hours. After the credentials expire, Minio no longer recognizes them or allows any kind of access from API requests made with them.
- Temporary credentials do not need to be stored with the application but are generated dynamically and provided to the application when requested. When (or even before) the temporary credentials expire, the application can request new credentials.

Following are advantages for using temporary credentials:

- No need embed long-term credentials with an application.
- No need to provide access to buckets and objects without having to define static credentials.
- Temporary credentials have a limited lifetime, no need to rotate them or explicitly revoke them when they're no longer needed. After temporary credentials expire, they cannot be reused.

## Identity Federation
[**Client grants**](./client-grants.md) - Let applications request `client_grants` using any well-known third party identity provider such as KeyCloak, WSO2. This is known as the client grants approach to temporary access. Using this approach helps clients keep Minio credentials to be secured. Minio STS client grants supports WSO2, Keycloak.

## Get started
In this document we will explain in detail on how to configure all the prerequisites, primarily WSO2, OPA (open policy agent).

### 1. Prerequisites
- [Configuring wso2](./wso2.md)
- [Configuring opa](./opa.md)
- [Configuring etcd (optional needed only in gateway or federation mode)](./etcd.md)

### 2. Setup Minio with WSO2, OPA
Make sure we have followed the previous step and configured each software independently, once done we can now proceed to use Minio STS API and Minio server to use these credentials to perform object API operations.

```
export MINIO_ACCESS_KEY=minio
export MINIO_SECRET_KEY=minio123
export MINIO_IAM_JWKS_URL=https://localhost:9443/oauth2/jwks
export MINIO_IAM_OPA_URL=http://localhost:8181/v1/data/httpapi/authz
minio server /mnt/data
```

### 3. Setup Minio Gateway with WSO2, OPA, ETCD
Make sure we have followed the previous step and configured each software independently, once done we can now proceed to use Minio STS API and Minio gateway to use these credentials to perform object API operations.

> NOTE: Minio gateway requires etcd to be configured to use STS API.

```
export MINIO_ACCESS_KEY=aws_access_key
export MINIO_SECRET_KEY=aws_secret_key
export MINIO_IAM_JWKS_URL=https://localhost:9443/oauth2/jwks
export MINIO_IAM_OPA_URL=http://localhost:8181/v1/data/httpapi/authz
export MINIO_ETCD_ENDPOINTS=localhost:2379
minio gateway s3
```

### 4. Test using full-example.go
On another terminal run `full-example.go` a sample client application which obtains JWT access tokens from an identity provider, in our case its WSO2. Uses the returned access token response to get new temporary credentials from the Minio server using the STS API call `AssumeRoleWithClientGrants`.

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

## Explore Further
- [Minio STS Quickstart Guide](https://docs.minio.io/docs/minio-sts-quickstart-guide)
- [The Minio documentation website](https://docs.minio.io)
