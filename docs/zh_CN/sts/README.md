# MinIO STS快速入门指南 [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

MinIO Security Token Service（STS）是一种端点服务，使客户端可以为MinIO资源请求临时凭证。临时凭证几乎与默认管理凭据相同，但仍存在一些差异：

- 顾名思义，临时证书是短期的。它们可以配置为持续几分钟到几个小时。凭据到期后，MinIO不再识别它们或允许通过它们发出的API请求进行任何类型的访问。
- 临时凭证不需要与应用程序一起存储，而是动态生成并在请求时提供给应用程序。临时凭证到期时（或甚至之前），应用程序可以请求新凭据。

以下是使用临时凭证的优点：

- 无需在应用程序中嵌入长期凭据。
- 无需定义静态凭据，无需提供对存储桶和对象的访问。
- 临时凭证的生命周期有限，无需轮换它们或明确撤销它们。过期的临时凭证无法重复使用。

## 身份联合

- [**Client grants**](https://github.com/minio/minio/blob/master/docs/sts/client-grants.md) -让应用程序使用任何知名的第三方身份提供商（如KeyCloak，WSO2）请求`client_grants`。这被称为客户端授予临时访问的方法。使用此方法可帮助客户保持MinIO凭据的安全。MinIO STS支持客户端授权，针对身份提供商（如WSO2，KeyCloak）进行测试。
- [**WebIdentity**](https://github.com/minio/minio/blob/master/docs/sts/web-identity.md) - 允许用户使用任何与OpenID（OIDC）兼容的Web身份提供商（如Facebook，Google等）请求临时凭证。
- [**AssumeRole**](https://github.com/minio/minio/blob/master/docs/sts/assume-role.md) - 让MinIO用户通过用户访问权限和密钥请求临时凭证。

## 开始
在本文档中，我们将详细说明如何配置所有先决条件，将重点放在WSO2，OPA（开放策略代理）的讲解上。

> 注意：如果您只对AssumeRole API感兴趣，请转至[此处](https://github.com/minio/minio/blob/master/docs/sts/assume-role.md)

### 1.先决条件
- [配置wso2](https://github.com/minio/minio/blob/master/docs/sts/wso2.md)
- [配置opa（可选）](https://github.com/minio/minio/blob/master/docs/sts/opa.md)
- [配置etcd（仅在网关或联合模式下可选）](https://github.com/minio/minio/blob/master/docs/sts/etcd.md)

### 2.使用WSO2设置MinIO
确保我们已经按照之前的步骤并独立配置了每个软件，一旦完成，我们现在就可以继续使用MinIO STS API和MinIO服务器来使用这些凭据去执行对象API操作。

```
export MINIO_ACCESS_KEY=minio
export MINIO_SECRET_KEY=minio123
export MINIO_IAM_JWKS_URL=https://localhost:9443/oauth2/jwks
minio server /mnt/data
```

### 3.使用WSO2，ETCD设置MinIO Gateway
确保我们已经按照之前的步骤并独立配置了每个软件，一旦完成，我们现在可以继续使用MinIO STS API和MinIO网关来使用这些凭据来执行对象API操作。

> 注意：MinIO网关要求将etcd配置为使用STS API。

```
export MINIO_ACCESS_KEY=aws_access_key
export MINIO_SECRET_KEY=aws_secret_key
export MINIO_IAM_JWKS_URL=https://localhost:9443/oauth2/jwks
export MINIO_ETCD_ENDPOINTS=http://localhost:2379
minio gateway s3
```

### 4.使用client-grants.go进行测试
在另一个终端上运行`client-grants.go`，一个从身份提供者获取JWT访问令牌的示例客户端应用程序，在我们的例子中身份提供者是WSO2。使用返回的访问令牌响应来从MinIO服务器得到新的临时凭证，在申请临时凭证的时候需要调用叫做`AssumeRoleWithClientGrants`的STS API。

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

## 进一步探索
- [MinIO管理员完整指南](https://docs.min.io/docs/minio-admin-complete-guide.html)
- [MinIO文档网站](https://docs.min.io/)
