# KMS快速入门指南 [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

MinIO使用密钥管理系统（KMS）来支持SSE-S3。如果客户端请求SSE-S3或
启用自动加密，则MinIO服务器使用唯一对象密钥加密每个对象，该对象密钥受KMS管理的主密钥保护。通常，所有对象键都受单个主密钥保护。

MinIO支持两种不同的KMS概念：  

- 外部KMS：MinIO可以配置为使用外部KMS，即 [Hashicorp Vault](https://www.vaultproject.io/)。外部KMS使MinIO与密钥管理相分离，将其作为存储系统。外部KMS可由专用安全团队管理，允许您通过按照需要启用或禁用相应的主密钥来授予/拒绝对（某些）对象的访问权限。

- 直接KMS主密钥：
MinIO也可以配置为直接使用环境变量 `MINIO_SSE_MASTER_KEY` 指定的主密钥。如果存储后端与MinIO服务器不在同一台计算机上，例如，如果使用网络驱动或MinIO网关，外部KMS则会导致过多的管理开销，这时直接主密钥将会非常有用。

注意：KMS主密钥主要用于测试目的。不建议将它们用于生产部署。此外，如果MinIO服务器遭到入侵，则主密钥也必须被视为已泄密。

**重要提示：**  

如果将多个MinIO服务器配置为指向同一后端的网关（例如相同的NAS存储），则所有网关的KMS配置**必须**相同。否则，一个网关可能无法解密由另一个网关创建的对象。运营商有责任确保一致性。



## 开始

### 1. 先决条件
安装MinIO - [MinIO快速入门指南](https://docs.min.io/docs/minio-quickstart-guide)。

### 2. 设置KMS
可以将Hashicorp Vault用作外部KMS，也可以根据用例直接指定主密钥。

#### 2.1 设置Hashicorp Vault
以下是使用传输后端配置Vault的示例快速入门和使用正确策略配置Approle的示例

MinIO需要以下的Vault设置：

- 用命名加密密钥环配置[传输后端](https://www.vaultproject.io/api/secret/transit/index.html)
- [AppRole](https://www.vaultproject.io/docs/auth/approle.html)基于传输后端读取/更新策略的身份验证。特别是，[生成数据密钥](https://www.vaultproject.io/api/secret/transit/index.html#generate-data-key)端点和[解密数据](https://www.vaultproject.io/api/secret/transit/index.html#decrypt-data)端点需要读取和更新策略。

**2.1.1 以开发模式启动Vault服务器**

在开发模式下，Vault服务器在内存中运行并开始启封。请注意，在开发模式下运行Vault是不安全的，重新启动时存储在Vault中的所有数据都将丢失。

```
vault server -dev
```

**2.1.2 设置vault传输后端并创建应用角色**

```
cat > vaultpolicy.hcl <<EOF
path "transit/datakey/plaintext/my-minio-key" { 
  capabilities = [ "read", "update"]
}
path "transit/decrypt/my-minio-key" { 
  capabilities = [ "read", "update"]
}
path "transit/encrypt/my-minio-key" { 
  capabilities = [ "read", "update"]
}

EOF

export VAULT_ADDR='http://127.0.0.1:8200'
vault auth enable approle    # enable approle style auth
vault secrets enable transit  # enable transit secrets engine
vault write -f  transit/keys/my-minio-key  #define a encryption key-ring for the transit path
vault policy write minio-policy ./vaultpolicy.hcl  #define a policy for AppRole to access transit path
vault write auth/approle/role/my-role token_num_uses=0  secret_id_num_uses=0  period=5m # period indicates it is renewable if token is renewed before the period is over
# define an AppRole
vault write auth/approle/role/my-role policies=minio-policy # apply policy to role
vault read auth/approle/role/my-role/role-id  # get Approle ID
vault write -f auth/approle/role/my-role/secret-id
```

AppRole ID，AppRole Secret Id，Vault端点和Vault密钥名称现在可用于启动vault作为KMS的minio服务器。

**2.1.3 Vault环境变量**

您需要在步骤2.1.2中定义的Vault端点，AppRole ID，AppRole SecretID和加密密钥环名称

```sh
export MINIO_SSE_VAULT_APPROLE_ID=9b56cc08-8258-45d5-24a3-679876769126
export MINIO_SSE_VAULT_APPROLE_SECRET=4e30c52f-13e4-a6f5-0763-d50e8cb4321f
export MINIO_SSE_VAULT_ENDPOINT=https://vault-endpoint-ip:8200
export MINIO_SSE_VAULT_KEY_NAME=my-minio-key
export MINIO_SSE_VAULT_AUTH_TYPE=approle
minio server ~/export
```

一个选择是，设置`MINIO_SSE_VAULT_CAPATH`为PEM编码的CA证书文件的目录，以使用mTLS进行客户端-服务器身份验证。

```
export MINIO_SSE_VAULT_CAPATH=/home/user/custom-certs
```

另一个选择是，设置 `MINIO_SSE_VAULT_NAMESPACEAppRole` 和Transit Secrets引擎是否已限定为Vault命名空间

```
export MINIO_SSE_VAULT_NAMESPACE=ns1
```

注意：如果正在使用[Vault命名空间](https://learn.hashicorp.com/vault/operations/namespaces)，则需要在设置approle和transit secrets引擎之前设置MINIO_SSE_VAULT_NAMESPACE变量。

到S3的MinIO网关支持加密。可以使用三种加密模式 - 加密可以设置为 ``pass-through`` 后端，``single encryption``（在网关处）或 ``double encryption``（网关上的单一加密并传递到后端）。这可以通过步骤2.1.2中设置的MINIO_GATEWAY_SSE和KMS环境变量来指定。

如果未设置MINIO_GATEWAY_SSE和KMS，则所有加密标头都将传递到后端。如果设置了KMS环境变量，``single encryption`` 则会在网关上自动执行，并且加密对象将保存在后端。

要指定 ``double encryption``，对于sse-s3 ，需要将MINIO_GATEWAY_SSE环境变量设置为“s3”，对于sse-c加密，需要设置为“c”。可以设置多个加密选项，以“;”分隔。对象在网关处加密，网关也会传递到后端。请注意，在SSE-C加密的情况下，网关会使用KDF从SSE-C客户端密钥派生出唯一的SSE-C密钥来进行传递。

```sh
export MINIO_GATEWAY_SSE="s3;c"
export MINIO_SSE_VAULT_APPROLE_ID=9b56cc08-8258-45d5-24a3-679876769126
export MINIO_SSE_VAULT_APPROLE_SECRET=4e30c52f-13e4-a6f5-0763-d50e8cb4321f
export MINIO_SSE_VAULT_ENDPOINT=https://vault-endpoint-ip:8200
export MINIO_SSE_VAULT_KEY_NAME=my-minio-key
export MINIO_SSE_VAULT_AUTH_TYPE=approle
minio gateway s3
```

#### 2.2 指定主密钥

KMS主密钥由主密钥ID（CMK）和编码为HEX值的256位主密钥组成，两者由一个`:`分割。
可以使用以下命令直接指定KMS主密钥：

```sh
export MINIO_SSE_MASTER_KEY=my-minio-key:6368616e676520746869732070617373776f726420746f206120736563726574
```

请使用您自己的主密钥。可以使用例如Linux / Mac / BSD *系统上的此命令生成随机主密钥：

``` 
head -c 32 /dev/urandom | xxd -c 32 -ps
```

### 3. 测试您的设置
要测试此设置，请使用步骤3中设置的环境变量启动minio服务器，并且服务器已准备好处理SSE-S3请求。

### 自动加密
**如果**指定了有效的KMS配置且存储后端支持加密对象，MinIO还可以启用自动加密。自动加密（如果启用）可确保使用指定的KMS配置加密所有上载的对象。

如果MinIO操作员希望确保对象**永远不会**以明文形式存储，则自动加密非常有用，例如，如果将敏感数据存储在公共云存储中。

要启用自动加密，请将环境变量设置为`on`：

```sh
export MINIO_SSE_AUTO_ENCRYPTION=on
```

要验证自动加密，请使用以下 `mc` 命令：

```sh
mc cp test.file myminio/crypt/
test.file:              5 B / 5 B  ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓  100.00% 337 B/s 0s
mc stat myminio/crypt/test.file
Name      : test.file
...
Encrypted :
  X-Amz-Server-Side-Encryption: AES256
```

注意：自动加密仅影响非SSE-C请求，因为使用SSE-C上载的对象已加密
，S3仅允许SSE-S3或SSE-C，但不允许对同一对象同时使用SSE-S3和SSE-C。


## 进一步探索

- [一起使用`mc`和MinIO服务](https://docs.min.io/docs/minio-client-quickstart-guide)
- [一起使用`aws-cli` 和MinIO服务](https://docs.min.io/docs/aws-cli-with-minio)
- [一起使用`s3cmd`和MinIO服务](https://docs.min.io/docs/s3cmd-with-minio)
- [一起使用`minio-go`服务](https://docs.min.io/docs/golang-client-quickstart-guide)
- [MinIO文档网站](https://docs.min.io/)
