## Minio Browser

Minio Browser使用Json Web Token来验证JSON RPC请求。

初使请求为用户提供的`AccessKey`和`SecretKey`生成一个token令牌

<blockquote>
目前这些令牌在10小时后到期，这是不可配置的。
</blockquote>

### 启动minio server

```
minio server /data
```

### JSON RPC APIs.

JSON RPC命名空间是`Web`.

#### 服务器操作

* ServerInfo -获取当前服务器信息，包括内存统计，minio二进制版本，golang运行时版本等。
* StorageInfo - 获取磁盘空间可用空间(Total/Free)，类型，磁盘在线/脱机状态，以及ReadQuorum和WriteQuorum计数。

#### 认证操作

* Login - 等待用户名密码输入，成功回复一个新的Json Web Token（JWT）。
* SetAuth - 使用新的用户名，密码更改访问凭据。
* GetAuth - 从服务器获取当前的身份验证。

#### Bucket/Object operations.

* ListBuckets - 列出所有的存储桶，需要有合法的token令牌。
* ListObjects - 列出存储对象, 需要有合法的token令牌。
* MakeBucket - 创建一个新的存储桶, 需要有合法的token令牌。
* RemoveObject - 从存储桶中移除一个对象,需要有合法的token令牌。
* Upload - 通过浏览器上传一个新的对象,需要有合法的token令牌。
* Download - 从存储桶中下载一个对象, 需要有合法的token令牌。
