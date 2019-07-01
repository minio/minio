# MinIO多用户快速入门指南 [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)
除了在服务器启动期间创建的默认用户外，MinIO还支持多个长期用户。在服务器启动后,可以向MinIO中添加新用户，并且可以配置服务器拒绝或允许每个用户对存储桶和资源的访问。本文档介绍了如何添加/删除用户以及修改其访问权限。

## 开始
在本文档中，我们将详细介绍如何配置多个用户。

### 1. 先决条件
- 安装mc - [MinIO客户端快速入门指南](https://docs.min.io/docs/minio-client-quickstart-guide.html)
- 安装MinIO - [MinIO快速入门指南](https://docs.min.io/docs/minio-quickstart-guide)
- 配置etcd（仅在网关或联合模式下可选） - [Etcd V3快速入门指南](https://github.com/minio/minio/blob/master/docs/sts/etcd.md)

### 2. 使用预设策略创建新用户

使用[`mc admin policy`](https://docs.min.io/docs/minio-admin-complete-guide.html#policies)创建预设策略。服务器提供一组默认的预设策略`writeonly，readonly`并且`readwrite` （这些策略适用于服务器上的所有资源）。可以使用`mc admin policy`命令按照自定义策略覆盖预设策略。

创建新的预设策略文件`getonly.json`。此策略使用户可以下载所有对象`my-bucketname`。

```json
cat > getonly.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": [
        "s3:GetObject"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:s3:::my-bucketname/*"
      ],
      "Sid": ""
    }
  ]
}
EOF
```
使用`getonly.json`策略文件按名称`getonly`创建新的预设策略。

```
mc admin policy add myminio getonly getonly.json
```
在MinIO创建新用户`newuser`，使用`mc admin user`,为这个`newuser`指定getonly预设策略。

```
mc admin user add myminio newuser newuser123 getonly
```

### 3. 禁用用户

禁用用户`newuser`。

```
mc admin user disable myminio newuser
```

### 4. 删除用户

删除用户`newuser`。

```
mc admin user remove myminio newuser
```

### 5. 更改用户策略

将用户`newuser`的策略更改为`putonly`预设策略。

```
mc admin user policy myminio newuser putonly
```

### 6. 列出所有用户

列出所有启用和禁用的用户。

```
mc admin user list myminio
```

### 7. 配置 `mc`
```
mc config host add myminio-newuser http://localhost:9000 newuser newuser123 --api s3v4
mc cat myminio-newuser/my-bucketname/my-objectname
```

## 进一步探索

- [MinIO客户端完整指南](https://docs.min.io/docs/minio-client-complete-guide)
- [MinIO STS快速入门指南](https://docs.min.io/docs/minio-sts-quickstart-guide)
- [MinIO管理员完整指南](https://docs.min.io/docs/minio-admin-complete-guide.html)
- [MinIO文档网站](https://docs.min.io/)
