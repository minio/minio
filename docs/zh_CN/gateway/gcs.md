# MinIO GCS 网关 [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)
MinIO GCS网关将亚马逊S3兼容性添加到Google云存储。

## 运行支持GCS的MinIO 网关
### 为GCS创建服务帐户密钥，并获取凭据文件
1. 访问 [API控制台凭证页面](https://console.developers.google.com/project/_/apis/credentials).
2. 选择您的项目或创建一个新项目， 记下你的项目ID。
3. 在凭据页面，选择 __Create credentials__ 下拉项，然后选择 __Service account key__。
4. 从 __Service account__下拉项, 选择 __New service account__
5. 填写 __Service account name__ 和 __Service account ID__
6. 对于 __Role__, 点击下拉项，选择 __Storage__ -> __Storage Admin__ _(完全控制GCS资源)_
7. 点击 __Create__ 按钮，下载凭据文件到你的桌面，文件名咱们就叫 credentials.json

注意: 设置 *Application Default Credentials*的替代方案 在 [这里](https://developers.google.com/identity/protocols/application-default-credentials)进行了描述。

### 使用 Docker
```
docker run -p 9000:9000 --name gcs-s3 \
 -v /path/to/credentials.json:/credentials.json \
 -e "GOOGLE_APPLICATION_CREDENTIALS=/credentials.json" \
 -e "MINIO_ACCESS_KEY=minioaccountname" \
 -e "MINIO_SECRET_KEY=minioaccountkey" \
 minio/minio gateway gcs yourprojectid
```

### 使用二进制
```
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
export MINIO_ACCESS_KEY=minioaccesskey
export MINIO_SECRET_KEY=miniosecretkey
minio gateway gcs yourprojectid
```

## 使用MinIO Browser验证
MinIO Gateway配有嵌入式网络对象浏览器。 将您的Web浏览器指向http://127.0.0.1:9000确保您的服务器已成功启动。

![Screenshot](https://github.com/minio/minio/blob/master/docs/screenshots/minio-browser-gateway.png?raw=true)

## 使用MinIO客户端 `mc`验证
`mc` 提供了诸如ls，cat，cp，mirror，diff等UNIX命令的替代方案。它支持文件系统和Amazon S3兼容的云存储服务。

### 配置  `mc`
```
mc alias set mygcs http://gateway-ip:9000 minioaccesskey miniosecretkey
```

### 列出GCS上的容器
```
mc ls mygcs
[2017-02-22 01:50:43 PST]     0B ferenginar/
[2017-02-26 21:43:51 PST]     0B my-container/
[2017-02-26 22:10:11 PST]     0B test-container1/
```

## 了解更多
- [`mc` 命令行接口](https://docs.min.io/cn/minio-client-quickstart-guide)
- [`aws` 命令行接口](https://docs.min.io/cn/aws-cli-with-minio)
- [`minio-go` Go SDK](https://docs.min.io/cn/golang-client-quickstart-guide)

