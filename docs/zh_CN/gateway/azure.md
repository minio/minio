
# Minio Azure 网关 [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)
Minio网关将亚马逊S3兼容性添加到微软Azure Blob存储。

## 运行支持微软Azure Blob存储的Minio网关
### 使用Docker
```
docker run -p 9000:9000 --name azure-s3 \
 -e "MINIO_ACCESS_KEY=azureaccountname" \
 -e "MINIO_SECRET_KEY=azureaccountkey" \
 minio/minio gateway azure
```

### 使用二进制
```
export MINIO_ACCESS_KEY=azureaccountname
export MINIO_SECRET_KEY=azureaccountkey
minio gateway azure
```
## 使用Minio浏览器验证
Minio Gateway配有嵌入式网络对象浏览器。 将您的Web浏览器指向http://127.0.0.1:9000确保您的服务器已成功启动。

![截图](https://github.com/minio/minio/blob/master/docs/screenshots/minio-browser-gateway.png?raw=true)
##使用Minio客户端 `mc`验证
`mc` 提供了诸如ls，cat，cp，mirror，diff等UNIX命令的替代方案。它支持文件系统和Amazon S3兼容的云存储服务。

### 配置 `mc`
```
mc config host add myazure http://gateway-ip:9000 azureaccountname azureaccountkey
```

### 列出微软Azure上的容器
```
mc ls myazure
[2017-02-22 01:50:43 PST]     0B ferenginar/
[2017-02-26 21:43:51 PST]     0B my-container/
[2017-02-26 22:10:11 PST]     0B test-container1/
```

### 已知的限制
[限制](https://github.com/minio/minio/blob/master/docs/gateway/azure-limitations.md)

## 了解更多
- [`mc` 命令行接口](https://docs.minio.io/docs/zh_CN/minio-client-quickstart-guide)
- [`aws` 命令行接口](https://docs.minio.io/docs/zh_CN/aws-cli-with-minio)
- [`minfs` 文件系统接口](http://docs.minio.io/docs/zh_CN/minfs-quickstart-guide)
- [`minio-go` Go SDK](https://docs.minio.io/docs/zh_CN/golang-client-quickstart-guide)
