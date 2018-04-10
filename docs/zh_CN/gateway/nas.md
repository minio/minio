# Minio NAS网关 [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)
Minio网关使用NAS存储支持Amazon S3。你可以在同一个共享NAS卷上运行多个minio实例，作为一个分布式的对象网关。

## 为NAS存储运行Minio网关
### 使用Docker
```
docker run -p 9000:9000 --name nas-s3 \
 -e "MINIO_ACCESS_KEY=minio" \
 -e "MINIO_SECRET_KEY=minio123" \
 minio/minio gateway nas /shared/nasvol
```

### 使用二进制
```
export MINIO_ACCESS_KEY=minioaccesskey
export MINIO_SECRET_KEY=miniosecretkey
minio gateway nas /shared/nasvol
```
## 使用浏览器进行验证
使用你的浏览器访问`http://127.0.0.1:9000`,如果能访问，恭喜你，启动成功了。

![Screenshot](https://raw.githubusercontent.com/minio/minio/master/docs/screenshots/minio-browser-gateway.png)

## 使用`mc`进行验证
`mc`为ls，cat，cp，mirror，diff，find等UNIX命令提供了一种替代方案。它支持文件系统和兼容Amazon S3的云存储服务（AWS Signature v2和v4）。

### 设置`mc`
```
mc config host add mynas http://gateway-ip:9000 access_key secret_key
```

### 列举nas上的存储桶
```
mc ls mynas
[2017-02-22 01:50:43 PST]     0B ferenginar/
[2017-02-26 21:43:51 PST]     0B my-bucket/
[2017-02-26 22:10:11 PST]     0B test-bucket1/
```

## 了解更多
- [`mc`快速入门](https://docs.minio.io/docs/minio-client-quickstart-guide)
- [使用 aws-cli](https://docs.minio.io/docs/aws-cli-with-minio)
- [使用 minio-go SDK](https://docs.minio.io/docs/golang-client-quickstart-guide)
