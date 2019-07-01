# MinIO GCS 网关 [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)
MinIO GCS网关将亚马逊S3兼容的API添加到Google云存储(GCS)中。

- [运行支持GCS的MinIO网关](#run-minio-gateway-for-gcs)
- [测试使用MinIO服务器](#test-using-minio-browser)
- [测试使用MinIO客户端](#test-using-minio-client)
- [了解更多](#explore-further)

## <a name="run-minio-gateway-for-gcs"></a>1.运行支持GCS的MinIO 网关

### 1.1 为GCS创建服务帐户密钥，并获取凭据文件
1. 访问 [API控制台凭证页面](https://console.developers.google.com/project/_/apis/credentials).
2. 选择您的项目或创建一个新项目， 记下你的项目ID。
3. 在凭据页面，选择**Create credentials** 下拉项，然后选择 **Service account key**。
4. 从 **Service account**下拉项, 选择 **New service account**
5. 填写 **Service account name** 和 **Service account ID**
6. 对于 **Role**, 点击下拉项，选择 **Storage** -> **Storage Admin**(完全控制GCS资源)_
7. 点击 **Create** 按钮，下载凭据文件到你的桌面，文件名咱们就叫 `credentials.json`

**注意:** 设置 *Application Default Credentials* 的替代方案在[这里](https://developers.google.com/identity/protocols/application-default-credentials)进行了描述。

### 1.2 使用Docker运行MinIO GCS网关
```sh
docker run -p 9000:9000 --name gcs-s3 \
 -v /path/to/credentials.json:/credentials.json \
 -e "GOOGLE_APPLICATION_CREDENTIALS=/credentials.json" \
 -e "MINIO_ACCESS_KEY=minioaccountname" \
 -e "MINIO_SECRET_KEY=minioaccountkey" \
 minio/minio gateway gcs yourprojected
```

### 1.3 使用二进制运行MinIOn GCS网关
```sh
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
export MINIO_ACCESS_KEY=minioaccesskey
export MINIO_SECRET_KEY=miniosecretkey
minio gateway gcs yourprojected
```

## <a name="test-using-minio-browser"></a>2. 使用MinIO Browser验证

MinIO Gateway配有嵌入式网络对象浏览器。将您的Web浏览器指 http://127.0.0.1:9000.  确保您的服务器已成功启动。为了测试MinIO网关是否在运行，可以打开一个web浏览器，转到 http://127.0.0.1:9000，并确保显示对象浏览器。

![Screenshot](https://github.com/minio/minio/blob/master/docs/screenshots/minio-browser-gateway.png?raw=true)

## <a name="test-using-minio-client"></a>3. 验证MinIO客户端 

MinIO客户端是一个名叫 `mc` 的命令行工具,能够提供与服务器交互的类UNIX命令（例如，ls,cat,cp,mirror,diff,find等等）
它支持文件系统和Amazon S3兼容的云存储服务（AWS签名V2和V4）。

### 3.1 用MinIO客户端配置网关
使用下列的命令去配置网关：
```
mc config host add mygcs http://gateway-ip:9000 minioaccesskey miniosecretkey
```

### 3.2 列出GCS上的容器

使用下面的命令列出GCS的容器：

```
mc ls mygcs
```

与之类似的响应如下所示：

```
[2017-02-22 01:50:43 PST]     0B ferenginar/
[2017-02-26 21:43:51 PST]     0B my-container/
[2017-02-26 22:10:11 PST]     0B test-container1/
```

### 3.3 已知的限制

网关继承了以下的GCS限制:

- 桶级别只支持只读存储桶策略；所有其他变体都将返回 `API Not implemented` 错误。
- `List Multipart Uploads` 和 `List Object parts`_ 会一直返回空List。因此，客户端需要记住已经上传的parts,并使用它用于 `Complete Multipart Upload`。

其他限制：
- 存储桶通知的APIs不被支持。

## <a name="explore-further"></a>4.了解更多
- [`mc` 命令行接口](https://docs.min.io/cn/minio-client-quickstart-guide)
- [`aws` 命令行接口](https://docs.min.io/cn/aws-cli-with-minio)
- [`minio-go` Go SDK](https://docs.min.io/cn/golang-client-quickstart-guide)

