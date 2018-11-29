# Minio NAS Gateway [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

This topic describes how to use Minio Gateway to add Amazon S3 compatibility to NAS storage.

- [Run Minio Gateway for NAS Storage](#run-minio-gateway-for-nas-storage) 
- [Test Using Minio Browser](#test-using-minio-browser) 
- [Test Using Minio Client](#test-using-minio-client) 
- [Explore Further](#explore-further)

## <a name="run-minio-gateway-for-nas-storage"></a>1. Run Minio Gateway for NAS Storage

### 1.1 Run Minio Gateway Using Docker

```sh
docker run -p 9000:9000 --name nas-s3 \
 -e "MINIO_ACCESS_KEY=minio" \
 -e "MINIO_SECRET_KEY=minio123" \
 minio/minio gateway nas /shared/nasvol
```

### 1.2 Run Minio Gateway Using the Minio Binary

```sh
export MINIO_ACCESS_KEY=minioaccesskey
export MINIO_SECRET_KEY=miniosecretkey
minio gateway nas /shared/nasvol
```

**Note:** Multiple Minio instances can be run on the same shared NAS volume as a distributed object gateway.


## <a name="test-using-minio-browser"></a>2. Test Using Minio Browser

Minio Gateway comes with an embedded web-based object browser that outputs content to http://127.0.0.1:9000. To test that Minio Gateway is running, open a web browser, navigate to http://127.0.0.1:9000, and ensure that the object browser is displayed.

![Screenshot](https://raw.githubusercontent.com/minio/minio/master/docs/screenshots/minio-browser-gateway.png)

## <a name="test-using-minio-client"></a>3. Test Using Minio Client

Minio Client is a command-line tool called `mc` that provides UNIX-like commands for interacting with the server  (e.g. `ls`, `cat`, `cp`, `mirror`, `diff`, `find`, etc.). 
`mc` supports file systems and Amazon S3-compatible cloud storage services (AWS Signature v2 and v4).

### 3.1 Configure the Gateway Using Minio Client

Use the following command to configure the gateway:

```sh
mc config host add mynas http://gateway-ip:9000 access_key secret_key
```

### 3.2 List the Buckets on NAS

Use the following command to list the containers on NAS:

```sh
mc ls mynas
[2017-02-22 01:50:43 PST]     0B ferenginar/
[2017-02-26 21:43:51 PST]     0B my-bucket/
[2017-02-26 22:10:11 PST]     0B test-bucket1/
```

## <a name="explore-further"></a>4. Explore Further
- [`mc` command-line interface](https://docs.minio.io/docs/minio-client-quickstart-guide)
- [`aws` command-line interface](https://docs.minio.io/docs/aws-cli-with-minio)
- [`minio-go` Go SDK](https://docs.minio.io/docs/golang-client-quickstart-guide)
