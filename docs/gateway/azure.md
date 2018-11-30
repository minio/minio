# Minio Azure Gateway [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

This topic describes how to use Minio Gateway to add Amazon S3 compatibility to Microsoft Azure Blob Storage.

- [Run Minio Gateway for Microsoft Azure Blob Storage](#run-minio-gateway-for-azure) 
- [Test Using Minio Browser](#test-using-minio-browser) 
- [Test Using Minio Client](#test-using-minio-client) 
- [Known Limitations](#known-limitations) 
- [Explore Further](#explore-further)

## <a name="run-minio-gateway-for-azure"></a> 1. Run Minio Gateway for Microsoft Azure Blob Storage

### 1.1 Run Minio Gateway Using Docker

```sh
docker run -p 9000:9000 --name azure-s3 \
 -e "MINIO_ACCESS_KEY=azurestorageaccountname" \
 -e "MINIO_SECRET_KEY=azurestorageaccountkey" \
 minio/minio gateway azure
```

### 1.2 Run Minio Gateway Using the Minio Binary

```sh
export MINIO_ACCESS_KEY=azurestorageaccountname
export MINIO_SECRET_KEY=azurestorageaccountkey
minio gateway azure
```

## <a name="test-using-minio-browser"></a> 2. Test Using Minio Browser

Minio Gateway comes with an embedded web-based object browser that outputs content to http://127.0.0.1:9000. To test that Minio Gateway is running, open a web browser, navigate to http://127.0.0.1:9000, and ensure that the object browser is displayed.

![Screenshot](https://github.com/minio/minio/blob/master/docs/screenshots/minio-browser-gateway.png?raw=true)

## <a name="test-using-minio-client"></a> 3. Test Using Minio Client

Minio Client is a command-line tool called `mc` that provides UNIX-like commands for interacting with the server  (e.g. `ls`, `cat`, `cp`, `mirror`, `diff`, `find`, etc.). 
`mc` supports file systems and Amazon S3-compatible cloud storage services (AWS Signature v2 and v4).

Test `mc` with the gateways using the instructions in the [Minio Client Quickstart Guide](https://docs.minio.io/docs/minio-client-quickstart-guide).

### 3.1 Configure the Gateway Using Minio Client

Use the following command to configure the gateway:

```sh
mc config host add myazure http://gateway-ip:9000 azurestorageaccountname azurestorageaccountkey
```

### 3.2 List the Containers on Microsoft Azure

Use the following command to list the containers on Microsoft Azure:

```sh
mc ls myazure
```

A response similar to this one should be displayed:

```
[2017-02-22 01:50:43 PST]     0B ferenginar/
[2017-02-26 21:43:51 PST]     0B my-container/
[2017-02-26 22:10:11 PST]     0B test-container1/
```

## <a name="known-limitations"></a>4. Known Limitations

Minio Gateway has the following limitations when used with Azure:

- It only supports read-only bucket policies at the bucket level; all other variations will return `API Not implemented`.
- Bucket names cannot contain the period (".") character.
- Non-empty buckets are removed when `DeleteBucket()` is invoked.
- The `List Multipart Uploads` command always returns an empty list.

Other limitations:

- Bucket notification APIs are not supported.

## <a name="explore-further"></a>5. Explore Further

- [`mc` command-line interface](https://docs.minio.io/docs/minio-client-quickstart-guide)
- [`aws` command-line interface](https://docs.minio.io/docs/aws-cli-with-minio)
- [`minio-go` Go SDK](https://docs.minio.io/docs/golang-client-quickstart-guide)
