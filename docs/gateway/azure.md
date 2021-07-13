# MinIO Azure Gateway [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)
MinIO Gateway adds Amazon S3 compatibility to Microsoft Azure Blob Storage.

## Run MinIO Gateway for Microsoft Azure Blob Storage
### Using Docker
```
podman run \
 -p 9000:9000 \
 -p 9001:9001 \
 --name azure-s3 \
 -e "MINIO_ROOT_USER=azurestorageaccountname" \
 -e "MINIO_ROOT_PASSWORD=azurestorageaccountkey" \
 minio/minio gateway azure --console-address ":9001"
```

### Using Binary
```
export MINIO_ROOT_USER=azureaccountname
export MINIO_ROOT_PASSWORD=azureaccountkey
minio gateway azure
```
## Test using MinIO Console
MinIO Gateway comes with an embedded web based object browser. Point your web browser to http://127.0.0.1:9000 to ensure that your server has started successfully.

| Dashboard                                                                                   | Creating a bucket                                                                           |
| -------------                                                                               | -------------                                                                               |
| ![Dashboard](https://github.com/minio/minio/blob/master/docs/screenshots/pic1.png?raw=true) | ![Dashboard](https://github.com/minio/minio/blob/master/docs/screenshots/pic2.png?raw=true) |

## Test using MinIO Client `mc`
`mc` provides a modern alternative to UNIX commands such as ls, cat, cp, mirror, diff etc. It supports filesystems and Amazon S3 compatible cloud storage services.

### Configure `mc`
```
mc alias set myazure http://gateway-ip:9000 azureaccountname azureaccountkey
```

### List containers on Microsoft Azure
```
mc ls myazure
[2017-02-22 01:50:43 PST]     0B ferenginar/
[2017-02-26 21:43:51 PST]     0B my-container/
[2017-02-26 22:10:11 PST]     0B test-container1/
```

### Use custom access/secret keys

If you do not want to share the credentials of the Azure blob storage with your users/applications, you can set the original credentials in the shell environment using `AZURE_STORAGE_ACCOUNT` and `AZURE_STORAGE_KEY` variables and assign different access/secret keys to `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD`.

### Known limitations
Gateway inherits the following Azure limitations:

- Only read-only bucket policy supported at bucket level, all other variations will return API Notimplemented error.
- Bucket names with "." in the bucket name are not supported.
- Non-empty buckets get removed on a DeleteBucket() call.
- _List Multipart Uploads_ always returns empty list.

Other limitations:

- Bucket notification APIs are not supported.

## Explore Further
- [`mc` command-line interface](https://docs.min.io/docs/minio-client-quickstart-guide)
- [`aws` command-line interface](https://docs.min.io/docs/aws-cli-with-minio)
- [`minio-go` Go SDK](https://docs.min.io/docs/golang-client-quickstart-guide)
