# Minio Azure Gateway [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)
Minio Gateway adds Amazon S3 compatibility to Microsoft Azure Blob Storage.

## Run Minio Gateway for Microsoft Azure Blob Storage
### Using Docker
```
docker run -p 9000:9000 --name azure-s3 \
 -e "MINIO_ACCESS_KEY=azurestorageaccountname" \
 -e "MINIO_SECRET_KEY=azurestorageaccountkey" \
 minio/minio gateway azure
```

### Using Binary
```
export MINIO_ACCESS_KEY=azureaccountname
export MINIO_SECRET_KEY=azureaccountkey
minio gateway azure
```
## Test using Minio Browser
Minio Gateway comes with an embedded web based object browser. Point your web browser to http://127.0.0.1:9000 to ensure that your server has started successfully.

![Screenshot](https://github.com/minio/minio/blob/master/docs/screenshots/minio-browser-gateway.png?raw=true)
## Test using Minio Client `mc`
`mc` provides a modern alternative to UNIX commands such as ls, cat, cp, mirror, diff etc. It supports filesystems and Amazon S3 compatible cloud storage services.

### Configure `mc`
```
mc config host add myazure http://gateway-ip:9000 azureaccountname azureaccountkey
```

### List containers on Microsoft Azure
```
mc ls myazure
[2017-02-22 01:50:43 PST]     0B ferenginar/
[2017-02-26 21:43:51 PST]     0B my-container/
[2017-02-26 22:10:11 PST]     0B test-container1/
```

### Known limitations
Gateway inherits the following Azure limitations:

- Only read-only bucket policy supported at bucket level, all other variations will return API Notimplemented error.
- Bucket names with "." in the bucket name are not supported.
- Non-empty buckets get removed on a DeleteBucket() call.
- _List Multipart Uploads_ and _List Object parts_ always returns empty list. i.e Client will need to remember all the parts that it has uploaded and use it for _Complete Multipart Upload_

Other limitations:

- Bucket notification APIs are not supported.

## Explore Further
- [`mc` command-line interface](https://docs.minio.io/docs/minio-client-quickstart-guide)
- [`aws` command-line interface](https://docs.minio.io/docs/aws-cli-with-minio)
- [`minfs` filesystem interface](http://docs.minio.io/docs/minfs-quickstart-guide)
- [`minio-go` Go SDK](https://docs.minio.io/docs/golang-client-quickstart-guide)
