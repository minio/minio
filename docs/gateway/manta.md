# Minio Manta Gateway [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)
Minio Gateway adds Amazon S3 compatibility to Manta Object Storage.

## Run Minio Gateway for Manta Object Storage
### Using Docker
```
docker run -p 9000:9000 --name manta-s3 \
 -e "MINIO_ACCESS_KEY=joyentaccountname" \
 -e "MINIO_SECRET_KEY=joyentkeyid" \
 -e "MANTA_KEY_MATERIAL=~/.ssh/id_rsa" \
 -e "MANTA_SUBUSER=devuser"
 minio/minio gateway manta
```

### Using Binary
```
export MINIO_ACCESS_KEY=joyentaccountname
export MINIO_SECRET_KEY=joyentkeyid
export MANTA_KEY_MATERIAL=~/.ssh/id_rsa
export MANTA_SUBUSER=devuser
minio gateway manta
```
## Test using Minio Browser
Minio Gateway comes with an embedded web based object browser. Point your web browser to http://127.0.0.1:9000 to ensure that your server has started successfully.

![Screenshot](https://github.com/minio/minio/blob/master/docs/screenshots/minio-browser-gateway.png?raw=true)
## Test using Minio Client `mc`
`mc` provides a modern alternative to UNIX commands such as ls, cat, cp, mirror, diff etc. It supports filesystems and Amazon S3 compatible cloud storage services.

### Configure `mc`
```
export MINIO_KEY_MATERIAL=~/.ssh/id_rsa
mc config host add mymanta http://gateway-ip:9000 joyentaccountname joyentkeyid
```

### List containers on Manta Object Storage
```
mc ls mymanta
[2017-02-22 01:50:43 PST]     0B ferenginar/
[2017-02-26 21:43:51 PST]     0B my-container/
[2017-02-26 22:10:11 PST]     0B test-container1/
```

### Known limitations
Gateway inherits the following Manta limitations:

- No support for MultiPartUpload.
- No support for bucket policies.

Other limitations:

- Bucket notification APIs are not supported.

## Explore Further
- [`mc` command-line interface](https://docs.minio.io/docs/minio-client-quickstart-guide)
- [`aws` command-line interface](https://docs.minio.io/docs/aws-cli-with-minio)
- [`minio-go` Go SDK](https://docs.minio.io/docs/golang-client-quickstart-guide)
