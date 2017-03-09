# Minio Gateway [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

Minio can be run in the gateway mode where it will be a S3 proxy to backend cloud storage providers. Supported backends are

- Azure Blob Storage

## Run Minio Gateway for Azure Blob Storage

### Using Docker

```
docker run -p 9000:9000 --name azure-s3 \
 -e "MINIO_ACCESS_KEY=azureaccountname" \
 -e "MINIO_SECRET_KEY=azureaccountkey" \
 minio/minio gateway azure
```

### Using Binary

```
export MINIO_ACCESS_KEY=azureaccountname
export MINIO_SECRET_KEY=azureaccountkey
minio gateway azure
```

## Test using Minio Client `mc`
`mc` provides a modern alternative to UNIX commands like ls, cat, cp, mirror, diff etc. It supports filesystems and Amazon S3 compatible cloud storage services.

```
mc config host add azure http://gateway-ip:9000 azureaccountname azureaccountkey
```

List azure containers.
```
mc ls azure
[2017-02-22 01:50:43 PST]     0B ferenginar/
[2017-02-26 21:43:51 PST]     0B my-container/
[2017-02-26 22:10:11 PST]     0B test-container1/
```

## Design Considerations

- Will not support Minio browser console.
- Will not support Minio bucket notifications.
- Only support S3 API proxy.

## Explore Further
- [Use `mc` with Minio Server](https://docs.minio.io/docs/minio-client-quickstart-guide)
- [Use `aws-cli` with Minio Server](https://docs.minio.io/docs/aws-cli-with-minio)
- [Use `s3cmd` with Minio Server](https://docs.minio.io/docs/s3cmd-with-minio)
- [Use `minio-go` SDK with Minio Server](https://docs.minio.io/docs/golang-client-quickstart-guide)
- [The Minio documentation website](https://docs.minio.io)