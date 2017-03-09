# Minio Gateway [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

Minio can be run in the gateway mode where it will be a proxy to backend cloud storage providers. Currently Minio supports Azure backend.

## Running in Gateway mode

```
$ export MINIO_ACCESS_KEY=azureaccountname
$ export MINIO_SECRET_KEY=azureaccountkey
$ minio gateway azure
```

S3 compatible clients can be used to connect to the Gateway.
