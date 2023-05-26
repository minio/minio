# Fan-Out Uploads [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

## Overview

MinIO implements an S3 extension to perform multiple concurrent fan-out upload operations. A perfect use case scenario for performing fan-out operations of incoming TSB (Time Shift Buffer's). TSBs are a method of facilitating time-shifted playback of television signaling, and media content.

MinIO implements an S3 extension to the [PostUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPOST.html) where in a special fan-out list is sent along with the TSB's for MinIO make multiple uploads from a single source stream. Optionally supports custom metadata, tags and other retention settings. All objects are also readable independently once upload is completed via the regular S3 [GetObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html) API.

## How to enable Fan-Out Uploads ?

Fan-Out uploads are automatically enabled if `x-minio-fanout-list` form-field is provided with the PostUpload API, to keep things simple higher level APIs are provided in our SDKs for example in `minio-go` SDK:

```
PutObjectFanOut(ctx context.Context, bucket string, fanOutContent io.Reader, fanOutReq minio.PutObjectFanOutRequest) ([]minio.PutObjectFanOutResponse, error)
```



