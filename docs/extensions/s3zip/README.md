# Perform S3 operations in a ZIP content[![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

## Overview

MinIO implements an S3 extension to list, stat and download files inside a ZIP file stored in any bucket. A perfect use case scenario is when you have a lot of small files archived in multiple ZIP files. Uploading them is faster than uploading small files individually. Besides, your S3 applications will be able to access to the data with little performance overhead.

The main limitation is that to update or delete content of a file inside a ZIP file the entire ZIP file must be replaced.

## How to enable S3 ZIP behavior ?

Ensure to set the following header `x-minio-extract` to `true` in your S3 requests.

## How to access to files inside a ZIP archive

Accessing to contents inside an archive can be done using regular S3 API with a modified request path. You just need to append the path of the content inside the archive to the path of the archive itself.

e.g.:
To download `2021/taxes.csv` archived in `financial.zip` and stored under a bucket named `company-data`, you can issue a GET request using the following path 'company-data/financial.zip/2021/taxes.csv`

## Contents properties

All properties except the file size are tied to the zip file. This means that modification date, headers, tags, etc. can only be set for the zip file as a whole. In similar fashion, replication will replicate the zip file as a whole and not individual files.

## Code Examples

[Using minio-go library](https://github.com/minio/minio/blob/master/docs/extensions/s3zip/examples/minio-go/main.go)
[Using AWS JS SDK v2](https://github.com/minio/minio/blob/master/docs/extensions/s3zip/examples/aws-js/main.js)
[Using boto3](https://github.com/minio/minio/blob/master/docs/extensions/s3zip/examples/boto3/main.py)

## Requirements and limits

- ListObjectsV2 can only list the most recent ZIP archive version of your object, applicable only for versioned buckets.
- ListObjectsV2 API calls must be used to list zip file content.
- Range requests for GetObject/HeadObject for individual files from zip is not supported.
- Names inside ZIP files are kept unmodified, but some may lead to invalid paths. See [Object key naming guidelines](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html) on safe names.
- This API behavior is limited for following **read** operations on files inside a zip archive:
  - `HeadObject`
  - `GetObject`
  - `ListObjectsV2`
- If the ZIP file directory isn't located within the last 100MB the file will not be parsed.
- A maximum of 100M inside a single zip is allowed. However, a reasonable limit of 100,000 files inside a single ZIP archive is recommended for best performance and memory usage trade-off.

## Content-Type

The Content-Type of the response will be determined by the extension and the following: https://pkg.go.dev/mime#TypeByExtension