# Compression Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

Minio server has compression enabled by default. This means highly compressible files ( text / csv / log / json etc ) uploaded to the server are compressed before being written to disk(s). This enables efficient disk space usage.

Minio adopts streaming compression format of `golang/snappy`. For better performance, Minio compresses only `compressible objects`. These are objects which will produce a better compression experience. For instance, already compressed objects like `.zip,.bz2` etc, are not fit for compression since they do not have compressible patterns in them.


Compression is disabled for

### 1. Non-Compressible objects

    These objects does not produce efficient `Run-length encoding (RLE)` which is a fitness factor for a lossless data compression.

    - Objects having the following extensions

      | `gz` | (GZIP)
      | `bz2` | (BZIP2)
      | `rar` | (WinRAR)
      | `zip` | (ZIP)
      | `7z` | (7-Zip)

    - Objects having the following content-types

      | `video/*` |
      | `audio/*` |
      | `application/zip` |
      | `application/x-gzip` |
      | `application/zip` |
      | `application/x-compress` |
      | `application/x-spoon` |
      
### 2. Encryption enabled requests

    Minio does not support encryption with compression because compression and encryption together enables room for side channel attacks.

### 3. Gateway Implementations

    Minio does not support compression for Azure/GCS/NAS implementations.


## Enable compression with defined extensions/content-types

Compression can be disabled by updating the `compress` config settings for Minio server config. Compression settings take extensions and mime-types which needs to be compressed.

```json
"compress": {
		"enabled": true,
		"extensions": [".txt",".log",".csv"],
		"mime-types": ["text/csv","text/plain"]
	}
```

Compression can be disabled by simply setting the `enable` to `false`. Since Text, log, csv files are highly compressible, These extensions/mime-types are included by default for compression.

The compression settings may also be set through environment variables. When set, environment variables override the defined `compression` config settings in the server config.

```bash
export MINIO_COMPRESS_EXTENSIONS=".pdf,.doc"
export MINIO_COMPRESS_MIMETYPES="application/pdf"
```

The above example informs the server to compress any objects having `.pdf` or `.doc` as their extension.


## To test the setup

To test this setup, practice put calls to the server using `mc` and use `mc ls` on the data directory to view the size of the object. 

## Explore Further

- [Use `mc` with Minio Server](https://docs.minio.io/docs/minio-client-quickstart-guide)
- [Use `aws-cli` with Minio Server](https://docs.minio.io/docs/aws-cli-with-minio)
- [Use `s3cmd` with Minio Server](https://docs.minio.io/docs/s3cmd-with-minio)
- [Use `minio-go` SDK with Minio Server](https://docs.minio.io/docs/golang-client-quickstart-guide)
- [The Minio documentation website](https://docs.minio.io)
