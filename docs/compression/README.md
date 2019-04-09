# Compression Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

MinIO server allows streaming compression to ensure efficient disk space usage. Compression happens inflight, i.e objects are compressed before being written to disk(s). MinIO uses [`golang/snappy`](https://github.com/golang/snappy) streaming compression due to its stability and performance.

## Get Started

### 1. Prerequisites

Install MinIO - [MinIO Quickstart Guide](https://docs.min.io/docs/minio-quickstart-guide).

### 2. Run MinIO with compression

Compression can be enabled by updating the `compress` config settings for MinIO server config. Config `compress` settings take extensions and mime-types to be compressed.

```json
"compress": {
        "enabled": true,
        "extensions": [".txt",".log",".csv", ".json"],
        "mime-types": ["text/csv","text/plain","application/json"]
}
```

Since text, log, csv, json files are highly compressible, These extensions/mime-types are included by default for compression.

To update the configuration, use `mc admin config get` command to get the current configuration file for the minio cluster in json format, and save it locally.

```sh
$ mc admin config get myminio/ > /tmp/myconfig
```

After updating the compression configuration in /tmp/myconfig , use `mc admin config set` command to update the configuration for the cluster. Restart the MinIO server to put the changes into effect.

```sh
$ mc admin config set myminio < /tmp/myconfig
```

The compression settings may also be set through environment variables. When set, environment variables override the defined `compress` config settings in the server config.

```bash
export MINIO_COMPRESS="true"
export MINIO_COMPRESS_EXTENSIONS=".pdf,.doc"
export MINIO_COMPRESS_MIMETYPES="application/pdf"
```

### 3. Note

- Already compressed objects are not fit for compression since they do not have compressible patterns. Such objects do not produce efficient [`Run-length encoding (RLE)`](https://en.wikipedia.org/wiki/Run-length_encoding) which is a fitness factor for a lossless data compression. Below is a list of common files and content-types which are not suitable for compression.

    - Extensions

      | `gz` | (GZIP)
      | `bz2` | (BZIP2)
      | `rar` | (WinRAR)
      | `zip` | (ZIP)
      | `7z` | (7-Zip)

    - Content-Types

      | `video/*` |
      | `audio/*` |
      | `application/zip` |
      | `application/x-gzip` |
      | `application/zip` |
      | `application/x-compress` |
      | `application/x-spoon` |

- MinIO does not support encryption with compression because compression and encryption together enables room for side channel attacks like [`CRIME and BREACH`](https://en.wikipedia.org/wiki/CRIME)

- MinIO does not support compression for Gateway (Azure/GCS/NAS) implementations.

## To test the setup

To test this setup, practice put calls to the server using `mc` and use `mc ls` on the data directory to view the size of the object.

## Explore Further

- [Use `mc` with MinIO Server](https://docs.min.io/docs/minio-client-quickstart-guide)
- [Use `aws-cli` with MinIO Server](https://docs.min.io/docs/aws-cli-with-minio)
- [Use `s3cmd` with MinIO Server](https://docs.min.io/docs/s3cmd-with-minio)
- [Use `minio-go` SDK with MinIO Server](https://docs.min.io/docs/golang-client-quickstart-guide)
- [The MinIO documentation website](https://docs.min.io)
