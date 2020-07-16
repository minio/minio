# Compression Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

MinIO server allows streaming compression to ensure efficient disk space usage. Compression happens inflight, i.e objects are compressed before being written to disk(s). MinIO uses [`klauspost/compress/s2`](https://github.com/klauspost/compress/tree/master/s2) streaming compression due to its stability and performance.

This algorithm is specifically optimized for machine generated content. Write throughput is typically at least 300MB/s per CPU core. Decompression speed is typically at least 1GB/s.
This means that in cases where raw IO is below these numbers compression will not only reduce disk usage but also help increase system throughput.
Typically enabling compression on spinning disk systems will increase speed when the content can be compressed.

## Get Started

### 1. Prerequisites

Install MinIO - [MinIO Quickstart Guide](https://docs.min.io/docs/minio-quickstart-guide).

### 2. Run MinIO with compression

Compression can be enabled by updating the `compress` config settings for MinIO server config. Config `compress` settings take extensions and mime-types to be compressed.

```
$ mc admin config get myminio compression
compression extensions=".txt,.log,.csv,.json,.tar,.xml,.bin" mime_types="text/*,application/json,application/xml"
```

Default config includes most common highly compressible content extensions and mime-types.

```
$ mc admin config set myminio compression extensions=".pdf" mime_types="application/pdf"
```

To show help on setting compression config values.
```
~ mc admin config set myminio compression
```

To enable compression for all content, with default extensions and mime-types.
```
~ mc admin config set myminio compression enable="on"
```

The compression settings may also be set through environment variables. When set, environment variables override the defined `compress` config settings in the server config.

```bash
export MINIO_COMPRESS="on"
export MINIO_COMPRESS_EXTENSIONS=".pdf,.doc"
export MINIO_COMPRESS_MIME_TYPES="application/pdf"
```

### 3. Note

- Already compressed objects are not fit for compression since they do not have compressible patterns. Such objects do not produce efficient [`LZ compression`](https://en.wikipedia.org/wiki/LZ77_and_LZ78) which is a fitness factor for a lossless data compression. Below is a list of common files and content-types which are not suitable for compression.

    - Extensions

      | `gz` | (GZIP)
      | `bz2` | (BZIP2)
      | `rar` | (WinRAR)
      | `zip` | (ZIP)
      | `7z` | (7-Zip)
      | `xz` | (LZMA)
      | `mp4` | (MP4)
      | `mkv` | (MKV media)
      | `mov` | (MOV)

    - Content-Types

      | `video/*` |
      | `audio/*` |
      | `application/zip` |
      | `application/x-gzip` |
      | `application/zip` |
      | `application/x-bz2` |
      | `application/x-compress` |
      | `application/x-xz` |

All files with these extensions and mime types are excluded from compression, even if compression is enabled for all types.

- MinIO does not support encryption with compression because compression and encryption together potentially enables room for side channel attacks like [`CRIME and BREACH`](https://blog.minio.io/c-e-compression-encryption-cb6b7f04a369)

- MinIO does not support compression for Gateway (Azure/GCS/NAS) implementations.

## To test the setup

To test this setup, practice put calls to the server using `mc` and use `mc ls` on the data directory to view the size of the object.

## Explore Further

- [Use `mc` with MinIO Server](https://docs.min.io/docs/minio-client-quickstart-guide)
- [Use `aws-cli` with MinIO Server](https://docs.min.io/docs/aws-cli-with-minio)
- [Use `s3cmd` with MinIO Server](https://docs.min.io/docs/s3cmd-with-minio)
- [Use `minio-go` SDK with MinIO Server](https://docs.min.io/docs/golang-client-quickstart-guide)
- [The MinIO documentation website](https://docs.min.io)
