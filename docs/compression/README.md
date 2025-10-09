# Compression Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

MinIO server allows streaming compression to ensure efficient disk space usage.
Compression happens inflight, i.e objects are compressed before being written to disk(s).
MinIO uses [`klauspost/compress/s2`](https://github.com/klauspost/compress/tree/master/s2)
streaming compression due to its stability and performance.

This algorithm is specifically optimized for machine generated content.
Write throughput is typically at least 500MB/s per CPU core,
and scales with the number of available CPU cores.
Decompression speed is typically at least 1GB/s.

This means that in cases where raw IO is below these numbers
compression will not only reduce disk usage but also help increase system throughput.
Typically, enabling compression on spinning disk systems
will increase speed when the content can be compressed.

## Get Started

### 1. Prerequisites

Install MinIO - [MinIO Quickstart Guide](https://docs.min.io/community/minio-object-store/operations/deployments/baremetal-deploy-minio-on-redhat-linux.html).

### 2. Run MinIO with compression

Compression can be enabled by updating the `compress` config settings for MinIO server config.
Config `compress` settings take extensions and mime-types to be compressed.

```bash
~ mc admin config get myminio compression
compression extensions=".txt,.log,.csv,.json,.tar,.xml,.bin" mime_types="text/*,application/json,application/xml"
```

Default config includes most common highly compressible content extensions and mime-types.

```bash
~ mc admin config set myminio compression extensions=".pdf" mime_types="application/pdf"
```

To show help on setting compression config values.

```bash
~ mc admin config set myminio compression
```

To enable compression for all content, no matter the extension and content type
(except for the default excluded types) set BOTH extensions and mime types to empty.

```bash
~ mc admin config set myminio compression enable="on" extensions="" mime_types=""
```

The compression settings may also be set through environment variables.
When set, environment variables override the defined `compress` config settings in the server config.

```bash
export MINIO_COMPRESSION_ENABLE="on"
export MINIO_COMPRESSION_EXTENSIONS=".txt,.log,.csv,.json,.tar,.xml,.bin"
export MINIO_COMPRESSION_MIME_TYPES="text/*,application/json,application/xml"
```

> [!NOTE]
> To enable compression for all content when using environment variables, set either or both of the extensions and MIME types to `*` instead of an empty string:
> ```bash
> export MINIO_COMPRESSION_ENABLE="on"
> export MINIO_COMPRESSION_EXTENSIONS="*"
> export MINIO_COMPRESSION_MIME_TYPES="*"
> ```

### 3. Compression + Encryption

Combining encryption and compression is not safe in all setups.
This is particularly so if the compression ratio of your content reveals information about it.
See [CRIME TLS](https://en.wikipedia.org/wiki/CRIME) as an example of this.

Therefore, compression is disabled when encrypting by default, and must be enabled separately.

Consult our security experts on [SUBNET](https://min.io/pricing) to help you evaluate if
your setup can use this feature combination safely.

To enable compression+encryption use:

```bash
~ mc admin config set myminio compression allow_encryption=on
```

Or alternatively through the environment variable `MINIO_COMPRESSION_ALLOW_ENCRYPTION=on`.

### 4. Excluded Types

- Already compressed objects are not fit for compression since they do not have compressible patterns.
Such objects do not produce efficient [`LZ compression`](https://en.wikipedia.org/wiki/LZ77_and_LZ78)
which is a fitness factor for a lossless data compression.

Pre-compressed input typically compresses in excess of 2GiB/s per core,
so performance impact should be minimal even if precompressed data is re-compressed.
Decompressing incompressible data has no significant performance impact.

Below is a list of common files and content-types which are typically not suitable for compression.

- Extensions

 | `gz`  | (GZIP)      |
 | `bz2` | (BZIP2)     |
 | `rar` | (WinRAR)    |
 | `zip` | (ZIP)       |
 | `7z`  | (7-Zip)     |
 | `xz`  | (LZMA)      |
 | `mp4` | (MP4)       |
 | `mkv` | (MKV media) |
 | `mov` | (MOV)       |

- Content-Types

 | `video/*`                |
 | `audio/*`                |
 | `application/zip`        |
 | `application/x-gzip`     |
 | `application/zip`        |
 | `application/x-bz2`      |
 | `application/x-compress` |
 | `application/x-xz`       |

All files with these extensions and mime types are excluded from compression,
even if compression is enabled for all types.

## To test the setup

To test this setup, practice put calls to the server using `mc` and use `mc ls` on
the data directory to view the size of the object.

## Explore Further

- [Use `mc` with MinIO Server](https://docs.min.io/community/minio-object-store/reference/minio-mc.html)
- [Use `aws-cli` with MinIO Server](https://docs.min.io/community/minio-object-store/integrations/aws-cli-with-minio.html)
- [Use `minio-go` SDK with MinIO Server](https://docs.min.io/community/minio-object-store/developers/go/minio-go.html)
- [The MinIO documentation website](https://docs.min.io/community/minio-object-store/index.html)
