## Introduction
This is a fork base on  [minio RELEASE.2022-03-05T06-32-39Z version](https://github.com/minio/minio/tree/RELEASE.2022-03-05T06-32-39Z). We implemented the feature of [juicefs]( https://github.com/juicedata/juicefs ) as one of its gateway implementations. This version supports using [juicefs]( https://github.com/juicedata/juicefs ) as the backend while using the full functionality native to minio gateway, such as [multi-user management]( https://docs.min.io/docs/minio-multi-user-quickstart-guide.html ).

### Compile 
```shell
$ git clone -b gateway git@github.com:juicedata/minio.git && cd minio

# will generate a binary named minio
$ make build
```

### Usage
The usage of this version of minio gateway is exactly the same as the usage of native minio gateway. For the usage of native functions, please refer to minio's [gateway documentation]( https://docs.min.io/docs/minio-gateway-for-s3.html ), and the configuration of juicefs itself can be passed in through the command line. You can use `minio gateway juicefs -h` to see all currently supported parameters.

```shell
$ minio gateway juicefs -h
NAME:
  minio gateway juicefs - JuiceFS Distributed File System (JuiceFS)

USAGE:
  minio gateway juicefs [FLAGS] Meta-URL

FLAGS:
  --verbose                    enable debug log
  --trace                      enable trace log
  --no-agent                   disable pprof (:6060) and gops (:6070) agent
  --no-color                   disable colors
  --bucket value               customized endpoint to access object store
  --get-timeout value          the max number of seconds to download an object (default: 60)
  --put-timeout value          the max number of seconds to upload an object (default: 60)
  --io-retries value           number of retries after network failure (default: 10)
  --max-uploads value          number of connections to upload (default: 20)
  --max-deletes value          number of threads to delete objects (default: 2)
  --buffer-size value          total read/write buffering in MB (default: 300)
  --upload-limit value         bandwidth limit for upload in Mbps (default: 0)
  --download-limit value       bandwidth limit for download in Mbps (default: 0)
  --prefetch value             prefetch N blocks in parallel (default: 1)
  --writeback                  upload objects in background
  --upload-delay value         delayed duration for uploading objects ("s", "m", "h") (default: 0s)
  --cache-dir value            directory paths of local cache, use colon to separate multiple paths (default: "/Users/duanjiaxing/.juicefs/cache")
  --cache-size value           size of cached objects in MiB (default: 102400)
  --free-space-ratio value     min free space (ratio) (default: 0.1)
  --cache-partial-only         cache only random/small read
  --backup-meta value          interval to automatically backup metadata in the object storage (0 means disable backup) (default: 1h0m0s)
  --heartbeat value            interval to send heartbeat; it's recommended that all clients use the same heartbeat value (default: 12s)
  --read-only                  allow lookup/read operations only
  --no-bgjob                   disable background jobs (clean-up, backup, etc.)
  --open-cache value           open files cache timeout in seconds (0 means disable this feature) (default: 0)
  --subdir value               mount a sub-directory as root
  --attr-cache value           attributes cache timeout in seconds (default: 1)
  --entry-cache value          file entry cache timeout in seconds (default: 0)
  --dir-entry-cache value      dir entry cache timeout in seconds (default: 1)
  --access-log value           path for JuiceFS access log
  --no-banner                  disable MinIO startup information
  --multi-buckets              use top level of directories as buckets
  --keep-etag                  keep the ETag for uploaded objects
  --umask value                umask for new file in octal (default: "022")
  --metrics value              address to export metrics (default: "127.0.0.1:9567")
  --consul value               consul address to register (default: "127.0.0.1:8500")
  --no-usage-report            do not send usage report
  --address value              bind to a specific ADDRESS:PORT, ADDRESS can be an IP or hostname (default: ":9000") [$MINIO_ADDRESS]
  --listeners value            bind N number of listeners per ADDRESS:PORT (default: 1) [$MINIO_LISTENERS]
  --console-address value      bind to a specific ADDRESS:PORT for embedded Console UI, ADDRESS can be an IP or hostname [$MINIO_CONSOLE_ADDRESS]
  --certs-dir value, -S value  path to certs directory (default: "/Users/duanjiaxing/.minio/certs")
  --quiet                      disable startup and info messages
  --anonymous                  hide sensitive information from logging
  --json                       output logs in JSON format
  --help, -h                   show help

Meta-URL:
  JuiceFS meta engine address

EXAMPLES:
  1. Start minio gateway server for JuiceFS backend
     $ export MINIO_ROOT_USER=accesskey
     $ export MINIO_ROOT_PASSWORD=secretkey
     $ minio gateway juicefs redis://localhost:6379/1

  2. Start minio gateway server for JuiceFS with edge caching enabled
     $ export MINIO_ROOT_USER=accesskey
     $ export MINIO_ROOT_PASSWORD=secretkey
     $ export MINIO_CACHE_DRIVES="/mnt/drive1,/mnt/drive2,/mnt/drive3,/mnt/drive4"
     $ export MINIO_CACHE_EXCLUDE="bucket1/*,*.png"
     $ export MINIO_CACHE_QUOTA=90
     $ export MINIO_CACHE_AFTER=3
     $ export MINIO_CACHE_WATERMARK_LOW=75
     $ export MINIO_CACHE_WATERMARK_HIGH=85
     $ minio gateway juicefs redis://localhost:6379/1
```
