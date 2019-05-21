# Minio for Samsung-NKV (Network Key Value)

Minio for Samsung-NKV provides S3 API on top of Samsung-NKV Storage. Minio uses erasure coding to provide disk and node failure protection.

### Install NKV library

Download [NKV library](https://dl.minio.io/server/minio/labs/samsung/) and run:

```
tar -xzvf nkv-sdk.tgz
```

Follow the instructions in README_INSTALL.txt available in the nkv-sdk.tgz package.

### Download Linux Binary

Download [Minio for NKV](https://dl.minio.io/server/minio/labs/samsung/minio)

Source for Minio for NKV is available at: https://github.com/minio/minio/tree/nkv

## Deploy Minio in single server erasure mode

For this example, consider a server with 4 NKV disks (`/dev/nvme0n1` `/dev/nvme1n1` `/dev/nvme2n1` `/dev/nvme3n1`). The `nkv_config.json` file will look like:

```js
{
  "fm_address": "10.1.20.91",
  "contact_fm": 0,
  "nkv_transport" : 0,
  "min_container_required" : 1,
  "min_container_path_required" : 1,
  "nkv_container_path_qd" : 16384,
  "nkv_core_pinning_required" : 0,
  "nkv_app_thread_core" : 22,
  "nkv_queue_depth_monitor_required" : 0,
  "nkv_queue_depth_threshold_per_path" : 8,
  "drive_iter_support_required" : 1,
  "iter_prefix_to_filter" : "meta",
  "nkv_listing_with_cached_keys" : 1,

  "nkv_num_path_per_container_to_iterate" : 0,
  "nkv_is_on_local_kv" : 1,

  "nkv_local_mounts": [
    {
      "mount_point": "/dev/nvme0n1"
    },
    {
      "mount_point": "/dev/nvme1n1"
    },
    {
      "mount_point": "/dev/nvme2n1"
    },
    {
      "mount_point": "/dev/nvme3n1"
    }
  ]
}
```

Minio server can be started like this:
```
export LD_LIBRARY_PATH=<nkv-package>/lib
export MINIO_NKV_CONFIG=/path/to/nkv_config.json
minio server /dev/nvme{0...3}n1
```

By default MinIO server will listen on the port 9000 for incoming S3 requests.

## Deploy Minio in distributed server erasure mode

For 4 servers (`host1`, `host2`, `host3`, `host4`), each server with 4 disks, the sample nkv_config.json is shown above.

Minio server should be started like this:
```
export LD_LIBRARY_PATH=<nkv-package>/lib
export MINIO_NKV_CONFIG=/path/to/nkv_config.json
minio server http://host{1...4}/dev/nvme{0...3}n1
```

Minio will run in distributed mode on NKV storage.

## Test using MinIO Browser

MinIO Server comes with an embedded web based object browser. Point your web browser to http://host1:9000 ensure your server has started successfully.

![Screenshot](https://github.com/minio/minio/blob/master/docs/screenshots/minio-browser.png?raw=true)


## Test using Minio Client `mc`

`mc` provides a modern alternative to UNIX commands like ls, cat, cp, mirror, diff etc. It supports filesystems and Amazon S3 compatible cloud storage services. Follow the [Minio Client Quickstart](https://docs.min.io/docs/minio-client-quickstart-guide.html) Guide for further instructions.
