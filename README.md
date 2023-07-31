## Introduction

This is a fork base on [MinIO RELEASE.2022-03-05T06-32-39Z version](https://github.com/minio/minio/tree/RELEASE.2022-03-05T06-32-39Z). We implemented the feature of [JuiceFS](https://github.com/juicedata/juicefs) as one of its gateway backend. This version supports full functionality of MinIO gateway such as [multi-user management](https://docs.min.io/docs/minio-multi-user-quickstart-guide.html) while using JuiceFS as a backend.

## branch info

| branch      | description                                                                   |
|-------------|-------------------------------------------------------------------------------|
| main        | base on [JuiceFS main branch](https://github.com/juicedata/juicefs/tree/main) |
| gateway     | base on [JuiceFS v1.0](https://github.com/juicedata/juicefs/tree/release-1.0) |
| Gateway-1.1 | base on [JuiceFS v1.1](https://github.com/juicedata/juicefs/tree/release-1.1) |

## Compile

> **Note**: The [gateway branch](https://github.com/juicedata/minio/tree/gateway) relies on a newer [release version](https://github.com/juicedata/juicefs/releases) of JuiceFS. Please refer to the [`go.mod`](go.mod) file for the specific JuiceFS version. If you want to use JuiceFS of the main line branch, please change the [main branch](https://github.com/juicedata/minio/tree/main) of this repository.

```shell
$ git clone -b gateway git@github.com:juicedata/minio.git && cd minio

# Will generate a binary named minio
$ make build

# If you need juicefs to support ceph RADOS as an object store, you need to install librados-dev first and then run:
$ make build-ceph
```

## Usage

The usage of this version of MinIO gateway is exactly the same as that of the native MinIO gateway. For the usage of native functions, please refer to MinIO's [document](https://docs.min.io/docs/minio-gateway-for-s3.html), while JuiceFS's own configuration options can be passed in via the command line. You can use `minio gateway juicefs -h` to see all currently supported options.

Similar to the S3 gateway integrated with JuiceFS, the gateway service can be started with the following command:

```shell
$ export MINIO_ROOT_USER=admin
$ export MINIO_ROOT_PASSWORD=12345678
$ ./minio gateway juicefs --console-address ':59001' redis://localhost:6379
```

The port number of the S3 gateway console is explicitly specified here as 59001. If not specified, a port will be randomly selected. According to the command line prompt, open the [http://127.0.0.1:59001](http://127.0.0.1:59001) address in the browser to access the console.

For more infomation, please refer to [https://juicefs.com/docs/community/s3_gateway](https://juicefs.com/docs/community/s3_gateway).
