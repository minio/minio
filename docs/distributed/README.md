# Distributed MinIO Quickstart Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

MinIO in distributed mode lets you pool multiple drives (even on different machines) into a single object storage server. As drives are distributed across several nodes, distributed MinIO can withstand multiple node failures and yet ensure full data protection.

## Why distributed MinIO?

MinIO in distributed mode can help you setup a highly-available storage system with a single object storage deployment. With distributed MinIO, you can optimally use storage devices, irrespective of their location in a network.

### Data protection

Distributed MinIO provides protection against multiple node/drive failures and [bit rot](https://github.com/minio/minio/blob/master/docs/erasure/README.md#what-is-bit-rot-protection) using [erasure code](https://docs.min.io/docs/minio-erasure-code-quickstart-guide). As the minimum disks required for distributed MinIO is 4 (same as minimum disks required for erasure coding), erasure code automatically kicks in as you launch distributed MinIO.

### High availability

A stand-alone MinIO server would go down if the server hosting the disks goes offline. In contrast, a distributed MinIO setup with _n_ disks will have your data safe as long as _n/2_ or more disks are online. You'll need a minimum of _(n/2 + 1)_ [Quorum](https://github.com/minio/dsync#lock-process) disks to create new objects though.

For example, an 8-node distributed MinIO setup with 1 disk per node would continue serving files, even if up to 4 disks are offline. But, you'll need at least 5 disks online to create new objects.

### Limits

As with MinIO in stand-alone mode, distributed MinIO has a per tenant limit of minimum 2 and maximum 32 servers. There are no limits on number of disks shared across these servers. If you need a multiple tenant setup, you can easily spin up multiple MinIO instances managed by orchestration tools like Kubernetes.

Note that with distributed MinIO you can play around with the number of nodes and drives as long as the limits are adhered to. For example, you can have 2 nodes with 4 drives each, 4 nodes with 4 drives each, 8 nodes with 2 drives each, 32 servers with 24 drives each and so on.

You can also use [storage classes](https://github.com/minio/minio/tree/master/docs/erasure/storage-class) to set custom data and parity distribution across total disks.

### Consistency Guarantees

MinIO follows strict **read-after-write** consistency model for all i/o operations both in distributed and standalone modes.

# Get started

If you're aware of stand-alone MinIO set up, the process remains largely the same, as the MinIO server automatically switches to stand-alone or distributed mode, depending on the command line parameters.

## 1. Prerequisites

Install MinIO - [MinIO Quickstart Guide](https://docs.min.io/docs/minio-quickstart-guide).

## 2. Run distributed MinIO

To start a distributed MinIO instance, you just need to pass drive locations as parameters to the minio server command. Then, you’ll need to run the same command on all the participating nodes.

*Note*

- All the nodes running distributed MinIO need to have same access key and secret key for the nodes to connect. To achieve this, it is **mandatory** to export access key and secret key as environment variables, `MINIO_ACCESS_KEY` and `MINIO_SECRET_KEY`, on all the nodes before executing MinIO server command.
- All the nodes running distributed MinIO need to be in a homogeneous environment, i.e. same operating system, same number of disks and same interconnects.
- `MINIO_DOMAIN` environment variable should be defined and exported if domain is needed to be set.
- MinIO distributed mode requires fresh directories. If required, the drives can be shared with other applications. You can do this by using a sub-directory exclusive to minio. For example, if you have mounted your volume under `/export`, pass `/export/data` as arguments to MinIO server.
- The IP addresses and drive paths below are for demonstration purposes only, you need to replace these with the actual IP addresses and drive paths/folders.
- Servers running distributed MinIO instances should be less than 3 seconds apart. You can use [NTP](http://www.ntp.org/) as a best practice to ensure consistent times across servers.
- Running Distributed MinIO on Windows is experimental as of now. Please proceed with caution.

Example 1: Start distributed MinIO instance on 8 nodes with 1 disk each mounted at `/export1` (pictured below), by running this command on all the 8 nodes:
![Distributed MinIO, 8 nodes with 1 disk each](https://github.com/minio/minio/blob/master/docs/screenshots/Architecture-diagram_distributed_8.jpg?raw=true)
#### GNU/Linux and macOS

```sh
export MINIO_ACCESS_KEY=<ACCESS_KEY>
export MINIO_SECRET_KEY=<SECRET_KEY>
minio server http://192.168.1.1{1...8}/export1
```


__NOTE:__ `{1...n}` shown have 3 dots! Using only 2 dots `{1..4}` will be interpreted by your shell and won't be passed to minio server, affecting the erasure coding order, which may impact performance and high availability. __Always use `{1...n}` (3 dots!) to allow minio server to optimally erasure-code data__

## 3. Test your setup
To test this setup, access the MinIO server via browser or [`mc`](https://docs.min.io/docs/minio-client-quickstart-guide).

## Explore Further
- [MinIO Large Bucket Support Guide](https://docs.min.io/docs/minio-large-bucket-support-quickstart-guide)
- [MinIO Erasure Code QuickStart Guide](https://docs.min.io/docs/minio-erasure-code-quickstart-guide)
- [Use `mc` with MinIO Server](https://docs.min.io/docs/minio-client-quickstart-guide)
- [Use `aws-cli` with MinIO Server](https://docs.min.io/docs/aws-cli-with-minio)
- [Use `s3cmd` with MinIO Server](https://docs.min.io/docs/s3cmd-with-minio)
- [Use `minio-go` SDK with MinIO Server](https://docs.min.io/docs/golang-client-quickstart-guide)
- [The MinIO documentation website](https://docs.min.io)
