# Distributed MinIO Quickstart Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

MinIO in distributed mode lets you pool multiple drives (even on different machines) into a single object storage server. As drives are distributed across several nodes, distributed MinIO can withstand multiple node failures and yet ensure full data protection.

## Why distributed MinIO?

MinIO in distributed mode can help you setup a highly-available storage system with a single object storage deployment. With distributed MinIO, you can optimally use storage devices, irrespective of their location in a network.

### Data protection

Distributed MinIO provides protection against multiple node/drive failures and [bit rot](https://github.com/minio/minio/blob/master/docs/erasure/README.md#what-is-bit-rot-protection) using [erasure code](https://docs.min.io/docs/minio-erasure-code-quickstart-guide). As the minimum disks required for distributed MinIO is 4 (same as minimum disks required for erasure coding), erasure code automatically kicks in as you launch distributed MinIO.

### High availability

A stand-alone MinIO server would go down if the server hosting the disks goes offline. In contrast, a distributed MinIO setup with _n_ disks will have your data safe as long as _n/2_ or more disks are online. You'll need a minimum of _(n/2 + 1)_ disks to create new objects.

For example, an 16-node distributed MinIO setup with 16 disks per node would continue serving files, even if up to 8 servers are offline. But, you'll need at least 9 servers online to create new objects.

You can also use [storage classes](https://github.com/minio/minio/tree/master/docs/erasure/storage-class) to set custom data and parity distribution per object.

### Consistency Guarantees

MinIO follows strict **read-after-write** and **list-after-write** consistency model for all i/o operations both in distributed and standalone modes.

# Get started

If you're aware of stand-alone MinIO set up, the process remains largely the same. MinIO server automatically switches to stand-alone or distributed mode, depending on the command line parameters.

## 1. Prerequisites

Install MinIO - [MinIO Quickstart Guide](https://docs.min.io/docs/minio-quickstart-guide).

## 2. Run distributed MinIO

To start a distributed MinIO instance, you just need to pass drive locations as parameters to the minio server command. Then, you’ll need to run the same command on all the participating nodes.

__NOTE:__

- All the nodes running distributed MinIO need to have same access key and secret key for the nodes to connect. To achieve this, it is __recommended__ to export access key and secret key as environment variables, `MINIO_ACCESS_KEY` and `MINIO_SECRET_KEY`, on all the nodes before executing MinIO server command.
- __MinIO creates erasure-coding sets of 4, 6, 8, 10, 12, 14 or 16 drives.  The number of drives you provide must be a multiple of one of those numbers.__
- __MinIO chooses the largest EC set size which divides into the total number of drives given.  For example, 8 drives will be used as a single EC set of size 8, not two sets of size 4.__
- __Each object is written to a single EC set, and therefore is spread over no more than 16 drives.__
- __All the nodes running distributed MinIO setup are recommended to be homogeneous, i.e. same operating system, same number of disks and same network interconnects.__
- MinIO distributed mode requires __fresh directories__. If required, the drives can be shared with other applications. You can do this by using a sub-directory exclusive to MinIO. For example, if you have mounted your volume under `/export`, pass `/export/data` as arguments to MinIO server.
- The IP addresses and drive paths below are for demonstration purposes only, you need to replace these with the actual IP addresses and drive paths/folders.
- Servers running distributed MinIO instances should be less than 15 minutes apart. You can enable [NTP](http://www.ntp.org/) service as a best practice to ensure same times across servers.
- `MINIO_DOMAIN` environment variable should be defined and exported for bucket DNS style support.
- Running Distributed MinIO on __Windows__ operating system is experimental. Please proceed with caution.

Example 1: Start distributed MinIO instance on 32 nodes with 32 drives each mounted at `/export1` to `/export32` (pictured below), by running this command on all the 32 nodes:

![Distributed MinIO, 32 nodes with 32 drives each](https://github.com/minio/minio/blob/master/docs/screenshots/Architecture-diagram_distributed_32.png?raw=true)

#### GNU/Linux and macOS

```sh
export MINIO_ACCESS_KEY=<ACCESS_KEY>
export MINIO_SECRET_KEY=<SECRET_KEY>
minio server http://host{1...32}/export{1...32}
```

> __NOTE:__ `{1...n}` shown have 3 dots! Using only 2 dots `{1..32}` will be interpreted by your shell and won't be passed to MinIO server, affecting the erasure coding order, which may impact performance and high availability. __Always use ellipses syntax `{1...n}` (3 dots!) for optimal erasure-code distribution__

#### Expanding existing distributed setup
MinIO supports expanding distributed erasure coded clusters by specifying new set of clusters on the command-line as shown below:

```sh
export MINIO_ACCESS_KEY=<ACCESS_KEY>
export MINIO_SECRET_KEY=<SECRET_KEY>
minio server http://host{1...32}/export{1...32} http://host{33...64}/export{1...32}
```

Now the server has expanded storage of *1024* more disks in total of *2048* disks, new object upload requests automatically start using the least used cluster. This expansion strategy works endlessly, so you can perpetually expand your clusters as needed.  When you restart, it is immediate and non-disruptive to the applications. Each group of servers in the command-line is called a zone. There are 2 zones in this example. New objects are placed in zones in proportion to the amount of free space in each zone. Within each zone, the location of the erasure-set of drives is determined based on a deterministic hashing algorithm.

> __NOTE:__ __Each zone you add must have the same erasure coding set size as the original zone, so the same data redundancy SLA is maintained.__
> For example, if your first zone was 8 drives, you could add further zones of 16, 32 or 1024 drives each. All you have to make sure is deployment SLA is multiples of original zone i.e 8.

## 3. Test your setup
To test this setup, access the MinIO server via browser or [`mc`](https://docs.min.io/docs/minio-client-quickstart-guide).

## Explore Further
- [MinIO Erasure Code QuickStart Guide](https://docs.min.io/docs/minio-erasure-code-quickstart-guide)
- [Use `mc` with MinIO Server](https://docs.min.io/docs/minio-client-quickstart-guide)
- [Use `aws-cli` with MinIO Server](https://docs.min.io/docs/aws-cli-with-minio)
- [Use `s3cmd` with MinIO Server](https://docs.min.io/docs/s3cmd-with-minio)
- [Use `minio-go` SDK with MinIO Server](https://docs.min.io/docs/golang-client-quickstart-guide)
- [The MinIO documentation website](https://docs.min.io)
