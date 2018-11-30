# Distributed Minio Quickstart Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

When running Minio in distributed mode (_distributed Minio_), multiple drives from different machines can be pooled into a single namespace. As drives are distributed across several nodes, distributed Minio ensures full data protection when multiple node failures occur.

## Why Distributed Minio?

Running Minio in distributed mode allows for the deployment of a highly-available, object storage system, enabling optimal use of storage devices regardless of their location on a network.

**Note:** Each cluster must reside in the same network, but multiple clusters from multiple networks can be federated. For more information about federation see the [Federation Quickstart Guide](https://docs.minio.io/docs/minio-federation-quickstart-guide.html).

### Data Protection

Distributed Minio provides protection against multiple node/drive failures and [bit rot](https://docs.minio.io/docs/minio-erasure-code-quickstart-guide.html#whatisrot) using [erasure code](https://docs.minio.io/docs/minio-erasure-code-quickstart-guide). Since distributed Minio and erasure code both require a minimum of four drives, erasure code is automatically used when distributed Minio is launched.

### High Availability

A standalone server running Minio Server will go down if the drives go offline. In contrast, a distributed Minio configuration with N drives will keep data safe as long as N/2 or more drives are online. This requires a minimum of (N/2 + 1) [Quorum](https://github.com/minio/dsync#lock-process) drives to create new objects.

For example, an 8-node distributed Minio configuration with one drive per node will continue serving files even if as many as 4 drives are offline. This configuration requires at least 5 drives online to create new objects.

### Limits

Distributed Minio requires at least 2 nodes and supports up to 32 nodes. There are no limits on the number of drives on each server, but the total number of drives in the deployment should be even. For a multi-tenant configuration, orchestrate multiple Minio instances using a tool like Kubernetes.

**Note:** The number of nodes in a Distributed Minio configuration must be within the limits. For example, a configuration can have 2 nodes with 4 drives each, 4 nodes with 4 drives each, 8 nodes with 2 drives each, 32 servers with 24 drives each, etc.

The [storage classes](https://github.com/minio/minio/tree/master/docs/erasure/storage-class) can be used to set custom data and parity distribution across all of the drives.

### Consistency Guarantees

Minio follows a strict **read-after-write** consistency model for all I/O operations in distributed and standalone modes.

## Set up Minio Server

This section describes how to set up and run distributed Minio.

- [Install Minio Server](#install-minio-server) 
- [Run Distributed Minio](#run-distributed-minio) 
- [Test the Configuration](#test-the-configuration)

The process to set up distributed Minio is largely the same as that for a standalone Minio configuration. Depending on the command line parameters, Minio Server automatically switches to stand-alone or distributed mode.

### <a name="install-minio-server"></a>1. Install Minio Server

Install Minio Server using the instructions in the [Minio Quickstart Guide](https://docs.minio.io/docs/minio-quickstart-guide).

### <a name="run-distributed-minio"></a>2. Run Distributed Minio

To start a distributed Minio instance:
1. Pass the drive locations as parameters to the `minio server` command.
2. Re-run the same command on all of the participating nodes.

**Note:**
* All nodes running distributed Minio must have the same access key and secret key for the nodes to connect. This requires that the access key and secret key be exported as the following environment variables on all nodes before executing the `minio server` command: `MINIO_ACCESS_KEY` and `MINIO_SECRET_KEY`.
* All nodes running distributed Minio must be in a homogeneous environment (meaning they have the same operating system, number of drives, and interconnects).
* If the domain must be set, the `MINIO_DOMAIN` environment variable must be defined and exported.
* Minio distributed mode requires directories that are exclusive to Minio. For example, if a volume is mounted under `/export`, pass `/export/data` as an argument to the `minio server` command.
* The IP addresses and drive paths below are intended for demonstration purposes only, and must be replaced with the actual IP addresses and drive paths/folders.
* The time on the servers running distributed Minio instances should within three seconds of each other. Use [NTP](http://www.ntp.org/) to ensure consistent times across servers.
* Running Distributed Minio on Windows is experimental as of now. Please proceed with caution.

#### Example

This example demonstrates how to start distributed Minio instances on eight nodes, with one drive each mounted at `/export1`: 

![Distributed Minio, 8 nodes with 1 drive each](https://github.com/minio/minio/blob/master/docs/screenshots/Architecture-diagram_distributed_8.jpg?raw=true)

Run the following command on all eight nodes:

```sh
export MINIO_ACCESS_KEY=<ACCESS_KEY>
export MINIO_SECRET_KEY=<SECRET_KEY>
minio server http://192.168.1.1{1...8}/export1
```

**Note:** Arguments such as `{1...n}` have three dots. Using only two dots (e.g. `{1..4}`) will be interpreted by the shell and won't be passed to Minio Server. This affects the erasure coding order, possibly impacting performance and high availability. Always use `{1...n}` (three dots) to use Minio Server erasure code on data optimally.

### <a name="test-the-configuration"></a>3. Test the Configuration
Test the configuration using `mc` as described in the [Minio Client Quickstart Guide](https://docs.minio.io/docs/minio-client-quickstart-guide).

## Explore Further
- [Minio Large Bucket Support Guide](https://docs.minio.io/docs/minio-large-bucket-support-quickstart-guide)
- [Minio Erasure Code Quickstart Guide](https://docs.minio.io/docs/minio-erasure-code-quickstart-guide)
- [Use `mc` with Minio Server](https://docs.minio.io/docs/minio-client-quickstart-guide)
- [Use `aws-cli` with Minio Server](https://docs.minio.io/docs/aws-cli-with-minio)
- [Use `s3cmd` with Minio Server](https://docs.minio.io/docs/s3cmd-with-minio)
- [Use `minio-go` SDK with Minio Server](https://docs.minio.io/docs/golang-client-quickstart-guide)
- [The Minio documentation website](https://docs.minio.io)
