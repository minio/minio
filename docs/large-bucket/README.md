# Large Bucket Support Quickstart Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

Large-bucket support allows a single Minio bucket to expand over multiple erasure-code deployment sets. Since erasure code is limited to 16 drives to store a tenant's data, large-bucket support can be used when there is a need for a larger number of disks or larger storage requirements. With large-bucket support, more than 16 drives can be used up front while deploying Minio Server, allowing for peta-scale storage systems without any special configuration.

Internally, Minio Server creates multiple, smaller erasure-coded sets, which are further combined into a single namespace. 

This topic describes how to get started with large-bucket deployments. For more information about advanced uses and limitations, see the [design document](./minio-large-bucket-support-design-guide.html).

- [Install Minio Server](#install-minio-server)
- [Run Minio on Multiple Drives](#run-minio-on-multiple-drives)
- [Test the Configuration](#test-the-configuration)
- [Explore Further](#explorer-further)

## <a name="install-minio-server"></a>1. Install Minio Server

Install Minio Server using the instructions in the [Minio Quickstart Guide](https://docs.minio.io/docs/minio-quickstart-guide).

## <a name="run-minio-on-multiple-drives"></a>2. Run Minio on Multiple Drives

The configuration for large-bucket support is the same as standard Minio Server usage, with the addition of the `...` convention to abbreviate the drive arguments. Remote drives in a distributed configuration are encoded as HTTP(s) URIs which can be similarly abbreviated as well.

**Note:** The drive paths below are for demonstration purposes only and must be replaced with the actual drive paths/folders from a production configuration.

### 2.1 Export the Access Key and Secret Key

All of the nodes running in distributed Minio need to have the same access key and secret key. Use the commands below on all of the nodes, to export the access key and secret key as environment variables before executing Minio Server:

```sh
export MINIO_ACCESS_KEY=<ACCESS_KEY>
export MINIO_SECRET_KEY=<SECRET_KEY>
```

### 2.1 Use a Large Bucket on Multiple Drives in Standalone Minio

Identify the paths to the disks (e.g. `/export1, /export2 .... /export24`) and invoke the following command on each of the nodes where Minio is to be launched with large-bucket support:

```sh
minio server /export{1...24}
```

### 2.2 Use a Large Bucket on Multiple Servers in Distributed Minio

Identify the paths to the disks (e.g. `http://host1/export1, http://host2/export2 .... http://host4/export16`), and invoke the following command on all of the nodes where Minio is to be launched with large-bucket support:

```sh
minio server http://host{1...4}/export{1...16}
```

## <a name="test-the-configuration"></a>3. Test the Configuration

To test this configuration, access Minio Server via the object browser or use [`mc`](https://docs.minio.io/docs/minio-client-quickstart-guide). The uploaded files should be accessible from all of the Minio endpoints.

## <a name="explorer-further"></a>4. Explore Further
- [Use `mc` with Minio Server](https://docs.minio.io/docs/minio-client-quickstart-guide)
- [Use `aws-cli` with Minio Server](https://docs.minio.io/docs/aws-cli-with-minio)
- [Use `s3cmd` with Minio Server](https://docs.minio.io/docs/s3cmd-with-minio)
- [Use `minio-go` SDK with Minio Server](https://docs.minio.io/docs/golang-client-quickstart-guide)
- [The Minio documentation website](https://docs.minio.io)
