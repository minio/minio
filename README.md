# MinIO Object Storage

[![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![license](https://img.shields.io/badge/license-AGPL%20V3-blue)](https://github.com/minio/minio/blob/master/LICENSE)

[![MinIO](https://raw.githubusercontent.com/minio/minio/master/.github/logo.svg?sanitize=true)](https://min.io)

MinIO is a High Performance Object Storage released under GNU Affero General Public License v3.0. 
It is API compatible with Amazon S3 cloud storage service. 
Use MinIO to build high performance infrastructure for machine learning, analytics and application data workloads.

MinIO generally supports deployment on 64-bit Linux or Kubernetes-based infrastructure.
The fastest way to start experimenting with MinIO is to run it as a container on local machine.
The following command uses [podman](https://podman.io/) and binds a local volume ``~/minio`` to the container for storage:

```sh
podman run \
  -p 9000:9000 \
  -p 9090:9090 \
  -v ~/minio:/data \
  -n minio \
  quay.io/minio/minio server /data --console-address ":9090"
```

MinIO starts using the default root credentials ``minioadmin:minioadmin`` and outputs it's logs to the terminal or shell.
You can pass the ``-dt`` parameters to run the MinIO container in a detached state with TTY.

Once started, you can perform object storage operations by pointing any [S3-compatible SDK](https://min.io/docs/minio/linux/developers/minio-drivers.html?ref=github-minio) to ``localhost:9000``.
Alternatively, open ``localhost:9090`` in your preferred browser to use the [MinIO Console](https://min.io/docs/minio/linux/administration/minio-console.html?ref=github-minio).

For more complete documentation on installing and deploying MinIO, see the following:

Production Deployments:

- [Linux](https://min.io/docs/minio/linux/index.html?ref=github-minio)
- [Kubernetes Upstream](https://min.io/docs/minio/kubernetes/upstream/index.html?ref=github-minio)
- [RedHat Openshift](https://min.io/docs/minio/kubernetes/openshift/index.html?ref=github-minio)
- [Amazon Elastic Kubernetes Service](https://min.io/docs/minio/kubernetes/eks/index.html?ref=github-minio)
- [Google Kubernetes Engine](https://min.io/docs/minio/kubernetes/gke/index.html?ref=github-minio)
- [Azure Kubernetes Service](https://min.io/docs/minio/kubernetes/aks/index.html?ref=github-minio)
- [SUSE Rancher](https://min.io/product/multicloud-suse-rancher?ref=github-minio)

**NOTE** MinIO strongly recommends using the [MinIO Kubernetes Operator](https://min.io/docs/minio/kubernetes/upstream/index.html?ref=github-minio) for deploying MinIO Tenants onto Kubernetes infrastructure, regardless of the Kubernetes "flavor" (upstream, OpenShift, Rancher, Cloud Hosted, etc.).

Local Development and Evaluation:

- [Docker/Podman](https://min.io/docs/minio/container/index.html?ref=github-minio)
- [macOS](https://min.io/docs/minio/macos/index.html?ref=github-minio)
- [Windows](https://min.io/docs/minio/windows/index.html?ref=github-minio)

## Connecting Using the MinIO Client

The [MinIO Client ``mc``](https://min.io/docs/minio/linux/reference/minio-mc.html#quickstart?ref=github-minio) provides a modern alternative to UNIX commands like ``ls``, ``cat``, ``cp``, ``mirror``, and ``diff``.
It supports both S3-compatible storage services like MinIO *and* POSIX filesystems.

``mc`` also includes support for the MinIO server's administrative API, allowing you to use it for both developer and administrative tasks on the MinIO server.

Use the ``mc alias set`` command to create an Alias for your MinIO deployment:

```sh
mc alias set local http://localhost:9000 minioadmin minioadmin
```

You can then use commands against that alias:

```sh
# Make a bucket
mc mb local/data

# Add data to the bucket
mc cp ~/Downloads/mydata/* local/data/

# List data in the bucket

mc ls local/data
```

## Connecting using the MinIO Console

The [MinIO Console](https://min.io/docs/minio/linux/administration/minio-console.html?ref=github-minio) provides a web interface for accessing your MinIO server and performing both standard S3 operations and administrative operations.
For example, you can use the Console for creating and configuring buckets, uploading objects to those buckets, creating lifecycle management rules, and configuring identity and access management settings.

MinIO by default 

# Build Instructions

MinIO requires [Go 1.19 or later](https://go.dev/dl/#stable). 
You must [install Golang](https://golang.org/doc/install) prior to building MinIO from source.

You can run ``make build`` to build the binary and run it using ``./minio`` from the Github repository source.
Alternatively, you can use `go install` to install MinIO using the latest commit:

```sh
go install github.com/minio/minio@latest
```

# Supported Architectures

MinIO publishes builds for the following architectures:

| Architecture         | OS             | URL                                                            |
|----------------------|----------------|----------------------------------------------------------------|
| 64-Bit Intel/AMD     | Linux          | https://dl.min.io/server/minio/release/linux-amd64/minio       |
|                      | Windows        | https://dl.min.io/server/minio/release/windows-amd64/minio.exe |
|                      | macOS (Pre-M1) | https://dl.min.io/server/minio/release/darwin-amd64/minio      |
|                      |                |                                                                |
| 64-Bit ARM           | Linux          | https://dl.min.io/server/minio/release/linux-arm64/minio       |
|                      | macOS (M1)     | https://dl.min.io/server/minio/release/darwin-arm64/minio      |
|                      |                |                                                                |
| 64-Bit PowerPC LE    | Linux          | https://dl.min.io/server/minio/release/linux-ppc64le/minio     |
|                      |                |                                                                |
| IBM Z-Series (S390X) | Linux          | https://dl.min.io/server/minio/release/linux-s390x/minio       |

## Recommended Operating Systems

MinIO recommends using Linux Operating Systems which support the 5.x or later Kernel.
For example:

- Red Hat Enterprise Linux 9.0 or later
- Ubuntu 20.04 LTS or later
- 

# Upgrades

Community users should follow within 6 months of the latest stable version of MinIO for best results.

MinIO Standard customers have 1 year long term support for any release.

MinIO Enterprise customers have 5 year long term support for any release.

You can perform MinIO upgrades by doing a binary replacement without taking the cluster down.
Once you have updated all MinIO server nodes with the new binary, restart the cluster and check for normal operations.
MinIO **does not** support "rolling upgrade" strategies operating on a single node at a time.
Upgrade and restart all nodes in parallel as per our guidelines.
MinIO restarts typically complete within seconds, and S3 SDK retry functionality enables minimal disruption to client applications.

***IMPORTANT***
Always test upgrades in a lower environment (DEV, QA, UAT) before applying to production.
Performing blind upgrades in production environments carries significant risk.

Always read the MinIO release notes before performing any upgrade.
MinIO does not require following tightly to the latest release, and you can selectively upgrade when specific features, fixes, or security patches relevant to your deployment become available.

For Kubernetes environments using the [MinIO Operator](https://min.io/docs/minio/kubernetes/upstream/operations/installation.html), you should update managed Tenants through the [Operator](https://min.io/docs/minio/kubernetes/upstream/operations/install-deploy-manage/upgrade-minio-tenant.html)

# Support

MinIO is released under GNU Affero General Public License v3.0. 
Community users can receive free best-effort assistance through the [MinIO Community Slack]() or the MinIO Github through [Issues]() or [Discussions]().

MinIO [SUBNET](https://min.io/pricing?ref=github-minio) provides Standard and Enterprise support plans with 24-hour and 1-hour SLAs respectively.
SUBNET-registered clusters are covered under the MinIO Commercial License, which provides exemptions for your proprietary applications from the AGPL3.0 copy-left provisions.

# Licensing

MinIO is released under [GNU Affero General Public License v3.0](LICENSE).
You can read more about license compliance under [COMPLIANCE](compliance.md)


## Contribute to MinIO Project

Please follow MinIO [Contributor's Guide](CONTRIBUTING.md)

# Explore Further

- [MinIO Training - Object Storage Essentials](https://www.youtube.com/playlist?list=PLFOIsHSSYIK3WitnqhqfpeZ6fRFKHxIr7)
- [MinIO Training - MinIO for Developers](https://www.youtube.com/playlist?list=PLFOIsHSSYIK37B3VtACkNksUw8_puUuAC)
- [MinIO Blog](https://blog.min.io/)