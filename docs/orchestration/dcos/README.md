# Deploy MinIO on DC/OS [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

To deploy MinIO on DC/OS, you can use our [official universe package](https://github.com/mesosphere/universe/tree/version-3.x/repo/packages/M/minio/6).

## 1. Prerequisites

  - DC/OS 1.9 or later
  - [Marathon-LB](https://dcos.io/docs/1.9/usage/service-discovery/marathon-lb/usage/) must be installed and running
  - Identify [IP of the public agent](https://dcos.io/docs/1.9/administration/locate-public-agent/) where Marathon-LB or an available hostname configured to point to the public agent(s) where Marathon-LB is running.


## 2. Setting up MinIO 

You can install MinIO Universe package using the DC/OS GUI or CLI. 

### MinIO installation on DC/OS GUI 
- Visit the DC/OS admin page, and click on Universe on the left menu bar. Then click on the Packages tab and search for MinIO, click on the ```Install``` button on the right hand side.

- Click on the `Install Package` button for the single-click default installation. This installs MinIO server instance with factory defaults. You can reach your MinIO server at `host:9000` where `host` is IP address or hostname of public-agent where Marathon-LB is installed. `minio` and `minio123` are the default access key and secret keys respectively.

- For more information on advanced installation of MinIO on DC/OS GUI, look [here](https://github.com/dcos/examples/blob/master/minio/1.9/README.md#minio-installation-using-gui).

### MinIO installation on DC/OS CLI

To install MinIO package via CLI, type

```bash
$ dcos package install minio
```

## 3. Uninstalling MinIO

To uninstall MinIO package via CLI, type

```bash
$ dcos package uninstall minio
```

### Explore Further

- [MinIO Erasure Code QuickStart Guide](https://docs.min.io/docs/minio-erasure-code-quickstart-guide)
- [DC/OS Project](https://docs.mesosphere.com/)

