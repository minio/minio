# Deploy Minio on DC/OS [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

To deploy Minio on DC/OS, you can use our [official universe package](https://github.com/mesosphere/universe/tree/version-3.x/repo/packages/M/minio/6).

## 1. Prerequisites

  - DC/OS 1.9 or later
  - [Marathon-LB](https://dcos.io/docs/1.9/usage/service-discovery/marathon-lb/usage/) must be installed and running
  - Identify [IP of the public agent](https://dcos.io/docs/1.9/administration/locate-public-agent/) where Marathon-LB or an available hostname configured to point to the public agent(s) where Marathon-LB is running.


## 2. Setting up Minio 

You can install Minio Universe package using the DC/OS GUI or CLI. 

### Minio installation on DC/OS GUI 
- Visit the DC/OS admin page, and click on Universe on the left menu bar. Then click on the Packages tab and search for Minio, click on the ```Install``` button on the right hand side.

- Click on the `Install Package` button for the single-click default installation. This installs Minio server instance with factory defaults. You can reach your Minio server at `host:9000` where `host` is IP address or hostname of public-agent where Marathon-LB is installed. `minio` and `minio123` are the default access key and secret keys respectively.

- For more information on advanced installation of Minio on DC/OS GUI, look [here](https://github.com/dcos/examples/blob/master/minio/1.9/README.md#minio-installation-using-gui).

### Minio installation on DC/OS CLI

To install Minio package via CLI, type

```bash
$ dcos package install minio
```

## 3. Uninstalling Minio

To uninstall Minio package via CLI, type

```bash
$ dcos package uninstall minio
```

### Explore Further

- [Minio Erasure Code QuickStart Guide](https://docs.minio.io/docs/minio-erasure-code-quickstart-guide)
- [DC/OS Project](https://docs.mesosphere.com/)

