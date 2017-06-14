# Deploy Minio on DC/OS [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

To deploy Minio on DC/OS, you can use a Universe package, or create a customized config file. We at Minio recently released an [official universe package](https://github.com/mesosphere/universe/tree/version-3.x/repo/packages/M/minio/0) to enable single click Minio deployment on a DC/OS cluster.

## 1. Prerequisites

- DC/OS 1.8 or later running on your cluster. 
- [Marathon-LB](https://dcos.io/docs/1.8/usage/service-discovery/marathon-lb/usage/) installed and running.
- IP address of the public agent(s) where Marathon-LB or an available hostname configured to point to the public agent(s) where Marathon-LB is running.

## 2. Setting up Minio 

You can install Minio Universe package using the DC/OS GUI or CLI. 

### Minio installation on DC/OS GUI 

Visit the DC/OS admin page, and click on `Universe` on the left menu bar. Then click on the `Packages` tab and search for Minio. Once you see the package, click the `Install Package` button on the right hand side. Then, enter configuration values like the storage and service type you’d like to use with your Minio instance. Finally enter the public Marathon-LB IP address under `networking >> public-agent`, and click `Review and Install`.

This completes the install process. Before you can access Minio server, get the access key and secret key from the Minio container logs. Click on `Services` and select Minio service in DC/OS admin page. Then go to the `logs` tab and copy the accesskey and secretkey.

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
