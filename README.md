# Minio Quickstart Guide
[![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio) [![Snap Status](https://build.snapcraft.io/badge/minio/minio.svg)](https://build.snapcraft.io/user/minio/minio)

Minio is an object storage server released under Apache License v2.0. It is compatible with Amazon S3 cloud storage service. It is best suited for storing unstructured data such as photos, videos, log files, backups and container / VM images. Size of an object can range from a few KBs to a maximum of 5TB.

Minio server is light enough to be bundled with the application stack, similar to NodeJS, Redis and MySQL.

## Docker Container
### Stable
```
docker pull minio/minio
docker run -p 9000:9000 minio/minio server /data
```

### Edge
```
docker pull minio/minio:edge
docker run -p 9000:9000 minio/minio:edge server /data
```
Please visit Minio Docker quickstart guide for more [here](https://docs.minio.io/docs/minio-docker-quickstart-guide)

## macOS
### Homebrew
Install minio packages using [Homebrew](http://brew.sh/)
```sh
brew install minio/stable/minio
minio server /data
```

> NOTE: If you previously installed minio using `brew install minio` then it is recommended that you reinstall minio from `minio/stable/minio` official repo instead.
```sh
brew uninstall minio
brew install minio/stable/minio
```

### Binary Download
| Platform| Architecture | URL|
| ----------| -------- | ------|
|Apple macOS|64-bit Intel|https://dl.minio.io/server/minio/release/darwin-amd64/minio |
```sh
chmod 755 minio
./minio server /data
```

## GNU/Linux
### Binary Download
| Platform| Architecture | URL|
| ----------| -------- | ------|
|GNU/Linux|64-bit Intel|https://dl.minio.io/server/minio/release/linux-amd64/minio |
```sh
chmod +x minio
./minio server /data
```

### Snap
Install minio using [Snap](https://snapcraft.io)
```sh
sudo snap install minio --edge
```

Start minio using `snap run` command
```sh
sudo snap connect minio:mount-observe
sudo snap run minio server /data
```

## Microsoft Windows
### Binary Download
| Platform| Architecture | URL|
| ----------| -------- | ------|
|Microsoft Windows|64-bit|https://dl.minio.io/server/minio/release/windows-amd64/minio.exe |
```sh
minio.exe server D:\Photos
```

## FreeBSD
### Port
Install minio packages using [pkg](https://github.com/freebsd/pkg)

```sh
pkg install minio
sysrc minio_enable=yes
sysrc minio_disks=/home/user/Photos
service minio start
```

## Install from Source
Source installation is only intended for developers and advanced users. If you do not have a working Golang environment, please follow [How to install Golang](https://docs.minio.io/docs/how-to-install-golang).

```sh
go get -u github.com/minio/minio
```

## Test using Minio Browser
Minio Server comes with an embedded web based object browser. Point your web browser to http://127.0.0.1:9000 ensure your server has started successfully.

![Screenshot](https://github.com/minio/minio/blob/master/docs/screenshots/minio-browser.jpg?raw=true)

## Test using Minio Client `mc`
`mc` provides a modern alternative to UNIX commands like ls, cat, cp, mirror, diff etc. It supports filesystems and Amazon S3 compatible cloud storage services. Follow the Minio Client [Quickstart Guide](https://docs.minio.io/docs/minio-client-quickstart-guide) for further instructions.

## Explore Further
- [Minio Erasure Code QuickStart Guide](https://docs.minio.io/docs/minio-erasure-code-quickstart-guide)
- [Use `mc` with Minio Server](https://docs.minio.io/docs/minio-client-quickstart-guide)
- [Use `aws-cli` with Minio Server](https://docs.minio.io/docs/aws-cli-with-minio)
- [Use `s3cmd` with Minio Server](https://docs.minio.io/docs/s3cmd-with-minio)
- [Use `minio-go` SDK with Minio Server](https://docs.minio.io/docs/golang-client-quickstart-guide)
- [The Minio documentation website](https://docs.minio.io)

## Contribute to Minio Project
Please follow Minio [Contributor's Guide](https://github.com/minio/minio/blob/master/CONTRIBUTING.md)
