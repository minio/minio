# MinIO Quickstart Guide
[![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

[![MinIO](https://raw.githubusercontent.com/minio/minio/master/.github/logo.svg?sanitize=true)](https://min.io)

MinIO is a High Performance Object Storage released under Apache License v2.0. It is API compatible with Amazon S3 cloud storage service. Use MinIO to build high performance infrastructure for machine learning, analytics and application data workloads.

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

> NOTE: Docker will not display the default keys unless you start the container with the `-it`(interactive TTY) argument. Generally, it is not recommended to use default keys with containers. Please visit MinIO Docker quickstart guide for more information [here](https://docs.min.io/docs/minio-docker-quickstart-guide)

## macOS
### Homebrew (recommended)
Install minio packages using [Homebrew](https://brew.sh/)
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
| Platform    | Architecture | URL                                                       |
| ----------  | --------     | ------                                                    |
| Apple macOS | 64-bit Intel | https://dl.min.io/server/minio/release/darwin-amd64/minio |
```sh
chmod 755 minio
./minio server /data
```

## GNU/Linux
### Binary Download
| Platform   | Architecture | URL                                                      |
| ---------- | --------     | ------                                                   |
| GNU/Linux  | 64-bit Intel | https://dl.min.io/server/minio/release/linux-amd64/minio |
```sh
wget https://dl.min.io/server/minio/release/linux-amd64/minio
chmod +x minio
./minio server /data
```

| Platform   | Architecture | URL                                                        |
| ---------- | --------     | ------                                                     |
| GNU/Linux  | ppc64le      | https://dl.min.io/server/minio/release/linux-ppc64le/minio |
```sh
wget https://dl.min.io/server/minio/release/linux-ppc64le/minio
chmod +x minio
./minio server /data
```

## Microsoft Windows
### Binary Download
| Platform          | Architecture | URL                                                            |
| ----------        | --------     | ------                                                         |
| Microsoft Windows | 64-bit       | https://dl.min.io/server/minio/release/windows-amd64/minio.exe |
```sh
minio.exe server D:\Photos
```

## FreeBSD
### Port
Install minio packages using [pkg](https://github.com/freebsd/pkg), MinIO doesn't officially build FreeBSD binaries but is maintained by FreeBSD upstream [here](https://www.freshports.org/www/minio).

```sh
pkg install minio
sysrc minio_enable=yes
sysrc minio_disks=/home/user/Photos
service minio start
```

## Install from Source
Source installation is only intended for developers and advanced users. If you do not have a working Golang environment, please follow [How to install Golang](https://golang.org/doc/install). Minimum version required is [go1.13](https://golang.org/dl/#stable)

```sh
GO111MODULE=on go get github.com/minio/minio
```

## Allow port access for Firewalls

By default MinIO uses the port 9000 to listen for incoming connections. If your platform blocks the port by default, you may need to enable access to the port.

### iptables

For hosts with iptables enabled (RHEL, CentOS, etc), you can use `iptables` command to enable all traffic coming to specific ports. Use below command to allow
access to port 9000

```sh
iptables -A INPUT -p tcp --dport 9000 -j ACCEPT
service iptables restart
```

Below command enables all incoming traffic to ports ranging from 9000 to 9010.

```sh
iptables -A INPUT -p tcp --dport 9000:9010 -j ACCEPT
service iptables restart
```

### ufw

For hosts with ufw enabled (Debian based distros), you can use `ufw` command to allow traffic to specific ports. Use below command to allow access to port 9000

```sh
ufw allow 9000
```

Below command enables all incoming traffic to ports ranging from 9000 to 9010.

```sh
ufw allow 9000:9010/tcp
```

### firewall-cmd

For hosts with firewall-cmd enabled (CentOS), you can use `firewall-cmd` command to allow traffic to specific ports. Use below commands to allow access to port 9000

```sh
firewall-cmd --get-active-zones
```

This command gets the active zone(s). Now, apply port rules to the relevant zones returned above. For example if the zone is `public`, use

```sh
firewall-cmd --zone=public --add-port=9000/tcp --permanent
```

Note that `permanent` makes sure the rules are persistent across firewall start, restart or reload. Finally reload the firewall for changes to take effect.

```sh
firewall-cmd --reload
```

## Test using MinIO Browser
MinIO Server comes with an embedded web based object browser. Point your web browser to http://127.0.0.1:9000 ensure your server has started successfully.

![Screenshot](https://github.com/minio/minio/blob/master/docs/screenshots/minio-browser.png?raw=true)

## Test using MinIO Client `mc`
`mc` provides a modern alternative to UNIX commands like ls, cat, cp, mirror, diff etc. It supports filesystems and Amazon S3 compatible cloud storage services. Follow the MinIO Client [Quickstart Guide](https://docs.min.io/docs/minio-client-quickstart-guide) for further instructions.

## Pre-existing data
When deployed on a single drive, MinIO server lets clients access any pre-existing data in the data directory. For example, if MinIO is started with the command  `minio server /mnt/data`, any pre-existing data in the `/mnt/data` directory would be accessible to the clients.

The above statement is also valid for all gateway backends.

## Upgrading MinIO
MinIO server supports rolling upgrades, i.e. you can update one MinIO instance at a time in a distributed cluster. This allows upgrades with no downtime. Upgrades can be done manually by replacing the binary with the latest release and restarting all servers in a rolling fashion. However, we recommend all our users to use [`mc admin update`](https://docs.min.io/docs/minio-admin-complete-guide.html#update) from the client. This will update all the nodes in the cluster and restart them, as shown in the following command from the MinIO client (mc):

```
mc admin update <minio alias, e.g., myminio>
```

**Important things to remember during upgrades**:

- `mc admin update` will only work if the user running MinIO has write access to the parent directory where the binary is located, for example if the current binary is at `/usr/local/bin/minio`, you would need write access to `/usr/local/bin`.
- In the case of federated setups `mc admin update` should be run against each cluster individually. Avoid updating `mc` until all clusters have been updated.
- If you are updating the server it is always recommended (unless explicitly mentioned in MinIO server release notes), to update `mc` once all the servers have been upgraded using `mc update`.
- `mc admin update` is disabled in docker/container environments, container environments provide their own mechanisms for updating running containers.
- If you are using Vault as KMS with MinIO, ensure you have followed the Vault upgrade procedure outlined here: https://www.vaultproject.io/docs/upgrading/index.html
- If you are using etcd with MinIO for the federation, ensure you have followed the etcd upgrade procedure outlined here: https://github.com/etcd-io/etcd/blob/master/Documentation/upgrades/upgrading-etcd.md

## Explore Further
- [MinIO Erasure Code QuickStart Guide](https://docs.min.io/docs/minio-erasure-code-quickstart-guide)
- [Use `mc` with MinIO Server](https://docs.min.io/docs/minio-client-quickstart-guide)
- [Use `aws-cli` with MinIO Server](https://docs.min.io/docs/aws-cli-with-minio)
- [Use `s3cmd` with MinIO Server](https://docs.min.io/docs/s3cmd-with-minio)
- [Use `minio-go` SDK with MinIO Server](https://docs.min.io/docs/golang-client-quickstart-guide)
- [The MinIO documentation website](https://docs.min.io)

## Contribute to MinIO Project
Please follow MinIO [Contributor's Guide](https://github.com/minio/minio/blob/master/CONTRIBUTING.md)

## License
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fminio%2Fminio.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2Fminio%2Fminio?ref=badge_large)
