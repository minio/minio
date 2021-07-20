# FS3 Quickstart Guide


# Install from Source
## Build the Source Code
#### Build UI
```bash
cd browser
npm install chalk
npm run release
# back to repository 
cd ..
```
#### Install Filecoin dependency
```bash
sudo apt install mesa-opencl-icd ocl-icd-opencl-dev gcc git bzr jq pkg-config curl clang build-essential hwloc libhwloc-dev wget -y && sudo apt upgrade -y
```
#### Install go module dependency
``` bash 
# get submodules
git submodule update --init --recursive
# build filecoin-ffi
make ffi
make 
```

#### Export environment variables
A wallet address is a must for sending deals to miner. You can change it via setting environment variable `filw_wallet`.
``` bash 
# export wallet address
export fil_wallet=MY_WALLET_ADDRESS 
```

## Run a Standalone FS3 Server
``` bash
 ./minio server ~/minio-data
```

The default fs3 volume address `GlobalVolumeAddress` is set as `~/minio-data`, which can be changed in `fs3/cmd/globals.go`. See more details in Pre-existing data.



The MinIO deployment starts using default root credentials `minioadmin:minioadmin`. You can test the deployment using the MinIO Browser, an embedded
web-based object browser built into MinIO Server. Point a web browser running on the host machine to http://127.0.0.1:9000 and log in with the
root credentials. You can use the Browser to create buckets, upload objects, and browse the contents of the MinIO server.

You can also connect using any S3-compatible tool, such as the MinIO Client `mc` commandline tool. See
[Test using MinIO Client `mc`](#test-using-minio-client-mc) for more information on using the `mc` commandline tool. For application developers,
see https://docs.min.io/docs/ and click **MinIO SDKs** in the navigation to view MinIO SDKs for supported languages.


> NOTE: Standalone MinIO servers are best suited for early development and evaluation. Certain features such as versioning, object locking, and bucket replication
require distributed deploying MinIO with Erasure Coding. For extended development and production, deploy MinIO with Erasure Coding enabled - specifically,
with a *minimum* of 4 drives per MinIO server. See [MinIO Erasure Code Quickstart Guide](https://docs.min.io/docs/minio-erasure-code-quickstart-guide.html)
for more complete documentation.

MinIO strongly recommends *against* using compiled-from-source MinIO servers for production environments.

# Deployment Recommendations

## Allow port access for Firewalls

By default FS3 uses the port 9000 to listen for incoming connections. If your platform blocks the port by default, you may need to enable access to the port.

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

## Pre-existing data
When deployed on a single drive, FS3 lets clients access any pre-existing data in the data directory. For example, if MinIO is started with the command  `minio server /mnt/data`, any pre-existing data in the `/mnt/data` directory would be accessible to the clients.

The above statement is also valid for all gateway backends.

# Test MinIO Connectivity

## Test using FS3 Browser
MinIO Server comes with an embedded web based object browser. Point your web browser to http://127.0.0.1:9000 to ensure your server has started successfully.



## Test using FS3 `mc`
`mc` provides a modern alternative to UNIX commands like ls, cat, cp, mirror, diff etc. It supports filesystems and Amazon S3 compatible cloud storage services. 

