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

The default FS3 volume address `GlobalVolumeAddress` is set as `~/minio-data`, which can be changed in `fs3/cmd/globals.go`. See more details in Pre-existing data.



The FS3 deployment starts using default root credentials `minioadmin:minioadmin`. You can test the deployment using the FS3 Browser, an embedded
web-based object browser built into FS3 Server. Point a web browser running on the host machine to http://127.0.0.1:9000 and log in with the
root credentials. You can use the Browser to create buckets, upload objects, send deals, retrieve data and browse the contents of the FS3 server.

You can also connect using any S3-compatible tool, such as the FS3 `mc` commandline tool.


## FS3 API
### Send Online Deals (single file)
POST`/send/{bucket}/{object}`

#### Example: 

Send request using POSTMAN

``` bash
# Headers
## Use a new User-Agent instead of the default User-Agent in Postman
User-Agent    Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36

# Body
{
    "VerifiedDeal":"false",
    "FastRetrieval":"true",
    "MinerId":"t00000",
    "Price": "0.000005",
    "Duration":"1036800"
}
```
Response from POSTMAN
```bash
{
    "filename": "~/minio-data/test/test.zip",
    "walletAddress": "nvauebtbb3kthiqbnaksb3gfkbaskrt4kwh3er",
    "verifiedDeal": "false",
    "fastRetrieval": "true",
    "dataCid": "bafyktrevft2ar7hmifqw5krqunu4zx76vaqpcjyxcmsbamc3547nyzvtuggg",
    "minerId": "t03354",
    "price": "0.000005",
    "duration": "1038500",
    "dealCid": "bafyreic5jfcksvii7pzv5zpszxw5hxtl7yuioppe5qoeruzp5ql5p6jsy"
}
```


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
When deployed on a single drive, FS3 lets clients access any pre-existing data in the data directory. For example, if FS3 is started with the command  `minio server /mnt/data`, any pre-existing data in the `/mnt/data` directory would be accessible to the clients.

The above statement is also valid for all gateway backends.

# Test FS3 Connectivity

## Test using FS3 Browser
FS3 Server comes with an embedded web based object browser. Point your web browser to http://127.0.0.1:9000 to ensure your server has started successfully.


