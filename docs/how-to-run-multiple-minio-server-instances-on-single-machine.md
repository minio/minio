# How to run multiple Minio server instances on single machine. [![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/minio/minio?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

![minio_MULTIVERSE](https://github.com/minio/minio/blob/master/docs/screenshots/multiport.png?raw=true)


In this document we will illustrate how to set up multiple Minio server instances on single machine. These Minio servers are running on their own port, data directory & configuration directory.

## 1. Prerequisites

* Download Minio server from [here](https://docs.minio.io/docs/minio)
* Download & Install mc from [here](https://docs.minio.io/docs/minio-client-quickstart-guide)
* Ports should be available for Minio server's use

## 2. Install and Configure Minio Server

Minio server is running on port 9002, 9003, 9004 with associated data directory and configuration file directory.

**Minio server on port ``9002``**

```sh
$ ./minio -C ~/.m1config server --address 192.168.1.11:9002 ~/data1/

Endpoint:  http://192.168.1.11:9002
AccessKey: XTW9SWKRWYKWE9M9K9RG 
SecretKey: pZehbS5UNrA9BAhYHnWC/QVvQ7vGVge48WGHzG9t 
Region:    us-east-1
```

**Minio server on port ``9003``**

```sh
$ ./minio -C ~/.m2config server --address 192.168.1.11:9003 ~/data2/                                                

Endpoint:  http://192.168.1.11:9003
AccessKey: UTD2WWPJOK754KMZKHWF 
SecretKey: DbikDIY4+wItcexJa4nyrwQC0V2r7kLsK5SsRgHb 
Region:    us-east-1
```

**Minio server on port ``9004``**

```sh
$ ./minio -C ~/.m3config server --address 192.168.1.11:9004 ~/data3/                                                 

Endpoint:  http://192.168.1.11:9004
AccessKey: KXLOJ908VEJ2K9RGUFHQ 
SecretKey: LpkeePMtEWAa6payiCovfrNKiFHhABsJhMwGynF8 
Region:    us-east-1
```

This is how directory structure will look like for ``minio1``, replace it with your local setup.

```sh
$ tree -la minio1/
minio1/
├── data1
└── .minio
    ├── certs
    ├── config.json
    └── config.json.old

3 directories, 2 files

```
**Testing it all**

Using terminal comamnd ``netstat`` we can see ``Minio Server`` is running on different ports on same machine.

```sh
$ netstat -ntlp | grep minio
tcp        0      0 192.168.1.11:9002       0.0.0.0:*               LISTEN      29573/minio     
tcp        0      0 192.168.1.11:9003       0.0.0.0:*               LISTEN      29597/minio     
tcp        0      0 192.168.1.11:9004       0.0.0.0:*               LISTEN      29631/minio     
```


# Explore Further
* [Minio Quickstart Guide](https://docs.minio.io/docs/minio-quickstart-guide)
* [Minio Client Complete Guide](https://docs.minio.io/docs/minio-client-complete-guide)

