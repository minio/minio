# How to use ``mc mirror`` to setup replication between two sites running Minio. [![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/minio/minio?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

![minio_MIRROR](https://raw.githubusercontent.com/minio/blob/master/docs/screenshots/miniomirror.jpeg?raw=true)



In this document we will illustrate how to set up replication between two Minio servers, `minio1` and `minio2` running on ``192.168.1.11`` and ``192.168.1.12`` respectively. We will mirror the data directory on `minio1` to the bucket on `minio2`.


## 1. Prerequisites

* Download Minio server from [here](https://docs.minio.io/docs/minio)
* Download & Install mc from [here](https://docs.minio.io/docs/minio-client-quickstart-guide)
* Familiarity with [``mc mirror``](https://docs.minio.io/docs/minio-client-complete-guide#mirror)

## 2. Install and Configure Minio Server

### Setup minio1

```sh
$ ./minio server minio1-data/

Endpoint:  http://127.0.0.1:9000  http://192.168.1.11:9000
AccessKey: MURIVYBYNPTYE7O8I779 
SecretKey: lVbZmz4CvGkBl7JKw5icuL7RCcSvpBJTkAJTFQwz
Region:    us-east-1
```
**mc alias**

Alias is a short name to your cloud storage service  for ``Minio client``. End-point, access and secret keys are supplied by your cloud storage provider. API signature is an optional argument. By default, it is set to ``S3v4``.

```sh
$ ./mc config host add minio1 http://192.168.1.11:9000 MURIVYBYNPTYE7O8I779 lVbZmz4CvGkBl7JKw5icuL7RCcSvpBJTkAJTFQwz
```

**Create buckets and add objects**

We have created few buckets using [``mc mb``](https://docs.minio.io/docs/minio-client-complete-guide#mb) and added objects to it using [``mc cp``](https://docs.minio.io/docs/minio-client-complete-guide#cp) Minio client commands. 

```sh
$ ./mc mb minio1/mybucket
$ ./mc cp myfile.txt minio1/bucket1
$ ./mc ls minio1
[2016-07-31 10:26:55 PDT]     0B bucket1/
[2016-07-31 09:36:17 PDT]     0B bucket2/
[2016-07-31 09:38:08 PDT]     0B bucket3/
```

### Setup minio2 

```sh
$ ./minio server minio2-data/

Endpoint:  http://127.0.0.1:9000  http://192.168.1.12:9000
AccessKey: YRDRWWQLEWS9OBJ31GZ2
SecretKey: y2sSWzx5ytwvkELcxOuSaQ8n3doNqoIilRpb5Kjj
Region:    us-east-1
```

```sh
$ ./mc config host add minio2 http://192.168.1.12:9000 YRDRWWQLEWS9OBJ31GZ2 y2sSWzx5ytwvkELcxOuSaQ8n3doNqoIilRpb5Kjj
```

**Create bucket**

We are creating destination bucket ``mbucket`` on ``minio2`` and adding ``minio2`` alias. The bucket ``mbucket`` will be used to mirror data directory of ``minio1``. 

```sh
$ ./mc mb minio1/mbucket
```

```sh
$ ./mc config host add minio2 http://192.168.1.12:9000 YRDRWWQLEWS9OBJ31GZ2 y2sSWzx5ytwvkELcxOuSaQ8n3doNqoIilRpb5Kjj

```

## 4. Setup crontab
Cron is a Unix/Linux system utility by which you can schedule a task process for particular duration, we have tested this setup on Ubuntu Linux.


### Script

Add crontab configuration on `minio1` providing path of data directory, ``minio1-data``. 

``--force`` option with ``mc mirror``  overwrites the destination contents, this would keep your contents in sync..  

```sh

#!/bin/bash
datadir="/home/minio/minio1-data/"
minio2="minio2/mbucket"
MC_PATH="/home/minio/mc"
$MC_PATH --quiet  mirror --force $minio1 $minio2

```

Set executable permissions on the script before adding a cron entry.

```sh

$ chmod 755 /home/minio/minio.sh
```

Set a new cron entry to run ``minio.sh`` script once every 30mins

```sh

$ crontab -e
*/30 * * * * /home/minio/minio.sh 
```

Note: We are going to introduce continuous replication feature in `mc mirror` which will enable the sites to be in sync without having the need to setup cron job.

# Explore Further
* [Minio Quickstart Guide](https://docs.minio.io/docs/minio-quickstart-guide)
* [Minio Client Complete Guide](https://docs.minio.io/docs/minio-client-complete-guide)
