# Server command line SPEC - Fri Apr  1 19:49:56 PDT 2016

## Single disk
Regular single server, single disk mode.
```
$ minio server <disk>
```

##  Multi disk
```
$ minio controller start - Start minio controller.
```
Prints a secret token to be used by all parties.

Start all servers without disk with `MINIO_CONTROLLER` env set, start in cluster mode.
```
$ MINIO_CONTROLLER=<host>:<token> minio server
```

## Minio Controller cli.

Set controller host and token.
```
$ export MINIO_CONTROLLER=<host>:<token>
```

Create a cluster from the pool of nodes.
```
$ minioctl new <clustername> <ip1>:/mnt/disk1 .. <ip16>:/mnt/disk1
```

Start the cluster.
```
$ minioctl start <clustername>
```

Stop the cluster
```
$ minioctl stop <clustername>
```
