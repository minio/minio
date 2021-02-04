# MinIO Server Debugging Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

### HTTP Trace
HTTP tracing can be enabled by using [`mc admin trace`](https://github.com/minio/mc/blob/master/docs/minio-admin-complete-guide.md#command-trace---display-minio-server-http-trace) command.

Example:
```sh
minio server /data
```

Default trace is succinct only to indicate the API operations being called and the HTTP response status.
```sh
mc admin trace myminio
```

To trace entire HTTP request
```sh
mc admin trace --verbose myminio
```

To trace entire HTTP request and also internode communication
```sh
mc admin trace --all --verbose myminio
```


### Subnet Health
Subnet Health diagnostics help ensure that the underlying infrastructure that runs MinIO is configured correctly, and is functioning properly. This test is one-shot long running one, that is recommended to be run as soon as the cluster is first provisioned, and each time a failure scenario is encountered. Note that the test incurs majority of the available resources on the system. Care must be taken when using this to debug failure scenario, so as to prevent larger outages. Health tests can be triggered using `mc admin subnet health` command.

Example:
```sh
minio server /data
```

The command takes no flags
```sh
mc admin subnet health myminio
```

The output printed will be of the form
```sh
● Admin Info ... ✔ 
● CPU ... ✔ 
● Disk Hardware ... ✔ 
● Os Info ... ✔ 
● Mem Info ... ✔ 
● Process Info ... ✔ 
● Config ... ✔ 
● Drive ... ✔ 
● Net ... ✔ 
*********************************************************************************
                                   WARNING!!
     ** THIS FILE MAY CONTAIN SENSITIVE INFORMATION ABOUT YOUR ENVIRONMENT ** 
     ** PLEASE INSPECT CONTENTS BEFORE SHARING IT ON ANY PUBLIC FORUM **
*********************************************************************************
mc: Health data saved to dc-11-health_20200321053323.json.gz
```

The gzipped output contains debugging information for your system
