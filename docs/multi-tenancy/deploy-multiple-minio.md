# Deploying multiple Minio instances [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/) [![codecov](https://codecov.io/gh/minio/minio/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/minio)

This document outlines various ways to deploy multiple Minio instances.

## Single machine deployment
To deploy multiple Minio instances on a single machine, you can use different ports on the same machine. In the example below, there are 3 minio instances on ports 9001, 9002 and 9002, running on the same machine but independent of each other.

*Run Minio-1*:
```
minio --config-dir /data/conf1 server --address :9001 /data/export1
```

*Run Minio-2*:
```
minio --config-dir /data/conf2 server --address :9002 /data/export2
```

*Run Minio-3*:
```
minio --config-dir /data/conf3 server --address :9003 /data/export3
```

## Cluster deployment
While deploying Minio in a cluster, you can either use container orchestration platforms like Kubernetes, DC/OS, Docker Swarm etc. or manage the cluster and Minio instances yourself. While orchestration platforms take care of mapping the requests and endpoints, you'll need to do that yourself in a self managed cluster. Reverse proxies such as Nginx or Traefik can help achieve this.

### Container orchestration platform deployment
Minio can be easily deployed on all the major orchestration platforms. Refer [this document](https://docs.minio.io/docs/minio-deployment-quickstart-guide) to get started with Minio on orchestration platforms.  

### Run Minio behind reverse proxy
In the example config below, a proxy runs on port 80 and reverse proxies the requests to Minio-1 or Minio-2 or Minio-3 depending on the access-key in the client request.

Let's assume:
* Minio-1's access key is ROML2P775VPAT7RLPOWU
* Minio-2's access key is ENT3GYJCKCD1Q79XLP4C
* Minio-3's access key is C988WQ23D98207ELOLPW.

#### Using Nginx
Run nginx with the following config in `/etc/nginx/sites-enabled`:

```
server {
    listen       80;
	location / {
	    proxy_set_header Host $http_host;
	    if ($http_authorization ~* "^AWS4-HMAC-SHA256 Credential=ROML2P775VPAT7RLPOWU") {
           # proxy the request to Minio-1
	       proxy_pass http://localhost:9001;
	    }
	    if ($http_authorization ~* "^AWS4-HMAC-SHA256 Credential=ENT3GYJCKCD1Q79XLP4C") {
           # proxy the request to Minio-2
	       proxy_pass http://localhost:9002;
	    }
	    if ($http_authorization ~* "^AWS4-HMAC-SHA256 Credential=C988WQ23D98207ELOLPW") {
           # proxy the request to Minio-3
	       proxy_pass http://localhost:9003;
	    }
    }
}
```

#### Using Traefik
Traefik can be downloaded from https://traefik.io/

`traefik.toml` config file:

```
defaultEntryPoints = ["http"]

[entryPoints]
  [entryPoints.http]
  address = ":80"

[file]
watch = true

[backends]
  [backends.backend1]
    [backends.backend1.servers.server1]
      url = "http://localhost:9001"

  [backends.backend2]
    [backends.backend2.servers.server1]
      url = "http://localhost:9002"

  [backends.backend3]
    [backends.backend3.servers.server1]
      url = "http://localhost:9003"

[frontends]
    [frontends.frontend1]
    backend = "backend1"
    passHostHeader = true
    [frontends.frontend1.routes.test_1]
    rule = "HeadersRegexp: Authorization, ^AWS4-HMAC-SHA256 Credential=ROML2P775VPAT7RLPOWU"

    [frontends.frontend2]
    backend = "backend2"
    passHostHeader = true
    [frontends.frontend2.routes.test_1]
    rule = "HeadersRegexp: Authorization, ^AWS4-HMAC-SHA256 Credential=ENT3GYJCKCD1Q79XLP4C"

    [frontends.frontend3]
    backend = "backend3"
    passHostHeader = true
    [frontends.frontend3.routes.test_1]
    rule = "HeadersRegexp: Authorization, ^AWS4-HMAC-SHA256 Credential=C988WQ23D98207ELOLPW"
```
