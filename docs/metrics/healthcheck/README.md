## MinIO Healthcheck

MinIO server exposes three un-authenticated, healthcheck endpoints - liveness probe and readiness probe and cluster probe.

### Liveness probe

Path: `/minio/health/live`

This probe is used to identify situations where the server is running but may not behave optimally, i.e. sluggish response or corrupt back-end. Such problems can be *only* fixed by a restart.

Internally, MinIO liveness probe handler checks if backend is alive and in read quorum to take requests.

When liveness probe fails, Kubernetes like platforms restart the container.

### Readiness probe

Path: `/minio/health/ready`

This probe is used to identify situations where the server is not ready to accept requests yet. In most cases, such conditions recover in some time such as quorum not available on drives due to load.

Internally, MinIO readiness probe handler checks for backend is alive and in read quorum then the server returns 200 OK, otherwise 503 Service Unavailable.

Platforms like Kubernetes *do not* forward traffic to a pod until its readiness probe is successful. 

### Cluster health probe

Path: `/minio/health/cluster`

This probe is used to identify if the node considers the cluster to be up. This probe can be used by sidekick (load-balancer) for site failover when all the nodes in the site indicate that the site is down.

Internally, MinIO cluster health handler checks if all the erasure sets are up and returns 200 OK, otherwise 503 Service Unavailable. An erasure set is considered to be up if there enough drives are available in the set to perform read operation.


### Configuration example

Sample `liveness` and `readiness` probe configuration in a Kubernetes `yaml` file can be found [here](https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes/minio-standalone-deployment.yaml).
