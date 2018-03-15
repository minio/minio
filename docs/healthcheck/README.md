## Minio Healthcheck

Minio server exposes two un-authenticated, healthcheck endpoints - liveness probe and readiness probe at `/minio/health/live` and `/minio/health/ready` respectively.

### Liveness probe

This probe is used to identify situations where the server is running but may not behave optimally, i.e. sluggish response or corrupt backend. Such problems can be *only* fixed by a restart.

Internally, Minio liveness probe handler does a ListBuckets call. If successful, the server returns 200 OK, otherwise 503 Service Unavailable.

When liveness probe fails, Kubernetes like platforms restart the container. 

### Readiness probe

This probe is used to identify situations where the server is not ready to accept requests yet. In most cases, such conditions recover in some time.

Internally, Minio readiness probe handler checks for total go-routines. If the number of go-routines is less than 1000 (threshold), the server returns 200 OK, otherwise 503 Service Unavailable.

Platforms like Kubernetes *do not* forward traffic to a pod until its readiness probe is successful. 

### Configuration example

Sample `liveness` and `readiness` probe configuration in a Kubernetes `yaml` file can be found [here](https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes-yaml/minio-standalone-deployment.yaml).
