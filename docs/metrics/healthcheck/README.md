## MinIO Healthcheck

MinIO server exposes two un-authenticated, healthcheck endpoints - liveness probe and readiness probe at `/minio/health/live` and `/minio/health/ready` respectively.

### Liveness probe

This probe is used to identify situations where the server is running but may not behave optimally, i.e. sluggish response or corrupt back-end. Such problems can be *only* fixed by a restart.

Internally, MinIO liveness probe handler checks if backend is alive and in read quorum to take requests.

When liveness probe fails, Kubernetes like platforms restart the container.

### Readiness probe

This probe is used to identify situations where the server is not ready to accept requests yet. In most cases, such conditions recover in some time such as quorum not available on drives due to load.

Internally, MinIO readiness probe handler checks for backend is alive and in read quorum then the server returns 200 OK, otherwise 503 Service Unavailable.

Platforms like Kubernetes *do not* forward traffic to a pod until its readiness probe is successful.

### Configuration example

Sample `liveness` and `readiness` probe configuration in a Kubernetes `yaml` file can be found [here](https://github.com/minio/minio/blob/master/docs/orchestration/kubernetes/minio-standalone-deployment.yaml).

### Configure readiness deadline
Readiness checks need to respond faster in orchestrated environments, to facilitate this you can use the following environment variable before starting MinIO

```
MINIO_API_READY_DEADLINE     (duration)  set the deadline for health check API /minio/health/ready e.g. "1m"
```

Set a *5s* deadline for MinIO to ensure readiness handler responds with-in 5seconds.
```
export MINIO_API_READY_DEADLINE=5s
```
