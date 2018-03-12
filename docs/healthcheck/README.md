## Minio Healthcheck

Minio server exposes two un-authenticated, healthcheck endpoints - liveness probe and readiness probe at `/minio/health/live` and `/minio/health/ready` respectively.

### Liveness probe
This probe is used to identify situations where the server is running but may not behave optimally, i.e. sluggish response or corrupt backend. Such problems can be *only* fixed by a restart.

Internally, Minio liveness probe handler does a ListBuckets call. If successful, the server returns 200 OK, otherwise 503 Service Unavailable.

When liveness probe fails, Kubernetes like platforms restart the container.

Sample configuration in a Kubernetes `yaml` file.

```yaml
livenessProbe:
  httpGet:
    path: /minio/health/live
    port: 9000
  initialDelaySeconds: 10
  periodSeconds: 20
```

### Readiness probe
This probe is used to identify situations where the server is not ready to accept requests yet. In most cases, such conditions recover in some time.

Internally, Minio readiness probe handler checks for total go-routines. If the number of go-routines is less than 1000 (threshold), the server returns 200 OK, otherwise 503 Service Unavailable.

Platforms like Kubernetes *do not* forward traffic to a pod until its readiness probe is successful.

Sample configuration in a Kubernetes `yaml` file.

```yaml
livenessProbe:
  httpGet:
    path: /minio/health/ready
    port: 9000
  initialDelaySeconds: 10
  periodSeconds: 20
```