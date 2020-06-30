## MinIO Healthcheck

MinIO server exposes three un-authenticated, healthcheck endpoints liveness probe, readiness probe and a cluster probe at `/minio/health/live`, `/minio/health/ready` and `/minio/health/cluster` respectively.

### Liveness probe

This probe always responds with '200 OK'. When liveness probe fails, Kubernetes like platforms restart the container.

```
          livenessProbe:
            httpGet:
              path: /minio/health/live
              port: 9000
              scheme: HTTP
            initialDelaySeconds: 3
            periodSeconds: 1
            timeoutSeconds: 1
            successThreshold: 1
            failureThreshold: 3
```

### Readiness probe

This probe always responds with '200 OK'. When readiness probe fails, Kubernetes like platforms *do not* forward traffic to a pod.

```
          readinessProbe:
            httpGet:
              path: /minio/health/ready
              port: 9000
              scheme: HTTP
            initialDelaySeconds: 3
            periodSeconds: 1
            timeoutSeconds: 1
            successThreshold: 1
            failureThreshold: 3

```

### Cluster probe

This probe is not useful in almost all cases, this is meant for administrators to see if quorum is available in any given cluster. The reply is '200 OK' if cluster has quorum if not it returns '503 Service Unavailable'.
