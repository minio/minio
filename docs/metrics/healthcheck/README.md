## MinIO Healthcheck

MinIO server exposes three un-authenticated, healthcheck endpoints liveness probe and a cluster probe at `/minio/health/live` and `/minio/health/cluster` respectively.

### Liveness probe

This probe always responds with '200 OK'. When liveness probe fails, Kubernetes like platforms restart the container.

```
livenessProbe:
  httpGet:
    path: /minio/health/live
    port: 9000
    scheme: HTTP
  initialDelaySeconds: 120
  periodSeconds: 15
  timeoutSeconds: 10
  successThreshold: 1
  failureThreshold: 3
```

### Cluster probe
This probe is not useful in almost all cases, this is meant for administrators to see if quorum is available in any given cluster. The reply is '200 OK' if cluster has quorum if not it returns '503 Service Unavailable'.

```
curl http://minio1:9001/minio/health/cluster
HTTP/1.1 503 Service Unavailable
Accept-Ranges: bytes
Content-Length: 0
Content-Security-Policy: block-all-mixed-content
Server: MinIO/GOGET.GOGET
Vary: Origin
X-Amz-Bucket-Region: us-east-1
X-Minio-Write-Quorum: 3
X-Amz-Request-Id: 16239D6AB80EBECF
X-Xss-Protection: 1; mode=block
Date: Tue, 21 Jul 2020 00:36:14 GMT
```

#### Checking cluster health for maintenance
You may query the cluster probe endpoint to check if the node which received the request can be taken down for maintenance, if the server replies back '412 Precondition Failed' this means you will lose HA. '200 OK' means you are okay to proceed.

```
curl http://minio1:9001/minio/health/cluster?maintenance=true
HTTP/1.1 412 Precondition Failed
Accept-Ranges: bytes
Content-Length: 0
Content-Security-Policy: block-all-mixed-content
Server: MinIO/GOGET.GOGET
Vary: Origin
X-Amz-Bucket-Region: us-east-1
X-Amz-Request-Id: 16239D63820C6E76
X-Xss-Protection: 1; mode=block
X-Minio-Write-Quorum: 3
Date: Tue, 21 Jul 2020 00:35:43 GMT
```
