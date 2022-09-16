# Log Search API Server for MinIO

## Development setup

1. Start Postgresql server in container with logsearch api:

```shell
docker-compose up
```

3. Minio setup:


env variables:

```shell
export MINIO_AUDIT_WEBHOOK_ENDPOINT=http://localhost:8080/api/ingest?token=12345
export MINIO_AUDIT_WEBHOOK_AUTH_TOKEN="12345"  
export MINIO_AUDIT_WEBHOOK_ENABLE="on"    
export MINIO_ROOT_USER=adminadmin
export MINIO_ROOT_PASSWORD=adminadmin
export MINIO_BROWSER=OFF
./minio gateway zcn
```

## sample api call to get the audit log.
```
http://localhost:8080/api/query?token=12345&q=raw&timeAsc&fp=api_name:Put*&pageSize=1000&last=1000h
```
## API reference

- API is coming from the minio operator for audit logs you can read more about it from here https://github.com/minio/operator/tree/master/logsearchapi
