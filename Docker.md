### Run Minio docker image
```docker run -p 9000:9000 minio/minio```

This will start minio server in the docker container, the data however is not persistent.

### Map data volumes from host
Map export and configuration directories from host for persistence.

```bash
docker run -p 9000:9000 -v $HOME/export:/export -v $HOME/.minio:/root/.minio minio/minio
```
