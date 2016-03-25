### Run Minio docker image

Start docker, however data is not persistent.

```bash
docker run -p 9000:9000 minio/minio fs /export/data
```

Map export and configuration directories from host for persistence.

```bash
docker run -p 9000:9000 --name minio1 -v /mnt/export/minio1:/export -v /mnt/config/minio1:/root/.minio minio/minio fs /export/data
```
