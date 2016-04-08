### Run Minio docker image

Start docker, however data is not persistent.

```bash
docker run -p 9000:9000 minio/minio server /export
```

Map export and configuration directories from host for persistence.

```bash
docker run -p 9000:9000 --name minio1 -v /mnt/export/minio1:/export -v /mnt/config/minio1:/root/.minio minio/minio export /export
```
