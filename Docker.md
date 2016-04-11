### Run Minio docker image

Start docker with random keys, generated name, and ephemeral data:

```bash
docker run -p 9000:9000 minio/minio server /export
```

Start docker, ephemeral data with consistent name (minio1) and keys (examples shown are from AWS documentation and should be changed):

```bash
docker run -p 9000:9000 --name minio1 \
  -e "MINIO_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE" \
  -e "MINIO_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
  minio/minio server /export
```

Map export and configuration directories from host for persistence:

```bash
docker run -p 9000:9000 --name minio1 \
  -v /mnt/export/minio1:/export \
  -v /mnt/config/minio1:/root/.minio \
  minio/minio server /export
```
