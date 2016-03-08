## Running Minio in Docker.

### Installing Docker.

```bash
sudo apt-get install Docker.io
```

### Generating `minio configs` for the first time.

```bash
docker run -p 9000 minio/minio:latest
```

### Persist `minio configs`.

```bash
docker commit <running_minio_container_id> minio/my-minio
docker stop <running_minio_container_id>
```

### Create a data volume container.

```bash
docker create -v /export --name minio-export minio/my-minio /bin/true
```

You can also map ``.minio`` directory containing authentication information.

```bash
docker create -v /export --name minio-export -v /.minio --name minio-config minio/my-minio /bin/true
```

You can then use the `--volumes-from` flag to mount the `/export` and ``/.minio`` volume in another container.

```bash
docker run -p 9000 --volumes-from minio-export --volumes-from minio-config --name minio-server1 minio/my-minio
```
