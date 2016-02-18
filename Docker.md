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

You can then use the `--volumes-from` flag to mount the `/export` volume in another container.

```bash
docker run -p 9000 --volumes-from minio-export --name minio1 minio/my-minio
```

### Setup a sample proxy in front using Caddy.

Please download [Caddy Server](https://caddyserver.com/download)

Create a caddy configuration file as below, change the ip addresses for your environment.
```bash
cat Caddyfile
10.0.0.3:2015 {
    proxy / localhost:9000 {
        proxy_header Host {host}
    }
    tls off
}
```

```bash
$ ./caddy
Activating privacy features... done.
10.0.0.3:2015
```
