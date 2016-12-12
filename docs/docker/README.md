# Minio Docker Quickstart Guide [![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/minio/minio?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

## 1. Test Minio on Docker.
Minio generates new access and secret keys each time you run this command. Container state is lost after you end this session. This mode is only intended for testing purpose.

```sh

docker run -p 9000:9000 minio/minio server /export

```

## 2. Run Minio Standalone on Docker.

Minio container requires a persistent volume to store configuration and application data. Following command maps local persistent directories from the host OS to virtual config `~/.minio` and export `/export` directories.

```sh

docker run -p 9000:9000 --name minio1 \
  -v /mnt/export/minio1:/export \
  -v /mnt/config/minio1:/root/.minio \
  minio/minio server /export

```

## 3. Run Minio Standalone on Docker with Custom Access and Secret Keys

To override Minio's auto-generated keys, you may pass secret and access keys explicitly as environment variables. Minio server also allows regular strings as access and secret keys.

```sh

docker run -p 9000:9000 --name minio1 \
  -e "MINIO_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE" \
  -e "MINIO_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
  -v /mnt/export/minio1:/export \
  -v /mnt/config/minio1:/root/.minio \
  minio/minio server /export

```

## 4. Test Distributed Minio on Docker

This example shows how to run 4 node Minio cluster inside different docker containers using [docker-compose](https://docs.docker.com/compose/). Please download [docker-compose.yml](https://raw.githubusercontent.com/minio/minio/master/docs/docker/docker-compose.yml) to your current working directory, docker-compose pulls the Minio Docker image.

### Run `docker-compose`

```sh
docker-compose pull
docker-compose up
```

Each instance is accessible on the host at ports 9001 through 9004, proceed to access the Web browser at http://localhost:9001/
