# Minio Docker Quickstart Guide [![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/minio/minio?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)


## 1. Test Minio Docker Container
Minio generates new access and secret keys each time you run this command. Container state is lost after you end this session. This mode is only intended for testing purpose.

```sh

docker run -p 9000:9000 minio/minio /export

```

## 2. Run One Minio Docker Container

Minio container requires a persistent volume to store configuration and application data. Following command maps local persistent directories from the host OS to virtual config `~/.minio` and export `/export` directories. 

```sh

docker run -p 9000:9000 --name minio1 \
  -v /mnt/export/minio1:/export \
  -v /mnt/config/minio1:/root/.minio \
  minio/minio /export

```

## 3. Custom Access and Secret Keys

To override Minio's auto-generated keys, you may pass secret and access keys explicitly as environment variables. Minio server also allows regular strings as access and secret keys.

```sh

docker run -p 9000:9000 --name minio1 \
  -e "MINIO_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE" \
  -e "MINIO_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
  -v /mnt/export/minio1:/export \
  -v /mnt/config/minio1:/root/.minio \
  minio/minio /export

```

## 4. Run Minio in clustered mode

Let's consider that we need to run 4 minio servers inside different docker instances.

### Prepare and start Minio docker instances
```sh
$ docker run -d --name=minio1 -e "MINIO_ACCESS_KEY=abcde" -e "MINIO_SECRET_KEY=secret00" -it -v/mnt/export/minio1:/export minio/minio
$ docker run -d --name=minio2 -e "MINIO_ACCESS_KEY=abcde" -e "MINIO_SECRET_KEY=secret00" -it -v/mnt/export/minio2:/export minio/minio
$ docker run -d --name=minio3 -e "MINIO_ACCESS_KEY=abcde" -e "MINIO_SECRET_KEY=secret00" -it -v/mnt/export/minio3:/export minio/minio
$ docker run -d --name=minio4 -e "MINIO_ACCESS_KEY=abcde" -e "MINIO_SECRET_KEY=secret00" -it -v/mnt/export/minio4:/export minio/minio
```

### Get docker instances IP addresses:
```sh
$ docker ps -q | xargs -n 1 docker inspect  --format '{{ .NetworkSettings.IPAddress }}'
172.17.0.2
172.17.0.3
172.17.0.4
172.17.0.5
```

### Run all minio instances
```sh
$ docker exec -d minio1 minio server 172.17.0.2:/export 172.17.0.3:/export 172.17.0.4:/export 172.17.0.5:/export
$ docker exec -d minio2 minio server 172.17.0.2:/export 172.17.0.3:/export 172.17.0.4:/export 172.17.0.5:/export
$ docker exec -d minio3 minio server 172.17.0.2:/export 172.17.0.3:/export 172.17.0.4:/export 172.17.0.5:/export
$ docker exec -d minio4 minio server 172.17.0.2:/export 172.17.0.3:/export 172.17.0.4:/export 172.17.0.5:/export
```
