### Run Minio docker image

## Test Minio Docker Container
Minio generates new access and secret keys each time you run this command. Container state is lost after you end this session. This mode is only intended for testing purpose.
```bash
docker run -p 9000:9000 minio/minio /export
```

## Run Minio Docker Container
Minio container requires a persistent volume to store configuration and application data. Following command maps local persistent directories from the host OS to virtual config `~/.minio` and export `/export` directories. 

```bash
docker run -p 9000:9000 --name minio1 \
  -v /mnt/export/minio1:/export \
  -v /mnt/config/minio1:/root/.minio \
  minio/minio /export
```

## Custom Access and Secret Keys
To override Minio's auto-generated keys, you may pass secret and access keys explicitly as environment variables. Minio server also allows regular strings as access and secret keys.
```bash
docker run -p 9000:9000 --name minio1 \
  -e "MINIO_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE" \
  -e "MINIO_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
  -v /mnt/export/minio1:/export \
  -v /mnt/config/minio1:/root/.minio \
  minio/minio /export
```
