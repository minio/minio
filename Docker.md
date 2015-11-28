# docker run

To run docker image:
```docker run -p 9000:9000 minio/minio:latest```

This will start minio server in the docker container, the data however is not persistent.
If you need persistent storage you can use the command:

```docker run -p 9000:9000 -v ${HOME}/.minio:/.minio -v ${HOME}/export:/export minio:latest```

Here the data uploaded to the minio server will be persisted to ${HOME}/export directory.

# docker build

To build the docker image:
```make dockerimage TAG="<tag>"```
