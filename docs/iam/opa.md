# OPA Quickstart Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

OPA is a lightweight general-purpose policy engine that can be co-located with MinIO server, in this document we talk about how to use OPA HTTP API to authorize requests. It can be used with any type of credentials (STS based like OpenID or LDAP, regular IAM users or service accounts).

OPA is enabled through MinIO's Access Management Plugin feature.

## Get started

### 1. Start OPA in a container

```sh
podman run -it \
    --name opa \
    --publish 8181:8181 \
    docker.io/openpolicyagent/opa:0.40.0-rootless \
       run --server \
           --log-format=json-pretty \
           --log-level=debug \
           --set=decision_logs.console=true
```

### 2. Create a sample OPA Policy

In another terminal, create a policy that allows root user all access and for all other users denies `PutObject`:

```sh
cat > example.rego <<EOF
package httpapi.authz

import input

default allow = false

# Allow the root user to perform any action.
allow {
 input.owner == true
}

# All other users may do anything other than call PutObject
allow {
 input.action != "s3:PutObject"
 input.owner == false
}
EOF
```

Then load the policy via OPA's REST API.

```
curl -X PUT --data-binary @example.rego \
  localhost:8181/v1/policies/putobject
```

### 4. Setup MinIO with OPA

Set the `MINIO_POLICY_PLUGIN_URL` as the endpoint that MinIO should send authorization requests to. Then start the server.

```sh
export MINIO_POLICY_PLUGIN_URL=http://localhost:8181/v1/data/httpapi/authz/allow
export MINIO_CI_CD=1
export MINIO_ROOT_USER=minio
export MINIO_ROOT_PASSWORD=minio123
minio server /mnt/data
```

### 5. Test with a regular IAM user

Ensure that `mc` is installed and the configured with the above server with the alias `myminio`.

```sh
# 1. Create a bucket and a user, and upload a file. These operations will succeed.
mc mb myminio/test
mc admin user add myminio foo foobar123
mc cp /etc/issue myminio/test/

# 2. Now access the server as user `foo`. These operations will also succeed.
export MC_HOST_foo=http://foo:foobar123@localhost:9000
mc ls foo/test
mc cat foo/test/issue

# 3. Attempt to upload an object as user `foo` - this will fail with a permissions error.
mc cp /etc/issue myminio/test/issue2
```
