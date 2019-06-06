**Using OPA is optional with MinIO. We recommend using [`policy` JWT claims](https://github.com/minio/minio/blob/master/docs/sts/wso2.md#4-jwt-claims) instead, let MinIO manage your policies using `mc admin policy` and apply them on the STS credentials.**

# OPA Quickstart Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)
OPA is a lightweight general-purpose policy engine that can be co-located with MinIO server, in this document we talk about how to use OPA HTTP API to authorize MinIO STS credentials.

## Get started
### 1. Prerequisites
- Docker 18.03 or above, refer here for [installation](https://docs.docker.com/install/).
- Docker compose 1.20 or above, refere here for [installation](https://docs.docker.com/compose/install/#prerequisites).

### 2. Start OPA
First, create a `docker-compose.yml` file that runs OPA and the demo web server.
```
cat >docker-compose.yml <<EOF
version: '2'
services:
  opa:
    image: openpolicyagent/opa:0.9.1
    ports:
      - 8181:8181
    command:
      - "run"
      - "--server"
      - "--log-level=debug"
  api_server:
    image: openpolicyagent/demo-restful-api:0.2
    ports:
      - 5000:5000
    environment:
      - OPA_ADDR=http://opa:8181
      - POLICY_PATH=/v1/data/httpapi/authz
EOF
```

Then run `docker-compose` to pull and run the containers.
```
docker-compose -f docker-compose.yml up
```

### 3. Create new OPA Policy
In another terminal, create a policy that allows users to upload objects
```
cat > putobject.rego <<EOF
package httpapi.authz

import input as http_api

allow {
 input.action = "s3:PutObject"
 input.owner = false
}

EOF
```

Then load the policy via OPA's REST API.
```
curl -X PUT --data-binary @putobject.rego \
  localhost:8181/v1/policies/putobject
```

### 4. Setup MinIO with OPA
MinIO server expects environment variable for OPA http API url as `MINIO_IAM_OPA_URL`, this environment variable takes a single entry.
```
export MINIO_IAM_OPA_URL=http://localhost:8181/v1/data/httpapi/authz
minio server /mnt/data
```

### 5. Test with MinIO STS API
Assuming that MinIO server is configured to support STS API by following the doc [MinIO STS Quickstart Guide](https://docs.min.io/docs/minio-sts-quickstart-guide), execute the following command to temporary credentials from MinIO server.
```
go run client-grants.go -cid PoEgXP6uVO45IsENRngDXj5Au5Ya -csec eKsw6z8CtOJVBtrOWvhRWL4TUCga

##### Credentials
{
	"accessKey": "IRBLVDGN5QGMDCMO1X8V",
	"secretKey": "KzS3UZKE7xqNdtRbKyfcWgxBS6P1G4kwZn4DXKuY",
	"expiration": "2018-08-21T15:49:38-07:00",
	"sessionToken": "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NLZXkiOiJJUkJMVkRHTjVRR01EQ01PMVg4ViIsImF1ZCI6IlBvRWdYUDZ1Vk80NUlzRU5SbmdEWGo1QXU1WWEiLCJhenAiOiJQb0VnWFA2dVZPNDVJc0VOUm5nRFhqNUF1NVlhIiwiZXhwIjoxNTM0ODkxNzc4LCJpYXQiOjE1MzQ4ODgxNzgsImlzcyI6Imh0dHBzOi8vbG9jYWxob3N0Ojk0NDMvb2F1dGgyL3Rva2VuIiwianRpIjoiMTg0NDMyOWMtZDY1YS00OGEzLTgyMjgtOWRmNzNmZTgzZDU2In0.4rKsZ8VkZnIS_ALzfTJ9UbEKPFlQVvIyuHw6AWTJcDFDVgQA2ooQHmH9wUDnhXBi1M7o8yWJ47DXP-TLPhwCgQ"
}
```

These credentials can now be used to perform MinIO API operations, these credentials automatically expire in 1hr. To understand more about credential expiry duration and client grants STS API read further [here](https://github.com/minio/minio/blob/master/docs/sts/client-grants.md).

## Explore Further
- [MinIO STS Quickstart Guide](https://docs.min.io/docs/minio-sts-quickstart-guide)
- [The MinIO documentation website](https://docs.min.io)
