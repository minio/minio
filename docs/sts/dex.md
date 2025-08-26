# Dex Quickstart Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

Dex is an identity service that uses OpenID Connect to drive authentication for apps. Dex acts as a portal to other identity providers through "connectors." This lets dex defer authentication to LDAP servers, SAML providers, or established identity providers like GitHub, Google, and Active Directory. Clients write their authentication logic once to talk to dex, then dex handles the protocols for a given backend.

## Prerequisites

Install Dex by following [Dex Getting Started Guide](https://dexidp.io/docs/getting-started/)

### Start Dex

```
~ ./bin/dex serve dex.yaml
time="2020-07-12T20:45:50Z" level=info msg="config issuer: http://127.0.0.1:5556/dex"
time="2020-07-12T20:45:50Z" level=info msg="config storage: sqlite3"
time="2020-07-12T20:45:50Z" level=info msg="config static client: Example App"
time="2020-07-12T20:45:50Z" level=info msg="config connector: mock"
time="2020-07-12T20:45:50Z" level=info msg="config connector: local passwords enabled"
time="2020-07-12T20:45:50Z" level=info msg="config response types accepted: [code token id_token]"
time="2020-07-12T20:45:50Z" level=info msg="config using password grant connector: local"
time="2020-07-12T20:45:50Z" level=info msg="config signing keys expire after: 3h0m0s"
time="2020-07-12T20:45:50Z" level=info msg="config id tokens valid for: 3h0m0s"
time="2020-07-12T20:45:50Z" level=info msg="listening (http) on 0.0.0.0:5556"
```

### Configure MinIO server with Dex

```
~ export MINIO_IDENTITY_OPENID_CLAIM_NAME=name
~ export MINIO_IDENTITY_OPENID_CONFIG_URL=http://127.0.0.1:5556/dex/.well-known/openid-configuration
~ minio server ~/test
```

### Run the `web-identity.go`

```
~ go run web-identity.go -cid example-app -csec ZXhhbXBsZS1hcHAtc2VjcmV0 \
     -config-ep http://127.0.0.1:5556/dex/.well-known/openid-configuration \
     -cscopes groups,openid,email,profile
```

```
~ mc admin policy create admin allaccess.json
```

Contents of `allaccess.json`

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:*"
      ],
      "Resource": [
        "arn:aws:s3:::*"
      ]
    }
  ]
}
```

### Visit <http://localhost:8080>

You will be redirected to dex login screen - click "Login with email", enter username password
> username: admin@example.com
> password: password

and then click "Grant access"

On the browser now you shall see the list of buckets output, along with your temporary credentials obtained from MinIO.

```
{
 "buckets": [
  "dl.minio.equipment",
  "dl.minio.service-fulfillment",
  "testbucket"
 ],
 "credentials": {
  "AccessKeyID": "Q31CVS1PSCJ4OTK2YVEM",
  "SecretAccessKey": "rmDEOKARqKYmEyjWGhmhLpzcncyu7Jf8aZ9bjDic",
  "SessionToken": "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NLZXkiOiJRMzFDVlMxUFNDSjRPVEsyWVZFTSIsImF0X2hhc2giOiI4amItZFE2OXRtZEVueUZaMUttNWhnIiwiYXVkIjoiZXhhbXBsZS1hcHAiLCJlbWFpbCI6ImFkbWluQGV4YW1wbGUuY29tIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsImV4cCI6IjE1OTQ2MDAxODIiLCJpYXQiOjE1OTQ1ODkzODQsImlzcyI6Imh0dHA6Ly8xMjcuMC4wLjE6NTU1Ni9kZXgiLCJuYW1lIjoiYWRtaW4iLCJzdWIiOiJDaVF3T0dFNE5qZzBZaTFrWWpnNExUUmlOek10T1RCaE9TMHpZMlF4TmpZeFpqVTBOallTQld4dlkyRnMifQ.nrbzIJz99Om7TvJ04jnSTmhvlM7aR9hMM1Aqjp2ONJ1UKYCvegBLrTu6cYR968_OpmnAGJ8vkd7sIjUjtR4zbw",
  "SignerType": 1
 }
}
```

Now you have successfully configured Dex IdP with MinIO.

> NOTE: Dex supports groups with external connectors so you can use `groups` as policy claim instead of `name`.

```
export MINIO_IDENTITY_OPENID_CLAIM_NAME=groups
```

and add relevant policies on MinIO using `mc admin policy create myminio/ <group_name> group-access.json`

## Explore Further

- [MinIO STS Quickstart Guide](https://docs.min.io/community/minio-object-store/developers/security-token-service.html)
- [The MinIO documentation website](https://docs.min.io/community/minio-object-store/index.html)
