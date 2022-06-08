# AssumeRoleWithCustomToken [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

## Introduction

To integrate with custom authentication methods using the [Identity Management Plugin](../iam/identity-management-plugin.md)), MinIO provides an STS API extension called `AssumeRoleWithCustomToken`.

After configuring the plugin, use the generated Role ARN with `AssumeRoleWithCustomToken` to get temporary credentials to access object storage.

## API Request

To make an STS API request with this method, send a POST request to the MinIO endpoint with following query parameters:

| Parameter       | Type    | Required |                                                                      |
|-----------------|---------|----------|----------------------------------------------------------------------|
| Action          | String  | Yes      | Value must be `AssumeRoleWithCustomToken`                         |
| Version         | String  | Yes      | Value must be `2011-06-15`                                           |
| Token           | String  | Yes      | Token to be authenticated by identity plugin                         |
| RoleArn         | String  | Yes      | Must match the Role ARN generated for the identity plugin            |
| DurationSeconds | Integer | No       | Duration of validity of generated credentials. Must be at least 900. |

The validity duration of the generated STS credentials is the minimum of the `DurationSeconds` parameter (if passed) and the validity duration returned by the Identity Management Plugin.

## API Response

XML response for this API is similar to [AWS STS AssumeRoleWithWebIdentity](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html#API_AssumeRoleWithWebIdentity_ResponseElements)

## Example request and response

Sample request with `curl`:

```sh
curl -XPOST 'http://localhost:9001/?Action=AssumeRoleWithCustomToken&Version=2011-06-15&Token=aaa&RoleArn=arn:minio:iam:::role/idmp-vGxBdLkOc8mQPU1-UQbBh-yWWVQ'
```

Prettified Response:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<AssumeRoleWithCustomTokenResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleWithCustomTokenResult>
    <Credentials>
      <AccessKeyId>24Y5H9VHE14H47GEOKCX</AccessKeyId>
      <SecretAccessKey>H+aBfQ9B1AeWWb++84hvp4tlFBo9aP+hUTdLFIeg</SecretAccessKey>
      <Expiration>2022-05-25T19:56:34Z</Expiration>
      <SessionToken>eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NLZXkiOiIyNFk1SDlWSEUxNEg0N0dFT0tDWCIsImV4cCI6MTY1MzUwODU5NCwiZ3JvdXBzIjpbImRhdGEtc2NpZW5jZSJdLCJwYXJlbnQiOiJjdXN0b206QWxpY2UiLCJyb2xlQXJuIjoiYXJuOm1pbmlvOmlhbTo6OnJvbGUvaWRtcC14eHgiLCJzdWIiOiJjdXN0b206QWxpY2UifQ.1tO1LmlUNXiy-wl-ZbkJLWTpaPlhaGqHehsi21lNAmAGCImHHsPb-GA4lRq6GkvHAODN5ZYCf_S-OwpOOdxFwA</SessionToken>
    </Credentials>
    <AssumedUser>custom:Alice</AssumedUser>
  </AssumeRoleWithCustomTokenResult>
  <ResponseMetadata>
    <RequestId>16F26E081E36DE63</RequestId>
  </ResponseMetadata>
</AssumeRoleWithCustomTokenResponse>
```
