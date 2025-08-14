# WSO2 Quickstart Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

WSO2 is an Identity Server open source and is released under Apache Software License Version 2.0, this document covers configuring WSO2 to be used as an identity provider for MinIO server STS API.

## Get started

### 1. Prerequisites

- JAVA 1.8 and above installed already and JAVA_HOME points to JAVA 1.8 installation.
- Download WSO2 follow their [installation guide](https://docs.wso2.com/display/IS540/Installation+Guide).

### 2. Configure WSO2

Once WSO2 is up and running, configure WSO2 to generate Self contained id_tokens. In OAuth 2.0 specification there are primarily two ways to provide id_tokens

1. The id_token is an identifier that is hard to guess. For example, a randomly generated string of sufficient length, that the server handling the protected resource can use to lookup the associated authorization information.
2. The id_token self-contains the authorization information in a manner that can be verified. For example, by encoding authorization information along with a signature into the token.

WSO2 generates tokens in first style by default, but if to be used with MinIO we should configure WSO2 to provide JWT tokens instead.

### 3. Generate Self-contained Access Tokens

By default, a UUID is issued as an id_token in WSO2 Identity Server, which is of the first type above. But, it also can be configured to issue a self-contained id_token (JWT), which is of the second type above.

- Open the `<IS_HOME>/repository/conf/identity/identity.xml` file and uncomment the following entry under `<OAuth>` element.

```
<IdentityOAuthTokenGenerator>org.wso2.carbon.identity.oauth2.token.JWTTokenIssuer</IdentityOAuthTokenGenerator>
```

- Restart the server.
- Configure an [OAuth service provider](https://docs.wso2.com/display/IS540/Adding+and+Configuring+a+Service+Provider).
- Initiate an id_token request to the WSO2 Identity Server, over a known [grant type](https://docs.wso2.com/display/IS540/OAuth+2.0+Grant+Types). For example, the following cURL command illustrates the syntax of an id_token request that can be initiated over the [Client Credentials Grant](https://docs.wso2.com/display/IS540/Client+Credentials+Grant) grant type.
  - Navigate to service provider section, expand Inbound Authentication Configurations and expand OAuth/OpenID Connect Configuration.
    - Copy the OAuth Client Key as the value for `<CLIENT_ID>`.
    - Copy the OAuth Client Secret as the value for `<CLIENT_SECRET>`.
  - By default, `<IS_HOST>` is localhost. However, if using a public IP, the respective IP address or domain needs to be specified.
  - By default, `<IS_HTTPS_PORT>` has been set to 9443. However, if the port offset has been incremented by n, the default port value needs to be incremented by n.

Request

```
curl -u <CLIENT_ID>:<CLIENT_SECRET> -k -d "grant_type=client_credentials" -H "Content-Type:application/x-www-form-urlencoded" https://<IS_HOST>:<IS_HTTPS_PORT>/oauth2/token
```

Example:

```
curl -u PoEgXP6uVO45IsENRngDXj5Au5Ya:eKsw6z8CtOJVBtrOWvhRWL4TUCga -k -d "grant_type=client_credentials" -H "Content-Type:application/x-www-form-urlencoded" https://localhost:9443/oauth2/token
```

In response, the self-contained JWT id_token will be returned as shown below.

```
{
  "id_token": "eyJ4NXQiOiJOVEF4Wm1NeE5ETXlaRGczTVRVMVpHTTBNekV6T0RKaFpXSTRORE5sWkRVMU9HRmtOakZpTVEiLCJraWQiOiJOVEF4Wm1NeE5ETXlaRGczTVRVMVpHTTBNekV6T0RKaFpXSTRORE5sWkRVMU9HRmtOakZpTVEiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiJQb0VnWFA2dVZPNDVJc0VOUm5nRFhqNUF1NVlhIiwiYXpwIjoiUG9FZ1hQNnVWTzQ1SXNFTlJuZ0RYajVBdTVZYSIsImlzcyI6Imh0dHBzOlwvXC9sb2NhbGhvc3Q6OTQ0M1wvb2F1dGgyXC90b2tlbiIsImV4cCI6MTUzNDg5MTc3OCwiaWF0IjoxNTM0ODg4MTc4LCJqdGkiOiIxODQ0MzI5Yy1kNjVhLTQ4YTMtODIyOC05ZGY3M2ZlODNkNTYifQ.ELZ8ujk2Xp9xTGgMqnCa5ehuimaAPXWlSCW5QeBbTJIT4M5OB_2XEVIV6p89kftjUdKu50oiYe4SbfrxmLm6NGSGd2qxkjzJK3SRKqsrmVWEn19juj8fz1neKtUdXVHuSZu6ws_bMDy4f_9hN2Jv9dFnkoyeNT54r4jSTJ4A2FzN2rkiURheVVsc8qlm8O7g64Az-5h4UGryyXU4zsnjDCBKYk9jdbEpcUskrFMYhuUlj1RWSASiGhHHHDU5dTRqHkVLIItfG48k_fb-ehU60T7EFWH1JBdNjOxM9oN_yb0hGwOjLUyCUJO_Y7xcd5F4dZzrBg8LffFmvJ09wzHNtQ",
  "token_type": "Bearer",
  "expires_in": 3600
}
```

### 4. JWT Claims

The id_token received is a signed JSON Web Token (JWT). Use a JWT decoder to decode the id_token to access the payload of the token that includes following JWT claims:

| Claim Name | Type           | Claim Value                                                                                                                                                                             |
|:----------:|:--------------:|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|
| iss        | _string_       | The issuer of the JWT. The '> Identity Provider Entity Id ' value of the OAuth2/OpenID Connect Inbound Authentication configuration of the Resident Identity Provider is returned here. |
| aud        | _string array_ | The token audience list. The client identifier of the OAuth clients that the JWT is intended for, is sent herewith.                                                                     |
| azp        | _string_       | The authorized party for which the token is issued to. The client identifier of the OAuth client that the token is issued for, is sent herewith.                                        |
| iat        | _integer_      | The token issue time.                                                                                                                                                                   |
| exp        | _integer_      | The token expiration time.                                                                                                                                                              |
| jti        | _string_       | Unique identifier for the JWT token.                                                                                                                                                    |
| policy     | _string_       | Canned policy name to be applied for STS credentials. (Recommended)                                                                                                                     |

Using the above `id_token` we can perform an STS request to MinIO to get temporary credentials for MinIO API operations. MinIO STS API uses [JSON Web Key Set Endpoint](https://docs.wso2.com/display/IS541/JSON+Web+Key+Set+Endpoint) to validate if JWT is valid and is properly signed.

**We recommend setting `policy` as a custom claim for the JWT service provider follow [here](https://docs.wso2.com/display/IS550/Configuring+Claims+for+a+Service+Provider) and [here](https://docs.wso2.com/display/IS550/Handling+Custom+Claims+with+the+JWT+Bearer+Grant+Type) for relevant docs on how to configure claims for a service provider.**

### 5. Setup MinIO with OpenID configuration URL

MinIO server expects environment variable for OpenID configuration url as `MINIO_IDENTITY_OPENID_CONFIG_URL`, this environment variable takes a single entry.

```
export MINIO_IDENTITY_OPENID_CONFIG_URL=https://localhost:9443/oauth2/oidcdiscovery/.well-known/openid-configuration
export MINIO_IDENTITY_OPENID_CLIENT_ID="843351d4-1080-11ea-aa20-271ecba3924a"
minio server /mnt/data
```

Assuming that MinIO server is configured to support STS API by following the doc [MinIO STS Quickstart Guide](https://docs.min.io/community/minio-object-store/developers/security-token-service.html), execute the following command to temporary credentials from MinIO server.

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

- [MinIO STS Quickstart Guide](https://docs.min.io/community/minio-object-store/developers/security-token-service.html)
- [The MinIO documentation website](https://docs.min.io/community/minio-object-store/index.html)
