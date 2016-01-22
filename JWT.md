### Generate RSA keys for JWT

```
mkdir -p ~/.minio/web
```

```
openssl genrsa -out ~/.minio/web/private.key 2048
```

```
openssl rsa -in ~/.minio/web/private.key -outform PEM -pubout -out ~/.minio/web/public.key
```
### Start minio server

```
minio server <testdir>
```

### Now you can make curl requests to the server at port 9001.

Currently username and password are defaulted for testing purposes.

```
curl -X POST -H "Content-Type: application/json" -d '{"username":"WLGDGYAQYIGI833EV05A", "password": "BYvgJM101sHngl2uzjXS/OBF/aMxAN06JrJ3qJlF"}' http://127.0.0.1:9001/login
{"token":"eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE0NTM1NDM0MjMsImlhdCI6MTQ1MzUwNzQyMywic3ViIjoiV0xHREdZQVFZSUdJODMzRVYwNUEifQ.zhL0vG5dwwak3SvpysW0CzdPRjpadrCLIpte2QHSxj2XjIQb2oK0dDD9Yvl-45E14CMVQhV3CCsf9LFaK2C94I5aop6nP7sSCyG2_l4w2xrfEPWKgyOY9P0QxUIPV3o43o2XjnMlU_6xE2mk8S9N7psk15sf0Ma1EoXkQlfqEZzbxyQjwKx4UxzkVpwN4k6wavtwU-rgVU0QwJwXXss0hVhY7HWtOzUGrhVWL42pOwNwZ73lrHpJkSyQi6fbc5lIALgFoeei_iSUXxRaJjvm36rn4vui3qLCoH79E-WhkoP_mqDvf_YfiTqcFHgdgnu2wtlQl90RNh2-wgR-XJiedQ"}
```

Replies back with a token which can be used to logout

```
curl -i -X GET -H "Authorization: Bearer eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE0NTM1NDM0MjMsImlhdCI6MTQ1MzUwNzQyMywic3ViIjoiV0xHREdZQVFZSUdJODMzRVYwNUEifQ.zhL0vG5dwwak3SvpysW0CzdPRjpadrCLIpte2QHSxj2XjIQb2oK0dDD9Yvl-45E14CMVQhV3CCsf9LFaK2C94I5aop6nP7sSCyG2_l4w2xrfEPWKgyOY9P0QxUIPV3o43o2XjnMlU_6xE2mk8S9N7psk15sf0Ma1EoXkQlfqEZzbxyQjwKx4UxzkVpwN4k6wavtwU-rgVU0QwJwXXss0hVhY7HWtOzUGrhVWL42pOwNwZ73lrHpJkSyQi6fbc5lIALgFoeei_iSUXxRaJjvm36rn4vui3qLCoH79E-WhkoP_mqDvf_YfiTqcFHgdgnu2wtlQl90RNh2-wgR-XJiedQ" http://127.0.0.1:9001/logout
HTTP/1.1 200 OK
Content-Type: application/json
Date: Sat, 23 Jan 2016 00:05:02 GMT
Content-Length: 0
```


Now attempt with wrong authorization, you should get 401.

```
$ curl -i -X GET -H "Authorization: Bearer testing123" http://127.0.0.1:9001/logout
HTTP/1.1 401 Unauthorized
Date: Sat, 23 Jan 2016 00:05:58 GMT
Content-Length: 0
Content-Type: text/plain; charset=utf-8
```

Without authorization logout is not possible.
```
$ curl -i -X GET http://127.0.0.1:9001/logout
HTTP/1.1 401 Unauthorized
Date: Sat, 23 Jan 2016 00:07:00 GMT
Content-Length: 0
Content-Type: text/plain; charset=utf-8
```
