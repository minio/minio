### 1. Run LDAP service
`yarn run-docker-ldap-service`

### 2. Add minio config for LDAP
`mc config host add myminio http://127.0.0.1:9000 minioadmin minioadmin`

```
cat <<EOF> allaccess.json
{
  "Statement": [
    {
      "Resource": [
        "arn:aws:s3:::*"
      ],
      "Action": [
        "s3:*"
      ],
      "Effect": "Allow"
    }
  ],
  "Version": "2012-10-17"
}
EOF
```

`mc admin policy add myminio allaccess allaccess.json`

`mc admin policy set myminio allaccess user='cn=Philip J. Fry,ou=people,dc=planetexpress,dc=com'`

`yarn enable-minio-identity-ldap`

### 3. Start minio server

`mkdir -p export/{bucket1,bucket2}`

`touch export/{bucket1,bucket2}/emptyfile.txt`

`minio server ./export`

### 4. Run node.js STS client
#### Install dependencies
`yarn install`

#### Run STS client
`node ldap.js`
