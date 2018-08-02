# 管理REST API

## 认证
- AWS signatureV4
- 我们使用`minio`作为区域。 这里区域仅用于签名计算。

##管理接口
- Service
  - Restart
  - Status
  - SetCredentials

- Locks
  - List
  - Clear

- Healing

### 服务管理接口
* Restart
  - POST /?service
  - x-minio-operation: restart
  - Response: On success 200

* Status
  - GET /?service
  - x-minio-operation: status
  - Response: On success 200, return json formatted object which contains StorageInfo and ServerVersion structures

* SetCredentials
  - GET /?service
  - x-minio-operation: set-credentials
  - Response: Success 200
  - Possible error responses
    - ErrMethodNotAllowed
    ```xml
    <Error>
        <Code>MethodNotAllowed</Code>
        <Message>The specified method is not allowed against this resource.</Message>
        <Key></Key>
        <BucketName></BucketName>
        <Resource>/</Resource>
        <RequestId>3L137</RequestId>
        <HostId>3L137</HostId>
    </Error>
    ```
    - ErrAdminBadCred
    ```xml
    <Error>
        <Code>XMinioBadCred</Code>
        <Message>XMinioBadCred</Message>
        <Key></Key>
        <BucketName></BucketName>
        <Resource>/</Resource>
        <RequestId>3L137</RequestId>
        <HostId>3L137</HostId>
    </Error>
    ```
    - ErrInternalError
    ```xml
    <Error>
        <Code>InternalError</Code>
        <Message>We encountered an internal error, please try again.</Message>
        <Key></Key>
        <BucketName></BucketName>
        <Resource>/</Resource>
        <RequestId>3L137</RequestId>
        <HostId>3L137</HostId>
    </Error>
    ```

### 修复

* ListBucketsHeal
  - GET /?heal
  - x-minio-operation: list-buckets
