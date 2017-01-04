# Management REST API

## Authentication
- AWS signatureV4
- We use "minio" as region. Here region is set only for signature calculation.

## List of management APIs
- Service
  - Stop
  - Restart
  - Status

- Locks
  - List
  - Clear

- Healing

### Service Management APIs
* Stop
  - POST /?service
  - x-minio-operation: stop
  - Response: On success 200

* Restart
  - POST /?service
  - x-minio-operation: restart
  - Response: On success 200

* Status
  - GET /?service
  - x-minio-operation: status
  - Response: On success 200, return json formatted StorageInfo object.

### Lock Management APIs
* ListLocks
  - GET /?lock&bucket=mybucket&prefix=myprefix&older-than=rel_time
  - x-minio-operation: list
  - Response: On success 200, json encoded response containing all locks held, older than rel_time. e.g, older than 3 hours.
  - Possible error responses
    - ErrInvalidBucketName
    <Error>
        <Code>InvalidBucketName</Code>
        <Message>The specified bucket is not valid.</Message>
        <Key></Key>
        <BucketName></BucketName>
        <Resource>/</Resource>
        <RequestId>3L137</RequestId>
        <HostId>3L137</HostId>
    </Error>

    - ErrInvalidObjectName
    <Error>
        <Code>XMinioInvalidObjectName</Code>
        <Message>Object name contains unsupported characters. Unsupported characters are `^*|\&#34;</Message>
        <Key></Key>
        <BucketName></BucketName>
        <Resource>/</Resource>
        <RequestId>3L137</RequestId>
        <HostId>3L137</HostId>
    </Error>

    - ErrInvalidDuration
      <Error>
          <Code>InvalidDuration</Code>
          <Message>Relative duration provided in the request is invalid.</Message>
          <Key></Key>
          <BucketName></BucketName>
          <Resource>/</Resource>
          <RequestId>3L137</RequestId>
          <HostId>3L137</HostId>
      </Error>


* ClearLocks
  - POST /?lock&bucket=mybucket&prefix=myprefix&older-than=rel_time
  - x-minio-operation: clear
  - Response: On success 200, json encoded response containing all locks cleared, older than rel_time. e.g, older than 3 hours.
  - Possible error responses, similar to errors listed in ListLocks.
    - ErrInvalidBucketName
    - ErrInvalidObjectName
    - ErrInvalidDuration
