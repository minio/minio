# Service REST API

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
