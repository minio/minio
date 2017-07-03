## Minio GCS Gateway Limitations

Gateway inherits the following GCS limitations:

- Maximum number of multiparts is 1024.
- No support for prefix based bucket policies. Only top level bucket policy is supported.
- _List Multipart Uploads_ and _List Object parts_ always returns empty list. i.e Client will need to remember all the parts that it has uploaded and use it for _Complete Multipart Upload_

Other limitations:
- Bucket notification not supported.
