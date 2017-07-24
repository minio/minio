## Minio GCS Gateway Limitations

Gateway inherits the following GCS limitations:

- Maximum number of parts per upload is 1024.
- No support for bucket policies yet.
- No support for bucket notifications yet.
- _List Multipart Uploads_ and _List Object parts_ always returns empty list. i.e Client will need to remember all the parts that it has uploaded and use it for _Complete Multipart Upload_

