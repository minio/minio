### List of Error Responses

This section provides information about Minio API errors. XML and JSON results have been prettified for readability. As a result, the Content-Length may not match exactly for both styles

#### Error Codes Table

The following table lists Minio error codes.

| Error Code | Description | HTTP Status Code |
|:---------- |:----------- |:----------------:|
| AccessDenied | Access Denied | 403 Forbidden |
| BadDigest | The Content-MD5 you specified did not match what we received. | 400 Bad Request |
| BucketAlreadyExists | The requested bucket name is not available. | 409 Conflict |
| EntityTooSmall | Your proposed upload is smaller than the minimum allowed object size. | 400 Bad Request |
| EntityTooLarge | Your proposed upload exceeds the maximum allowed object size. | 400 Bad Request |
| IncompleteBody | You did not provide the number of bytes specified by the Content-Length HTTP header | 400 Bad Request |
| InternalError | We encountered an internal error. Please try again. | 500 Internal Server Error |
| InvalidAccessKeyId | The access key Id you provided does not exist in our records. | 403 Forbidden |
| InvalidBucketName | The specified bucket is not valid. | 400 Bad Request |
| InvalidDigest | The Content-MD5 you specified is not valid. | 400 Bad Request |
| InvalidRange | The requested range cannot be satisfied. | 416 Requested Range Not Satisfiable |
| MalformedXML | The XML you provided was not well-formed or did not validate against our published schema. | 400 Bad Request |
| MissingContentLength | You must provide the Content-Length HTTP header. | 411 Length Required |
| MissingRequestBodyError | Request body is empty. | 400 Bad Request |
| NoSuchBucket | The specified bucket does not exist. | 404 Not Found |
| NoSuchKey | The specified key does not exist. | 404 Not Found |
| NoSuchUpload | The specified multipart upload does not exist | 404 Not Found |
| NotImplemented | A header you provided implies functionality that is not implemented. | 501 Not Implemented |
| RequestTimeTooSkewed | The difference between the request time and the server's time is too large. | 403 Forbidden |
| SignatureDoesNotMatch | The request signature we calculated does not match the signature you provided. | 403 Forbidden |
| TooManyBuckets | You have attempted to create more buckets than allowed. | 400 Bad Request |
<br />

#### REST Error Responses

When there is an error, the header information contains:

 - Content-Type: application/xml or application/json (Depending on ``Accept`` HTTP header)
 - An appropriate 4xx, or 5xx HTTP status code

The body or the response also contains information about the error. The following sample error response shows the structure of response elements common to all REST error responses.

```
<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchKey</Code>
  <Message>The resource you requested does not exist</Message>
  <Resource>/minio-bucket/images.bz2</Resource>
  <RequestId>********</RequestId>
</Error>
```

The following table explains the REST error response elements

| Name | Description |
|:---------- |:-----------|
|*Code*| The error code is a string that uniquely identifies an error condition. <br />*Type: String*<br />*Ancestor: Error*|
|*Error*| XML container for error. <br />*Type: Container*<br />*Ancestor: None*|
|*Message*| The error message contains a generic description of the error condition. <br />*Type: String*<br />*Ancestor: Error*|
|*RequestId*| ID of the request associated with the error. <br />*Type: String*<br />*Ancestor: Error*|
|*Resource*| The bucket or object for which the error generated. <br />*Type: String*<br />*Ancestor: Error*|
