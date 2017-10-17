## Minio GCS 网关限制

网关继承了以下GCS限制:

- 每次上传最多parts数量是1024。
- 暂不支持存储桶策略。
- 暂不支持存储桶通知。
- _List Multipart Uploads_ 和 _List Object parts_ 会一直返回空List,即客户端需要记住已经上传的parts,并使用它用于 _Complete Multipart Upload_。

