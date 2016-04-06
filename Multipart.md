## Multipart backend format

When multipart upload is used for objects, below meta-data/staging files are created 

- New multipart upload call creates file ```EXPORT_DIR/.minio/BUCKET/PATH/TO/OBJECT/UPLOAD_ID.uploadid```
- Put object part call creates file ```EXPORT_DIR/.minio/BUCKET/PATH/TO/OBJECT/UPLOAD_ID.PART_NUMBER.MD5SUM_STRING```
- Abort multipart call removes all files matching ```EXPORT_DIR/.minio/BUCKET/PATH/TO/OBJECT/UPLOAD_ID.*```
- Complete multipart call does
  1. Create a staging file ```EXPORT_DIR/.minio/BUCKET/PATH/TO/OBJECT/UPLOAD_ID.complete.TEMP_NAME``` then rename to ```EXPORT_DIR/.minio/BUCKET/PATH/TO/OBJECT/UPLOAD_ID.complete```
  2. Rename staging file ```EXPORT_DIR/.minio/BUCKET/PATH/TO/OBJECT/UPLOAD_ID.complete``` to ```EXPORT_DIR/BUCKET/PATH/TO/OBJECT```
