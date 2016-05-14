## Multipart

Each incoming part is uploaded in the following format.

Placeholder uploads.json to indicate a leaf.

 ```EXPORT_DIR/.minio/multipart/BUCKET/PATH/TO/OBJECT/uploads.json```

Incomplete file

 ```EXPORT_DIR/.minio/multipart/BUCKET/PATH/TO/OBJECT/UPLOADID/00000.incomplete```

Actual parts

 ```EXPORT_DIR/.minio/multipart/BUCKET/PATH/TO/OBJECT/UPLOADID/PART_NUMBER.MD5SUM_STRING```

## FS Format.

Each of these parts are concatenated back to a single contigous file.

```EXPORT_DIR/BUCKET/PATH/TO/OBJECT```

## XL Format.

Each of these parts are kept as is in the following format.

Special json file indicate the metata multipart information, essentially list of parts etc.

```EXPORT_DIR/BUCKET/PATH/TO/OBJECT/00000.minio.multipart```

All the parts that were uploaded.

```EXPORT_DIR/BUCKET/PATH/TO/OBJECT/PART_NUMBER.minio.multipart```
