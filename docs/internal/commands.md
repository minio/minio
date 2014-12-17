User Commands
=============
* put [bucket] [path] [file]
* put [bucket] [path] < stdin
* get [bucket] [path] > stdout
* verify-bucket [bucket]
* verify-object [bucket] [path]
* fix-bucket [bucket]
* fix-object [bucket] [path]

stderr prints json on error

System Commands
===============
* initialize-repo
* split-stream
* merge-stream
* encode
* decode
* add-to-index
* add-to-store
* get-from-index
* get-from-store
* crc
* md5sum-stream
* verify-repo
* verify-object
* whitelist-failure

Potential Workflow Pseudocode (single pass)
=============================
```sh`
add /bucket/name local-file
    localFile : io.Reader = open(file)
    md5sum-stream localFile
    chunks = split-file localFile
    for each chunk in chunks:
        encode chunk
        crc chunk
        add-to-store name,chunk,length,crc
    add-to-index name,chunkcount,md5,ts
```
