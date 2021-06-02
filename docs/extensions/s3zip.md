# Perform S3 operations in a ZIP content[![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

### Overview
MinIO implements an S3 extension to list, stat and download files inside a ZIP file stored in any bucket. A perfect use case scenario is when you have a lot of small files archived in multiple ZIP files. Uploading them is faster than uploading small files individually. Besides, your S3 applications will be able to access to the data with little performance overhead.

### How to enable S3 ZIP extension ?

Just use mc to enable it: `mc admin config set <your-alias> api s3zip=on`

### How to access to files inside a ZIP archive

After enabling S3 ZIP extension and uploading archives with '.zip' extension, you can access data by invoking regular S3 API, with modified path. Therefore, accessing to a file inside the object `taxes.txt` inside `archive.zip` can be done with the following path `archive.zip/taxes.txt`.

e.g.:
```
$ mc cp archive.zip myminio/testbucket/
archive.zip:    791 B / 791 B ┃▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓┃ 80 MiB/s 0s
...
$ mc ls -r myminio/testbucket/archive.zip/
[2021-04-02 12:49:34 CET] 3.4KiB financial-2021-04.txt
[2021-05-02 12:49:34 CET]   624B financial-2021-05.txt
.... the rest of archive content list ..
$ mc cat myminio/testbucket/archive.zip/financial-2021-05.txt
.... financial csv content ...
```

### Limitation
- Listing does not understand S3 versioning, you cannot list the content of a specific version of an archive.

