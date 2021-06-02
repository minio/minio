# Perform S3 operations in a ZIP content[![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

### Overview
MinIO implements an S3 extension to list, stat and download files inside a ZIP file stored in any bucket. A perfect use case scenario is when you have a lot of small files archived in multiple ZIP files. Uploading them is faster than uploading small files individually. Besides, your S3 applications will be able to access to the data with little performance overhead.

The main limitation is that to update or delete content of a file inside a ZIP file the entire ZIP file must be replaced.

### How to enable S3 ZIP extension ?

Just use mc to enable it: `mc admin config set <your-alias> api s3zip=on`

### How to access to files inside a ZIP archive

After enabling S3 ZIP extension and uploading archives with '.zip' extension, you can access data by invoking regular S3 API, with modified path. 

Accessing to a file inside the object `taxes.txt` inside `archive.zip` can be done with the following path `archive.zip/taxes.txt`.

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

All properties except the file size are tied to the zip file. This means that modification date, headers, tags, etc. can only be set for the zip file as a whole. In similar fashion, replication will replicate the zip file as a whole and not individual files.

### Limitation
- Listing only operates on the most recent version of your object.
- ListObjectsV2 API calls must be used to list zip file content.
- Names inside ZIP files are kept unmodified, but some may lead to invalid paths. See [Object key naming guidelines](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html) on safe names.
- The following API calls supports operations on files inside ZIP files: `HeadObject`,  `GetObject`. Other calls can only operate on the zip file as a whole.
- Keeping the number of files inside a single ZIP below 100,000 is recommended to keep individual files accesses fast.
- If the ZIP file directory isn't located within the last 100MB the file will not be parsed.
