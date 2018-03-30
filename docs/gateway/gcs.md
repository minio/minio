# Minio GCS Gateway [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)
Minio GCS Gateway adds Amazon S3 compatibility to Google Cloud Storage.

## Run Minio Gateway for GCS
### Create service account key for GCS and get the credentials file
1. Go to the [API Console Credentials page](https://console.developers.google.com/project/_/apis/credentials).
2. Select your project or create a new project. Note down your project ID.
3. On the Credentials page, select the __Create credentials__ drop-down, then select __Service account key__.
4. From the __Service account__ drop-down, select __New service account__
5. Fill up __Service account name__ and __Service account ID__
6. For the __Role__, click the selec dropdown to choose __Storage__ -> __Storage Admin__ _(Full control of GCS resources)_
7. Click the __Create__ button. This will download a credentials file to your desktop. Let's call this credentials.json

Note: Alternate ways to setup *Application Default Credentials* is explained [here](https://developers.google.com/identity/protocols/application-default-credentials)

### Using Docker
```
docker run -p 9000:9000 --name gcs-s3 \
 -v /path/to/credentials.json:/credentials.json \
 -e "GOOGLE_APPLICATION_CREDENTIALS=/credentials.json" \
 -e "MINIO_ACCESS_KEY=minioaccountname" \
 -e "MINIO_SECRET_KEY=minioaccountkey" \
 minio/minio gateway gcs yourprojectid
```

### Using Binary
```
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
export MINIO_ACCESS_KEY=minioaccesskey
export MINIO_SECRET_KEY=miniosecretkey
minio gateway gcs yourprojectid
```

## Test using Minio Browser
Minio Gateway comes with an embedded web based object browser. Point your web browser to http://127.0.0.1:9000 to ensure that your server has started successfully.

![Screenshot](https://github.com/minio/minio/blob/master/docs/screenshots/minio-browser-gateway.png?raw=true)

## Test using Minio Client `mc`
`mc` provides a modern alternative to UNIX commands such as ls, cat, cp, mirror, diff etc. It supports filesystems and Amazon S3 compatible cloud storage services.

### Configure `mc`
```
mc config host add mygcs http://gateway-ip:9000 minioaccesskey miniosecretkey
```

### List containers on GCS
```
mc ls mygcs
[2017-02-22 01:50:43 PST]     0B ferenginar/
[2017-02-26 21:43:51 PST]     0B my-container/
[2017-02-26 22:10:11 PST]     0B test-container1/
```

### Known limitations
Gateway inherits the following GCS limitations:

- Maximum number of multipart parts per upload is 1024.
- Only read-only or write-only bucket policy supported at bucket level, all other variations will return API Notimplemented error.
- _List Multipart Uploads_ and _List Object parts_ always returns empty list. i.e Client will need to remember all the parts that it has uploaded and use it for _Complete Multipart Upload_

Other limitations:

- Bucket notification APIs are not supported.

## Explore Further
- [`mc` command-line interface](https://docs.minio.io/docs/minio-client-quickstart-guide)
- [`aws` command-line interface](https://docs.minio.io/docs/aws-cli-with-minio)
- [`minio-go` Go SDK](https://docs.minio.io/docs/golang-client-quickstart-guide)

