# MinIO GCS Gateway [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

MinIO GCS Gateway allows you to access Google Cloud Storage (GCS) with Amazon S3-compatible APIs

- [Run MinIO Gateway for GCS](#run-minio-gateway-for-gcs)
- [Test Using MinIO Console](#test-using-minio-browser)
- [Test Using MinIO Client](#test-using-minio-client)

## 1. Run MinIO Gateway for GCS

### 1.1 Create a Service Account key for GCS and get the Credentials File
1. Navigate to the [API Console Credentials page](https://console.developers.google.com/project/_/apis/credentials).
2. Select a project or create a new project. Note the project ID.
3. Select the **Create credentials** dropdown on the **Credentials** page, and click **Service account key**.
4. Select **New service account** from the **Service account** dropdown.
5. Populate the **Service account name** and **Service account ID**.
6. Click the dropdown for the **Role** and choose **Storage** > **Storage Admin** *(Full control of GCS resources)*.
7. Click the **Create** button to download a credentials file and rename it to `credentials.json`.

**Note:** For alternate ways to set up *Application Default Credentials*, see [Setting Up Authentication for Server to Server Production Applications](https://developers.google.com/identity/protocols/application-default-credentials).

### 1.2 Run MinIO GCS Gateway Using Docker
```sh
podman run \
 -p 9000:9000 \
 -p 9001:9001 \
 --name gcs-s3 \
 -v /path/to/credentials.json:/credentials.json \
 -e "GOOGLE_APPLICATION_CREDENTIALS=/credentials.json" \
 -e "MINIO_ROOT_USER=minioaccountname" \
 -e "MINIO_ROOT_PASSWORD=minioaccountkey" \
 minio/minio gateway gcs yourprojectid --console-address ":9001"
```

### 1.3 Run MinIO GCS Gateway Using the MinIO Binary

```sh
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
export MINIO_ROOT_USER=minioaccesskey
export MINIO_ROOT_PASSWORD=miniosecretkey
minio gateway gcs yourprojectid
```

## 2. Test Using MinIO Console

MinIO Gateway comes with an embedded web-based object browser that outputs content to http://127.0.0.1:9000. To test that MinIO Gateway is running, open a web browser, navigate to http://127.0.0.1:9000, and ensure that the object browser is displayed.

| Dashboard                                                                                   | Creating a bucket                                                                           |
| -------------                                                                               | -------------                                                                               |
| ![Dashboard](https://github.com/minio/minio/blob/master/docs/screenshots/pic1.png?raw=true) | ![Dashboard](https://github.com/minio/minio/blob/master/docs/screenshots/pic2.png?raw=true) |

## 3. Test Using MinIO Client

MinIO Client is a command-line tool called `mc` that provides UNIX-like commands for interacting with the server (e.g. ls, cat, cp, mirror, diff, find, etc.).  `mc` supports file systems and Amazon S3-compatible cloud storage services (AWS Signature v2 and v4).

### 3.1 Configure the Gateway using MinIO Client

Use the following command to configure the gateway:

```sh
mc alias set mygcs http://gateway-ip:9000 minioaccesskey miniosecretkey
```

### 3.2 List Containers on GCS

Use the following command to list the containers on GCS:

```sh
mc ls mygcs
```

A response similar to this one should be displayed:

```
[2017-02-22 01:50:43 PST]     0B ferenginar/
[2017-02-26 21:43:51 PST]     0B my-container/
[2017-02-26 22:10:11 PST]     0B test-container1/
```

### 3.3 Known limitations
MinIO Gateway has the following limitations when used with GCS:

* It only supports read-only and write-only bucket policies at the bucket level; all other variations will return `API Not implemented`.
* The `List Multipart Uploads` and `List Object parts` commands always return empty lists. Therefore, the client must store all of the parts that it has uploaded and use that information when invoking the `_Complete Multipart Upload` command.

Other limitations:

* Bucket notification APIs are not supported.

## 4. Explore Further
- [`mc` command-line interface](https://docs.min.io/docs/minio-client-quickstart-guide)
- [`aws` command-line interface](https://docs.min.io/docs/aws-cli-with-minio)
- [`minio-go` Go SDK](https://docs.min.io/docs/golang-client-quickstart-guide)
