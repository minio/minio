# MinIO Server Debugging Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Docker Pulls](https://img.shields.io/docker/pulls/minio/minio.svg?maxAge=604800)](https://hub.docker.com/r/minio/minio/)

## HTTP Trace

HTTP tracing can be enabled by using [`mc admin trace`](https://docs.min.io/community/minio-object-store/reference/minio-mc-admin/mc-admin-trace.html) command.

Example:

```sh
minio server /data
```

Default trace is succinct only to indicate the API operations being called and the HTTP response status.

```sh
mc admin trace myminio
```

To trace entire HTTP request

```sh
mc admin trace --verbose myminio
```

To trace entire HTTP request and also internode communication

```sh
mc admin trace --all --verbose myminio
```

## Subnet Health

Subnet Health diagnostics help ensure that the underlying infrastructure that runs MinIO is configured correctly, and is functioning properly. This test is one-shot long running one, that is recommended to be run as soon as the cluster is first provisioned, and each time a failure scenario is encountered. Note that the test incurs majority of the available resources on the system. Care must be taken when using this to debug failure scenario, so as to prevent larger outages. Health tests can be triggered using `mc support diagnostics` command.

Example:

```sh
minio server /data{1...4}
```

The command takes no flags

```sh
mc support diagnostics myminio/
```

The output printed will be of the form

```sh
● Admin Info ... ✔ 
● CPU ... ✔ 
● Disk Hardware ... ✔ 
● Os Info ... ✔ 
● Mem Info ... ✔ 
● Process Info ... ✔ 
● Config ... ✔ 
● Drive ... ✔ 
● Net ... ✔ 
*********************************************************************************
                                   WARNING!!
     ** THIS FILE MAY CONTAIN SENSITIVE INFORMATION ABOUT YOUR ENVIRONMENT ** 
     ** PLEASE INSPECT CONTENTS BEFORE SHARING IT ON ANY PUBLIC FORUM **
*********************************************************************************
mc: Health data saved to dc-11-health_20200321053323.json.gz
```

The gzipped output contains debugging information for your system

## Decoding Metadata

Metadata is stored in `xl.meta` files for erasure coded objects. Each disk in the set containing the object has this file. The file format is a binary format and therefore requires tools to view values.

### Installing xl-meta

To install, [Go](https://golang.org/dl/) must be installed. Once installed, execute this to install the binary:

```bash
go install github.com/minio/minio/docs/debugging/xl-meta@latest
```

### Using xl-meta

Executing `xl-meta` will look for an `xl.meta` in the current folder and decode it to JSON. It is also possible to specify multiple files or wildcards, for example `xl-meta ./**/xl.meta` will output decoded metadata recursively. It is possible to view what inline data is stored inline in the metadata using `--data` parameter `xl-meta -data xl.json` will display an id -> data size. To export inline data to a file use the `--export` option.

### Remotely Inspecting backend data

`mc support inspect` allows collecting files based on *path* from all backend drives. Matching files will be collected in a zip file with their respective host+drive+path. A MinIO host from October 2021 or later is required for full functionality. Syntax is `mc support inspect ALIAS/path/to/files`. This can for example be used to collect `xl.meta` from objects that are misbehaving. To collect `xl.meta` from a specific object, for example placed at `ALIAS/bucket/path/to/file.txt` append `/xl.meta`, for instance `mc support inspect ALIAS/bucket/path/to/file.txt/xl.meta`. All files can be collected, so this can also be used to retrieve `part.*` files, etc.

Wildcards can be used, for example `mc support inspect ALIAS/bucket/path/**/xl.meta` will collect all `xl.meta` recursively. `mc support inspect ALIAS/bucket/path/to/file.txt/*/part.*` will collect parts for all versions for the object located at `bucket/path/to/file.txt`.

`xl-meta` accepts zip files as input and will output all `xl.meta` files found within the archive. For example:

```
$ mc support inspect play/test123/test*/xl.meta
mc: File data successfully downloaded as inspect.6f96b336.zip
$ xl-meta inspect.6f96b336.zip
{
        "bf6178f9-4014-4008-9699-86f2fac62226/test123/testw3c.pdf/xl.meta": {"Versions":[{"Type":1,"V2Obj":{"ID":"aGEA/ZUOR4ueRIZsAgfDqA==","DDir":"9MMwM47bS+K6KvQqN3hlDw==","EcAlgo":1,"EcM":2,"EcN":2,"EcBSize":1048576,"EcIndex":4,"EcDist":[4,1,2,3],"CSumAlgo":1,"PartNums":[1],"PartETags":[""],"PartSizes":[101974],"PartASizes":[176837],"Size":101974,"MTime":1634106631319256439,"MetaSys":{"X-Minio-Internal-compression":"a2xhdXNwb3N0L2NvbXByZXNzL3My","X-Minio-Internal-actual-size":"MTc2ODM3","x-minio-internal-objectlock-legalhold-timestamp":"MjAyMS0xMC0xOVQyMjozNTo0Ni4zNTE4MDU3NTda"},"MetaUsr":{"x-amz-object-lock-mode":"COMPLIANCE","x-amz-object-lock-retain-until-date":"2022-10-13T06:30:31.319Z","etag":"67ed8f49b7137cb957858ce468f2e79e","content-type":"application/pdf","x-amz-object-lock-legal-hold":"OFF"}}}]},
        "fe012443-6ba9-4ef2-bb94-b729d2060c78/test123/testw3c.pdf/xl.meta": {"Versions":[{"Type":1,"V2Obj":{"ID":"aGEA/ZUOR4ueRIZsAgfDqA==","DDir":"9MMwM47bS+K6KvQqN3hlDw==","EcAlgo":1,"EcM":2,"EcN":2,"EcBSize":1048576,"EcIndex":1,"EcDist":[4,1,2,3],"CSumAlgo":1,"PartNums":[1],"PartETags":[""],"PartSizes":[101974],"PartASizes":[176837],"Size":101974,"MTime":1634106631319256439,"MetaSys":{"X-Minio-Internal-compression":"a2xhdXNwb3N0L2NvbXByZXNzL3My","X-Minio-Internal-actual-size":"MTc2ODM3","x-minio-internal-objectlock-legalhold-timestamp":"MjAyMS0xMC0xOVQyMjozNTo0Ni4zNTE4MDU3NTda"},"MetaUsr":{"content-type":"application/pdf","x-amz-object-lock-legal-hold":"OFF","x-amz-object-lock-mode":"COMPLIANCE","x-amz-object-lock-retain-until-date":"2022-10-13T06:30:31.319Z","etag":"67ed8f49b7137cb957858ce468f2e79e"}}}]},
        "5dcb9f38-08ea-4728-bb64-5cecc7102436/test123/testw3c.pdf/xl.meta": {"Versions":[{"Type":1,"V2Obj":{"ID":"aGEA/ZUOR4ueRIZsAgfDqA==","DDir":"9MMwM47bS+K6KvQqN3hlDw==","EcAlgo":1,"EcM":2,"EcN":2,"EcBSize":1048576,"EcIndex":2,"EcDist":[4,1,2,3],"CSumAlgo":1,"PartNums":[1],"PartETags":[""],"PartSizes":[101974],"PartASizes":[176837],"Size":101974,"MTime":1634106631319256439,"MetaSys":{"X-Minio-Internal-compression":"a2xhdXNwb3N0L2NvbXByZXNzL3My","X-Minio-Internal-actual-size":"MTc2ODM3","x-minio-internal-objectlock-legalhold-timestamp":"MjAyMS0xMC0xOVQyMjozNTo0Ni4zNTE4MDU3NTda"},"MetaUsr":{"content-type":"application/pdf","x-amz-object-lock-legal-hold":"OFF","x-amz-object-lock-mode":"COMPLIANCE","x-amz-object-lock-retain-until-date":"2022-10-13T06:30:31.319Z","etag":"67ed8f49b7137cb957858ce468f2e79e"}}}]},
        "48beacc7-4be0-4660-9026-4eceaf147504/test123/testw3c.pdf/xl.meta": {"Versions":[{"Type":1,"V2Obj":{"ID":"aGEA/ZUOR4ueRIZsAgfDqA==","DDir":"9MMwM47bS+K6KvQqN3hlDw==","EcAlgo":1,"EcM":2,"EcN":2,"EcBSize":1048576,"EcIndex":3,"EcDist":[4,1,2,3],"CSumAlgo":1,"PartNums":[1],"PartETags":[""],"PartSizes":[101974],"PartASizes":[176837],"Size":101974,"MTime":1634106631319256439,"MetaSys":{"X-Minio-Internal-compression":"a2xhdXNwb3N0L2NvbXByZXNzL3My","X-Minio-Internal-actual-size":"MTc2ODM3","x-minio-internal-objectlock-legalhold-timestamp":"MjAyMS0xMC0xOVQyMjozNTo0Ni4zNTE4MDU3NTda"},"MetaUsr":{"x-amz-object-lock-retain-until-date":"2022-10-13T06:30:31.319Z","x-amz-object-lock-legal-hold":"OFF","etag":"67ed8f49b7137cb957858ce468f2e79e","content-type":"application/pdf","x-amz-object-lock-mode":"COMPLIANCE"}}}]}
}
```

Optionally `--encrypt` can be specified. This will output an encrypted file and a decryption key:

```
$ mc support inspect --encrypt play/test123/test*/*/part.*
mc: Encrypted file data successfully downloaded as inspect.ad2b43d8.enc
mc: Decryption key: ad2b43d847fdb14e54c5836200177f7158b3f745433525f5d23c0e0208e50c9948540b54

mc: The decryption key will ONLY be shown here. It cannot be recovered.
mc: The encrypted file can safely be shared without the decryption key.
mc: Even with the decryption key, data stored with encryption cannot be accessed.
```

This file can be decrypted using the decryption tool below:

### Installing decryption tool

To install, [Go](https://golang.org/dl/) must be installed.

Once installed, execute this to install the binary:

```bash
go install github.com/minio/minio/docs/debugging/inspect@latest
```

### Usage

To decrypt the file above:

```
$ inspect -key=ad2b43d847fdb14e54c5836200177f7158b3f745433525f5d23c0e0208e50c9948540b54 inspect.ad2b43d8.enc
Output decrypted to inspect.ad2b43d8.zip
```

If `--key` is not specified an interactive prompt will ask for it. The file name will contain the beginning of the key. This can be used to verify that the key is for the encrypted file.
