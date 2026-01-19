# Select API Quickstart Guide [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)

Traditional retrieval of objects is always as whole entities, i.e GetObject for a 5 GiB object, will always return 5 GiB of data. S3 Select API allows us to retrieve a subset of data by using simple SQL expressions. By using Select API to retrieve only the data needed by the application, drastic performance improvements can be achieved.

You can use the Select API to query objects with following features:

- Objects must be in CSV, JSON, or Parquet(*) format.
- UTF-8 is the only encoding type the Select API supports.
- GZIP or BZIP2 - CSV and JSON files can be compressed using GZIP, BZIP2, [ZSTD](https://facebook.github.io/zstd/), and streaming formats of [LZ4](https://lz4.github.io/lz4/), [S2](https://github.com/klauspost/compress/tree/master/s2#s2-compression) and [SNAPPY](http://google.github.io/snappy/).
- Parquet API supports columnar compression for  using GZIP, Snappy, LZ4. Whole object compression is not supported for Parquet objects.
- Server-side encryption - The Select API supports querying objects that are protected with server-side encryption.

Type inference and automatic conversion of values is performed based on the context when the value is un-typed (such as when reading CSV data). If present, the CAST function overrides automatic conversion.

The [mc sql](https://docs.min.io/community/minio-object-store/reference/minio-mc/mc-sql.html) command can be used for executing queries using the command line.

(*) Parquet is disabled on the MinIO server by default. See below how to enable it.

## Enabling Parquet Format

Parquet is DISABLED by default since hostile crafted input can easily crash the server.

If you are in a controlled environment where it is safe to assume no hostile content can be uploaded to your cluster you can safely enable Parquet.
To enable Parquet set the environment variable `MINIO_API_SELECT_PARQUET=on`.

## Example using Python API

### 1. Prerequisites

- Install MinIO Server from [here](https://docs.min.io/community/minio-object-store/operations/deployments/baremetal-deploy-minio-on-redhat-linux.html#procedure).
- Familiarity with AWS S3 API.
- Familiarity with Python and installing dependencies.

### 2. Install boto3

Install `aws-sdk-python` from AWS SDK for Python official docs [here](https://aws.amazon.com/sdk-for-python/)

### 3. Example

As an example, let us take a gzip compressed CSV file. Without S3 Select, we would need to download, decompress and process the entire CSV to get the data you needed. With Select API, can use a simple SQL expression to return only the data from the CSV you’re interested in, instead of retrieving the entire object. Following Python example shows how to retrieve the first column `Location` from an object containing data in CSV format.

Please replace ``endpoint_url``,``aws_access_key_id``, ``aws_secret_access_key``, ``Bucket`` and ``Key`` with your local setup in this ``select.py`` file.

```py
#!/usr/bin/env/env python3
import boto3

s3 = boto3.client('s3',
                  endpoint_url='http://localhost:9000',
                  aws_access_key_id='minio',
                  aws_secret_access_key='minio123',
                  region_name='us-east-1')

r = s3.select_object_content(
    Bucket='mycsvbucket',
    Key='sampledata/TotalPopulation.csv.gz',
    ExpressionType='SQL',
    Expression="select * from s3object s where s.Location like '%United States%'",
    InputSerialization={
        'CSV': {
            "FileHeaderInfo": "USE",
        },
        'CompressionType': 'GZIP',
    },
    OutputSerialization={'CSV': {}},
)

for event in r['Payload']:
    if 'Records' in event:
        records = event['Records']['Payload'].decode('utf-8')
        print(records)
    elif 'Stats' in event:
        statsDetails = event['Stats']['Details']
        print("Stats details bytesScanned: ")
        print(statsDetails['BytesScanned'])
        print("Stats details bytesProcessed: ")
        print(statsDetails['BytesProcessed'])
```

## 4. Run the Program

Upload a sample dataset to MinIO using the following commands.

```sh
curl "https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/CSV_FILES/WPP2019_TotalPopulationBySex.csv" > TotalPopulation.csv
mc mb myminio/mycsvbucket
gzip TotalPopulation.csv
mc cp TotalPopulation.csv.gz myminio/mycsvbucket/sampledata/
```

Now let us proceed to run our select example to query for `Location` which matches `United States`.

```sh
$ python3 select.py
840,United States of America,2,Medium,1950,1950.5,79233.218,79571.179,158804.395

840,United States of America,2,Medium,1951,1951.5,80178.933,80726.116,160905.035

840,United States of America,2,Medium,1952,1952.5,81305.206,82019.632,163324.851

840,United States of America,2,Medium,1953,1953.5,82565.875,83422.307,165988.190
....
....
....

Stats details bytesScanned:
6758866
Stats details bytesProcessed:
25786743
```

For a more detailed SELECT SQL reference, please see [here](https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference-select.html)

## 5. Explore Further

- [Use `mc` with MinIO Server](https://docs.min.io/community/minio-object-store/reference/minio-mc.html)
- [Use `mc sql` with MinIO Server](https://docs.min.io/community/minio-object-store/reference/minio-mc/mc-sql.html#command-mc.sql)
- [Use `minio-go` SDK with MinIO Server](https://docs.min.io/community/minio-object-store/developers/go/minio-go.html)
- [Use `aws-cli` with MinIO Server](https://docs.min.io/community/minio-object-store/integrations/aws-cli-with-minio.html)
- [The MinIO documentation website](https://docs.min.io/community/minio-object-store/index.html)

## 6. Implementation Status

- Full AWS S3 [SELECT SQL](https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference-select.html) syntax is supported.
- All [operators](https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference-operators.html) are supported.
- All aggregation, conditional, type-conversion and string functions are supported.
- JSON path expressions such as `FROM S3Object[*].path` are not yet evaluated.
- Large numbers (outside of the signed 64-bit range) are not yet supported.
- The Date [functions](https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference-date.html) `DATE_ADD`, `DATE_DIFF`, `EXTRACT` and `UTCNOW` along with type conversion using `CAST` to the `TIMESTAMP` data type are currently supported.
- AWS S3's [reserved keywords](https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference-keyword-list.html) list is not yet respected.
- CSV input fields (even quoted) cannot contain newlines even if `RecordDelimiter` is something else.
