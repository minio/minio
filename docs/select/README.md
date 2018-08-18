# Select API Quickstart Guide [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)
Traditional retrieval of objects is always as whole entities, i.e GetObject for a 5 GiB object, will always return 5 GiB of data. S3 Select API allows us to retrieve a subset of data by using simple SQL expressions. By using Select API to retrieve only the data needed by the application, drastic performance improvements can be achieved.

> This implementation is compatible with AWS S3 Select API

## 1. Prerequisites
- Install Minio Server from [here](http://docs.minio.io/docs/minio-quickstart-guide).
- Familiarity with AWS S3 API
- Familiarity with Python and installing dependencies.

## 2. Install boto3
Install `aws-sdk-python` from AWS SDK for Python official docs [here](https://aws.amazon.com/sdk-for-python/)

## 3. Example
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
Upload first a sample dataset downloaded from [TotalPopulation.csv](https://esa.un.org/unpd/wpp/DVD/Files/1_Indicators%20(Standard)/CSV_FILES/WPP2017_TotalPopulationBySex.csv) using the following commands.
```sh
$ curl "https://esa.un.org/unpd/wpp/DVD/Files/1_Indicators%20(Standard)/CSV_FILES/WPP2017_TotalPopulationBySex.csv" > TotalPopulation.csv
$ mc mb myminio/mycsvbucket
$ gzip TotalPopulation.csv
$ mc cp TotalPopulation.csv.gz myminio/mycsvbucket/sampledata/
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

## 5. Explore Further
- [Use `mc` with Minio Server](https://docs.minio.io/docs/minio-client-quickstart-guide)
- [Use `minio-go` SDK with Minio Server](https://docs.minio.io/docs/golang-client-quickstart-guide)
- [Use `aws-cli` with Minio Server](https://docs.minio.io/docs/aws-cli-with-minio)
- [Use `s3cmd` with Minio Server](https://docs.minio.io/docs/s3cmd-with-minio)
- [The Minio documentation website](https://docs.minio.io)
