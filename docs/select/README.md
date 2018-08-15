# How to use Minio S3 Select [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)

This document explains current limitations of the Minio S3 Select support.

## 1. Features to be implemented
1). JSON documents as supported Objects

2). CAST expression

3). Date Functions

4). Returning types other than float from aggregation queries.

5). Bracket and Reversal Notation with SQL Like operator.

6). SUBSTRING currently is not supported and TRIM only works with default arguments of trim leading and trailing spaces

## 2. Sample Usage with AWS Boto Client
```python
import boto3
from botocore.client import Config
import os
s3 = boto3.resource('s3',
                    endpoint_url='ENDPOINT',
                    aws_access_key_id='ACCESSKEY',
                  aws_secret_access_key='SECRETKEY',
                   config=Config(signature_version='s3v4'),
                    region_name='us-east-1')
s3_client = s3.meta.client

r = s3_client.select_object_content(
        Bucket='myBucket',
        Key='myKey',
        ExpressionType='SQL',
        Expression = "SELECT * FROM S3OBJECT AS A",
        InputSerialization = {'CSV': {"FieldDelimiter": ",","FileHeaderInfo":"USE"}},
        OutputSerialization = {'CSV': {}},
    )
```  
## 3. Sample Usage with Minio-Go Client

```go
	// Initialize minio client object.
	minioClient, err := minio.New(endpoint, accessKeyID, secretAccessKey, useSSL)
	if err != nil {
		log.Fatalln(err)
	}
	input := minio.SelectObjectInput{
		RecordDelimiter: "\n",
		FieldDelimiter:  ",",
		FileHeaderInfo:  minio.CSVFileHeaderInfoUse,
	}
	output := minio.SelectObjectOutput{
		RecordDelimiter: "\n",
		FieldDelimiter:  ",",
	}
	opts := minio.SelectObjectOptions{
		Type:   minio.SelectObjectTypeCSV,
		Input:  input,
		Output: output,
	}
	myReader, err := minioClient.SelectObjectContent(ctx, "sqlselectapi", "player.csv", "Select * from S3OBJECT WHERE last_name = 'James'", opts)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer myReader.Close()

	results, resultWriter := io.Pipe()
	go func() {
		defer resultWriter.Close()
		for event := range myReader.Events() {
			switch e := event.(type) {
			case *minio.RecordEvent:
				resultWriter.Write(e.Payload)
			case *minio.ProgressEvent:
				fmt.Println("Progress")
			case *minio.StatEvent:
				fmt.Println(string(e.Payload))
			case *minio.EndEvent:
				fmt.Println("Ended")
				return
			}
		}
	}()
	resReader := csv.NewReader(results)
	for {
		record, err := resReader.Read()
		if err == io.EOF {
			break
		}
		// Print out the records
		fmt.Println(record)
	}
	if err := myReader.Err(); err != nil {
		fmt.Println(err)
	}
```
