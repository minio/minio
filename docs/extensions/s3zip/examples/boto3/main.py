#!/usr/bin/env/python

import boto3
from botocore.client import Config

s3 = boto3.client('s3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='YOUR-ACCESSKEYID',
        aws_secret_access_key='YOUR-SECRETACCESSKEY',
        config=Config(signature_version='s3v4'),
        region_name='us-east-1')


def _add_header(request, **kwargs):
    request.headers.add_header('x-minio-extract', 'true')
event_system = s3.meta.events
event_system.register_first('before-sign.s3.*', _add_header)

# List zip contents
response = s3.list_objects_v2(Bucket="your-bucket", Prefix="path/to/file.zip/")
print(response)

# Download data.csv stored in the zip file
s3.download_file(Bucket='your-bucket', Key='path/to/file.zip/data.csv', Filename='/tmp/data.csv')

