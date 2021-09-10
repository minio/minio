#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

import boto3
from boto3.session import Session
from botocore.session import get_session

from client_grants import ClientGrantsCredentialProvider

boto3.set_stream_logger('boto3.resources', logging.DEBUG)

bc_session = get_session()
bc_session.get_component('credential_provider').insert_before(
    'env',
    ClientGrantsCredentialProvider('NZLOOFRSluw9RfIkuHGqfk1HFp4a',
                                   '0Z4VTG8uJBSekn42HE40DK9vQb4a'),
)

boto3_session = Session(botocore_session=bc_session)
s3 = boto3_session.resource('s3', endpoint_url='http://localhost:9000')

with open('/etc/hosts', 'rb') as data:
    s3.meta.client.upload_fileobj(data,
                                  'testbucket',
                                  'hosts',
                                  ExtraArgs={'ServerSideEncryption': 'AES256'})

# Upload with server side encryption, using temporary credentials
s3.meta.client.upload_file('/etc/hosts',
                           'testbucket',
                           'hosts',
                           ExtraArgs={'ServerSideEncryption': 'AES256'})

# Download encrypted object using temporary credentials
s3.meta.client.download_file('testbucket', 'hosts', '/tmp/hosts')
