#!/usr/bin/env python
# -*- coding: utf-8 -*-
# MinIO Python Library for Amazon S3 Compatible Cloud Storage,
# (C) 2015-2020 MinIO, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import io

from minio import Minio
from minio.select.options import (SelectObjectOptions, CSVInput,
                                  RequestProgress, InputSerialization,
                                  OutputSerialization, CSVOutput, JsonOutput)

from utils import *

def test_sql_api(test_name, client, bucket_name, input_data, sql_opts, expected_output):
    """ Test if the passed SQL request has the output equal to the passed execpted one"""
    object_name = generate_object_name()
    got_output = b''
    try:
        bytes_content = io.BytesIO(input_data)
        client.put_object(bucket_name, object_name, io.BytesIO(input_data), len(input_data))
        data = client.select_object_content(bucket_name, object_name, sql_opts)
        # Get the records
        records = io.BytesIO()
        for d in data.stream(10*1024):
            records.write(d.encode('utf-8'))
        got_output = records.getvalue()
    except Exception as select_err:
        if not isinstance(expected_output, Exception):
            raise ValueError('Test {} unexpectedly failed with: {}'.format(test_name, select_err))
    else:
        if isinstance(expected_output, Exception):
            raise ValueError('Test {}: expected an exception, got {}'.format(test_name, got_output))
        if got_output != expected_output:
            raise ValueError('Test {}: data mismatch. Expected : {}, Received {}'.format(test_name, expected_output, got_output))
    finally:
        client.remove_object(bucket_name, object_name)


def test_csv_input_custom_quote_char(client, log_output):
    # Get a unique bucket_name and object_name
    log_output.args['bucket_name'] = bucket_name = generate_bucket_name()

    tests = [
            # Invalid quote character, should fail
            ('""', '"', b'col1,col2,col3\n', Exception()),
            # UTF-8 quote character
            ('ع', '"', 'عcol1ع,عcol2ع,عcol3ع\n'.encode(), b'{"_1":"col1","_2":"col2","_3":"col3"}\n'),
            # Only one field is quoted
            ('"', '"', b'"col1",col2,col3\n', b'{"_1":"col1","_2":"col2","_3":"col3"}\n'),
            ('"', '"', b'"col1,col2,col3"\n', b'{"_1":"col1,col2,col3"}\n'),
            ('\'', '"', b'"col1",col2,col3\n', b'{"_1":"\\"col1\\"","_2":"col2","_3":"col3"}\n'),
            ('', '"', b'"col1",col2,col3\n', b'{"_1":"\\"col1\\"","_2":"col2","_3":"col3"}\n'),
            ('', '"', b'"col1",col2,col3\n', b'{"_1":"\\"col1\\"","_2":"col2","_3":"col3"}\n'),
            ('', '"', b'"col1","col2","col3"\n', b'{"_1":"\\"col1\\"","_2":"\\"col2\\"","_3":"\\"col3\\""}\n'),
            ('"', '"', b'""""""\n', b'{"_1":"\\"\\""}\n'),
            ('"', '"', b'A",B\n', b'{"_1":"A\\"","_2":"B"}\n'),
            ('"', '"', b'A"",B\n', b'{"_1":"A\\"\\"","_2":"B"}\n'),
            ('"', '\\', b'A\\B,C\n', b'{"_1":"A\\\\B","_2":"C"}\n'),
            ('"', '"', b'"A""B","CD"\n', b'{"_1":"A\\"B","_2":"CD"}\n'),
            ('"', '\\', b'"A\\B","CD"\n', b'{"_1":"AB","_2":"CD"}\n'),
            ('"', '\\', b'"A\\,","CD"\n', b'{"_1":"A,","_2":"CD"}\n'),
            ('"', '\\', b'"A\\"B","CD"\n', b'{"_1":"A\\"B","_2":"CD"}\n'),
            ('"', '\\', b'"A\\""\n', b'{"_1":"A\\""}\n'),
            ('"', '\\', b'"A\\"\\"B"\n', b'{"_1":"A\\"\\"B"}\n'),
            ('"', '\\', b'"A\\"","\\"B"\n', b'{"_1":"A\\"","_2":"\\"B"}\n'),
    ]

    client.make_bucket(bucket_name)

    try:
        for idx, (quote_char, escape_char, data, expected_output) in enumerate(tests):
            sql_opts = SelectObjectOptions(
                    expression="select * from s3object",
                    input_serialization=InputSerialization(
                        compression_type="NONE",
                        csv=CSVInput(FileHeaderInfo="NONE",
                            RecordDelimiter="\n",
                            FieldDelimiter=",",
                            QuoteCharacter=quote_char,
                            QuoteEscapeCharacter=escape_char,
                            Comments="#",
                            AllowQuotedRecordDelimiter="FALSE",),
                        ),
                    output_serialization=OutputSerialization(
                        json = JsonOutput(
                           RecordDelimiter="\n",
                           )
                        ),
                    request_progress=RequestProgress(
                        enabled="False"
                        )
                    )

            test_sql_api(f'test_{idx}', client, bucket_name, data, sql_opts, expected_output)
    finally:
        client.remove_bucket(bucket_name)

    # Test passes
    print(log_output.json_report())

def test_csv_output_custom_quote_char(client, log_output):
    # Get a unique bucket_name and object_name
    log_output.args['bucket_name'] = bucket_name = generate_bucket_name()

    tests = [
            # UTF-8 quote character
            ("''", "''", b'col1,col2,col3\n', Exception()),
            ("'", "'", b'col1,col2,col3\n', b"'col1','col2','col3'\n"),
            ("", '"', b'col1,col2,col3\n', b'\x00col1\x00,\x00col2\x00,\x00col3\x00\n'),
            ('"', '"', b'col1,col2,col3\n', b'"col1","col2","col3"\n'),
            ('"', '"', b'col"1,col2,col3\n', b'"col""1","col2","col3"\n'),
            ('"', '"', b'""""\n', b'""""\n'),
            ('"', '"', b'\n', b''),
            ("'", "\\", b'col1,col2,col3\n', b"'col1','col2','col3'\n"),
            ("'", "\\", b'col""1,col2,col3\n', b"'col\"\"1','col2','col3'\n"),
            ("'", "\\", b'col\'1,col2,col3\n', b"'col\\'1','col2','col3'\n"),
            ("'", "\\", b'"col\'1","col2","col3"\n', b"'col\\'1','col2','col3'\n"),
            ("'", "\\", b'col\'\n', b"'col\\''\n"),
            # Two consecutive escaped quotes
            ("'", "\\", b'"a"""""\n', b"'a\"\"'\n"),
    ]

    client.make_bucket(bucket_name)

    try:
        for idx, (quote_char, escape_char, input_data, expected_output) in enumerate(tests):
            sql_opts = SelectObjectOptions(
                    expression="select * from s3object",
                    input_serialization=InputSerialization(
                        compression_type="NONE",
                        csv=CSVInput(FileHeaderInfo="NONE",
                            RecordDelimiter="\n",
                            FieldDelimiter=",",
                            QuoteCharacter='"',
                            QuoteEscapeCharacter='"',
                            Comments="#",
                            AllowQuotedRecordDelimiter="FALSE",),
                        ),
                    output_serialization=OutputSerialization(
                        csv=CSVOutput(QuoteFields="ALWAYS",
                            RecordDelimiter="\n",
                            FieldDelimiter=",",
                            QuoteCharacter=quote_char,
                            QuoteEscapeCharacter=escape_char,)
                        ),
                    request_progress=RequestProgress(
                        enabled="False"
                        )
                    )

            test_sql_api(f'test_{idx}', client, bucket_name, input_data, sql_opts, expected_output)
    finally:
        client.remove_bucket(bucket_name)

    # Test passes
    print(log_output.json_report())


