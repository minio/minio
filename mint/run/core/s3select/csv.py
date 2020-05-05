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

def exec_select(client, bucket_name, object_content, options, log_output):
    log_output.args['object_name'] = object_name = generate_object_name()
    try:
        bytes_content = io.BytesIO(object_content)
        client.put_object(bucket_name, object_name, io.BytesIO(object_content), len(object_content))

        data = client.select_object_content(bucket_name, object_name, options)
        # Get the records
        records = io.BytesIO()
        for d in data.stream(10*1024):
            records.write(d.encode('utf-8'))

        return records.getvalue()

    except Exception as err:
        raise Exception(err)
    finally:
        try:
            client.remove_object(bucket_name, object_name)
        except Exception as err:
            raise Exception(err)


def test_csv_input_custom_quote_char(client, log_output):
    # Get a unique bucket_name and object_name
    log_output.args['bucket_name'] = bucket_name = generate_bucket_name()

    tests = [
            # Invalid quote character, should fail
            ('""', '"', b'col1,col2,col3\n', Exception()),
            # UTF-8 quote character
            ('ع', '"', b'\xd8\xb9col1\xd8\xb9,\xd8\xb9col2\xd8\xb9,\xd8\xb9col3\xd8\xb9\n', b'{"_1":"col1","_2":"col2","_3":"col3"}\n'),
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

    try:
        client.make_bucket(bucket_name)

        for idx, (quote_char, escape_char, object_content, expected_output) in enumerate(tests):
            options = SelectObjectOptions(
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

            got_output = b''

            try:
                got_output = exec_select(client, bucket_name, object_content, options, log_output)
            except Exception as select_err:
                if not isinstance(expected_output, Exception):
                    raise ValueError('Test {} unexpectedly failed with: {}'.format(idx+1, select_err))
            else:
                if isinstance(expected_output, Exception):
                    raise ValueError('Test {}: expected an exception, got {}'.format(idx+1, got_output))
                if got_output != expected_output:
                    raise ValueError('Test {}: data mismatch. Expected : {}, Received {}'.format(idx+1, expected_output, got_output))

    except Exception as err:
        raise Exception(err)
    finally:
        try:
            client.remove_bucket(bucket_name)
        except Exception as err:
            raise Exception(err)

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

    try:
        client.make_bucket(bucket_name)

        for idx, (quote_char, escape_char, object_content, expected_output) in enumerate(tests):
            options = SelectObjectOptions(
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

            got_output = b''

            try:
                got_output = exec_select(client, bucket_name, object_content, options, log_output)
            except Exception as select_err:
                if not isinstance(expected_output, Exception):
                    raise ValueError('Test {} unexpectedly failed with: {}'.format(idx+1, select_err))
            else:
                if isinstance(expected_output, Exception):
                    raise ValueError('Test {}: expected an exception, got {}'.format(idx+1, got_output))
                if got_output != expected_output:
                    raise ValueError('Test {}: data mismatch. Expected : {}. Received: {}.'.format(idx+1, expected_output, got_output))

    except Exception as err:
        raise Exception(err)
    finally:
        try:
            client.remove_bucket(bucket_name)
        except Exception as err:
            raise Exception(err)

    # Test passes
    print(log_output.json_report())


