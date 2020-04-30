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

# from __future__ import division
# from __future__ import absolute_import

import os
import io
from sys import exit
import uuid
import inspect
import json
import time
import traceback

from minio import Minio
from minio.select.options import (SelectObjectOptions, CSVInput,
                                  RequestProgress, InputSerialization,
                                  OutputSerialization, CSVOutput, JsonOutput)

class LogOutput(object):
    """
    LogOutput is the class for log output. It is required standard for all
    SDK tests controlled by mint.
    Here are its attributes:
            'name': name of the SDK under test, e.g. 's3select'
            'function': name of the method/api under test with its signature
                        The following python code can be used to
                        pull args information of a <method> and to
                        put together with the method name:
                        <method>.__name__+'('+', '.join(args_list)+')'
                        e.g. 'remove_object(bucket_name, object_name)'
            'args': method/api arguments with their values, in
                    dictionary form: {'arg1': val1, 'arg2': val2, ...}
            'duration': duration of the whole test in milliseconds,
                        defaults to 0
            'alert': any extra information user is needed to be alerted about,
                     like whether this is a Blocker/Gateway/Server related
                     issue, etc., defaults to None
            'message': descriptive error message, defaults to None
            'error': stack-trace/exception message(only in case of failure),
                     actual low level exception/error thrown by the program,
                     defaults to None
            'status': exit status, possible values are 'PASS', 'FAIL', 'NA',
                      defaults to 'PASS'
    """

    PASS = 'PASS'
    FAIL = 'FAIL'
    NA = 'NA'

    def __init__(self, meth, test_name):
        self.__args_list = inspect.getargspec(meth).args[1:]
        self.__name = 'minio-py:'+test_name
        self.__function = meth.__name__+'('+', '.join(self.__args_list)+')'
        self.__args = {}
        self.__duration = 0
        self.__alert = ''
        self.__message = None
        self.__error = None
        self.__status = self.PASS
        self.__start_time = time.time()

    @property
    def name(self): return self.__name

    @property
    def function(self): return self.__function

    @property
    def args(self): return self.__args

    @name.setter
    def name(self, val): self.__name = val

    @function.setter
    def function(self, val): self.__function = val

    @args.setter
    def args(self, val): self.__args = val

    def json_report(self, err_msg='', alert='', status=''):
        self.__args = {k: v for k, v in self.__args.items() if v and v != ''}
        entry = {'name': self.__name,
                 'function': self.__function,
                 'args': self.__args,
                 'duration': int(round((time.time() - self.__start_time)*1000)),
                 'alert': str(alert),
                 'message': str(err_msg),
                 'error': traceback.format_exc() if err_msg and err_msg != '' else '',
                 'status': status if status and status != '' else
                 self.FAIL if err_msg and err_msg != '' else self.PASS
                 }
        return json.dumps({k: v for k, v in entry.items() if v and v != ''})

def generate_bucket_name():
    return "s3select-test-" + uuid.uuid4().__str__()


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


def exec_select(client, bucket_name, object_content, options, log_output):
    log_output.args['object_name'] = object_name = uuid.uuid4().__str__()
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


def main():
    """
    Functional testing for S3 select.
    """

    try:
        access_key = os.getenv('ACCESS_KEY', 'Q3AM3UQ867SPQQA43P2F')
        secret_key = os.getenv('SECRET_KEY',
                               'zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG')
        server_endpoint = os.getenv('SERVER_ENDPOINT', 'play.min.io')
        secure = os.getenv('ENABLE_HTTPS', '1') == '1'
        if server_endpoint == 'play.min.io':
            access_key = 'Q3AM3UQ867SPQQA43P2F'
            secret_key = 'zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG'
            secure = True

        client = Minio(server_endpoint, access_key, secret_key, secure=False)

        log_output = LogOutput(client.select_object_content, 'test_csv_input_quote_char')
        test_csv_input_custom_quote_char(client, log_output)

        log_output = LogOutput(client.select_object_content, 'test_csv_output_quote_char')
        test_csv_output_custom_quote_char(client, log_output)

    except Exception as err:
        print(log_output.json_report(err))
        exit(1)

if __name__ == "__main__":
    # Execute only if run as a script
    main()
