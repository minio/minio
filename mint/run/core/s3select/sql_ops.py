#!/usr/bin/env python
# -*- coding: utf-8 -*-
# MinIO Python Library for Amazon S3 Compatible Cloud Storage,
# (C) 2020 MinIO, Inc.
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

import io
from datetime import datetime

from minio import Minio
from minio.select.options import (SelectObjectOptions, CSVInput, JSONInput,
                                  RequestProgress, InputSerialization,
                                  OutputSerialization, CSVOutput, JsonOutput)

from utils import *

def test_sql_expressions_custom_input_output(client, input_bytes, sql_input, sql_output, tests, log_output):
    bucket_name = generate_bucket_name()
    object_name = generate_object_name()

    log_output.args['total_tests'] = 0
    log_output.args['total_success'] = 0

    client.make_bucket(bucket_name)
    try:
        content = io.BytesIO(bytes(input_bytes, 'utf-8'))
        client.put_object(bucket_name, object_name, content, len(input_bytes))

        for idx, (test_name, select_expression, expected_output) in enumerate(tests):
            if select_expression == '':
                continue
            try:
                log_output.args['total_tests'] += 1
                options = SelectObjectOptions(
                    expression=select_expression,
                    input_serialization=sql_input,
                    output_serialization=sql_output,
                    request_progress=RequestProgress(
                        enabled="False"
                        )
                    )

                data = client.select_object_content(bucket_name, object_name, options)

                # Get the records
                records = io.BytesIO()
                for d in data.stream(10*1024):
                    records.write(d.encode('utf-8'))
                got_output = records.getvalue()

                if got_output != expected_output:
                    if type(expected_output) == datetime:
                        # Attempt to parse the date which will throw an exception for any issue
                        datetime.strptime(got_output.decode("utf-8").strip(), '%Y-%m-%dT%H:%M:%S.%f%z')
                    else:
                        raise ValueError('Test {}: data mismatch. Expected : {}. Received: {}.'.format(idx+1, expected_output, got_output))

                log_output.args['total_success'] += 1
            except Exception as err:
                continue ## TODO, raise instead
                # raise Exception(err)
    finally:
        client.remove_object(bucket_name, object_name)
        client.remove_bucket(bucket_name)


def test_sql_expressions(client, input_json_bytes, tests, log_output):
    input_serialization = InputSerialization(
            compression_type="NONE",
            json=JSONInput(Type="DOCUMENT"),
            )

    output_serialization=OutputSerialization(
            csv=CSVOutput(QuoteFields="ASNEEDED")
            )

    test_sql_expressions_custom_input_output(client, input_json_bytes,
            input_serialization, output_serialization, tests, log_output)


def test_sql_operators(client, log_output):

    json_testfile = """{"id": 1, "name": "John", "age": 3}
{"id": 2, "name": "Elliot", "age": 4}
{"id": 3, "name": "Yves", "age": 5}
{"id": 4, "name": null, "age": 0}
"""

    tests = [
       # Logical operators
       ("AND", "select * from S3Object s where s.id = 1 AND s.name = 'John'", b'1,John,3\n'),
       ("NOT", "select * from S3Object s where NOT s.id = 1", b'2,Elliot,4\n3,Yves,5\n4,,0\n'),
       ("OR", "select * from S3Object s where s.id = 1 OR s.id = 3", b'1,John,3\n3,Yves,5\n'),
       # Comparison Operators
       ("<", "select * from S3Object s where s.age < 4", b'1,John,3\n4,,0\n'),
       (">", "select * from S3Object s where s.age > 4", b'3,Yves,5\n'),
       ("<=", "select * from S3Object s where s.age <= 4", b'1,John,3\n2,Elliot,4\n4,,0\n'),
       (">=", "select * from S3Object s where s.age >= 4", b'2,Elliot,4\n3,Yves,5\n'),
       ("=", "select * from S3Object s where s.age = 4", b'2,Elliot,4\n'),
       ("<>", "select * from S3Object s where s.age <> 4", b'1,John,3\n3,Yves,5\n4,,0\n'),
       ("!=", "select * from S3Object s where s.age != 4", b'1,John,3\n3,Yves,5\n4,,0\n'),
       ("BETWEEN", "select * from S3Object s where s.age BETWEEN 4 AND 5", b'2,Elliot,4\n3,Yves,5\n'),
       ("IN", "select * from S3Object s where s.age IN (3,5)", b'1,John,3\n3,Yves,5\n'),
       # Pattern Matching Operators
       ("LIKE_", "select * from S3Object s where s.name LIKE '_ves'", b'3,Yves,5\n'),
       ("LIKE%", "select * from S3Object s where s.name LIKE 'Ell%t'", b'2,Elliot,4\n'),
       # Unitary Operators
       ("NULL", "select * from S3Object s where s.name IS NULL", b'4,,0\n'),
       ("NOT_NULL", "select * from S3Object s where s.age IS NOT NULL", b'1,John,3\n2,Elliot,4\n3,Yves,5\n4,,0\n'),
       # Math Operators
       ("+", "select * from S3Object s where s.age = 1+3 ", b'2,Elliot,4\n'),
       ("-", "select * from S3Object s where s.age = 5-1 ", b'2,Elliot,4\n'),
       ("*", "select * from S3Object s where s.age = 2*2 ", b'2,Elliot,4\n'),
       ("%", "select * from S3Object s where s.age = 10%6 ", b'2,Elliot,4\n'),
    ]

    try:
        test_sql_expressions(client, json_testfile, tests, log_output)
    except Exception as select_err:
        raise select_err
        # raise ValueError('Test {} unexpectedly failed with: {}'.format(test_name, select_err))
        # pass

    # Test passes
    print(log_output.json_report())


def test_sql_operators_precedence(client, log_output):

    json_testfile = """{"id": 1, "name": "Eric"}"""

    tests = [
       ("-_1", "select -3*3 from S3Object", b'-9\n'),
       ("*", "select 10-3*2 from S3Object", b'4\n'),
       ("/", "select 13-10/5 from S3Object", b'11\n'),
       ("%", "select 13-10%5 from S3Object", b'13\n'),
       ("+", "select 1+1*3 from S3Object", b'4\n'),
       ("-_2", "select 1-1*3 from S3Object", b'-2\n'),
       ("=", "select * from S3Object as s where s.id = 13-12", b'1,Eric\n'),
       ("<>", "select * from S3Object as s where s.id <> 1-1", b'1,Eric\n'),
       ("NOT", "select * from S3Object where false OR NOT false", b'1,Eric\n'),
       ("AND", "select * from S3Object where true AND true OR false ", b'1,Eric\n'),
       ("OR", "select * from S3Object where false OR NOT false", b'1,Eric\n'),
       ("IN", "select * from S3Object as s where s.id <> -1 AND s.id IN (1,2,3)", b'1,Eric\n'),
       ("BETWEEN", "select * from S3Object as s where s.id <> -1 AND s.id BETWEEN -1 AND 3", b'1,Eric\n'),
       ("LIKE", "select * from S3Object as s where s.id <> -1 AND s.name LIKE 'E%'", b'1,Eric\n'),
    ]

    try:
        test_sql_expressions(client, json_testfile, tests, log_output)
    except Exception as select_err:
        raise select_err
        # raise ValueError('Test {} unexpectedly failed with: {}'.format(test_name, select_err))
        # pass

    # Test passes
    print(log_output.json_report())



def test_sql_functions_agg_cond_conv(client, log_output):

    json_testfile = """{"id": 1, "name": "John", "age": 3}
{"id": 2, "name": "Elliot", "age": 4}
{"id": 3, "name": "Yves", "age": 5}
{"id": 4, "name": "Christine", "age": null}
{"id": 5, "name": "Eric", "age": 0}
"""
    tests = [
       # Aggregate functions
       ("COUNT", "select count(*) from S3Object s", b'5\n'),
       ("AVG", "select avg(s.age) from S3Object s", b'3\n'),
       ("MAX", "select max(s.age) from S3Object s", b'5\n'),
       ("MIN", "select min(s.age) from S3Object s", b'0\n'),
       ("SUM", "select sum(s.age) from S3Object s", b'12\n'),
       # Conditional functions
       ("COALESCE", "SELECT COALESCE(s.age, 99) FROM S3Object s", b'3\n4\n5\n99\n0\n'),
       ("NULLIF", "SELECT NULLIF(s.age, 0) FROM S3Object s", b'3\n4\n5\n\n\n'),
       ## Conversion functions
       ("CAST", "SELECT CAST(s.age AS FLOAT) FROM S3Object s", b'3.0\n4.0\n5.0\n\n0.0\n'),

    ]

    try:
        test_sql_expressions(client, json_testfile, tests, log_output)
    except Exception as select_err:
        raise select_err
        # raise ValueError('Test {} unexpectedly failed with: {}'.format(test_name, select_err))
        # pass

    # Test passes
    print(log_output.json_report())


def test_sql_functions_date(client, log_output):

    json_testfile = """
{"id": 1, "name": "John", "datez": "2017-01-02T03:04:05.006+07:30"}
"""

    tests = [
       # DATE_ADD
       ("DATE_ADD_1", "select DATE_ADD(year, 5, TO_TIMESTAMP(s.datez)) from S3Object as s", b'2022-01-02T03:04:05.006+07:30\n'),
       ("DATE_ADD_2", "select DATE_ADD(month, 1, TO_TIMESTAMP(s.datez)) from S3Object as s", b'2017-02-02T03:04:05.006+07:30\n'),
       ("DATE_ADD_3", "select DATE_ADD(day, -1, TO_TIMESTAMP(s.datez)) from S3Object as s", b'2017-01-01T03:04:05.006+07:30\n'),
       ("DATE_ADD_4", "select DATE_ADD(hour, 1, TO_TIMESTAMP(s.datez)) from S3Object as s", b'2017-01-02T04:04:05.006+07:30\n'),
       ("DATE_ADD_5", "select DATE_ADD(minute, 5, TO_TIMESTAMP(s.datez)) from S3Object as s", b'2017-01-02T03:09:05.006+07:30\n'),
       ("DATE_ADD_6", "select DATE_ADD(second, 5, TO_TIMESTAMP(s.datez)) from S3Object as s", b'2017-01-02T03:04:10.006+07:30\n'),
       # DATE_DIFF
       ("DATE_DIFF_1", "select DATE_DIFF(year, TO_TIMESTAMP(s.datez), TO_TIMESTAMP('2011-01-01T')) from S3Object as s", b'-6\n'),
       ("DATE_DIFF_2", "select DATE_DIFF(month, TO_TIMESTAMP(s.datez), TO_TIMESTAMP('2011T')) from S3Object as s", b'-72\n'),
       ("DATE_DIFF_3", "select DATE_DIFF(day, TO_TIMESTAMP(s.datez), TO_TIMESTAMP('2010-01-02T')) from S3Object as s", b'-2556\n'),
       # EXTRACT
       ("EXTRACT_1", "select EXTRACT(year FROM TO_TIMESTAMP(s.datez)) from S3Object as s", b'2017\n'),
       ("EXTRACT_2", "select EXTRACT(month FROM TO_TIMESTAMP(s.datez)) from S3Object as s", b'1\n'),
       ("EXTRACT_3", "select EXTRACT(hour FROM TO_TIMESTAMP(s.datez)) from S3Object as s", b'3\n'),
       ("EXTRACT_4", "select EXTRACT(minute FROM TO_TIMESTAMP(s.datez)) from S3Object as s", b'4\n'),
       ("EXTRACT_5", "select EXTRACT(timezone_hour FROM TO_TIMESTAMP(s.datez)) from S3Object as s", b'7\n'),
       ("EXTRACT_6", "select EXTRACT(timezone_minute FROM TO_TIMESTAMP(s.datez)) from S3Object as s", b'30\n'),
       # TO_STRING
       ("TO_STRING_1", "select TO_STRING(TO_TIMESTAMP(s.datez), 'MMMM d, y') from S3Object as s", b'"January 2, 2017"\n'),
       ("TO_STRING_2", "select TO_STRING(TO_TIMESTAMP(s.datez), 'MMM d, yyyy') from S3Object as s", b'"Jan 2, 2017"\n'),
       ("TO_STRING_3", "select TO_STRING(TO_TIMESTAMP(s.datez), 'M-d-yy') from S3Object as s", b'1-2-17\n'),
       ("TO_STRING_4", "select TO_STRING(TO_TIMESTAMP(s.datez), 'MM-d-y') from S3Object as s", b'01-2-2017\n'),
       ("TO_STRING_5", "select TO_STRING(TO_TIMESTAMP(s.datez), 'MMMM d, y h:m a') from S3Object as s", b'"January 2, 2017 3:4 AM"\n'),
       ("TO_STRING_6", "select TO_STRING(TO_TIMESTAMP(s.datez), 'y-MM-dd''T''H:m:ssX') from S3Object as s", b'2017-01-02T3:4:05+0730\n'),
       ("TO_STRING_7", "select TO_STRING(TO_TIMESTAMP(s.datez), 'y-MM-dd''T''H:m:ssX') from S3Object as s", b'2017-01-02T3:4:05+0730\n'),
       ("TO_STRING_8", "select TO_STRING(TO_TIMESTAMP(s.datez), 'y-MM-dd''T''H:m:ssXXXX') from S3Object as s", b'2017-01-02T3:4:05+0730\n'),
       ("TO_STRING_9", "select TO_STRING(TO_TIMESTAMP(s.datez), 'y-MM-dd''T''H:m:ssXXXXX') from S3Object as s", b'2017-01-02T3:4:05+07:30\n'),
       ("TO_TIMESTAMP", "select TO_TIMESTAMP(s.datez) from S3Object as s", b'2017-01-02T03:04:05.006+07:30\n'),
       ("UTCNOW", "select UTCNOW() from S3Object", datetime(1,1,1)),

    ]

    try:
        test_sql_expressions(client, json_testfile, tests, log_output)
    except Exception as select_err:
        raise select_err
        # raise ValueError('Test {} unexpectedly failed with: {}'.format(test_name, select_err))
        # pass

    # Test passes
    print(log_output.json_report())

def test_sql_functions_string(client, log_output):

    json_testfile = """
{"id": 1, "name": "John"}
{"id": 2, "name": "       \tfoobar\t         "}
{"id": 3, "name": "1112211foobar22211122"}
"""

    tests = [
       # CHAR_LENGTH
       ("CHAR_LENGTH", "select CHAR_LENGTH(s.name) from S3Object as s", b'4\n24\n21\n'),
       ("CHARACTER_LENGTH", "select CHARACTER_LENGTH(s.name) from S3Object as s", b'4\n24\n21\n'),
       # LOWER
       ("LOWER", "select LOWER(s.name) from S3Object as s where s.id= 1", b'john\n'),
       # SUBSTRING
       ("SUBSTRING_1", "select SUBSTRING(s.name FROM 2) from S3Object as s where s.id = 1", b'ohn\n'),
       ("SUBSTRING_2", "select SUBSTRING(s.name FROM 2 FOR 2) from S3Object as s where s.id = 1", b'oh\n'),
       ("SUBSTRING_3", "select SUBSTRING(s.name FROM -1 FOR 2) from S3Object as s where s.id = 1", b'\n'),
       # TRIM
       ("TRIM_1", "select TRIM(s.name) from S3Object as s where s.id = 2", b'\tfoobar\t\n'),
       ("TRIM_2", "select TRIM(LEADING FROM s.name) from S3Object as s where s.id = 2", b'\tfoobar\t         \n'),
       ("TRIM_3", "select TRIM(TRAILING FROM s.name) from S3Object as s where s.id = 2", b'       \tfoobar\t\n'),
       ("TRIM_4", "select TRIM(BOTH FROM s.name) from S3Object as s where s.id = 2", b'\tfoobar\t\n'),
       ("TRIM_5", "select TRIM(BOTH '12' FROM s.name) from S3Object as s where s.id = 3", b'foobar\n'),
       # UPPER
       ("UPPER", "select UPPER(s.name) from S3Object as s where s.id= 1", b'JOHN\n'),
    ]

    try:
        test_sql_expressions(client, json_testfile, tests, log_output)
    except Exception as select_err:
        raise select_err
        # raise ValueError('Test {} unexpectedly failed with: {}'.format(test_name, select_err))
        # pass

    # Test passes
    print(log_output.json_report())


def test_sql_datatypes(client, log_output):
    json_testfile = """
{"name": "John"}
"""
    tests = [
       ("bool", "select CAST('true' AS BOOL) from S3Object", b'true\n'),
       ("int", "select CAST('13' AS INT) from S3Object", b'13\n'),
       ("integer", "select CAST('13' AS INTEGER) from S3Object", b'13\n'),
       ("string", "select CAST(true AS STRING) from S3Object", b'true\n'),
       ("float", "select CAST('13.3' AS FLOAT) from S3Object", b'13.3\n'),
       ("decimal", "select CAST('14.3' AS FLOAT) from S3Object", b'14.3\n'),
       ("numeric", "select CAST('14.3' AS FLOAT) from S3Object", b'14.3\n'),
       ("timestamp", "select CAST('2007-04-05T14:30Z' AS TIMESTAMP) from S3Object", b'2007-04-05T14:30Z\n'),
    ]

    try:
        test_sql_expressions(client, json_testfile, tests, log_output)
    except Exception as select_err:
        raise select_err
        # raise ValueError('Test {} unexpectedly failed with: {}'.format(test_name, select_err))
        # pass

    # Test passes
    print(log_output.json_report())


def test_sql_select(client, log_output):

    json_testfile = """{"id": 1, "created": "June 27", "modified": "July 6" }
{"id": 2, "Created": "June 28", "Modified": "July 7", "Cast": "Random Date" }"""
    tests = [
       ("select_1", "select * from S3Object", b'1,June 27,July 6\n2,June 28,July 7,Random Date\n'),
       ("select_2", "select * from S3Object s", b'1,June 27,July 6\n2,June 28,July 7,Random Date\n'),
       ("select_3", "select * from S3Object as s", b'1,June 27,July 6\n2,June 28,July 7,Random Date\n'),
       ("select_4", "select s.line from S3Object as s", b'\n\n'),
       ("select_5", 'select s."Created" from S3Object as s', b'\nJune 28\n'),
       ("select_5", 'select s."Cast" from S3Object as s', b'\nRandom Date\n'),
       ("where", 'select s.created from S3Object as s', b'June 27\nJune 28\n'),
       ("limit", 'select * from S3Object as s LIMIT 1', b'1,June 27,July 6\n'),
    ]

    try:
        test_sql_expressions(client, json_testfile, tests, log_output)
    except Exception as select_err:
        raise select_err
        # raise ValueError('Test {} unexpectedly failed with: {}'.format(test_name, select_err))
        # pass

    # Test passes
    print(log_output.json_report())

def test_sql_select_json(client, log_output):
    json_testcontent = """{ "Rules": [ {"id": "1"}, {"expr": "y > x"}, {"id": "2", "expr": "z = DEBUG"} ]}
{ "created": "June 27", "modified": "July 6" }
"""
    tests = [
       ("select_1", "SELECT id FROM S3Object[*].Rules[*].id", b'{"id":"1"}\n{}\n{"id":"2"}\n{}\n'),
       ("select_2", "SELECT id FROM S3Object[*].Rules[*].id WHERE id IS NOT MISSING", b'{"id":"1"}\n{"id":"2"}\n'),
       ("select_3", "SELECT d.created, d.modified FROM S3Object[*] d", b'{}\n{"created":"June 27","modified":"July 6"}\n'),
       ("select_4", "SELECT _1.created, _1.modified FROM S3Object[*]", b'{}\n{"created":"June 27","modified":"July 6"}\n'),
       ("select_5", "Select s.rules[1].expr from S3Object s", b'{"expr":"y > x"}\n{}\n'),
    ]

    input_serialization = InputSerialization(json=JSONInput(Type="DOCUMENT"))
    output_serialization = OutputSerialization(json=JsonOutput())
    try:
        test_sql_expressions_custom_input_output(client, json_testcontent,
                input_serialization, output_serialization, tests, log_output)
    except Exception as select_err:
        raise select_err
        # raise ValueError('Test {} unexpectedly failed with: {}'.format(test_name, select_err))
        # pass

    # Test passes
    print(log_output.json_report())


def test_sql_select_csv_no_header(client, log_output):
    json_testcontent = """val1,val2,val3
val4,val5,val6
"""
    tests = [
       ("select_1", "SELECT s._2 FROM S3Object as s", b'val2\nval5\n'),
    ]

    input_serialization=InputSerialization(
            csv=CSVInput(
                FileHeaderInfo="NONE",
                AllowQuotedRecordDelimiter="FALSE",
                ),
            )

    output_serialization=OutputSerialization(csv=CSVOutput())
    try:
        test_sql_expressions_custom_input_output(client, json_testcontent,
                input_serialization, output_serialization, tests, log_output)
    except Exception as select_err:
        raise select_err
        # raise ValueError('Test {} unexpectedly failed with: {}'.format(test_name, select_err))
        # pass

    # Test passes
    print(log_output.json_report())


