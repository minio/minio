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
from sys import exit
from minio import Minio

from utils import LogOutput
from sql_ops import *
from csv import *

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

        log_output = LogOutput(client.select_object_content, 'test_sql_operators')
        test_sql_operators(client, log_output)

        log_output = LogOutput(client.select_object_content, 'test_sql_operators_precedence')
        test_sql_operators_precedence(client, log_output)

        log_output = LogOutput(client.select_object_content, 'test_sql_functions_agg_cond_conv')
        test_sql_functions_agg_cond_conv(client, log_output)

        log_output = LogOutput(client.select_object_content, 'test_sql_functions_date')
        test_sql_functions_date(client, log_output)

        log_output = LogOutput(client.select_object_content, 'test_sql_functions_string')
        test_sql_functions_string(client, log_output)

        log_output = LogOutput(client.select_object_content, 'test_sql_datatypes')
        test_sql_datatypes(client, log_output)

        log_output = LogOutput(client.select_object_content, 'test_sql_select')
        test_sql_select(client, log_output)

        log_output = LogOutput(client.select_object_content, 'test_sql_select_json')
        test_sql_select_json(client, log_output)

        log_output = LogOutput(client.select_object_content, 'test_sql_select_csv')
        test_sql_select_csv_no_header(client, log_output)

    except Exception as err:
        print(log_output.json_report(err))
        exit(1)

if __name__ == "__main__":
    # Execute only if run as a script
    main()



