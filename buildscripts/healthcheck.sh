#!/bin/sh
#
# Minio Cloud Storage, (C) 2017 Minio, Inc.
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
#

_init () {
    scheme="http://"
    address="127.0.0.1:9000"
    resource="/minio/index.html"
}

HealthCheckMain () {
    # Get the http response code
    http_response=$(curl -s -k -o /dev/null -I -w "%{http_code}" ${scheme}${address}${resource})

    # Get the http response body
    http_response_body=$(curl -k -s ${scheme}${address}${resource})

    # server returns response 403 and body "SSL required" if non-TLS connection is attempted on a TLS-configured server. 
    # change the scheme and try again
    if [ "$http_response" = "403" ] && [ "$http_response_body" = "SSL required" ]; then
        scheme="https://"
        http_response=$(curl -s -k -o /dev/null -I -w "%{http_code}" ${scheme}${address}${resource})
    fi

    # If http_repsonse is 200 - server is up. 
    # When MINIO_BROWSER is set to off, curl responds with 404. We assume that the the server is up
    [ "$http_response" = "200" ] || [ "$http_response" = "404" ]
}

_init && HealthCheckMain
