#!/bin/bash
#
#

set -e

rm -f parquet.thrift
wget -q https://github.com/apache/parquet-format/raw/df6132b94f273521a418a74442085fdd5a0aa009/src/main/thrift/parquet.thrift
thrift --gen go parquet.thrift
gofmt -w -s gen-go/parquet
