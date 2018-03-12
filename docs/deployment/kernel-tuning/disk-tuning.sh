#!/bin/bash

## Minio Cloud Storage, (C) 2017 Minio, Inc.
##
## Licensed under the Apache License, Version 2.0 (the "License");
## you may not use this file except in compliance with the License.
## You may obtain a copy of the License at
##
##     http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS,
## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
## See the License for the specific language governing permissions and
## limitations under the License.

# This script changes protected files, and must be run as root

for i in $(ls -d /sys/block/*/queue/iosched 2>/dev/null); do
    iosched_dir=$(echo $i | awk '/iosched/ {print $1}')
    [ -z $iosched_dir ] && {
        continue
    }
    ## Change each disk ioscheduler to be "deadline"
    ## Deadline dispatches I/Os in batches. A batch is a
    ## sequence of either read or write I/Os which are in
    ## increasing LBA order (the one-way elevator). After
    ## processing each batch, the I/O scheduler checks to
    ## see whether write requests have been starved for too
    ## long, and then decides whether to start a new batch
    ## of reads or writes
    path=$(dirname $iosched_dir)
    [ -f $path/scheduler ] && {
        echo "deadline" > $path/scheduler
    }
    ## This controls how many requests may be allocated
    ## in the block layer for read or write requests.
    ## Note that the total allocated number may be twice
    ## this amount, since it applies only to reads or
    ## writes (not the accumulate sum).
    [ -f $path/nr_requests ] && {
        echo "256" > $path/nr_requests
    }
    ## This is the maximum number of kilobytes
    ## supported in a single data transfer at
    ## block layer.
    [ -f $path/max_sectors_kb ] && {
        echo "1024" > $path/max_sectors_kb || true
    }
done
