#!/bin/bash
#
#

# handle command line arguments
if [ $# -ne 2 ]; then
    echo "usage: run.sh <OUTPUT-LOG-FILE> <ERROR-LOG-FILE>"
    exit 1
fi

output_log_file="$1"
error_log_file="$2"

# configure awscli
aws configure set aws_access_key_id "$ACCESS_KEY"
aws configure set aws_secret_access_key "$SECRET_KEY"
aws configure set default.region "$SERVER_REGION"

# run tests for virtual style if provided
if [ "$ENABLE_VIRTUAL_STYLE" -eq 1 ]; then
   # Setup endpoint scheme
   endpoint="http://$DOMAIN:$SERVER_PORT"
   if [ "$ENABLE_HTTPS" -eq 1 ]; then
       endpoint="https://$DOMAIN:$SERVER_PORT"
   fi
   dnsmasq --address="/$DOMAIN/$SERVER_IP" --user=root
   echo -e "nameserver 127.0.0.1\n$(cat /etc/resolv.conf)" > /etc/resolv.conf
   aws configure set default.s3.addressing_style virtual
   ./test.sh "$endpoint"  1>>"$output_log_file" 2>"$error_log_file"
   aws configure set default.s3.addressing_style path
fi

endpoint="http://$SERVER_ENDPOINT"
if [ "$ENABLE_HTTPS" -eq 1 ]; then
    endpoint="https://$SERVER_ENDPOINT"
fi
# run path style tests
./test.sh "$endpoint"  1>>"$output_log_file" 2>"$error_log_file"
