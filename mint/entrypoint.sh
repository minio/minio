#!/bin/bash
#
#

./mint.sh "$@"  &

# Get the pid to be used for kill command if required
main_pid="$!"
trap 'echo -e "\nAborting Mint..."; kill $main_pid' SIGINT SIGTERM
# use -n here to catch mint.sh exit code, notify to ci
wait -n
