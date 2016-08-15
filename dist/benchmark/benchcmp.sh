#!/bin/bash          

# usage: ./benchcmp.sh <commit-sha1> <commit-sha2>
# Exit on any non zero return value on execution of a command. 
set -e 

# path of benchcmp. 
benchcmp=${GOPATH}/bin/benchcmp

# function which runs the benchmark comparison. 
RunBenchCmp () {
	# Path for storing output of benchmark at commit 1. 
	commit1Bench=/tmp/minio-$1.bench
	# Path for storing output of benchmark at commit 2. 
	commit2Bench=/tmp/minio-$2.bench
	# switch to commit $1. 
	git checkout $1
	# Check if the benchmark results for given commit 1 already exists. 
	# Benchmarks are time/resource consuming operations, run only if the the results doesn't exist. 
	if [[ ! -f $commit1Bench  ]]
	then

		echo "Running benchmarks at $1"
		go test -run=NONE -bench=. | tee $commit1Bench 
	fi
	# get back to the commit from which it was started. 
	git checkout -
	echo "Checking into commit $2"
	# switch to commit $2 
	git checkout $2
	# Check if the benchmark results for given commit 2 already exists. 
	# Benchmarks are time/resource consuming operations, run only if the the results doesn't exist. 
	if [[ ! -f $commit2Bench  ]]
	then
	# Running benchmarks at $2.
		echo "Running benchmarks at $2"
		go test -run=NONE -bench=. | tee $commit2Bench 
	fi
	
	# get back to the commit from which it was started. 
	git checkout - 
	# Comparing the benchmarks.
	echo "Running benchmark comparison between $1 and $2 ..."
	$benchcmp $commit1Bench $commit2Bench 
	echo "Done."
}

# check if 2 commit SHA's of snapshots of code for which benchmp has to be done is provided. 
if [ ! $# -eq 2 ]
then
	# exit if commit SHA's are not provided. 
	echo $#
	echo "Need Commit SHA's of 2 snapshots to be supplied to run benchmark comparison."
	exit 1
fi

# check if benchcmp exists. 
if [[ -x "$benchcmp" ]]
then
	RunBenchCmp $1 $2
else
	# install benchcmp if doesnt't exist. 
	echo "fetching Benchcmp..."
	go get -u golang.org/x/tools/cmd/benchcmp
	echo "Done."
	RunBenchCmp $1 $2
fi
