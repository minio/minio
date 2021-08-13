#!/bin/bash
#

set -e
set -E
set -o pipefail

if [ ! -x "$PWD/minio" ]; then
    echo "minio executable binary not found in current directory"
    exit 1
fi

WORK_DIR="$PWD/.verify-$RANDOM"

export SERVER_ENDPOINT="127.0.0.1:9000"
export ACCESS_KEY="minio"
export SECRET_KEY="minio123"
export BUCKET_NAME="my-test-bucket"
export ENABLE_HTTPS=0
export GO111MODULE=on
export GOGC=25


MINIO_CONFIG_DIR="$WORK_DIR/.minio"

#harcoded OS for now
wget https://dl.min.io/server/minio/release/linux-amd64/archive/minio.RELEASE.2021-07-22T05-23-32Z -P "$WORK_DIR"
OLDER_MINIO_NAME="minio.RELEASE.2021-07-22T05-23-32Z"
chmod +x $OLDER_MINIO_NAME

OLDER_MINIO=( "$PWD/$OLDER_MINIO_NAME" --config-dir "$MINIO_CONFIG_DIR" ) 
MINIO=( "$PWD/minio" --config-dir "$MINIO_CONFIG_DIR" )
MINIO_NAME="minio"

function start_minio_fs() 
{
    version=$1 #pass in the actual version of minio server
    name=$2 #name of version, will just be minio if it is latest
    if ["$name" == "minio"]; then
        export MINIO_ROOT_USER=$ACCESS_KEY
        export MINIO_ROOT_PASSWORD=$SECRET_KEY  
    else 
        export MINIO_ACCESS_KEY=$ACCESS_KEY
        export MINIO_SECRET_KEY=$SECRET_KEY
    fi
        "${version[@]}" server "${WORK_DIR}/fs-disk" >"$WORK_DIR/fs-minio.log" 2>&1 &
    sleep 5
}

function start_minio_erasure()
{
    version=$1 #pass in the version of minio
    "${version[@]}" server "${WORK_DIR}/erasure-disk1" "${WORK_DIR}/erasure-disk2" "${WORK_DIR}/erasure-disk3" "${WORK_DIR}/erasure-disk4" >"$WORK_DIR/erasure-minio.log" 2>&1 &
    fi
    sleep 10
}

function start_minio_erasure_sets()
{   
    version=$1 #pass in the version of minio
    export MINIO_ENDPOINTS="${WORK_DIR}/erasure-disk-sets{1...32}"
    "${version[@]}" server > "$WORK_DIR/erasure-minio-sets.log" 2>&1 &
    sleep 10
}

function start_minio_pool_erasure_sets()
{
    version=$1 #pass in the version of minio
    name=$2 #name of version, will just be minio if it is latest
    if ["$name" == "minio"]; then
        export MINIO_ROOT_USER=$ACCESS_KEY
        export MINIO_ROOT_PASSWORD=$SECRET_KEY  
    else 
        export MINIO_ACCESS_KEY=$ACCESS_KEY
        export MINIO_SECRET_KEY=$SECRET_KEY
    fi
    
    export MINIO_ENDPOINTS="http://127.0.0.1:9000${WORK_DIR}/pool-disk-sets{1...4} http://127.0.0.1:9001${WORK_DIR}/pool-disk-sets{5...8}"
    "${version[@]}" server --address ":9000" > "$WORK_DIR/pool-minio-9000.log" 2>&1 &
    "${version[@]}" server --address ":9001" > "$WORK_DIR/pool-minio-9001.log" 2>&1 &
    
    sleep 10
}

function start_minio_pool_erasure_sets_ipv6()
{
    version=$1 #pass in the version of minio
    name=$2 #name of version, will just be minio if it is latest
    if ["$name" == "minio"]; then
        export MINIO_ROOT_USER=$ACCESS_KEY
        export MINIO_ROOT_PASSWORD=$SECRET_KEY  
    else 
        export MINIO_ACCESS_KEY=$ACCESS_KEY
        export MINIO_SECRET_KEY=$SECRET_KEY
    fi
 
 "${version[@]}" server --address="[::1]:9000" > "$WORK_DIR/pool-minio-ipv6-9000.log" 2>&1 &
    "${version[@]}" server --address="[::1]:9001" > "$WORK_DIR/pool-minio-ipv6-9001.log" 2>&1 &
    sleep 20
}

function start_minio_dist_erasure()
{
    version=$1 #pass in the version of minio
    name=$2 #name of version, will just be minio if it is latest
    if ["$name" == "minio"]; then
        export MINIO_ROOT_USER=$ACCESS_KEY
        export MINIO_ROOT_PASSWORD=$SECRET_KEY  
    else 
        export MINIO_ACCESS_KEY=$ACCESS_KEY
        export MINIO_SECRET_KEY=$SECRET_KEY
    fi
    
    export MINIO_ENDPOINTS="http://127.0.0.1:9000${WORK_DIR}/dist-disk1 http://127.0.0.1:9001${WORK_DIR}/dist-disk2 http://127.0.0.1:9002${WORK_DIR}/dist-disk3 http://127.0.0.1:9003${WORK_DIR}/dist-disk4"
    for i in $(seq 0 3); do
        "${version[@]}" server --address ":900${i}" > "$WORK_DIR/dist-minio-900${i}.log" 2>&1 &
    done
    sleep 20
}



function run_create_test_fs()
{
    #2 inputs minio variable set up in top and corresponding name of minio
    #have create as function and verify as function and then pass var names of the release
    minioVersion=$1
    versionName=$2

    start_minio_fs minioVersion versionName #starts older version #instead pass minio variable name

    cd "$WORK_DIR"
    
    echo "Starting create on older version"
    (./kitchensink create $SERVER_ENDPOINT $ACCESS_KEY $SECRET_KEY $BUCKET_NAME)
    rv=$? #if this fails then i just do pkill of minio.Release and printing log files dont do verify
    if [ "$rv" -ne 0 ]; then
        pkill versionName
        cat "$WORK_DIR/fs-minio.log"
        return "$rv"
    fi

    (./kitchensink verify $SERVER_ENDPOINT $ACCESS_KEY $SECRET_KEY $BUCKET_NAME)
    rv=$?

    pkill versionName
    
    sleep 5

    if [ "$rv" -ne 0 ]; then
        cat "$WORK_DIR/fs-minio.log"
    fi
    rm -f "$WORK_DIR/fs-minio.log"
}

function run_verify_test_fs()
{
    minioVersion=$1
    versionName=$2

    start_minio_fs minioVersion versionName #starts newer

    echo "Verifying on newer version"
    (./kitchensink verify $SERVER_ENDPOINT $ACCESS_KEY $SECRET_KEY $BUCKET_NAME)
    rv=$?

    ./kitchensink delete $SERVER_ENDPOINT $ACCESS_KEY $SECRET_KEY $BUCKET_NAME
    
    pkill versionName

    sleep 3

    if [ "$rv" -ne 0 ]; then
        cat "$WORK_DIR/fs-minio.log"
    fi
    rm -f "$WORK_DIR/fs-minio.log"

    return "$rv"
}

function run_create_test_erasure()
{
    minioVersion=$1
    versionName=$2

    start_minio_erasure minioVersion versionName

    cd "$WORK_DIR"
    rv=$?

    echo "Starting create on older version"
    (./kitchensink create $SERVER_ENDPOINT $ACCESS_KEY $SECRET_KEY $BUCKET_NAME)
    rv=$?
    if [ "$rv" -ne 0 ]; then
        pkill versionName
        cat "$WORK_DIR/erasure-minio.log"
        return "$rv"
    fi
    (./kitchensink verify $SERVER_ENDPOINT $ACCESS_KEY $SECRET_KEY $BUCKET_NAME)
    rv=$?

    pkill versionName
    sleep 5

    if [ "$rv" -ne 0 ]; then    
        cat "$WORK_DIR/erasure-minio.log"
    fi
    rm -f "$WORK_DIR/erasure-minio.log"

    return "$rv"
}

function run_verify_test_erasure()
{
    minioVersion=$1
    versionName=$2

    start_minio_erasure minioVersion versionName #starts newer

    echo "Verifying on newer version"
    (./kitchensink verify $SERVER_ENDPOINT $ACCESS_KEY $SECRET_KEY $BUCKET_NAME)
    rv=$?

    ./kitchensink delete $SERVER_ENDPOINT $ACCESS_KEY $SECRET_KEY $BUCKET_NAME
    
    pkill versionName

    sleep 3

    if [ "$rv" -ne 0 ]; then
        cat "$WORK_DIR/erasure-minio.log"
    fi
    rm -f "$WORK_DIR/erasure-minio.log"

    return "$rv"
}

function run_create_erasure_sets()
{
    minioVersion=$1
    versionName=$2

    start_minio_erasure_sets minioVersion versionName

    cd "$WORK_DIR"
    rv=$?

    echo "Starting create on older version"
    (./kitchensink create $SERVER_ENDPOINT $ACCESS_KEY $SECRET_KEY $BUCKET_NAME)
    rv=$?
    if [ "$rv" -ne 0 ]; then
        pkill versionName
        cat "$WORK_DIR/erasure-minio-sets.log"
        return "$rv"
    fi
    (./kitchensink verify $SERVER_ENDPOINT $ACCESS_KEY $SECRET_KEY $BUCKET_NAME)
    rv=$?

    pkill versionName
    sleep 5

    if [ "$rv" -ne 0 ]; then
        cat "$WORK_DIR/erasure-minio-sets.log"
    fi
    rm -f "$WORK_DIR/erasure-minio-sets.log"

    return "$rv"
}

function run_verify_erasure_sets() {
    minioVersion=$1
    versionName=$2

    start_minio_erasure_sets minioVersion versionName #starts newer

    echo "Verifying on newer version"
    (./kitchensink verify $SERVER_ENDPOINT $ACCESS_KEY $SECRET_KEY $BUCKET_NAME)
    rv=$?

    ./kitchensink delete $SERVER_ENDPOINT $ACCESS_KEY $SECRET_KEY $BUCKET_NAME
    
    pkill versionName

    sleep 3

    if [ "$rv" -ne 0 ]; then
        cat "$WORK_DIR/erasure-minio-sets.log"
    fi
    rm -f "$WORK_DIR/erasure-minio-sets.log"

    return "$rv"
}


function run_create_pool_erasure_sets()
{
    minioVersion=$1
    versionName=$2

    start_minio_pool_erasure_sets minioVersion versionName

    cd "$WORK_DIR"

    echo "Starting create on older version"
    (./kitchensink create $SERVER_ENDPOINT $ACCESS_KEY $SECRET_KEY $BUCKET_NAME)
    rv=$?
    if [ "$rv" -ne 0 ]; then
        pkill versionName
        cat "$WORK_DIR/erasure-minio.log"
        return "$rv"
    fi
    (./kitchensink verify $SERVER_ENDPOINT $ACCESS_KEY $SECRET_KEY $BUCKET_NAME)
    rv=$?

    pkill versionName
    sleep 5

    if [ "$rv" -ne 0 ]; then
        for i in $(seq 0 1); do
            echo "server$i log:"
            cat "$WORK_DIR/pool-minio-900$i.log"
        done
    fi

    for i in $(seq 0 1); do
        rm -f "$WORK_DIR/pool-minio-900$i.log"
    done

    return "$rv"
}

function run_verify_pool_erasure_sets()
{
    minioVersion=$1
    versionName=$2

    start_minio_erasure_pool_sets minioVersion versionName #starts newer

    echo "Verifying on newer version"
    (./kitchensink verify $SERVER_ENDPOINT $ACCESS_KEY $SECRET_KEY $BUCKET_NAME)
    rv=$?

    ./kitchensink delete $SERVER_ENDPOINT $ACCESS_KEY $SECRET_KEY $BUCKET_NAME
    
    pkill versionName

    sleep 3

    if [ "$rv" -ne 0 ]; then
        cat "$WORK_DIR/erasure-minio.log"
    fi
    rm -f "$WORK_DIR/erasure-minio.log"

    return "$rv"
}

function run_create_pool_erasure_sets_ipv6()
{
    minioVersion=$1
    versionName=$2

    start_minio_pool_erasure_sets_ipv6 minioVersion versionName

    export SERVER_ENDPOINT="[::1]:9000"

    cd "$WORK_DIR"
    
    echo "Starting create on older version"
    (./kitchensink create $SERVER_ENDPOINT $ACCESS_KEY $SECRET_KEY $BUCKET_NAME)
    rv=$?
    if [ "$rv" -ne 0 ]; then
        pkill versionName
        cat "$WORK_DIR/erasure-minio.log"
        return "$rv"
    fi
    (./kitchensink verify $SERVER_ENDPOINT $ACCESS_KEY $SECRET_KEY $BUCKET_NAME)
    rv=$?

    pkill versionName
    sleep 5

    if [ "$rv" -ne 0 ]; then
        for i in $(seq 0 1); do
            echo "server$i log:"
            cat "$WORK_DIR/pool-minio-ipv6-900$i.log"
        done
    fi

    for i in $(seq 0 1); do
        rm -f "$WORK_DIR/pool-minio-ipv6-900$i.log"
    done

    return "$rv"
}

function run_verify_pool_erasure_sets_ipv6()
{
    minioVersion=$1
    versionName=$2

    start_minio_pool_erasure_sets_ipv6 minioVersion versionName

    echo "Verifying on newer version"
    (./kitchensink verify $SERVER_ENDPOINT $ACCESS_KEY $SECRET_KEY $BUCKET_NAME)
    rv=$?

    ./kitchensink delete $SERVER_ENDPOINT $ACCESS_KEY $SECRET_KEY $BUCKET_NAME
    
    pkill versionName

    sleep 3

    if [ "$rv" -ne 0 ]; then
        cat "$WORK_DIR/erasure-minio.log"
    fi
    rm -f "$WORK_DIR/erasure-minio.log"

    return "$rv"
}


function run_create_dist_erasure()
{
    minioVersion=$1
    versionName=$2

    start_minio_dist_erasure minioVersion versionName

    cd "$WORK_DIR"

    echo "Starting create on older version"
    (./kitchensink create $SERVER_ENDPOINT $ACCESS_KEY $SECRET_KEY $BUCKET_NAME)
    rv=$?
    if [ "$rv" -ne 0 ]; then
        pkill versionName
        cat "$WORK_DIR/erasure-minio.log"
        return "$rv"
    fi
    (./kitchensink verify $SERVER_ENDPOINT $ACCESS_KEY $SECRET_KEY $BUCKET_NAME)
    rv=$?

    pkill versionName
    sleep 5

    if [ "$rv" -ne 0 ]; then
        echo "server1 log:"
        cat "$WORK_DIR/dist-minio-9000.log"
        echo "server2 log:"
        cat "$WORK_DIR/dist-minio-9001.log"
        echo "server3 log:"
        cat "$WORK_DIR/dist-minio-9002.log"
        echo "server4 log:"
        cat "$WORK_DIR/dist-minio-9003.log"
    fi

    rm -f "$WORK_DIR/dist-minio-9000.log" "$WORK_DIR/dist-minio-9001.log" "$WORK_DIR/dist-minio-9002.log" "$WORK_DIR/dist-minio-9003.log"

    return "$rv"
}

function run_verify_dist_erasure()
{
    minioVersion=$1
    versionName=$2

    start_minio_dist_erasure minioVersion versionName #starts newer

    echo "Verifying on newer version"
    (./kitchensink verify $SERVER_ENDPOINT $ACCESS_KEY $SECRET_KEY $BUCKET_NAME)
    rv=$?

    ./kitchensink delete $SERVER_ENDPOINT $ACCESS_KEY $SECRET_KEY $BUCKET_NAME
    
    pkill versionName

    sleep 3

    if [ "$rv" -ne 0 ]; then
        echo "server1 log:"
        cat "$WORK_DIR/dist-minio-9000.log"
        echo "server2 log:"
        cat "$WORK_DIR/dist-minio-9001.log"
        echo "server3 log:"
        cat "$WORK_DIR/dist-minio-9002.log"
        echo "server4 log:"
        cat "$WORK_DIR/dist-minio-9003.log"
    fi

    rm -f "$WORK_DIR/dist-minio-9000.log" "$WORK_DIR/dist-minio-9001.log" "$WORK_DIR/dist-minio-9002.log" "$WORK_DIR/dist-minio-9003.log"

    return "$rv"
}


function purge()
{
    rm -rf "$1"
}

function __init__()
{
    echo "Initializing environment"
    mkdir -p "$WORK_DIR"
    mkdir -p "$MINIO_CONFIG_DIR"

    ## version is purposefully set to '3' for minio to migrate configuration file
    echo '{"version": "3", "credential": {"accessKey": "minio", "secretKey": "minio123"}, "region": "us-east-1"}' > "$MINIO_CONFIG_DIR/config.json"

    KITCHENSINK_VERSION=$(curl --retry 10 -Ls -o /dev/null -w "%{url_effective}" https://github.com/minio/kitchensink/releases/latest | sed "s/https:\/\/github.com\/minio\/kitchensink\/releases\/tag\///")
    if [ -z "$KITCHENSINK_VERSION" ]; then
        echo "unable to get kitchensink version from github"
        exit 1
    fi

    test_run_dir="$WORK_DIR/kitchensink"
    $WGET --output-document="${test_run_dir}/kitchensink" "https://github.com/minio/kitchensink/releases/download/${KITCHENSINK_VERSION}/kitchensink-linux-amd64"
    chmod a+x "${test_run_dir}/kitchensink"
}

function main()
{
    echo "Testing in FS setup"
    if ! ([run_create_test_fs OLDER_MINIO OLDER_MINIO_NAME] || [run_verify_test_fs MINIO MINIO_NAME])
    then
        echo "FAILED"
        purge "$WORK_DIR"
        exit 1
    fi

    echo "Testing in Erasure setup"
    if ! ([run_create_test_erasure OLDER_MINIO OLDER_MINIO_NAME] || [run_verify_test_erasure MINIO MINIO_NAME]) 
    then
        echo "FAILED"
        purge "$WORK_DIR"
        exit 1
    fi

    #tries to migrate which gives error
    echo "Testing in Distributed Erasure setup"
    if ! ([run_create_dist_erasure OLDER_MINIO OLDER_MINIO_NAME] || [run_verify_dist_erasure MINIO MINIO_NAME])
    then
        echo "FAILED"
        purge "$WORK_DIR"
        exit 1
    fi

    echo "Testing in Erasure setup as sets"
    if ! ([run_create_erasure_sets OLDER_MINIO OLDER_MINIO_NAME] || [run_verify_erasure_sets MINIO MINIO_NAME])
    then
        echo "FAILED"
        purge "$WORK_DIR"
        exit 1
    fi

    echo "Testing in Distributed Eraure expanded setup"
    if ! ([run_create_pool_erasure_sets OLDER_MINIO OLDER_MINIO_NAME] || [run_verify_pool_erasure_sets MINIO MINIO_NAME])
    then
        echo "FAILED"
        purge "$WORK_DIR"
        exit 1
    fi

    echo "Testing in Distributed Erasure expanded setup with ipv6"
    if ! ([run_create_pool_erasure_sets_ipv6 OLDER_MINIO OLDER_MINIO_NAME] || [run_verify_pool_erasure_sets_ipv6 MINIO MINIO_NAME])
    then
        echo "FAILED"
        purge "$WORK_DIR"
        exit 1
    fi

    purge "$WORK_DIR"
}

( __init__ "$@" && main "$@" )
rv=$?
purge "$WORK_DIR"
exit "$rv"
