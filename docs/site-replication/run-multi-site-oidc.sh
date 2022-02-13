#!/usr/bin/env bash

# shellcheck disable=SC2120
exit_1() {
    cleanup
    exit 1
}

cleanup() {
    echo "Cleaning up instances of MinIO"
    pkill minio
    pkill -9 minio
    rm -rf /tmp/minio{1,2,3}
}

cleanup

unset MINIO_KMS_KES_CERT_FILE
unset MINIO_KMS_KES_KEY_FILE
unset MINIO_KMS_KES_ENDPOINT
unset MINIO_KMS_KES_KEY_NAME

export MINIO_CI_CD=1
export MINIO_BROWSER=off
export MINIO_ROOT_USER="minio"
export MINIO_ROOT_PASSWORD="minio123"
export MINIO_KMS_AUTO_ENCRYPTION=off
export MINIO_PROMETHEUS_AUTH_TYPE=public
export MINIO_KMS_SECRET_KEY=my-minio-key:OSMM+vkKUTCvQs9YL/CVMIMt43HFhkUpqJxTmGl6rYw=
export MINIO_IDENTITY_OPENID_CONFIG_URL="http://localhost:5556/dex/.well-known/openid-configuration"
export MINIO_IDENTITY_OPENID_CLIENT_ID="minio-client-app"
export MINIO_IDENTITY_OPENID_CLIENT_SECRET="minio-client-app-secret"
export MINIO_IDENTITY_OPENID_CLAIM_NAME="groups"
export MINIO_IDENTITY_OPENID_SCOPES="openid,groups"

export MINIO_IDENTITY_OPENID_REDIRECT_URI="http://127.0.0.1:10000/oauth_callback"
minio server --address ":9001" --console-address ":10000" /tmp/minio1/{1...4} >/tmp/minio1_1.log 2>&1 &

export MINIO_IDENTITY_OPENID_REDIRECT_URI="http://127.0.0.1:11000/oauth_callback"
minio server --address ":9002" --console-address ":11000" /tmp/minio2/{1...4} >/tmp/minio2_1.log 2>&1 &

export MINIO_IDENTITY_OPENID_REDIRECT_URI="http://127.0.0.1:12000/oauth_callback"
minio server --address ":9003" --console-address ":12000" /tmp/minio3/{1...4} >/tmp/minio3_1.log 2>&1 &

if [ ! -f ./mc ]; then
    wget -O mc https://dl.minio.io/client/mc/release/linux-amd64/mc \
        && chmod +x mc
fi

sleep 10

export MC_HOST_minio1=http://minio:minio123@localhost:9001
export MC_HOST_minio2=http://minio:minio123@localhost:9002
export MC_HOST_minio3=http://minio:minio123@localhost:9003

./mc admin replicate add minio1 minio2 minio3

./mc admin policy add minio1 projecta ./docs/site-replication/rw.json
sleep 5

./mc admin policy info minio2 projecta >/dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "expecting the command to succeed, exiting.."
    exit_1;
fi
./mc admin policy info minio3 projecta >/dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "expecting the command to succeed, exiting.."
    exit_1;
fi

./mc admin policy remove minio3 projecta

sleep 10
./mc admin policy info minio1 projecta
if [ $? -eq 0 ]; then
    echo "expecting the command to fail, exiting.."
    exit_1;
fi

./mc admin policy info minio2 projecta
if [ $? -eq 0 ]; then
    echo "expecting the command to fail, exiting.."
    exit_1;
fi

./mc admin policy add minio1 projecta ./docs/site-replication/rw.json
sleep 5

# Generate STS credential with STS call to minio1
STS_CRED=$(MINIO_ENDPOINT=http://localhost:9001 go run ./docs/site-replication/gen-oidc-sts-cred.go)

MC_HOST_foo=http://${STS_CRED}@localhost:9001 ./mc ls foo
if [ $? -ne 0 ]; then
    echo "Expected sts credential to work, exiting.."
    exit_1;
fi

sleep 2

# Check that the STS credential works on minio2 and minio3.
MC_HOST_foo=http://${STS_CRED}@localhost:9002 ./mc ls foo
if [ $? -ne 0 ]; then
    echo "Expected sts credential to work, exiting.."
    exit_1;
fi

MC_HOST_foo=http://${STS_CRED}@localhost:9003 ./mc ls foo
if [ $? -ne 0 ]; then
    echo "Expected sts credential to work, exiting.."
    exit_1;
fi

STS_ACCESS_KEY=$(echo ${STS_CRED} | cut -d ':' -f 1)

# Create service account for STS user
./mc admin user svcacct add minio2 $STS_ACCESS_KEY --access-key testsvc --secret-key testsvc123
if [ $? -ne 0 ]; then
    echo "adding svc account failed, exiting.."
    exit_1;
fi

sleep 10

./mc admin user svcacct info minio1 testsvc
if [ $? -ne 0 ]; then
    echo "svc account not mirrored, exiting.."
    exit_1;
fi

./mc admin user svcacct info minio2 testsvc
if [ $? -ne 0 ]; then
    echo "svc account not mirrored, exiting.."
    exit_1;
fi

./mc admin user svcacct rm minio1 testsvc
if [ $? -ne 0 ]; then
    echo "removing svc account failed, exiting.."
    exit_1;
fi

sleep 10
./mc admin user svcacct info minio2 testsvc
if [ $? -eq 0 ]; then
    echo "svc account found after delete, exiting.."
    exit_1;
fi

./mc admin user svcacct info minio3 testsvc
if [ $? -eq 0 ]; then
    echo "svc account found after delete, exiting.."
    exit_1;
fi

./mc mb minio1/newbucket

sleep 5
./mc stat minio2/newbucket
if [ $? -ne 0 ]; then
    echo "expecting bucket to be present. exiting.."
    exit_1;
fi

./mc stat minio3/newbucket
if [ $? -ne 0 ]; then
    echo "expecting bucket to be present. exiting.."
    exit_1;
fi

./mc cp README.md minio2/newbucket/

sleep 5
./mc stat minio1/newbucket/README.md
if [ $? -ne 0 ]; then
    echo "expecting object to be present. exiting.."
    exit_1;
fi

./mc stat minio3/newbucket/README.md
if [ $? -ne 0 ]; then
    echo "expecting object to be present. exiting.."
    exit_1;
fi

./mc rm minio3/newbucket/README.md
sleep 5

./mc stat minio2/newbucket/README.md
if [ $? -eq 0 ]; then
    echo "expected file to be deleted, exiting.."
    exit_1;
fi

./mc stat minio1/newbucket/README.md
if [ $? -eq 0 ]; then
    echo "expected file to be deleted, exiting.."
    exit_1;
fi

./mc mb --with-lock minio3/newbucket-olock
sleep 5

enabled_minio2=$(./mc stat --json minio2/newbucket-olock| jq -r .metadata.ObjectLock.enabled)
if [ $? -ne 0 ]; then
    echo "expected bucket to be mirrored with object-lock but not present, exiting..."
    exit_1;
fi

if [ "${enabled_minio2}" != "Enabled" ]; then
    echo "expected bucket to be mirrored with object-lock enabled, exiting..."
    exit_1;
fi

enabled_minio1=$(./mc stat --json minio1/newbucket-olock| jq -r .metadata.ObjectLock.enabled)
if [ $? -ne 0 ]; then
    echo "expected bucket to be mirrored with object-lock but not present, exiting..."
    exit_1;
fi

if [ "${enabled_minio1}" != "Enabled" ]; then
    echo "expected bucket to be mirrored with object-lock enabled, exiting..."
    exit_1;
fi
