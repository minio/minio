#!/bin/bash

# This script is used to test the migration of IAM content from old minio
# instance to new minio instance.
#
# To run it locally, start the LDAP server in github.com/minio/minio-iam-testing
# repo (e.g. make podman-run), and then run this script.
#
# This script assumes that LDAP server is at:
#
#   `localhost:389`
#
# if this is not the case, set the environment variable
# `_MINIO_LDAP_TEST_SERVER`.

OLD_VERSION=RELEASE.2024-03-26T22-10-45Z
OLD_BINARY_LINK=https://dl.min.io/server/minio/release/linux-amd64/archive/minio.${OLD_VERSION}

__init__() {
	if which curl &>/dev/null; then
		echo "curl is already installed"
	else
		echo "Installing curl:"
		sudo apt install curl -y
	fi

	export GOPATH=/tmp/gopath
	export PATH="${PATH}":"${GOPATH}"/bin

	if which mc &>/dev/null; then
		echo "mc is already installed"
	else
		echo "Installing mc:"
		go install github.com/minio/mc@latest
	fi

	if [ ! -x ./minio.${OLD_VERSION} ]; then
		echo "Downloading minio.${OLD_VERSION} binary"
		curl -o minio.${OLD_VERSION} ${OLD_BINARY_LINK}
		chmod +x minio.${OLD_VERSION}
	fi

	if [ -z "$_MINIO_LDAP_TEST_SERVER" ]; then
		export _MINIO_LDAP_TEST_SERVER=localhost:389
		echo "Using default LDAP endpoint: $_MINIO_LDAP_TEST_SERVER"
	fi

	rm -rf /tmp/data
}

create_iam_content_in_old_minio() {
	echo "Creating IAM content in old minio instance."

	MINIO_CI_CD=1 ./minio.${OLD_VERSION} server /tmp/data/{1...4} &
	sleep 5

	set -x
	mc alias set old-minio http://localhost:9000 minioadmin minioadmin
	mc ready old-minio
	mc idp ldap add old-minio \
		server_addr=localhost:389 \
		server_insecure=on \
		lookup_bind_dn=cn=admin,dc=min,dc=io \
		lookup_bind_password=admin \
		user_dn_search_base_dn=dc=min,dc=io \
		user_dn_search_filter="(uid=%s)" \
		group_search_base_dn=ou=swengg,dc=min,dc=io \
		group_search_filter="(&(objectclass=groupOfNames)(member=%d))"
	mc admin service restart old-minio

	mc idp ldap policy attach old-minio readwrite --user=UID=dillon,ou=people,ou=swengg,dc=min,dc=io
	mc idp ldap policy attach old-minio readwrite --group=CN=project.c,ou=groups,ou=swengg,dc=min,dc=io

	mc idp ldap policy entities old-minio

	mc admin cluster iam export old-minio
	set +x

	mc admin service stop old-minio
}

import_iam_content_in_new_minio() {
	echo "Importing IAM content in new minio instance."
	# Assume current minio binary exists.
	MINIO_CI_CD=1 ./minio server /tmp/data/{1...4} &
	sleep 5

	set -x
	mc alias set new-minio http://localhost:9000 minioadmin minioadmin
	echo "BEFORE IMPORT mappings:"
	mc ready new-minio
	mc idp ldap policy entities new-minio
	mc admin cluster iam import new-minio ./old-minio-iam-info.zip
	echo "AFTER IMPORT mappings:"
	mc idp ldap policy entities new-minio
	set +x

	# mc admin service stop new-minio
}

verify_iam_content_in_new_minio() {
	output=$(mc idp ldap policy entities new-minio --json)

	groups=$(echo "$output" | jq -r '.result.policyMappings[] | select(.policy == "readwrite") | .groups[]')
	if [ "$groups" != "cn=project.c,ou=groups,ou=swengg,dc=min,dc=io" ]; then
		echo "Failed to verify groups: $groups"
		exit 1
	fi

	users=$(echo "$output" | jq -r '.result.policyMappings[] | select(.policy == "readwrite") | .users[]')
	if [ "$users" != "uid=dillon,ou=people,ou=swengg,dc=min,dc=io" ]; then
		echo "Failed to verify users: $users"
		exit 1
	fi

	mc admin service stop new-minio
}

main() {
	create_iam_content_in_old_minio

	import_iam_content_in_new_minio

	verify_iam_content_in_new_minio
}

(__init__ "$@" && main "$@")
