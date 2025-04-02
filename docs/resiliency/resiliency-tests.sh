#!/usr/bin/env bash

TESTS_RUN_STATUS=1

function cleanup() {
	echo "Cleaning up MinIO deployment"
	docker compose -f "${DOCKER_COMPOSE_FILE}" down --volumes
	for container in $(docker ps -q); do
		echo Removing docker $container
		docker rm -f $container >/dev/null 2>&1
		docker wait $container
	done
}

function cleanup_and_prune() {
	cleanup
	docker system prune --volumes --force
	docker image prune --all --force
}

function verify_resiliency() {
	docs/resiliency/resiliency-verify-script.sh
	RESULT=$(grep "script passed" <resiliency-verify.log)
	if [ "$RESULT" != "script passed" ]; then
		echo -e "${RED}${1} Failed${NC}"
		TESTS_RUN_STATUS=$((TESTS_RUN_STATUS & 0))
	else
		echo -e "${GREEN}${1} Passed${NC}"
	fi
}

function verify_resiliency_failure() {
	docs/resiliency/resiliency-verify-failure-script.sh
	RESULT=$(grep "script passed" <resiliency-verify-failure.log)
	if [ "$RESULT" != "script passed" ]; then
		echo -e "${RED}${1} Failed${NC}"
		TESTS_RUN_STATUS=$((TESTS_RUN_STATUS & 0))
	else
		echo -e "${GREEN}${1} Passed${NC}"
	fi
}

function verify_resiliency_healing() {
	local WANT=$2
	docs/resiliency/resiliency-verify-healing-script.sh "$WANT"
	RESULT=$(grep "script passed" <resiliency-verify-healing.log)
	if [ "$RESULT" != "script passed" ]; then
		echo -e "${RED}${1} Failed${NC}"
		TESTS_RUN_STATUS=$((TESTS_RUN_STATUS & 0))
	else
		echo -e "${GREEN}${1} Passed${NC}"
	fi
}

function test_resiliency_success_with_server_down() {
	echo
	echo -e "${GREEN}Running test_resiliency_success_with_server_down ...${NC}"
	# Stop one node
	docker stop resiliency-minio1-1
	sleep 10

	verify_resiliency "${FUNCNAME[0]}"

	# Finally restart the node
	docker start resiliency-minio1-1

	./mc ready myminio
}

function test_resiliency_failure_with_server_down_and_single_disk_offline() {
	echo
	echo -e "${GREEN}Running test_resiliency_failure_with_server_down_and_single_disk_offline ...${NC}"
	# Stop one node
	docker stop resiliency-minio1-1
	# In additional, suspend one more disk per set in order to induce a failure
	docker exec resiliency-minio2-1 /bin/sh -c "mv /data2/.minio.sys /data2/.minio.bkp && touch /data2/.minio.sys"
	docker exec resiliency-minio3-1 /bin/sh -c "mv /data6/.minio.sys /data6/.minio.bkp && touch /data6/.minio.sys"
	sleep 10

	verify_resiliency_failure "${FUNCNAME[0]}"

	# Enable the disks back on nodes
	docker exec resiliency-minio2-1 /bin/sh -c "rm -rf /data2/.minio.sys && mv /data2/.minio.bkp /data2/.minio.sys"
	docker exec resiliency-minio3-1 /bin/sh -c "rm -rf /data6/.minio.sys && mv /data6/.minio.bkp /data6/.minio.sys"

	# Finally restart the node
	docker start resiliency-minio1-1
	./mc ready myminio
}

function test_resiliency_failure_with_servers_down() {
	echo
	echo -e "${GREEN}Running test_resiliency_failure_with_servers_down ...${NC}"
	# Stop two nodes
	docker stop resiliency-minio1-1
	docker stop resiliency-minio2-1
	sleep 10

	verify_resiliency_failure "${FUNCNAME[0]}"

	# Restart the nodes
	docker start resiliency-minio1-1
	docker start resiliency-minio2-1

	./mc ready myminio
}

function test_resiliency_success_with_disks_offline() {
	echo
	echo -e "${GREEN}Running test_resiliency_success_with_disks_offline ...${NC}"
	# There are 8 disks on each node with EC:4 and two erasure sets.
	# We should be able to safely suspend one disk per set from each server.
	docker exec resiliency-minio1-1 /bin/sh -c "mv /data1/.minio.sys /data1/.minio.bkp && touch /data1/.minio.sys"
	docker exec resiliency-minio1-1 /bin/sh -c "mv /data5/.minio.sys /data5/.minio.bkp && touch /data5/.minio.sys"
	docker exec resiliency-minio2-1 /bin/sh -c "mv /data1/.minio.sys /data1/.minio.bkp && touch /data1/.minio.sys"
	docker exec resiliency-minio2-1 /bin/sh -c "mv /data5/.minio.sys /data5/.minio.bkp && touch /data5/.minio.sys"
	docker exec resiliency-minio3-1 /bin/sh -c "mv /data1/.minio.sys /data1/.minio.bkp && touch /data1/.minio.sys"
	docker exec resiliency-minio3-1 /bin/sh -c "mv /data5/.minio.sys /data5/.minio.bkp && touch /data5/.minio.sys"
	docker exec resiliency-minio4-1 /bin/sh -c "mv /data1/.minio.sys /data1/.minio.bkp && touch /data1/.minio.sys"
	docker exec resiliency-minio4-1 /bin/sh -c "mv /data5/.minio.sys /data5/.minio.bkp && touch /data5/.minio.sys"
	sleep 10

	verify_resiliency "${FUNCNAME[0]}"

	# Finally enable the disks back on nodes
	docker exec resiliency-minio1-1 /bin/sh -c "rm -rf /data1/.minio.sys && mv /data1/.minio.bkp /data1/.minio.sys"
	docker exec resiliency-minio1-1 /bin/sh -c "rm -rf /data5/.minio.sys && mv /data5/.minio.bkp /data5/.minio.sys"
	docker exec resiliency-minio2-1 /bin/sh -c "rm -rf /data1/.minio.sys && mv /data1/.minio.bkp /data1/.minio.sys"
	docker exec resiliency-minio2-1 /bin/sh -c "rm -rf /data5/.minio.sys && mv /data5/.minio.bkp /data5/.minio.sys"
	docker exec resiliency-minio3-1 /bin/sh -c "rm -rf /data1/.minio.sys && mv /data1/.minio.bkp /data1/.minio.sys"
	docker exec resiliency-minio3-1 /bin/sh -c "rm -rf /data5/.minio.sys && mv /data5/.minio.bkp /data5/.minio.sys"
	docker exec resiliency-minio4-1 /bin/sh -c "rm -rf /data1/.minio.sys && mv /data1/.minio.bkp /data1/.minio.sys"
	docker exec resiliency-minio4-1 /bin/sh -c "rm -rf /data5/.minio.sys && mv /data5/.minio.bkp /data5/.minio.sys"

	./mc ready myminio
}

function test_resiliency_failure_with_too_many_disks_offline() {
	echo
	echo -e "${GREEN}Running test_resiliency_failure_with_too_many_disks_offline ...${NC}"
	# There are 8 disks on each node with EC:4 and two erasure sets.
	# We should be able to safely suspend one disk per set from each server.
	# suspending one additional disk from each set should cause failures
	docker exec resiliency-minio1-1 /bin/sh -c "mv /data1/.minio.sys /data1/.minio.bkp && touch /data1/.minio.sys"
	docker exec resiliency-minio2-1 /bin/sh -c "mv /data1/.minio.sys /data1/.minio.bkp && touch /data1/.minio.sys"
	docker exec resiliency-minio3-1 /bin/sh -c "mv /data1/.minio.sys /data1/.minio.bkp && touch /data1/.minio.sys"
	docker exec resiliency-minio4-1 /bin/sh -c "mv /data1/.minio.sys /data1/.minio.bkp && touch /data1/.minio.sys"
	docker exec resiliency-minio1-1 /bin/sh -c "mv /data5/.minio.sys /data5/.minio.bkp && touch /data5/.minio.sys"
	docker exec resiliency-minio2-1 /bin/sh -c "mv /data5/.minio.sys /data5/.minio.bkp && touch /data5/.minio.sys"
	docker exec resiliency-minio3-1 /bin/sh -c "mv /data5/.minio.sys /data5/.minio.bkp && touch /data5/.minio.sys"
	docker exec resiliency-minio4-1 /bin/sh -c "mv /data5/.minio.sys /data5/.minio.bkp && touch /data5/.minio.sys"
	docker exec resiliency-minio2-1 /bin/sh -c "mv /data2/.minio.sys /data2/.minio.bkp && touch /data2/.minio.sys"
	docker exec resiliency-minio3-1 /bin/sh -c "mv /data6/.minio.sys /data6/.minio.bkp && touch /data6/.minio.sys"
	sleep 10

	verify_resiliency_failure "${FUNCNAME[0]}"

	# Finally enable the disks back on nodes
	docker exec resiliency-minio1-1 /bin/sh -c "rm -rf /data1/.minio.sys && mv /data1/.minio.bkp /data1/.minio.sys"
	docker exec resiliency-minio2-1 /bin/sh -c "rm -rf /data1/.minio.sys && mv /data1/.minio.bkp /data1/.minio.sys"
	docker exec resiliency-minio3-1 /bin/sh -c "rm -rf /data1/.minio.sys && mv /data1/.minio.bkp /data1/.minio.sys"
	docker exec resiliency-minio4-1 /bin/sh -c "rm -rf /data1/.minio.sys && mv /data1/.minio.bkp /data1/.minio.sys"
	docker exec resiliency-minio1-1 /bin/sh -c "rm -rf /data5/.minio.sys && mv /data5/.minio.bkp /data5/.minio.sys"
	docker exec resiliency-minio2-1 /bin/sh -c "rm -rf /data5/.minio.sys && mv /data5/.minio.bkp /data5/.minio.sys"
	docker exec resiliency-minio3-1 /bin/sh -c "rm -rf /data5/.minio.sys && mv /data5/.minio.bkp /data5/.minio.sys"
	docker exec resiliency-minio4-1 /bin/sh -c "rm -rf /data5/.minio.sys && mv /data5/.minio.bkp /data5/.minio.sys"
	docker exec resiliency-minio2-1 /bin/sh -c "rm -rf /data2/.minio.sys && mv /data2/.minio.bkp /data2/.minio.sys"
	docker exec resiliency-minio3-1 /bin/sh -c "rm -rf /data6/.minio.sys && mv /data6/.minio.bkp /data6/.minio.sys"

	./mc ready myminio
}

function find_erasure_set_for_file() {
	local DATA_DRIVE=-1
	local FILE=$1
	local DIR=$2
	# Check for existence of file in erasure set 1
	docker exec resiliency-minio1-1 /bin/sh -c "stat /data1/test-bucket/$DIR/$FILE/xl.meta" >/dev/null 2>&1
	STATUS=$?
	if [ $STATUS -eq 0 ]; then
		DATA_DRIVE=1
	fi

	if [ $DATA_DRIVE -eq -1 ]; then
		# Check for existence of file in erasure set 2
		docker exec resiliency-minio1-1 /bin/sh -c "stat /data5/test-bucket/$DIR/$FILE/xl.meta" >/dev/null 2>&1
		STATUS=$?
		if [ $STATUS -eq 0 ]; then
			DATA_DRIVE=5
		fi
	fi
	echo $DATA_DRIVE
}

function test_resiliency_healing_missing_xl_metas() {
	echo
	echo -e "${GREEN}Running test_resiliency_healing_missing_xl_metas ...${NC}"

	DIR="initial-data"
	FILE="file1"
	DATA_DRIVE=$(find_erasure_set_for_file $FILE $DIR)
	STATUS=$?
	if [ $STATUS -ne 0 ]; then
		echo -e "${RED}Could not find erasure set for file: ${FILE}${NC}"
		echo -e "${RED}"${FUNCNAME[0]}" Failed${NC}"
		TESTS_RUN_STATUS=$((TESTS_RUN_STATUS & 0))
		return 1
	fi

	# Remove single xl.meta -- status still green
	OUTPUT=$(docker exec resiliency-minio1-1 /bin/sh -c "rm /data$((DATA_DRIVE))/test-bucket/initial-data/$FILE/xl.meta")
	WANT='{ "before": { "color": "green", "missing": 1, "corrupted": 0 }, "after": { "color": "green", "missing": 0, "corrupted": 0 }, "args": {"file": "'${FILE}'", "dir": "'${DIR}'"} }'
	verify_resiliency_healing "${FUNCNAME[0]}" "${WANT}"

	# Remove two xl.meta's -- status becomes yellow
	OUTPUT=$(docker exec resiliency-minio1-1 /bin/sh -c "rm /data$((DATA_DRIVE))/test-bucket/initial-data/$FILE/xl.meta")
	OUTPUT=$(docker exec resiliency-minio2-1 /bin/sh -c "rm /data$((DATA_DRIVE + 1))/test-bucket/initial-data/$FILE/xl.meta")
	WANT='{ "before": { "color": "yellow", "missing": 2, "corrupted": 0 }, "after": { "color": "green", "missing": 0, "corrupted": 0 }, "args": {"file": "'${FILE}'", "dir": "'${DIR}'"} }'
	verify_resiliency_healing "${FUNCNAME[0]}" "${WANT}"

	# Remove three xl.meta's -- status becomes red (3 missing)
	OUTPUT=$(docker exec resiliency-minio1-1 /bin/sh -c "rm /data$((DATA_DRIVE))/test-bucket/initial-data/$FILE/xl.meta")
	OUTPUT=$(docker exec resiliency-minio2-1 /bin/sh -c "rm /data$((DATA_DRIVE + 1))/test-bucket/initial-data/$FILE/xl.meta")
	OUTPUT=$(docker exec resiliency-minio3-1 /bin/sh -c "rm /data$((DATA_DRIVE + 2))/test-bucket/initial-data/$FILE/xl.meta")
	WANT='{ "before": { "color": "red", "missing": 3, "corrupted": 0 }, "after": { "color": "green", "missing": 0, "corrupted": 0 }, "args": {"file": "'${FILE}'", "dir": "'${DIR}'"} }'
	verify_resiliency_healing "${FUNCNAME[0]}" "${WANT}"

	# Remove four xl.meta's -- status becomes red (4 missing)
	OUTPUT=$(docker exec resiliency-minio1-1 /bin/sh -c "rm /data$((DATA_DRIVE))/test-bucket/initial-data/$FILE/xl.meta")
	OUTPUT=$(docker exec resiliency-minio2-1 /bin/sh -c "rm /data$((DATA_DRIVE + 1))/test-bucket/initial-data/$FILE/xl.meta")
	OUTPUT=$(docker exec resiliency-minio3-1 /bin/sh -c "rm /data$((DATA_DRIVE + 2))/test-bucket/initial-data/$FILE/xl.meta")
	OUTPUT=$(docker exec resiliency-minio4-1 /bin/sh -c "rm /data$((DATA_DRIVE + 3))/test-bucket/initial-data/$FILE/xl.meta")
	WANT='{ "before": { "color": "red", "missing": 4, "corrupted": 0 }, "after": { "color": "green", "missing": 0, "corrupted": 0 }, "args": {"file": "'${FILE}'", "dir": "'${DIR}'"} }'
	verify_resiliency_healing "${FUNCNAME[0]}" "${WANT}"
}

function test_resiliency_healing_truncated_parts() {
	echo
	echo -e "${GREEN}Running test_resiliency_healing_truncated_parts ...${NC}"

	DIR="initial-data"
	FILE="file2"
	DATA_DRIVE=$(find_erasure_set_for_file $FILE $DIR)
	STATUS=$?
	if [ $STATUS -ne 0 ]; then
		echo -e "${RED}Could not find erasure set for file: ${FILE}${NC}"
		echo -e "${RED}"${FUNCNAME[0]}" Failed${NC}"
		TESTS_RUN_STATUS=$((TESTS_RUN_STATUS & 0))
		return 1
	fi

	# Truncate single part -- status still green
	OUTPUT=$(docker exec resiliency-minio1-1 /bin/sh -c "truncate --size=10K /data$((DATA_DRIVE))/test-bucket/initial-data/$FILE/*/part.1")
	WANT='{ "before": { "color": "green", "missing": 0, "corrupted": 1 }, "after": { "color": "green", "missing": 0, "corrupted": 0 }, "args": {"file": "'${FILE}'", "dir": "'${DIR}'"} }'
	verify_resiliency_healing "${FUNCNAME[0]}" "${WANT}"

	# Truncate two parts -- status becomes yellow (2 missing)
	OUTPUT=$(docker exec resiliency-minio2-1 /bin/sh -c "truncate --size=10K /data{$((DATA_DRIVE))..$((DATA_DRIVE + 1))}/test-bucket/initial-data/$FILE/*/part.1")
	WANT='{ "before": { "color": "yellow", "missing": 0, "corrupted": 2 }, "after": { "color": "green", "missing": 0, "corrupted": 0 }, "args": {"file": "'${FILE}'", "dir": "'${DIR}'"} }'
	verify_resiliency_healing "${FUNCNAME[0]}" "${WANT}"

	# Truncate three parts -- status becomes red (3 missing)
	OUTPUT=$(docker exec resiliency-minio3-1 /bin/sh -c "truncate --size=10K /data{$((DATA_DRIVE))..$((DATA_DRIVE + 2))}/test-bucket/initial-data/$FILE/*/part.1")
	WANT='{ "before": { "color": "red", "missing": 0, "corrupted": 3 }, "after": { "color": "green", "missing": 0, "corrupted": 0 }, "args": {"file": "'${FILE}'", "dir": "'${DIR}'"} }'
	verify_resiliency_healing "${FUNCNAME[0]}" "${WANT}"

	# Truncate four parts -- status becomes red (4 missing)
	OUTPUT=$(docker exec resiliency-minio4-1 /bin/sh -c "truncate --size=10K /data{$((DATA_DRIVE))..$((DATA_DRIVE + 3))}/test-bucket/initial-data/$FILE/*/part.1")
	WANT='{ "before": { "color": "red", "missing": 0, "corrupted": 4 }, "after": { "color": "green", "missing": 0, "corrupted": 0 }, "args": {"file": "'${FILE}'", "dir": "'${DIR}'"} }'
	verify_resiliency_healing "${FUNCNAME[0]}" "${WANT}"
}

function induce_bitrot() {
	local NODE=$1
	local DIR=$2
	local FILE=$3
	# Figure out the UUID of the directory where the `part.*` files are stored
	UUID=$(docker exec resiliency-minio$NODE-1 /bin/sh -c "ls -l $DIR/test-bucket/initial-data/$FILE/*/part.1")
	UUID=$(echo $UUID | cut -d " " -f 9 | cut -d "/" -f 6)

	# Determine head and tail size of file where we will introduce bitrot
	FILE_SIZE=$(docker exec resiliency-minio$NODE-1 /bin/sh -c "stat --printf="%s" $DIR/test-bucket/initial-data/$FILE/$UUID/part.1")
	TAIL_SIZE=$((FILE_SIZE - 32 * 2))

	# Extract head and tail of file
	$(docker exec resiliency-minio$NODE-1 /bin/sh -c "cat $DIR/test-bucket/initial-data/$FILE/$UUID/part.1 | head --bytes 32 > /tmp/head")
	$(docker exec resiliency-minio$NODE-1 /bin/sh -c "cat $DIR/test-bucket/initial-data/$FILE/$UUID/part.1 | tail --bytes $TAIL_SIZE > /tmp/tail")

	# Corrupt the part by writing head twice followed by tail
	$(docker exec resiliency-minio$NODE-1 /bin/sh -c "cat /tmp/head /tmp/head /tmp/tail > $DIR/test-bucket/initial-data/$FILE/$UUID/part.1")
}

function test_resiliency_healing_induced_bitrot() {
	echo
	echo -e "${GREEN}Running test_resiliency_healing_induced_bitrot ...${NC}"

	DIR="initial-data"
	FILE="file3"
	DATA_DRIVE=$(find_erasure_set_for_file $FILE $DIR)
	STATUS=$?
	if [ $STATUS -ne 0 ]; then
		echo -e "${RED}Could not find erasure set for file: ${FILE}${NC}"
		echo -e "${RED}"${FUNCNAME[0]}" Failed${NC}"
		TESTS_RUN_STATUS=$((TESTS_RUN_STATUS & 0))
		return 1
	fi

	# Induce bitrot in single part -- status still green
	induce_bitrot "2" "/data"$((DATA_DRIVE + 1)) $FILE
	WANT='{ "before": { "color": "green", "missing": 0, "corrupted": 1 }, "after": { "color": "green", "missing": 0, "corrupted": 0 }, "args": {"file": "'${FILE}'", "dir": "'${DIR}'", "deep": true} }'
	verify_resiliency_healing "${FUNCNAME[0]}" "${WANT}"

	# Induce bitrot in two parts -- status becomes yellow (2 corrupted)
	induce_bitrot "2" "/data"$((DATA_DRIVE)) $FILE
	induce_bitrot "1" "/data"$((DATA_DRIVE + 1)) $FILE
	WANT='{ "before": { "color": "yellow", "missing": 0, "corrupted": 2 }, "after": { "color": "green", "missing": 0, "corrupted": 0 }, "args": {"file": "'${FILE}'", "dir": "'${DIR}'", "deep": true} }'
	verify_resiliency_healing "${FUNCNAME[0]}" "${WANT}"

	# Induce bitrot in three parts -- status becomes red (3 corrupted)
	induce_bitrot "3" "/data"$((DATA_DRIVE)) $FILE
	induce_bitrot "2" "/data"$((DATA_DRIVE + 1)) $FILE
	induce_bitrot "1" "/data"$((DATA_DRIVE + 2)) $FILE
	WANT='{ "before": { "color": "red", "missing": 0, "corrupted": 3 }, "after": { "color": "green", "missing": 0, "corrupted": 0 }, "args": {"file": "'${FILE}'", "dir": "'${DIR}'", "deep": true} }'
	verify_resiliency_healing "${FUNCNAME[0]}" "${WANT}"

	# Induce bitrot in four parts -- status becomes red (4 corrupted)
	induce_bitrot "4" "/data"$((DATA_DRIVE)) $FILE
	induce_bitrot "3" "/data"$((DATA_DRIVE + 1)) $FILE
	induce_bitrot "2" "/data"$((DATA_DRIVE + 2)) $FILE
	induce_bitrot "1" "/data"$((DATA_DRIVE + 3)) $FILE
	WANT='{ "before": { "color": "red", "missing": 0, "corrupted": 4 }, "after": { "color": "green", "missing": 0, "corrupted": 0 }, "args": {"file": "'${FILE}'", "dir": "'${DIR}'", "deep": true} }'
	verify_resiliency_healing "${FUNCNAME[0]}" "${WANT}"
}

function induce_bitrot_for_xlmeta() {
	local NODE=$1
	local DIR=$2
	local FILE=$3

	# Determine head and tail size of file where we will introduce bitrot
	FILE_SIZE=$(docker exec resiliency-minio$NODE-1 /bin/sh -c "stat --printf="%s" $DIR/test-bucket/inlined-data/$FILE/xl.meta")
	HEAD_SIZE=$((FILE_SIZE - 32 * 2))

	# Extract head and tail of file
	$(docker exec resiliency-minio$NODE-1 /bin/sh -c "cat $DIR/test-bucket/inlined-data/$FILE/xl.meta | head --bytes $HEAD_SIZE > /head")
	$(docker exec resiliency-minio$NODE-1 /bin/sh -c "cat $DIR/test-bucket/inlined-data/$FILE/xl.meta | tail --bytes 32 > /tail")

	# Corrupt xl.meta by writing head followed by tail twice
	$(docker exec resiliency-minio$NODE-1 /bin/sh -c "cat /head /tail tmp/tail > $DIR/test-bucket/inlined-data/$FILE/xl.meta")
}

function test_resiliency_healing_inlined_metadata() {
	echo
	echo -e "${GREEN}Running test_resiliency_healing_inlined_metadata ...${NC}"

	DIR="inlined-data"
	FILE="inlined"
	DATA_DRIVE=$(find_erasure_set_for_file $FILE $DIR)
	STATUS=$?
	if [ $STATUS -ne 0 ]; then
		echo -e "${RED}Could not find erasure set for file: ${FILE}${NC}"
		echo -e "${RED}"${FUNCNAME[0]}" Failed${NC}"
		TESTS_RUN_STATUS=$((TESTS_RUN_STATUS & 0))
		return 1
	fi

	# Induce bitrot in single inlined xl.meta -- status still green
	induce_bitrot_for_xlmeta "2" "/data"$((DATA_DRIVE + 1)) $FILE
	WANT='{ "before": { "color": "green", "missing": 0, "corrupted": 1 }, "after": { "color": "green", "missing": 0, "corrupted": 0 }, "args": {"file": "'${FILE}'", "dir": "'${DIR}'"} }'
	verify_resiliency_healing "${FUNCNAME[0]}" "${WANT}"

	# Induce bitrot in two inlined xl.meta's -- status becomes yellow (2 corrupted)
	induce_bitrot_for_xlmeta "3" "/data"$((DATA_DRIVE + 1)) $FILE
	induce_bitrot_for_xlmeta "3" "/data"$((DATA_DRIVE + 2)) $FILE
	WANT='{ "before": { "color": "yellow", "missing": 0, "corrupted": 2 }, "after": { "color": "green", "missing": 0, "corrupted": 0 }, "args": {"file": "'${FILE}'", "dir": "'${DIR}'"} }'
	verify_resiliency_healing "${FUNCNAME[0]}" "${WANT}"

	# Induce bitrot in three inlined xl.meta's -- status becomes red (3 corrupted)
	induce_bitrot_for_xlmeta "4" "/data"$((DATA_DRIVE + 1)) $FILE
	induce_bitrot_for_xlmeta "4" "/data"$((DATA_DRIVE + 2)) $FILE
	induce_bitrot_for_xlmeta "4" "/data"$((DATA_DRIVE + 3)) $FILE
	WANT='{ "before": { "color": "red", "missing": 0, "corrupted": 3 }, "after": { "color": "green", "missing": 0, "corrupted": 0 }, "args": {"file": "'${FILE}'", "dir": "'${DIR}'"} }'
	verify_resiliency_healing "${FUNCNAME[0]}" "${WANT}"

	# Induce bitrot in four inlined xl.meta's -- status becomes red (4 corrupted)
	induce_bitrot_for_xlmeta "1" "/data"$((DATA_DRIVE)) $FILE
	induce_bitrot_for_xlmeta "1" "/data"$((DATA_DRIVE + 1)) $FILE
	induce_bitrot_for_xlmeta "1" "/data"$((DATA_DRIVE + 2)) $FILE
	induce_bitrot_for_xlmeta "1" "/data"$((DATA_DRIVE + 3)) $FILE
	WANT='{ "before": { "color": "red", "missing": 0, "corrupted": 4 }, "after": { "color": "green", "missing": 0, "corrupted": 0 }, "args": {"file": "'${FILE}'", "dir": "'${DIR}'"} }'
	verify_resiliency_healing "${FUNCNAME[0]}" "${WANT}"
}

function main() {
	if [ ! -f ./mc ]; then
		wget -q https://dl.minio.io/client/mc/release/linux-amd64/mc && chmod +x ./mc
	fi

	export MC_HOST_myminio=http://minioadmin:minioadmin@localhost:9000

	cleanup_and_prune

	# Run resiliency tests against MinIO
	docker compose -f "${DOCKER_COMPOSE_FILE}" up -d

	# Initial setup
	docs/resiliency/resiliency-initial-script.sh
	RESULT=$(grep "script passed" <resiliency-initial.log)
	if [ "$RESULT" != "script passed" ]; then
		cleanup_and_prune
		exit 1
	fi

	test_resiliency_healing_missing_xl_metas
	test_resiliency_healing_truncated_parts
	test_resiliency_healing_induced_bitrot
	test_resiliency_healing_inlined_metadata
	test_resiliency_success_with_disks_offline
	test_resiliency_failure_with_too_many_disks_offline
	test_resiliency_success_with_server_down
	test_resiliency_failure_with_server_down_and_single_disk_offline
	test_resiliency_failure_with_servers_down

	local rv=0
	if [ ${TESTS_RUN_STATUS} -ne 1 ]; then
		rv=1
	fi

	cleanup_and_prune
	exit $rv
}

main "$@"
