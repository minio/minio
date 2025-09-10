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


function test_bucket_creation_spread_across_instances() {
	echo
	echo -e "${GREEN}Running test_bucket_creation_spread_across_instances ...${NC}"

	for i in {1..4}; do
		./mc mb minio${i}/bucket${i}
		echo "Hi $i!" | ./mc pipe minio$i/bucket$i/obj$i.txt
		STATUS=$?
		if [ $STATUS -ne 0 ]; then
			echo -e "${RED}Could not create bucket on instance: ${i}${NC}"
			echo -e "${RED}"${FUNCNAME[0]}" Failed${NC}"
			TESTS_RUN_STATUS=$((TESTS_RUN_STATUS & 0))
			return 1
		fi
	done

	for i in {1..4}; do
		if test $(mc ls minio$i/ | grep -o 'bucket[1-4]/' | wc -l) != 4 ; then 
			echo -e "${RED}Bucket creation not spread across instances${NC}"
			echo -e "${RED}"${FUNCNAME[0]}" Failed${NC}"
			TESTS_RUN_STATUS=$((TESTS_RUN_STATUS & 0))
			return 1
		fi

		# for each cluster, verify all objects from others are visible
		for j in {1..4}; do
			if test "$(mc cat minio$i/bucket$j/obj$j.txt)" != "Hi $j!"; then 
				echo -e "${RED}Object creation not observed across instances${NC}"
				echo -e "${RED}"${FUNCNAME[0]}" Failed${NC}"
				TESTS_RUN_STATUS=$((TESTS_RUN_STATUS & 0))
				return 1
			fi
		done
	done
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
	docs/federation/federation-initial-script.sh
	RESULT=$(grep "script passed" < federation-initial.log)
	if [ "$RESULT" != "script passed" ]; then
		cleanup_and_prune
		exit 1
	fi

	# Test bucket creation spread across instances
	test_bucket_creation_spread_across_instances

	local rv=0
	if [ ${TESTS_RUN_STATUS} -ne 1 ]; then
		rv=1
	fi

	cleanup_and_prune
	exit $rv
}

main "$@"
