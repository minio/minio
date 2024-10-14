#!/usr/bin/env bash

# shellcheck disable=SC2120
exit_1() {
	cleanup_and_prune
	exit 1
}

cleanup() {
	echo "Cleaning up MinIO deployment"
	# Remove any existing buckets
	bkts=$(mc ls myminio --json | jq '.key')
	if [ "$bkts" != null ] && [ "${bkts}" != "" ]; then
		for entry in $bkts; do
			bkt=${entry//\"/}
			echo "Removing bucket: $bkt"
			mc rb "myminio/$bkt" --force --dangerous
		done
	fi
	docker compose -f "${DOCKER_COMPOSE_FILE}" stop
}

cleanup_and_prune() {
	cleanup
	docker container prune --force
}

if [ ! -f ./mc ]; then
	wget -O mc https://dl.minio.io/client/mc/release/linux-amd64/mc &&
		chmod +x mc
fi

export MC_HOST_myminio=http://minioadmin:minioadmin@localhost:9000

cleanup_and_prune

# Run mint test in a loop against MinIO
count=0
while true; do
	if [ $count -eq 10 ]; then
		break
	fi
	time docker compose -f "${DOCKER_COMPOSE_FILE}" up -d
	{ docker run --net=docker-compose_default -e SERVER_ENDPOINT=nginx:9000 -e ACCESS_KEY=minioadmin -e SECRET_KEY=minioadmin -e ENABLE_HTTPS=0 -e MINT_MODE=full minio/mint:edge >/tmp/mint-tests.log; } 2>&1
	out=$(grep "nginx_1.*503" </tmp/mint-tests.log)
	if [ "${out}" != "" ]; then
		echo "Error: $out"
		echo "BUG: Found a 503 error in MinIO logs"
		exit_1
	fi
	cleanup
	count=$((count + 1))
done

echo "SUCCESS"
cleanup_and_prune
