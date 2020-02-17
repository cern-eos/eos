#!/bin/bash -ve

./eos-docker/scripts/collect_logs.sh eos-logs/
sudo ./eos-docker/scripts/shutdown_services.sh

#docker system prune --all --filter until=30m --force # @note will exploit flag when docker is updated
docker system prune --force # remove unused docker resources