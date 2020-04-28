#!/bin/bash -ve

./eos-docker/scripts/collect_logs.sh eos-logs-${CI_JOB_ID}/
sudo ./eos-docker/scripts/shutdown_services.sh

# remove unused docker resources -> be it on the runner itself, here shutdown_services.sh should be enough
#docker system prune --all --filter until=30m --force # @note will exploit flag when docker is updated
#docker system prune --force # remove unused docker resources
