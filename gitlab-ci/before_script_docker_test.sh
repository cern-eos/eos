#!/bin/bash -ve

sudo ./eos-docker/scripts/shutdown_services.sh
./eos-docker/scripts/remove_unused_images.sh

# always only one between $CI_COMMIT_TAG $CI_PIPELINE_ID exists
export IMAGE_TAG="${BASETAG}${CI_COMMIT_TAG}${CI_PIPELINE_ID}";
export CLI_IMAGE_TAG="${BASETAG}${CLI_BASETAG}${CI_COMMIT_TAG}${CI_PIPELINE_ID}";

docker pull gitlab-registry.cern.ch/dss/eos:${IMAGE_TAG}
docker pull gitlab-registry.cern.ch/dss/eos:${CLI_IMAGE_TAG}

sudo ./eos-docker/scripts/start_services.sh -q -i gitlab-registry.cern.ch/dss/eos:${IMAGE_TAG} -u gitlab-registry.cern.ch/dss/eos:${CLI_IMAGE_TAG} -k ${KRB5:-"mit"} ;
