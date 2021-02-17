#!/bin/bash -ve

./eos-docker/scripts/shutdown_services.sh
./eos-docker/scripts/remove_unused_images.sh

# either $CI_COMMIT_TAG either $CI_COMMIT_SHORT_SHA
export IMAGE_TAG="${BASETAG}${CI_COMMIT_TAG:-$CI_COMMIT_SHORT_SHA}"
export CLI_IMAGE_TAG="${BASETAG}${CLI_BASETAG}${CI_COMMIT_TAG:-$CI_COMMIT_SHORT_SHA}"

./eos-docker/scripts/start_services.sh -q -i gitlab-registry.cern.ch/dss/eos:${IMAGE_TAG} -u gitlab-registry.cern.ch/dss/eos:${CLI_IMAGE_TAG} -k ${KRB5:-"mit"}
