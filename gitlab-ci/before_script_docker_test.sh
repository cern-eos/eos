#!/bin/bash -ve

./eos-docker/scripts/shutdown_services.sh
./eos-docker/scripts/remove_unused_images.sh

export IMAGE_REPO="gitlab-registry.cern.ch/dss/eos/eos-ci"
# either $CI_COMMIT_TAG either $CI_COMMIT_SHORT_SHA
export IMAGE_TAG="${CI_COMMIT_TAG:-$CI_COMMIT_SHORT_SHA}${OS_TAG}"
export CLI_IMAGE_TAG="${CLI_BASETAG}${CI_COMMIT_TAG:-$CI_COMMIT_SHORT_SHA}${OS_TAG}"

./eos-docker/scripts/start_services.sh -q -i ${IMAGE_REPO}:${IMAGE_TAG} -u ${IMAGE_REPO}:${CLI_IMAGE_TAG} -k ${KRB5:-"mit"}
