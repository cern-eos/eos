#!/bin/bash -ve

sudo ./eos-docker/scripts/shutdown_services.sh
./eos-docker/scripts/remove_unused_images.sh

if [[ -n $CI_COMMIT_TAG ]]; then
  export IMAGE_TAG="${BASETAG}${CI_COMMIT_TAG}";
  export CLI_IMAGE_TAG="${BASETAG}${CLI_BASETAG}${CI_COMMIT_TAG}";
else
  export IMAGE_TAG="${BASETAG}${CI_PIPELINE_ID}";
  export CLI_IMAGE_TAG="${BASETAG}${CLI_BASETAG}${CI_PIPELINE_ID}";
fi


docker pull gitlab-registry.cern.ch/dss/eos:${IMAGE_TAG}
docker pull gitlab-registry.cern.ch/dss/eos:${CLI_IMAGE_TAG}

sudo ./eos-docker/scripts/start_services.sh -q -i gitlab-registry.cern.ch/dss/eos:${IMAGE_TAG} -u gitlab-registry.cern.ch/dss/eos:${CLI_IMAGE_TAG} -k ${KRB5:-"mit"} ;
