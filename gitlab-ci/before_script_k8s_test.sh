#!/bin/bash -ve

source /home/gitlab-runner/coe-cluster-config/st-gitlab-k8s-02/env_st-gitlab-k8s-02.sh # get access configs for the cluster
git clone https://gitlab.cern.ch/eos/eos-on-k8s.git
export K8S_NAMESPACE=$(echo ${CI_JOB_NAME}-${CI_JOB_ID}-${CI_PIPELINE_ID} | tr '_' '-' | tr '[:upper:]' '[:lower:]')

# always only one between $CI_COMMIT_TAG $CI_PIPELINE_ID exists
export IMAGE_TAG="${BASETAG}${CI_COMMIT_TAG}${CI_PIPELINE_ID}";
export CLI_IMAGE_TAG="${BASETAG}${CLI_BASETAG}${CI_COMMIT_TAG}${CI_PIPELINE_ID}";

if [[ "$CI_JOB_NAME" =~ "InMemory" ]]; then
    ./eos-on-k8s/create-all.sh -i ${IMAGE_TAG} -u ${CLI_IMAGE_TAG} -n ${K8S_NAMESPACE} -k ${KRB5:-"mit"};
else
    ./eos-on-k8s/create-all.sh -i ${IMAGE_TAG} -u ${CLI_IMAGE_TAG} -n ${K8S_NAMESPACE} -q -k ${KRB5:-"mit"};
fi;
