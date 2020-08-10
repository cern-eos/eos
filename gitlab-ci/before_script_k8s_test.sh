#!/bin/bash -ve

source /home/gitlab-runner/coe-cluster-config/st-gitlab-k8s-02/env_st-gitlab-k8s-02.sh # get access configs for the cluster
git clone https://gitlab.cern.ch/eos/eos-on-k8s.git
export K8S_NAMESPACE=$(echo ${CI_JOB_NAME}-${CI_JOB_ID}-${CI_PIPELINE_ID} | tr '_' '-' | tr '[:upper:]' '[:lower:]')

# either $CI_COMMIT_TAG either $CI_COMMIT_SHORT_SHA
export IMAGE_TAG="${BASETAG}${CI_COMMIT_TAG:-$CI_COMMIT_SHORT_SHA}"
export CLI_IMAGE_TAG="${BASETAG}${CLI_BASETAG}${CI_COMMIT_TAG:-$CI_COMMIT_SHORT_SHA}"

./eos-on-k8s/create-all.sh -i ${IMAGE_TAG} -u ${CLI_IMAGE_TAG} -n ${K8S_NAMESPACE} -q -k ${KRB5:-"mit"}
