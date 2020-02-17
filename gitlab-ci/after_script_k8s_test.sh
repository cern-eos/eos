#!/bin/bash -ve

source /home/gitlab-runner/coe-cluster-config/st-gitlab-k8s-02/env_st-gitlab-k8s-02.sh # get access configs for the cluster
export K8S_NAMESPACE=$(echo ${CI_JOB_NAME}-${CI_JOB_ID}-${CI_PIPELINE_ID} | tr '_' '-' | tr '[:upper:]' '[:lower:]')
./eos-on-k8s/collect_logs.sh ${K8S_NAMESPACE} eos-logs-${K8S_NAMESPACE}/
./eos-on-k8s/delete-all.sh ${K8S_NAMESPACE}
rm -rf eos-on-k8s/
