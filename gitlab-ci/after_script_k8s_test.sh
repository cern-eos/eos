#!/bin/bash -ve

export KUBECONFIG=$K8S_CONFIG # get access configs for the cluster
export K8S_NAMESPACE=$(echo ${CI_JOB_NAME}-${CI_JOB_ID}-${CI_PIPELINE_ID} | tr '_' '-' | tr '[:upper:]' '[:lower:]')
./eos-on-k8s/collect_logs.sh ${K8S_NAMESPACE} eos-logs-${CI_JOB_ID}/
./eos-on-k8s/delete-all.sh ${K8S_NAMESPACE}
rm -rf eos-on-k8s/
