#!/bin/bash -ve

export KUBECONFIG=$K8S_CONFIG # get access configs for the cluster
export K8S_NAMESPACE=$(echo ${CI_JOB_NAME}-${CI_JOB_ID}-${CI_PIPELINE_ID} | tr '_' '-' | tr '[:upper:]' '[:lower:]')
set -o pipefail
# shellcheck source=gitlab-ci/utilities_func_for_tests.sh
source ./gitlab-ci/utilities_func_for_tests.sh --type k8s "$K8S_NAMESPACE"
cleanup_status=0
if [[ ${EOS_K8S_HA_TEST:-0} == 1 ]]; then
  # Fallback for interrupted setup or a finalizer that could not delete the namespace.
  if ! cleanup_k8s_fixture "$K8S_NAMESPACE" "eos-logs-${CI_JOB_ID}/" 2>&1 |
      tee -a "eos-ha-${CI_JOB_ID}.log"; then
    cleanup_status=1
  fi
else
  if ! cleanup_k8s_fixture "$K8S_NAMESPACE" "eos-logs-${CI_JOB_ID}/"; then
    cleanup_status=1
  fi
fi
rm -rf eos-on-k8s/
(( cleanup_status == 0 ))
