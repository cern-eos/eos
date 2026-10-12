#!/usr/bin/env bash
# Run the HA test and propagate fixture cleanup failures to the CI job.
# Copyright (C) 2026 CERN/Switzerland. GPL-3.0-or-later.
set -uo pipefail

: "${CI_JOB_ID:?CI_JOB_ID is required}"
: "${K8S_NAMESPACE:?K8S_NAMESPACE is required}"
: "${KUBECONFIG:?KUBECONFIG is required}"
[[ $CI_JOB_ID =~ ^[0-9]+$ ]] || { echo 'error: invalid CI_JOB_ID' >&2; exit 2; }
[[ $K8S_NAMESPACE != default && $K8S_NAMESPACE =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] || {
  echo 'error: refusing empty, default or invalid HA namespace' >&2
  exit 2
}
log_file="eos-ha-${CI_JOB_ID}.log"
# shellcheck source=gitlab-ci/utilities_func_for_tests.sh
source ./gitlab-ci/utilities_func_for_tests.sh --type k8s "$K8S_NAMESPACE"

# shellcheck disable=SC2329 # Invoked by the EXIT trap.
finalize() {
  local status=$? cleanup_status
  trap - EXIT INT TERM
  cleanup_k8s_fixture "$K8S_NAMESPACE" "eos-logs-${CI_JOB_ID}/" 2>&1 | tee -a "$log_file"
  cleanup_status=$?
  if (( status == 0 && cleanup_status != 0 )); then status=$cleanup_status; fi
  exit "$status"
}
trap 'finalize' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

bash ./gitlab-ci/test_mgm_ha.sh 2>&1 | tee "$log_file"
statuses=("${PIPESTATUS[@]}")
if (( statuses[0] != 0 )); then exit "${statuses[0]}"; fi
exit "${statuses[1]}"
