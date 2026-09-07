#!/bin/bash -ve

export KUBECONFIG=$K8S_CONFIG # get access configs for the cluster
git clone https://gitlab.cern.ch/eos/eos-on-k8s.git
export K8S_NAMESPACE=$(echo ${CI_JOB_NAME}-${CI_JOB_ID}-${CI_PIPELINE_ID} | tr '_' '-' | tr '[:upper:]' '[:lower:]')

export IMAGE_REPO="gitlab-registry.cern.ch/dss/eos/eos-ci"
# either $CI_COMMIT_TAG either $CI_COMMIT_SHORT_SHA
export IMAGE_TAG="${CI_COMMIT_TAG:-$CI_COMMIT_SHORT_SHA}${OS_TAG}"
export CLI_IMAGE_TAG="${CLI_BASETAG}${CI_COMMIT_TAG:-$CI_COMMIT_SHORT_SHA}${OS_TAG}"

# Turn the grpc-gateway REST API on so that eos-grpc-gateway-test can exercise it.
sed -i '/^        env:$/a\        - name: EOS_MGM_ENABLE_REST_API\n          value: "1"' eos-on-k8s/templates/eos-mgm.template.yaml
grep -q "EOS_MGM_ENABLE_REST_API" eos-on-k8s/templates/eos-mgm.template.yaml || \
  { echo "error: failed to enable the REST API in the eos-on-k8s MGM template"; exit 1; }

./eos-on-k8s/create-all.sh -b ${IMAGE_REPO} -i ${IMAGE_TAG} -u ${CLI_IMAGE_TAG} -n ${K8S_NAMESPACE} -k ${KRB5:-"mit"}
