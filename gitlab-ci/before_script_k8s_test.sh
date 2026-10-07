#!/bin/bash -ve

export KUBECONFIG=$K8S_CONFIG # get access configs for the cluster
git clone https://gitlab.cern.ch/eos/eos-on-k8s.git
export K8S_NAMESPACE=$(echo ${CI_JOB_NAME}-${CI_JOB_ID}-${CI_PIPELINE_ID} | tr '_' '-' | tr '[:upper:]' '[:lower:]')

export IMAGE_REPO="${CI_REGISTRY_IMAGE}/eos-ci"
# either $CI_COMMIT_TAG either $CI_COMMIT_SHORT_SHA
export IMAGE_TAG="${CI_COMMIT_TAG:-$CI_COMMIT_SHORT_SHA}${OS_TAG}"
export CLI_IMAGE_TAG="${CLI_BASETAG}${CI_COMMIT_TAG:-$CI_COMMIT_SHORT_SHA}${OS_TAG}"

if [[ ${EOS_MGM_ENABLE_TAPE_REST:-0} == 1 ]]; then
  # The fixture's configure.sh runs before MGM startup and on every readiness probe.
  cat >> eos-on-k8s/templates/configmap.template.yaml <<'EOF'

    if [[ $(hostname -s) == eos-mgm1 ]]; then
      grep -qxF 'export EOS_HA_REDIRECT_READS=1' /etc/sysconfig/eos ||
        printf '%s\n' 'export EOS_HA_REDIRECT_READS=1' >> /etc/sysconfig/eos
      for setting in 'mgmofs.tapeenabled true' 'mgmofs.taperestapi.sitename eos-ci'; do
        grep -qxF "$setting" /etc/xrd.cf.mgm ||
          printf '%s\n' "$setting" >> /etc/xrd.cf.mgm
      done
    fi
EOF
fi

./eos-on-k8s/create-all.sh -b ${IMAGE_REPO} -i ${IMAGE_TAG} -u ${CLI_IMAGE_TAG} -n ${K8S_NAMESPACE} -k ${KRB5:-"mit"} ${EOS_MGM_ENABLE_REST_API:+-r}
