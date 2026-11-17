#!/bin/bash -ve

export KUBECONFIG=$K8S_CONFIG # get access configs for the cluster
git clone https://gitlab.cern.ch/eos/eos-on-k8s.git || exit $?
if [[ ${EOS_K8S_HA_TEST:-0} == 1 ]]; then
  EOS_K8S_FIXTURE_DIR="$PWD/eos-on-k8s" python3 ./gitlab-ci/tests/test_k8s_fixture.py || exit $?
fi
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
      # WLCG tokens of the issuer that "eos-tape-rest-token-test -S" serves.
      # Unconditional: a missing library must fail the job, not skip the test.
      grep -q '^mgmofs\.macaroonslib .*libXrdAccSciTokens' /etc/xrd.cf.mgm ||
        sed -i '/^mgmofs\.macaroonslib /s/$/ libXrdAccSciTokens.so/' /etc/xrd.cf.mgm
      # XrdSciTokens re-reads its configuration, so replace it, never rewrite it.
      printf '%s\n' '[Global]' 'audience = https://wlcg.cern.ch/jwt/v1/any' '' \
        '[Issuer tapetest]' "issuer = https://$(hostname -f):4443" 'base_path = /' \
        'map_subject = False' 'default_user = eos-user' > /etc/xrootd/scitokens.cfg.new
      if cmp -s /etc/xrootd/scitokens.cfg.new /etc/xrootd/scitokens.cfg; then
        rm -f /etc/xrootd/scitokens.cfg.new
      else
        mv -f /etc/xrootd/scitokens.cfg.new /etc/xrootd/scitokens.cfg
      fi
    fi
EOF
fi

./eos-on-k8s/create-all.sh -b "${IMAGE_REPO}" -i "${IMAGE_TAG}" \
  -u "${CLI_IMAGE_TAG}" -n "${K8S_NAMESPACE}" -k "${KRB5:-mit}" \
  -m "${EOS_K8S_MGM_COUNT:-1}" -f "${EOS_K8S_FST_COUNT:-8}" \
  ${EOS_MGM_ENABLE_REST_API:+-r} || exit $?

if [[ ${EOS_K8S_HA_TEST:-0} == 1 ]]; then
  timeout -s KILL 30 kubectl --request-timeout=20s label namespace \
    "$K8S_NAMESPACE" eos.cern.ch/ha-test=true --overwrite || exit $?
fi
