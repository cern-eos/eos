#!/bin/bash

#Gitlab CI enabled branches should be listed here

if [[ "${CI_BUILD_REF_NAME}" == "beryl_aquamarine" ]] || [[ ${CI_BUILD_REF_NAME} == 0.3* ]] ; then
  REFNAME="aquamarine"
else
  if [[ "${CI_JOB_STAGE}" == "publish" ]]; then
    REFNAME="citrine-xdc"
  else
    REFNAME="citrine"
  fi
fi

export BRANCH=$REFNAME
