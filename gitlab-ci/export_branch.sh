#!/bin/bash

#Gitlab CI enabled branches should be listed here

if [[ "${CI_BUILD_REF_NAME}" == "beryl_aquamarine" ]] || [[ ${CI_BUILD_REF_NAME} == 0.3* ]] ; then
  REFNAME="aquamarine"
else
  REFNAME="citrine"
fi

export BRANCH=$REFNAME
