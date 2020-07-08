#!/bin/bash

#Gitlab CI enabled branches should be listed here

if [[ "${CI_BUILD_REF_NAME}" == "beryl_aquamarine" ]] || [[ ${CI_BUILD_REF_NAME} == 0.3* ]] ; then
  export BRANCH="aquamarine"
else
  export BRANCH="citrine"
fi
