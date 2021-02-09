#!/bin/bash

#Gitlab CI enabled branches should be listed here

if [[ "${CI_COMMIT_REF_NAME}" == "beryl_aquamarine" ]] || [[ ${CI_COMMIT_REF_NAME} == 0.3* ]] ; then
  export CODENAME="aquamarine" # @note not supported anymore
else
  export CODENAME="citrine"
fi
