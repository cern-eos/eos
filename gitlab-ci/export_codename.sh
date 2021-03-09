#!/bin/bash

#Gitlab CI enabled branches should be listed here

if [[ "${CI_COMMIT_REF_NAME}" == "citrine" ]] || [[ ${CI_COMMIT_REF_NAME} == 4.* ]] ; then
  export CODENAME="citrine" # @note not supported anymore
else
  export CODENAME="diopside"
fi
