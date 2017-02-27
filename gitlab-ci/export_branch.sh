#!/bin/bash

#Gitlab CI enabled branches should be listed here

if [[ "${CI_BUILD_REF_NAME}" == "beryl_aquamarine" ]]; then
  REFNAME="aquamarine"
elif [[ "${CI_BUILD_REF_NAME}"  == "master" ]]; then
  REFNAME="citrine"
fi

export BRANCH=$REFNAME
