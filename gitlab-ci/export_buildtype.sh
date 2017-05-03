#!/bin/bash

COMMIT_LEN=$1
LOCATION=$2
RELEASE_LEN=$(find $LOCATION -name "eos-*.src.rpm" -print0 \
    | awk -F "-" '{print $3;}' \
    | awk -F "." '{print length($1);}')

if [[ ${RELEASE_LEN} -eq ${COMMIT_LEN} ]]; then
  BUILD_TYPE="commit"
else
  BUILD_TYPE="tag"
fi

export BUILD_TYPE=${BUILD_TYPE}
