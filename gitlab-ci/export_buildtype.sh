#!/bin/bash

COMMIT_LEN=$1
RELEASE_LEN=$(find . -name "eos-*.src.rpm" -print0 \
    | awk -F "-" '{print $3;}' \
    | awk -F "." '{print length($1);}')

if [[ ${RELEASE_LEN} -eq ${COMMIT_LEN} ]]; then
  BUILD_TYPE="commit"
else
  BUILD_TYPE="tag"
fi

export BUILD_TYPE=${BUILD_TYPE}
