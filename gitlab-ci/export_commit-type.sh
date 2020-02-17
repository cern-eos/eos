#!/bin/bash

if [[ -n $CI_COMMIT_TAG ]]; then
    export COMMIT_TYPE=tag
else
    export COMMIT_TYPE=commit # @note schedules will work as commit
fi

# GitLab CI/CD predefined environment variables reference https://docs.gitlab.com/ee/ci/variables/predefined_variables.html
